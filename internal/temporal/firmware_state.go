package temporal

import (
	"errors"
	"strings"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// Registered names of the firmware upload's workflow, activities, and query. They are explicit
// because Temporal addresses a workflow and an activity by name, and a producer — a starter, the
// Temporal CLI, or an operator — has only the name to go on.
const (
	// FirmwareWorkflowName is the registered name of FirmwareWorkflow.
	FirmwareWorkflowName = "firmware-workflow"
	// ValidateFirmwareModelsActivityName is the registered name of the target-model step.
	ValidateFirmwareModelsActivityName = "validate-firmware-models"
	// StoreFirmwareBinaryActivityName is the registered name of the binary storage step.
	StoreFirmwareBinaryActivityName = "store-firmware-binary"
	// UpsertFirmwareMetadataActivityName is the registered name of the metadata step that
	// records each state transition of an upload.
	UpsertFirmwareMetadataActivityName = "upsert-firmware-metadata"
	// DiscardFirmwareUploadActivityName is the registered name of the step that removes what an
	// upload stored when it cannot complete its record.
	DiscardFirmwareUploadActivityName = "discard-firmware-upload"
	// GetFirmwareUploadStateQueryType is the query returning one upload's state.
	GetFirmwareUploadStateQueryType = "get-firmware-upload-state"
)

// The activity error types the workflow classifies failures by: a refused model and a version
// another upload owns are decisions no retry can change, and each names the class the upload
// concludes with.
const (
	firmwareModelsRefusedErrorType = "FirmwareModelsRefused"
	firmwareConflictErrorType      = "FirmwareVersionConflict"
)

// FirmwareWorkflowID returns the stable workflow id of a firmware version's upload. One version
// has exactly one upload execution under this id for as long as its upload runs, so a retried
// upload reaches the execution that owns the version instead of starting a second one.
func FirmwareWorkflowID(version string) string {
	return "firmware-upload-" + version
}

// firmwareUploadState is the upload workflow's only mutable data: the version it stores, the
// models it targets, the spooled binary it streams from, and the state it has reached. The
// firmware document holds a copy of the state, but this is what the state query and every next
// step read.
type firmwareUploadState struct {
	// Version is the firmware version the upload stores; the upload's identity.
	Version string `json:"version"`
	// Models are the target device models the upload declared.
	Models []string `json:"models"`
	// SpoolPath is the path of the file the request body was spooled to; the storage step
	// streams it into GridFS. The path travels instead of the bytes because an activity's input
	// is serialized.
	SpoolPath string `json:"spool_path"`
	// WorkflowID is the id of the workflow execution driving the upload.
	WorkflowID string `json:"workflow_id"`
	// State is the state the upload has reached: accepted, storing, available, or failed.
	State string `json:"state"`
	// FirmwareID is the stored firmware's id; empty until the record exists.
	FirmwareID string `json:"firmware_id,omitempty"`
	// Checksum is the stored binary's checksum; empty until the binary has been stored.
	Checksum string `json:"checksum,omitempty"`
	// Size is the stored binary's size in bytes.
	Size int64 `json:"size,omitempty"`
	// Failure names the class of a failed upload; empty unless it failed.
	Failure string `json:"failure,omitempty"`
	// Reason is the operator-safe reason an upload failed; empty unless it failed.
	Reason string `json:"reason,omitempty"`
}

// newFirmwareUploadState returns the state of an upload the workflow has just accepted.
func newFirmwareUploadState(ctx workflow.Context, in firmware.UploadRequest) firmwareUploadState {
	return firmwareUploadState{
		Version:    strings.TrimSpace(in.Version),
		Models:     in.Models,
		SpoolPath:  in.SpoolPath,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		State:      firmware.UploadAccepted,
	}
}

// validate refuses an upload the workflow cannot drive: one that names no version, no target
// model, or no spooled binary.
func (s firmwareUploadState) validate() error {
	if s.Version == "" {
		return errors.New("firmware upload: version required")
	}
	if len(s.Models) == 0 {
		return errors.New("firmware upload: at least one target model required")
	}
	if strings.TrimSpace(s.SpoolPath) == "" {
		return errors.New("firmware upload: spooled binary path required")
	}
	return nil
}

// storing moves the upload to the state it reaches as its binary streams into storage. A
// concluded upload keeps the state it concluded with.
func (s *firmwareUploadState) storing() {
	if s.concluded() {
		return
	}
	s.State = firmware.UploadStoring
}

// available concludes the upload with the record it stored.
func (s *firmwareUploadState) available(rec firmware.Record) {
	if s.concluded() {
		return
	}
	s.State = firmware.UploadAvailable
	s.FirmwareID = rec.ID
	s.Checksum = rec.Checksum
	s.Size = rec.Size
}

// fail concludes the upload as failed, naming the class of failure and the operator-safe reason.
// A concluded upload is terminal, so a later failure cannot overwrite an outcome.
func (s *firmwareUploadState) fail(class, reason string) {
	if s.concluded() {
		return
	}
	s.State = firmware.UploadFailed
	s.Failure = class
	s.Reason = reason
}

// concluded reports whether the upload has reached a terminal state, after which it runs no
// further step and its recorded state does not change again.
func (s firmwareUploadState) concluded() bool {
	return s.State == firmware.UploadAvailable || s.State == firmware.UploadFailed
}

// view returns the state as the query and the workflow's result report it.
func (s firmwareUploadState) view() firmware.UploadState {
	return firmware.UploadState{
		Version:    s.Version,
		State:      s.State,
		Concluded:  s.concluded(),
		FirmwareID: s.FirmwareID,
		Models:     s.Models,
		Checksum:   s.Checksum,
		Size:       s.Size,
		Failure:    s.Failure,
		Reason:     s.Reason,
	}
}

// uploadFailure classifies an activity failure: the class the upload concludes with and the
// operator-safe reason to record. The class comes from the error type the refusing step reports,
// and the reason is that step's own message — never the scheduling detail Temporal wraps around
// it, which names worker identities and event ids.
func uploadFailure(err error) (class, reason string) {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case firmwareModelsRefusedErrorType:
			return firmware.FailureRefused, failureReason(appErr)
		case firmwareConflictErrorType:
			return firmware.FailureConflict, failureReason(appErr)
		}
		return firmware.FailureInternal, failureReason(appErr)
	}
	return firmware.FailureInternal, "the upload could not be completed"
}

// failureReason returns the operator-safe reason an application error carries, falling back to a
// fixed description when it names none.
func failureReason(err *temporal.ApplicationError) string {
	if err.Message() != "" {
		return err.Message()
	}
	return "the upload could not be completed"
}
