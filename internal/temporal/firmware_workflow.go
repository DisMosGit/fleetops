package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// Timeouts of the upload's steps. Storage is the long one: a firmware binary is streamed from the
// spooled file into GridFS, so the step's budget has to cover reading a large file while the
// database writes it.
const (
	uploadStepTimeout    = time.Minute
	uploadStorageTimeout = 10 * time.Minute
)

// FirmwareModelCatalog reports the device models a firmware may target: the models at least one
// registered device runs. *devices.Store satisfies it, and the interface lives here because this
// package is where the upload's validation step consumes it.
type FirmwareModelCatalog interface {
	// DeviceModels lists the distinct device models of the registered devices.
	DeviceModels(ctx context.Context) ([]string, error)
}

// FirmwareUploadStore is the store the upload's steps write through. *firmware.Store satisfies
// it, and the interface lives here because this package is where the steps are scheduled.
type FirmwareUploadStore interface {
	// StoreBinary streams the spooled binary at path into GridFS, addressed by version.
	StoreBinary(ctx context.Context, version, path string) (firmware.StoredBinary, error)
	// UpsertMetadata writes one state transition of the upload's record, keyed by version.
	UpsertMetadata(ctx context.Context, md firmware.UploadMetadata) (firmware.Record, error)
	// DiscardUpload removes the record one upload owns and the binary of its version.
	DiscardUpload(ctx context.Context, version, workflowID string) error
}

// DiscardFirmwareUploadRequest is the compensating step's input: the upload whose record and
// binary are to be removed.
type DiscardFirmwareUploadRequest struct {
	// Version is the firmware version the upload stores.
	Version string `json:"version"`
	// WorkflowID is the id of the workflow execution that owns the upload.
	WorkflowID string `json:"workflow_id"`
}

// NewValidateFirmwareModelsActivity returns the target-model validation step as an activity,
// bound to the device registry's catalog. A model no registered device runs is a refusal no retry
// can change, so it is reported as a non-retryable application error and the upload concludes
// without storing anything.
func NewValidateFirmwareModelsActivity(
	catalog FirmwareModelCatalog,
) func(ctx context.Context, req firmware.UploadRequest) error {
	return func(ctx context.Context, req firmware.UploadRequest) error {
		err := firmware.ValidateUpload(ctx, catalog, req.Version, req.Models)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, firmware.ErrIncompatible), errors.Is(err, firmware.ErrInvalidMetadata):
			return temporal.NewNonRetryableApplicationError(
				err.Error(), firmwareModelsRefusedErrorType, err)
		default:
			return fmt.Errorf("validate firmware models: %w", err)
		}
	}
}

// NewStoreFirmwareBinaryActivity returns the storage step as an activity, bound to store: it
// streams the spooled binary into GridFS addressed by version and reports the checksum it
// computed on the way. The step is idempotent under retry — a retried attempt replaces the
// version's object instead of leaving a second one, and it re-reads the same spooled file rather
// than a stream an earlier attempt already consumed.
func NewStoreFirmwareBinaryActivity(
	store FirmwareUploadStore,
) func(ctx context.Context, req firmware.UploadRequest) (firmware.StoredBinary, error) {
	return func(ctx context.Context, req firmware.UploadRequest) (firmware.StoredBinary, error) {
		stored, err := store.StoreBinary(ctx, req.Version, req.SpoolPath)
		if err != nil {
			return firmware.StoredBinary{}, err
		}
		return stored, nil
	}
}

// NewUpsertFirmwareMetadataActivity returns the metadata step as an activity, bound to store: it
// writes one state transition of the upload's record, creating the record on the upload's first
// transition and advancing it on later ones, so a retried write leaves one record. A version
// another upload owns is a refusal no retry can change and is reported as non-retryable.
func NewUpsertFirmwareMetadataActivity(
	store FirmwareUploadStore,
) func(ctx context.Context, md firmware.UploadMetadata) (firmware.Record, error) {
	return func(ctx context.Context, md firmware.UploadMetadata) (firmware.Record, error) {
		rec, err := store.UpsertMetadata(ctx, md)
		if err == nil {
			return rec, nil
		}
		if errors.Is(err, firmware.ErrVersionConflict) {
			return firmware.Record{}, temporal.NewNonRetryableApplicationError(
				err.Error(), firmwareConflictErrorType, err)
		}
		return firmware.Record{}, err
	}
}

// NewDiscardFirmwareUploadActivity returns the compensating step as an activity, bound to store:
// it removes the record an upload owns and the binary of its version, so an upload that stored a
// binary it cannot record leaves neither an orphaned binary nor a record. Both removals are
// idempotent, so a retried attempt converges.
func NewDiscardFirmwareUploadActivity(
	store FirmwareUploadStore,
) func(ctx context.Context, req DiscardFirmwareUploadRequest) error {
	return func(ctx context.Context, req DiscardFirmwareUploadRequest) error {
		return store.DiscardUpload(ctx, req.Version, req.WorkflowID)
	}
}

// FirmwareWorkflow drives one firmware upload from acceptance to its terminal state. It owns the
// upload's whole life: it validates the target models, claims the version's record, drives the
// binary into GridFS as its checksum is computed, records the metadata that references it, and
// concludes as available or failed — in which case it removes what it stored and records why.
//
// Every step is an activity with its own retry policy, and every step is idempotent on its own:
// the workflow performs no I/O, and a retried step does not repeat another step's side effect.
// The state query answers while the upload runs, so an operator reads the state the upload has
// reached instead of the request that started it.
func FirmwareWorkflow(ctx workflow.Context, in firmware.UploadRequest) (firmware.UploadState, error) {
	state := newFirmwareUploadState(ctx, in)
	if err := state.validate(); err != nil {
		return firmware.UploadState{}, err
	}
	if err := workflow.SetQueryHandler(ctx, GetFirmwareUploadStateQueryType,
		func() (firmware.UploadState, error) {
			return state.view(), nil
		}); err != nil {
		return firmware.UploadState{}, fmt.Errorf("register %s query: %w",
			GetFirmwareUploadStateQueryType, err)
	}

	validateCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: uploadStepTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        10 * time.Second,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{firmwareModelsRefusedErrorType},
		},
	})
	metadataCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: uploadStepTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        10 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{firmwareConflictErrorType},
		},
	})
	storeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: uploadStorageTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})

	// Validation comes before anything is written: an upload whose models the device registry
	// does not know must leave neither a record nor a binary behind.
	err := workflow.ExecuteActivity(validateCtx, ValidateFirmwareModelsActivityName, in).
		Get(validateCtx, nil)
	if err != nil {
		return concludeFailed(ctx, metadataCtx, &state, false, err), nil
	}

	// The record is claimed before the binary is stored, so the version belongs to exactly one
	// upload while it runs and its document carries the state the upload has reached. A version
	// another upload already holds fails the claim and is concluded as the conflict it is.
	if _, err := recordTransition(metadataCtx, state, firmware.UploadAccepted, "", nil); err != nil {
		return concludeFailed(ctx, metadataCtx, &state, false, err), nil
	}
	state.storing()
	if _, err := recordTransition(metadataCtx, state, firmware.UploadStoring, "", nil); err != nil {
		return concludeFailed(ctx, metadataCtx, &state, true, err), nil
	}

	var stored firmware.StoredBinary
	err = workflow.ExecuteActivity(storeCtx, StoreFirmwareBinaryActivityName, in).
		Get(storeCtx, &stored)
	if err != nil {
		return concludeFailed(ctx, metadataCtx, &state, true, err), nil
	}

	rec, err := recordTransition(metadataCtx, state, firmware.UploadAvailable, "", &stored)
	if err != nil {
		// The record that would reference the binary cannot be written, so the upload stored
		// nothing usable: remove both and conclude failed, leaving no record behind.
		discardUpload(ctx, storeCtx, state)
		return concludeFailed(ctx, metadataCtx, &state, false, err), nil
	}
	state.available(rec)
	return state.view(), nil
}

// recordTransition writes one state transition of the upload's record through the metadata step.
func recordTransition(
	actxCtx workflow.Context,
	state firmwareUploadState,
	to, reason string,
	stored *firmware.StoredBinary,
) (firmware.Record, error) {
	var rec firmware.Record
	md := firmware.UploadMetadata{
		Version:    state.Version,
		Models:     state.Models,
		WorkflowID: state.WorkflowID,
		State:      to,
		Reason:     reason,
		Binary:     stored,
	}
	if err := workflow.ExecuteActivity(actxCtx, UpsertFirmwareMetadataActivityName, md).
		Get(actxCtx, &rec); err != nil {
		return firmware.Record{}, err
	}
	return rec, nil
}

// concludeFailed records why an upload failed and returns the state it concluded with. An upload
// that claimed its version moves its record to failed so its document names the reason; one that
// never claimed it leaves no record at all. A record that cannot be written does not change the
// outcome — the state query the workflow answers is authoritative — so the failure is logged and
// the upload concludes failed either way.
func concludeFailed(
	ctx workflow.Context,
	actxCtx workflow.Context,
	state *firmwareUploadState,
	claimed bool,
	err error,
) firmware.UploadState {
	class, reason := uploadFailure(err)
	state.fail(class, reason)
	if claimed {
		_, recErr := recordTransition(actxCtx, *state, firmware.UploadFailed, reason, nil)
		if recErr != nil {
			workflow.GetLogger(ctx).Error("record failed firmware upload",
				"version", state.Version, "err", recErr)
		}
	}
	return state.view()
}

// discardUpload removes what an upload stored when it cannot complete its record. A discard that
// fails is logged rather than retried: the upload's outcome is already decided and its state
// query reports the reason, so the version's record — still showing the state it reached before
// the failure — is the only trace the failed compensation leaves.
func discardUpload(ctx workflow.Context, actxCtx workflow.Context, state firmwareUploadState) {
	err := workflow.ExecuteActivity(actxCtx, DiscardFirmwareUploadActivityName,
		DiscardFirmwareUploadRequest{Version: state.Version, WorkflowID: state.WorkflowID}).
		Get(actxCtx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("discard incomplete firmware upload",
			"version", state.Version, "err", err)
	}
}
