package firmware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// The states an upload's record moves through. They are the values of the record's state field
// and the states the workflow that owns an upload reports, so one vocabulary describes both what
// is stored and what the state query answers.
const (
	// UploadAccepted is the state of an upload that has been accepted and not started storing.
	UploadAccepted = "accepted"
	// UploadStoring is the state of an upload whose binary is streaming into GridFS.
	UploadStoring = "storing"
	// UploadAvailable is the state of an upload whose binary and record exist and agree.
	UploadAvailable = "available"
	// UploadFailed is the state of an upload that concluded without storing a usable firmware.
	UploadFailed = "failed"
	// UploadUnknown is what a reader reports for a record written before uploads carried a
	// state. Unknown is not failed: the upload may well have succeeded.
	UploadUnknown = "unknown"
)

// The classes of failure an upload can conclude with. They are what a caller maps onto its own
// surface: refused target models are a request to fix, a version conflict is another upload's
// record, and anything else is a step that could not be completed.
const (
	// FailureRefused reports target models the device registry does not know.
	FailureRefused = "refused"
	// FailureConflict reports a version another upload already owns.
	FailureConflict = "conflict"
	// FailureInternal reports a step that could not be completed within its retry budget.
	FailureInternal = "internal"
)

// StoredBinary is what storing one upload's binary produced: the GridFS object the version
// addresses, the checksum computed while the binary streamed into it, and the stored size.
type StoredBinary struct {
	// Ref is the GridFS object the binary is stored as.
	Ref string `json:"ref"`
	// Checksum is the lowercase hexadecimal SHA-256 digest of the stored binary.
	Checksum string `json:"checksum"`
	// Size is the stored binary's size in bytes.
	Size int64 `json:"size"`
}

// UploadMetadata is one transition of an upload's firmware document: what is known about the
// upload at the state being recorded. It is the input of the metadata step, so a caller does not
// need the store's record type to write one.
type UploadMetadata struct {
	// Version is the firmware version the upload stores.
	Version string `json:"version"`
	// Models are the target device models the upload declared.
	Models []string `json:"models"`
	// WorkflowID is the id of the workflow execution that owns the upload.
	WorkflowID string `json:"workflow_id"`
	// State is the state the upload has reached: accepted, storing, available, or failed.
	State string `json:"state"`
	// Reason is the operator-safe reason the upload failed; empty unless State is failed.
	Reason string `json:"reason,omitempty"`
	// Binary is what the upload stored; nil until its binary has been stored.
	Binary *StoredBinary `json:"binary,omitempty"`
}

// UploadState is one upload's state as the workflow that owns it reports it: the state query
// answers it while the upload runs, and a concluded execution records it as its result. It is
// also what a record-backed reader answers, which is how an upload recorded without a workflow
// still reports what its document holds.
type UploadState struct {
	// Version is the firmware version the upload stores; the upload's identity.
	Version string `json:"version"`
	// State is the state the upload has reached: accepted, storing, available, failed, or
	// unknown for a record written before uploads carried a state.
	State string `json:"state"`
	// Concluded reports whether the upload has reached available or failed, after which it
	// runs no further step.
	Concluded bool `json:"concluded"`
	// FirmwareID is the stored firmware's id; empty until the binary has been stored.
	FirmwareID string `json:"firmware_id,omitempty"`
	// Models are the target device models the upload declared.
	Models []string `json:"models,omitempty"`
	// Checksum is the stored binary's checksum; empty until the binary has been stored.
	Checksum string `json:"checksum,omitempty"`
	// Size is the stored binary's size in bytes.
	Size int64 `json:"size,omitempty"`
	// Failure names the class of a failed upload; empty while it runs or once it is available.
	Failure string `json:"failure,omitempty"`
	// Reason is the operator-safe reason the upload failed; empty unless it failed.
	Reason string `json:"reason,omitempty"`
}

// StoreBinary streams the spooled upload at path into GridFS addressed by version: the version
// determines the object, so a retried step replaces that object instead of adding a second one.
// The checksum is computed while the bytes stream into storage, so the binary is never held in
// memory whole, and the returned size is what was actually stored.
func (s *Store) StoreBinary(ctx context.Context, version, path string) (StoredBinary, error) {
	if strings.TrimSpace(version) == "" {
		return StoredBinary{}, fmt.Errorf("%w: version required", ErrInvalidMetadata)
	}
	if strings.TrimSpace(path) == "" {
		return StoredBinary{}, fmt.Errorf("%w: spooled binary path required", ErrInvalidMetadata)
	}
	file, err := os.Open(path)
	if err != nil {
		return StoredBinary{}, fmt.Errorf("open spooled firmware %s: %w", version, err)
	}

	hash := sha256.New()
	ref := binaryRef(version)
	size, storeErr := s.binaries.put(ctx, ref, version, io.TeeReader(file, hash))
	closeErr := file.Close()
	if storeErr != nil {
		return StoredBinary{}, fmt.Errorf("store firmware binary %s: %w", version, storeErr)
	}
	if closeErr != nil {
		return StoredBinary{}, fmt.Errorf("close spooled firmware %s: %w", version, closeErr)
	}
	return StoredBinary{Ref: ref, Checksum: hex.EncodeToString(hash.Sum(nil)), Size: size}, nil
}

// UpsertMetadata writes one transition of an upload's record, keyed by version: the upload's
// first transition creates the record and a later one advances it, so a retried write leaves one
// record rather than a second. A transition the record has already reached leaves it unchanged,
// and a version another upload owns fails with ErrVersionConflict without touching that record.
func (s *Store) UpsertMetadata(ctx context.Context, md UploadMetadata) (Record, error) {
	if strings.TrimSpace(md.Version) == "" {
		return Record{}, fmt.Errorf("%w: version required", ErrInvalidMetadata)
	}
	if strings.TrimSpace(md.WorkflowID) == "" {
		return Record{}, fmt.Errorf("%w: owning workflow required", ErrInvalidMetadata)
	}
	if stateRank(md.State) == 0 {
		return Record{}, fmt.Errorf("%w: unknown upload state %q", ErrInvalidMetadata, md.State)
	}
	id, err := newID("fw")
	if err != nil {
		return Record{}, fmt.Errorf("firmware id: %w", err)
	}
	rec := Record{
		ID:               id,
		Version:          md.Version,
		Models:           md.Models,
		CreatedAt:        time.Now().UTC(),
		State:            md.State,
		UploadWorkflowID: md.WorkflowID,
		FailureReason:    md.Reason,
	}
	if md.Binary != nil {
		rec.Checksum, rec.Size, rec.GridFSID = md.Binary.Checksum, md.Binary.Size, md.Binary.Ref
	}
	transitions, err := s.transitions()
	if err != nil {
		return Record{}, err
	}
	return transitions.upsert(ctx, rec)
}

// DiscardUpload removes what an upload stored: the record it owns and the binary addressed by
// its version. It is the compensation of an upload that stored a binary but cannot complete its
// record, so it is idempotent — a record or object already gone is not an error — and a record
// that is not this upload's, or one that already became available, is refused rather than
// discarded.
func (s *Store) DiscardUpload(ctx context.Context, version, workflowID string) error {
	if strings.TrimSpace(version) == "" {
		return fmt.Errorf("%w: version required", ErrInvalidMetadata)
	}
	rec, err := s.meta.findByVersion(ctx, version)
	switch {
	case errors.Is(err, ErrNotFound):
		// The upload's record is already gone; its object may still be there.
	case err != nil:
		return fmt.Errorf("discard upload %s: %w", version, err)
	case rec.UploadWorkflowID != workflowID || stateRank(rec.State) >= stateRank(UploadAvailable):
		return fmt.Errorf("discard upload %s: %w: %s", version, ErrVersionConflict, version)
	default:
		transitions, err := s.transitions()
		if err != nil {
			return err
		}
		if err := transitions.removeClaim(ctx, version, workflowID); err != nil {
			return fmt.Errorf("discard upload %s: %w", version, err)
		}
	}
	if err := s.binaries.remove(ctx, binaryRef(version)); err != nil {
		return fmt.Errorf("discard upload %s binary: %w", version, err)
	}
	return nil
}

// UploadState reports the state the record carrying version holds: the upload's state, whether
// it concluded, and the firmware facts known so far. A version the registry does not hold fails
// with an error wrapping ErrNotFound, and a record written before uploads carried a state
// reports UploadUnknown rather than a failure.
func (s *Store) UploadState(ctx context.Context, version string) (UploadState, error) {
	rec, err := s.MetadataByVersion(ctx, version)
	if err != nil {
		return UploadState{}, err
	}
	state := rec.State
	if state == "" {
		state = UploadUnknown
	}
	return UploadState{
		Version:    rec.Version,
		State:      state,
		Concluded:  state == UploadAvailable || state == UploadFailed,
		FirmwareID: rec.ID,
		Models:     rec.Models,
		Checksum:   rec.Checksum,
		Size:       rec.Size,
		Reason:     rec.FailureReason,
	}, nil
}

// transitions returns the store's metadata collection as the seam that records an upload's state
// transitions, reporting a metadata implementation that cannot record them rather than dropping
// the upload's state silently.
func (s *Store) transitions() (metadataTransitions, error) {
	transitions, ok := s.meta.(metadataTransitions)
	if !ok {
		return nil, fmt.Errorf("record firmware upload: %T cannot record upload transitions", s.meta)
	}
	return transitions, nil
}

// binaryRef returns the GridFS address of the binary of version. It is derived from the version
// so that storage is addressed by the version that owns it: a retried step recomputes the same
// address and replaces one object instead of leaving a second.
func binaryRef(version string) string {
	sum := sha256.Sum256([]byte(version))
	return "fwbin-" + hex.EncodeToString(sum[:8])
}

// stateRank orders the upload states so that a write never moves a record backwards: accepted,
// then storing, then a terminal state. An unrecognized state ranks below every transition, so a
// record carrying one is never advanced by an upload.
func stateRank(state string) int {
	switch state {
	case UploadAccepted:
		return 1
	case UploadStoring:
		return 2
	case UploadAvailable, UploadFailed:
		return 3
	default:
		return 0
	}
}
