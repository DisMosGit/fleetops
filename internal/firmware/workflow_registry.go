package firmware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// UploadRequest is one upload the endpoint has accepted: the version it stores, the models it
// targets, and the path of the spooled binary the storage step streams from. The path travels
// rather than the bytes because an activity's input is serialized and a binary must not be held
// in an activity payload.
type UploadRequest struct {
	// Version is the firmware version the upload stores.
	Version string `json:"version"`
	// Models are the target device models the upload declared.
	Models []string `json:"models"`
	// SpoolPath is the path of the file the request body was spooled to.
	SpoolPath string `json:"spool_path"`
}

// UploadRun is a handle on the workflow execution that owns one upload.
type UploadRun interface {
	// Attached reports whether the execution was already running when this upload reached it.
	// An attached upload is another request's: its outcome is that request's to report, and
	// this request stored nothing.
	Attached() bool
	// Await waits for the upload to conclude and returns the state it concluded with.
	Await(ctx context.Context) (UploadState, error)
}

// UploadStarter begins the workflow that owns an upload, or attaches to the execution already
// running it. *temporal.FirmwareUploadStarter satisfies it.
type UploadStarter interface {
	// StartOrAttach starts the upload's execution under its version-derived identity, or
	// attaches to the execution that already owns the version.
	StartOrAttach(ctx context.Context, req UploadRequest) (UploadRun, error)
}

// UploadStates reads one upload's state without owning its execution: the state a running upload
// reports, or the outcome a concluded one recorded. *temporal.FirmwareUploadStates satisfies it.
type UploadStates interface {
	// State returns the state of the upload of version, an upload with no execution wrapping
	// ErrNotFound.
	State(ctx context.Context, version string) (UploadState, error)
}

// WorkflowRegistry records uploads through the workflow that owns them: it spools the request's
// binary to the configured directory, starts or attaches to the version's execution, waits for
// its outcome, and removes the spool file once the upload concluded. It is the Registry the
// control plane wires, so the upload endpoint keeps its contract while the upload's steps run as
// activities of one durable execution.
type WorkflowRegistry struct {
	starter UploadStarter
	states  UploadStates
	spool   *Spool
	wait    time.Duration
	log     *slog.Logger
}

// NewWorkflowRegistry returns the upload registry over the workflow backend, spooling request
// bodies in spool and waiting at most wait for an upload to conclude.
func NewWorkflowRegistry(
	starter UploadStarter,
	states UploadStates,
	spool *Spool,
	wait time.Duration,
	log *slog.Logger,
) *WorkflowRegistry {
	return &WorkflowRegistry{starter: starter, states: states, spool: spool, wait: wait, log: log}
}

// Save spools the request's binary and drives the upload's workflow to its outcome: the record it
// stored, or an error wrapping the sentinel the endpoint maps onto its status codes. The spool
// file is removed once the upload concluded, and an upload that outlived the wait keeps it — the
// workflow is still storing from it — and is reported as ErrUploadContinues.
func (r *WorkflowRegistry) Save(
	ctx context.Context,
	version string,
	models []string,
	binary io.Reader,
) (Record, error) {
	path, err := r.spool.Write(binary)
	if err != nil {
		return Record{}, fmt.Errorf("spool firmware upload %s: %w", version, err)
	}
	rec, err := r.drive(ctx, UploadRequest{Version: version, Models: models, SpoolPath: path})
	if errors.Is(err, ErrUploadContinues) {
		return Record{}, err
	}
	if rmErr := r.spool.Remove(path); rmErr != nil {
		return Record{}, errors.Join(err, rmErr)
	}
	return rec, err
}

// UploadState reports the state of one version's upload.
func (r *WorkflowRegistry) UploadState(ctx context.Context, version string) (UploadState, error) {
	return r.states.State(ctx, version)
}

// drive starts or attaches to the upload's execution and reports what the upload concluded with.
func (r *WorkflowRegistry) drive(ctx context.Context, req UploadRequest) (Record, error) {
	run, err := r.starter.StartOrAttach(ctx, req)
	if err != nil {
		return Record{}, err
	}
	if run.Attached() {
		// Another request's upload owns the version: it is the one storing and recording, so
		// this request's binary was never stored and the version is the conflict it is.
		return Record{}, fmt.Errorf("save firmware %s: %w: an upload of that version is in flight",
			req.Version, ErrVersionConflict)
	}

	waitCtx, cancel := context.WithTimeout(ctx, r.wait)
	defer cancel()
	state, err := run.Await(waitCtx)
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		// The wait is what ended, not the upload: the execution continues durably, so its
		// spool file stays for the storage step that may still read it.
		r.log.Info("firmware upload continues after the wait", "version", req.Version, "err", err)
		return Record{}, fmt.Errorf("%w: wait for upload %s: %w", ErrUploadContinues, req.Version, err)
	case err != nil:
		return Record{}, fmt.Errorf("wait for upload %s: %w", req.Version, err)
	}
	return outcome(req.Version, state)
}

// outcome maps a concluded upload's state onto the registry's answer: the record it stored, or an
// error wrapping the sentinel the endpoint maps onto its status codes.
func outcome(version string, state UploadState) (Record, error) {
	if state.State == UploadAvailable {
		return Record{
			ID:       state.FirmwareID,
			Version:  state.Version,
			Models:   state.Models,
			Checksum: state.Checksum,
			Size:     state.Size,
		}, nil
	}
	switch state.Failure {
	case FailureRefused:
		return Record{}, fmt.Errorf("save firmware %s: %w: %s", version, ErrIncompatible, state.Reason)
	case FailureConflict:
		return Record{}, fmt.Errorf("save firmware %s: %w: %s", version, ErrVersionConflict, state.Reason)
	default:
		return Record{}, fmt.Errorf("save firmware %s: upload concluded %s: %s",
			version, state.State, state.Reason)
	}
}
