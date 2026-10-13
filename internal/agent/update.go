package agent

import (
	"context"
	"errors"
	"log/slog"
	"time"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// FirmwareFetcher fetches one firmware binary to disk and returns the path of the verified
// file. *Downloader satisfies it.
type FirmwareFetcher interface {
	// Fetch downloads one firmware binary, reporting completion percentages through
	// onProgress as the transfer advances.
	Fetch(ctx context.Context, deviceID, firmwareID, expectedChecksum string, onProgress func(int32)) (string, error)
}

// FirmwareApply is the firmware application step the update handler drives. *Applier
// satisfies it.
type FirmwareApply interface {
	// Apply applies one downloaded firmware to one device.
	Apply(ctx context.Context, deviceID, version string) error
}

// FirmwareAdopter records the firmware version a device runs after a successful apply.
// *Fleet satisfies it.
type FirmwareAdopter interface {
	// SetFirmware adopts version as the named device's current firmware.
	SetFirmware(deviceID, version string) error
}

// RetryInitial is the wait before the first firmware-download retry; the wait doubles from
// there, capped by the retry policy's Max.
const RetryInitial = 100 * time.Millisecond

// defaultRetryAttempts and defaultRetryMax bound the retries of a handler built without an
// explicit policy.
const (
	defaultRetryAttempts = 3
	defaultRetryMax      = 5 * time.Second
)

// FetchRetry bounds the retry of a failed firmware fetch: how many attempts one command may
// make in total, and the cap on the wait between them. Its zero value makes a single attempt,
// so a handler left at the zero value never retries.
type FetchRetry struct {
	// Attempts is the total number of fetch attempts one command may make; below one means one.
	Attempts int
	// Max caps the wait between two attempts; zero makes the wait immediate.
	Max time.Duration
}

// defaultFetchRetry returns the retry policy of a handler built without an explicit one.
func defaultFetchRetry() FetchRetry {
	return FetchRetry{Attempts: defaultRetryAttempts, Max: defaultRetryMax}
}

// UpdateHandler executes the commands the control plane dispatches to the devices behind one
// agent. A StartUpdate is one command with one terminal result: it downloads the firmware,
// retrying a transient download failure within its attempt budget, verifies it, applies it
// through the apply step, adopts the new version on success, and reports every phase —
// downloading, applying, completed or failed — plus exactly one terminal result, whichever
// step concludes it.
type UpdateHandler struct {
	fetcher  FirmwareFetcher
	applier  FirmwareApply
	adopt    FirmwareAdopter
	reporter *UpdateReporter
	retry    FetchRetry
	log      *slog.Logger
	sleep    func(ctx context.Context, d time.Duration) error
}

// NewUpdateHandler returns the command handler driving one update cycle per StartUpdate,
// reporting through reporter and retrying a failed fetch under the package's default budget.
func NewUpdateHandler(
	fetcher FirmwareFetcher,
	applier FirmwareApply,
	adopt FirmwareAdopter,
	reporter *UpdateReporter,
	log *slog.Logger,
) *UpdateHandler {
	return NewUpdateHandlerWithRetry(fetcher, applier, adopt, reporter, defaultFetchRetry(), log)
}

// NewUpdateHandlerWithRetry returns the command handler driving one update cycle per
// StartUpdate, reporting through reporter and retrying a failed fetch under retry.
func NewUpdateHandlerWithRetry(
	fetcher FirmwareFetcher,
	applier FirmwareApply,
	adopt FirmwareAdopter,
	reporter *UpdateReporter,
	retry FetchRetry,
	log *slog.Logger,
) *UpdateHandler {
	return &UpdateHandler{
		fetcher:  fetcher,
		applier:  applier,
		adopt:    adopt,
		reporter: reporter,
		retry:    retry,
		log:      log,
		sleep:    sleepCtx,
	}
}

// Handle executes one dispatched command. A failed update is an outcome, not a handler fault:
// it is reported and Handle returns nil, and only a failure to report itself is returned as
// an error.
func (h *UpdateHandler) Handle(ctx context.Context, cmd *agentv1.Command) error {
	if start := cmd.GetStartUpdate(); start != nil {
		return h.handleUpdate(ctx, cmd, start)
	}
	if abort := cmd.GetAbortUpdate(); abort != nil {
		// Abort cancellation semantics arrive with the rollout stage; until then the
		// command is acknowledged at the log boundary and changes nothing.
		h.log.Info("abort update not acted on",
			"command_id", cmd.GetCommandId(), "device_id", cmd.GetDeviceId(),
			"reason", abort.GetReason())
		return nil
	}
	h.log.Warn("command without a kind",
		"command_id", cmd.GetCommandId(), "device_id", cmd.GetDeviceId())
	return nil
}

// handleUpdate runs one download-verify-apply cycle and reports it. Its failure path reports
// the failed phase and the terminal failed result under the same operator-safe detail; when
// the context is already gone, nothing more can be reported and the failure is all that is
// left to return.
func (h *UpdateHandler) handleUpdate(
	ctx context.Context,
	cmd *agentv1.Command,
	start *agentv1.StartUpdate,
) error {
	deviceID, firmwareID := cmd.GetDeviceId(), start.GetFirmwareId()

	fail := func(cause error, detail string) error {
		h.log.Warn("firmware update failed",
			"command_id", cmd.GetCommandId(), "device_id", deviceID,
			"firmware_id", firmwareID, "err", cause)
		if ctx.Err() != nil {
			return cause
		}
		if err := h.reporter.Status(ctx, deviceID, firmwareID,
			agentv1.UpdatePhase_UPDATE_PHASE_FAILED, 0, detail); err != nil {
			return err
		}
		return h.reporter.Conclude(ctx, cmd, agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED, detail)
	}

	if err := h.reporter.Status(ctx, deviceID, firmwareID,
		agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, 0, ""); err != nil {
		return err
	}
	path, err := h.fetchWithRetry(ctx, deviceID, firmwareID, start.GetChecksum(), func(percent int32) {
		// Progress is frequent and fine-grained: a report the control plane misses is
		// logged and superseded by the next one, never allowed to fail the update.
		if err := h.reporter.Status(ctx, deviceID, firmwareID,
			agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, percent, ""); err != nil {
			h.log.Warn("report download progress",
				"command_id", cmd.GetCommandId(), "device_id", deviceID, "err", err)
		}
	})
	if err != nil {
		return fail(err, downloadDetail(err))
	}
	h.log.Info("firmware downloaded",
		"command_id", cmd.GetCommandId(), "device_id", deviceID,
		"firmware_id", firmwareID, "path", path)

	if err := h.reporter.Status(ctx, deviceID, firmwareID,
		agentv1.UpdatePhase_UPDATE_PHASE_APPLYING, 0, ""); err != nil {
		return err
	}
	if err := h.applier.Apply(ctx, deviceID, start.GetVersion()); err != nil {
		return fail(err, applyDetail(err))
	}
	if err := h.adopt.SetFirmware(deviceID, start.GetVersion()); err != nil {
		return fail(err, "device did not adopt the firmware")
	}

	if err := h.reporter.Status(ctx, deviceID, firmwareID,
		agentv1.UpdatePhase_UPDATE_PHASE_COMPLETED, 100, ""); err != nil {
		return err
	}
	return h.reporter.Conclude(ctx, cmd, agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, "")
}

// fetchWithRetry fetches one firmware binary under the handler's attempt budget. A retry
// announces itself with the downloading phase again — the control plane saw the failed attempt
// — after waiting a growing capped delay, retries only a failure a retry can clear, and returns
// the context's error the moment the command's context ends. The failed phase and the terminal
// result stay in the caller, so a retried download still produces exactly one of each.
func (h *UpdateHandler) fetchWithRetry(
	ctx context.Context,
	deviceID, firmwareID, checksum string,
	onProgress func(int32),
) (string, error) {
	attempts := h.retry.Attempts
	if attempts < 1 {
		attempts = 1
	}
	// The delays are deterministic: an update retry needs no jitter, so the capped-backoff
	// sequence runs without a random source.
	delays := newBackoff(RetryInitial, h.retry.Max, 0, nil)

	var last error
	for attempt := range attempts {
		if attempt > 0 {
			delay := delays.Delay()
			h.log.Warn("retrying firmware download",
				"device_id", deviceID, "firmware_id", firmwareID,
				"attempt", attempt+1, "delay", delay, "err", last)
			if err := h.sleep(ctx, delay); err != nil {
				return "", err
			}
			if err := h.reporter.Status(ctx, deviceID, firmwareID,
				agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, 0, ""); err != nil {
				return "", err
			}
		}
		path, err := h.fetcher.Fetch(ctx, deviceID, firmwareID, checksum, onProgress)
		if err == nil {
			return path, nil
		}
		last = err
		if !retryableFetchError(err) {
			break
		}
	}
	return "", last
}

// downloadDetail maps a download failure onto its operator-safe report detail: the failure
// class names the reason, and anything else reports as the generic download failure.
func downloadDetail(err error) string {
	switch {
	case errors.Is(err, ErrChecksumMismatch):
		return ErrChecksumMismatch.Error()
	case errors.Is(err, ErrTruncatedTransfer):
		return ErrTruncatedTransfer.Error()
	case errors.Is(err, ErrStall):
		return ErrStall.Error()
	default:
		return "download failed"
	}
}

// applyDetail maps an apply failure onto its operator-safe report detail.
func applyDetail(err error) string {
	if errors.Is(err, ErrApplyFailed) {
		return ErrApplyFailed.Error()
	}
	return "firmware apply failed"
}
