package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DisMosGit/fleetops/internal/telemetry"
)

// AnnounceRollbackRequest is the announce-rollback activity's request: which phase announces
// itself, the rollout it belongs to, why the rollback happened, and what it has achieved.
type AnnounceRollbackRequest struct {
	// Phase is the phase to announce: started or completed.
	Phase telemetry.RollbackPhase `json:"phase"`
	// RolloutID is the rollout that is rolling back.
	RolloutID string `json:"rollout_id"`
	// FirmwareID and FirmwareVersion are the firmware the rollout deployed.
	FirmwareID      string `json:"firmware_id"`
	FirmwareVersion string `json:"firmware_version,omitempty"`
	// Region and Model are the target selector the rollout drew its devices from.
	Region string `json:"region"`
	Model  string `json:"model"`
	// WaveID is the wave whose failure ended the rollout.
	WaveID string `json:"wave_id,omitempty"`
	// Outcome is the outcome that ended the rollout.
	Outcome RolloutOutcome `json:"outcome"`
	// Decision is the failing wave's measured health; nil when the wave ended the rollout before
	// it could be measured.
	Decision *WaveHealth `json:"decision,omitempty"`
	// PlanSteps and PlanDevices are how many steps and how many devices the rollback compensates.
	PlanSteps   int `json:"plan_steps"`
	PlanDevices int `json:"plan_devices"`
	// Rollback is the rollback as the state query reports it: what each step achieved, the
	// reconciled inventory, and the devices that could not be restored. It is set on the
	// completed phase, which is the announcement that reports achievements.
	Rollback *RollbackView `json:"rollback,omitempty"`
	// OccurredAt is the workflow's clock reading at the moment of the announcement: a value
	// recorded in history, so a replay announces the same moment and a retry repeats it.
	OccurredAt time.Time `json:"occurred_at"`
}

// NewAnnounceRollbackActivity returns the announce-rollback activity bound to notifier: it builds
// the rollback event for its phase from the request and publishes it, waiting for the broker's
// confirmation before returning. The step that publishes is recorded as complete only once the
// broker has taken the event, so a rollback is never recorded as announced when it was not.
func NewAnnounceRollbackActivity(notifier RollbackNotifier) func(ctx context.Context, req AnnounceRollbackRequest) error {
	return func(ctx context.Context, req AnnounceRollbackRequest) error {
		event, err := rollbackEvent(req)
		if err != nil {
			return err
		}
		if err := notifier.Announce(ctx, event); err != nil {
			return fmt.Errorf("announce %s of rollout %s: %w", req.Phase, req.RolloutID, err)
		}
		return nil
	}
}

// rollbackEvent maps one announcement request onto the event published under the notification key
// space. Its identity is derived from the rollout and the phase, so a retried publication is the
// same event rather than a second one.
func rollbackEvent(req AnnounceRollbackRequest) (telemetry.RollbackEvent, error) {
	if req.RolloutID == "" || req.Phase == "" {
		return telemetry.RollbackEvent{}, errors.New("announce rollback: rollout id and phase required")
	}
	event := telemetry.NewRollbackEvent(req.RolloutID, req.Phase, req.OccurredAt)
	event.Rollout.FirmwareID = req.FirmwareID
	event.Rollout.FirmwareVersion = req.FirmwareVersion
	event.Rollout.Region = req.Region
	event.Rollout.Model = req.Model
	event.Rollout.WaveID = req.WaveID
	event.Rollout.Outcome = string(req.Outcome)
	event.Rollout.PlanSteps = req.PlanSteps
	event.Rollout.PlanDevices = req.PlanDevices
	if req.Decision != nil {
		event.Rollout.Decision = &telemetry.RollbackDecision{
			Verdict:      req.Decision.Verdict.String(),
			SuccessRatio: req.Decision.SuccessRatio,
			SampleSize:   req.Decision.SampleSize,
			WindowStart:  req.Decision.WindowStart,
			WindowEnd:    req.Decision.WindowEnd,
		}
	}
	event.Progress = rollbackAnnouncementProgress(req.Rollback, req.Phase)
	return event, nil
}

// nonNilIDs returns ids as an empty slice when it is nil, so an array field is announced and
// recorded as an empty array rather than null.
func nonNilIDs(ids []string) []string {
	if ids == nil {
		return []string{}
	}
	return ids
}

// rollbackAnnouncementProgress maps the rollback's recorded achievements onto the announcement's
// progress: the totals a dashboard or an alert reacts to, the reconciled inventory, the devices
// that could not be restored, and each step's own outcome. It is nil on the started phase, which
// precedes every compensation and therefore reports nothing achieved.
func rollbackAnnouncementProgress(view *RollbackView, phase telemetry.RollbackPhase) *telemetry.RollbackProgress {
	if phase != telemetry.RollbackCompleted || view == nil {
		return nil
	}
	progress := &telemetry.RollbackProgress{
		Inventory:           make([]telemetry.FirmwareInventoryEntry, 0, len(view.Inventory)),
		UnrestoredDeviceIDs: nonNilIDs(view.UnrestoredDeviceIDs),
		Steps:               make([]telemetry.RollbackStepOutcome, 0, len(view.Plan)),
	}
	for _, count := range view.Inventory {
		progress.Inventory = append(progress.Inventory, telemetry.FirmwareInventoryEntry{
			Version: count.Version,
			Devices: count.Devices,
		})
	}
	for _, step := range view.Plan {
		progress.Restored += step.Restored
		progress.Failed += step.Failed
		progress.Unreported += step.Unreported
		progress.Skipped += step.Skipped
		progress.Unavailable += step.Unavailable
		progress.Agreed += step.Agreed
		progress.Corrected += step.Corrected
		progress.Unverified += step.Unverified
		progress.Steps = append(progress.Steps, telemetry.RollbackStepOutcome{
			Kind:        string(step.Kind),
			WaveID:      step.WaveID,
			Status:      string(step.Status),
			Devices:     step.Devices,
			Restored:    step.Restored,
			Failed:      step.Failed,
			Unreported:  step.Unreported,
			Skipped:     step.Skipped,
			Unavailable: step.Unavailable,
			Agreed:      step.Agreed,
			Corrected:   step.Corrected,
			Unverified:  step.Unverified,
			Detail:      step.Detail,
		})
	}
	return progress
}
