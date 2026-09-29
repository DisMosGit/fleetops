package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// DeviceRestoreOutcome names how one device's restore ended, as the rollback step that dispatched
// it needs to know: the device is back on the version it ran before, it reported a failure, it
// never reported, it had nothing to restore, or the restore could not be attempted at all.
type DeviceRestoreOutcome string

// The outcomes one device's downgrade activity reports. They are total: every device a step
// targeted is reported as exactly one of them, so nothing is ever recorded as achieved that was
// not observed.
const (
	// RestoreRestored means the device reported the restore command concluded successfully.
	RestoreRestored DeviceRestoreOutcome = "restored"
	// RestoreFailed means the device reported the restore command concluded unsuccessfully.
	RestoreFailed DeviceRestoreOutcome = "failed"
	// RestoreUnreported means the device never concluded the restore command before the deadline.
	// A command superseded before it concluded is unreported too: the device never said what
	// happened, and the rollback is told exactly that.
	RestoreUnreported DeviceRestoreOutcome = "unreported"
	// RestoreSkipped means the device does not run the firmware being rolled back, so it has
	// nothing to restore — it never took the firmware, or an earlier attempt already restored it.
	RestoreSkipped DeviceRestoreOutcome = "skipped"
	// RestoreUnavailable means the restore could not be attempted or its outcome could not be
	// determined: the device records no previous version, that version is unknown to the
	// registry or does not target the device's model, or the command could not be delivered.
	RestoreUnavailable DeviceRestoreOutcome = "unavailable"
)

// DeviceRestore is one device's restore as its activity reported it back to the rollback step.
type DeviceRestore struct {
	// DeviceID is the device the restore was attempted for.
	DeviceID string `json:"device_id"`
	// Outcome is how the restore ended.
	Outcome DeviceRestoreOutcome `json:"outcome"`
	// Detail is why: the device's own failure detail, or the reason the restore was skipped or
	// could not be attempted.
	Detail string `json:"detail,omitempty"`
	// FoundFw is the firmware version the device's workflow reported when the activity read it.
	FoundFw string `json:"found_fw,omitempty"`
}

// DowngradeDeviceRequest is the downgrade-device activity's request: one device to restore, the
// wave whose update it compensates, the firmware the device is being rolled back from, and the
// deadline by which a reported result is still useful.
//
// The deadline is an absolute moment the workflow computed from its own clock, so a replay, a
// retry, or a restart recomputes the same one and a worker outage cannot extend the wait.
type DowngradeDeviceRequest struct {
	// RolloutID is the rollout that is rolling back; the restore's command id derives from it.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave whose update this restore compensates.
	WaveID string `json:"wave_id"`
	// DeviceID is the device to restore.
	DeviceID string `json:"device_id"`
	// DeployedFw is the firmware version the rollout deployed — the one a device has to be
	// running for a restore to be needed at all.
	DeployedFw string `json:"deployed_fw"`
	// Deadline is the moment after which a result is no longer useful: the wait stops there and
	// the device counts as unreported.
	Deadline time.Time `json:"deadline"`
}

// rollbackCommandID returns the id of the restore command one rollback issues to one device. A
// rollout rolls back once, so the rollout and the device identify the restore, and every attempt of
// one device's restore repeats the id: a device that already accepted it treats the redelivery as
// the same command rather than as a second one.
//
// It is deliberately not CommandID(waveID, deviceID): the two commands mean different things, and a
// device would otherwise treat the restore as a redelivery of the update it already concluded.
func rollbackCommandID(rolloutID, deviceID string) string {
	return "rollback-" + rolloutID + "-" + deviceID
}

// deviceRestorer restores one device to the firmware it ran before, over the device command seam
// and the device-state reader a wave's update activity already uses.
type deviceRestorer struct {
	// commander delivers the restore command to the device's workflow.
	commander DeviceCommander
	// states reads the device's authoritative state, both to decide whether a restore is needed
	// and to learn its result while the restore waits.
	states DeviceStateReader
	// versions resolves the device's previous firmware version in the registry.
	versions FirmwareVersions
	// poll is how long the wait is between state observations.
	poll time.Duration
}

// DowngradeDeviceOption adjusts one optional behaviour of the downgrade-device activity. It exists
// for the test suites that drive the real activity: production always takes the defaults, and
// nothing configures the activity through the configuration file.
type DowngradeDeviceOption func(*deviceRestorer)

// WithDowngradePollInterval sets how long the wait is between the activity's state observations.
// Tests use it so a device that reports on a later observation does not spend the production
// interval waiting; the deadline still bounds the wait whatever the interval.
func WithDowngradePollInterval(poll time.Duration) DowngradeDeviceOption {
	return func(r *deviceRestorer) {
		if poll > 0 {
			r.poll = poll
		}
	}
}

// NewDowngradeDeviceActivity returns the downgrade-device activity bound to the device command
// seam, the device-state reader, and the firmware registry: it restores one device to the firmware
// version its own workflow records as the one it ran before, delivering an ordinary update command
// for that version under the rollback's command id and waiting for the device to report the
// command's outcome, bounded by the request's deadline.
//
// The activity reports one outcome per device and never fails the step on a device's account: a
// device that does not run the firmware being rolled back is skipped, a device whose restore cannot
// be attempted or delivered is unavailable with the reason, and a device that never reports is
// unreported at the deadline. Only a failure of the activity's own reads — the device's state, the
// registry lookup — is an error, and that is left to the caller's retry policy rather than recorded
// as a device outcome.
func NewDowngradeDeviceActivity(
	commander DeviceCommander,
	states DeviceStateReader,
	versions FirmwareVersions,
	options ...DowngradeDeviceOption,
) func(ctx context.Context, req DowngradeDeviceRequest) (DeviceRestore, error) {
	restorer := deviceRestorer{
		commander: commander,
		states:    states,
		versions:  versions,
		poll:      updatePollInterval,
	}
	for _, option := range options {
		option(&restorer)
	}
	return restorer.restore
}

// restore restores one device to the firmware it ran before the deployed one.
func (r deviceRestorer) restore(ctx context.Context, req DowngradeDeviceRequest) (DeviceRestore, error) {
	if req.RolloutID == "" || req.DeviceID == "" {
		return DeviceRestore{}, errors.New("downgrade device: rollout id and device id required")
	}
	state, err := r.states.State(ctx, req.DeviceID)
	if err != nil {
		return DeviceRestore{}, fmt.Errorf("read device %s state: %w", req.DeviceID, err)
	}
	if state.CurrentFw != req.DeployedFw {
		// The device does not run the firmware being rolled back. Either it never took it or an
		// earlier attempt already restored it: both mean there is nothing to do, and the guard
		// is what makes a retried restore a no-op rather than a second command.
		return DeviceRestore{
			DeviceID: req.DeviceID,
			Outcome:  RestoreSkipped,
			Detail: fmt.Sprintf("device runs %s, not the rolled-back firmware %s",
				state.CurrentFw, req.DeployedFw),
			FoundFw: state.CurrentFw,
		}, nil
	}

	target, reason, err := r.target(ctx, state)
	if err != nil {
		return DeviceRestore{}, err
	}
	if reason != "" {
		return DeviceRestore{
			DeviceID: req.DeviceID,
			Outcome:  RestoreUnavailable,
			Detail:   reason,
			FoundFw:  state.CurrentFw,
		}, nil
	}

	commandID := rollbackCommandID(req.RolloutID, req.DeviceID)
	if err := r.commander.SignalCommandIssued(ctx, CommandIssuedSignal{
		CommandID:  commandID,
		DeviceID:   req.DeviceID,
		Kind:       CommandKindUpdate,
		FirmwareID: target.ID,
		Version:    target.Version,
		Checksum:   target.Checksum,
	}); err != nil {
		// A restore that cannot be delivered is a fact about this device, not a failure of the
		// step: it is recorded as unrestored with the delivery failure as its reason, and every
		// other device of the step is still restored.
		return DeviceRestore{
			DeviceID: req.DeviceID,
			Outcome:  RestoreUnavailable,
			Detail:   fmt.Sprintf("deliver restore command: %v", err),
			FoundFw:  state.CurrentFw,
		}, nil
	}
	return r.awaitRestore(ctx, req, commandID, state.CurrentFw)
}

// target resolves what a device is restored to: the firmware version the device's own workflow
// records as the one it ran before, resolved in the registry and checked against the device's
// model. It reports the reason no restore can be attempted — what the step records as unavailable —
// or an error when the lookup itself failed and the activity's retries should try again.
//
// A device that has never reported its model has nothing to check the firmware's targets against,
// so the check is skipped rather than turned into a refusal: an unknown model is not evidence that
// the firmware is wrong for the device, and refusing would leave the device on the bad firmware for
// a reason that is not about the firmware.
func (r deviceRestorer) target(ctx context.Context, state State) (firmware.Record, string, error) {
	if state.PreviousFw == "" {
		return firmware.Record{},
			fmt.Sprintf("device %s records no previous firmware version", state.DeviceID), nil
	}
	rec, err := r.versions.MetadataByVersion(ctx, state.PreviousFw)
	if errors.Is(err, firmware.ErrNotFound) {
		return firmware.Record{},
			fmt.Sprintf("firmware version %s is unknown to the registry", state.PreviousFw), nil
	}
	if err != nil {
		return firmware.Record{}, "", fmt.Errorf("resolve firmware version %s: %w", state.PreviousFw, err)
	}
	if state.Model != "" && !slices.Contains(rec.Models, state.Model) {
		return firmware.Record{},
			fmt.Sprintf("firmware version %s does not target model %s", rec.Version, state.Model), nil
	}
	return rec, "", nil
}

// awaitRestore waits for one device to conclude the restore command this activity delivered,
// observing the device's state at the poll interval until the command concludes or the deadline
// passes. Every observation failure is treated as "not reported yet": a read that failed is not
// evidence about the device, so the wait only ends on a recorded outcome or at the deadline.
func (r deviceRestorer) awaitRestore(
	ctx context.Context,
	req DowngradeDeviceRequest,
	commandID, foundFw string,
) (DeviceRestore, error) {
	// A deadline that has already passed leaves nothing to wait for, and the command's fate is
	// still worth one read: the device may have concluded it before this attempt started.
	for {
		if concluded, ok := observeCommand(ctx, r.states, req.DeviceID, commandID); ok {
			restore := DeviceRestore{DeviceID: req.DeviceID, FoundFw: foundFw}
			if concluded.Outcome == OutcomeSucceeded {
				restore.Outcome = RestoreRestored
				return restore, nil
			}
			restore.Outcome = RestoreFailed
			restore.Detail = concluded.Detail
			return restore, nil
		}
		if !time.Now().Before(req.Deadline) {
			return DeviceRestore{
				DeviceID: req.DeviceID,
				Outcome:  RestoreUnreported,
				FoundFw:  foundFw,
			}, nil
		}
		// The heartbeat tells Temporal the wait is alive rather than stuck, so a lost attempt is
		// retried promptly instead of waiting out its heartbeat timeout. Outside an activity
		// there is no heartbeat to record, which is only the case in unit tests.
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, req.DeviceID)
		}

		// Never sleep past the deadline: the last wait is clipped so the activity stops when the
		// step stops waiting.
		wait := r.poll
		if remaining := time.Until(req.Deadline); remaining < wait {
			wait = remaining
		}
		if err := sleep(ctx, wait); err != nil {
			return DeviceRestore{}, err
		}
	}
}
