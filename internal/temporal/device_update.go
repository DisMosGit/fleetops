package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

// updatePollInterval is how often a waiting update activity re-reads the device's state. It
// bounds how late a device's already-reported result is noticed, not how long the wait lasts:
// the wait ends at the request's deadline whatever the interval.
const updatePollInterval = 2 * time.Second

// DeviceUpdateOutcome names how one target device's update ended, as the wave that dispatched it
// needs to know: it applied the firmware, it reported a failure, or it never reported before the
// wave stopped waiting.
type DeviceUpdateOutcome string

// The outcomes one device's update activity reports.
const (
	// UpdateSucceeded means the device reported the update command concluded successfully.
	UpdateSucceeded DeviceUpdateOutcome = "succeeded"
	// UpdateFailed means the device reported the update command concluded unsuccessfully.
	UpdateFailed DeviceUpdateOutcome = "failed"
	// UpdateUnreported means the device never concluded the command before the deadline. A
	// command that was superseded before it concluded is unreported too: the device never
	// said what happened, and a wave is told exactly that.
	UpdateUnreported DeviceUpdateOutcome = "unreported"
)

// DeviceUpdate is one device's update as its activity reported it back to the wave.
type DeviceUpdate struct {
	// DeviceID is the device the update commanded.
	DeviceID string `json:"device_id"`
	// Outcome is how the update ended.
	Outcome DeviceUpdateOutcome `json:"outcome"`
	// Detail is the device's own failure detail; set for UpdateFailed only.
	Detail string `json:"detail,omitempty"`
}

// UpdateDeviceRequest is the update-device activity's request: one device to command, the
// firmware to deploy, and the deadline by which a reported result is still useful. The deadline
// is an absolute moment the workflow computed from its own clock, so a replay, a retry, or a
// restart recomputes the same one and a worker outage cannot extend the wait.
type UpdateDeviceRequest struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave commanding the device; the command id derives from it.
	WaveID string `json:"wave_id"`
	// DeviceID is the device to command.
	DeviceID string `json:"device_id"`
	// Firmware is the firmware the command deploys.
	Firmware Firmware `json:"firmware"`
	// Deadline is the moment after which a result is no longer useful: the wait stops there
	// and the device counts as unreported.
	Deadline time.Time `json:"deadline"`
}

// deviceUpdater commands one device and waits for its reported result.
type deviceUpdater struct {
	// commander delivers the update command to the device's workflow.
	commander DeviceCommander
	// states reads the device's authoritative state while the update waits.
	states DeviceStateReader
	// poll is how long the wait is between state observations.
	poll time.Duration
}

// UpdateDeviceOption adjusts one optional behaviour of the update-device activity. It exists for
// the test suites that drive the real activity: production always takes the defaults, and nothing
// configures the activity through the configuration file.
type UpdateDeviceOption func(*deviceUpdater)

// WithUpdatePollInterval sets how long the wait is between the activity's state observations.
// Tests use it so a device that reports on a later observation does not spend the production
// interval waiting; the deadline still bounds the wait whatever the interval.
func WithUpdatePollInterval(poll time.Duration) UpdateDeviceOption {
	return func(u *deviceUpdater) {
		if poll > 0 {
			u.poll = poll
		}
	}
}

// NewUpdateDeviceActivity returns the update-device activity bound to the device command seam and
// the device-state reader: it delivers one update command to one device's workflow — under the
// command id derived from the wave and the device, so a retried or duplicated delivery is a no-op
// for a device that already accepted it — and then waits for that device to report the command's
// outcome, bounded by the request's deadline.
//
// An attempt that dies while it waits is retried by the caller's retry policy, and the retry
// re-reads the device's recorded state rather than depending on live observation, so a result
// reported before the retry is still found and the device is never commanded twice.
//
// A command that cannot be delivered at all is an error rather than an outcome: a wave the
// rollout could not command is a fact about the wave, and the caller fails it on that basis.
func NewUpdateDeviceActivity(
	commander DeviceCommander,
	states DeviceStateReader,
	options ...UpdateDeviceOption,
) func(ctx context.Context, req UpdateDeviceRequest) (DeviceUpdate, error) {
	updater := deviceUpdater{commander: commander, states: states, poll: updatePollInterval}
	for _, option := range options {
		option(&updater)
	}
	return updater.update
}

// update delivers one device's update command and waits for its reported result.
func (u deviceUpdater) update(ctx context.Context, req UpdateDeviceRequest) (DeviceUpdate, error) {
	commandID := CommandID(req.WaveID, req.DeviceID)
	if err := u.commander.SignalCommandIssued(ctx, CommandIssuedSignal{
		CommandID:  commandID,
		DeviceID:   req.DeviceID,
		Kind:       CommandKindUpdate,
		FirmwareID: req.Firmware.ID,
		Version:    req.Firmware.Version,
		Checksum:   req.Firmware.Checksum,
	}); err != nil {
		return DeviceUpdate{}, fmt.Errorf("command device %s for wave %s: %w",
			req.DeviceID, req.WaveID, err)
	}
	return u.awaitResult(ctx, req, commandID)
}

// awaitResult waits for one device to conclude the command this activity delivered, observing the
// device's state at the poll interval until the command concludes or the deadline passes. Every
// observation failure is treated as "not reported yet" — a query error is not evidence about the
// device, and a wave must not fail because an observer blinked, nor roll back because its
// observer did — so the wait only ends on a recorded outcome or at the deadline.
func (u deviceUpdater) awaitResult(
	ctx context.Context,
	req UpdateDeviceRequest,
	commandID string,
) (DeviceUpdate, error) {
	// A deadline that has already passed leaves nothing to wait for, and the command's fate is
	// still worth one read: the device may have concluded it before this attempt started.
	for {
		if concluded, ok := u.observe(ctx, req.DeviceID, commandID); ok {
			return DeviceUpdate{
				DeviceID: req.DeviceID,
				Outcome:  updateOutcome(concluded.Outcome),
				Detail:   concluded.Detail,
			}, nil
		}
		if !time.Now().Before(req.Deadline) {
			return DeviceUpdate{DeviceID: req.DeviceID, Outcome: UpdateUnreported}, nil
		}
		// The heartbeat tells Temporal the wait is alive rather than stuck, so a lost attempt
		// is retried promptly instead of waiting out its heartbeat timeout. Outside an
		// activity there is no heartbeat to record, which is only the case in unit tests.
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, req.DeviceID)
		}

		// Never sleep past the deadline: the last wait is clipped so the activity stops when
		// the wave stops waiting.
		wait := u.poll
		if remaining := time.Until(req.Deadline); remaining < wait {
			wait = remaining
		}
		if err := sleep(ctx, wait); err != nil {
			return DeviceUpdate{}, err
		}
	}
}

// observe reads one device's state and reports what the command this activity delivered
// concluded. It reports ok=false when it cannot: the device has not concluded that command —
// still pending, superseded by a newer one, or never heard from — or the read itself failed.
// Neither is evidence about the device, so both leave the wait running to its deadline.
func (u deviceUpdater) observe(
	ctx context.Context,
	deviceID, commandID string,
) (ConcludedCommand, bool) {
	state, err := u.states.State(ctx, deviceID)
	if err != nil {
		if !errors.Is(err, ErrDeviceNotFound) && activity.IsActivity(ctx) {
			activity.GetLogger(ctx).Warn("observe device state",
				"device_id", deviceID, "command_id", commandID, "error", err)
		}
		return ConcludedCommand{}, false
	}
	if state.LastCommand == nil || state.LastCommand.Command.CommandID != commandID {
		// Either the command is still pending or a newer command superseded it. Neither is
		// this command's result.
		return ConcludedCommand{}, false
	}
	return *state.LastCommand, true
}

// updateOutcome maps the outcome a device reported onto the outcome its wave records. The two
// vocabularies agree on every value a device can report, so this is only the type boundary
// between "what the device said" and "what the wave records".
func updateOutcome(outcome CommandOutcome) DeviceUpdateOutcome {
	switch outcome {
	case OutcomeSucceeded:
		return UpdateSucceeded
	default:
		return UpdateFailed
	}
}

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
