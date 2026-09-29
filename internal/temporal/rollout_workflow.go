package temporal

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// Retry policies of the rollout's activities. Every write converges on the state it records and
// every read is a plain query, so retries are cheap — and bounded, so a permanently failing side
// effect resolves instead of holding the rollout open for the life of its run chain. The policies
// are built per caller rather than shared: a policy is mutable configuration, and one copy handed
// to several activities would let a change to one move all of them.
func storeRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    10 * time.Second,
		MaximumAttempts:    5,
	}
}

// firmwareRetryPolicy is the load-firmware policy: it also names the two refusals no retry can
// change, so a firmware that is unknown or does not target the selector's model fails the rollout
// instead of retrying.
func firmwareRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:        time.Second,
		BackoffCoefficient:     2,
		MaximumInterval:        10 * time.Second,
		MaximumAttempts:        5,
		NonRetryableErrorTypes: []string{firmwareUnknownErrorType, firmwareMismatchErrorType},
	}
}

// dispatchRetryPolicy is the dispatch-wave-update policy. Its retries are what make a partially
// delivered wave finish: the re-signalled devices dedup the commands they already accepted.
func dispatchRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    30 * time.Second,
		MaximumAttempts:    5,
	}
}

// healthRetryPolicy is the evaluate-wave-health policy: a failed evaluation is retried, and a
// resolved failure leaves the wave undecided rather than deciding it.
func healthRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    15 * time.Second,
		MaximumAttempts:    5,
	}
}

// RolloutWorkflow drives one canary rollout from start to finish. It records the rollout, resolves
// the firmware it deploys, and then drives the configured wave sequence in order, holding at most
// one wave in flight:
//
//   - every wave resolves its target membership and records it before anything is dispatched, and
//     a share that adds no device is recorded as skipped rather than gated;
//   - every dispatched wave is held open by a durable timer for its configured health window,
//     anchored at the wave's recorded start, while its update commands are delivered concurrently;
//   - a healthy wave is promoted, an unhealthy one rolls the rollout back, and an undecided one is
//     re-measured until it is decided or its decision timeout passes, after which it counts as
//     unhealthy;
//   - a wave configured to require approval waits for the operator's approve_next_wave signal
//     before it is resolved, and one approval authorizes exactly one wave.
//
// Every wait is a durable timer or a signal receive on the workflow clock, so a worker restart is
// a non-event: no wave is dispatched twice, no window restarts or shortens, and no approval is
// lost.
func RolloutWorkflow(ctx workflow.Context, in RolloutInput) error {
	state := newRolloutState(in)
	if err := state.validate(); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, GetRolloutStateQueryType, func() (RolloutView, error) {
		return state.view(), nil
	}); err != nil {
		return fmt.Errorf("register %s query: %w", GetRolloutStateQueryType, err)
	}
	approvals := workflow.GetSignalChannel(ctx, ApproveNextWaveSignalName)

	// The rollout is recorded as running before anything else, so an operator always has a
	// document naming the workflow that owns it.
	if err := recordRollout(ctx, &state); err != nil {
		return err
	}

	fw, err := loadFirmware(ctx, state)
	if err != nil {
		if outcome, ok := rolloutStartFailure(err); ok {
			// A firmware that is unknown or does not target the selector's model is a rollout
			// that cannot start: it concludes as failed and dispatches nothing.
			state.fail(outcome)
			workflow.GetLogger(ctx).Error("rollout cannot start",
				"rollout_id", state.RolloutID, "firmware_id", state.FirmwareID, "outcome", outcome)
			return recordRollout(ctx, &state)
		}
		return fmt.Errorf("load firmware %s: %w", state.FirmwareID, err)
	}

	for position, wave := range state.Settings.Waves {
		if wave.RequireApproval && !state.ApprovalOutstanding {
			// Nothing about a gated wave is decided before an operator approves it: no
			// membership is resolved and no command is delivered while the rollout waits.
			state.holdForApproval(position)
			if err := recordRollout(ctx, &state); err != nil {
				return err
			}
			awaitApproval(ctx, approvals, &state)
			state.resume()
			state.consumeApproval()
			if err := recordRollout(ctx, &state); err != nil {
				return err
			}
		} else if wave.RequireApproval {
			// A banked approval is spent on this wave and on no other.
			state.consumeApproval()
		}

		resolved, err := resolveWave(ctx, state, position)
		if err != nil {
			return err
		}
		state.startWave(position, resolved)

		if len(resolved.DeviceIDs) == 0 {
			// A share that adds no device can never gather evidence, so gating it would only
			// guarantee an undecided timeout: it is recorded and the rollout advances.
			state.recordWave(position, rollout.WaveSkipped, 0)
			if err := recordWaveState(ctx, state, position, rollout.WaveSkipped, 0); err != nil {
				return err
			}
			continue
		}

		advance, err := driveWave(ctx, &state, position, resolved, fw, approvals)
		if err != nil {
			return err
		}
		if !advance {
			// The wave ended the rollout, which is recorded as it concluded.
			return nil
		}
	}

	state.complete()
	return recordRollout(ctx, &state)
}

// driveWave drives one resolved wave: its dispatch, its durable health window, and its gate
// decision. It reports whether the rollout may advance to the next wave; a rollout the wave ended
// is recorded as concluded either way.
func driveWave(
	ctx workflow.Context,
	state *rolloutState,
	position int,
	resolved ResolvedWave,
	fw Firmware,
	approvals workflow.ReceiveChannel,
) (bool, error) {
	// The timer is created before the dispatch and anchored at the wave's recorded start, so
	// dispatch and window run concurrently and a slow dispatch eats into the window instead of
	// extending it. The window a gate reads is the window the wave document claims.
	timer := timerUntil(ctx, state.windowDeadline(resolved.StartedAt))
	dispatch := dispatchWave(ctx, *state, position, resolved, fw)

	if err := awaitDispatch(ctx, approvals, state, dispatch); err != nil {
		// The wave's commands cannot be delivered: it is recorded as failed and the rollout
		// transitions into rollback instead of advancing.
		state.recordWave(position, rollout.WaveFailed, 0)
		if recordErr := recordWaveState(ctx, *state, position, rollout.WaveFailed, 0); recordErr != nil {
			return false, recordErr
		}
		workflow.GetLogger(ctx).Error("wave dispatch failed",
			"rollout_id", state.RolloutID, "wave_id", state.Waves[position].ID, "error", err)
		return false, rollback(ctx, state, position, OutcomeDispatchFailed, nil)
	}

	state.recordWave(position, rollout.WaveEvaluating, 0)
	if err := recordWaveState(ctx, *state, position, rollout.WaveEvaluating, 0); err != nil {
		return false, err
	}

	// The wave is judged only once both the dispatch and the window have completed.
	awaitWindow(ctx, approvals, state, timer)

	decision, err := gateWave(ctx, approvals, state, position, resolved.StartedAt)
	if err != nil {
		return false, err
	}
	state.recordWave(position, decision.Status, decision.Health.SuccessRatio)
	if err := recordWaveState(ctx, *state, position, decision.Status, decision.Health.SuccessRatio); err != nil {
		return false, err
	}
	if decision.Status == rollout.WaveHealthy {
		return true, nil
	}

	outcome := OutcomeUnhealthyWave
	if decision.TimedOut {
		outcome = OutcomeDecisionTimeout
	}
	health := decision.Health
	return false, rollback(ctx, state, position, outcome, &health)
}

// gateWave measures a wave until its verdict is decided. A healthy or unhealthy verdict ends the
// wait; an undecided one — thin evidence is not a verdict — is re-measured after another health
// window, sliding the window forward, until the wave has been undecided for its decision timeout,
// after which it is treated as unhealthy.
func gateWave(
	ctx workflow.Context,
	approvals workflow.ReceiveChannel,
	state *rolloutState,
	position int,
	startedAt time.Time,
) (waveDecision, error) {
	deadline := state.decisionDeadline(startedAt)
	for {
		at := workflow.Now(ctx)
		health, err := evaluateWave(ctx, *state, state.Waves[position].ID, at)
		if err != nil {
			// A failed evaluation never decides a wave: the activity's retry policy owns
			// transient failure, and a resolved failure leaves the wave undecided rather
			// than wrongly promoted or rolled back.
			return waveDecision{}, err
		}
		switch health.Verdict {
		case wavehealth.VerdictHealthy:
			return waveDecision{Health: health, Status: rollout.WaveHealthy}, nil
		case wavehealth.VerdictUnhealthy:
			return waveDecision{Health: health, Status: rollout.WaveUnhealthy}, nil
		}

		if !at.Before(deadline) {
			return waveDecision{
				Health:   health,
				Status:   rollout.WaveUnhealthy,
				TimedOut: true,
			}, nil
		}
		// Another window of evidence, bounded by what is left of the decision timeout so the
		// wave cannot be measured past it.
		next := at.Add(state.Settings.HealthWindow)
		if deadline.Before(next) {
			next = deadline
		}
		awaitWindow(ctx, approvals, state, timerUntil(ctx, next))
	}
}

// rollback concludes a rollout on the wave that ended it, carrying that wave's decision, and
// records the terminal state. No further wave is resolved, dispatched, or gated.
func rollback(
	ctx workflow.Context,
	state *rolloutState,
	position int,
	outcome RolloutOutcome,
	decision *WaveHealth,
) error {
	state.rollback(position, outcome, decision)
	workflow.GetLogger(ctx).Error("rollout rolled back",
		"rollout_id", state.RolloutID,
		"wave_id", state.EndedBy,
		"outcome", outcome)
	return recordRollout(ctx, state)
}

// loadFirmware loads the metadata of the firmware the rollout deploys.
func loadFirmware(ctx workflow.Context, state rolloutState) (Firmware, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         firmwareRetryPolicy(),
	})
	var fw Firmware
	err := workflow.ExecuteActivity(ctx, LoadFirmwareActivityName, LoadFirmware{
		FirmwareID: state.FirmwareID,
		Model:      state.Model,
	}).Get(ctx, &fw)
	if err != nil {
		return Firmware{}, err
	}
	return fw, nil
}

// rolloutStartFailure maps a load-firmware failure onto the terminal outcome it names. Only the
// two refusals the firmware registry decided are rollouts that cannot start; every other error is
// a transient failure the activity's retries own.
func rolloutStartFailure(err error) (RolloutOutcome, bool) {
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		return "", false
	}
	switch appErr.Type() {
	case firmwareUnknownErrorType:
		return OutcomeFirmwareUnknown, true
	case firmwareMismatchErrorType:
		return OutcomeFirmwareMismatch, true
	default:
		return "", false
	}
}

// resolveWave resolves and records one wave's target membership. The wave's window opens at the
// workflow's own clock reading, and the store returns the recorded membership for a wave that
// already has a document, so a retried resolution converges instead of re-deriving it.
func resolveWave(ctx workflow.Context, state rolloutState, position int) (ResolvedWave, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         storeRetryPolicy(),
	})
	wave := state.Settings.Waves[position]
	req := ResolveWaveRequest{
		RolloutID: state.RolloutID,
		WaveID:    rollout.WaveID(state.RolloutID, position, wave.Percent),
		Percent:   wave.Percent,
		Region:    state.Region,
		Model:     state.Model,
		StartedAt: workflow.Now(ctx),
	}
	var resolved ResolvedWave
	if err := workflow.ExecuteActivity(ctx, ResolveWaveTargetsActivityName, req).Get(ctx, &resolved); err != nil {
		return ResolvedWave{}, fmt.Errorf("resolve wave %s of rollout %s: %w",
			req.WaveID, state.RolloutID, err)
	}
	return resolved, nil
}

// recordRollout records the rollout's current state. Every transition goes through it, so the
// stored document is the workflow's own picture rather than an artifact of activity timing.
func recordRollout(ctx workflow.Context, state *rolloutState) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         storeRetryPolicy(),
	})
	req := RecordRolloutRequest{
		RolloutID:  state.RolloutID,
		FirmwareID: state.FirmwareID,
		WorkflowID: state.WorkflowID,
		Region:     state.Region,
		Model:      state.Model,
		Status:     state.Status,
	}
	if err := workflow.ExecuteActivity(ctx, RecordRolloutStateActivityName, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("record rollout %s as %s: %w", state.RolloutID, state.Status, err)
	}
	return nil
}

// recordWaveState records a wave's decided state, so the stored wave document carries the status
// and success rate the workflow decided on.
func recordWaveState(
	ctx workflow.Context,
	state rolloutState,
	position int,
	status rollout.WaveStatus,
	successRate float64,
) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         storeRetryPolicy(),
	})
	req := RecordWaveRequest{
		RolloutID:   state.RolloutID,
		WaveID:      state.Waves[position].ID,
		Status:      status,
		SuccessRate: successRate,
	}
	if err := workflow.ExecuteActivity(ctx, RecordWaveStateActivityName, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("record wave %s as %s: %w", req.WaveID, status, err)
	}
	return nil
}

// dispatchWave starts the wave's update commands: one activity delivering a command per target
// device's workflow, under a command id derived from the wave and the device.
func dispatchWave(
	ctx workflow.Context,
	state rolloutState,
	position int,
	resolved ResolvedWave,
	fw Firmware,
) workflow.Future {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// A wave's dispatch signals every device it targets: hundreds of workflow signals at
		// fleet scale, so it gets a minute rather than seconds.
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         dispatchRetryPolicy(),
	})
	return workflow.ExecuteActivity(ctx, DispatchWaveUpdateActivityName, DispatchWaveRequest{
		RolloutID: state.RolloutID,
		WaveID:    state.Waves[position].ID,
		DeviceIDs: resolved.DeviceIDs,
		Firmware:  fw,
	})
}

// evaluateWave measures one wave's health at the moment the workflow decided to judge it. The
// moment is the workflow's own clock reading, so a replay evaluates the same window.
func evaluateWave(ctx workflow.Context, state rolloutState, waveID string, at time.Time) (WaveHealth, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         healthRetryPolicy(),
	})
	var health WaveHealth
	err := workflow.ExecuteActivity(ctx, EvaluateWaveHealthActivityName, EvaluateWaveRequest{
		RolloutID: state.RolloutID,
		WaveID:    waveID,
		At:        at,
	}).Get(ctx, &health)
	if err != nil {
		return WaveHealth{}, fmt.Errorf("evaluate wave %s of rollout %s: %w",
			waveID, state.RolloutID, err)
	}
	return health, nil
}

// timerUntil returns a durable timer firing at at, or nil when at has already passed — an elapsed
// window is never waited on again.
func timerUntil(ctx workflow.Context, at time.Time) workflow.Future {
	remaining := at.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		return nil
	}
	return workflow.NewTimer(ctx, remaining)
}

// awaitDispatch waits for a wave's dispatch to finish, folding approval signals into the state as
// they arrive. It reports the dispatch failure, which makes the wave undeliverable.
func awaitDispatch(
	ctx workflow.Context,
	approvals workflow.ReceiveChannel,
	state *rolloutState,
	dispatch workflow.Future,
) error {
	dispatched := false
	var dispatchErr error
	for !dispatched {
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(dispatch, func(f workflow.Future) {
			dispatched = true
			dispatchErr = f.Get(ctx, nil)
		})
		sel.AddReceive(approvals, func(c workflow.ReceiveChannel, _ bool) {
			receiveApproval(ctx, c, state)
		})
		sel.Select(ctx)
	}
	if dispatchErr != nil {
		return fmt.Errorf("dispatch wave: %w", dispatchErr)
	}
	return nil
}

// awaitWindow waits out a wave's health window — or the remainder of it — folding approval signals
// into the state as they arrive. A nil timer is a window that has already elapsed: there is
// nothing left to wait for.
func awaitWindow(
	ctx workflow.Context,
	approvals workflow.ReceiveChannel,
	state *rolloutState,
	timer workflow.Future,
) {
	if timer == nil {
		return
	}
	elapsed := false
	for !elapsed {
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(timer, func(workflow.Future) { elapsed = true })
		sel.AddReceive(approvals, func(c workflow.ReceiveChannel, _ bool) {
			receiveApproval(ctx, c, state)
		})
		sel.Select(ctx)
	}
}

// awaitApproval blocks until an approval is outstanding, folding each delivered approval in. The
// wait is a signal receive on the workflow clock, so an approval delivered while no worker was
// executing the rollout is applied when it resumes.
func awaitApproval(ctx workflow.Context, approvals workflow.ReceiveChannel, state *rolloutState) {
	for !state.ApprovalOutstanding {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(approvals, func(c workflow.ReceiveChannel, _ bool) {
			receiveApproval(ctx, c, state)
		})
		sel.Select(ctx)
	}
}

// receiveApproval folds one delivered approval signal into the state. The signal carries nothing:
// its arrival is the decision.
func receiveApproval(ctx workflow.Context, approvals workflow.ReceiveChannel, state *rolloutState) {
	var signal ApproveNextWaveSignal
	approvals.Receive(ctx, &signal)
	state.applyApproval()
	workflow.GetLogger(ctx).Info("approval received",
		"rollout_id", state.RolloutID, "outstanding", state.ApprovalOutstanding)
}
