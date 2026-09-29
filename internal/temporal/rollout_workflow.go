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

// dispatchRetryPolicy is the per-device update policy. A device the rollout could not command is
// the case its retries exist for, and the retries are bounded so a permanently unreachable device
// resolves — as a failed wave — instead of holding the rollout open.
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
//   - every dispatched wave commands each of its target devices through one update activity and
//     waits for that device's reported result within the configured result timeout, so the wave's
//     dispatch is complete only once every target has settled — success, failure, or unreported;
//   - a dispatched wave is held open by a durable timer for its configured health window,
//     anchored at the wave's recorded start, while its update commands are delivered and awaited
//     concurrently;
//   - a healthy wave is promoted, an unhealthy one rolls the rollout back, and an undecided one is
//     re-measured until it is decided or its decision timeout passes, after which it counts as
//     unhealthy;
//   - a wave configured to require approval waits for the operator's approve_next_wave signal
//     before it is resolved, and one approval authorizes exactly one wave;
//   - a pause_rollout signal holds the rollout at the next wave boundary — no membership resolved,
//     nothing dispatched — and a resume_rollout signal lets it continue from exactly where it
//     stopped. A wave already in flight is still driven to its recorded decision and can still
//     roll the rollout back: a pause holds promotion, never safety.
//
// Every wait is a durable timer or a signal receive on the workflow clock, and every wait folds
// the operator's control signals, so a worker restart is a non-event: no wave is dispatched twice,
// no window restarts or shortens, and no approval, pause, or resume is lost.
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
	signals := rolloutSignals{
		approvals: workflow.GetSignalChannel(ctx, ApproveNextWaveSignalName),
		pauses:    workflow.GetSignalChannel(ctx, PauseRolloutSignalName),
		resumes:   workflow.GetSignalChannel(ctx, ResumeRolloutSignalName),
	}

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
	// The loaded version is what the run's RolloutFirmware attribute mirrors, so the attribute
	// is filled in as soon as it is known: an operator filtering the UI by firmware version
	// finds the rollout from the moment it knows what it deploys.
	state.FirmwareVersion = fw.Version
	upsertRolloutAttrs(ctx, &state)

	for position, wave := range state.Settings.Waves {
		// A paused rollout resolves no membership and dispatches nothing: the hold is at the
		// wave boundary, before anything about the wave is decided.
		if err := holdWhilePaused(ctx, signals, &state); err != nil {
			return err
		}
		if state.terminal() {
			return nil
		}

		if wave.RequireApproval && !state.ApprovalOutstanding {
			// Nothing about a gated wave is decided before an operator approves it: no
			// membership is resolved and no command is delivered while the rollout waits.
			state.holdForApproval(position)
			if err := recordRollout(ctx, &state); err != nil {
				return err
			}
			if err := awaitApproval(ctx, signals, &state); err != nil {
				return err
			}
			if state.terminal() {
				return nil
			}
			state.releaseApprovalHold()
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
			state.recordWave(position, rollout.WaveSkipped, 0, waveOutcomes{})
			if err := recordWaveState(ctx, state, position, rollout.WaveSkipped, 0, waveOutcomes{}); err != nil {
				return err
			}
			continue
		}

		advance, err := driveWave(ctx, &state, position, resolved, fw, signals)
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

// rolloutSignals are the operator signals one rollout run folds at every wait: the approval a
// gated wave needs, and the pause and resume that hold the rollout and continue it.
type rolloutSignals struct {
	approvals workflow.ReceiveChannel
	pauses    workflow.ReceiveChannel
	resumes   workflow.ReceiveChannel
}

// holdWhilePaused blocks a paused rollout at a wave boundary. It resolves nothing and dispatches
// nothing while it holds, so no wave starts and no command is delivered. Every signal that arrives
// while it holds is folded — through awaitControl's callbacks — so a resume releases the hold and
// an approval that arrives during the pause is banked before the gated wave is reached, which is
// what makes the operator's own approval readable through the state query at the time it is sent.
//
// The hold is a signal receive on the workflow clock, so a pause delivered while no worker was
// executing the rollout is applied when one resumes, and the wait is unbounded on purpose: a pause
// lasts until an operator resumes the rollout, and the workflow waiting for that signal is the
// hold.
func holdWhilePaused(ctx workflow.Context, signals rolloutSignals, state *rolloutState) error {
	for state.Paused && !state.terminal() {
		awaitControl(ctx, signals, state)
	}
	return nil
}

// driveWave drives one resolved wave: one update activity per target device, its durable health
// window, and its gate decision. It reports whether the rollout may advance to the next wave; a
// rollout the wave ended is recorded as concluded either way.
func driveWave(
	ctx workflow.Context,
	state *rolloutState,
	position int,
	resolved ResolvedWave,
	fw Firmware,
	signals rolloutSignals,
) (bool, error) {
	// The timer is created before the dispatch and anchored at the wave's recorded start, so
	// dispatch and window run concurrently and a slow dispatch eats into the window instead of
	// extending it. The window a gate reads is the window the wave document claims.
	timer := timerUntil(ctx, state.windowDeadline(resolved.StartedAt))
	// The updates are scheduled under a context the wave can cancel, which is how a wave whose
	// commands could not be delivered stops waiting on the devices it never reached instead of
	// waiting out their result deadlines.
	updateCtx, cancelUpdates := workflow.WithCancel(ctx)
	defer cancelUpdates()
	updates := updateWave(updateCtx, *state, position, resolved, fw)

	outcomes, err := collectUpdates(ctx, signals, state, resolved, updates, cancelUpdates)
	if err != nil {
		// The wave's commands cannot be delivered: it is recorded as failed and the rollout
		// transitions into rollback instead of advancing.
		state.recordWave(position, rollout.WaveFailed, 0, outcomes)
		if recordErr := recordWaveState(ctx, *state, position, rollout.WaveFailed, 0, outcomes); recordErr != nil {
			return false, recordErr
		}
		workflow.GetLogger(ctx).Error("wave dispatch failed",
			"rollout_id", state.RolloutID, "wave_id", state.Waves[position].ID, "error", err)
		return false, rollback(ctx, state, position, OutcomeDispatchFailed, nil)
	}

	// Every device has settled, so the wave's dispatch is complete: its outcome sets are
	// recorded before it is measured, which is what makes "the wave was dispatched to exactly
	// these devices and these are the ones that did not take it" readable while it waits.
	state.recordWave(position, rollout.WaveEvaluating, 0, outcomes)
	if err := recordWaveState(ctx, *state, position, rollout.WaveEvaluating, 0, outcomes); err != nil {
		return false, err
	}

	// The wave is judged only once both the dispatch and the window have completed.
	if err := awaitWindow(ctx, signals, state, timer); err != nil {
		return false, err
	}

	decision, err := gateWave(ctx, signals, state, position, resolved.StartedAt)
	if err != nil {
		return false, err
	}
	state.recordWave(position, decision.Status, decision.Health.SuccessRatio, outcomes)
	if err := recordWaveState(ctx, *state, position, decision.Status, decision.Health.SuccessRatio, outcomes); err != nil {
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
	signals rolloutSignals,
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
		if err := awaitWindow(ctx, signals, state, timerUntil(ctx, next)); err != nil {
			return waveDecision{}, err
		}
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
// stored document is the workflow's own picture rather than an artifact of activity timing; the
// status it writes is the projected one (see rolloutState.reported), so the document cannot
// disagree with the search attribute or the state query.
//
// It also keeps the run's search attributes in step with the same state, upserting only when a
// mirrored value changed: the attributes and the document are written from one state, so an
// operator filtering the Temporal UI and an operator reading the collection see the same rollout.
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
		Status:     state.reported(),
	}
	if err := workflow.ExecuteActivity(ctx, RecordRolloutStateActivityName, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("record rollout %s as %s: %w", state.RolloutID, req.Status, err)
	}
	upsertRolloutAttrs(ctx, state)
	return nil
}

// upsertRolloutAttrs mirrors the rollout's state into its search attributes, upserting only the
// map a state change actually moved. A failed upsert is logged rather than returned: the state
// query and the recorded document stay authoritative, and the recorded map stays stale until an
// upsert succeeds, so the next transition retries it.
func upsertRolloutAttrs(ctx workflow.Context, state *rolloutState) {
	next := rolloutSearchAttributes(*state)
	if attrsEqual(state.LastAttrs, next) {
		return
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, attrUpdates(next)...); err != nil {
		workflow.GetLogger(ctx).Error("upsert rollout search attributes",
			"rollout_id", state.RolloutID, "status", state.reported(), "error", err)
		return
	}
	state.LastAttrs = next
}

// recordWaveState records a wave's decided state, so the stored wave document carries the status,
// the success rate the gate measured, and the device outcomes the wave's dispatch collected.
func recordWaveState(
	ctx workflow.Context,
	state rolloutState,
	position int,
	status rollout.WaveStatus,
	successRate float64,
	outcomes waveOutcomes,
) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         storeRetryPolicy(),
	})
	req := RecordWaveRequest{
		RolloutID:           state.RolloutID,
		WaveID:              state.Waves[position].ID,
		Status:              status,
		SuccessRate:         successRate,
		FailedDeviceIDs:     outcomes.Failed,
		UnreportedDeviceIDs: outcomes.Unreported,
	}
	if err := workflow.ExecuteActivity(ctx, RecordWaveStateActivityName, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("record wave %s as %s: %w", req.WaveID, status, err)
	}
	return nil
}

// updateWave schedules one update activity per target device: each delivers that device's update
// command through the device command seam and waits for the device's reported result, up to the
// wave's result deadline. Every activity is scheduled before any is awaited, so the wave's devices
// are commanded concurrently and the workflow is never blocked on one of them.
//
// The deadline is computed here, from the workflow's own clock reading, and carried in every
// request: it is a value recorded in history, so a replay, a retry, or a restart recomputes the
// same one and a worker outage cannot extend the window a wave waits in.
func updateWave(
	ctx workflow.Context,
	state rolloutState,
	position int,
	resolved ResolvedWave,
	fw Firmware,
) []workflow.Future {
	deadline := workflow.Now(ctx).Add(state.Settings.ResultTimeout)
	// StartToCloseTimeout leaves room for the deadline to be reached and then some: the
	// activity stops waiting at the deadline, and the slack covers the last poll, the return
	// trip, and a retry of an attempt that died just short of it.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: state.Settings.ResultTimeout + resultTimeoutSlack,
		HeartbeatTimeout:    heartbeatTimeout(state.Settings.ResultTimeout),
		RetryPolicy:         dispatchRetryPolicy(),
	})

	updates := make([]workflow.Future, 0, len(resolved.DeviceIDs))
	for _, deviceID := range resolved.DeviceIDs {
		updates = append(updates, workflow.ExecuteActivity(ctx, UpdateDeviceActivityName, UpdateDeviceRequest{
			RolloutID: state.RolloutID,
			WaveID:    state.Waves[position].ID,
			DeviceID:  deviceID,
			Firmware:  fw,
			Deadline:  deadline,
		}))
	}
	return updates
}

// collectUpdates waits for every one of a wave's updates in target order, folding operator signals
// into the state as they arrive, and reports the outcomes the wave records.
//
// Waiting in target order keeps the workflow's callback set small and its history linear: a wave
// costs a couple of events per target device. The first delivery error cancels the remaining
// activities and fails the wave, because a wave the rollout could not command is not evidence of
// health — and waiting out the deadlines of the rest would delay the rollback by up to the result
// timeout for no new information.
func collectUpdates(
	ctx workflow.Context,
	signals rolloutSignals,
	state *rolloutState,
	resolved ResolvedWave,
	updates []workflow.Future,
	cancelRemaining func(),
) (waveOutcomes, error) {
	var outcomes waveOutcomes
	for i, update := range updates {
		var result DeviceUpdate
		err := awaitUpdate(ctx, signals, state, update, &result)
		if err != nil {
			cancelRemaining()
			return outcomes, fmt.Errorf("update device %s of wave %s: %w",
				resolved.DeviceIDs[i], resolved.WaveID, err)
		}
		switch result.Outcome {
		case UpdateSucceeded:
			// The device took the update: recorded by the health measurement, not here.
		case UpdateFailed:
			outcomes.Failed = append(outcomes.Failed, result.DeviceID)
		default:
			outcomes.Unreported = append(outcomes.Unreported, result.DeviceID)
		}
	}
	return outcomes, nil
}

// awaitUpdate waits for one device's update activity, folding operator signals into the state
// while it waits.
func awaitUpdate(
	ctx workflow.Context,
	signals rolloutSignals,
	state *rolloutState,
	update workflow.Future,
	result *DeviceUpdate,
) error {
	settled := false
	var updateErr error
	await := func(sel workflow.Selector) {
		sel.AddFuture(update, func(f workflow.Future) {
			settled = true
			updateErr = f.Get(ctx, result)
		})
	}
	for !settled {
		awaitControlUntil(ctx, signals, state, await)
	}
	return updateErr
}

// resultTimeoutSlack is what a wave's update activities may take beyond the result deadline: the
// deadline is reached by waiting, and the slack covers the last poll's return trip plus one retry
// of an attempt that died just short of it.
const resultTimeoutSlack = 30 * time.Second

// heartbeatTimeout bounds how long a lost update attempt goes unnoticed, so a dead attempt is
// retried while there is still time to re-read the device's recorded outcome before the wave
// stops waiting.
//
// It is never shorter than the activity's observation interval: an attempt can only heartbeat
// between two observations, so a timeout below that interval would kill an attempt that is
// waiting exactly as designed, and every attempt would be retried for no reason.
func heartbeatTimeout(resultTimeout time.Duration) time.Duration {
	if timeout := resultTimeout / 2; timeout > minHeartbeatTimeout {
		return timeout
	}
	return minHeartbeatTimeout
}

// minHeartbeatTimeout is the floor under a wave's heartbeat timeout: comfortably above the
// observation interval, so an activity that is waiting as designed keeps its attempt alive.
const minHeartbeatTimeout = updatePollInterval + updatePollInterval/2

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

// awaitWindow waits out a wave's health window — or the remainder of it — folding operator signals
// into the state as they arrive. A nil timer is a window that has already ended: there is nothing
// left to wait for. The window's own deadline is untouched by anything folded here: a pause does
// not restart, shorten, or extend it, so the wave is judged when it was always going to be.
func awaitWindow(
	ctx workflow.Context,
	signals rolloutSignals,
	state *rolloutState,
	timer workflow.Future,
) error {
	if timer == nil {
		return nil
	}
	elapsed := false
	await := func(sel workflow.Selector) {
		sel.AddFuture(timer, func(workflow.Future) { elapsed = true })
	}
	for !elapsed {
		awaitControlUntil(ctx, signals, state, await)
	}
	return nil
}

// awaitApproval blocks until an approval is outstanding, folding every operator signal in while it
// holds. The wait is a signal receive on the workflow clock, so an approval, a pause, or a resume
// delivered while no worker was executing the rollout is applied when it resumes. A paused rollout
// keeps waiting here: whether it may proceed is the caller's decision, and this only folds what
// arrived.
func awaitApproval(ctx workflow.Context, signals rolloutSignals, state *rolloutState) error {
	for !state.ApprovalOutstanding && !state.terminal() {
		awaitControl(ctx, signals, state)
	}
	return nil
}

// awaitControl waits for the operator's next control signal and folds it into the state. It is the
// hold a paused rollout sits in and the signal-only step every other wait folds through.
func awaitControl(ctx workflow.Context, signals rolloutSignals, state *rolloutState) {
	awaitControlUntil(ctx, signals, state, nil)
}

// awaitControlUntil waits until the wait described by await ends — or, when await is nil, until the
// operator's next control signal arrives — folding every signal that arrives on the way into the
// state. Every wait in the rollout goes through it, so no wait path can ignore a pause or a
// resume, and a folded signal records the rollout when it moved the status the operator sees, so
// the document stays current without a write per duplicate.
//
// await registers the wait's own callback; it signals its own completion through whatever its
// caller checks, so the caller loops until that condition holds.
func awaitControlUntil(
	ctx workflow.Context,
	signals rolloutSignals,
	state *rolloutState,
	await func(workflow.Selector),
) {
	sel := workflow.NewSelector(ctx)
	if await != nil {
		await(sel)
	}
	sel.AddReceive(signals.pauses, func(c workflow.ReceiveChannel, _ bool) {
		var signal PauseRolloutSignal
		c.Receive(ctx, &signal)
		before := state.reported()
		state.pause()
		workflow.GetLogger(ctx).Info("pause received",
			"rollout_id", state.RolloutID, "paused", state.Paused)
		logControlRecord(ctx, state, before)
	})
	sel.AddReceive(signals.resumes, func(c workflow.ReceiveChannel, _ bool) {
		var signal ResumeRolloutSignal
		c.Receive(ctx, &signal)
		before := state.reported()
		state.resume()
		workflow.GetLogger(ctx).Info("resume received",
			"rollout_id", state.RolloutID, "paused", state.Paused)
		logControlRecord(ctx, state, before)
	})
	sel.AddReceive(signals.approvals, func(c workflow.ReceiveChannel, _ bool) {
		receiveApproval(ctx, c, state)
	})
	sel.Select(ctx)
}

// logControlRecord records the rollout when a folded control signal moved the status it reports. A
// write failure is logged rather than returned: a selector callback cannot fail the workflow, the
// state is already updated, and the next wait records the same status again — the write is
// idempotent, so the retry converges instead of duplicating.
func logControlRecord(ctx workflow.Context, state *rolloutState, before rollout.RolloutStatus) {
	if state.reported() == before {
		return
	}
	if err := recordRollout(ctx, state); err != nil {
		workflow.GetLogger(ctx).Error("record rollout after a control signal",
			"rollout_id", state.RolloutID, "status", state.reported(), "error", err)
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
