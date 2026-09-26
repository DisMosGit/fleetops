package temporal

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Registered names and signal/query types of the device entity. Workflows and activities
// register under explicit names — never function-name reflection — and producers address
// signals by these constants.
const (
	// DeviceWorkflowName is the registered name of DeviceWorkflow.
	DeviceWorkflowName = "device-workflow"
	// DispatchActivityName is the registered name of the dispatch-command activity.
	DispatchActivityName = "dispatch-command"
	// SnapshotActivityName is the registered name of the snapshot-device-state activity.
	SnapshotActivityName = "snapshot-device-state"
	// GetStateQueryType is the state query returning the authoritative device state.
	GetStateQueryType = "get-state"
	// HeartbeatSignalName carries a HeartbeatSignal.
	HeartbeatSignalName = "heartbeat"
	// CommandIssuedSignalName carries a CommandIssuedSignal.
	CommandIssuedSignalName = "command_issued"
	// CommandResultSignalName carries a CommandResultSignal.
	CommandResultSignalName = "command_result"
	// ConfigChangedSignalName carries a ConfigChangedSignal.
	ConfigChangedSignalName = "config_changed"
)

// maxSignalsPerRun bounds one run's event history. Every delivered signal adds history
// events, so after this many the run rolls over — see the rolling continuation in
// DeviceWorkflow. Not a configuration knob on purpose: nothing tunes it in this fleet, and a
// knob nobody touches is scope, not flexibility.
const maxSignalsPerRun = 128

// DeviceWorkflowID returns the stable workflow id of a device's entity workflow. One device
// has exactly one run chain under this id for its whole life.
func DeviceWorkflowID(deviceID string) string {
	return "device-" + deviceID
}

// DeviceWorkflow is the long-lived per-device entity: it owns the device's authoritative
// state (current firmware, last heartbeat, pending command, configuration snapshot) and
// folds in signals — heartbeat, command_issued, command_result, config_changed — exactly
// once per delivery key. It keeps the outside world in step with that state: search
// attributes mirror it for the Temporal UI, and a snapshot activity projects it into the
// fleet database periodically and on every meaningful transition. The state is the
// workflow's only mutable data and travels whole across rolling continuations, so the
// entity outlives any run while its history stays bounded. The state argument is the empty
// state of a new device on first start and the carried-over state on every continuation.
func DeviceWorkflow(ctx workflow.Context, state deviceState) error {
	if err := state.validate(); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, GetStateQueryType, func() (State, error) {
		return state.view(), nil
	}); err != nil {
		return fmt.Errorf("register %s query: %w", GetStateQueryType, err)
	}

	signals := signalChannels{
		heartbeat: workflow.GetSignalChannel(ctx, HeartbeatSignalName),
		issued:    workflow.GetSignalChannel(ctx, CommandIssuedSignalName),
		result:    workflow.GetSignalChannel(ctx, CommandResultSignalName),
		config:    workflow.GetSignalChannel(ctx, ConfigChangedSignalName),
	}
	dispatchCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// Delivery waits for the device to be reachable: retries back off to a capped
		// interval and continue for the life of the run. A malformed command is the one
		// failure no retry can fix. A superseded or concluded command arriving late on the
		// wire is harmless — its result is deduplicated by command id.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        30 * time.Second,
			NonRetryableErrorTypes: []string{invalidCommandErrorType},
		},
	})
	snapshotCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// The snapshot write converges on the newest state, so retries are cheap — and
		// bounded, so a permanently failing write resolves instead of holding the single
		// in-flight snapshot slot for the life of the run.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	})

	var dispatch, snapshot workflow.Future
	var dispatching, dispatchFailed string // command ids
	var snapshotPending bool               // a snapshot was asked for while one is in flight
	timer := workflow.NewTimer(ctx, state.Settings.SnapshotInterval)
	var attrs map[string]any // the search attributes last upserted; nil before the first

	// schedule applies the consequences of one decision point — run start, an applied
	// signal, or a snapshot tick: the derived liveness judgement, the snapshots the state
	// changes call for, and the search attributes that mirror the state. It is the only
	// place the entity reaches out to the world besides command dispatch.
	schedule := func(applied transitions, tick bool) {
		flipped := state.refreshLiveness(workflow.Now(ctx))
		if tick {
			timer = workflow.NewTimer(ctx, state.Settings.SnapshotInterval)
		}
		if applied.needsSnapshot() || flipped || tick {
			if snapshot != nil {
				// Coalesce onto the write already in flight: its successor projects the
				// state as it stands when the in-flight one completes.
				snapshotPending = true
			} else {
				snapshot = workflow.ExecuteActivity(snapshotCtx, SnapshotActivityName,
					snapshotOf(state, workflow.Now(ctx)))
			}
		}
		if next := searchAttributes(state); !attrsEqual(attrs, next) {
			if err := workflow.UpsertTypedSearchAttributes(ctx, attrUpdates(next)...); err != nil {
				// The state query stays authoritative. A failed upsert is retried at the
				// next decision point, because the recorded map stays stale until one
				// succeeds.
				workflow.GetLogger(ctx).Error("upsert search attributes",
					"device_id", state.DeviceID, "error", err)
			} else {
				attrs = next
			}
		}
	}
	schedule(transitions{}, false)

	for {
		if state.SignalsApplied >= maxSignalsPerRun {
			signals.drainAll(&state)
			state.SignalsApplied = 0
			// An in-flight snapshot is abandoned here on purpose: the write is idempotent
			// and the continuing run's next tick refreshes the projection.
			return workflow.NewContinueAsNewError(ctx, DeviceWorkflowName, state)
		}

		if state.Pending != nil && !state.Pending.Dispatched &&
			dispatching != state.Pending.Command.CommandID &&
			dispatchFailed != state.Pending.Command.CommandID {
			dispatching = state.Pending.Command.CommandID
			dispatch = workflow.ExecuteActivity(dispatchCtx, DispatchActivityName, state.Pending.Command)
		}
		// A concluded command's in-flight delivery is dropped: whatever it reports can only
		// concern a command that is no longer pending.
		if dispatching != "" && (state.Pending == nil || state.Pending.Command.CommandID != dispatching) {
			dispatch, dispatching = nil, ""
		}

		var applied transitions
		tick := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(signals.heartbeat, func(c workflow.ReceiveChannel, _ bool) {
			var h HeartbeatSignal
			c.Receive(ctx, &h)
			applied = state.applyHeartbeat(h)
		})
		sel.AddReceive(signals.issued, func(c workflow.ReceiveChannel, _ bool) {
			var cmd CommandIssuedSignal
			c.Receive(ctx, &cmd)
			applied = state.applyCommandIssued(cmd)
		})
		sel.AddReceive(signals.result, func(c workflow.ReceiveChannel, _ bool) {
			var r CommandResultSignal
			c.Receive(ctx, &r)
			applied = state.applyCommandResult(r)
		})
		sel.AddReceive(signals.config, func(c workflow.ReceiveChannel, _ bool) {
			var change ConfigChangedSignal
			c.Receive(ctx, &change)
			applied = state.applyConfigChanged(change)
		})
		if dispatch != nil {
			sel.AddFuture(dispatch, func(f workflow.Future) {
				if err := f.Get(ctx, nil); err != nil {
					// The retry policy owns transient failure, so a resolved future is a
					// permanent one: log it and leave the command undispatched rather than
					// spinning on it.
					dispatchFailed = dispatching
					workflow.GetLogger(ctx).Error("dispatch command failed",
						"command_id", dispatching, "error", err)
				} else if state.Pending != nil && state.Pending.Command.CommandID == dispatching {
					state.Pending.Dispatched = true
				}
				dispatch, dispatching = nil, ""
			})
		}
		if snapshot != nil {
			sel.AddFuture(snapshot, func(f workflow.Future) {
				if err := f.Get(ctx, nil); err != nil {
					// A lost snapshot write costs at most one snapshot interval of
					// staleness — the periodic snapshot bounds it — and never disturbs
					// the entity.
					workflow.GetLogger(ctx).Error("snapshot device state failed",
						"device_id", state.DeviceID, "error", err)
				}
				snapshot = nil
				if snapshotPending {
					snapshotPending = false
					snapshot = workflow.ExecuteActivity(snapshotCtx, SnapshotActivityName,
						snapshotOf(state, workflow.Now(ctx)))
				}
			})
		}
		if timer != nil {
			sel.AddFuture(timer, func(workflow.Future) {
				timer = nil
				tick = true
			})
		}
		sel.Select(ctx)
		schedule(applied, tick)
	}
}

// signalChannels are the four signal channels of one device workflow run.
type signalChannels struct {
	heartbeat, issued, result, config workflow.ReceiveChannel
}

// drainAll folds every buffered signal into state and returns once the channels are empty. A
// run must drain before it continues as new: unhandled buffered signals die with the run,
// while signals arriving after the drain are buffered against the workflow id and reach the
// next run — together that is exactly-once delivery around the rollover.
func (ch signalChannels) drainAll(state *deviceState) {
	for {
		drained := false
		for {
			var h HeartbeatSignal
			if !ch.heartbeat.ReceiveAsync(&h) {
				break
			}
			state.applyHeartbeat(h)
			drained = true
		}
		for {
			var cmd CommandIssuedSignal
			if !ch.issued.ReceiveAsync(&cmd) {
				break
			}
			state.applyCommandIssued(cmd)
			drained = true
		}
		for {
			var r CommandResultSignal
			if !ch.result.ReceiveAsync(&r) {
				break
			}
			state.applyCommandResult(r)
			drained = true
		}
		for {
			var change ConfigChangedSignal
			if !ch.config.ReceiveAsync(&change) {
				break
			}
			state.applyConfigChanged(change)
			drained = true
		}
		if !drained {
			return
		}
	}
}
