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
// once per delivery key. The state is the workflow's only mutable data and travels whole
// across rolling continuations, so the entity outlives any run while its history stays
// bounded. The state argument is the empty state of a new device on first start and the
// carried-over state on every continuation.
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

	var dispatch workflow.Future
	var dispatching, dispatchFailed string // command ids
	for {
		if state.SignalsApplied >= maxSignalsPerRun {
			signals.drainAll(&state)
			state.SignalsApplied = 0
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

		sel := workflow.NewSelector(ctx)
		sel.AddReceive(signals.heartbeat, func(c workflow.ReceiveChannel, _ bool) {
			var h HeartbeatSignal
			c.Receive(ctx, &h)
			state.applyHeartbeat(h)
		})
		sel.AddReceive(signals.issued, func(c workflow.ReceiveChannel, _ bool) {
			var cmd CommandIssuedSignal
			c.Receive(ctx, &cmd)
			state.applyCommandIssued(cmd)
		})
		sel.AddReceive(signals.result, func(c workflow.ReceiveChannel, _ bool) {
			var r CommandResultSignal
			c.Receive(ctx, &r)
			state.applyCommandResult(r)
		})
		sel.AddReceive(signals.config, func(c workflow.ReceiveChannel, _ bool) {
			var change ConfigChangedSignal
			c.Receive(ctx, &change)
			state.applyConfigChanged(change)
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
		sel.Select(ctx)
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
