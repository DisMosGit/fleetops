package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// signalClient is the slice of the Temporal client the signaler needs — signal-with-start is
// the only call it makes. client.Client satisfies it, and tests hand-write a fake.
type signalClient interface {
	SignalWithStartWorkflow(
		ctx context.Context,
		workflowID string,
		signalName string,
		signalArg any,
		options client.StartWorkflowOptions,
		workflow any,
		workflowArgs ...any,
	) (client.WorkflowRun, error)
}

// Signaler delivers device events to the per-device workflow: every call signals the device's
// workflow and starts its run chain first when the device has none — one run chain per device
// id, created lazily on first contact and never duplicated. New run chains are seeded with the
// device's empty state under the configured settings.
type Signaler struct {
	client    signalClient
	taskQueue string
	settings  DeviceSettings
}

// NewSignaler returns a signaler that starts and signals device workflows on taskQueue, seeding
// new run chains with the entity settings they decide under.
func NewSignaler(c signalClient, taskQueue string, settings DeviceSettings) *Signaler {
	return &Signaler{client: c, taskQueue: taskQueue, settings: settings}
}

// SignalHeartbeat delivers one heartbeat to its device's workflow, carrying the event id a
// redelivery reuses. It implements the agentserver.DeviceSignaler seam.
func (s *Signaler) SignalHeartbeat(
	ctx context.Context,
	rec devices.Record,
	hb *agentv1.Heartbeat,
) error {
	return s.signalWithStart(ctx, rec.ID, HeartbeatSignalName, HeartbeatSignal{
		EventID:   hb.GetEventId(),
		DeviceID:  rec.ID,
		Region:    rec.Region,
		Model:     rec.Model,
		CurrentFw: hb.GetCurrentFw(),
		Timestamp: hb.GetTs().AsTime(),
	})
}

// SignalCommandResult delivers the terminal result of one command to its device's workflow.
// It implements the agentserver.DeviceSignaler seam.
func (s *Signaler) SignalCommandResult(ctx context.Context, res *agentv1.ReportRequest) error {
	var outcome CommandOutcome
	switch res.GetOutcome() {
	case agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED:
		outcome = OutcomeSucceeded
	case agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED:
		outcome = OutcomeFailed
	default:
		return fmt.Errorf("signal command result %s: outcome %q not supported",
			res.GetCommandId(), res.GetOutcome())
	}
	return s.signalWithStart(ctx, res.GetDeviceId(), CommandResultSignalName, CommandResultSignal{
		DeviceID:  res.GetDeviceId(),
		CommandID: res.GetCommandId(),
		Outcome:   outcome,
		Detail:    res.GetDetail(),
	})
}

// SignalCommandIssued delivers a command to its device's workflow, which records it as the
// pending command and dispatches it to the agent. Its callers arrive with the rollout stage.
func (s *Signaler) SignalCommandIssued(ctx context.Context, cmd CommandIssuedSignal) error {
	return s.signalWithStart(ctx, cmd.DeviceID, CommandIssuedSignalName, cmd)
}

// SignalConfigChanged delivers a versioned configuration snapshot to its device's workflow.
// Its callers arrive with the rollout stage.
func (s *Signaler) SignalConfigChanged(ctx context.Context, change ConfigChangedSignal) error {
	return s.signalWithStart(ctx, change.DeviceID, ConfigChangedSignalName, change)
}

// signalWithStart addresses one signal to a device's workflow, seeding the run chain with the
// device's empty state when there is none yet.
func (s *Signaler) signalWithStart(
	ctx context.Context,
	deviceID, signalName string,
	payload any,
) error {
	if deviceID == "" {
		return fmt.Errorf("signal %s: device id required", signalName)
	}
	workflowID := DeviceWorkflowID(deviceID)
	// The workflow type is its registered name, not the function value: starting by
	// reflection would stamp every execution with the function name the UI shows.
	if _, err := s.client.SignalWithStartWorkflow(ctx, workflowID, signalName, payload,
		client.StartWorkflowOptions{TaskQueue: s.taskQueue},
		DeviceWorkflowName, newDeviceState(deviceID, s.settings),
	); err != nil {
		return fmt.Errorf("signal %s to %s: %w", signalName, workflowID, err)
	}
	return nil
}
