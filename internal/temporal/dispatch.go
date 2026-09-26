package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/temporal"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// invalidCommandErrorType is the Temporal error type of a command that can never dispatch.
// The caller's retry policy lists it as non-retryable, so a malformed command fails visibly
// instead of retrying forever.
const invalidCommandErrorType = "invalid_command"

// CommandDispatcher delivers one command to its target device's agent connection.
// *agentserver.Hub satisfies it.
type CommandDispatcher interface {
	// Send dispatches one command to its target device's live stream.
	Send(ctx context.Context, cmd *agentv1.Command) error
}

// NewDispatchActivity returns the dispatch-command activity bound to dispatcher. Delivery is
// at-least-once and keyed by command id: a retry — or a delivery that outlived its command —
// repeats the same command id, whose only observable effect is the report the workflow
// already deduplicates. The activity carries no retry policy of its own; its caller supplies
// one.
func NewDispatchActivity(
	dispatcher CommandDispatcher,
) func(ctx context.Context, cmd CommandIssuedSignal) error {
	return func(ctx context.Context, cmd CommandIssuedSignal) error {
		out, err := buildCommand(cmd)
		if err != nil {
			return temporal.NewApplicationError(err.Error(), invalidCommandErrorType)
		}
		if err := dispatcher.Send(ctx, out); err != nil {
			return fmt.Errorf("dispatch command %s: %w", cmd.CommandID, err)
		}
		return nil
	}
}

// buildCommand maps an issued command onto the wire contract.
func buildCommand(cmd CommandIssuedSignal) (*agentv1.Command, error) {
	if cmd.CommandID == "" || cmd.DeviceID == "" {
		return nil, fmt.Errorf("build command: command id and device id required")
	}
	switch cmd.Kind {
	case CommandKindUpdate:
		if cmd.FirmwareID == "" || cmd.Version == "" {
			return nil, fmt.Errorf("build command %s: firmware id and version required", cmd.CommandID)
		}
		return &agentv1.Command{
			CommandId: cmd.CommandID,
			DeviceId:  cmd.DeviceID,
			Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
				FirmwareId: cmd.FirmwareID,
				Version:    cmd.Version,
				Checksum:   cmd.Checksum,
			}},
		}, nil
	case CommandKindAbort:
		return &agentv1.Command{
			CommandId: cmd.CommandID,
			DeviceId:  cmd.DeviceID,
			Kind: &agentv1.Command_AbortUpdate{AbortUpdate: &agentv1.AbortUpdate{
				Reason: cmd.Reason,
			}},
		}, nil
	default:
		return nil, fmt.Errorf("build command %s: unknown kind %q", cmd.CommandID, cmd.Kind)
	}
}
