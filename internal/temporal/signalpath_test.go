package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// envSignalClient forwards signal-with-start calls into the test environment's running
// workflow, standing in for the Temporal server: the environment's run is the run chain a
// first signal-with-start lazily creates, and every call delivers its signal to it.
type envSignalClient struct {
	env *testsuite.TestWorkflowEnvironment
}

func (c *envSignalClient) SignalWithStartWorkflow(
	_ context.Context,
	workflowID, signalName string,
	signalArg any,
	_ client.StartWorkflowOptions,
	_ any,
	_ ...any,
) (client.WorkflowRun, error) {
	if workflowID != DeviceWorkflowID("dev-1") {
		return nil, fmt.Errorf("signal %s aimed at %q, want %q",
			signalName, workflowID, DeviceWorkflowID("dev-1"))
	}
	c.env.SignalWorkflow(signalName, signalArg)
	return nil, nil
}

// TestSignalPathEndToEnd runs the composed signal path — signaler, dispatch activity, device
// workflow — through one run: heartbeats move state, an issued command dispatches to the
// agent and is concluded by its result, and duplicate deliveries at the signaler (the seam
// the Report RPC drives) and at the signal channel both produce exactly one state change.
func TestSignalPathEndToEnd(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{}
	env := newDeviceWorkflowEnvWith(NewDispatchActivity(dispatcher), &snapshotRecorder{})
	signaler := NewSignaler(&envSignalClient{env: env}, "fleetops", testSettings())
	rec := devices.Record{ID: "dev-1", Model: "oak-s3", Region: "eu-west"}
	hbTime := time.Unix(1000, 0)

	env.RegisterDelayedCallback(func() {
		hb := &agentv1.Heartbeat{
			EventId: "evt-1", DeviceId: "dev-1", CurrentFw: "fw-1", Ts: timestamppb.New(hbTime),
		}
		if err := signaler.SignalHeartbeat(context.Background(), rec, hb); err != nil {
			t.Errorf("SignalHeartbeat: %v", err)
		}
		// The redelivery an agent retry produces: same event id, second signal.
		if err := signaler.SignalHeartbeat(context.Background(), rec, hb); err != nil {
			t.Errorf("SignalHeartbeat redelivery: %v", err)
		}
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		cmd := CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
		}
		if err := signaler.SignalCommandIssued(context.Background(), cmd); err != nil {
			t.Errorf("SignalCommandIssued: %v", err)
		}
		if err := signaler.SignalCommandIssued(context.Background(), cmd); err != nil {
			t.Errorf("SignalCommandIssued redelivery: %v", err)
		}
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		report := &agentv1.ReportRequest{
			IdempotencyKey: "key-1", CommandId: "cmd-1", DeviceId: "dev-1",
			Outcome: agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		}
		if err := signaler.SignalCommandResult(context.Background(), report); err != nil {
			t.Errorf("SignalCommandResult: %v", err)
		}
		// What Report does with a repeated idempotency key: accepted again, signaled again.
		if err := signaler.SignalCommandResult(context.Background(), report); err != nil {
			t.Errorf("SignalCommandResult redelivery: %v", err)
		}
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// A duplicate at the signal layer for a concluded command: no state change.
		env.SignalWorkflow(CommandResultSignalName, CommandResultSignal{
			DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed,
		})
	}, 4*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		change := ConfigChangedSignal{
			DeviceID: "dev-1", Version: 1, Snapshot: json.RawMessage(`{"interval":"5s"}`),
		}
		if err := signaler.SignalConfigChanged(context.Background(), change); err != nil {
			t.Errorf("SignalConfigChanged: %v", err)
		}
		if err := signaler.SignalConfigChanged(context.Background(), change); err != nil {
			t.Errorf("SignalConfigChanged redelivery: %v", err)
		}
	}, 5*time.Millisecond)
	padToRollover(env, 5)
	env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

	carried := finishRun(t, env)
	want := State{
		DeviceID:        "dev-1",
		Region:          "eu-west",
		Model:           "oak-s3",
		CurrentFw:       "fw-2",
		LastHeartbeatAt: hbTime,
		Config:          ConfigSnapshot{Version: 1, Data: json.RawMessage(`{"interval":"5s"}`)},
		// The success the agent reported, untouched by the duplicate claiming failure.
		LastCommand: &ConcludedCommand{
			Command: CommandIssuedSignal{
				CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
				FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
			},
			Outcome: OutcomeSucceeded,
		},
	}
	if diff := cmp.Diff(want, carried.view()); diff != "" {
		t.Errorf("end-state mismatch (-want +got):\n%s", diff)
	}

	// Exactly one dispatch reached the agent seam, carrying the command as issued.
	if len(dispatcher.sent) != 1 {
		t.Fatalf("agent seam received %d commands, want 1", len(dispatcher.sent))
	}
	wantCommand := &agentv1.Command{
		CommandId: "cmd-1", DeviceId: "dev-1",
		Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
			FirmwareId: "fw-2", Version: "fw-2", Checksum: "sum",
		}},
	}
	if diff := cmp.Diff(wantCommand, dispatcher.sent[0].(*agentv1.Command), protocmp.Transform()); diff != "" {
		t.Errorf("dispatched command mismatch (-want +got):\n%s", diff)
	}
}
