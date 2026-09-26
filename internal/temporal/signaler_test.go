package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/sdk/client"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// fakeSignalClient is a hand-written signalClient double recording signal-with-start calls.
type fakeSignalClient struct {
	mu    sync.Mutex
	calls []signalCall
	err   error
}

// signalCall is one recorded signal-with-start.
type signalCall struct {
	workflowID   string
	workflowType any
	signalName   string
	signalArg    any
	options      client.StartWorkflowOptions
	workflowArg  any
}

func (c *fakeSignalClient) SignalWithStartWorkflow(
	_ context.Context,
	workflowID string,
	signalName string,
	signalArg any,
	options client.StartWorkflowOptions,
	workflowType any,
	workflowArgs ...any,
) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	var seed any
	if len(workflowArgs) > 0 {
		seed = workflowArgs[0]
	}
	c.calls = append(c.calls, signalCall{
		workflowID:   workflowID,
		workflowType: workflowType,
		signalName:   signalName,
		signalArg:    signalArg,
		options:      options,
		workflowArg:  seed,
	})
	return nil, nil
}

func (c *fakeSignalClient) recorded() []signalCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]signalCall(nil), c.calls...)
}

func TestSignalerDeliversOneSignalPerCall(t *testing.T) {
	t.Parallel()

	fc := &fakeSignalClient{}
	signaler := NewSignaler(fc, "fleetops", testSettings())
	hbTime := time.Unix(1000, 0)
	rec := devices.Record{ID: "dev-1", Model: "oak-s3", Region: "eu-west"}

	if err := signaler.SignalHeartbeat(context.Background(), rec, &agentv1.Heartbeat{
		EventId: "evt-1", DeviceId: "dev-1", CurrentFw: "fw-1", Ts: timestamppb.New(hbTime),
	}); err != nil {
		t.Fatalf("SignalHeartbeat: %v", err)
	}
	if err := signaler.SignalCommandIssued(context.Background(), CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}); err != nil {
		t.Fatalf("SignalCommandIssued: %v", err)
	}
	if err := signaler.SignalCommandResult(context.Background(), &agentv1.ReportRequest{
		IdempotencyKey: "key-1", CommandId: "cmd-1", DeviceId: "dev-1",
		Outcome: agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
	}); err != nil {
		t.Fatalf("SignalCommandResult: %v", err)
	}
	if err := signaler.SignalConfigChanged(context.Background(), ConfigChangedSignal{
		DeviceID: "dev-1", Version: 3, Snapshot: []byte(`{"a":1}`),
	}); err != nil {
		t.Fatalf("SignalConfigChanged: %v", err)
	}

	calls := fc.recorded()
	if len(calls) != 4 {
		t.Fatalf("signal-with-start calls = %d, want one per signal", len(calls))
	}
	wantNames := []string{
		HeartbeatSignalName, CommandIssuedSignalName, CommandResultSignalName, ConfigChangedSignalName,
	}
	for i, call := range calls {
		if call.workflowID != "device-dev-1" {
			t.Errorf("call %d workflow id = %q, want device-dev-1", i, call.workflowID)
		}
		// The run chain starts under the workflow's registered name: starting by function
		// reflection would stamp executions with a name the UI must not show.
		if call.workflowType != DeviceWorkflowName {
			t.Errorf("call %d workflow type = %v, want %q", i, call.workflowType, DeviceWorkflowName)
		}
		if call.signalName != wantNames[i] {
			t.Errorf("call %d signal name = %q, want %q", i, call.signalName, wantNames[i])
		}
		if call.options.TaskQueue != "fleetops" {
			t.Errorf("call %d task queue = %q, want fleetops", i, call.options.TaskQueue)
		}
		// The start argument is the seed of a lazily created run chain: the empty state of
		// the device under the configured entity settings.
		seed, ok := call.workflowArg.(deviceState)
		if !ok || seed.validate() != nil || seed.DeviceID != "dev-1" || seed.Settings != testSettings() {
			t.Errorf("call %d start argument = %+v, want the empty state of dev-1 under the settings",
				i, call.workflowArg)
		}
	}

	if diff := cmp.Diff(HeartbeatSignal{
		EventID: "evt-1", DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
		CurrentFw: "fw-1", Timestamp: hbTime,
	}, calls[0].signalArg); diff != "" {
		t.Errorf("heartbeat payload mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}, calls[1].signalArg); diff != "" {
		t.Errorf("command payload mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(CommandResultSignal{
		DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
	}, calls[2].signalArg); diff != "" {
		t.Errorf("result payload mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(ConfigChangedSignal{
		DeviceID: "dev-1", Version: 3, Snapshot: []byte(`{"a":1}`),
	}, calls[3].signalArg); diff != "" {
		t.Errorf("config payload mismatch (-want +got):\n%s", diff)
	}
}

func TestSignalerSignalWithStartErrors(t *testing.T) {
	t.Parallel()

	t.Run("missing device id signals nothing", func(t *testing.T) {
		t.Parallel()
		fc := &fakeSignalClient{}
		err := NewSignaler(fc, "fleetops", testSettings()).SignalCommandIssued(
			context.Background(), CommandIssuedSignal{CommandID: "cmd-1"})
		if err == nil {
			t.Fatal("SignalCommandIssued without device id returned nil")
		}
		if got := fc.recorded(); len(got) != 0 {
			t.Errorf("signal-with-start calls = %d, want none", len(got))
		}
	})

	t.Run("unsupported outcome signals nothing", func(t *testing.T) {
		t.Parallel()
		fc := &fakeSignalClient{}
		err := NewSignaler(fc, "fleetops", testSettings()).SignalCommandResult(
			context.Background(), &agentv1.ReportRequest{
				IdempotencyKey: "key-1", CommandId: "cmd-1", DeviceId: "dev-1",
			})
		if err == nil {
			t.Fatal("SignalCommandResult with an unspecified outcome returned nil")
		}
		if got := fc.recorded(); len(got) != 0 {
			t.Errorf("signal-with-start calls = %d, want none", len(got))
		}
	})

	t.Run("client error is wrapped", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("temporal is down")
		signaler := NewSignaler(&fakeSignalClient{err: boom}, "fleetops", testSettings())
		err := signaler.SignalHeartbeat(context.Background(), devices.Record{ID: "dev-1"},
			&agentv1.Heartbeat{EventId: "evt-1", Ts: timestamppb.New(time.Unix(1, 0))})
		if !errors.Is(err, boom) {
			t.Errorf("SignalHeartbeat error = %v, want it wrapped", err)
		}
	})
}
