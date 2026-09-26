package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// errDeviceNotConnected stands in for the dispatcher's not-connected failure.
var errDeviceNotConnected = errors.New("device not connected")

// fakeDispatcher is a hand-written CommandDispatcher double recording what it sends.
type fakeDispatcher struct {
	sent []proto.Message
	err  error
}

func (d *fakeDispatcher) Send(_ context.Context, cmd *agentv1.Command) error {
	d.sent = append(d.sent, cmd)
	return d.err
}

func TestBuildCommand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cmd  CommandIssuedSignal
		want *agentv1.Command
	}{
		{
			name: "update command carries the firmware targets",
			cmd: CommandIssuedSignal{
				CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
				FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
			},
			want: &agentv1.Command{
				CommandId: "cmd-1", DeviceId: "dev-1",
				Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
					FirmwareId: "fw-2", Version: "fw-2", Checksum: "sum",
				}},
			},
		},
		{
			name: "abort command carries its reason",
			cmd: CommandIssuedSignal{
				CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
			},
			want: &agentv1.Command{
				CommandId: "cmd-2", DeviceId: "dev-1",
				Kind: &agentv1.Command_AbortUpdate{AbortUpdate: &agentv1.AbortUpdate{Reason: "stop"}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dispatcher := &fakeDispatcher{}
			if err := NewDispatchActivity(dispatcher)(context.Background(), tc.cmd); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			if len(dispatcher.sent) != 1 {
				t.Fatalf("dispatcher received %d commands, want 1", len(dispatcher.sent))
			}
			got, ok := dispatcher.sent[0].(*agentv1.Command)
			if !ok {
				t.Fatalf("dispatched %T, want *agentv1.Command", dispatcher.sent[0])
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("dispatched command mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDispatchInvalidCommand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cmd  CommandIssuedSignal
	}{
		{
			name: "missing command id",
			cmd:  CommandIssuedSignal{DeviceID: "dev-1", Kind: CommandKindAbort},
		},
		{
			name: "missing device id",
			cmd:  CommandIssuedSignal{CommandID: "cmd-1", Kind: CommandKindAbort},
		},
		{
			name: "update without version",
			cmd: CommandIssuedSignal{
				CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate, FirmwareID: "fw-2",
			},
		},
		{
			name: "unknown kind",
			cmd:  CommandIssuedSignal{CommandID: "cmd-1", DeviceID: "dev-1", Kind: "reboot"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dispatcher := &fakeDispatcher{}
			err := NewDispatchActivity(dispatcher)(context.Background(), tc.cmd)

			// A malformed command is wrong forever: it must come back as the error type the
			// retry policy refuses to retry, and it must dispatch nothing.
			var appErr *temporal.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("dispatch error = %T (%v), want *temporal.ApplicationError", err, err)
			}
			if appErr.Type() != invalidCommandErrorType {
				t.Errorf("dispatch error type = %q, want %q", appErr.Type(), invalidCommandErrorType)
			}
			if len(dispatcher.sent) != 0 {
				t.Errorf("dispatcher received %d commands, want none", len(dispatcher.sent))
			}
		})
	}
}

func TestDispatchErrorSurfaces(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{err: errDeviceNotConnected}
	cmd := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
	}
	err := NewDispatchActivity(dispatcher)(context.Background(), cmd)
	if !errors.Is(err, errDeviceNotConnected) {
		t.Errorf("dispatch error = %v, want it to wrap the dispatcher error", err)
	}
}

// TestWorkflowRetriesUndeliveredCommand pins the retry path: a device that is not connected
// yet keeps the activity retrying until delivery succeeds, and the pending command is
// dispatched exactly through the same command id.
func TestWorkflowRetriesUndeliveredCommand(t *testing.T) {
	t.Parallel()

	rec := &dispatchRecorder{
		failures: map[string]int{"cmd-1": 1},
		fail:     errors.New("device not connected"),
	}
	env := newDeviceWorkflowEnv(rec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandIssuedSignalName, CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-2", Version: "fw-2",
		})
	}, time.Millisecond)
	// Spacing the fillers past the retry's initial interval lets the retried delivery land
	// before the rollover.
	for i := 1; i < maxSignalsPerRun; i++ {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{EventID: "filler"})
		}, time.Duration(i)*10*time.Millisecond)
	}
	env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1"))

	carried := finishRun(t, env)
	if carried.Pending == nil || !carried.Pending.Dispatched {
		t.Fatalf("pending command = %+v, want dispatched after a retry", carried.Pending)
	}
	if got := len(rec.recorded()); got != 2 {
		t.Errorf("dispatch attempts = %d, want 2 (one failure, one retry)", got)
	}
}

// TestWorkflowSupersededDispatchIsHarmless pins the supersede path: the superseded command's
// delivery may never land, and that must not disturb the newer command or the state.
func TestWorkflowSupersededDispatchIsHarmless(t *testing.T) {
	t.Parallel()

	rec := &dispatchRecorder{
		failures: map[string]int{"cmd-1": maxSignalsPerRun},
		fail:     errors.New("device not connected"),
	}
	first := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
	second := CommandIssuedSignal{
		CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-3", Version: "fw-3",
	}
	carried := runDevice(t, newDeviceState("dev-1"), rec,
		testSignal{CommandIssuedSignalName, first},
		testSignal{CommandIssuedSignalName, second},
		testSignal{CommandResultSignalName, CommandResultSignal{
			DeviceID: "dev-1", CommandID: "cmd-2", Outcome: OutcomeSucceeded,
		}},
		testSignal{CommandResultSignalName, CommandResultSignal{
			DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed,
		}},
	)

	want := State{DeviceID: "dev-1", CurrentFw: "fw-3"}
	if diff := cmp.Diff(want, carried.view()); diff != "" {
		t.Errorf("state mismatch (-want +got):\n%s", diff)
	}
	names := rec.recorded()
	if len(names) < 2 || names[0].CommandID != "cmd-1" || names[1].CommandID != "cmd-2" {
		t.Errorf("dispatches = %+v, want cmd-1 then cmd-2", names)
	}
}

// TestWorkflowDoesNotRetryInvalidCommand pins the non-retryable wiring end to end: the real
// dispatch activity rejects a malformed command with the error type the retry policy refuses
// to retry, so the workflow attempts it exactly once and leaves the command undispatched.
func TestWorkflowDoesNotRetryInvalidCommand(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{}
	var mu sync.Mutex
	attempts := 0
	real := NewDispatchActivity(dispatcher)
	env := newDeviceWorkflowEnvWith(func(ctx context.Context, cmd CommandIssuedSignal) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return real(ctx, cmd)
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandIssuedSignalName, CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: "reboot",
		})
	}, time.Millisecond)
	padToRollover(env, 1)
	env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1"))

	carried := finishRun(t, env)
	if carried.Pending == nil || carried.Pending.Dispatched {
		t.Fatalf("pending command = %+v, want undispatched after a permanent failure", carried.Pending)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Errorf("dispatch attempts = %d, want 1 — a permanent failure must not retry", attempts)
	}
	if len(dispatcher.sent) != 0 {
		t.Errorf("dispatcher received %d commands, want none", len(dispatcher.sent))
	}
}
