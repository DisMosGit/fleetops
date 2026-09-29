package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// testSignal is one queued signal delivery: its signal name and payload. Fields are
// exported because a workflow's arguments round-trip through serialization.
type testSignal struct {
	Name    string
	Payload any
}

// dispatchRecorder stands in for the dispatch activity's side effect: it records every
// dispatch the workflow performs and can fail chosen commands a scripted number of times to
// exercise retry and supersede behavior.
type dispatchRecorder struct {
	mu       sync.Mutex
	calls    []CommandIssuedSignal
	failures map[string]int // remaining failures per command id
	fail     error          // error returned while a failure remains
}

// run is the recorded dispatch activity.
func (d *dispatchRecorder) run(_ context.Context, cmd CommandIssuedSignal) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, cmd)
	if d.failures[cmd.CommandID] > 0 {
		d.failures[cmd.CommandID]--
		return d.fail
	}
	return nil
}

// recorded returns the dispatches performed so far.
func (d *dispatchRecorder) recorded() []CommandIssuedSignal {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]CommandIssuedSignal(nil), d.calls...)
}

// errSnapshotWrite stands in for the snapshot store's write failure.
var errSnapshotWrite = errors.New("mongo is down")

// snapshotRecorder stands in for the snapshot activity's side effect: it records every
// snapshot the workflow persists and can fail chosen writes to exercise failure isolation.
type snapshotRecorder struct {
	mu       sync.Mutex
	saved    []Snapshot
	failures int // remaining failures
}

// run is the recorded snapshot activity.
func (s *snapshotRecorder) run(_ context.Context, snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		return errSnapshotWrite
	}
	s.saved = append(s.saved, snap)
	return nil
}

// recorded returns the snapshots persisted so far.
func (s *snapshotRecorder) recorded() []Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Snapshot(nil), s.saved...)
}

// newDeviceWorkflowEnv returns a test environment hosting DeviceWorkflow under its
// registered name and its two activities — dispatch-command and snapshot-device-state —
// backed by rec and snaps.
func newDeviceWorkflowEnv(rec *dispatchRecorder, snaps *snapshotRecorder) *testsuite.TestWorkflowEnvironment {
	return newDeviceWorkflowEnvWith(rec.run, snaps)
}

// newDeviceWorkflowEnvWith is newDeviceWorkflowEnv with a caller-supplied dispatch activity
// implementation. The suite logger is discarded: the test clock auto-fires a timer per queued
// signal, and that debug noise drowns the test output.
func newDeviceWorkflowEnvWith(
	dispatch func(context.Context, CommandIssuedSignal) error,
	snaps *snapshotRecorder,
) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(discardLogger{})
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DeviceWorkflow, workflow.RegisterOptions{Name: DeviceWorkflowName})
	env.RegisterActivityWithOptions(dispatch, activity.RegisterOptions{Name: DispatchActivityName})
	env.RegisterActivityWithOptions(snaps.run, activity.RegisterOptions{Name: SnapshotActivityName})
	return env
}

// discardLogger swallows the SDK's structured test logs.
type discardLogger struct{}

func (discardLogger) Debug(string, ...any) {}
func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}

// queueSignals schedules the given signal deliveries in order, one per virtual millisecond.
func queueSignals(env *testsuite.TestWorkflowEnvironment, signals ...testSignal) {
	for i, s := range signals {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(s.Name, s.Payload)
		}, time.Duration(i+1)*time.Millisecond)
	}
}

// padToRollover schedules filler heartbeats so the run processes exactly maxSignalsPerRun
// signals and rolls over. A filler is a structural no-op: zero timestamp never applies, and
// one repeated event id keeps the dedup window from growing.
func padToRollover(env *testsuite.TestWorkflowEnvironment, queued int) {
	for i := queued; i < maxSignalsPerRun; i++ {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{EventID: "filler"})
		}, time.Duration(i+1)*time.Millisecond)
	}
}

// runDevice executes one device workflow run delivering signals in order and then filler
// heartbeats up to the rollover threshold, and returns the state the run carried over.
func runDevice(
	t *testing.T,
	input deviceState,
	rec *dispatchRecorder,
	snaps *snapshotRecorder,
	signals ...testSignal,
) deviceState {
	t.Helper()
	env := newDeviceWorkflowEnv(rec, snaps)
	queueSignals(env, signals...)
	padToRollover(env, len(signals))
	env.ExecuteWorkflow(DeviceWorkflow, input)
	return finishRun(t, env)
}

// finishRun asserts the run rolled over and returns the state it carried over.
func finishRun(t *testing.T, env *testsuite.TestWorkflowEnvironment) deviceState {
	t.Helper()
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow run did not complete")
	}
	var canErr *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowResult(nil), &canErr) {
		t.Fatalf("run ended with %v, want ContinueAsNewError", env.GetWorkflowResult(nil))
	}
	var carried deviceState
	if err := converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &carried); err != nil {
		t.Fatalf("decode carried state: %v", err)
	}
	return carried
}

func TestDeviceWorkflowStateQuery(t *testing.T) {
	t.Parallel()

	// The update the query's run concludes: its recorded outcome is what a rollout's
	// waiting update activity reads back through this query.
	update := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
	}

	var queried State
	env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
			EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandIssuedSignalName, update)
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandResultSignalName, CommandResultSignal{
			DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "flash error",
		})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ConfigChangedSignalName, ConfigChangedSignal{
			DeviceID: "dev-1", Version: 1, Snapshot: json.RawMessage(`{"interval":"5s"}`),
		})
	}, 4*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(GetStateQueryType)
		if err != nil {
			t.Errorf("query state: %v", err)
			return
		}
		if err := value.Get(&queried); err != nil {
			t.Errorf("decode state query: %v", err)
		}
	}, 5*time.Millisecond)
	padToRollover(env, 4)
	env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

	want := State{
		DeviceID:        "dev-1",
		CurrentFw:       "fw-1",
		LastHeartbeatAt: time.Unix(1000, 0),
		Config:          ConfigSnapshot{Version: 1, Data: json.RawMessage(`{"interval":"5s"}`)},
		LastCommand: &ConcludedCommand{
			Command: update, Outcome: OutcomeFailed, Detail: "flash error",
		},
	}
	if diff := cmp.Diff(want, queried); diff != "" {
		t.Errorf("state query mismatch (-want +got):\n%s", diff)
	}
	// The concluded command is device state, not run bookkeeping: it travels with the
	// entity across its rolling continuation.
	if diff := cmp.Diff(want, finishRun(t, env).view()); diff != "" {
		t.Errorf("carried state mismatch (-want +got):\n%s", diff)
	}
}

// TestDeviceWorkflowPreviousFirmware pins what a rollback reads from the entity: the version the
// device ran before its current one is recorded when its firmware changes, is readable through the
// state query, is untouched by a redelivered conclusion, and travels with the entity across its
// rolling continuation.
func TestDeviceWorkflowPreviousFirmware(t *testing.T) {
	t.Parallel()

	update := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
	}
	result := CommandResultSignal{DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded}

	var queried State
	env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
			EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandIssuedSignalName, update)
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(CommandResultSignalName, result)
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// A redelivery of the conclusion that moved the device: the command is no longer
		// pending, so it concludes nothing and the recorded history stands.
		env.SignalWorkflow(CommandResultSignalName, result)
	}, 4*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(GetStateQueryType)
		if err != nil {
			t.Errorf("query state: %v", err)
			return
		}
		if err := value.Get(&queried); err != nil {
			t.Errorf("decode state query: %v", err)
		}
	}, 5*time.Millisecond)
	// Four signals were delivered above; the rollover is padded from there. The query is not
	// a signal and does not count towards the run's history budget.
	padToRollover(env, 4)
	env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

	want := State{
		DeviceID:        "dev-1",
		CurrentFw:       "fw-2",
		PreviousFw:      "fw-1",
		LastHeartbeatAt: time.Unix(1000, 0),
		LastCommand:     &ConcludedCommand{Command: update, Outcome: OutcomeSucceeded},
	}
	if diff := cmp.Diff(want, queried); diff != "" {
		t.Errorf("state query mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, finishRun(t, env).view()); diff != "" {
		t.Errorf("carried state mismatch (-want +got):\n%s", diff)
	}
}

func TestDeviceWorkflowSignalIdempotency(t *testing.T) {
	t.Parallel()

	hb := HeartbeatSignal{
		EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
	}
	staleHB := HeartbeatSignal{
		EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-9", Timestamp: time.Unix(999, 0),
	}
	cmd := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
	result := CommandResultSignal{DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded}
	config := ConfigChangedSignal{
		DeviceID: "dev-1", Version: 2, Snapshot: json.RawMessage(`{"a":1}`),
	}

	cases := []struct {
		name       string
		first      []testSignal
		duplicates []testSignal
	}{
		{
			name:       "duplicate heartbeat is dropped",
			first:      []testSignal{{HeartbeatSignalName, hb}},
			duplicates: []testSignal{{HeartbeatSignalName, hb}},
		},
		{
			name:       "stale heartbeat changes nothing",
			first:      []testSignal{{HeartbeatSignalName, hb}},
			duplicates: []testSignal{{HeartbeatSignalName, staleHB}},
		},
		{
			name: "duplicate command result is dropped",
			first: []testSignal{
				{CommandIssuedSignalName, cmd},
				{CommandResultSignalName, result},
			},
			duplicates: []testSignal{{CommandResultSignalName, result}},
		},
		{
			name:       "duplicate command_issued for the pending command is a no-op",
			first:      []testSignal{{CommandIssuedSignalName, cmd}},
			duplicates: []testSignal{{CommandIssuedSignalName, cmd}},
		},
		{
			name:  "older or repeated configuration version is ignored",
			first: []testSignal{{ConfigChangedSignalName, config}},
			duplicates: []testSignal{
				{ConfigChangedSignalName, config},
				{ConfigChangedSignalName, ConfigChangedSignal{
					DeviceID: "dev-1", Version: 1, Snapshot: json.RawMessage(`{"b":2}`),
				}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clean := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, &snapshotRecorder{}, tc.first...)
			noisy := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, &snapshotRecorder{},
				append(append([]testSignal{}, tc.first...), tc.duplicates...)...)
			if diff := cmp.Diff(clean.view(), noisy.view()); diff != "" {
				t.Errorf("duplicates changed device state (-without +with duplicates):\n%s", diff)
			}
		})
	}
}

func TestDeviceWorkflowCommandLifecycle(t *testing.T) {
	t.Parallel()

	update := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
	}
	update3 := CommandIssuedSignal{
		CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-3", Version: "fw-3",
	}
	heartbeat := testSignal{HeartbeatSignalName, HeartbeatSignal{
		EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
	}}

	t.Run("issued command becomes pending and dispatches", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{}, testSignal{CommandIssuedSignalName, update})

		want := State{DeviceID: "dev-1", Pending: &PendingCommand{Command: update, Dispatched: true}}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]CommandIssuedSignal{update}, rec.recorded()); diff != "" {
			t.Errorf("dispatches mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("newer command supersedes the pending one", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			testSignal{CommandIssuedSignalName, update},
			testSignal{CommandIssuedSignalName, update3},
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
			}},
		)

		// The superseded command's result must not conclude the newer one, and firmware
		// adoption belongs to the command that concluded.
		want := State{DeviceID: "dev-1", Pending: &PendingCommand{Command: update3, Dispatched: true}}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("success adopts the commanded firmware", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			heartbeat,
			testSignal{CommandIssuedSignalName, update},
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
			}},
		)

		want := State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-2",
			PreviousFw:      "fw-1",
			LastHeartbeatAt: time.Unix(1000, 0),
			LastCommand:     &ConcludedCommand{Command: update, Outcome: OutcomeSucceeded},
		}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("failure leaves firmware unchanged", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			heartbeat,
			testSignal{CommandIssuedSignalName, update},
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "flash error",
			}},
		)

		want := State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-1",
			LastHeartbeatAt: time.Unix(1000, 0),
			LastCommand: &ConcludedCommand{
				Command: update, Outcome: OutcomeFailed, Detail: "flash error",
			},
		}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a redelivered result leaves the concluded command alone", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		result := testSignal{CommandResultSignalName, CommandResultSignal{
			DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "flash error",
		}}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			heartbeat,
			testSignal{CommandIssuedSignalName, update},
			result,
			// The same delivery again, and a later one claiming success: neither can
			// reach the concluded command, which is what a caller reading its result
			// for a dispatched command depends on.
			result,
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
			}},
		)

		want := State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-1",
			LastHeartbeatAt: time.Unix(1000, 0),
			LastCommand: &ConcludedCommand{
				Command: update, Outcome: OutcomeFailed, Detail: "flash error",
			},
		}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("result for a non-pending command changes nothing", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		carried := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			testSignal{CommandIssuedSignalName, update},
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-99", Outcome: OutcomeFailed,
			}},
		)

		want := State{DeviceID: "dev-1", Pending: &PendingCommand{Command: update, Dispatched: true}}
		if diff := cmp.Diff(want, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDeviceWorkflowRollingContinuation(t *testing.T) {
	t.Parallel()

	heartbeat := testSignal{HeartbeatSignalName, HeartbeatSignal{
		EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
	}}
	update := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}

	t.Run("state and dedup memory survive the rollover", func(t *testing.T) {
		t.Parallel()
		rec := &dispatchRecorder{}
		first := runDevice(t, newDeviceState("dev-1", testSettings()), rec, &snapshotRecorder{},
			heartbeat, testSignal{CommandIssuedSignalName, update})

		if !first.RecentEventIDs.has("evt-1") || !first.RecentCommandIDs.has("cmd-1") {
			t.Fatalf("carry-over lost dedup memory: events %v, commands %v",
				first.RecentEventIDs.Keys, first.RecentCommandIDs.Keys)
		}
		if first.SignalsApplied != 0 {
			t.Errorf("SignalsApplied carried out of the run = %d, want 0", first.SignalsApplied)
		}

		second := runDevice(t, first, &dispatchRecorder{}, &snapshotRecorder{})
		if diff := cmp.Diff(first.view(), second.view()); diff != "" {
			t.Errorf("rollover changed device state (-before +after):\n%s", diff)
		}
		if diff := cmp.Diff(first.RecentEventIDs, second.RecentEventIDs); diff != "" {
			t.Errorf("event dedup memory mismatch (-before +after):\n%s", diff)
		}
		if diff := cmp.Diff(first.RecentCommandIDs, second.RecentCommandIDs); diff != "" {
			t.Errorf("command dedup memory mismatch (-before +after):\n%s", diff)
		}
	})

	t.Run("duplicate signals across a continuation are dropped", func(t *testing.T) {
		t.Parallel()
		first := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, &snapshotRecorder{}, heartbeat)

		// The redelivered heartbeat reuses its event id but claims a newer timestamp: only
		// the carried dedup memory can drop it. The fresh one after it must still apply.
		second := runDevice(t, first, &dispatchRecorder{}, &snapshotRecorder{},
			testSignal{HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(9000, 0),
			}},
			testSignal{HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
			}},
		)

		want := State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-1",
			LastHeartbeatAt: time.Unix(2000, 0),
		}
		if diff := cmp.Diff(want, second.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("signals delivered around the rollover are applied exactly once", func(t *testing.T) {
		t.Parallel()
		// The last real signal of a run and its redelivery to the continuing run — what an
		// at-least-once producer does around a rollover — must total one application.
		first := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, &snapshotRecorder{}, heartbeat)
		second := runDevice(t, first, &dispatchRecorder{}, &snapshotRecorder{}, heartbeat)

		want := State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-1",
			LastHeartbeatAt: time.Unix(1000, 0),
		}
		if diff := cmp.Diff(want, second.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("unknown carry version fails loudly", func(t *testing.T) {
		t.Parallel()
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
		env.ExecuteWorkflow(DeviceWorkflow, deviceState{CarryVersion: carryVersion + 1, DeviceID: "dev-1"})
		err := env.GetWorkflowResult(nil)
		if err == nil {
			t.Fatal("run at an unknown carry version completed without error")
		}
		// A workflow failure crosses a serialization boundary, so the sentinel's identity
		// does not survive — what must survive is the loud, self-explaining failure.
		if !strings.Contains(err.Error(), ErrUnsupportedCarryVersion.Error()) {
			t.Errorf("run error = %v, want it to name %q", err, ErrUnsupportedCarryVersion)
		}
	})
}

// bufferedSignalsWorkflow buffers a fixed burst of signals on the entity's signal channels
// without processing any of them, then drains them into one state and returns it — the exact
// handoff the rolling continuation performs before it continues as new.
func bufferedSignalsWorkflow(ctx workflow.Context) (deviceState, error) {
	heartbeat, issued, result, config, update := workflow.NewBufferedChannel(ctx, 8),
		workflow.NewBufferedChannel(ctx, 8),
		workflow.NewBufferedChannel(ctx, 8),
		workflow.NewBufferedChannel(ctx, 8),
		workflow.NewBufferedChannel(ctx, 8)
	heartbeat.Send(ctx, HeartbeatSignal{
		EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(1000, 0),
	})
	heartbeat.Send(ctx, HeartbeatSignal{
		EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: time.Unix(2000, 0),
	})
	heartbeat.Send(ctx, HeartbeatSignal{
		EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: time.Unix(2000, 0),
	})
	issued.Send(ctx, CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-3", Version: "fw-3",
	})
	config.Send(ctx, ConfigChangedSignal{
		DeviceID: "dev-1", Version: 7, Snapshot: json.RawMessage(`{"a":1}`),
	})
	update.Send(ctx, UpdateStatusSignal{
		DeviceID: "dev-1", FirmwareID: "fw-3",
		Phase: PhaseFailed, ProgressPercent: 20, Detail: "checksum mismatch",
	})
	chans := signalChannels{heartbeat: heartbeat, issued: issued, result: result, config: config, update: update}
	state := newDeviceState("dev-1", testSettings())
	chans.drainAll(&state)
	return state, nil
}

// TestDrainAllAppliesBufferedSignals pins the drain before a continuation: signals sitting
// buffered on the channels when a run rolls over die with the run unless drainAll folds them
// into the carried state first. (The test environment delivers each signal as its own
// workflow task, so the buffered handoff is exercised here rather than through the loop.)
func TestDrainAllAppliesBufferedSignals(t *testing.T) {
	t.Parallel()

	env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
	env.RegisterWorkflow(bufferedSignalsWorkflow)
	env.ExecuteWorkflow(bufferedSignalsWorkflow)

	var got deviceState
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("drain workflow: %v", err)
	}
	want := State{
		DeviceID:        "dev-1",
		CurrentFw:       "fw-2",
		PreviousFw:      "fw-1",
		LastHeartbeatAt: time.Unix(2000, 0),
		Pending: &PendingCommand{Command: CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-3", Version: "fw-3",
		}},
		Update: &UpdateStatus{
			FirmwareID: "fw-3", Phase: PhaseFailed, ProgressPercent: 20, Detail: "checksum mismatch",
		},
		Config: ConfigSnapshot{Version: 7, Data: json.RawMessage(`{"a":1}`)},
	}
	if diff := cmp.Diff(want, got.view()); diff != "" {
		t.Errorf("drained state mismatch (-want +got):\n%s", diff)
	}
	if !got.RecentEventIDs.has("evt-2") {
		t.Error("drained state lost the dedup memory of the buffered signals")
	}
}
