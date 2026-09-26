package temporal

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// wantAttrs builds the exact search-attribute collection a run must upsert for the given
// mirrored values.
func wantAttrs(region, model, fw string, online bool) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(
		temporal.NewSearchAttributeKeyKeyword(SearchAttrDeviceRegion).ValueSet(region),
		temporal.NewSearchAttributeKeyKeyword(SearchAttrDeviceModel).ValueSet(model),
		temporal.NewSearchAttributeKeyKeyword(SearchAttrDeviceFirmware).ValueSet(fw),
		temporal.NewSearchAttributeKeyBool(SearchAttrDeviceOnline).ValueSet(online),
	)
}

// padSpaced schedules filler heartbeats far enough apart that virtual time marches on through
// the run — which is what lets timers and liveness judgements fire before the rollover.
func padSpaced(env *testsuite.TestWorkflowEnvironment, queued int, spacing time.Duration) {
	for i := queued; i < maxSignalsPerRun; i++ {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{EventID: "filler"})
		}, time.Duration(i+1)*spacing)
	}
}

// TestDeviceWorkflowSearchAttributes pins when the entity upserts its search attributes and
// with which values: the complete map at run start, then only when a mirrored value changes.
// Every test installs exact upsert expectations — an extra or missing upsert fails the test.
func TestDeviceWorkflowSearchAttributes(t *testing.T) {
	t.Parallel()

	t.Run("run start upserts the full map and routine heartbeats add nothing", func(t *testing.T) {
		t.Parallel()
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
		env.OnUpsertTypedSearchAttributes(wantAttrs("", "", "", false)).Once()
		env.RegisterDelayedCallback(func() {
			// Timestamp only: no mirrored value changes.
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", Timestamp: time.Unix(2000, 0),
			})
		}, time.Millisecond)
		padToRollover(env, 1)
		env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

		finishRun(t, env)
		env.AssertExpectations(t)
	})

	t.Run("firmware change upserts the changed map", func(t *testing.T) {
		t.Parallel()
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
		env.OnUpsertTypedSearchAttributes(wantAttrs("", "", "", false)).Once()
		env.OnUpsertTypedSearchAttributes(wantAttrs("", "", "fw-1", false)).Once()
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
			})
		}, time.Millisecond)
		padToRollover(env, 1)
		env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

		finishRun(t, env)
		env.AssertExpectations(t)
	})

	t.Run("each liveness flip upserts the changed map", func(t *testing.T) {
		t.Parallel()
		base := time.Unix(1000, 0)
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
		env.SetStartTime(base)
		env.OnUpsertTypedSearchAttributes(wantAttrs("", "", "", false)).Once()
		env.OnUpsertTypedSearchAttributes(wantAttrs("eu-west", "oak-s3", "fw-1", true)).Once()
		env.OnUpsertTypedSearchAttributes(wantAttrs("eu-west", "oak-s3", "fw-1", false)).Once()

		state := newDeviceState("dev-1", DeviceSettings{
			SnapshotInterval: time.Hour, OfflineThreshold: 10 * time.Second,
		})
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
				CurrentFw: "fw-1", Timestamp: base.Add(time.Second),
			})
		}, time.Millisecond)
		// Fillers keep decisions coming every third of a second, so the offline flip is
		// judged soon after the threshold lapses rather than at the next snapshot tick.
		padSpaced(env, 1, 333*time.Millisecond)
		env.ExecuteWorkflow(DeviceWorkflow, state)

		finishRun(t, env)
		env.AssertExpectations(t)
	})

	t.Run("values are re-upserted unchanged across a rolling continuation", func(t *testing.T) {
		t.Parallel()
		heartbeat := testSignal{HeartbeatSignalName, HeartbeatSignal{
			EventID: "evt-1", DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
			CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
		}}
		first := runDevice(t, newDeviceState("dev-1", testSettings()),
			&dispatchRecorder{}, &snapshotRecorder{}, heartbeat)

		// The continuing run starts by upserting exactly the map the carried state derives.
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, &snapshotRecorder{})
		env.OnUpsertTypedSearchAttributes(wantAttrs("eu-west", "oak-s3", "fw-1", false)).Once()
		padToRollover(env, 0)
		env.ExecuteWorkflow(DeviceWorkflow, first)

		finishRun(t, env)
		env.AssertExpectations(t)
	})
}

// TestDeviceWorkflowSnapshotScheduling pins when the entity persists a state snapshot: at the
// configured cadence and immediately on every meaningful transition — never for a routine
// heartbeat — and never at the cost of the entity itself when writes fail.
func TestDeviceWorkflowSnapshotScheduling(t *testing.T) {
	t.Parallel()

	t.Run("periodic snapshots fire at the cadence", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{}
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, snaps)
		state := newDeviceState("dev-1", DeviceSettings{
			SnapshotInterval: time.Second, OfflineThreshold: 30 * time.Second,
		})

		var ticks int
		env.RegisterDelayedCallback(func() {
			ticks = len(snaps.recorded())
		}, 5500*time.Millisecond)
		padSpaced(env, 0, 333*time.Millisecond)
		env.ExecuteWorkflow(DeviceWorkflow, state)

		finishRun(t, env)
		// Ticks land at 1s..5s before the observation point at 5.5s — one snapshot each.
		if ticks != 5 {
			t.Errorf("snapshots after five intervals = %d, want 5", ticks)
		}
	})

	t.Run("meaningful transitions snapshot immediately", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{}
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, snaps)

		var observed []Snapshot
		env.RegisterDelayedCallback(func() {
			observed = snaps.recorded()
		}, 5500*time.Microsecond)
		queueSignals(env,
			testSignal{HeartbeatSignalName, HeartbeatSignal{ // firmware adoption
				EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
			}},
			testSignal{CommandIssuedSignalName, CommandIssuedSignal{
				CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
				FirmwareID: "fw-2", Version: "fw-2",
			}},
			testSignal{CommandResultSignalName, CommandResultSignal{
				DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
			}},
			testSignal{ConfigChangedSignalName, ConfigChangedSignal{
				DeviceID: "dev-1", Version: 1, Snapshot: []byte(`{"interval":"5s"}`),
			}},
			testSignal{HeartbeatSignalName, HeartbeatSignal{ // routine: timestamp only
				EventID: "evt-2", DeviceID: "dev-1", Timestamp: time.Unix(3000, 0),
			}},
		)
		padToRollover(env, 5)
		env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

		finishRun(t, env)
		// Firmware adoption, the command set, its conclusion, and the config apply — one
		// snapshot each; the routine heartbeat adds none.
		if len(observed) != 4 {
			t.Fatalf("snapshots after the transitions = %d, want 4", len(observed))
		}
		if observed[1].Pending == nil {
			t.Errorf("command snapshot carries no pending command: %+v", observed[1])
		}
		if observed[3].Config.Version != 1 {
			t.Errorf("config snapshot carries version %d, want 1", observed[3].Config.Version)
		}
	})

	t.Run("liveness flips snapshot immediately", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{}
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, snaps)
		base := time.Unix(1000, 0)
		env.SetStartTime(base)
		state := newDeviceState("dev-1", DeviceSettings{
			SnapshotInterval: time.Hour, OfflineThreshold: 10 * time.Second,
		})

		var observed []Snapshot
		env.RegisterDelayedCallback(func() {
			observed = snaps.recorded()
		}, 12*time.Second)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", Timestamp: base.Add(time.Second),
			})
		}, time.Millisecond)
		padSpaced(env, 1, 333*time.Millisecond)
		env.ExecuteWorkflow(DeviceWorkflow, state)

		finishRun(t, env)
		// One snapshot per flip: online when the heartbeat lands, offline once the
		// threshold lapses. Nothing else fires — no ticks, no transitions.
		if len(observed) != 2 {
			t.Fatalf("snapshots = %d, want 2 (online flip, offline flip)", len(observed))
		}
		if !observed[0].Online {
			t.Error("first snapshot does not carry the device as online")
		}
		if observed[1].Online {
			t.Error("second snapshot does not carry the device as offline")
		}
	})

	t.Run("a permanently failing snapshot never disturbs the entity", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{failures: 1000}
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, snaps)
		update := CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-2", Version: "fw-2",
		}
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
			})
		}, time.Millisecond)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CommandIssuedSignalName, update)
		}, 2*time.Millisecond)
		padSpaced(env, 2, 100*time.Millisecond)
		env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

		// The entity rolls over with its state intact even though every snapshot write
		// failed; a lost projection is one interval of staleness, not a lost device.
		carried := finishRun(t, env)
		if diff := cmp.Diff(State{
			DeviceID:        "dev-1",
			CurrentFw:       "fw-1",
			LastHeartbeatAt: time.Unix(2000, 0),
			Pending:         &PendingCommand{Command: update, Dispatched: true},
		}, carried.view()); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
		if got := len(snaps.recorded()); got != 0 {
			t.Errorf("saved snapshots = %d, want 0", got)
		}
	})

	t.Run("a snapshot write is retried to success", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{failures: 1}
		env := newDeviceWorkflowEnv(&dispatchRecorder{}, snaps)

		var saved int
		env.RegisterDelayedCallback(func() {
			saved = len(snaps.recorded())
		}, 3*time.Second)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(HeartbeatSignalName, HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: time.Unix(2000, 0),
			})
		}, time.Millisecond)
		padSpaced(env, 1, 250*time.Millisecond)
		env.ExecuteWorkflow(DeviceWorkflow, newDeviceState("dev-1", testSettings()))

		finishRun(t, env)
		if saved != 1 {
			t.Errorf("saved snapshots = %d, want 1 (one failure, one retry)", saved)
		}
	})
}
