//go:build integration

package temporal

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/DisMosGit/fleetops/internal/temporaltest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// updateRecordedHistory rewrites the golden history the replay test reads. It is how the recorded
// file is produced — the value only ever comes from a real run — so regenerate it rather than
// editing it by hand:
//
//	go test -tags integration -count=1 -run TestRecordDeviceWorkflowHistory ./internal/temporal/ -update
var updateRecordedHistory = flag.Bool("update", false, "rewrite the recorded histories under testdata")

// The facts of the run the golden history is recorded from: the device it drives and the periodic
// snapshot cadence it runs under. The unit test beside this file replays the recording, so what it
// holds is a contract with that test rather than a knob.
const (
	// recordedDeviceID is the device the recorded run drives.
	recordedDeviceID = "device-recorded"
	// recordedSnapshotInterval is the periodic snapshot cadence of the recorded run: short enough
	// that the snapshot timer's decisions are in the recording, long enough not to dominate it.
	recordedSnapshotInterval = 500 * time.Millisecond
)

// TestRecordDeviceWorkflowHistory drives one device entity through the decisions its history has to
// reproduce — a heartbeat, two issued commands dispatched to the control plane, update progress, a
// configuration apply, and the results that conclude both commands — against a real Temporal server,
// and records the events into testdata/device_history.json. The unit test beside it replays that
// file, which is what keeps a change to DeviceWorkflow that is not replay-safe from reaching a
// rollout.
//
// The activities are stand-ins: the recording is about the workflow's decisions, not their effects,
// so both succeed and only count. Without -update the run still records and replays, and still
// asserts what the recording holds; it just leaves the golden file alone.
func TestRecordDeviceWorkflowHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	grace := testexec.Get(t).Wait(60 * time.Second)

	server := temporaltest.For(t)
	tc := server.Client
	// The entity upserts search attributes at every decision point, so the namespace carries them
	// before the run starts: without this the upserts fail — tolerably, but the recording would
	// hold decision points polluted by a bootstrap the fleet always performs.
	if err := EnsureSearchAttributes(ctx, tc.OperatorService(), server.Namespace); err != nil {
		t.Fatalf("bootstrap the namespace's search attributes: %v", err)
	}

	activities := &recordedActivities{}
	deviceWork := worker.New(tc, recordedControlQueue, worker.Options{})
	deviceWork.RegisterWorkflowWithOptions(DeviceWorkflow, workflow.RegisterOptions{Name: DeviceWorkflowName})
	deviceWork.RegisterActivityWithOptions(activities.snapshot,
		activity.RegisterOptions{Name: SnapshotActivityName})
	control := worker.New(tc, recordedDispatchQueue, worker.Options{})
	control.RegisterActivityWithOptions(activities.dispatch,
		activity.RegisterOptions{Name: DispatchActivityName})
	startRecordedWorker(t, deviceWork)
	startRecordedWorker(t, control)

	_, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        DeviceWorkflowID(recordedDeviceID),
		TaskQueue: recordedControlQueue,
	}, DeviceWorkflowName, newDeviceState(recordedDeviceID, DeviceSettings{
		SnapshotInterval:  recordedSnapshotInterval,
		OfflineThreshold:  time.Minute,
		DispatchTaskQueue: recordedDispatchQueue,
	}))
	if err != nil {
		t.Fatalf("start the recorded device workflow: %v", err)
	}

	states := NewDeviceStates(tc)
	recordSignal(t, tc, states, grace, HeartbeatSignalName, HeartbeatSignal{
		EventID:   "hb-1",
		DeviceID:  recordedDeviceID,
		Region:    "eu-west",
		Model:     "oak-s3",
		CurrentFw: "1.0.0",
		Timestamp: time.Now().UTC(),
	}, func(state State) bool {
		return state.Region == "eu-west" && state.Model == "oak-s3" && state.Online
	})

	// The wave's command: issued, dispatched on the control plane's queue, and then concluded by the
	// result the device reports. Progress arrives between the two, the way a real update reports it.
	recordSignal(t, tc, states, grace, CommandIssuedSignalName, CommandIssuedSignal{
		CommandID:  "cmd-1",
		DeviceID:   recordedDeviceID,
		Kind:       CommandKindUpdate,
		FirmwareID: "fw-1",
		Version:    "1.1.0",
		Checksum:   "sha256:recorded",
	}, func(state State) bool {
		return state.Pending != nil && state.Pending.Command.CommandID == "cmd-1" && state.Pending.Dispatched
	})
	recordSignal(t, tc, states, grace, UpdateStatusSignalName, UpdateStatusSignal{
		DeviceID:        recordedDeviceID,
		FirmwareID:      "fw-1",
		Phase:           PhaseDownloading,
		ProgressPercent: 50,
	}, func(state State) bool {
		return state.Update != nil && state.Update.Phase == PhaseDownloading
	})
	recordSignal(t, tc, states, grace, ConfigChangedSignalName, ConfigChangedSignal{
		DeviceID: recordedDeviceID,
		Version:  1,
		Snapshot: json.RawMessage(`{"log_level":"debug"}`),
	}, func(state State) bool {
		return state.Config.Version == 1
	})
	recordSignal(t, tc, states, grace, CommandResultSignalName, CommandResultSignal{
		DeviceID:  recordedDeviceID,
		CommandID: "cmd-1",
		Outcome:   OutcomeSucceeded,
	}, func(state State) bool {
		return state.LastCommand != nil && state.LastCommand.Command.CommandID == "cmd-1"
	})

	// A second command, of the other kind: the entity accepts it, supersedes what the state holds,
	// dispatches it, and concludes it with the failure the device reported.
	recordSignal(t, tc, states, grace, CommandIssuedSignalName, CommandIssuedSignal{
		CommandID: "cmd-2",
		DeviceID:  recordedDeviceID,
		Kind:      CommandKindAbort,
		Reason:    "recorded run",
	}, func(state State) bool {
		return state.Pending != nil && state.Pending.Command.CommandID == "cmd-2" && state.Pending.Dispatched
	})
	recordSignal(t, tc, states, grace, CommandResultSignalName, CommandResultSignal{
		DeviceID:  recordedDeviceID,
		CommandID: "cmd-2",
		Outcome:   OutcomeFailed,
		Detail:    "recorded run",
	}, func(state State) bool {
		return state.LastCommand != nil && state.LastCommand.Command.CommandID == "cmd-2"
	})

	// The recorded events are replayed here first, so a recording that could never replay is caught
	// where it was made rather than by the unit suite later.
	history := recordedDeviceWorkflowHistory(t, tc)
	if err := replayAsWorkflow(history, DeviceWorkflow); err != nil {
		t.Errorf("replay the history this run recorded: %v", err)
	}
	snapshots, dispatches := activities.counts()
	if dispatches < 2 || snapshots < 1 {
		t.Errorf("the recorded run's activities ran %d dispatches and %d snapshots, want two dispatches and at least one snapshot",
			dispatches, snapshots)
	}

	if !*updateRecordedHistory {
		t.Logf("recorded %d history events; pass -update to rewrite %s",
			len(history.GetEvents()), recordedDeviceHistoryFile)
		return
	}
	writeRecordedDeviceHistory(t, history)
}

// recordedActivities stands in for the two activities the recorded run calls: the dispatch of a
// command to the control plane and the projection of the state into the fleet database. Which run
// decided them is what the history records; what they did is not, so both succeed and count.
type recordedActivities struct {
	mu         sync.Mutex
	snapshots  int
	dispatches int
}

// snapshot stands in for the snapshot-device-state activity.
func (a *recordedActivities) snapshot(context.Context, Snapshot) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.snapshots++
	return nil
}

// dispatch stands in for the dispatch-command activity.
func (a *recordedActivities) dispatch(context.Context, CommandIssuedSignal) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dispatches++
	return nil
}

// counts returns how many of each activity ran.
func (a *recordedActivities) counts() (snapshots, dispatches int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snapshots, a.dispatches
}

// recordSignal delivers one signal and waits until its effect is visible in the state query, so the
// recorded history has a decided order of events rather than a race between delivery and the fetch.
func recordSignal[T any](
	t *testing.T,
	tc temporalclient.Client,
	states *DeviceStates,
	grace time.Duration,
	name string,
	payload T,
	held func(State) bool,
) {
	t.Helper()

	if err := tc.SignalWorkflow(
		context.Background(), DeviceWorkflowID(recordedDeviceID), "", name, payload,
	); err != nil {
		t.Fatalf("signal %s: %v", name, err)
	}
	deadline := time.Now().Add(grace)
	for {
		state, err := states.State(context.Background(), recordedDeviceID)
		if err != nil {
			t.Fatalf("read the recorded device's state: %v", err)
		}
		if held(state) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %s signal was not folded into the state within %s: last state %+v",
				name, grace, state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// recordedDeviceWorkflowHistory reads the recorded run's events. The entity is still running when
// this returns — it is a long-lived entity, that is the point — and a partial history is what a
// determinism check replays: every event the server has written has to be one the workflow could
// have produced.
func recordedDeviceWorkflowHistory(t *testing.T, tc temporalclient.Client) *historypb.History {
	t.Helper()

	iter := tc.GetWorkflowHistory(context.Background(), DeviceWorkflowID(recordedDeviceID), "",
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read the recorded device history: %v", err)
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		t.Fatal("the recorded device workflow has an empty history")
	}
	return history
}

// writeRecordedDeviceHistory encodes history into the golden file the replay test reads.
func writeRecordedDeviceHistory(t *testing.T, history *historypb.History) {
	t.Helper()

	encoded, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if err != nil {
		t.Fatalf("encode the recorded device history: %v", err)
	}
	dir := filepath.Dir(recordedDeviceHistoryFile)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	if err := os.WriteFile(recordedDeviceHistoryFile, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("write %s: %v", recordedDeviceHistoryFile, err)
	}
	t.Logf("recorded %d history events into %s", len(history.GetEvents()), recordedDeviceHistoryFile)
}

// startRecordedWorker runs one worker until the test ends.
func startRecordedWorker(t *testing.T, w worker.Worker) {
	t.Helper()

	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			t.Logf("recorded worker stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Log("a recorded worker did not stop within 30s")
		}
	})
}
