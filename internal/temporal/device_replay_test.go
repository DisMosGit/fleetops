package temporal

import (
	"log/slog"
	"os"
	"slices"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

// The two task queues the recorded run scheduled on — the entity and its snapshots on one, the
// command dispatch on the control plane's, the way the binaries split them. The integration recorder
// stages the run with these, and the assertions below read them back out of the history, so a
// recording of some other world cannot pass for this one.
const (
	// recordedControlQueue is the queue the recorded entity and its snapshot activity ran on.
	recordedControlQueue = "recorded-device-workflow"
	// recordedDispatchQueue is the queue the recorded command dispatch was scheduled on.
	recordedDispatchQueue = "recorded-dispatch"
)

// recordedDeviceHistoryFile is the recorded history of a device entity workflow: the events of one
// real run against a real Temporal server, kept beside the code it replays. It is a golden file —
// produced by the integration recorder (TestRecordDeviceWorkflowHistory in
// device_history_integration_test.go) and regenerated with -update, never edited by hand — so a
// change to DeviceWorkflow that is not replay-safe fails here, in the fast suite, instead of in a
// rollout.
const recordedDeviceHistoryFile = "testdata/device_history.json"

// TestDeviceWorkflowReplaysARecordedHistory replays the recorded device history against the workflow
// under test: a run that passes once and flaps on replay is a bug, and the recorded events are the
// only ones the workflow may have produced. The negative control replays the same events against a
// foreign workflow, which proves the assertion is one that can fail.
func TestDeviceWorkflowReplaysARecordedHistory(t *testing.T) {
	t.Parallel()

	history := readRecordedDeviceHistory(t)

	if err := replayAsWorkflow(history, DeviceWorkflow); err != nil {
		t.Errorf("replay the recorded device history against %s: %v", DeviceWorkflowName, err)
	}

	// The same history cannot be what a different workflow produced, so a replayer that answered
	// nil here would prove nothing about the recorded run. The foreign workflow is registered
	// under the device workflow's name on purpose: the name in the history is what the replayer
	// looks up, so the replay reaches the wrong code rather than finding no registration.
	if err := replayAsWorkflow(history, RolloutWorkflow); err == nil {
		t.Errorf("replaying a device history against %s = nil, want a replay failure", RolloutWorkflowName)
	}
}

// TestRecordedDeviceHistoryExercisesTheEntity asserts the golden history is one a determinism
// regression could be caught by: every signal the entity folds in and every activity it decides to
// run is in it, dispatched on the queue the state says. A regenerated recording that lost the
// interesting decisions would otherwise pass its replay vacuously.
func TestRecordedDeviceHistoryExercisesTheEntity(t *testing.T) {
	t.Parallel()

	history := readRecordedDeviceHistory(t)

	started := history.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	if got := started.GetWorkflowType().GetName(); got != DeviceWorkflowName {
		t.Fatalf("recorded history starts workflow %q, want %q", got, DeviceWorkflowName)
	}

	// Two commands: the update the wave issues and the abort that supersedes a pending command.
	// Both are concluded, so both the accept and the conclude paths of the pending-command
	// lifecycle are in the recorded events.
	tests := []struct {
		signal string
		want   int
	}{
		{signal: HeartbeatSignalName, want: 1},
		{signal: CommandIssuedSignalName, want: 2},
		{signal: CommandResultSignalName, want: 2},
		{signal: UpdateStatusSignalName, want: 1},
		{signal: ConfigChangedSignalName, want: 1},
	}
	for _, tc := range tests {
		if got := historySignalDeliveries(history, tc.signal); got < tc.want {
			t.Errorf("recorded history carries %d %s signals, want at least %d", got, tc.signal, tc.want)
		}
	}

	activities := historyScheduledActivities(history)
	queues := make(map[string]string, len(activities))
	for _, activity := range activities {
		if _, seen := queues[activity.Type]; !seen {
			queues[activity.Type] = activity.TaskQueue
		}
	}
	for _, want := range []string{DispatchActivityName, SnapshotActivityName} {
		if _, ok := queues[want]; !ok {
			t.Errorf("recorded history schedules no %s activity (scheduled: %v)", want, activities)
		}
	}
	if got := queues[DispatchActivityName]; got != recordedDispatchQueue {
		t.Errorf("recorded dispatch activity was scheduled on %q, want the control plane's %q",
			got, recordedDispatchQueue)
	}
	if got := queues[SnapshotActivityName]; got != recordedControlQueue {
		t.Errorf("recorded snapshot activity was scheduled on %q, want the entity's own %q",
			got, recordedControlQueue)
	}

	// The periodic snapshot timer is what keeps a quiet entity's projection fresh; a recording
	// without its start would not replay the timer decision at all.
	if !hasTimerStarted(history) {
		t.Error("recorded history starts no timer, want the periodic snapshot timer")
	}
}

// readRecordedDeviceHistory reads and decodes the golden history, failing with the file it could not
// use rather than with an empty replay that proves nothing.
func readRecordedDeviceHistory(t *testing.T) *historypb.History {
	t.Helper()

	encoded, err := os.ReadFile(recordedDeviceHistoryFile)
	if err != nil {
		t.Fatalf("read the recorded device history: %v", err)
	}
	history := &historypb.History{}
	// The file is the Temporal history JSON the SDK's own replayer reads: a protojson encoding of
	// the history the server returned.
	if err := protojson.Unmarshal(encoded, history); err != nil {
		t.Fatalf("decode %s: %v", recordedDeviceHistoryFile, err)
	}
	if len(history.GetEvents()) == 0 {
		t.Fatalf("%s holds no events", recordedDeviceHistoryFile)
	}
	return history
}

// replayAsWorkflow replays one recorded history against wf, registered under the device workflow's
// name — the name the recorded history carries.
func replayAsWorkflow(history *historypb.History, wf any) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: DeviceWorkflowName})
	return replayer.ReplayWorkflowHistory(slog.New(slog.DiscardHandler), history)
}

// historySignalDeliveries returns how often one signal was delivered to the workflow in a history.
func historySignalDeliveries(history *historypb.History, name string) int {
	deliveries := 0
	for _, event := range history.GetEvents() {
		signaled := event.GetWorkflowExecutionSignaledEventAttributes()
		if signaled != nil && signaled.GetSignalName() == name {
			deliveries++
		}
	}
	return deliveries
}

// scheduledActivity is one activity a history scheduled: the activity type and the queue the
// workflow scheduled it on.
type scheduledActivity struct {
	// Type is the activity type the workflow scheduled.
	Type string
	// TaskQueue is the queue the activity task was scheduled on.
	TaskQueue string
}

// historyScheduledActivities returns every activity the history scheduled, in order.
func historyScheduledActivities(history *historypb.History) []scheduledActivity {
	var activities []scheduledActivity
	for _, event := range history.GetEvents() {
		scheduled := event.GetActivityTaskScheduledEventAttributes()
		if scheduled == nil {
			continue
		}
		activities = append(activities, scheduledActivity{
			Type:      scheduled.GetActivityType().GetName(),
			TaskQueue: scheduled.GetTaskQueue().GetName(),
		})
	}
	return activities
}

// hasTimerStarted reports whether the history starts a timer — the periodic snapshot timer whose
// firing is a decision like any other.
func hasTimerStarted(history *historypb.History) bool {
	return slices.ContainsFunc(history.GetEvents(), func(event *historypb.HistoryEvent) bool {
		return event.GetTimerStartedEventAttributes() != nil
	})
}
