//go:build integration

package main

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// The assertions below are how a scenario states what a restart did and did not do: the work the
// workflow decided (its history), the effects it applied (the command seam, the broker, and the
// fleet's records), and whether the run is deterministic (its history replayed). They are all
// identity-based, because at-least-once delivery is what a restart changes and an identity is what
// keeps a redelivery from becoming a second effect.

// scheduledUnit is one piece of work a workflow decided to run: the activity type it scheduled, and
// how many times it scheduled it. A retry does not schedule an activity again — the retry is the
// same unit — so a unit scheduled twice is the workflow re-deciding work it had already decided.
type scheduledUnit struct {
	// Type is the activity type the unit scheduled.
	Type string
	// Schedules is how many times the unit was scheduled.
	Schedules int
}

// scheduledUnits returns the history's scheduled activities by the workflow's own activity id.
func scheduledUnits(history *historypb.History) map[string]scheduledUnit {
	units := make(map[string]scheduledUnit)
	for _, event := range history.GetEvents() {
		scheduled := event.GetActivityTaskScheduledEventAttributes()
		if scheduled == nil {
			continue
		}
		unit := units[scheduled.GetActivityId()]
		unit.Type = scheduled.GetActivityType().GetName()
		unit.Schedules++
		units[scheduled.GetActivityId()] = unit
	}
	return units
}

// assertScheduledOnce asserts that every logical unit in the history was scheduled exactly once and
// that the work it decided is exactly the given activity types: the restarts re-decided no work, and
// the run decided the work it was supposed to. Each unit is a schedule of its own — a retry repeats
// the unit rather than scheduling it again — while the types are compared as a set, because how many
// evaluations a wave needs is the gate's business, not this assertion's.
func assertScheduledOnce(t *testing.T, history *historypb.History, wantTypes ...string) {
	t.Helper()
	units := scheduledUnits(history)
	if len(units) == 0 {
		t.Fatal("the rollout history schedules no activity at all")
	}
	var got []string
	seen := make(map[string]bool, len(units))
	for id, unit := range units {
		if unit.Schedules != 1 {
			t.Errorf("activity %s (%s) was scheduled %d times, want once", id, unit.Type, unit.Schedules)
		}
		if !slices.Contains(wantTypes, unit.Type) {
			t.Errorf("the rollout scheduled %s (%s), which this scenario does not expect", unit.Type, id)
		}
		if !seen[unit.Type] {
			seen[unit.Type] = true
			got = append(got, unit.Type)
		}
	}
	want := slices.Clone(wantTypes)
	slices.Sort(got)
	slices.Sort(want)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("decided activity types mismatch (-want +got):\n%s", diff)
	}
}

// signalCount returns how many times the history records one signal being delivered to the workflow,
// which is how a scenario states that exactly one approval was consumed.
func signalCount(history *historypb.History, name string) int {
	count := 0
	for _, event := range history.GetEvents() {
		if signaled := event.GetWorkflowExecutionSignaledEventAttributes(); signaled != nil &&
			signaled.GetSignalName() == name {
			count++
		}
	}
	return count
}

// workflowHistory collects a concluded workflow's history into the shape the replayer takes.
func workflowHistory(
	t *testing.T,
	c temporalclient.Client,
	rolloutID string,
) *historypb.History {
	t.Helper()
	iter := c.GetWorkflowHistory(context.Background(), temporal.RolloutWorkflowID(rolloutID), "",
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read rollout %s history: %v", rolloutID, err)
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		t.Fatalf("rollout %s has an empty history", rolloutID)
	}
	return history
}

// replayHistory replays one history under the registrations add installs and reports the replayer's
// verdict: nil when the history is one those registrations could have produced, and the break it
// found otherwise.
func replayHistory(
	t *testing.T,
	history *historypb.History,
	add func(worker.WorkflowReplayer),
) error {
	t.Helper()
	replayer := worker.NewWorkflowReplayer()
	add(replayer)
	return replayer.ReplayWorkflowHistory(smokeLogger(), history)
}

// registerRolloutWorkflow registers the rollout workflow under the explicit name the worker
// registers it with, which is the registration a rollout's own history replays against.
func registerRolloutWorkflow(replayer worker.WorkflowReplayer) {
	replayer.RegisterWorkflowWithOptions(temporal.RolloutWorkflow, workflow.RegisterOptions{
		Name: temporal.RolloutWorkflowName,
	})
}

// assertReplays asserts that a concluded rollout's history replays against the rollout workflow
// registered under its own name — a run that passes once and flaps on replay is a bug — and returns
// the history, so a scenario can prove the assertion is one that can fail.
func assertReplays(
	t *testing.T,
	c temporalclient.Client,
	rolloutID string,
) *historypb.History {
	t.Helper()
	history := workflowHistory(t, c, rolloutID)
	if err := replayHistory(t, history, registerRolloutWorkflow); err != nil {
		t.Errorf("replay rollout %s history: %v", rolloutID, err)
	}
	return history
}

// assertReplayRejectsAForeignWorkflow asserts that the replay assertion is one that can fail: the
// same history replayed against a different workflow registration is not a history that workflow
// could have produced, so a replayer that answered nil here would prove nothing about the run.
func assertReplayRejectsAForeignWorkflow(t *testing.T, history *historypb.History) {
	t.Helper()
	err := replayHistory(t, history, func(replayer worker.WorkflowReplayer) {
		// Registered under the rollout's name, so the replayer finds a workflow for the
		// history's type — and it is the wrong workflow for it.
		replayer.RegisterWorkflowWithOptions(temporal.DeviceWorkflow, workflow.RegisterOptions{
			Name: temporal.RolloutWorkflowName,
		})
	})
	if err == nil {
		t.Error("replaying a rollout history against a device workflow = nil, want a replay failure")
	}
}

// assertWaveCommandIdentities asserts that every device of every given wave was commanded under
// exactly the one command id its wave and device derive, and that no command the rollout issued
// addresses anything else. A redelivery of an attempt repeats an id — that is what makes it a no-op
// for a device that already accepted it — while a second id, or an id for a device no wave targets,
// is an effect a restart duplicated.
func assertWaveCommandIdentities(
	t *testing.T,
	dispatcher *commandRecorder,
	waves []rollout.WaveRecord,
) {
	t.Helper()
	want := make(map[string]string)
	for _, wave := range waves {
		for _, device := range wave.DeviceIDs {
			want[temporal.CommandID(wave.ID, device)] = device
		}
	}
	if len(want) == 0 {
		t.Fatal("no wave targets a device, so there is no update identity to assert")
	}

	delivered := make(map[string]int, len(want))
	for _, cmd := range dispatcher.recorded() {
		if strings.HasPrefix(cmd.CommandID, rollbackCommandPrefix) {
			// A restore is a compensation, not a wave's dispatch: its identity is asserted as
			// the rollback's own.
			continue
		}
		device, expected := want[cmd.CommandID]
		if !expected {
			t.Errorf("command %s for device %s addresses no device of the rollout's waves",
				cmd.CommandID, cmd.DeviceID)
			continue
		}
		delivered[cmd.CommandID]++
		if cmd.DeviceID != device {
			t.Errorf("command %s targets %s, want %s", cmd.CommandID, cmd.DeviceID, device)
		}
	}
	for commandID, device := range want {
		if delivered[commandID] == 0 {
			t.Errorf("device %s was never commanded for wave %s", device, commandID)
		}
	}
}

// assertRestoreIdentities asserts that every compensated device was restored under exactly one
// restore command id — however many times an attempt was redelivered — and that the id names the
// device it restores.
func assertRestoreIdentities(t *testing.T, dispatcher *commandRecorder, devices []string) {
	t.Helper()
	for _, device := range devices {
		ids := make(map[string]int)
		for _, cmd := range dispatcher.deliveredCommands(device) {
			if !strings.HasPrefix(cmd.CommandID, rollbackCommandPrefix) {
				continue
			}
			ids[cmd.CommandID]++
			if !strings.HasSuffix(cmd.CommandID, device) {
				t.Errorf("restore command %s does not name the device it restores (%s)",
					cmd.CommandID, device)
			}
		}
		switch {
		case len(ids) == 0:
			t.Errorf("device %s was never restored", device)
		case len(ids) > 1:
			t.Errorf("device %s was restored under %d command ids (%v), want one",
				device, len(ids), ids)
		}
	}
}

// assertAnnouncementIdentities asserts that each rollback phase was announced under exactly one
// event id — the id the rollout and the phase derive — however many copies of the announcement were
// published, and that every copy repeated that id rather than minting a second event.
func assertAnnouncementIdentities(
	t *testing.T,
	events []telemetry.RollbackEvent,
	rolloutID string,
) {
	t.Helper()
	ids := make(map[telemetry.RollbackPhase]map[string]int)
	for _, event := range events {
		want := telemetry.RollbackEventID(rolloutID, event.Phase)
		if event.EventID != want {
			t.Errorf("announcement %s carries event id %q, want %q", event.Phase, event.EventID, want)
		}
		if ids[event.Phase] == nil {
			ids[event.Phase] = make(map[string]int)
		}
		ids[event.Phase][event.EventID]++
	}
	for _, phase := range []telemetry.RollbackPhase{telemetry.RollbackStarted, telemetry.RollbackCompleted} {
		switch copies := ids[phase]; {
		case len(copies) == 0:
			t.Errorf("the rollback published no %s announcement", phase)
		case len(copies) > 1:
			t.Errorf("the %s phase was announced under %d event ids (%v), want one",
				phase, len(copies), copies)
		}
	}
}

// recordedFirmware reads the firmware version the fleet records for one device.
func recordedFirmware(t *testing.T, db *mongo.Database, deviceID string) string {
	t.Helper()
	var doc struct {
		CurrentFw string `bson:"current_fw"`
	}
	err := db.Collection("devices").FindOne(context.Background(),
		bson.D{{Key: "_id", Value: deviceID}}).Decode(&doc)
	if err != nil {
		t.Fatalf("read device %s record: %v", deviceID, err)
	}
	return doc.CurrentFw
}

// assertRecordsMatchDevices asserts that the fleet's recorded firmware version for every given
// device is the version that device's workflow — the authority on its own firmware — holds, so no
// device is recorded on a version it does not run.
func assertRecordsMatchDevices(t *testing.T, w *restartWorld, devices []string) {
	t.Helper()
	for _, device := range devices {
		state, err := w.dispatcher.State(context.Background(), device)
		if err != nil {
			t.Errorf("read device %s authority state: %v", device, err)
			continue
		}
		if state.CurrentFw == "" {
			t.Errorf("device %s authority holds no firmware version", device)
			continue
		}
		if recorded := recordedFirmware(t, w.db, device); recorded != state.CurrentFw {
			t.Errorf("device %s is recorded on %q but runs %q", device, recorded, state.CurrentFw)
		}
	}
}

// assertNoFurtherEffects asserts that a concluded rollout applies nothing more: the worker is
// restarted one final time, and the rollout's activity ledger, its recorded document, and the
// commands the seam accepted are all unchanged by it. The settle pause is what gives a
// would-be effect the chance to appear; nothing about the assertion depends on when it happens.
func assertNoFurtherEffects(t *testing.T, w *restartWorld) {
	t.Helper()
	before := w.ledger.count()
	commandsBefore := len(w.dispatcher.recorded())
	record := readRollout(t, w, w.opts.rolloutID)

	if _, err := w.worker.Restart(); err != nil {
		t.Fatalf("restart the worker after the rollout concluded: %v", err)
	}
	// The resumption check runs for this restart too: a concluded rollout's state is the one
	// reading that must never move again.
	w.sampleResumption()
	time.Sleep(restartSettle)

	if after := w.ledger.count(); after != before {
		t.Errorf("the worker picked up %d activity attempts after the rollout concluded, want none",
			after-before)
	}
	if after := len(w.dispatcher.recorded()); after != commandsBefore {
		t.Errorf("the seam accepted %d commands after the rollout concluded, want none",
			after-commandsBefore)
	}
	if after := readRollout(t, w, w.opts.rolloutID); after != record {
		t.Errorf("the rollout record changed after it concluded (-before +after):\n%s",
			cmp.Diff(record, after))
	}
}

// readRollout reads one rollout document.
func readRollout(t *testing.T, w *restartWorld, rolloutID string) rollout.RolloutRecord {
	t.Helper()
	var record rollout.RolloutRecord
	err := w.rollouts.FindOne(context.Background(),
		bson.D{{Key: "_id", Value: rolloutID}}).Decode(&record)
	if err != nil {
		t.Fatalf("read rollout %s: %v", rolloutID, err)
	}
	return record
}

// workflowStatus reports the status the server holds for one workflow execution, which is how a
// scenario states that a concluded rollout really is closed.
func workflowStatus(
	t *testing.T,
	c temporalclient.Client,
	rolloutID string,
) enumspb.WorkflowExecutionStatus {
	t.Helper()
	described, err := c.DescribeWorkflowExecution(context.Background(),
		temporal.RolloutWorkflowID(rolloutID), "")
	if err != nil {
		t.Fatalf("describe rollout %s workflow: %v", rolloutID, err)
	}
	return described.GetWorkflowExecutionInfo().GetStatus()
}
