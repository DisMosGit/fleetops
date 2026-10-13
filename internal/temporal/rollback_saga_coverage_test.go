package temporal

import (
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
)

// TestRolloutThatCannotStartRunsNoCompensation backs "A rollout that could not start runs no
// compensation": a rollout that concludes as failed because its firmware is unknown or does not
// target the selector's model derives no plan at all, so no compensation step executes and no
// rollback notification is published.
func TestRolloutThatCannotStartRunsNoCompensation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		records []firmware.Record
		want    RolloutOutcome
	}{
		{
			name:    "an unknown firmware fails the rollout",
			records: []firmware.Record{},
			want:    OutcomeFirmwareUnknown,
		},
		{
			name: "a firmware that does not target the selector's model fails the rollout",
			records: []firmware.Record{{
				ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3-mini"}, Checksum: "sha256:0f1e2d",
			}},
			want: OutcomeFirmwareMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutTestSettings()
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(20)...)
			fakes.firmware = map[string]firmware.Record{}
			for _, rec := range tc.records {
				fakes.firmware[rec.ID] = rec
			}

			view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

			if view.Status != rollout.RolloutFailed || view.Outcome != tc.want {
				t.Fatalf("rollout concluded as %s/%s, want failed/%s", view.Status, view.Outcome, tc.want)
			}
			// Nothing was dispatched, so no plan exists: the rollout reports no rollback at all
			// rather than one whose steps ran and achieved nothing.
			if view.Rollback != nil {
				t.Errorf("failed rollout reports rollback %+v, want none", view.Rollback)
			}
			recorded, ok := fakes.recordedRollout("ro-1")
			if !ok {
				t.Fatal("the failed rollout was never recorded")
			}
			if recorded.Rollback != nil {
				t.Errorf("recorded rollback = %+v, want none", recorded.Rollback)
			}
			// No compensation step ran: no rollback notification was published, no command of
			// any kind reached a device — a restore among them — and no wave was resolved.
			if announcements := fakes.recordedAnnouncements(); len(announcements) != 0 {
				t.Errorf("announcements = %+v, want no rollback notification", announcements)
			}
			if commands := fakes.commandDeliveries(); len(commands) != 0 {
				t.Errorf("commands = %+v, want none", commands)
			}
			if waves := fakes.recordedWaveEvents(); len(waves) != 0 {
				t.Errorf("wave writes = %v, want no wave resolved or moved", waves)
			}
		})
	}
}

// rerunPlanWorkflowName is the name the plan-rerun workflow is registered under in the test
// environment. It is a test-only workflow: the production rollout runs its plan once, and the
// "Re-running a finished plan changes nothing" scenario is about what a second run of the same
// finished plan does.
const rerunPlanWorkflowName = "rollback-plan-rerun"

// rerunPlanWorkflow drives one wave's update and then the rollback plan that compensates it, twice:
// the second run is the "made to run its steps again" the scenario describes. It is the smallest
// driver that reaches a finished plan — resolve, dispatch, collect, then run the plan — so the
// second run meets the same device workflows and the same store the first one left behind.
func rerunPlanWorkflow(ctx workflow.Context, in RolloutInput) error {
	state := newRolloutState(in)
	if err := state.validate(); err != nil {
		return err
	}
	if err := recordRollout(ctx, &state); err != nil {
		return err
	}
	fw, err := loadFirmware(ctx, state)
	if err != nil {
		return err
	}
	state.FirmwareVersion = fw.Version
	resolved, err := resolveWave(ctx, state, 0)
	if err != nil {
		return err
	}
	state.startWave(0, resolved)

	signals := rolloutSignals{
		approvals: workflow.GetSignalChannel(ctx, ApproveNextWaveSignalName),
		pauses:    workflow.GetSignalChannel(ctx, PauseRolloutSignalName),
		resumes:   workflow.GetSignalChannel(ctx, ResumeRolloutSignalName),
	}
	updateCtx, cancelUpdates := workflow.WithCancel(ctx)
	updates := updateWave(updateCtx, state, 0, resolved, fw)
	if _, err := collectUpdates(ctx, signals, &state, resolved, updates, cancelUpdates); err != nil {
		cancelUpdates()
		return err
	}
	cancelUpdates()

	state.beginRollback(0, OutcomeUnhealthyWave, nil)
	if err := runRollbackPlan(ctx, &state); err != nil {
		return err
	}
	// The finished plan is made to run its steps again.
	return runRollbackPlan(ctx, &state)
}

// planRunSnapshot is the store and delivery state the first plan run ended with, captured from
// inside the hook that observes its terminal status write. Everything recorded after it belongs to
// the second run.
type planRunSnapshot struct {
	mu       sync.Mutex
	captured bool
	rollout  rollout.RolloutRecord
	commands []CommandIssuedSignal
}

// capture records the fakes' state the first time it is called. The hook fires for every
// non-running status write, and only the first run's terminal write is the one the second run
// follows.
func (s *planRunSnapshot) capture(fakes *rolloutFakes) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.captured {
		return
	}
	rec, _ := fakes.recordedRollout("ro-1")
	s.rollout, s.commands, s.captured = rec, fakes.commandDeliveries(), true
}

// recorded returns what was captured, failing the test when the hook never ran.
func (s *planRunSnapshot) recorded(t *testing.T) (rollout.RolloutRecord, []CommandIssuedSignal) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.captured {
		t.Fatal("the first plan run never recorded a terminal status")
	}
	return s.rollout, s.commands
}

// TestRerunningAFinishedPlanChangesNothing backs "Re-running a finished plan changes nothing": a
// plan that has completed and is made to run its steps again either finds each device already on
// the version it restored or commands it again under the command id it already carried — no device
// gets a new command id, and the recorded rollback does not regress.
func TestRerunningAFinishedPlanChangesNothing(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2", "dev-3")
	fakes.addFirmware(previousFirmware())
	// dev-1 and dev-2 conclude their restore on the first run; dev-3 takes the firmware and
	// never reports its restore, so the second run has to restore it again.
	fakes.devices().succeedUpdateThenRestore("dev-1", OutcomeSucceeded, "")
	fakes.devices().succeedUpdateThenRestore("dev-2", OutcomeSucceeded, "")
	fakes.devices().succeedUpdateThenRestore("dev-3", "", "")

	env := newRolloutEnv(fakes)
	env.RegisterWorkflowWithOptions(rerunPlanWorkflow,
		workflow.RegisterOptions{Name: rerunPlanWorkflowName})
	firstRun := &planRunSnapshot{}
	fakes.observeControlRecord(func(status rollout.RolloutStatus) {
		if status == rollout.RolloutRolledBack {
			firstRun.capture(fakes)
		}
	})

	env.ExecuteWorkflow(rerunPlanWorkflowName, rolloutInputFor(settings))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("rerun workflow error: %v", err)
	}

	rollbackRecorded, firstCommands := firstRun.recorded(t)
	delivered := fakes.commandDeliveries()
	if len(delivered) < len(firstCommands) {
		t.Fatalf("recorded %d deliveries, want at least the first run's %d",
			len(delivered), len(firstCommands))
	}
	secondRun := delivered[len(firstCommands):]
	// The second run ran: it published the plan's announcements again and left the deliveries
	// behind, even though the store refused to move a concluded rollout.
	if len(secondRun) == 0 {
		t.Fatal("the second run delivered nothing, want it to have run the plan's steps")
	}
	if announcements := fakes.recordedAnnouncements(); len(announcements) != 4 {
		t.Errorf("announcements = %d, want two phases from each of the plan's two runs",
			len(announcements))
	}

	// No device is commanded with a new id: every restore of the second run repeats a command id
	// the first run already delivered.
	firstIDs := make([]string, 0, len(firstCommands))
	for _, cmd := range firstCommands {
		firstIDs = append(firstIDs, cmd.CommandID)
	}
	for _, cmd := range secondRun {
		if !slices.Contains(firstIDs, cmd.CommandID) {
			t.Errorf("the second run commanded %s with the new id %q, want the id it already had",
				cmd.DeviceID, cmd.CommandID)
		}
	}

	// Every compensated device is either still on the version the plan restored it to or was
	// restored again under that same command id.
	wave, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100))
	if !ok {
		t.Fatal("the wave the plan compensates was never recorded")
	}
	for _, deviceID := range wave.DeviceIDs {
		if got := fakes.devices().firmwareOf(deviceID); got == previousFirmware().Version {
			continue
		}
		restoredAgain := slices.ContainsFunc(secondRun, func(cmd CommandIssuedSignal) bool {
			return cmd.DeviceID == deviceID
		})
		if !restoredAgain {
			t.Errorf("device %s runs %s after the second run and was not restored again",
				deviceID, fakes.devices().firmwareOf(deviceID))
		}
	}

	// The recorded rollback does not regress: the document the first run concluded with is the
	// document that stands, carrying what the first run achieved rather than the second run's
	// outcomes.
	final, ok := fakes.recordedRollout("ro-1")
	if !ok {
		t.Fatal("the rollout was never recorded")
	}
	if diff := cmp.Diff(rollbackRecorded, final); diff != "" {
		t.Errorf("recorded rollout changed on the second run (-after first +after second):\n%s", diff)
	}
	if final.Rollback == nil {
		t.Fatal("the recorded rollback is missing")
	}
	step := final.Rollback.Steps[1]
	if step.Restored != 2 || step.Unreported != 1 {
		t.Errorf("recorded downgrade = %+v, want the first run's two restored and one unreported",
			step)
	}
}
