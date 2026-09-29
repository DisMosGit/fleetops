package temporal

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/telemetry"
)

// rollbackRestoreTimeout is the result timeout the rollback tests drive: short, so a device that
// never concludes its restore is provable without the suite waiting out the production default.
const rollbackRestoreTimeout = 200 * time.Millisecond

// rollbackTestSettings returns the policy the rollback tests drive: a short health window, so a
// wave concludes quickly, and the sequence the test scripts.
func rollbackTestSettings(resultTimeout time.Duration, waves ...RolloutWave) RolloutSettings {
	settings := rolloutSettingsWith(2*time.Second, 30*time.Second, waves...)
	settings.ResultTimeout = resultTimeout
	return settings
}

// rollbackTestState returns a rollout state driving the given wave sequence, with no wave in flight.
func rollbackTestState(waves ...RolloutWave) rolloutState {
	return newRolloutState(rolloutInputFor(rollbackTestSettings(rollbackRestoreTimeout, waves...)))
}

// dispatchWave records that a wave was resolved to the given devices, the way the workflow records
// it before anything is dispatched.
func dispatchWave(s *rolloutState, position int, deviceIDs ...string) {
	s.startWave(position, ResolvedWave{
		WaveID:    rollout.WaveID(s.RolloutID, position, s.Settings.Waves[position].Percent),
		DeviceIDs: deviceIDs,
	})
}

// TestDeriveRollbackPlan pins the plan's shape: the announcement, the dispatched waves in reverse,
// the reconciliation, and the completion announcement — with nothing contributed by a wave the
// rollout never dispatched, and no plan at all for a rollout with nothing to compensate.
func TestDeriveRollbackPlan(t *testing.T) {
	t.Parallel()

	single := func(kind rollout.RollbackStepKind, waveID string, devices int) RollbackStepView {
		return RollbackStepView{Kind: kind, WaveID: waveID, Status: rollout.RollbackStepPending, Devices: devices}
	}
	pending := func(kind rollout.RollbackStepKind) RollbackStepView {
		return RollbackStepView{Kind: kind, Status: rollout.RollbackStepPending}
	}

	tests := []struct {
		name  string
		build func(*rolloutState)
		want  []RollbackStepView
	}{
		{
			name: "dispatched waves are compensated most recent first",
			build: func(s *rolloutState) {
				dispatchWave(s, 0, "dev-0")
				dispatchWave(s, 1, "dev-1")
				dispatchWave(s, 2, "dev-2", "dev-3")
			},
			want: []RollbackStepView{
				pending(rollout.RollbackNotifyStarted),
				single(rollout.RollbackDowngrade, "ro-1-w2-25", 2),
				single(rollout.RollbackDowngrade, "ro-1-w1-5", 1),
				single(rollout.RollbackDowngrade, "ro-1-w0-1", 1),
				{Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepPending, Devices: 4},
				pending(rollout.RollbackNotifyCompleted),
			},
		},
		{
			name: "a share that resolved to no device contributes no step",
			build: func(s *rolloutState) {
				dispatchWave(s, 0, "dev-0")
				dispatchWave(s, 1)
			},
			want: []RollbackStepView{
				pending(rollout.RollbackNotifyStarted),
				single(rollout.RollbackDowngrade, "ro-1-w0-1", 1),
				{Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepPending, Devices: 1},
				pending(rollout.RollbackNotifyCompleted),
			},
		},
		{
			name: "a wave the rollout never reached contributes no step",
			build: func(s *rolloutState) {
				dispatchWave(s, 0, "dev-0", "dev-1")
			},
			want: []RollbackStepView{
				pending(rollout.RollbackNotifyStarted),
				single(rollout.RollbackDowngrade, "ro-1-w0-1", 2),
				{Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepPending, Devices: 2},
				pending(rollout.RollbackNotifyCompleted),
			},
		},
		{
			name:  "a rollout that dispatched nothing derives no plan",
			build: func(*rolloutState) {},
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := rollbackTestState(
				RolloutWave{Percent: 1}, RolloutWave{Percent: 5},
				RolloutWave{Percent: 25}, RolloutWave{Percent: 100},
			)
			tc.build(&state)

			plan := deriveRollbackPlan(state)
			progress := &rollbackProgress{Steps: plan}
			if tc.want == nil {
				if len(plan) != 0 {
					t.Fatalf("plan = %+v, want none for a rollout with nothing to compensate", plan)
				}
				return
			}
			if diff := cmp.Diff(tc.want, progress.view().Plan); diff != "" {
				t.Errorf("plan mismatch (-want +got):\n%s", diff)
			}
			// The same state derives the identical plan: the plan is a pure function of the
			// rollout's own recorded progress, which is what makes a replay a non-event.
			again := &rollbackProgress{Steps: deriveRollbackPlan(state)}
			if diff := cmp.Diff(progress.view().Plan, again.view().Plan); diff != "" {
				t.Errorf("re-derived plan differs (-first +second):\n%s", diff)
			}
		})
	}
}

// TestRollbackPlanRunsAndIsRecorded drives a real rollback through the whole plan: an unhealthy
// wave ends the rollout, the waves it dispatched are compensated most recent first, the inventory
// is reconciled, both announcements are published in plan order, and the terminal status is
// recorded only once every step has run.
func TestRollbackPlanRunsAndIsRecorded(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(
		rollbackRestoreTimeout,
		RolloutWave{Percent: 1}, RolloutWave{Percent: 5}, RolloutWave{Percent: 100},
	)
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(200)...)
	fakes.addFirmware(previousFirmware())
	// Every device takes the firmware a wave commands and then concludes the rollback's restore.
	fakes.devices().concludeRestores(OutcomeSucceeded, deviceIDs(200)...)
	// The first wave is promoted; the second fails its gate and ends the rollout.
	fakes.scriptHealth(healthyAt(0.99), unhealthyAt(0.4))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeUnhealthyWave {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/unhealthy_wave", view.Status, view.Outcome)
	}
	if view.Rollback == nil {
		t.Fatal("a rolled-back rollout reports no rollback")
	}

	// The first wave moved two devices and the second eight: the plan compensates the second
	// first, and the reconciliation covers all ten.
	wave1 := rollout.WaveID("ro-1", 1, 5)
	wave0 := rollout.WaveID("ro-1", 0, 1)
	wantPlan := []RollbackStepView{
		{Kind: rollout.RollbackNotifyStarted, Status: rollout.RollbackStepCompleted},
		{
			Kind: rollout.RollbackDowngrade, WaveID: wave1, Status: rollout.RollbackStepCompleted,
			Devices: 8, Restored: 8,
		},
		{
			Kind: rollout.RollbackDowngrade, WaveID: wave0, Status: rollout.RollbackStepCompleted,
			Devices: 2, Restored: 2,
		},
		{
			// The fleet recorded the firmware the rollout deployed, so the reconciliation
			// corrects every device the plan restored.
			Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepCompleted,
			Devices: 10, Corrected: 10,
		},
		{Kind: rollout.RollbackNotifyCompleted, Status: rollout.RollbackStepCompleted},
	}
	if diff := cmp.Diff(wantPlan, view.Rollback.Plan); diff != "" {
		t.Errorf("rollback plan mismatch (-want +got):\n%s", diff)
	}
	wantInventory := []FirmwareCount{{Version: previousFirmware().Version, Devices: 10}}
	if diff := cmp.Diff(wantInventory, view.Rollback.Inventory); diff != "" {
		t.Errorf("reconciled inventory mismatch (-want +got):\n%s", diff)
	}
	if len(view.Rollback.UnrestoredDeviceIDs) != 0 {
		t.Errorf("unrestored devices = %v, want none", view.Rollback.UnrestoredDeviceIDs)
	}

	// The steps ran in plan order: the most recently dispatched wave's devices were restored
	// before the promoted wave's, which is what makes the plan a saga rather than a checklist.
	restores := restoreDeliveries(fakes.commandDeliveries())
	if got := len(restores); got != 10 {
		t.Fatalf("restore commands = %d, want 10", got)
	}
	second := deviceIDs(200)[2:10]
	first := deviceIDs(200)[:2]
	if lastOf(restores, second) > firstOf(restores, first) {
		t.Errorf("restore order = %v, want the second wave's devices before the first wave's",
			restores)
	}

	// Both announcements were published, in plan order, and the completion carries what the
	// compensations achieved.
	announcements := fakes.recordedAnnouncements()
	if len(announcements) != 2 {
		t.Fatalf("announcements = %d, want the started and completed phases", len(announcements))
	}
	started, completed := announcements[0], announcements[1]
	if started.Phase != telemetry.RollbackStarted || completed.Phase != telemetry.RollbackCompleted {
		t.Fatalf("announcement phases = %s, %s, want started then completed", started.Phase, completed.Phase)
	}
	if started.EventID != telemetry.RollbackEventID("ro-1", telemetry.RollbackStarted) ||
		completed.EventID != telemetry.RollbackEventID("ro-1", telemetry.RollbackCompleted) {
		t.Errorf("announcement identities = %s, %s, want the rollout and phase they derive from",
			started.EventID, completed.EventID)
	}
	if started.Progress != nil {
		t.Errorf("started announcement carries progress %+v, want none before any compensation",
			started.Progress)
	}
	if started.Rollout.PlanSteps != 5 || started.Rollout.PlanDevices != 10 {
		t.Errorf("started announcement plan = %d steps/%d devices, want 5/10",
			started.Rollout.PlanSteps, started.Rollout.PlanDevices)
	}
	if started.Rollout.WaveID != wave1 || started.Rollout.Outcome != string(OutcomeUnhealthyWave) {
		t.Errorf("started announcement names %s/%s, want %s/%s",
			started.Rollout.WaveID, started.Rollout.Outcome, wave1, OutcomeUnhealthyWave)
	}
	if started.Rollout.Decision == nil || started.Rollout.Decision.Verdict != "unhealthy" {
		t.Errorf("started announcement decision = %+v, want the wave's unhealthy measurement",
			started.Rollout.Decision)
	}
	if completed.Progress == nil {
		t.Fatal("completed announcement carries no progress")
	}
	if completed.Progress.Restored != 10 || completed.Progress.Unreported != 0 {
		t.Errorf("completed progress = %+v, want 10 restored", completed.Progress)
	}
	if diff := cmp.Diff(
		[]telemetry.FirmwareInventoryEntry{{Version: previousFirmware().Version, Devices: 10}},
		completed.Progress.Inventory,
	); diff != "" {
		t.Errorf("announced inventory mismatch (-want +got):\n%s", diff)
	}

	// The rollout document carries the same rollback, and its terminal status was written only
	// after the plan had run: the compensating phase precedes it in the recorded sequence.
	recorded, ok := fakes.recordedRollout("ro-1")
	if !ok {
		t.Fatal("the rollout was never recorded")
	}
	if recorded.Status != rollout.RolloutRolledBack {
		t.Errorf("recorded status = %s, want rolled_back", recorded.Status)
	}
	if recorded.Rollback == nil || recorded.Rollback.Outcome != string(OutcomeUnhealthyWave) {
		t.Fatalf("recorded rollback = %+v, want the outcome that ended the rollout", recorded.Rollback)
	}
	if len(recorded.Rollback.Steps) != 5 {
		t.Errorf("recorded steps = %d, want 5", len(recorded.Rollback.Steps))
	}
	if recorded.Rollback.Steps[1].WaveID != wave1 || recorded.Rollback.Steps[1].Restored != 8 {
		t.Errorf("recorded first downgrade = %+v, want the second wave's eight devices",
			recorded.Rollback.Steps[1])
	}
	statuses := fakes.rolloutStatuses()
	if len(statuses) == 0 || statuses[len(statuses)-1] != rollout.RolloutRolledBack {
		t.Fatalf("recorded statuses = %v, want the terminal status last", statuses)
	}
	if !slicesContains(statuses, rollout.RolloutRollingBack) {
		t.Errorf("recorded statuses = %v, want the compensating phase recorded", statuses)
	}

	// The fleet's records now agree with the devices: every compensated device is recorded on
	// the version the plan restored it to.
	for _, deviceID := range deviceIDs(200)[:10] {
		if got := fakes.fleet[deviceID]; got != previousFirmware().Version {
			t.Errorf("device %s recorded on %s, want %s", deviceID, got, previousFirmware().Version)
		}
	}
}

// TestRollbackRecordsWhatItCouldNotRestore pins the per-device outcomes of one downgrade step: a
// device that is restored, one that never took the firmware, and one that never reports are each
// recorded once, and only the devices left on the bad firmware are reported as unrestored.
func TestRollbackRecordsWhatItCouldNotRestore(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2", "dev-3")
	fakes.addFirmware(previousFirmware())
	// dev-1 takes the firmware and is restored, dev-2 never takes it, and dev-3 takes it and
	// never reports its restore.
	fakes.devices().succeedUpdateThenRestore("dev-1", OutcomeSucceeded, "")
	fakes.devices().failWith("checksum mismatch", "dev-2")
	fakes.devices().succeedUpdateThenRestore("dev-3", "", "")
	fakes.scriptHealth(unhealthyAt(0.4))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack {
		t.Fatalf("rollout concluded as %s, want rolled_back", view.Status)
	}
	wantStep := RollbackStepView{
		Kind: rollout.RollbackDowngrade, WaveID: rollout.WaveID("ro-1", 0, 100),
		Status: rollout.RollbackStepCompleted, Devices: 3,
		Restored: 1, Skipped: 1, Unreported: 1,
	}
	if diff := cmp.Diff(wantStep, view.Rollback.Plan[1]); diff != "" {
		t.Errorf("downgrade step mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"dev-3"}, view.Rollback.UnrestoredDeviceIDs); diff != "" {
		t.Errorf("unrestored devices mismatch (-want +got):\n%s", diff)
	}
	// The unrestored device is still on the deployed firmware, which is exactly what the
	// inventory reports: the two restored devices on the old version, dev-3 on the new one.
	wantInventory := []FirmwareCount{
		{Version: previousFirmware().Version, Devices: 2},
		{Version: rolloutTestFirmware().Version, Devices: 1},
	}
	if diff := cmp.Diff(wantInventory, view.Rollback.Inventory); diff != "" {
		t.Errorf("reconciled inventory mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(
		RollbackStepView{
			Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepCompleted,
			Devices: 3, Agreed: 1, Corrected: 2,
		},
		view.Rollback.Plan[2],
	); diff != "" {
		t.Errorf("reconciliation step mismatch (-want +got):\n%s", diff)
	}

	// The completion announcement reports the same: what was restored, what was skipped, what
	// was never reported, and which devices are still on the bad firmware.
	announcements := fakes.recordedAnnouncements()
	progress := announcements[len(announcements)-1].Progress
	if progress == nil {
		t.Fatal("the completion announcement carries no progress")
	}
	if progress.Restored != 1 || progress.Skipped != 1 || progress.Unreported != 1 {
		t.Errorf("announced progress = %+v, want one restored, one skipped, one unreported", progress)
	}
	if diff := cmp.Diff([]string{"dev-3"}, progress.UnrestoredDeviceIDs); diff != "" {
		t.Errorf("announced unrestored devices mismatch (-want +got):\n%s", diff)
	}
}

// TestRollbackAnnouncementFailureIsRecordedAgainstItsStep pins that an announcement the broker never
// took is a failed step and nothing more: the compensations still run, they are still recorded, and
// the rollout still concludes with the outcome that ended it.
func TestRollbackAnnouncementFailureIsRecordedAgainstItsStep(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1")
	fakes.addFirmware(previousFirmware())
	fakes.devices().succeedUpdateThenRestore("dev-1", OutcomeSucceeded, "")
	fakes.scriptHealth(unhealthyAt(0.4))
	fakes.announceErr = errors.New("broker is unreachable")

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeUnhealthyWave {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/unhealthy_wave", view.Status, view.Outcome)
	}
	if len(fakes.recordedAnnouncements()) != 0 {
		t.Error("an announcement was recorded although the broker refused every publication")
	}
	for _, position := range []int{0, len(view.Rollback.Plan) - 1} {
		step := view.Rollback.Plan[position]
		if step.Status != rollout.RollbackStepFailed {
			t.Errorf("announcement step %d status = %s, want failed", position, step.Status)
		}
		if !strings.Contains(step.Detail, "broker is unreachable") {
			t.Errorf("announcement step %d detail = %q, want the publication failure", position, step.Detail)
		}
	}
	// The compensations between the two announcements still ran and are still recorded.
	recorded, _ := fakes.recordedRollout("ro-1")
	for _, step := range recorded.Rollback.Steps[1 : len(recorded.Rollback.Steps)-1] {
		if step.Status != rollout.RollbackStepCompleted {
			t.Errorf("step %s recorded as %s, want it unaffected by the failed announcements",
				step.Kind, step.Status)
		}
	}
}

// TestRollbackWithoutAMeasuredDecisionNamesNone pins the announcement of a rollout that could not
// measure its failing wave: a dispatch failure has no verdict, and the event carries none rather
// than a fabricated one.
func TestRollbackWithoutAMeasuredDecisionNamesNone(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	fakes.addFirmware(previousFirmware())
	// The wave's commands cannot be delivered at all: it fails before it can be measured, and
	// the rollback's own restores cannot be delivered either.
	fakes.devices().refuseDelivery("dev-1", "dev-2")

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeDispatchFailed {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/dispatch_failed", view.Status, view.Outcome)
	}
	announcements := fakes.recordedAnnouncements()
	if len(announcements) != 2 {
		t.Fatalf("announcements = %d, want both phases", len(announcements))
	}
	if announcements[0].Rollout.Decision != nil {
		t.Errorf("started announcement decision = %+v, want none for an unmeasured wave",
			announcements[0].Rollout.Decision)
	}
	// One outcome per device even when nothing could be delivered, and the plan ran to its end.
	wantStep := RollbackStepView{
		Kind: rollout.RollbackDowngrade, WaveID: rollout.WaveID("ro-1", 0, 100),
		Status: rollout.RollbackStepCompleted, Devices: 2, Unavailable: 2,
	}
	if diff := cmp.Diff(wantStep, view.Rollback.Plan[1]); diff != "" {
		t.Errorf("downgrade step mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"dev-1", "dev-2"}, view.Rollback.UnrestoredDeviceIDs); diff != "" {
		t.Errorf("unrestored devices mismatch (-want +got):\n%s", diff)
	}
	progress := announcements[1].Progress
	if progress == nil || progress.Unavailable != 2 || progress.Restored != 0 {
		t.Errorf("completed progress = %+v, want both devices reported unavailable", progress)
	}
}

// TestRollbackReportsItsProgressWithoutWaiting pins the mid-plan picture: while the plan is
// compensating, the state query answers immediately with the compensating status, the steps already
// recorded, the step in progress, and the wave each step compensates.
func TestRollbackReportsItsProgressWithoutWaiting(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(
		rollbackRestoreTimeout,
		RolloutWave{Percent: 1}, RolloutWave{Percent: 5}, RolloutWave{Percent: 100},
	)
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(200)...)
	fakes.addFirmware(previousFirmware())
	fakes.devices().concludeRestores(OutcomeSucceeded, deviceIDs(200)...)
	fakes.scriptHealth(healthyAt(0.99), unhealthyAt(0.4))

	env := newRolloutEnv(fakes)
	// The second downgrade step is the moment to look: the first step's outcome is recorded, the
	// second is the one in progress, and the plan's remaining steps are still pending.
	var midPlan RolloutView
	fakes.observeControlRecord(func(status rollout.RolloutStatus) {
		if status != rollout.RolloutRollingBack {
			return
		}
		value, err := env.QueryWorkflow(GetRolloutStateQueryType)
		if err != nil {
			t.Errorf("query the compensating rollout: %v", err)
			return
		}
		var view RolloutView
		if err := value.Get(&view); err != nil {
			t.Errorf("decode the compensating rollout: %v", err)
			return
		}
		// The moment the spec describes: the first downgrade step has been recorded and the
		// second is the one in progress.
		if view.Rollback != nil && len(view.Rollback.Plan) == 5 &&
			view.Rollback.Plan[1].Status == rollout.RollbackStepCompleted &&
			view.Rollback.Plan[2].Status == rollout.RollbackStepRunning {
			midPlan = view
		}
	})

	view := runRollout(t, env, rolloutInputFor(settings))

	if midPlan.Status != rollout.RolloutRollingBack {
		t.Fatalf("mid-plan status = %s, want rolling_back", midPlan.Status)
	}
	if midPlan.Rollback == nil || len(midPlan.Rollback.Plan) != 5 {
		t.Fatalf("mid-plan rollback = %+v, want the five-step plan", midPlan.Rollback)
	}
	if midPlan.Rollback.Plan[1].Status != rollout.RollbackStepCompleted {
		t.Errorf("mid-plan first downgrade = %s, want completed", midPlan.Rollback.Plan[1].Status)
	}
	if midPlan.Rollback.Plan[2].Status != rollout.RollbackStepRunning {
		t.Errorf("mid-plan second downgrade = %s, want running", midPlan.Rollback.Plan[2].Status)
	}
	if midPlan.Rollback.Plan[3].Status != rollout.RollbackStepPending {
		t.Errorf("mid-plan reconciliation = %s, want pending", midPlan.Rollback.Plan[3].Status)
	}
	if midPlan.Rollback.Plan[2].WaveID != rollout.WaveID("ro-1", 0, 1) {
		t.Errorf("mid-plan second downgrade compensates %s, want the first wave",
			midPlan.Rollback.Plan[2].WaveID)
	}

	if view.Status != rollout.RolloutRolledBack {
		t.Fatalf("rollout concluded as %s, want rolled_back", view.Status)
	}
	if statuses := fakes.rolloutStatuses(); slicesContains(statuses, rollout.RolloutPaused) {
		t.Errorf("recorded statuses = %v, want no pause recorded during the rollback", statuses)
	}
	if view.Rollback.Plan[2].WaveID != rollout.WaveID("ro-1", 0, 1) {
		t.Errorf("concluded plan lost the wave the second downgrade compensates")
	}
}

// restoreDeliveries returns the restore commands among the seam's deliveries, in delivery order.
// A restore is an update command addressed by a rollback command id.
func restoreDeliveries(delivered []CommandIssuedSignal) []CommandIssuedSignal {
	var restores []CommandIssuedSignal
	for _, cmd := range delivered {
		if strings.HasPrefix(cmd.CommandID, "rollback-") {
			restores = append(restores, cmd)
		}
	}
	return restores
}

// firstOf returns the delivery position of the first command in devices, or -1 when none was
// delivered.
func firstOf(restores []CommandIssuedSignal, devices []string) int {
	for i, cmd := range restores {
		if stringsContain(devices, cmd.DeviceID) {
			return i
		}
	}
	return -1
}

// lastOf returns the delivery position of the last command in devices, or -1 when none was
// delivered.
func lastOf(restores []CommandIssuedSignal, devices []string) int {
	for i := len(restores) - 1; i >= 0; i-- {
		if stringsContain(devices, restores[i].DeviceID) {
			return i
		}
	}
	return -1
}

// stringsContain reports whether the slice holds the value.
func stringsContain(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// slicesContains reports whether the slice holds the value.
func slicesContains[T comparable](values []T, want T) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// TestRolloutReportedStatus pins the projected status of every phase a rollout can be in: a
// compensating rollout reports the phase, not the operator's hold, and no terminal status is
// projected before the plan has run.
func TestRolloutReportedStatus(t *testing.T) {
	t.Parallel()

	begin := func(s *rolloutState) {
		dispatchWave(s, 0, "dev-1")
		s.beginRollback(0, OutcomeUnhealthyWave, nil)
	}
	finish := func(s *rolloutState) {
		begin(s)
		s.finishRollback()
	}

	tests := []struct {
		name  string
		apply func(*rolloutState)
		want  rollout.RolloutStatus
	}{
		{name: "a running rollout reports running", apply: func(*rolloutState) {}, want: rollout.RolloutRunning},
		{name: "a paused rollout reports the pause", apply: func(s *rolloutState) { s.pause() }, want: rollout.RolloutPaused},
		{
			name:  "a rollout holding for approval reports the hold",
			apply: func(s *rolloutState) { s.holdForApproval(0) },
			want:  rollout.RolloutAwaitingApproval,
		},
		{name: "a compensating rollout reports the phase", apply: begin, want: rollout.RolloutRollingBack},
		{
			name:  "a compensating rollout is not held by a pause folded before it",
			apply: func(s *rolloutState) { s.pause(); begin(s) },
			want:  rollout.RolloutRollingBack,
		},
		{
			name:  "a compensating rollout ignores a pause folded after it began",
			apply: func(s *rolloutState) { begin(s); s.pause() },
			want:  rollout.RolloutRollingBack,
		},
		{name: "a rolled-back rollout reports the terminal status", apply: finish, want: rollout.RolloutRolledBack},
		{
			name:  "a rolled-back rollout ignores a pause",
			apply: func(s *rolloutState) { finish(s); s.pause() },
			want:  rollout.RolloutRolledBack,
		},
		{name: "a completed rollout reports completed", apply: func(s *rolloutState) { s.complete() }, want: rollout.RolloutCompleted},
		{
			name:  "a failed rollout reports failed",
			apply: func(s *rolloutState) { s.fail(OutcomeFirmwareUnknown) },
			want:  rollout.RolloutFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := rollbackTestState(RolloutWave{Percent: 100})
			tc.apply(&state)
			if got := state.reported(); got != tc.want {
				t.Errorf("reported status = %s, want %s", got, tc.want)
			}
			if tc.want == rollout.RolloutRollingBack {
				if state.terminal() {
					t.Error("a compensating rollout reports itself as terminal")
				}
				if state.Status != rollout.RolloutRollingBack {
					t.Errorf("compensating status = %s, want rolling_back", state.Status)
				}
			}
		})
	}
}

// TestRollbackStateControlChangesNothing pins that once a rollout has begun compensating, a pause, a
// resume, and an approval leave its state — and everything it reports — exactly as it was.
func TestRollbackStateControlChangesNothing(t *testing.T) {
	t.Parallel()

	state := rollbackTestState(RolloutWave{Percent: 100})
	dispatchWave(&state, 0, "dev-1")
	state.beginRollback(0, OutcomeUnhealthyWave, nil)
	before := state.view()

	state.pause()
	state.resume()
	state.applyApproval()

	if state.Paused {
		t.Error("a pause folded during the rollback set the hold")
	}
	if state.ApprovalOutstanding {
		t.Error("an approval folded during the rollback was banked")
	}
	if diff := cmp.Diff(before, state.view()); diff != "" {
		t.Errorf("control signals changed the compensating rollout (-before +after):\n%s", diff)
	}
}

// TestRollbackRecordMapping pins the document the workflow writes for its rollback: nothing for a
// rollout that never entered rollback, the plan's own state while it runs, and what every step
// achieved once it has run.
func TestRollbackRecordMapping(t *testing.T) {
	t.Parallel()

	t.Run("a rollout that never entered rollback writes no record", func(t *testing.T) {
		t.Parallel()

		state := rollbackTestState(RolloutWave{Percent: 100})
		if got := state.Rollback.record(); got != nil {
			t.Errorf("record = %+v, want none", got)
		}
	})

	t.Run("a compensating rollout writes its plan as it runs", func(t *testing.T) {
		t.Parallel()

		state := rollbackTestState(RolloutWave{Percent: 100})
		dispatchWave(&state, 0, "dev-1", "dev-2")
		state.beginRollback(0, OutcomeUnhealthyWave, nil)
		state.Rollback.startStep(0)
		state.Rollback.completeStep(0)

		got := state.Rollback.record()
		if got.Outcome != string(OutcomeUnhealthyWave) {
			t.Errorf("recorded outcome = %q, want %q", got.Outcome, OutcomeUnhealthyWave)
		}
		if len(got.Steps) != 4 {
			t.Fatalf("recorded steps = %d, want 4", len(got.Steps))
		}
		if got.Steps[0].Status != rollout.RollbackStepCompleted ||
			got.Steps[1].Status != rollout.RollbackStepPending {
			t.Errorf("recorded step statuses = %s, %s, want the first completed and the second pending",
				got.Steps[0].Status, got.Steps[1].Status)
		}
		if got.Steps[1].Devices != 2 || got.Steps[1].WaveID != rollout.WaveID("ro-1", 0, 100) {
			t.Errorf("recorded downgrade step = %+v, want the wave's two devices", got.Steps[1])
		}
		if len(got.Inventory) != 0 || len(got.UnrestoredDeviceIDs) != 0 {
			t.Errorf("recorded inventory = %v and unrestored = %v, want both empty mid-plan",
				got.Inventory, got.UnrestoredDeviceIDs)
		}
	})

	t.Run("a concluded rollback writes what every step achieved", func(t *testing.T) {
		t.Parallel()

		state := rollbackTestState(RolloutWave{Percent: 100})
		dispatchWave(&state, 0, "dev-1", "dev-2", "dev-3")
		state.beginRollback(0, OutcomeUnhealthyWave, nil)
		state.Rollback.recordDowngrade(1, []DeviceRestore{
			{DeviceID: "dev-1", Outcome: RestoreRestored},
			{DeviceID: "dev-2", Outcome: RestoreSkipped},
			{DeviceID: "dev-3", Outcome: RestoreUnreported},
		})
		state.Rollback.recordReconciliation(2, []DeviceInventory{
			{DeviceID: "dev-1", Outcome: InventoryCorrected, Version: "1.0.0"},
			{DeviceID: "dev-2", Outcome: InventoryAgreed, Version: "1.0.0"},
			{DeviceID: "dev-3", Outcome: InventoryUnverified, Detail: "no record"},
		})
		state.Rollback.completeStep(0)
		state.Rollback.completeStep(3)
		state.finishRollback()

		got := state.Rollback.record()
		downgrade := got.Steps[1]
		if downgrade.Restored != 1 || downgrade.Skipped != 1 || downgrade.Unreported != 1 {
			t.Errorf("recorded downgrade counts = %+v, want one of each outcome", downgrade)
		}
		reconcile := got.Steps[2]
		if reconcile.Agreed != 1 || reconcile.Corrected != 1 || reconcile.Unverified != 1 {
			t.Errorf("recorded reconciliation counts = %+v, want one of each outcome", reconcile)
		}
		if diff := cmp.Diff(
			[]rollout.FirmwareInventoryRecord{{Version: "1.0.0", Devices: 2}},
			got.Inventory,
		); diff != "" {
			t.Errorf("recorded inventory mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]string{"dev-3"}, got.UnrestoredDeviceIDs); diff != "" {
			t.Errorf("recorded unrestored devices mismatch (-want +got):\n%s", diff)
		}
		// The document and the state query are built from one state, so they cannot disagree.
		if diff := cmp.Diff(state.Rollback.view().UnrestoredDeviceIDs, got.UnrestoredDeviceIDs); diff != "" {
			t.Errorf("the query and the document disagree (-query +document):\n%s", diff)
		}
	})
}

// TestRollbackStopsWhenThePlanCannotBeRecorded pins the one failure that does stop a rollback: a
// record write that cannot succeed leaves the plan's progress unrecorded, so the run fails loudly
// instead of compensating invisibly. The rollback has already been recorded by then, so the store
// going down is what the next write meets.
func TestRollbackStopsWhenThePlanCannotBeRecorded(t *testing.T) {
	t.Parallel()

	// compensating is a rollout that rolls back on the one wave it dispatches.
	compensating := func() (*rolloutFakes, RolloutSettings) {
		settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1")
		fakes.addFirmware(previousFirmware())
		fakes.devices().succeedUpdateThenRestore("dev-1", OutcomeSucceeded, "")
		fakes.scriptHealth(unhealthyAt(0.4))
		return fakes, settings
	}

	t.Run("the rollback itself cannot be recorded", func(t *testing.T) {
		t.Parallel()

		fakes, settings := compensating()
		// The gate's write is the last one before the rollback: from there the store is down,
		// and the rollback's own record is the write that meets it.
		fakes.observeWaveState(func(update rollout.WaveStateUpdate) {
			if update.Status == rollout.WaveUnhealthy {
				fakes.setRecordErr(errors.New("mongo is down"))
			}
		})

		env := newRolloutEnv(fakes)
		env.ExecuteWorkflow(RolloutWorkflowName, rolloutInputFor(settings))
		err := env.GetWorkflowError()
		if err == nil || !strings.Contains(err.Error(), "mongo is down") {
			t.Fatalf("workflow error = %v, want the recording failure", err)
		}
	})

	t.Run("the first step of the plan cannot be recorded", func(t *testing.T) {
		t.Parallel()

		fakes, settings := compensating()
		// The store goes down after the rollback has been recorded: the plan's own progress is
		// what can no longer be written.
		written := 0
		fakes.observeControlRecord(func(status rollout.RolloutStatus) {
			if status != rollout.RolloutRollingBack {
				return
			}
			written++
			if written == 1 {
				fakes.setRecordErr(errors.New("mongo is down"))
			}
		})

		env := newRolloutEnv(fakes)
		env.ExecuteWorkflow(RolloutWorkflowName, rolloutInputFor(settings))
		err := env.GetWorkflowError()
		if err == nil || !strings.Contains(err.Error(), "mongo is down") {
			t.Fatalf("workflow error = %v, want the recording failure", err)
		}
	})
}

// TestRolloutFailsWhenItCannotRecordItsStart pins that a rollout whose very first write cannot
// succeed fails rather than running unrecorded: an operator has to be able to find the run that
// owns a rollout, so a store that refuses it stops it.
func TestRolloutFailsWhenItCannotRecordItsStart(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1")
	fakes.recordErr = errors.New("mongo is down")

	env := newRolloutEnv(fakes)
	env.ExecuteWorkflow(RolloutWorkflowName, rolloutInputFor(settings))
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "mongo is down") {
		t.Fatalf("workflow error = %v, want the recording failure", err)
	}
	if announcements := fakes.recordedAnnouncements(); len(announcements) != 0 {
		t.Errorf("announcements = %d, want none for a rollout that never started", len(announcements))
	}
}
