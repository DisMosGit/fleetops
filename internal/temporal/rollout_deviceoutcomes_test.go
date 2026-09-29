package temporal

import (
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// rolloutUnreportedTimeout is the result timeout the tests that prove an unreported device drive.
// It is short so the wait is provable without the suite spending the production default, and long
// enough for a device scripted to report late to be found first.
const rolloutUnreportedTimeout = 200 * time.Millisecond

// unreportedSettings returns a one-wave policy whose result timeout is short enough to wait out.
func unreportedSettings(resultTimeout time.Duration) RolloutSettings {
	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	settings.ResultTimeout = resultTimeout
	return settings
}

// TestRolloutWorkflowRecordsPerDeviceOutcomes pins what a wave records once its devices have
// settled: the devices that reported a failed update and the devices that never reported, both on
// the workflow's own view and on the wave document it writes.
func TestRolloutWorkflowRecordsPerDeviceOutcomes(t *testing.T) {
	t.Parallel()

	settings := unreportedSettings(rolloutUnreportedTimeout)
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	// One device takes the update, one fails it with a detail, and two never report at all.
	fakes.devices().failWith("flash error", "dev-0001")
	fakes.devices().deleteConclusion("dev-0002", "dev-0003")
	fakes.scriptHealth(healthyAt(0.75))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	waveID := rollout.WaveID("ro-1", 0, 100)
	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed: the failures are recorded, not gating",
			view.Status)
	}
	if len(view.Waves) != 1 {
		t.Fatalf("view carries %d waves, want 1", len(view.Waves))
	}
	got := view.Waves[0]
	if got.TargetCount != 4 {
		t.Errorf("target count = %d, want 4", got.TargetCount)
	}
	if got.FailedCount != 1 {
		t.Errorf("failed count = %d, want 1", got.FailedCount)
	}
	if got.UnreportedCount != 2 {
		t.Errorf("unreported count = %d, want 2", got.UnreportedCount)
	}

	rec, ok := fakes.recordedWave(waveID)
	if !ok {
		t.Fatalf("wave %s was never recorded", waveID)
	}
	if want := []string{"dev-0001"}; !slices.Equal(rec.FailedDeviceIDs, want) {
		t.Errorf("recorded failed_device_ids = %v, want %v", rec.FailedDeviceIDs, want)
	}
	if want := []string{"dev-0002", "dev-0003"}; !slices.Equal(rec.UnreportedDeviceIDs, want) {
		t.Errorf("recorded unreported_device_ids = %v, want %v", rec.UnreportedDeviceIDs, want)
	}
	if rec.Status != rollout.WaveHealthy || rec.SuccessRate != 0.75 {
		t.Errorf("recorded wave = %+v, want it decided healthy at 0.75", rec)
	}

	// Every device was commanded exactly once, under the wave's derived command id: the device
	// that reported on its second delivery was never commanded twice, its second delivery was
	// the activity's own wait re-reading the device.
	wantCommands := []string{"dev-0000", "dev-0001", "dev-0002", "dev-0003"}
	gotCommands := make([]string, 0, len(fakes.recordedCommands()))
	for _, cmd := range fakes.recordedCommands() {
		gotCommands = append(gotCommands, cmd.DeviceID)
		if want := CommandID(waveID, cmd.DeviceID); cmd.CommandID != want {
			t.Errorf("command id = %q, want %q", cmd.CommandID, want)
		}
	}
	if diff := cmp.Diff(wantCommands, gotCommands); diff != "" {
		t.Errorf("commanded devices mismatch (-want +got):\n%s", diff)
	}
}

// TestRolloutWorkflowMixedOutcomesStillGateTheWave pins the promotion boundary this change
// deliberately does not move: a wave with failing devices is still promoted when its measured
// health clears the configured gate, and the failures are recorded rather than turned into a
// second gate.
func TestRolloutWorkflowMixedOutcomesStillGateTheWave(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	// Three devices take the update, one fails it — and the health measurement decides.
	fakes.devices().failWith("checksum mismatch", "dev-0001")
	fakes.scriptHealth(healthyAt(0.9))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	rec, _ := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100))
	if !slices.Equal(rec.FailedDeviceIDs, []string{"dev-0001"}) {
		t.Errorf("recorded failed_device_ids = %v, want [dev-0001]", rec.FailedDeviceIDs)
	}
	if rec.Status != rollout.WaveHealthy {
		t.Errorf("recorded wave status = %q, want %q: the gate decides, not the device count",
			rec.Status, rollout.WaveHealthy)
	}

	// The same wave with a failing measurement rolls the rollout back, and the device outcomes
	// are on the document that ended it.
	failing := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	failing.devices().failWith("checksum mismatch", "dev-0001")
	failing.scriptHealth(unhealthyAt(0.6))

	rolledBack := runRollout(t, newRolloutEnv(failing), rolloutInputFor(settings))
	if rolledBack.Status != rollout.RolloutRolledBack ||
		rolledBack.Outcome != OutcomeUnhealthyWave {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/unhealthy_wave",
			rolledBack.Status, rolledBack.Outcome)
	}
	ended, _ := failing.recordedWave(rolledBack.EndedBy)
	if !slices.Equal(ended.FailedDeviceIDs, []string{"dev-0001"}) {
		t.Errorf("recorded failed_device_ids = %v, want [dev-0001]", ended.FailedDeviceIDs)
	}
}

// TestRolloutWorkflowUnreportedDeviceStopsTheWait pins the bound on a wave's dispatch: a device
// that never reports does not hold the wave open, it is recorded as unreported, and the wave is
// still judged by its gate.
func TestRolloutWorkflowUnreportedDeviceStopsTheWait(t *testing.T) {
	t.Parallel()

	settings := unreportedSettings(rolloutUnreportedTimeout)
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	fakes.devices().deleteConclusion("dev-2")
	fakes.scriptHealth(healthyAt(0.99))

	started := time.Now()
	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))
	elapsed := time.Since(started)

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	if view.Waves[0].UnreportedCount != 1 {
		t.Errorf("unreported count = %d, want 1", view.Waves[0].UnreportedCount)
	}
	rec, _ := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100))
	if !slices.Equal(rec.UnreportedDeviceIDs, []string{"dev-2"}) {
		t.Errorf("recorded unreported_device_ids = %v, want [dev-2]", rec.UnreportedDeviceIDs)
	}
	if rec.Status != rollout.WaveHealthy {
		t.Errorf("recorded wave status = %q, want the gate's %q", rec.Status, rollout.WaveHealthy)
	}
	// The wave stopped waiting at its result timeout rather than at the health window: the
	// window is five minutes and the wait is a fifth of a second.
	if elapsed > time.Minute {
		t.Errorf("the wave waited %s, want it to stop at the result timeout", elapsed)
	}
}

// TestRolloutWorkflowRollsBackWhenADeviceCannotBeCommanded pins the one device outcome that is an
// error rather than a result: a device the rollout could not command fails the wave and rolls the
// rollout back, and the wave's remaining activities are cancelled rather than waited out.
func TestRolloutWorkflowRollsBackWhenADeviceCannotBeCommanded(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2", "dev-3")
	// Every device in the wave is unreachable: the first delivery error the wave awaits fails it,
	// and the devices it had not awaited are cancelled rather than waited out.
	fakes.devices().refuseDelivery("dev-1", "dev-2", "dev-3")

	started := time.Now()
	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))
	elapsed := time.Since(started)

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeDispatchFailed {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/dispatch_failed",
			view.Status, view.Outcome)
	}
	if view.EndedBy != rollout.WaveID("ro-1", 0, 100) {
		t.Errorf("ended by %q, want the wave that could not be delivered", view.EndedBy)
	}
	if view.Decision != nil {
		t.Errorf("decision = %+v, want none for a wave that was never measured", view.Decision)
	}
	rec, ok := fakes.recordedWave(view.EndedBy)
	if !ok || rec.Status != rollout.WaveFailed {
		t.Errorf("recorded wave = %+v (recorded %v), want it recorded as failed", rec, ok)
	}
	if len(fakes.recordedEvaluations()) != 0 {
		t.Error("a wave was evaluated, want no gate after a delivery failure")
	}
	// The wave failed on the delivery error rather than waiting out its uncommanded devices: the
	// wave's result timeout is the production default, so waiting it out would be visible here.
	if elapsed > 30*time.Second {
		t.Errorf("the wave took %s, want the remaining activities cancelled", elapsed)
	}
}

// TestRolloutWorkflowAnswersTheStateQueryWhileAWaveIsInFlight pins what an operator reads while a
// wave is in flight: the query is answered inside the wave's own window, after its devices have
// settled and before its gate has judged it, so the devices that did not take the update are
// already visible while the wave is still `evaluating`.
func TestRolloutWorkflowAnswersTheStateQueryWhileAWaveIsInFlight(t *testing.T) {
	t.Parallel()

	settings := unreportedSettings(2 * time.Second)
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(3)...)
	// dev-0000 concludes successfully and dev-0001 fails immediately; dev-0002 never reports, so
	// the wave stays in its dispatch until the result timeout passes.
	fakes.devices().failWith("flash error", "dev-0001")
	fakes.devices().deleteConclusion("dev-0002")
	fakes.scriptHealth(healthyAt(0.6))

	env := newRolloutEnv(fakes)
	// The query runs as the wave records the outcomes it collected, which is the moment every
	// device of the wave has settled and the wave has not been judged yet.
	midFlight := &scheduledQuery{}
	fakes.observeWaveState(func(update rollout.WaveStateUpdate) {
		if update.Status != rollout.WaveEvaluating {
			return
		}
		value, err := env.QueryWorkflow(GetRolloutStateQueryType)
		if err != nil {
			midFlight.record(RolloutView{}, err)
			return
		}
		var view RolloutView
		if err := value.Get(&view); err != nil {
			midFlight.record(RolloutView{}, err)
			return
		}
		midFlight.record(view, nil)
	})
	view := runRollout(t, env, rolloutInputFor(settings))

	inFlight := midFlight.result(t)
	if inFlight.Status != rollout.RolloutRunning {
		t.Errorf("status in flight = %q, want %q", inFlight.Status, rollout.RolloutRunning)
	}
	if inFlight.Outcome != "" {
		t.Errorf("outcome in flight = %q, want none while the rollout runs", inFlight.Outcome)
	}
	if len(inFlight.Waves) != 1 {
		t.Fatalf("view carries %d waves, want 1", len(inFlight.Waves))
	}
	if inFlight.Waves[0].Status != rollout.WaveEvaluating {
		t.Errorf("wave status in flight = %q, want %q",
			inFlight.Waves[0].Status, rollout.WaveEvaluating)
	}
	if inFlight.Waves[0].TargetCount != 3 {
		t.Errorf("target count in flight = %d, want 3", inFlight.Waves[0].TargetCount)
	}
	if inFlight.Waves[0].FailedCount != 1 {
		t.Errorf("failed count in flight = %d, want 1", inFlight.Waves[0].FailedCount)
	}
	if inFlight.Waves[0].UnreportedCount != 1 {
		t.Errorf("unreported count in flight = %d, want 1: the silent device ran out of time",
			inFlight.Waves[0].UnreportedCount)
	}
	if inFlight.Waves[0].SuccessRate != 0 {
		t.Errorf("success rate in flight = %v, want 0 until the gate decides",
			inFlight.Waves[0].SuccessRate)
	}

	// The same query once the wave was judged reports the decided wave, so the counts read in
	// flight were the wave's own outcomes rather than a half-written record.
	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	if view.Waves[0].Status != rollout.WaveHealthy || view.Waves[0].SuccessRate != 0.6 {
		t.Errorf("wave at conclusion = %+v, want it healthy at the gate's 0.6", view.Waves[0])
	}
	if view.Waves[0].FailedCount != 1 || view.Waves[0].UnreportedCount != 1 {
		t.Errorf("counts at conclusion = %d failed, %d unreported, want 1 and 1",
			view.Waves[0].FailedCount, view.Waves[0].UnreportedCount)
	}
}

// TestRolloutWorkflowFailsOnADeviceThatCannotStart pins the rollout's start refusal against the
// per-device path: a firmware the selector's model is not targeted by fails the rollout before any
// device is commanded, so no update activity ever runs.
func TestRolloutWorkflowFailsOnADeviceThatCannotStart(t *testing.T) {
	t.Parallel()

	settings := rolloutTestSettings()
	in := rolloutInputFor(settings)
	in.Model = "oak-s3-mini"
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1")

	view := runRollout(t, newRolloutEnv(fakes), in)

	if view.Status != rollout.RolloutFailed || view.Outcome != OutcomeFirmwareMismatch {
		t.Fatalf("rollout concluded as %s/%s, want failed/firmware_mismatch",
			view.Status, view.Outcome)
	}
	if commands := fakes.recordedCommands(); len(commands) != 0 {
		t.Errorf("commanded %d devices, want none for a rollout that cannot start", len(commands))
	}
}

// TestRolloutWorkflowDeliveryErrorNamesTheDevice pins what a wave reports when one device cannot
// be commanded at all: every attempt repeats that device's derived command id, the wave is
// recorded failed, and the rollout rolls back rather than promoting a wave it could not command.
func TestRolloutWorkflowDeliveryErrorNamesTheDevice(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	fakes.devices().succeed("dev-1")
	fakes.devices().refuseDelivery("dev-2")
	fakes.scriptHealth(healthyAt(0.99))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	waveID := rollout.WaveID("ro-1", 0, 100)
	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeDispatchFailed {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/dispatch_failed",
			view.Status, view.Outcome)
	}
	if view.EndedBy != waveID {
		t.Errorf("ended by %q, want %q", view.EndedBy, waveID)
	}

	// Every attempt at the unreachable device repeats the same command id, so a device that did
	// accept a command is unaffected by the retry.
	attempts := 0
	for _, cmd := range fakes.recordedCommands() {
		if want := CommandID(waveID, cmd.DeviceID); cmd.CommandID != want {
			t.Errorf("command id = %q, want %q", cmd.CommandID, want)
		}
		if cmd.DeviceID == "dev-2" {
			attempts++
		}
	}
	if attempts < 2 {
		t.Errorf("delivery attempts to the unreachable device = %d, want it retried", attempts)
	}
}
