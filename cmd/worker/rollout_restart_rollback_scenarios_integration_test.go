//go:build integration

package main

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// TestRollbackSurvivesRestartsInEveryCompensation drives a rollout whose second wave regresses into
// a full rollback with a restart injected at every activity of the compensation plan: the rollback
// has to resume at the step it reached, compensate the later wave before the earlier one, and apply
// each compensation once, with one device whose restore fails and one that never reports.
func TestRollbackSurvivesRestartsInEveryCompensation(t *testing.T) {
	world := newRestartWorld(t, worldOptions{
		rolloutID:        "ro-restart-rollback",
		waves:            []temporal.RolloutWave{{Percent: 50}, {Percent: 100}},
		fleet:            3,
		plan:             holdEvery(compensationActivityNames()...),
		sampleResumption: true,
	})
	scripts := world.dispatcher.script()
	scripts.failRestore[world.fleet[1]] = "flash write failed"
	scripts.neverRestore[world.fleet[2]] = true
	world.startRollout()

	// The first wave is promoted on a healthy fleet; the second is judged on a fleet that has gone
	// bad, which is what rolls the rollout back.
	firstWaveID := rollout.WaveID(world.opts.rolloutID, 0, 50)
	if wave := waitForWave(t, world.waves, firstWaveID); wave.Status != rollout.WaveHealthy {
		t.Fatalf("first wave = %+v, want the promoted wave (%d fleet samples)",
			wave, world.fleetSamples())
	}
	world.breakFleet()
	record := awaitConcludedRollback(t, world)
	awaitClosure(t, world)
	secondWave := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 1, 100))
	if secondWave.Status != rollout.WaveUnhealthy {
		t.Errorf("regressing wave status = %q, want unhealthy", secondWave.Status)
	}

	// The recorded plan is the plan the workflow derives, in the order it ran it: the announcement,
	// the later wave's compensation before the earlier one's, the reconciliation, and the
	// completion announcement — each step recorded once, with what it achieved.
	wantPlan := []rollout.RollbackStepRecord{
		{Kind: rollout.RollbackNotifyStarted, Status: rollout.RollbackStepCompleted},
		{
			Kind: rollout.RollbackDowngrade, WaveID: secondWave.ID,
			Status: rollout.RollbackStepCompleted, Devices: 2, Failed: 1, Unreported: 1,
		},
		{
			Kind: rollout.RollbackDowngrade, WaveID: firstWaveID,
			Status: rollout.RollbackStepCompleted, Devices: 1, Restored: 1,
		},
		{
			Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepCompleted,
			Devices: 3, Agreed: 1, Corrected: 2,
		},
		{Kind: rollout.RollbackNotifyCompleted, Status: rollout.RollbackStepCompleted},
	}
	if record.Rollback == nil {
		t.Fatal("the rollout document carries no rollback record")
	}
	if diff := cmp.Diff(wantPlan, record.Rollback.Steps); diff != "" {
		t.Errorf("recorded rollback plan mismatch (-want +got):\n%s", diff)
	}
	if want := []string{world.fleet[1], world.fleet[2]}; !slices.Equal(record.Rollback.UnrestoredDeviceIDs, want) {
		t.Errorf("unrestored devices = %v, want %v", record.Rollback.UnrestoredDeviceIDs, want)
	}

	// The restores reached the devices in reverse wave order: the second wave's devices were all
	// restored before the first wave's, which is what makes the plan a saga rather than a set.
	restores := world.dispatcher.restoreCommands()
	if len(restores) != 3 {
		t.Fatalf("restore commands = %d, want one per compensated device", len(restores))
	}
	firstWave := readWave(t, world.waves, firstWaveID)
	secondWaveFirst := commandPosition(t, restores, secondWave.DeviceIDs[0])
	lastSecondWave := commandPosition(t, restores, secondWave.DeviceIDs[len(secondWave.DeviceIDs)-1])
	firstWaveOnly := commandPosition(t, restores, firstWave.DeviceIDs...)
	if secondWaveFirst < 0 || firstWaveOnly < 0 || secondWaveFirst > firstWaveOnly {
		t.Errorf("restore order = %v, want the second wave's devices before the first wave's", restores)
	}
	if lastSecondWave < 0 || lastSecondWave > firstWaveOnly {
		t.Errorf("restore order = %v, want every restore of the later wave before the earlier wave's",
			restores)
	}
	// Each compensated device was restored under exactly one restore command id, however many times
	// an attempt was redelivered, and each phase was announced under exactly one event id.
	assertRestoreIdentities(t, world.dispatcher, world.fleet)
	assertAnnouncementIdentities(t, awaitAnnouncements(t, world.broker, 2), world.opts.rolloutID)
	assertHeldAcross(t, world, compensationActivityNames()...)
	assertScheduledOnce(t, assertReplays(t, world.client, world.opts.rolloutID), rolloutActivityNames()...)
}

// TestRollbackBookkeepingMatchesTheDevices drives the same rollback world and asserts what the
// reconciliation leaves behind: the fleet's records agree with the devices' own workflows — the
// authority on what each device runs — the devices the rollback could not restore are recorded on
// the firmware they still run, and the reconciled inventory accounts for every device the rollback
// touched exactly once.
func TestRollbackBookkeepingMatchesTheDevices(t *testing.T) {
	world := newRestartWorld(t, worldOptions{
		rolloutID: "ro-restart-bookkeeping",
		waves:     []temporal.RolloutWave{{Percent: 50}, {Percent: 100}},
		fleet:     3,
		// No restart is injected: the reconciliation's own correctness is the subject here.
		plan: &restartPlan{},
	})
	scripts := world.dispatcher.script()
	scripts.failRestore[world.fleet[1]] = "flash write failed"
	scripts.neverRestore[world.fleet[2]] = true
	world.startRollout()

	firstWaveID := rollout.WaveID(world.opts.rolloutID, 0, 50)
	if wave := waitForWave(t, world.waves, firstWaveID); wave.Status != rollout.WaveHealthy {
		t.Fatalf("first wave = %+v, want the promoted wave (%d fleet samples)",
			wave, world.fleetSamples())
	}
	world.breakFleet()
	record := awaitConcludedRollback(t, world)
	awaitClosure(t, world)
	firstWave := readWave(t, world.waves, firstWaveID)
	secondWave := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 1, 100))

	// Every device the rollback touched is recorded on the version its workflow holds, which is what
	// "the bookkeeping matches reality" means: the restored device on the firmware it was restored
	// to, and each device the rollback could not restore on the firmware it still runs.
	assertRecordsMatchDevices(t, world, world.fleet)
	if got := recordedFirmware(t, world.db, world.fleet[0]); got != smokeInitialFirmware {
		t.Errorf("restored device %s is recorded on %q, want %q",
			world.fleet[0], got, smokeInitialFirmware)
	}
	for _, unrestored := range []string{world.fleet[1], world.fleet[2]} {
		if got := recordedFirmware(t, world.db, unrestored); got != "2.0.0" {
			t.Errorf("unrestored device %s is recorded on %q, want the firmware it still runs",
				unrestored, got)
		}
	}
	if want := []string{world.fleet[1], world.fleet[2]}; !slices.Equal(record.Rollback.UnrestoredDeviceIDs, want) {
		t.Errorf("unrestored devices = %v, want the devices whose restore did not conclude",
			record.Rollback.UnrestoredDeviceIDs)
	}

	// The reconciliation accounts for every compensated device exactly once: its outcomes sum to
	// the devices its step was given, and the inventory it established sums to the same fleet.
	step := reconcileStep(t, record.Rollback.Steps)
	accounted := step.Agreed + step.Corrected + step.Unverified
	if step.Devices != len(world.fleet) || accounted != step.Devices {
		t.Errorf("the reconciliation accounts for %d of %d devices (agreed %d, corrected %d, unverified %d), want every one",
			accounted, step.Devices, step.Agreed, step.Corrected, step.Unverified)
	}
	if step.Unverified != 0 {
		t.Errorf("the reconciliation left %d devices unverified, want every device compared", step.Unverified)
	}
	inventoried := 0
	for _, count := range record.Rollback.Inventory {
		inventoried += count.Devices
	}
	if inventoried != len(world.fleet) {
		t.Errorf("the inventory totals %d devices, want the %d the rollback touched",
			inventoried, len(world.fleet))
	}
	wantInventory := []rollout.FirmwareInventoryRecord{
		{Version: smokeInitialFirmware, Devices: 1},
		{Version: "2.0.0", Devices: 2},
	}
	if diff := cmp.Diff(wantInventory, record.Rollback.Inventory); diff != "" {
		t.Errorf("reconciled inventory mismatch (-want +got):\n%s", diff)
	}

	// The compensated devices are the membership of the waves the rollout dispatched, most recent
	// first; the restore identities are asserted over the same set.
	assertRestoreIdentities(t, world.dispatcher, compensatedDevicesOf(firstWave, secondWave))
	assertScheduledOnce(t, assertReplays(t, world.client, world.opts.rolloutID), rolloutActivityNames()...)

}
