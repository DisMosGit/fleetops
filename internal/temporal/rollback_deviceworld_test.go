package temporal

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// TestDowngradeRecordsAResolvedFailureForTheDeviceAlone backs "A resolved failure is recorded for
// the device alone": one device's restore fails after the downgrade activity's retries are
// exhausted — its authoritative state cannot be read, so the failure resolves as an unavailable
// device rather than as a transient one — and the step records that device as unrestored while its
// peer is still restored and the plan still runs to its end.
func TestDowngradeRecordsAResolvedFailureForTheDeviceAlone(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	fakes.addFirmware(previousFirmware())
	// dev-2 takes the firmware and concludes its restore. dev-1's workflow cannot be read at
	// all, so its restore cannot even be attempted: the activity fails, its retries are
	// exhausted, and the failure is the step's to record.
	fakes.devices().succeedUpdateThenRestore("dev-2", OutcomeSucceeded, "")
	fakes.devices().refuseStateReads("dev-1")
	fakes.scriptHealth(unhealthyAt(0.4))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack {
		t.Fatalf("rollout concluded as %s, want rolled_back", view.Status)
	}
	// One outcome per device the step targeted: the failing device is recorded as unavailable —
	// the outcome that carries the failure as its reason — and its peer as restored.
	wantStep := RollbackStepView{
		Kind: rollout.RollbackDowngrade, WaveID: rollout.WaveID("ro-1", 0, 100),
		Status: rollout.RollbackStepCompleted, Devices: 2, Restored: 1, Unavailable: 1,
	}
	if diff := cmp.Diff(wantStep, view.Rollback.Plan[1]); diff != "" {
		t.Errorf("downgrade step mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"dev-1"}, view.Rollback.UnrestoredDeviceIDs); diff != "" {
		t.Errorf("unrestored devices mismatch (-want +got):\n%s", diff)
	}

	// The failing device did not cancel its peer's restore: dev-2 was commanded the restore and
	// its workflow left it on the version the plan restored it to.
	if got := fakes.devices().firmwareOf("dev-2"); got != previousFirmware().Version {
		t.Errorf("dev-2 runs %s, want the restored %s", got, previousFirmware().Version)
	}
	var restored []string
	for _, cmd := range restoreDeliveries(fakes.commandDeliveries()) {
		restored = append(restored, cmd.DeviceID)
		if want := rollbackCommandID("ro-1", cmd.DeviceID); cmd.CommandID != want {
			t.Errorf("restore command id = %q, want %q", cmd.CommandID, want)
		}
	}
	if diff := cmp.Diff([]string{"dev-2"}, restored); diff != "" {
		t.Errorf("restored devices mismatch (-want +got):\n%s", diff)
	}

	// One device's unrestored firmware did not abort the plan: the reconciliation ran, both
	// announcements were published, and the rollout reached its terminal status.
	if got := view.Rollback.Plan[2].Status; got != rollout.RollbackStepCompleted {
		t.Errorf("reconciliation step = %s, want it run despite the failed restore", got)
	}
	if announcements := fakes.recordedAnnouncements(); len(announcements) != 2 {
		t.Errorf("announcements = %d, want the started and completed phases", len(announcements))
	}
	recorded, ok := fakes.recordedRollout("ro-1")
	if !ok || recorded.Status != rollout.RolloutRolledBack {
		t.Errorf("recorded rollout = %+v (recorded %v), want the terminal rolled_back", recorded, ok)
	}
}

// TestReconcileInventoryLeavesAnUnreadableDeviceUnverified backs "An unreadable device is never
// assumed correct": a device whose authoritative state cannot be read is reported as unverified
// with the reason, its record is left exactly as it stands rather than corrected or cleared, and
// the inventory counts it as running nothing rather than the version the fleet records.
func TestReconcileInventoryLeavesAnUnreadableDeviceUnverified(t *testing.T) {
	t.Parallel()

	settings := rollbackTestSettings(rollbackRestoreTimeout, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	fakes.addFirmware(previousFirmware())
	fakes.devices().succeedUpdateThenRestore("dev-2", OutcomeSucceeded, "")
	// dev-1's record will be the only one the reconciliation cannot check: its workflow refuses
	// every read, so the activity fails and its retries are exhausted.
	fakes.devices().refuseStateReads("dev-1")
	fakes.scriptHealth(unhealthyAt(0.4))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack {
		t.Fatalf("rollout concluded as %s, want rolled_back", view.Status)
	}

	// The record that could not be checked is left as it stands: dev-1 stays recorded on the
	// firmware the fleet held — the deployed one, which nothing verified it still runs — while
	// dev-2's record is corrected to the version its workflow reports.
	if got := fakes.fleet["dev-1"]; got != rolloutTestFirmware().Version {
		t.Errorf("dev-1 recorded on %q, want its record left as it stands on %q",
			got, rolloutTestFirmware().Version)
	}
	if got := fakes.fleet["dev-2"]; got != previousFirmware().Version {
		t.Errorf("dev-2 recorded on %q, want the corrected %q", got, previousFirmware().Version)
	}
	wantStep := RollbackStepView{
		Kind: rollout.RollbackReconcileInventory, Status: rollout.RollbackStepCompleted,
		Devices: 2, Corrected: 1, Unverified: 1,
	}
	if diff := cmp.Diff(wantStep, view.Rollback.Plan[2]); diff != "" {
		t.Errorf("reconciliation step mismatch (-want +got):\n%s", diff)
	}
	// An unverified device is visible as exactly that: the inventory accounts for the device it
	// could not verify and no version carries it, so it is never silently assumed correct.
	if diff := cmp.Diff(
		[]FirmwareCount{{Version: previousFirmware().Version, Devices: 1}},
		view.Rollback.Inventory,
	); diff != "" {
		t.Errorf("reconciled inventory mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"dev-1"}, view.Rollback.UnrestoredDeviceIDs); diff != "" {
		t.Errorf("unrestored devices mismatch (-want +got):\n%s", diff)
	}
}
