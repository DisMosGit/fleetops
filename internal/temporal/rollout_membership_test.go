package temporal

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// TestRolloutWorkflowRecordsMembershipBeforeDispatch backs "Membership is recorded before anything
// is dispatched": every update command of a wave is accepted only after the wave's own document —
// its rollout id, percentage, target device set, and start time — is in the store. The check is
// made at the instant of each delivery, from the document the store held then, rather than after
// the run: a workflow that dispatched first and recorded afterwards would pass an after-the-fact
// check but fails this one.
func TestRolloutWorkflowRecordsMembershipBeforeDispatch(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(2*time.Second, 30*time.Second,
		RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.98))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	// Both waves dispatched, so both are evidence for the ordering rather than one wave the
	// rollout happened to skip.
	if len(view.Waves) != 2 || view.Waves[0].TargetCount != 2 || view.Waves[1].TargetCount != 2 {
		t.Fatalf("waves = %+v, want two dispatched waves of two targets each", view.Waves)
	}

	waveIDs := make([]string, 0, len(settings.Waves))
	for position, wave := range settings.Waves {
		waveIDs = append(waveIDs, rollout.WaveID("ro-1", position, wave.Percent))
	}
	dispatched := map[string][]dispatchRecord{}
	observed := 0
	for _, dispatch := range fakes.recordedDispatches() {
		if !slices.Contains(waveIDs, dispatch.WaveID) {
			continue
		}
		dispatched[dispatch.WaveID] = append(dispatched[dispatch.WaveID], dispatch)
		observed++
	}
	// Every delivered command belongs to one of the rollout's waves, so no command escaped the
	// per-wave assertions below.
	if commands := fakes.recordedCommands(); observed != len(commands) {
		t.Fatalf("observed %d dispatches for %d delivered commands, want every command observed",
			observed, len(commands))
	}

	for position, wave := range settings.Waves {
		waveID := waveIDs[position]
		rec, ok := fakes.recordedWave(waveID)
		if !ok {
			t.Fatalf("wave %s was never recorded", waveID)
		}
		if rec.RolloutID != "ro-1" || rec.Percent != wave.Percent || rec.StartedAt.IsZero() {
			t.Errorf("recorded wave %s = %+v, want its rollout, percentage, and start time",
				waveID, rec)
		}

		got := dispatched[waveID]
		if len(got) != len(rec.DeviceIDs) {
			t.Fatalf("wave %s dispatched %d commands for %d target devices",
				waveID, len(got), len(rec.DeviceIDs))
		}
		for _, dispatch := range got {
			// The scenario's assertion: at the moment this command was accepted, the wave
			// document already existed and carried the membership the command belongs to.
			if !dispatch.Recorded {
				t.Errorf("command %s was dispatched before wave %s was recorded",
					dispatch.CommandID, waveID)
				continue
			}
			if dispatch.Wave.RolloutID != "ro-1" || dispatch.Wave.Percent != wave.Percent {
				t.Errorf("wave %s at the dispatch of %s = %+v, want its rollout and percentage",
					waveID, dispatch.CommandID, dispatch.Wave)
			}
			deviceID := strings.TrimPrefix(dispatch.CommandID, waveID+"-")
			if !slices.Contains(dispatch.Wave.DeviceIDs, deviceID) {
				t.Errorf("wave %s at the dispatch of %s targets %v, want it to name %s",
					waveID, dispatch.CommandID, dispatch.Wave.DeviceIDs, deviceID)
			}
			if !dispatch.Wave.StartedAt.Equal(rec.StartedAt) {
				t.Errorf("wave %s started at %v when %s was dispatched, want its recorded %v",
					waveID, dispatch.Wave.StartedAt, dispatch.CommandID, rec.StartedAt)
			}
		}
	}
}
