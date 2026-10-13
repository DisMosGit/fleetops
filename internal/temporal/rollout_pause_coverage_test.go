package temporal

import (
	"slices"
	"testing"
	"time"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// TestRolloutWorkflowPauseDuringTheDispatchWaitIsApplied backs "A pause during the dispatch wait is
// applied": the pause signal is folded while the first wave's update commands are being delivered
// and their results collected — the wave's dispatch wait is the run's first wait — and the wave
// still collects what it would have and is decided; no next wave starts until the resume releases
// the hold.
func TestRolloutWorkflowPauseDuringTheDispatchWaitIsApplied(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.98))

	first := rollout.WaveID("ro-1", 0, 50)
	second := rollout.WaveID("ro-1", 1, 100)
	env := newRolloutEnv(fakes)
	// The pause is delivered before the run reaches its first wait, so that wait — the dispatch
	// of the first wave — is where it is folded. The resume comes long after that wave's window
	// has elapsed, so the rollout is still paused when the wave is judged and nothing advances
	// before the resume is folded.
	queueSignalsAt(env, signalPause(0), signalResume(20*time.Second))

	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed after the resume", view.Status)
	}

	// The recorded history is the proof of where the pause was applied: it was folded before the
	// wave's dispatch concluded and its outcome was recorded, the wave was still judged, and the
	// next wave was resolved only after the resume was folded.
	events := fakes.recordedEvents()
	paused := slices.Index(events, "rollout paused")
	evaluating := slices.Index(events, "wave "+first+" evaluating")
	decided := slices.Index(events, "wave "+first+" healthy")
	resolvedNext := slices.Index(events, "resolve "+second)
	resumed := lastIndex(events, "rollout running")
	for _, event := range []struct {
		name  string
		index int
	}{
		{name: "the pause folded", index: paused},
		{name: "the wave's outcome recorded", index: evaluating},
		{name: "the wave decided", index: decided},
		{name: "the next wave resolved", index: resolvedNext},
		{name: "the resume folded", index: resumed},
	} {
		if event.index < 0 {
			t.Fatalf("%s is missing from the recorded events: %v", event.name, events)
		}
	}
	if paused > evaluating {
		t.Errorf("the pause was folded after the wave's dispatch concluded (events %v), "+
			"want it applied during the dispatch wait", events)
	}
	if decided < evaluating {
		t.Errorf("the wave was decided before its dispatch concluded (events %v)", events)
	}
	if resolvedNext < resumed {
		t.Errorf("the next wave was resolved before the resume (events %v), want no wave started "+
			"while the rollout was paused", events)
	}

	// The wave's collection finished as it would have: every target was commanded exactly once
	// and settled successfully, and the gate's decision is what the wave records.
	rec, ok := fakes.recordedWave(first)
	if !ok {
		t.Fatalf("wave %s was never recorded", first)
	}
	if rec.Status != rollout.WaveHealthy || rec.SuccessRate != 0.99 {
		t.Errorf("recorded wave = %+v, want it decided healthy on its window", rec)
	}
	if len(rec.FailedDeviceIDs) != 0 || len(rec.UnreportedDeviceIDs) != 0 {
		t.Errorf("recorded wave outcomes = %v failed, %v unreported, want every device settled",
			rec.FailedDeviceIDs, rec.UnreportedDeviceIDs)
	}
	var commanded []string
	for _, cmd := range fakes.recordedCommands() {
		if slices.Contains(rec.DeviceIDs, cmd.DeviceID) {
			commanded = append(commanded, cmd.DeviceID)
		}
	}
	if !slices.Equal(commanded, rec.DeviceIDs) {
		t.Errorf("commanded devices = %v, want the wave's targets %v", commanded, rec.DeviceIDs)
	}
}

// lastIndex returns the position of the last occurrence of want in values, or -1 when it holds
// none.
func lastIndex(values []string, want string) int {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] == want {
			return i
		}
	}
	return -1
}

// TestRolloutWorkflowPausedQueryReportsThePause backs "A paused rollout reports the pause": the
// state query evaluated while the rollout is paused reports the pause, and once the wave in flight
// has been judged it names the wave the resume will start — the next pending sequence entry — which
// is exactly the wave the resumed run then starts.
func TestRolloutWorkflowPausedQueryReportsThePause(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.98))

	first := rollout.WaveID("ro-1", 0, 50)
	second := rollout.WaveID("ro-1", 1, 100)
	env := newRolloutEnv(fakes)
	queueSignalsAt(env, signalPause(0), signalResume(20*time.Second))

	// Two moments of the same pause: when the signal is recorded, with the first wave still in
	// flight, and when that wave has been judged and the rollout holds at the boundary.
	atPause := &scheduledQuery{}
	atHold := &scheduledQuery{}
	fakes.observeControlRecord(func(status rollout.RolloutStatus) {
		if status == rollout.RolloutPaused {
			recordQuery(env, atPause)
		}
	})
	fakes.observeWaveState(func(update rollout.WaveStateUpdate) {
		if update.WaveID != first || update.Status != rollout.WaveHealthy {
			return
		}
		// The moment the hold begins: the wave the pause reports as next has no document yet,
		// because nothing starts it until the resume is folded.
		if _, ok := fakes.recordedWave(second); ok {
			t.Error("the next wave was resolved while the rollout was paused")
		}
		recordQuery(env, atHold)
	})
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed after the resume", view.Status)
	}

	// The pause was applied at all: without its own record there is no paused state to query.
	if statuses := fakes.rolloutStatuses(); !slices.Contains(statuses, rollout.RolloutPaused) {
		t.Fatalf("recorded statuses = %v, want the rollout recorded as %q", statuses, rollout.RolloutPaused)
	}
	paused := atPause.result(t)
	if paused.Status != rollout.RolloutPaused {
		t.Errorf("status while paused = %q, want %q", paused.Status, rollout.RolloutPaused)
	}
	if paused.Current != 0 || len(paused.Waves) != 2 || paused.Waves[0].TargetCount != 2 {
		t.Errorf("paused view = %+v, want the wave in flight reported with its membership",
			paused)
	}

	held := atHold.result(t)
	if held.Status != rollout.RolloutPaused {
		t.Fatalf("status after the wave was judged = %q, want it still %q",
			held.Status, rollout.RolloutPaused)
	}
	if len(held.Waves) != 2 {
		t.Fatalf("view carries %d waves, want the configured 2", len(held.Waves))
	}
	if held.Waves[0].Status != rollout.WaveHealthy || held.Waves[0].SuccessRate != 0.99 {
		t.Errorf("wave in flight = %+v, want it judged on its window", held.Waves[0])
	}
	// The wave it will start when it is resumed: the next sequence entry is still pending — it
	// has no membership yet — and the resume starts exactly that one.
	if held.Waves[1].Status != rollout.WavePending || held.Waves[1].Percent != 100 {
		t.Errorf("wave to start after the resume = %+v, want the pending 100%% wave", held.Waves[1])
	}
	next, ok := fakes.recordedWave(second)
	if !ok {
		t.Fatal("the resumed rollout started no next wave")
	}
	if next.Percent != held.Waves[1].Percent {
		t.Errorf("the resumed rollout started the %d%% wave, want the pending %d%% one it reported",
			next.Percent, held.Waves[1].Percent)
	}
	// "When it is resumed" is the resume and not before it: the resolution the query named
	// follows the resume's own record.
	events := fakes.recordedEvents()
	resumed, resolved := lastIndex(events, "rollout running"), slices.Index(events, "resolve "+second)
	if resumed < 0 || resolved < 0 || resolved < resumed {
		t.Errorf("the next wave was resolved before the resume (events %v), want it started on the resume",
			events)
	}
}
