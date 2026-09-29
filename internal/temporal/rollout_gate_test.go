package temporal

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/testsuite"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// approveAt schedules an operator's approve_next_wave signal at the given workflow time.
func approveAt(env *testsuite.TestWorkflowEnvironment, at time.Duration) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ApproveNextWaveSignalName, ApproveNextWaveSignal{})
	}, at)
}

func TestRolloutGateWaitsTheDurableWindow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// lag is how far in the past the resolution records the wave's start: what a wave
		// looks like once a rollout resumes after its worker stopped.
		lag time.Duration
	}{
		{name: "a wave is judged one window after its start"},
		{name: "a window already under way waits only the remainder", lag: 2 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutTestSettings()
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
			fakes.resolveLag = tc.lag
			fakes.scriptHealth(healthyAt(0.99))

			view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

			evals := fakes.recordedEvaluations()
			wave, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 1))
			if !ok {
				t.Fatal("the first wave was never resolved")
			}
			if len(evals) == 0 {
				t.Fatal("the wave was never evaluated")
			}
			// The window is anchored at the recorded start: exactly one configured window
			// after it, however long ago that was.
			if waited := evals[0].At.Sub(wave.StartedAt); waited != settings.HealthWindow {
				t.Errorf("the wave was judged %v after its start, want %v",
					waited, settings.HealthWindow)
			}
			answers := fakes.recordedHealth()
			if len(answers) == 0 {
				t.Fatal("the wave was never measured")
			}
			if !answers[0].WindowStart.Equal(wave.StartedAt) {
				t.Errorf("the measured window starts at %v, want the wave's recorded start %v",
					answers[0].WindowStart, wave.StartedAt)
			}
			if view.Waves[0].Status != rollout.WaveHealthy {
				t.Errorf("first wave = %s, want it promoted after its window", view.Waves[0].Status)
			}
		})
	}

	t.Run("an elapsed window is not waited on again", func(t *testing.T) {
		t.Parallel()

		settings := rolloutTestSettings()
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
		// The recorded start is older than a whole window, so the remainder is nothing: the
		// gate judges the wave as soon as the rollout reaches it.
		fakes.resolveLag = 10 * time.Minute
		fakes.scriptHealth(healthyAt(0.99))

		view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

		wave, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 1))
		if !ok {
			t.Fatal("the first wave was never resolved")
		}
		evals := fakes.recordedEvaluations()
		if len(evals) == 0 {
			t.Fatal("the wave was never evaluated")
		}
		if waited := evals[0].At.Sub(wave.StartedAt); waited != fakes.resolveLag {
			t.Errorf("the wave was judged %v after its start, want no further wait beyond %v",
				waited, fakes.resolveLag)
		}
		if view.Waves[0].Status != rollout.WaveHealthy {
			t.Errorf("first wave = %s, want it promoted", view.Waves[0].Status)
		}
	})
}

func TestRolloutGateDecidesOnTheVerdict(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		health     WaveHealth
		wantStatus rollout.RolloutStatus
		wantWave   rollout.WaveStatus
	}{
		{
			name:       "a ratio at the boundary promotes",
			health:     healthyAt(0.95),
			wantStatus: rollout.RolloutCompleted,
			wantWave:   rollout.WaveHealthy,
		},
		{
			name:       "a ratio below the boundary rolls back",
			health:     unhealthyAt(0.94),
			wantStatus: rollout.RolloutRolledBack,
			wantWave:   rollout.WaveUnhealthy,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutTestSettings()
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
			fakes.scriptHealth(tc.health)

			view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

			if view.Status != tc.wantStatus {
				t.Fatalf("rollout concluded as %s, want %s", view.Status, tc.wantStatus)
			}
			first := rollout.WaveID("ro-1", 0, 1)
			if rec, ok := fakes.recordedWave(first); !ok || rec.Status != tc.wantWave ||
				rec.SuccessRate != tc.health.SuccessRatio {
				t.Errorf("recorded wave = %+v (recorded %v), want %s at %v",
					rec, ok, tc.wantWave, tc.health.SuccessRatio)
			}
			if view.Waves[0].Status != tc.wantWave ||
				view.Waves[0].SuccessRate != tc.health.SuccessRatio {
				t.Errorf("reported first wave = %+v, want %s at %v",
					view.Waves[0], tc.wantWave, tc.health.SuccessRatio)
			}
		})
	}
}

func TestRolloutGateRemeasuresAnUndecidedWave(t *testing.T) {
	t.Parallel()

	// One wave, so every measurement in the run belongs to it.
	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
	// Thin evidence first, a decided verdict one window later.
	fakes.scriptHealth(undecided(), healthyAt(0.97))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	evals := fakes.recordedEvaluations()
	if len(evals) != 2 {
		t.Fatalf("evaluations = %d, want the undecided wave measured again", len(evals))
	}
	if step := evals[1].At.Sub(evals[0].At); step != settings.HealthWindow {
		t.Errorf("the re-measure came %v after the first, want another health window of %v",
			step, settings.HealthWindow)
	}
	answers := fakes.recordedHealth()
	if len(answers) != 2 {
		t.Fatalf("measurements = %d, want the undecided wave measured again", len(answers))
	}
	if !answers[1].WindowStart.After(answers[0].WindowStart) {
		t.Errorf("the re-measured window starts at %v, want it slid forward past %v",
			answers[1].WindowStart, answers[0].WindowStart)
	}
	if !answers[1].WindowEnd.Equal(evals[1].At) {
		t.Errorf("the re-measured window ends at %v, want the decision moment %v",
			answers[1].WindowEnd, evals[1].At)
	}

	// The wave is promoted on the decided verdict, and it was never recorded healthy before it.
	first := rollout.WaveID("ro-1", 0, 100)
	if view.Status != rollout.RolloutCompleted || view.Waves[0].Status != rollout.WaveHealthy {
		t.Fatalf("rollout concluded as %s with first wave %s, want completed and healthy",
			view.Status, view.Waves[0].Status)
	}
	if healthy := strings.Count(strings.Join(fakes.recordedEvents(), "\n"), first+" healthy"); healthy != 1 {
		t.Errorf("the wave was recorded healthy %d times, want once on the decided verdict", healthy)
	}
}

func TestRolloutGateTreatsANeverDecidedWaveAsUnhealthy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		timeout time.Duration
		want    []time.Duration // how long after the wave's start each measurement ran
	}{
		{
			name:    "a decision timeout that is a whole number of windows",
			timeout: 30 * time.Minute,
			want:    []time.Duration{5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 20 * time.Minute, 25 * time.Minute, 30 * time.Minute},
		},
		{
			name:    "a decision timeout between two windows",
			timeout: 12 * time.Minute,
			want:    []time.Duration{5 * time.Minute, 10 * time.Minute, 12 * time.Minute},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutSettingsWith(5*time.Minute, tc.timeout, RolloutWave{Percent: 100})
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
			// The fleet never reports enough evidence to decide.
			fakes.scriptHealth(undecided())

			view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

			wave, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100))
			if !ok {
				t.Fatal("the wave was never resolved")
			}
			evals := fakes.recordedEvaluations()
			got := make([]time.Duration, 0, len(evals))
			for _, eval := range evals {
				got = append(got, eval.At.Sub(wave.StartedAt))
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("measurement times after the wave's start mismatch (-want +got):\n%s", diff)
			}

			if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeDecisionTimeout {
				t.Fatalf("rollout concluded as %s/%s, want rolled_back/decision_timeout",
					view.Status, view.Outcome)
			}
			if view.Waves[0].Status != rollout.WaveUnhealthy {
				t.Errorf("first wave = %s, want a wave that never gathered evidence treated as unhealthy",
					view.Waves[0].Status)
			}
			if view.EndedBy != wave.ID {
				t.Errorf("ended by %q, want the undecided wave %q", view.EndedBy, wave.ID)
			}
			if view.Decision == nil || view.Decision.Verdict != wavehealth.VerdictUndecided {
				t.Errorf("decision = %+v, want the undecided measurement it concluded on", view.Decision)
			}
		})
	}
}

func TestRolloutGateRetriesAFailedEvaluation(t *testing.T) {
	t.Parallel()

	// One wave, so every attempt in the run belongs to it.
	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
	fakes.healthFails = 1
	fakes.scriptHealth(healthyAt(0.98))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want the retried evaluation to promote", view.Status)
	}
	evals := fakes.recordedEvaluations()
	if len(evals) != 2 {
		t.Fatalf("evaluations = %d, want the failed one retried", len(evals))
	}
	if !evals[0].At.Equal(evals[1].At) {
		t.Errorf("the retry measured at %v, want the same decision moment %v", evals[1].At, evals[0].At)
	}
	if view.Waves[0].Status != rollout.WaveHealthy {
		t.Errorf("first wave = %s, want it promoted on the verdict that came back", view.Waves[0].Status)
	}
}

func TestRolloutGateRollsBackAndLeavesPromotedWavesAlone(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute,
		RolloutWave{Percent: 25}, RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
	fakes.scriptHealth(healthyAt(0.99), unhealthyAt(0.4))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeUnhealthyWave {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/unhealthy_wave", view.Status, view.Outcome)
	}
	failing := rollout.WaveID("ro-1", 1, 50)
	if view.EndedBy != failing {
		t.Errorf("ended by %q, want the failing wave %q", view.EndedBy, failing)
	}
	// The decision the gate made is carried whole: verdict, ratio, sample size, and window.
	if view.Decision == nil {
		t.Fatal("the rollback carries no decision")
	}
	want := WaveHealth{
		Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: 0.4, SampleSize: 100,
		WindowStart: view.Decision.WindowStart, WindowEnd: view.Decision.WindowEnd,
	}
	if diff := cmp.Diff(want, *view.Decision); diff != "" {
		t.Errorf("rollback decision mismatch (-want +got):\n%s", diff)
	}
	if view.Decision.WindowEnd.Before(view.Decision.WindowStart) {
		t.Errorf("decision window = %v..%v, want a forward window",
			view.Decision.WindowStart, view.Decision.WindowEnd)
	}

	// The waves promoted before the failing one keep their recorded outcome, and the wave
	// after it never starts.
	promoted, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 25))
	if !ok || promoted.Status != rollout.WaveHealthy || promoted.SuccessRate != 0.99 {
		t.Errorf("promoted wave = %+v (recorded %v), want its healthy outcome unchanged", promoted, ok)
	}
	if _, ok := fakes.recordedWave(rollout.WaveID("ro-1", 2, 100)); ok {
		t.Error("the wave after the failing one was resolved, want the rollout to have stopped")
	}
	if got := len(fakes.recordedCommands()); got != 50 {
		t.Errorf("dispatched commands = %d, want only the two waves that ran (25 + 25 devices)", got)
	}
	if view.Waves[2].Status != rollout.WavePending {
		t.Errorf("third wave = %s, want it never started", view.Waves[2].Status)
	}
}

func TestRolloutGateHoldsAGatedWaveUntilApproval(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute,
		RolloutWave{Percent: 100, RequireApproval: true})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(10)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	waiting := scheduleQuery(env, time.Minute)
	approveAt(env, 2*time.Minute)

	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want the approved wave to carry it to completion", view.Status)
	}
	held := waiting.result(t)
	if held.Status != rollout.RolloutAwaitingApproval || held.Current != 0 {
		t.Fatalf("while waiting the query reports %s at wave %d, want awaiting approval at the gated wave",
			held.Status, held.Current)
	}
	if held.ApprovalOutstanding {
		t.Error("the query reports an outstanding approval, want none before the operator approved")
	}
	if held.Waves[0].Status != rollout.WavePending || held.Waves[0].TargetCount != 0 {
		t.Errorf("gated wave = %+v, want it unresolved while the rollout waits", held.Waves[0])
	}
	// Nothing about the wave was decided before the approval: its membership was resolved
	// only after the rollout had recorded that it was waiting.
	events := fakes.recordedEvents()
	heldFor := indexOfEvent(events, "rollout "+string(rollout.RolloutAwaitingApproval))
	resolved := indexOfEvent(events, "resolve "+rollout.WaveID("ro-1", 0, 100))
	if heldFor < 0 || resolved < 0 || resolved < heldFor {
		t.Errorf("events = %v, want the gated wave resolved only after the rollout held for approval",
			events)
	}
}

func TestRolloutGateSpendsOneApprovalPerGatedWave(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute,
		RolloutWave{Percent: 25, RequireApproval: true},
		RolloutWave{Percent: 100, RequireApproval: true})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(20)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	// One approval lets the first gated wave start. The second gated wave is reached after the
	// first one's window closed, and it must wait for its own.
	approveAt(env, time.Millisecond)
	waiting := scheduleQuery(env, 9*time.Minute)
	approveAt(env, 10*time.Minute)

	view := runRollout(t, env, rolloutInputFor(settings))

	held := waiting.result(t)
	if held.Status != rollout.RolloutAwaitingApproval || held.Current != 1 {
		t.Fatalf("the second gated wave reports %s at wave %d, want it waiting for its own approval",
			held.Status, held.Current)
	}
	if held.Waves[0].Status != rollout.WaveHealthy {
		t.Errorf("first wave = %s, want the approved wave promoted", held.Waves[0].Status)
	}
	if held.ApprovalOutstanding {
		t.Error("the query reports an outstanding approval, want the first one consumed")
	}
	if view.Status != rollout.RolloutCompleted {
		t.Errorf("rollout concluded as %s, want both approved waves to complete it", view.Status)
	}
}

func TestRolloutGateBanksAnEarlyApproval(t *testing.T) {
	t.Parallel()

	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute,
		RolloutWave{Percent: 10}, RolloutWave{Percent: 100, RequireApproval: true})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(20)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	// The approval arrives while the first, ungated wave is inside its health window — the
	// signal waits durably in the workflow's channel, exactly as it does when no worker is
	// executing the rollout.
	approveAt(env, time.Millisecond)
	banked := scheduleQuery(env, 3*time.Minute)

	view := runRollout(t, env, rolloutInputFor(settings))

	outstanding := banked.result(t)
	if !outstanding.ApprovalOutstanding {
		t.Error("the query reports no outstanding approval, want the early approval held and reported")
	}
	if outstanding.Status != rollout.RolloutRunning || outstanding.Current != 0 {
		t.Errorf("the rollout reports %s at wave %d, want it still running its first wave",
			outstanding.Status, outstanding.Current)
	}
	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want the banked approval to carry the gated wave", view.Status)
	}
	// The ungated wave never consumed the approval, so the gated wave never had to wait.
	for _, event := range fakes.recordedEvents() {
		if event == "rollout "+string(rollout.RolloutAwaitingApproval) {
			t.Errorf("events = %v, want no wait: the banked approval covered the gated wave",
				fakes.recordedEvents())
		}
	}
}

// indexOfEvent returns the position of event in events, or -1 when it never happened.
func indexOfEvent(events []string, event string) int {
	for i, got := range events {
		if got == event {
			return i
		}
	}
	return -1
}
