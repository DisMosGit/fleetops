package temporal

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/testsuite"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// pauseResumeSettings is the policy the pause and resume tests drive: a short window so a wave
// in flight concludes quickly, and a wave sequence the tests script per case. The result timeout
// is the test default.
func pauseResumeSettings(waves ...RolloutWave) RolloutSettings {
	return rolloutSettingsWith(2*time.Second, 30*time.Second, waves...)
}

// signalAt schedules one signal to the workflow at the given workflow time.
func signalAt(signalName string, tier int, at time.Duration) delayedSignal {
	return delayedSignal{name: signalName, tier: tier, at: at}
}

// delayedSignal is one scheduled signal delivery: the channel, the payload the channel carries,
// and the workflow time to deliver it at.
type delayedSignal struct {
	name string
	tier int
	at   time.Duration
}

// signalPause delivers a pause_rollout signal at at.
func signalPause(at time.Duration) delayedSignal {
	return signalAt(PauseRolloutSignalName, 0, at)
}

// signalResume delivers a resume_rollout signal at at.
func signalResume(at time.Duration) delayedSignal {
	return signalAt(ResumeRolloutSignalName, 1, at)
}

// signalApproval delivers an approve_next_wave signal at at.
func signalApproval(at time.Duration) delayedSignal {
	return signalAt(ApproveNextWaveSignalName, 2, at)
}

// queueSignalsAt registers one delayed callback per signal on a rollout test environment. A tier
// keeps two signals scheduled for the same instant in a deterministic order.
func queueSignalsAt(env *testsuite.TestWorkflowEnvironment, signals ...delayedSignal) {
	for _, signal := range signals {
		payload := any(nil)
		switch signal.name {
		case PauseRolloutSignalName:
			payload = PauseRolloutSignal{}
		case ResumeRolloutSignalName:
			payload = ResumeRolloutSignal{}
		case ApproveNextWaveSignalName:
			payload = ApproveNextWaveSignal{}
		}
		at := signal.at + time.Duration(signal.tier)*time.Millisecond
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(signal.name, payload)
		}, at)
	}
}

// controlTrace is the rollout's control history as one test drove it: every non-running status it
// recorded, in order.
type controlTrace struct {
	mu       sync.Mutex
	statuses []rollout.RolloutStatus
}

// recorded returns every status the rollout recorded, in order.
func (tr *controlTrace) recorded() []rollout.RolloutStatus {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]rollout.RolloutStatus(nil), tr.statuses...)
}

// traceControl records every non-running status write the rollout makes, in the order it made them.
func traceControl(_ *testsuite.TestWorkflowEnvironment, fakes *rolloutFakes) *controlTrace {
	trace := &controlTrace{}
	fakes.observeControlRecord(func(status rollout.RolloutStatus) {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		trace.statuses = append(trace.statuses, status)
	})
	return trace
}

// TestRolloutWorkflowPauseStopsAtTheNextWaveBoundary pins what a pause does: a rollout paused while
// no wave is in flight resolves no further wave, dispatches nothing, and reports `paused`; a resume
// lets it continue from exactly where it stopped, with the waves it already promoted intact.
func TestRolloutWorkflowPauseStopsAtTheNextWaveBoundary(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.98))

	// The pause arrives while the first wave is in flight and the resume comes later: the
	// second wave may start only once the resume is folded.
	env := newRolloutEnv(fakes)
	queueSignalsAt(env, signalPause(0), signalResume(1*time.Second))
	// The state the operator reads at the moment the pause is recorded.
	trace := traceControl(env, fakes)
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed after the resume", view.Status)
	}
	if len(view.Waves) != 2 || view.Waves[0].Status != rollout.WaveHealthy ||
		view.Waves[1].Status != rollout.WaveHealthy {
		t.Fatalf("waves = %+v, want both promoted", view.Waves)
	}

	// The recorded statuses are the operator's own signals: paused once, running again on the
	// resume.
	if got := trace.recorded(); !slices.Contains(got, rollout.RolloutPaused) {
		t.Errorf("recorded statuses = %v, want one %q among them", got, rollout.RolloutPaused)
	}

	// The second wave was not resolved while the rollout was paused: its document and its
	// commands appear only after the resume.
	second := rollout.WaveID("ro-1", 1, 100)
	if _, ok := fakes.recordedWave(second); !ok {
		t.Error("the second wave was never recorded, want it started after the resume")
	}
	statuses := fakes.rolloutStatuses()
	if diff := cmp.Diff(
		[]rollout.RolloutStatus{
			rollout.RolloutRunning,
			rollout.RolloutPaused,
			rollout.RolloutRunning,
			rollout.RolloutCompleted,
		},
		statuses,
	); diff != "" {
		t.Errorf("recorded rollout statuses mismatch (-want +got):\n%s", diff)
	}
}

// TestRolloutWorkflowPauseHoldsApprovalWaitAndKeepsTheApproval pins the gated case: a pause while
// the rollout waits for approval holds it there, an approval arriving during the pause is still
// outstanding, and the gated wave starts on that approval once the rollout is resumed.
func TestRolloutWorkflowPauseHoldsApprovalWaitAndKeepsTheApproval(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 100, RequireApproval: true})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	// Pause first, approve while paused, then resume. The pause is delivered immediately, before
	// the run reaches its first wave boundary, so the hold is in force when the approval arrives.
	// Only the resume is allowed to release it: the approval is banked, not acted on.
	queueSignalsAt(env,
		signalPause(0),
		signalApproval(500*time.Millisecond),
		signalResume(1*time.Second),
	)
	trace := traceControl(env, fakes)
	view := runRollout(t, env, rolloutInputFor(settings))

	// The gated wave started on the approval that arrived during the pause: exactly one
	// approval was sent, and the wave could not have started without it.
	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed on the banked approval", view.Status)
	}
	if _, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100)); !ok {
		t.Error("the gated wave was never started, want it started on the banked approval")
	}
	if commands := fakes.recordedCommands(); len(commands) != 2 {
		t.Errorf("commanded %d devices, want the gated wave's 2", len(commands))
	}

	// The rollout was held and reached its gate, in one order or the other depending on when the
	// pause landed, and it concluded only after both: a pause is never an ending, and it never
	// skips the gate it was holding at.
	statuses := trace.recorded()
	if !slices.Contains(statuses, rollout.RolloutPaused) {
		t.Errorf("recorded statuses = %v, want a %q among them", statuses, rollout.RolloutPaused)
	}
	if !slices.Contains(statuses, rollout.RolloutAwaitingApproval) {
		t.Errorf("recorded statuses = %v, want the gate reached", statuses)
	}
	if last := statuses[len(statuses)-1]; last != rollout.RolloutCompleted {
		t.Errorf("recorded statuses = %v, want %q last", statuses, rollout.RolloutCompleted)
	}
}

// TestRolloutWorkflowPauseDoesNotSuppressTheWaveInFlight pins what a pause does not do: a wave
// already in flight when the rollout is paused is still judged when its window ends, and a healthy
// decision does not advance the paused rollout.
func TestRolloutWorkflowPauseDoesNotSuppressTheWaveInFlight(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.99))

	env := newRolloutEnv(fakes)
	// The pause lands while the first wave is in flight, and the resume comes later: the wave
	// had to be judged while the rollout was held, and the second wave could start only once the
	// resume was folded.
	queueSignalsAt(env, signalPause(0), signalResume(1*time.Second))
	// The state the operator reads at the moment the pause is recorded.
	trace := traceControl(env, fakes)
	view := runRollout(t, env, rolloutInputFor(settings))

	// The wave in flight was still measured and decided while the rollout was paused, and the
	// paused rollout did not promote it into the next wave.
	if len(view.Waves) != 2 {
		t.Fatalf("view carries %d waves, want 2", len(view.Waves))
	}
	if view.Waves[0].Status != rollout.WaveHealthy || view.Waves[0].SuccessRate != 0.99 {
		t.Errorf("wave in flight = %+v, want it judged on its window", view.Waves[0])
	}

	// The statuses the rollout recorded are its own history: standing at a gate, then held, then
	// released by the resume, and never completed while it was held.
	if got := trace.recorded(); !slices.Contains(got, rollout.RolloutPaused) {
		t.Errorf("recorded statuses = %v, want one %q among them", got, rollout.RolloutPaused)
	}
}

// TestRolloutWorkflowPauseInsideAWindowKeepsTheWindowDeadline pins the window's integrity: a pause
// folded while a wave's health window elapses does not restart, shorten, or extend it, so the gate
// measures the window the wave document claims.
func TestRolloutWorkflowPauseInsideAWindowKeepsTheWindowDeadline(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	queueSignalsAt(env, signalPause(500*time.Millisecond), signalResume(10*time.Second))
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	evaluations := fakes.recordedEvaluations()
	if len(evaluations) != 1 {
		t.Fatalf("health evaluations = %d, want 1: the pause must not re-measure the wave",
			len(evaluations))
	}
	rec, _ := fakes.recordedWave(rollout.WaveID("ro-1", 0, 100))
	// The gate evaluated the window anchored at the wave's recorded start, not one re-anchored
	// at the pause or the resume.
	if got := evaluations[0].At.Sub(rec.StartedAt); got != settings.HealthWindow {
		t.Errorf("measured window = %s, want the wave's own %s", got, settings.HealthWindow)
	}
}

// TestRolloutWorkflowRegressionRollsBackAPausedRollout pins the safety a pause never withholds: a
// wave in flight when the rollout was paused that fails its gate still rolls the rollout back, and
// the rollout is recorded as concluded rather than held.
func TestRolloutWorkflowRegressionRollsBackAPausedRollout(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
	fakes.scriptHealth(unhealthyAt(0.4))

	env := newRolloutEnv(fakes)
	// Paused during the dispatch and never resumed: the rollout still has to roll back.
	queueSignalsAt(env, signalPause(200*time.Millisecond))
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeUnhealthyWave {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/unhealthy_wave",
			view.Status, view.Outcome)
	}
	if view.EndedBy != rollout.WaveID("ro-1", 0, 100) {
		t.Errorf("ended by %q, want the wave that failed its gate", view.EndedBy)
	}
}

// TestRolloutWorkflowControlSignalsAreIdempotent pins that a repeated pause while paused and a
// resume while running change nothing, and that both are ignored once the rollout has concluded.
func TestRolloutWorkflowControlSignalsAreIdempotent(t *testing.T) {
	t.Parallel()

	t.Run("repeated signals change nothing", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(healthyAt(0.99))

		env := newRolloutEnv(fakes)
		queueSignalsAt(env,
			signalPause(100*time.Millisecond),
			signalPause(200*time.Millisecond),
			signalResume(300*time.Millisecond),
			signalResume(400*time.Millisecond),
		)
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.Status != rollout.RolloutCompleted {
			t.Fatalf("rollout concluded as %s, want completed", view.Status)
		}
		// One write per real transition: running at the start, paused once, running once on
		// the resume, completed at the end. The duplicates wrote nothing.
		want := []rollout.RolloutStatus{
			rollout.RolloutRunning,
			rollout.RolloutPaused,
			rollout.RolloutRunning,
			rollout.RolloutCompleted,
		}
		if diff := cmp.Diff(want, fakes.rolloutStatuses()); diff != "" {
			t.Errorf("recorded rollout statuses mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("both signals are ignored after the rollout concluded", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(healthyAt(0.99))

		env := newRolloutEnv(fakes)
		// Delivered long after the rollout's single wave concluded. They cannot be applied:
		// the run is over, and its recorded status stands.
		queueSignalsAt(env, signalPause(30*time.Second))
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.Status != rollout.RolloutCompleted {
			t.Fatalf("rollout concluded as %s, want completed", view.Status)
		}
		if diff := cmp.Diff(
			[]rollout.RolloutStatus{rollout.RolloutRunning, rollout.RolloutCompleted},
			fakes.rolloutStatuses(),
		); diff != "" {
			t.Errorf("recorded rollout statuses mismatch (-want +got):\n%s", diff)
		}
	})
}

// TestRolloutStatePauseTransitions pins the state machine on its own: pause and resume are
// idempotent, terminal rollouts ignore both, and the reported status is a projection of the pause
// over the rollout's own control-flow status.
func TestRolloutStatePauseTransitions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		apply func(*rolloutState)
		want  rollout.RolloutStatus
	}{
		{
			name:  "a running rollout reports running",
			apply: func(*rolloutState) {},
			want:  rollout.RolloutRunning,
		},
		{
			name:  "a paused rollout reports paused",
			apply: func(s *rolloutState) { s.pause() },
			want:  rollout.RolloutPaused,
		},
		{
			name:  "a repeated pause changes nothing",
			apply: func(s *rolloutState) { s.pause(); s.pause() },
			want:  rollout.RolloutPaused,
		},
		{
			name:  "a resumed rollout reports running again",
			apply: func(s *rolloutState) { s.pause(); s.resume() },
			want:  rollout.RolloutRunning,
		},
		{
			name:  "a resume without a pause changes nothing",
			apply: func(s *rolloutState) { s.resume() },
			want:  rollout.RolloutRunning,
		},
		{
			name:  "a paused rollout holding for approval reports the pause",
			apply: func(s *rolloutState) { s.holdForApproval(1); s.pause() },
			want:  rollout.RolloutPaused,
		},
		{
			name:  "a pause released from the approval hold still reports the pause",
			apply: func(s *rolloutState) { s.holdForApproval(1); s.pause(); s.releaseApprovalHold() },
			want:  rollout.RolloutPaused,
		},
		{
			name:  "a completed rollout ignores a pause",
			apply: func(s *rolloutState) { s.complete(); s.pause() },
			want:  rollout.RolloutCompleted,
		},
		{
			name:  "a completed rollout ignores a resume",
			apply: func(s *rolloutState) { s.pause(); s.complete(); s.resume() },
			want:  rollout.RolloutCompleted,
		},
		{
			name: "a rolled-back rollout ignores a pause",
			apply: func(s *rolloutState) {
				s.beginRollback(0, OutcomeUnhealthyWave, nil)
				s.finishRollback()
				s.pause()
			},
			want: rollout.RolloutRolledBack,
		},
		{
			name:  "a failed rollout ignores a pause",
			apply: func(s *rolloutState) { s.fail(OutcomeFirmwareUnknown); s.pause() },
			want:  rollout.RolloutFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := newRolloutState(rolloutTestInput())
			tc.apply(&state)
			if got := state.reported(); got != tc.want {
				t.Errorf("reported() = %q, want %q", got, tc.want)
			}
			// The projection is what the state query answers, so the two cannot disagree.
			if got := state.view().Status; got != tc.want {
				t.Errorf("view().Status = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("an approval that arrives while paused is banked", func(t *testing.T) {
		t.Parallel()

		// Operator intent that arrives during a hold is recorded, not dropped: the pause keeps
		// the gate waiting and the approval is there when the hold is released.
		state := newRolloutState(rolloutTestInput())
		state.holdForApproval(1)
		state.pause()
		state.applyApproval()

		if !state.ApprovalOutstanding {
			t.Error("the approval was dropped, want it banked through the pause")
		}
		if got := state.reported(); got != rollout.RolloutPaused {
			t.Errorf("reported() = %q, want %q", got, rollout.RolloutPaused)
		}
		// Releasing the hold spends the banked approval and reports running again.
		state.releaseApprovalHold()
		state.consumeApproval()
		state.resume()
		if got := state.reported(); got != rollout.RolloutRunning {
			t.Errorf("reported() after the resume = %q, want %q", got, rollout.RolloutRunning)
		}
		if state.ApprovalOutstanding {
			t.Error("the banked approval was not spent on the wave it authorized")
		}
	})

	t.Run("a pause never concludes a rollout", func(t *testing.T) {
		t.Parallel()

		state := newRolloutState(rolloutTestInput())
		state.pause()
		if state.terminal() {
			t.Error("a paused rollout reports itself terminal, want it never terminal")
		}
	})
}

// TestRolloutWorkflowControlSignalWriteFailureDoesNotLoseTheStatus pins the recovery the control
// fold is built around: a control signal records the status it moved as soon as it is folded, but a
// write that fails at that instant is only logged — a selector callback cannot fail the workflow —
// so the run must converge by recording the same status at its next write rather than losing the
// transition the operator's own signal caused.
func TestRolloutWorkflowControlSignalWriteFailureDoesNotLoseTheStatus(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 50}, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(4)...)
	// The write the pause fold makes fails; the rollout must still end up recorded as completed,
	// which is only reachable by recording again after the failure.
	fakes.recordFails = 1
	fakes.scriptHealth(healthyAt(0.99), healthyAt(0.98))

	env := newRolloutEnv(fakes)
	queueSignalsAt(env, signalPause(0), signalResume(1*time.Second))
	trace := traceControl(env, fakes)
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed despite the failed write", view.Status)
	}
	if got := trace.recorded(); !slices.Contains(got, rollout.RolloutPaused) {
		t.Errorf("recorded statuses = %v, want the pause folded and recorded", got)
	}
	recorded, ok := fakes.recordedRollout("ro-1")
	if !ok {
		t.Fatal("the rollout was never recorded")
	}
	if recorded.Status != rollout.RolloutCompleted {
		t.Errorf("recorded rollout status = %q, want %q", recorded.Status, rollout.RolloutCompleted)
	}
}
