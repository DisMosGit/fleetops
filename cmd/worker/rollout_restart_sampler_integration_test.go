//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// The world every restart scenario drives: one region of one model running the firmware the smoke
// deploys, so the rollout's targeting, command identity, and version resolution are the production
// ones.
// resumptionSampler reads a rollout's state query after every injected restart and refuses a sample
// that regressed against the previous one: a restart that lost folded state — a promoted wave, an
// approval, a pause — shows up as a regression rather than as a rollout that took an odd route.
type resumptionSampler struct {
	states  *temporal.RolloutStates
	rollout string

	mu      sync.Mutex
	last    *stateSample
	samples int
}

// stateSample is one reading of a rollout's authoritative state, reduced to what a restart could
// regress: the status the operator sees, the position of the wave in flight, and each wave's
// recorded status.
type stateSample struct {
	Status  rollout.RolloutStatus
	Current int
	Waves   []temporal.WaveView
}

// newResumptionSampler returns a sampler over one rollout's state query.
func newResumptionSampler(t *testing.T, c temporalclient.Client, rolloutID string) *resumptionSampler {
	t.Helper()
	return &resumptionSampler{states: temporal.NewRolloutStates(c), rollout: rolloutID}
}

// sample reads the rollout's state and refuses it when it regressed against the sample before it.
// The read is retried while the fresh worker generation has not started answering queries yet: the
// sample is taken moments after the generation that answered the previous one stopped, and a
// workflow whose worker is gone answers nothing until one polls again.
func (s *resumptionSampler) sample(ctx context.Context) error {
	deadline := time.Now().Add(restartQueryGrace)
	var lastErr error
	attempts := 0
	for {
		attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, restartQueryAttempt)
		view, err := s.states.State(attemptCtx, s.rollout)
		cancel()
		if err == nil {
			return s.observe(view)
		}
		lastErr = err
		if !time.Now().Before(deadline) {
			return fmt.Errorf("read rollout %s state after a restart: %d attempts over %v: %w",
				s.rollout, attempts, restartQueryGrace, lastErr)
		}
		time.Sleep(smokePollInterval)
	}
}

// observe compares one reading with the sample before it and keeps it.
func (s *resumptionSampler) observe(view temporal.RolloutView) error {
	next := stateSample{Status: view.Status, Current: view.Current, Waves: view.Waves}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last != nil {
		if err := regressionBetween(*s.last, next); err != nil {
			return fmt.Errorf("rollout %s after restart %d: %w", s.rollout, s.samples+1, err)
		}
	}
	s.last = &next
	s.samples++
	return nil
}

// count returns how many samples the sampler took.
func (s *resumptionSampler) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.samples
}

// regressionBetween reports the regression next shows against prev, or nil when next is a state a
// restart could legitimately have produced from prev. The three rules are the three ways a restart
// could lose folded state: a promoted wave cannot become unpromoted, a recorded status cannot move
// backwards, and the wave in flight cannot move back.
func regressionBetween(prev, next stateSample) error {
	if statusProgress(next.Status) < statusProgress(prev.Status) {
		return fmt.Errorf("status moved back from %s to %s", prev.Status, next.Status)
	}
	if prev.Current >= 0 && next.Current >= 0 && next.Current < prev.Current {
		return fmt.Errorf("the wave in flight moved back from %d to %d", prev.Current, next.Current)
	}
	for position := range prev.Waves {
		if position >= len(next.Waves) {
			break
		}
		before, after := prev.Waves[position].Status, next.Waves[position].Status
		// A promoted wave is the strongest statement a wave makes, so it never becomes anything
		// else — not even another decided status of the same rank.
		if before == rollout.WaveHealthy && after != rollout.WaveHealthy {
			return fmt.Errorf("wave %d was promoted and is now %s", position, after)
		}
		if waveProgress(after) < waveProgress(before) {
			return fmt.Errorf("wave %d moved back from %s to %s", position, before, after)
		}
	}
	return nil
}

// statusProgress orders a rollout's statuses by how far the rollout has come. Running, paused, and
// awaiting an approval are one step: an operator's hold and a gate are positions inside a forward
// run, not progress. Entering rollback and concluding are the steps after them, and no status ever
// moves back a step.
func statusProgress(status rollout.RolloutStatus) int {
	switch status {
	case rollout.RolloutRollingBack:
		return 2
	case rollout.RolloutRolledBack, rollout.RolloutCompleted, rollout.RolloutFailed:
		return 3
	default:
		return 1
	}
}

// waveProgress orders a wave's statuses the same way: a resolved membership precedes a measured
// window, and a decided wave is the last step. A wave never moves back a step, and a promoted wave
// never leaves the last one.
func waveProgress(status rollout.WaveStatus) int {
	switch status {
	case rollout.WavePending:
		return 0
	case rollout.WaveDispatching:
		return 1
	case rollout.WaveEvaluating:
		return 2
	default:
		// Every decided status — healthy, unhealthy, failed, skipped — is the last step.
		return 3
	}
}

// TestRegressionBetweenFlagsOnlyRegressions is the resumption detector's own table: a sample that
// advances, holds, or repeats is a state a restart could have produced, while each of the three ways
// a restart could lose folded state — a promoted wave that became unpromoted, a recorded status that
// moved backwards, and a wave in flight that moved back — is refused. It needs no containers.
func TestRegressionBetweenFlagsOnlyRegressions(t *testing.T) {
	sample := func(
		status rollout.RolloutStatus,
		current int,
		waves ...rollout.WaveStatus,
	) stateSample {
		views := make([]temporal.WaveView, 0, len(waves))
		for _, wave := range waves {
			views = append(views, temporal.WaveView{Status: wave})
		}
		return stateSample{Status: status, Current: current, Waves: views}
	}
	tests := []struct {
		name string
		prev stateSample
		next stateSample
		// want is the regression the detector has to name; empty means the sample is accepted.
		want string
	}{
		{
			name: "an identical reading holds",
			prev: sample(rollout.RolloutRunning, 0, rollout.WaveDispatching),
			next: sample(rollout.RolloutRunning, 0, rollout.WaveDispatching),
		},
		{
			name: "a wave advances from resolved to measured",
			prev: sample(rollout.RolloutRunning, 0, rollout.WaveDispatching),
			next: sample(rollout.RolloutRunning, 0, rollout.WaveEvaluating),
		},
		{
			name: "a wave is promoted and the next one is dispatched",
			prev: sample(rollout.RolloutRunning, 0, rollout.WaveEvaluating, rollout.WavePending),
			next: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveDispatching),
		},
		{
			name: "a gate holds the rollout without progress",
			prev: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WavePending),
			next: sample(rollout.RolloutAwaitingApproval, 1, rollout.WaveHealthy, rollout.WavePending),
		},
		{
			name: "a pause holds the rollout without progress",
			prev: sample(rollout.RolloutRunning, 0, rollout.WaveEvaluating),
			next: sample(rollout.RolloutPaused, 0, rollout.WaveEvaluating),
		},
		{
			name: "a resumed rollout keeps its progress",
			prev: sample(rollout.RolloutPaused, 0, rollout.WaveEvaluating),
			next: sample(rollout.RolloutRunning, 0, rollout.WaveEvaluating),
		},
		{
			name: "a concluded rollout leaves its wave behind",
			prev: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveEvaluating),
			next: sample(rollout.RolloutCompleted, -1, rollout.WaveHealthy, rollout.WaveHealthy),
		},
		{
			name: "a rollback advances through compensation to its conclusion",
			prev: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveUnhealthy),
			next: sample(rollout.RolloutRolledBack, -1, rollout.WaveHealthy, rollout.WaveUnhealthy),
		},
		{
			name: "a promoted wave becoming unpromoted is refused",
			prev: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveHealthy),
			next: sample(rollout.RolloutPaused, 1, rollout.WaveHealthy, rollout.WaveEvaluating),
			want: "was promoted",
		},
		{
			name: "a decided wave becoming undecided is refused",
			prev: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveUnhealthy),
			next: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveDispatching),
			want: "moved back",
		},
		{
			name: "a status moving backwards is refused",
			prev: sample(rollout.RolloutRollingBack, -1, rollout.WaveHealthy),
			next: sample(rollout.RolloutRunning, -1, rollout.WaveHealthy),
			want: "status moved back",
		},
		{
			name: "a terminal status moving backwards is refused",
			prev: sample(rollout.RolloutRolledBack, -1, rollout.WaveUnhealthy),
			next: sample(rollout.RolloutRollingBack, -1, rollout.WaveUnhealthy),
			want: "status moved back",
		},
		{
			name: "the wave in flight moving back is refused",
			prev: sample(rollout.RolloutRunning, 2, rollout.WaveHealthy, rollout.WaveHealthy, rollout.WaveEvaluating),
			next: sample(rollout.RolloutRunning, 1, rollout.WaveHealthy, rollout.WaveHealthy, rollout.WaveEvaluating),
			want: "wave in flight moved back",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := regressionBetween(tc.prev, tc.next)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("regressionBetween(%+v, %+v) = %v, want the sample accepted", tc.prev, tc.next, err)
			case tc.want != "" && err == nil:
				t.Errorf("regressionBetween(%+v, %+v) = nil, want the regression %q", tc.prev, tc.next, tc.want)
			case tc.want != "" && !strings.Contains(err.Error(), tc.want):
				t.Errorf("regressionBetween(%+v, %+v) = %v, want it to name %q", tc.prev, tc.next, err, tc.want)
			}
		})
	}
}
