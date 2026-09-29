//go:build integration

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	sdktemporal "go.temporal.io/sdk/temporal"
)

// The gate's own tests: the plan's decisions, the two injected endings, and the ledger's view of the
// work. They drive the gate directly, with no worker and no containers.

// TestRestartPlanDecidesEachAttempt is the plan's own table: a boundary plan holds only the first
// attempt of the activities it names, a crash-after plan runs and fails only the first attempt, and
// an activity no plan names runs every attempt. It needs no containers.
func TestRestartPlanDecidesEachAttempt(t *testing.T) {
	tests := []struct {
		name     string
		plan     *restartPlan
		activity string
		attempt  int32
		want     gateDecision
	}{
		{
			name: "a boundary plan holds the first attempt", plan: holdEvery("update-device"),
			activity: "update-device", attempt: 1, want: gateHold,
		},
		{
			name: "a boundary plan runs the first retry", plan: holdEvery("update-device"),
			activity: "update-device", attempt: 2, want: gateRun,
		},
		{
			name:     "a boundary plan holds every activity it names",
			plan:     holdEvery("update-device", "downgrade-device"),
			activity: "downgrade-device", attempt: 1, want: gateHold,
		},
		{
			name: "a boundary plan runs an activity it does not name",
			plan: holdEvery("update-device"), activity: "record-wave-state", attempt: 1, want: gateRun,
		},
		{
			name: "a crash-after plan fails the first attempt after running it",
			plan: crashAfterEvery("load-firmware"), activity: "load-firmware", attempt: 1,
			want: gateCrashAfter,
		},
		{
			name: "a crash-after plan runs the first retry",
			plan: crashAfterEvery("load-firmware"), activity: "load-firmware", attempt: 2, want: gateRun,
		},
		{
			name: "a crash-after plan runs an activity it does not name",
			plan: crashAfterEvery("load-firmware"), activity: "update-device", attempt: 1, want: gateRun,
		},
		{
			name: "an empty plan runs every attempt", plan: &restartPlan{},
			activity: "update-device", attempt: 1, want: gateRun,
		},
		{
			name: "no plan runs every attempt", plan: nil,
			activity: "update-device", attempt: 1, want: gateRun,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.plan.decide(tc.activity, tc.attempt); got != tc.want {
				t.Errorf("decide(%s, %d) = %q, want %q", tc.activity, tc.attempt, got, tc.want)
			}
		})
	}
}

// TestActivityGateAppliesThePlanToEachAttempt drives the gate with synthetic attempts and a
// stand-in for the wrapped activity, so the two injected endings and the ledger are asserted without
// a rollout: a held attempt is ended by the harness's stop and executes nothing, a crash-after
// attempt executes the activity and is then failed, and an attempt no plan names simply runs. It
// needs no containers.
func TestActivityGateAppliesThePlanToEachAttempt(t *testing.T) {
	tests := []struct {
		name string
		plan *restartPlan
		// activity is the registered name every attempt of the case carries.
		activity string
		// attempts is how many attempts the case drives, numbered from one as the server stamps
		// them.
		attempts int32
		// wantExecuted are the attempts whose wrapped activity ran.
		wantExecuted []int32
		// wantOutcomes is how each attempt ended.
		wantOutcomes map[int32]attemptOutcome
		// wantFailed are the attempts that ended with the injected failure. The gate is the only
		// source of a failure in this test: the stand-in activity always returns cleanly.
		wantFailed map[int32]bool
	}{
		{
			name: "boundary plan: the first attempt is held and cancelled, the retry runs",
			plan: holdEvery("update-device"), activity: "update-device", attempts: 2,
			wantExecuted: []int32{2},
			wantOutcomes: map[int32]attemptOutcome{1: attemptCancelled, 2: attemptRan},
			wantFailed:   map[int32]bool{1: true},
		},
		{
			name: "crash-after plan: the first attempt runs and is failed, the retry runs",
			plan: crashAfterEvery("load-firmware"), activity: "load-firmware", attempts: 2,
			wantExecuted: []int32{1, 2},
			wantOutcomes: map[int32]attemptOutcome{1: attemptCrashed, 2: attemptRan},
			wantFailed:   map[int32]bool{1: true},
		},
		{
			name: "an activity the plan does not name runs every attempt",
			plan: crashAfterEvery("load-firmware"), activity: "record-wave-state", attempts: 2,
			wantExecuted: []int32{1, 2},
			wantOutcomes: map[int32]attemptOutcome{1: attemptRan, 2: attemptRan},
		},
		{
			name: "an empty plan runs every attempt",
			plan: &restartPlan{}, activity: "update-device", attempts: 3,
			wantExecuted: []int32{1, 2, 3},
			wantOutcomes: map[int32]attemptOutcome{1: attemptRan, 2: attemptRan, 3: attemptRan},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ledger := &attemptLedger{}
			gate := newActivityGate(tc.plan, ledger)
			gate.worker = func() string { return "suite-worker-1" }

			// executions counts how many times the wrapped activity ran for each attempt: a held
			// attempt executes nothing at all.
			executions := make(map[int32]int, tc.attempts)
			for attempt := int32(1); attempt <= tc.attempts; attempt++ {
				current := attempt
				ctx, cancel := context.WithCancel(context.Background())
				held := make(chan struct{})
				// The harness's side of a hold: it reads the notice and stops the worker, which
				// cancels the attempt. The goroutine ends with the attempt either way.
				go func() {
					select {
					case <-gate.holds:
						close(held)
						cancel()
					case <-ctx.Done():
					}
				}()

				_, err := gate.execute(ctx, gateAttempt{
					ActivityID: "1",
					Name:       tc.activity,
					Attempt:    current,
					WorkflowID: "rollout-ro-gate",
					RunID:      "run-1",
				}, func(context.Context) (any, error) {
					executions[current]++
					return "updated", nil
				})
				cancel()

				if want := tc.wantFailed[current]; want != (err != nil) {
					t.Errorf("attempt %d returned %v, want an injected failure: %t", current, err, want)
				}
				if tc.wantOutcomes[current] == attemptCancelled {
					// The attempt can only have ended as cancelled after the harness was asked to
					// restart the worker that held it.
					select {
					case <-held:
					default:
						t.Errorf("attempt %d ended as cancelled without a restart being asked for", current)
					}
				}
				if err != nil {
					var appErr *sdktemporal.ApplicationError
					if !errors.As(err, &appErr) || appErr.Type() != restartInjectedType {
						t.Errorf("attempt %d failed with %v, want a retryable %s error",
							current, err, restartInjectedType)
					}
				}
			}

			wantExecutions := make(map[int32]int, len(tc.wantExecuted))
			for _, attempt := range tc.wantExecuted {
				wantExecutions[attempt] = 1
			}
			if diff := cmp.Diff(wantExecutions, executions); diff != "" {
				t.Errorf("executions per attempt mismatch (-want +got):\n%s", diff)
			}

			wantLedger := make([]attemptOutcome, 0, tc.attempts)
			for attempt := int32(1); attempt <= tc.attempts; attempt++ {
				wantLedger = append(wantLedger, tc.wantOutcomes[attempt])
			}
			gotLedger := make([]attemptOutcome, 0, tc.attempts)
			for _, attempt := range ledger.all() {
				gotLedger = append(gotLedger, attempt.Outcome)
				if attempt.Worker != "suite-worker-1" || attempt.ActivityID != "1" ||
					attempt.Name != tc.activity || attempt.WorkflowID != "rollout-ro-gate" {
					t.Errorf("ledger attempt = %+v, want the attempt it observed", attempt)
				}
			}
			if diff := cmp.Diff(wantLedger, gotLedger); diff != "" {
				t.Errorf("ledger outcomes mismatch (-want +got):\n%s", diff)
			}
			if got := ledger.byName()[tc.activity]; got != int(tc.attempts) {
				t.Errorf("ledger attempts for %s = %d, want %d", tc.activity, got, tc.attempts)
			}
		})
	}
}

// TestAttemptLedgerGroupsRetriesIntoOneUnit asserts the ledger's view of the work the workflow
// decided: retries of one logical unit share its activity id and are grouped together, while two
// units of the same activity stay distinct. It needs no containers.
func TestAttemptLedgerGroupsRetriesIntoOneUnit(t *testing.T) {
	ledger := &attemptLedger{}
	ledger.record(activityAttempt{ActivityID: "7", Name: "update-device", Attempt: 1})
	ledger.record(activityAttempt{ActivityID: "7", Name: "update-device", Attempt: 2})
	ledger.record(activityAttempt{ActivityID: "8", Name: "update-device", Attempt: 1})
	ledger.record(activityAttempt{ActivityID: "9", Name: "evaluate-wave-health", Attempt: 1})

	units := ledger.units()
	if len(units) != 3 {
		t.Errorf("units = %d, want one per activity id", len(units))
	}
	if got := len(units["7"]); got != 2 {
		t.Errorf("attempts of unit 7 = %d, want the retry grouped with its first attempt", got)
	}
	if got := ledger.byName()["update-device"]; got != 3 {
		t.Errorf("attempts of update-device = %d, want 3", got)
	}
	if got := ledger.count(); got != 4 {
		t.Errorf("ledger count = %d, want 4", got)
	}
	if got := len(ledger.filter(func(a activityAttempt) bool { return a.Name == "update-device" })); got != 3 {
		t.Errorf("filtered attempts = %d, want 3", got)
	}
}
