//go:build integration

package main

import (
	"sync"
)

// The attempt ledger: what the gate observed, so a scenario can state how many restarts it
// injected and how each attempt ended instead of inferring both from timing.

// attemptOutcome is how one activity attempt ended.
type attemptOutcome string

const (
	// attemptRan is an attempt whose activity executed and returned without an injected failure.
	attemptRan attemptOutcome = "ran"
	// attemptCrashed is an attempt whose activity executed and was then failed on purpose.
	attemptCrashed attemptOutcome = "crashed-after"
	// attemptCancelled is a held attempt the worker's stop cancelled: its activity never executed.
	attemptCancelled attemptOutcome = "cancelled"
)

// activityAttempt is one attempt of one activity as the gate observed it: which logical unit it
// belongs to, which worker generation picked it up, and how it ended.
type activityAttempt struct {
	// ActivityID is the workflow's identity for the logical unit this attempt belongs to. Every
	// retry of one unit repeats it, which is what makes "the work was decided once" assertable.
	ActivityID string
	// Name is the activity's registered name.
	Name string
	// Attempt is the attempt number the server stamped, starting at one.
	Attempt int32
	// WorkflowID and RunID identify the workflow execution the attempt belongs to.
	WorkflowID string
	RunID      string
	// Worker is the identity of the worker generation that picked the attempt up.
	Worker string
	// Decision is what the plan said to do with the attempt.
	Decision gateDecision
	// Outcome is how the attempt ended.
	Outcome attemptOutcome
	// Err is the failure the attempt ended with; empty for an attempt that returned cleanly.
	Err string
}

// attemptLedger records every attempt the suite's worker picked up, so a scenario can state how many
// restarts it injected and how each attempt ended rather than inferring both from timing.
type attemptLedger struct {
	mu       sync.Mutex
	attempts []activityAttempt
}

// record appends one observed attempt.
func (l *attemptLedger) record(attempt activityAttempt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts = append(l.attempts, attempt)
}

// all returns every recorded attempt, in pickup order.
func (l *attemptLedger) all() []activityAttempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]activityAttempt(nil), l.attempts...)
}

// count returns how many attempts were recorded.
func (l *attemptLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.attempts)
}

// byName counts the recorded attempts per activity name.
func (l *attemptLedger) byName() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	counts := make(map[string]int, len(l.attempts))
	for _, attempt := range l.attempts {
		counts[attempt.Name]++
	}
	return counts
}

// units groups the recorded attempts by the activity id they belong to, keeping the order in which
// each unit was first seen. A unit is one piece of work the workflow decided to run; its retries
// share the id and appear in attempt order.
func (l *attemptLedger) units() map[string][]activityAttempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	units := make(map[string][]activityAttempt)
	for _, attempt := range l.attempts {
		units[attempt.ActivityID] = append(units[attempt.ActivityID], attempt)
	}
	return units
}

// held returns the attempts the plan held, in pickup order.
func (l *attemptLedger) held() []activityAttempt {
	return l.filter(func(attempt activityAttempt) bool { return attempt.Decision == gateHold })
}

// crashed returns the attempts whose activity executed and was then failed on purpose.
func (l *attemptLedger) crashed() []activityAttempt {
	return l.filter(func(attempt activityAttempt) bool { return attempt.Outcome == attemptCrashed })
}

// filter returns the recorded attempts matching keep, in pickup order.
func (l *attemptLedger) filter(keep func(activityAttempt) bool) []activityAttempt {
	var matched []activityAttempt
	for _, attempt := range l.all() {
		if keep(attempt) {
			matched = append(matched, attempt)
		}
	}
	return matched
}
