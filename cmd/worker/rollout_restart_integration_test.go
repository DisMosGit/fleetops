//go:build integration

// The restart suite proves what a worker restart does to a rollout in flight. The CLI-based smoke
// in rollout_integration_test.go drives a rollout to completion on one worker; this suite drives the
// same workflow — the worker's own registrations, the real fleet database, the real broker, and a
// containerized Temporal server — while stopping and starting the worker that executes it, so the
// durability the workflow's own comment claims is something these tests assert rather than
// something a reader has to believe:
//
//   - every scenario samples the rollout's state after each injected restart and refuses a sample
//     that regressed, which is where "it resumes from the correct point" is measured;
//   - every scenario asserts the work the workflow decided and the effects it applied by identity,
//     so a restart that re-decided work or applied one effect twice fails the suite;
//   - every scenario replays its completed history against the rollout workflow, so a run that
//     passes once and flaps on replay is a bug here.
//
// Restarts are injected, never raced. The worker runs with an activity interceptor — the
// activityGate below — whose plan decides, per activity and attempt, whether an attempt runs
// normally, is held until the harness has stopped the worker that picked it up, or executes and is
// then failed the way a worker that died after its effect landed looks to Temporal. The attempt
// ledger records what happened, so a scenario states how many restarts it injected instead of
// inferring it from timing.
//
// The suite needs Docker: it boots the fleet database and the broker the integration suites already
// use, plus the containerized Temporal server from internal/temporaltest, and it needs no temporal
// CLI on PATH. Run it with:
//
//	go test -tags integration ./cmd/worker/ -run TestRolloutRestarts -count=1 -v
//
// Scenarios are deliberately not parallel: each boots its own four-container stack, and container
// startup dominates the suite's cost.
package main

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"

	"github.com/DisMosGit/fleetops/internal/temporal"
)

const (
	// restartStopTimeout bounds a worker stop while an activity attempt is held. It is deliberately
	// short: a stop cancels the held attempt, and the cancellation has to happen well inside the
	// update activity's own heartbeat timeout, or the server would retry the attempt while the old
	// worker was still polling and the restart would stop being a fact the suite injected.
	restartStopTimeout = 500 * time.Millisecond
	// restartStopGrace bounds waiting for a stopped worker's Run to return. A worker whose
	// activities are held returns as soon as the stop timeout cancels them; the grace only covers
	// the runtime's own bookkeeping.
	restartStopGrace = 30 * time.Second
	// restartNoticeLimit is how many hold notices the gate may have outstanding. One notice per
	// worker generation is enough — a stop ends every attempt that generation held — so the
	// channel only has to absorb a burst of concurrent holds before the harness reacts.
	restartNoticeLimit = 16
	// restartSettle is how long an assertion that nothing more happened waits before it compares
	// the world with itself: it gives a would-be effect the chance to appear, and nothing about
	// the assertion depends on when it happens.
	restartSettle = 3 * time.Second
	// restartInjectedType is the application-error type the gate fails an injected attempt with. It
	// is a plain application error on purpose: Temporal's own retry policy owns it, exactly as it
	// owns a real worker death, and the workflow's start-failure mapping does not recognize it.
	restartInjectedType = "restart_injected"
)

// gateDecision is what the harness does with one activity attempt.
type gateDecision string

const (
	// gateRun executes the attempt normally.
	gateRun gateDecision = "run"
	// gateHold blocks the attempt until the harness has stopped the worker that picked it up. The
	// stop is what ends it: the attempt is cancelled, never executed, and Temporal's retry
	// delivers the same logical unit to the fresh worker.
	gateHold gateDecision = "hold"
	// gateCrashAfter executes the attempt and then fails it, which is what a worker that died
	// after the attempt's effect landed but before its result was recorded looks like to Temporal.
	// The harness restarts the worker before the retry, so the attempt that repeats the effect runs
	// on a worker that never saw the first one.
	gateCrashAfter gateDecision = "crash-after"
)

// restartPlan is the suite's restart-injection policy: which activities have their first attempt
// held while the worker is restarted, and which have it executed and then failed. Only the first
// attempt of an activity is ever injected — a redelivered unit must run, or the rollout could never
// progress — so a plan is a statement about boundaries and about effects, not a way to break the
// workflow.
type restartPlan struct {
	// holds are the registered activity names whose first attempt is held.
	holds map[string]bool
	// crashes are the registered activity names whose first attempt runs and is then failed.
	crashes map[string]bool
}

// holdEvery returns the boundary-restart plan: the first attempt of every named activity is held
// until the harness has stopped the worker that picked it up.
func holdEvery(names ...string) *restartPlan {
	return &restartPlan{holds: nameSet(names)}
}

// crashAfterEvery returns the post-effect-restart plan: the first attempt of every named activity
// executes and is then failed, so its retry lands on a fresh worker after the effect was applied.
func crashAfterEvery(names ...string) *restartPlan {
	return &restartPlan{crashes: nameSet(names)}
}

// nameSet indexes names for the membership tests a plan makes.
func nameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

// decide returns what happens to one attempt of one activity: the first attempt of a held activity
// is held, the first attempt of a crashed activity runs and is failed, and every other attempt runs.
func (p *restartPlan) decide(name string, attempt int32) gateDecision {
	switch {
	case p == nil:
		return gateRun
	case attempt == 1 && p.crashes[name]:
		return gateCrashAfter
	case attempt == 1 && p.holds[name]:
		return gateHold
	default:
		return gateRun
	}
}

// rolloutActivityNames returns the activities the worker registers for the rollout, which is the
// whole set a boundary restart can be injected at.
func rolloutActivityNames() []string {
	return []string{
		temporal.LoadFirmwareActivityName,
		temporal.ResolveWaveTargetsActivityName,
		temporal.RecordRolloutStateActivityName,
		temporal.RecordWaveStateActivityName,
		temporal.UpdateDeviceActivityName,
		temporal.DowngradeDeviceActivityName,
		temporal.ReconcileInventoryActivityName,
		temporal.AnnounceRollbackActivityName,
		temporal.EvaluateWaveHealthActivityName,
	}
}

// compensationActivityNames returns the activities a rollback's plan runs, which is the set a
// restart inside the rollback is injected at.
func compensationActivityNames() []string {
	return []string{
		temporal.AnnounceRollbackActivityName,
		temporal.DowngradeDeviceActivityName,
		temporal.ReconcileInventoryActivityName,
	}
}

// activityGate is the test-only worker interceptor that injects the suite's restarts. It wraps every
// activity attempt the harness worker picks up, records it in the ledger, and applies the plan: an
// attempt either executes the activity the worker really registered, is held until the worker's stop
// cancels it, or executes and is then failed.
//
// The gate is a worker interceptor rather than a wrapper registered in place of the real activity,
// so what the suite restarts around is the worker's own registrations and its own retry policies.
type activityGate struct {
	interceptor.WorkerInterceptorBase

	plan   *restartPlan
	ledger *attemptLedger
	// holds receives one notice per worker generation whose attempts were held: the harness reads
	// it and stops that worker, which is what ends the held attempts.
	holds chan struct{}
	// worker names the worker generation polling right now. It is set by the restartable worker
	// that owns this gate, before the first generation starts.
	worker func() string

	mu sync.Mutex
	// asked is the identity of the generation the harness was last asked to restart. A wave
	// commands several devices concurrently, so several attempts can be held at once and one stop
	// ends all of them.
	asked string
}

// newActivityGate returns a gate over plan and ledger, with no worker identity yet: the restartable
// worker sets it before it starts polling.
func newActivityGate(plan *restartPlan, ledger *attemptLedger) *activityGate {
	return &activityGate{
		plan:   plan,
		ledger: ledger,
		holds:  make(chan struct{}, restartNoticeLimit),
	}
}

// InterceptActivity wraps the activity chain with the gate's attempt handling.
func (g *activityGate) InterceptActivity(
	_ context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &gatedActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, gate: g}
}

// gatedActivity applies one gate decision to one activity attempt. It embeds the interceptor base so
// the chain it wraps stays reachable, and its own ExecuteActivity is where the decision is applied.
type gatedActivity struct {
	interceptor.ActivityInboundInterceptorBase
	gate *activityGate
}

// ExecuteActivity records the attempt, consults the plan, and ends the attempt the way the plan
// says. The activity the worker registered is called through the embedded base — never instead of —
// so an attempt that ran is an attempt of the real activity, and an attempt that was held or failed
// on purpose executed nothing.
func (a *gatedActivity) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	return a.gate.execute(ctx, gateAttempt{
		ActivityID: info.ActivityID,
		Name:       info.ActivityType.Name,
		Attempt:    info.Attempt,
		WorkflowID: info.WorkflowExecution.ID,
		RunID:      info.WorkflowExecution.RunID,
	}, func(ctx context.Context) (any, error) {
		return a.Next.ExecuteActivity(ctx, in)
	})
}

// gateAttempt is what the gate needs to know about one attempt to decide it. It exists so the
// gate's decisions and the ledger can be driven — and asserted on — without an activity task.
type gateAttempt struct {
	ActivityID string
	Name       string
	Attempt    int32
	WorkflowID string
	RunID      string
}

// execute runs one activity attempt through the gate: it records the attempt, applies the plan's
// decision, and either runs the wrapped activity or ends the attempt the injected failure says.
func (g *activityGate) execute(
	ctx context.Context,
	attempt gateAttempt,
	next func(context.Context) (any, error),
) (any, error) {
	switch g.plan.decide(attempt.Name, attempt.Attempt) {
	case gateCrashAfter:
		out, err := next(ctx)
		if err != nil {
			// The activity failed on its own: the attempt's own failure is what the workflow
			// should see, not the injected one.
			g.record(attempt, gateCrashAfter, attemptRan, err)
			return out, err
		}
		g.record(attempt, gateCrashAfter, attemptCrashed, nil)
		// The retry has to land on a worker that is not the one whose result was lost, so the
		// harness is asked to restart exactly as it is for a held attempt.
		g.requestRestart()
		return out, restartInjected("attempt failed after its effect landed", nil)
	case gateHold:
		g.requestRestart()
		// Only the stop ends a held attempt: there is no release path the test controls, so an
		// attempt that ran is an attempt the plan did not hold.
		<-ctx.Done()
		g.record(attempt, gateHold, attemptCancelled, ctx.Err())
		return nil, restartInjected("held attempt cancelled by the worker stop", ctx.Err())
	default:
		out, err := next(ctx)
		g.record(attempt, gateRun, attemptRan, err)
		return out, err
	}
}

// record appends one attempt to the ledger, stamped with the generation that picked it up.
func (g *activityGate) record(
	attempt gateAttempt,
	decision gateDecision,
	outcome attemptOutcome,
	err error,
) {
	worker := "unknown"
	if g.worker != nil {
		worker = g.worker()
	}
	record := activityAttempt{
		ActivityID: attempt.ActivityID,
		Name:       attempt.Name,
		Attempt:    attempt.Attempt,
		WorkflowID: attempt.WorkflowID,
		RunID:      attempt.RunID,
		Worker:     worker,
		Decision:   decision,
		Outcome:    outcome,
	}
	if err != nil {
		record.Err = err.Error()
	}
	g.ledger.record(record)
}

// requestRestart asks the harness to stop the worker generation this attempt was picked up by, once
// per generation: the stop is what ends every attempt that generation held.
func (g *activityGate) requestRestart() {
	worker := g.recordedWorker()
	g.mu.Lock()
	alreadyAsked := g.asked == worker
	g.asked = worker
	g.mu.Unlock()
	if alreadyAsked {
		return
	}
	select {
	case g.holds <- struct{}{}:
	default:
		// The harness has a notice it has not acted on yet, and that restart stops this
		// generation too.
	}
}

// recordedWorker is the identity stamped on this attempt's ledger record.
func (g *activityGate) recordedWorker() string {
	if g.worker == nil {
		return "unknown"
	}
	return g.worker()
}

// restartInjected returns the retryable failure an injected attempt ends with. It is an ordinary
// application error, so Temporal's own retry policy decides what happens next — exactly as it does
// for an attempt a real worker died in the middle of.
func restartInjected(reason string, cause error) error {
	if cause != nil {
		return sdktemporal.NewApplicationErrorWithCause(reason, restartInjectedType, cause)
	}
	return sdktemporal.NewApplicationError(reason, restartInjectedType)
}
