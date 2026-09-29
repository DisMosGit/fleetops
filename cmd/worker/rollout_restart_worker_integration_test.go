//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// The world every restart scenario drives: one region of one model running the firmware the smoke
// deploys, so the rollout's targeting, command identity, and version resolution are the production
// ones.
// restartableWorker hosts the worker's real rollout registrations one generation at a time, so a
// scenario can stop the worker driving a rollout and start a fresh one that picks the same work up.
// Every generation polls the same task queue with the same registrations, the same activity retry
// policies, and its own identity, which is what makes "which worker ran this attempt" answerable
// from the ledger.
type restartableWorker struct {
	t      *testing.T
	client temporalclient.Client
	cfg    config.Config
	deps   rolloutDeps
	gate   *activityGate

	mu         sync.Mutex
	generation int
	running    *workerGeneration
	injected   int
	closed     bool
	driver     chan struct{}
}

// workerGeneration is one started worker: the identity it polls under, the stop channel its Run
// watches, and the channel closed once Run has returned.
type workerGeneration struct {
	identity string
	stop     chan any
	done     chan struct{}
}

// newRestartableWorker wires the worker's own rollout dependencies — the real fleet database, the
// fleet's health evaluation, the device seam the scenario scripts, and the broker publisher the
// rollback announcements go through — behind the gate interceptor, and returns the harness without
// starting it.
func newRestartableWorker(
	t *testing.T,
	cfg config.Config,
	db *mongo.Database,
	commands temporal.DeviceCommander,
	devices temporal.DeviceStateReader,
	notifier temporal.RollbackNotifier,
	client temporalclient.Client,
	gate *activityGate,
) *restartableWorker {
	t.Helper()
	deps := newRolloutDeps(db, commands, devices, rolloutHealthSettings(cfg.Rollout), notifier)
	// The observation interval is shrunk the way the smoke shrinks it: the suite drives the real
	// update activity, and a device that never reports has to be provable in seconds.
	deps.updateOptions = []temporal.UpdateDeviceOption{
		temporal.WithUpdatePollInterval(smokeUpdatePollInterval),
	}
	h := &restartableWorker{t: t, client: client, cfg: cfg, deps: deps, gate: gate}
	gate.worker = h.identity
	return h
}

// Start runs the first worker generation. It is the scenario's own goroutine that calls it, so a
// worker that cannot start fails the test rather than being logged from a background restart.
func (h *restartableWorker) Start() {
	h.t.Helper()
	if err := h.start(); err != nil {
		h.t.Fatalf("start the suite worker: %v", err)
	}
}

// Halt stops the worker generation polling now and leaves the harness stopped, so a scenario can act
// while no worker is executing the rollout — which is what an outage is. It does not close the
// harness: Start brings the next generation up.
func (h *restartableWorker) Halt() error {
	return h.stopCurrent(false)
}

// Restart stops the worker generation polling now and starts the next one, and reports how many
// restarts have been injected. The stop waits for the old worker to finish — which is where a held
// attempt is cancelled — before the fresh generation starts polling, so no attempt can be picked up
// by a worker the harness believes is gone.
func (h *restartableWorker) Restart() (int, error) {
	if err := h.stopCurrent(false); err != nil {
		return h.restarts(), err
	}
	if err := h.start(); err != nil {
		return h.restarts(), err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.injected++
	return h.injected, nil
}

// Stop stops the worker generation polling now and waits for it to finish, and marks the harness
// finished, so a restart racing the end of a scenario cannot leave a worker polling behind it.
func (h *restartableWorker) Stop() error {
	return h.stopCurrent(true)
}

// identity returns the identity of the worker generation polling now, or an empty string when none
// is.
func (h *restartableWorker) identity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.running == nil {
		return ""
	}
	return h.running.identity
}

// restarts returns how many restarts have been injected.
func (h *restartableWorker) restarts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.injected
}

// driveRestarts restarts the worker once for every hold the gate reports, until ctx ends. Every
// restart is triggered by a hold the plan injected — never by a timer — so the count the harness
// reports is the number of boundaries the plan crossed.
func (h *restartableWorker) driveRestarts(ctx context.Context, after func()) {
	h.driver = make(chan struct{})
	go func() {
		defer close(h.driver)
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.gate.holds:
				if _, err := h.Restart(); err != nil {
					// A fatal assertion is not available outside the test's own goroutine, so
					// the failure is reported and the driver stops rather than restarting for
					// every hold that follows.
					h.t.Errorf("restart the worker after an injected hold: %v", err)
					return
				}
				if after != nil {
					after()
				}
			}
		}
	}()
}

// awaitDriver waits for the restart driver to stop, so a scenario's cleanup can be sure no restart
// and no assertion of its own runs after the test has ended.
func (h *restartableWorker) awaitDriver() {
	if h.driver == nil {
		return
	}
	select {
	case <-h.driver:
	case <-time.After(restartStopGrace):
		h.t.Log("the restart driver did not stop within the grace period")
	}
}

// start brings up one worker generation over the rollout's registrations: the namespace bootstrap
// the worker binary runs at startup, the gate interceptor, an identity of its own, and a stop
// timeout that bounds a stop while an attempt is held.
func (h *restartableWorker) start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errors.New("the worker harness is stopped")
	}
	if h.running != nil {
		return fmt.Errorf("worker generation %s is still running", h.running.identity)
	}
	// The namespace bootstrap is part of startup, so every generation runs it against the real
	// frontend: the attributes a workflow upserts are the ones this registers.
	if err := bootstrapNamespace(context.Background(), h.client.OperatorService(),
		h.cfg.Temporal.Namespace); err != nil {
		return fmt.Errorf("bootstrap namespace: %w", err)
	}

	h.generation++
	identity := fmt.Sprintf("suite-worker-%d", h.generation)
	w := worker.New(h.client, h.cfg.Temporal.TaskQueue, worker.Options{
		Identity: identity,
		// A stop has to end a held attempt rather than wait for it: the timeout bounds the stop,
		// and the cancellation it triggers is what Temporal retries on the next worker.
		WorkerStopTimeout: restartStopTimeout,
		Interceptors:      []interceptor.WorkerInterceptor{h.gate},
	})
	registerRollout(w, h.deps)

	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			h.t.Logf("worker generation %s stopped: %v", identity, err)
		}
	}()
	h.running = &workerGeneration{identity: identity, stop: stop, done: done}
	return nil
}

// stopCurrent stops the running generation, if any, and waits for its Run to return. closed marks
// the harness finished, which only a scenario's own Stop does: a restart has to be able to start
// the next generation.
func (h *restartableWorker) stopCurrent(closed bool) error {
	h.mu.Lock()
	if closed {
		h.closed = true
	}
	run := h.running
	h.running = nil
	h.mu.Unlock()
	if run == nil {
		return nil
	}
	close(run.stop)
	select {
	case <-run.done:
		return nil
	case <-time.After(restartStopGrace):
		return fmt.Errorf("worker generation %s did not stop within %v", run.identity, restartStopGrace)
	}
}
