//go:build integration

package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/temporaltest"
)

// The world every restart scenario drives: one region of one model running the firmware the smoke
// deploys, so the rollout's targeting, command identity, and version resolution are the production
// ones.
const (
	// restartRegion is the region the scenarios' fleets live in; each scenario has its own
	// database, so one name serves all of them.
	restartRegion = "eu-west"
	// restartModel is the device model the firmware targets.
	restartModel = "oak-s3"
	// restartFirmwareID is the firmware the smoke helpers seed and the scenarios deploy.
	restartFirmwareID = "fw-smoke"
	// restartQueryGrace bounds reading the rollout's state after a restart. The read is retried
	// rather than reported as a failure of the rollout: right after a restart the workflow's sticky
	// task queue still names the worker that stopped, so the query waits for the fresh generation to
	// pick the workflow up.
	restartQueryGrace = time.Minute
	// restartFleetReportGrace bounds waiting for the fleet's samples to arrive after a heartbeat
	// stream starts: the wave gates measure reports, so a world whose fleet is not reporting is a
	// broken world rather than an unhealthy wave, and it has to say so.
	restartFleetReportGrace = 15 * time.Second
	// restartQueryAttempt bounds one state read. A query the workflow cannot answer is bounded here
	// rather than left to the transport, so the sampler's own grace is what the wait is measured in.
	restartQueryAttempt = 5 * time.Second
)

// restartWorld is the stack one restart scenario drives: the real fleet database, the real broker,
// the containerized Temporal server, and a worker harness over the worker's own rollout
// registrations and configuration mapping. It is the smoke's world with a worker that can be
// stopped and started, and with the rollout it drives named up front so the state can be sampled
// from the first injected restart on.
type restartWorld struct {
	t          *testing.T
	ctx        context.Context
	cfg        config.Config
	opts       worldOptions
	db         *mongo.Database
	broker     *rabbittest.Harness
	server     *temporaltest.Harness
	client     temporalclient.Client
	rollouts   *mongo.Collection
	waves      *mongo.Collection
	dispatcher *commandRecorder
	gate       *activityGate
	ledger     *attemptLedger
	worker     *restartableWorker
	settings   temporal.RolloutSettings
	fleet      []string
	generation int
	healthy    func()
	sampler    *resumptionSampler
}

// worldOptions are what a scenario chooses before its stack boots.
type worldOptions struct {
	// rolloutID identifies the rollout the scenario drives; the workflow id derives from it.
	rolloutID string
	// waves is the canary sequence the rollout drives.
	waves []temporal.RolloutWave
	// fleet is how many devices the rollout's region holds.
	fleet int
	// healthWindow is the width of every wave's health window; zero takes the smoke's window.
	healthWindow time.Duration
	// plan is the restart injection policy. A nil plan injects nothing, which is what the
	// bookkeeping scenario drives.
	plan *restartPlan
	// sampleResumption makes the world read the rollout's state after every injected restart and
	// refuse a sample that regressed, which is where "it resumes from the correct point" is
	// measured.
	sampleResumption bool
}

// newRestartWorld boots the stack a scenario drives: MongoDB, RabbitMQ, and a containerized Temporal
// server, the worker's own configuration mapping and registrations behind the gate, a seeded fleet
// reporting healthy heartbeats, and one running worker generation. Everything it starts is released
// through t.Cleanup.
func newRestartWorld(t *testing.T, opts worldOptions) *restartWorld {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	db := mongotest.Start(t).DB
	broker := rabbittest.Start(t)
	server := temporaltest.Start(t)

	cfg := config.Defaults()
	cfg.Temporal.Address = server.Address
	cfg.Temporal.Namespace = server.Namespace
	cfg.RabbitMQ.URL = broker.URL
	window := opts.healthWindow
	if window == 0 {
		window = smokeHealthWindow
	}
	cfg.Rollout.HealthWindow = config.Duration{Duration: window}
	cfg.Rollout.DecisionTimeout = config.Duration{Duration: 30 * time.Second}
	cfg.Rollout.ResultTimeout = config.Duration{Duration: smokeResultTimeout}
	cfg.Rollout.MinSamples = smokeMinSamples
	cfg.Rollout.SampleHealthThreshold = 0.6
	cfg.Rollout.MinSuccessRatio = 0.95

	settings, err := temporal.NewRolloutSettings(
		cfg.Rollout.HealthWindow.Duration,
		cfg.Rollout.DecisionTimeout.Duration,
		cfg.Rollout.ResultTimeout.Duration,
		opts.waves,
	)
	if err != nil {
		t.Fatalf("build rollout settings: %v", err)
	}

	// The rollback's announcements are published for real: the worker hosts the publisher the
	// announcements go through, and the notification queue they land on is the broker's.
	notifier := telemetry.NewNotifier(
		cfg.RabbitMQ.URL,
		telemetry.NewTopology(
			cfg.RabbitMQ.MaxAttempts, cfg.RabbitMQ.RetryBase.Duration, cfg.RabbitMQ.RetryMax.Duration,
		),
		smokeLogger(),
	)
	notifierRun, notifierReady := startNotifier(notifier, ctx)
	go func() { _ = notifierRun() }()
	<-notifierReady

	ledger := &attemptLedger{}
	gate := newActivityGate(opts.plan, ledger)
	dispatcher := &commandRecorder{}
	suiteWorker := newRestartableWorker(
		t, cfg, db, dispatcher, dispatcher, notifier, server.Client, gate,
	)

	world := &restartWorld{
		t: t, ctx: ctx, cfg: cfg, opts: opts, db: db, broker: broker, server: server,
		client: server.Client, rollouts: db.Collection("rollouts"), waves: db.Collection("waves"),
		dispatcher: dispatcher, gate: gate, ledger: ledger, worker: suiteWorker, settings: settings,
	}
	// The firmware the rollout deploys and the version a rollback restores devices to are seeded
	// through the smoke's own helpers, so the scenarios deploy what the smoke deploys.
	seedFirmware(t, db)
	seedPreviousFirmware(t, db)

	world.fleet = seedFleet(t, db, restartRegion, opts.fleet)
	world.healthy = world.startFleetHealth(0.95)
	// The fleet has to be reporting before the scenario starts: a wave gated on a fleet that never
	// reported would be judged undecided and then unhealthy, which is a broken world rather than a
	// rollout finding.
	world.awaitFleetSamples(0)

	if opts.sampleResumption {
		world.sampler = newResumptionSampler(t, server.Client, opts.rolloutID)
	}
	world.worker.driveRestarts(ctx, world.afterRestart)
	world.worker.Start()
	// The driver is cancelled and waited for before the worker is stopped, and the worker is
	// stopped before the containers go: a restart in flight when the scenario ends would otherwise
	// report its failure after the test had finished.
	t.Cleanup(func() {
		if err := world.worker.Stop(); err != nil {
			t.Logf("stop the suite worker: %v", err)
		}
	})
	t.Cleanup(func() {
		cancel()
		world.worker.awaitDriver()
	})
	return world
}

// startFleetHealth streams samples for the world's fleet at the given health through the documented
// ingestion contract — the emulator's role, minus the stream — until the returned stop function is
// called, and registers the stream's teardown with the test. The returned function stops the stream
// once: a scenario that flips the fleet's health calls it while the test is running, and the
// teardown calls it again.
//
// It is the smoke's stream with one difference that matters to these scenarios: every sample's id
// carries a generation of its own, so the samples a fleet writes after its health was flipped are
// new documents rather than upserts of the stream that ran before. The smoke's ids restart at one
// per stream, which would let a flipped fleet keep reporting its old health until its counter passed
// the samples the healthy stream had already written.
func (w *restartWorld) startFleetHealth(health float64) func() {
	w.t.Helper()
	w.generation++
	generation := w.generation
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		ticker := time.NewTicker(smokeHeartbeatGap)
		defer ticker.Stop()
		written := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				written++
				now := time.Now().UTC()
				for _, device := range w.fleet {
					eventID := fmt.Sprintf("restart-%s-%d-%d", device, generation, written)
					_, err := w.db.Collection("telemetry").UpdateOne(ctx,
						bson.D{{Key: "_id", Value: eventID}},
						bson.D{{Key: "$setOnInsert", Value: bson.D{
							{Key: "_id", Value: eventID},
							{Key: "ts", Value: now},
							{Key: "meta", Value: bson.D{
								{Key: "device_id", Value: device},
								{Key: "region", Value: restartRegion},
								{Key: "model", Value: restartModel},
							}},
							{Key: "cpu", Value: 0.2},
							{Key: "mem", Value: 0.3},
							{Key: "health", Value: health},
						}}},
						options.UpdateOne().SetUpsert(true),
					)
					if err != nil {
						w.t.Errorf("write heartbeat for %s: %v", device, err)
						return
					}
				}
			}
		}
	}()
	var once sync.Once
	stopped := func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
	w.t.Cleanup(stopped)
	return stopped
}

// fleetSamples counts the samples the fleet has reported, which is the evidence a wave's gate
// measures: a wave judged without evidence is the one failure whose reason is in this count.
func (w *restartWorld) fleetSamples() int64 {
	w.t.Helper()
	count, err := w.db.Collection("telemetry").CountDocuments(context.Background(),
		bson.D{{Key: "meta.device_id", Value: bson.D{{Key: "$in", Value: w.fleet}}}})
	if err != nil {
		return -1
	}
	return count
}

// breakFleet flips the fleet's reported health from healthy to failing, which is how a scenario
// drives a later wave's gate to roll the rollout back, and waits for the failing samples to land so
// the wave that follows is really measured on them. The devices still take their update commands:
// what a gate measures is the fleet's reported health, not the command seam's script.
func (w *restartWorld) breakFleet() {
	w.t.Helper()
	w.healthy()
	before := w.fleetSamples()
	w.healthy = w.startFleetHealth(0.1)
	w.awaitFleetSamples(before)
}

// awaitFleetSamples waits until the fleet has reported more than after samples.
func (w *restartWorld) awaitFleetSamples(after int64) {
	w.t.Helper()
	deadline := time.Now().Add(restartFleetReportGrace)
	for time.Now().Before(deadline) {
		if w.fleetSamples() > after {
			return
		}
		time.Sleep(smokePollInterval)
	}
	w.t.Fatalf("the fleet reported no sample beyond %d within %v", after, restartFleetReportGrace)
}

// startRollout starts the scenario's rollout with the world's wave sequence, through the same
// starter and the same settings mapping the worker's own start path uses.
func (w *restartWorld) startRollout() {
	w.t.Helper()
	if err := temporal.NewRolloutStarter(w.client, w.cfg.Temporal.TaskQueue, w.settings).
		Start(w.ctx, temporal.RolloutRequest{
			RolloutID:  w.opts.rolloutID,
			FirmwareID: restartFirmwareID,
			Region:     restartRegion,
			Model:      restartModel,
		}); err != nil {
		w.t.Fatalf("start rollout %s: %v", w.opts.rolloutID, err)
	}
}

// afterRestart is what runs once a fresh worker generation is polling: the rollout's state is
// sampled, so a restart that resumed somewhere else than it stopped fails the scenario.
func (w *restartWorld) afterRestart() {
	w.sampleResumption()
}

// sampleResumption reads the rollout's state and refuses it when it regressed against the previous
// reading. A world without a sampler samples nothing.
func (w *restartWorld) sampleResumption() {
	if w.sampler == nil {
		return
	}
	if err := w.sampler.sample(w.ctx); err != nil {
		w.t.Errorf("%v", err)
	}
}
