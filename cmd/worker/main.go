// Command worker runs the FleetOps Temporal workers: it hosts the DeviceWorkflow entity, the
// rollout workflow, and the activities whose side effects live in this process — the device-state
// snapshot, the rollout's fleet-database reads and writes, its device-workflow command signals,
// and its wave-health evaluation. It loads the shared configuration, bootstraps the namespace's
// search attributes, and serves the liveness/readiness probes alongside the worker. Nothing it
// does is replica-local: running several copies against the shared task queue is how the entity
// fleet scales out.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// shutdownTimeout bounds graceful shutdown: how long the worker drains in-flight work and the
// probe server finishes answering before the process exits.
const shutdownTimeout = 5 * time.Second

func main() {
	configPath := flag.String(
		"config", "", "path to the YAML configuration file (defaults apply when omitted)",
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

// registry is the slice of the Temporal worker registry the wire-up needs — worker.Worker
// satisfies it, and tests hand-write a fake.
type registry interface {
	// RegisterWorkflowWithOptions registers a workflow function under an explicit name.
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	// RegisterActivityWithOptions registers an activity function under an explicit name.
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// registerDevice registers the device entity workflow and its worker-hosted activity under their
// explicit names. Activities run beside the side effect they own: the snapshot writes this
// process's database, while dispatch-command stays in the control-plane process beside the agent
// streams it dispatches on.
func registerDevice(w registry, snapshots temporal.StateSnapshotter) {
	w.RegisterWorkflowWithOptions(temporal.DeviceWorkflow, workflow.RegisterOptions{
		Name: temporal.DeviceWorkflowName,
	})
	w.RegisterActivityWithOptions(temporal.NewSnapshotActivity(snapshots), activity.RegisterOptions{
		Name: temporal.SnapshotActivityName,
	})
}

// rolloutDeps are the side effects the rollout workflow's activities own, wired to this process:
// the firmware registry its commands name, the fleet database its membership and records live in,
// the device command seam its waves are dispatched through, and the wave-health evaluation its
// gates decide on.
type rolloutDeps struct {
	firmware temporal.FirmwareSource
	targets  temporal.TargetResolver
	rollouts temporal.RolloutRecorder
	waves    temporal.WaveRecorder
	commands temporal.DeviceCommander
	health   temporal.HealthEvaluator
}

// newRolloutDeps wires the rollout's side effects to the fleet database, the configured health
// policy, and the device signaler of this process.
func newRolloutDeps(
	db *mongo.Database,
	commands temporal.DeviceCommander,
	health wavehealth.Settings,
) rolloutDeps {
	store := rollout.NewStore(db)
	// The same fleet database answers both halves of an evaluation: which devices a wave
	// targets and what their heartbeats reported.
	samples := wavehealth.NewStore(db)
	return rolloutDeps{
		firmware: firmware.NewStore(db),
		targets:  store,
		rollouts: store,
		waves:    store,
		commands: commands,
		health:   wavehealth.New(samples, samples, health),
	}
}

// registerRollout registers the rollout workflow and its worker-hosted activities under their
// explicit names. The one activity whose side effect is a live agent stream (dispatch-command)
// stays in the control plane: dispatch-wave-update only signals device workflows, whose command-id
// dedup makes a redelivery a no-op, so it belongs beside the workflow that owns it.
func registerRollout(w registry, deps rolloutDeps) {
	w.RegisterWorkflowWithOptions(temporal.RolloutWorkflow, workflow.RegisterOptions{
		Name: temporal.RolloutWorkflowName,
	})
	activities := []struct {
		name string
		fn   any
	}{
		{temporal.LoadFirmwareActivityName, temporal.NewLoadFirmwareActivity(deps.firmware)},
		{temporal.ResolveWaveTargetsActivityName, temporal.NewResolveWaveActivity(deps.targets)},
		{temporal.RecordRolloutStateActivityName, temporal.NewRecordRolloutActivity(deps.rollouts)},
		{temporal.RecordWaveStateActivityName, temporal.NewRecordWaveActivity(deps.waves)},
		{temporal.DispatchWaveUpdateActivityName, temporal.NewDispatchWaveActivity(deps.commands)},
		{temporal.EvaluateWaveHealthActivityName, temporal.NewEvaluateWaveActivity(deps.health)},
	}
	for _, a := range activities {
		w.RegisterActivityWithOptions(a.fn, activity.RegisterOptions{Name: a.name})
	}
}

// rolloutSettings maps the configured canary sequence and gate timing onto the policy a rollout
// drives under: the settings a start path hands to the workflow, so the sequence an operator
// configures is the sequence the rollout drives, with the approval flags they set.
func rolloutSettings(cfg config.Rollout) temporal.RolloutSettings {
	waves := make([]temporal.RolloutWave, 0, len(cfg.Waves))
	for _, wave := range cfg.Waves {
		waves = append(waves, temporal.RolloutWave{
			Percent:         wave.Percent,
			RequireApproval: wave.RequireApproval,
		})
	}
	return temporal.RolloutSettings{
		HealthWindow:    cfg.HealthWindow.Duration,
		DecisionTimeout: cfg.DecisionTimeout.Duration,
		Waves:           waves,
	}
}

// rolloutHealthSettings maps the configured thresholds onto the policy the rollout's health
// evaluations apply: what counts as a successful sample, the ratio a wave must reach to be
// promoted, and how much evidence a verdict needs.
func rolloutHealthSettings(cfg config.Rollout) wavehealth.Settings {
	return wavehealth.Settings{
		HealthWindow:          cfg.HealthWindow.Duration,
		SampleHealthThreshold: cfg.SampleHealthThreshold,
		MinSuccessRatio:       cfg.MinSuccessRatio,
		MinSamples:            cfg.MinSamples,
	}
}

// run serves the liveness/readiness probes on the configured health address and runs the
// Temporal worker hosting DeviceWorkflow and the rollout workflow on the configured task queue
// until ctx is cancelled. The namespace's custom search attributes are registered before the first
// task is polled, so no workflow can upsert an attribute the UI cannot filter on.
func run(ctx context.Context, cfg config.Config) error {
	tc, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect to temporal: %w", err)
	}
	defer tc.Close()

	if err := temporal.EnsureSearchAttributes(ctx, tc.OperatorService(), cfg.Temporal.Namespace); err != nil {
		return fmt.Errorf("ensure search attributes: %w", err)
	}

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoDB.URI))
	if err != nil {
		return fmt.Errorf("connect to mongodb: %w", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			slog.Error("disconnect from mongodb", "err", err)
		}
	}()
	db := mongoClient.Database(cfg.MongoDB.Database)

	// New device run chains decide under the configured snapshot cadence and offline
	// threshold, and the rollout activities decide under the configured rollout policy.
	signaler := temporal.NewSignaler(tc, cfg.Temporal.TaskQueue, temporal.DeviceSettings{
		SnapshotInterval: cfg.Snapshots.Interval.Duration,
		OfflineThreshold: cfg.Liveness.OfflineThreshold.Duration,
	})

	// The worker keeps no replica-local state, so any replica may execute any workflow or
	// activity task and replicas can come and go without coordinating.
	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{WorkerStopTimeout: shutdownTimeout})
	registerDevice(w, devices.NewSnapshotStore(db))
	registerRollout(w, newRolloutDeps(db, signaler, rolloutHealthSettings(cfg.Rollout)))

	checks, err := health.NewDependencyChecks(cfg.MongoDB.URI, cfg.RabbitMQ.URL, cfg.Temporal.Address)
	if err != nil {
		return fmt.Errorf("dependency checks: %w", err)
	}
	srv := &http.Server{
		Addr:    cfg.Observability.HealthAddr,
		Handler: health.NewHandler(checks...),
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		slog.Info("temporal worker started", "task_queue", cfg.Temporal.TaskQueue)
		stop := make(chan any)
		go func() {
			<-gctx.Done()
			close(stop)
		}()
		if err := w.Run(stop); err != nil {
			return fmt.Errorf("run temporal worker: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		slog.Info("probe server listening", "addr", cfg.Observability.HealthAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve probes on %s: %w", cfg.Observability.HealthAddr, err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down probe server: %w", err)
		}
		return nil
	})
	return g.Wait()
}
