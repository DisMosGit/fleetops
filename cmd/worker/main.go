// Command worker runs the FleetOps Temporal workers: it hosts the DeviceWorkflow entity, the
// rollout workflow, and the activities whose side effects live in this process — the device-state
// snapshot, the rollout's fleet-database reads and writes, its device-workflow command signals,
// its per-device downgrade and inventory reconciliation, its rollback announcements, and its
// wave-health evaluation. It loads the shared configuration, bootstraps the namespace's search
// attributes, hosts the broker publisher a rollback's announcements are published through, and
// serves the liveness/readiness probes alongside the worker. Nothing it does is replica-local:
// running several copies against the shared task queue is how the entity fleet scales out.
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
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/telemetry"
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

// searchAttributeRegistry is the slice of the Temporal operator service the startup bootstrap
// needs — operatorservice.OperatorServiceClient satisfies it, and tests hand-write a fake.
type searchAttributeRegistry interface {
	// ListSearchAttributes reports the namespace's registered search attributes.
	ListSearchAttributes(
		ctx context.Context, in *operatorservice.ListSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.ListSearchAttributesResponse, error)
	// AddSearchAttributes registers custom search attributes on a namespace.
	AddSearchAttributes(
		ctx context.Context, in *operatorservice.AddSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.AddSearchAttributesResponse, error)
}

// bootstrapNamespace prepares the namespace this worker's workflows run in: the custom search
// attributes both workflow families upsert are registered before the first task is polled, so no
// run can mirror a value the Temporal UI cannot filter on. It is idempotent and safe when several
// replicas do it at once, which is what makes startup a plain call rather than a coordination.
func bootstrapNamespace(ctx context.Context, reg searchAttributeRegistry, namespace string) error {
	if err := temporal.EnsureSearchAttributes(ctx, reg, namespace); err != nil {
		return fmt.Errorf("ensure search attributes: %w", err)
	}
	return nil
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
// the firmware registry its commands name and its downgrades resolve a previous version in, the
// fleet database its membership and records live in and its reconciliation corrects, the device
// command seam its per-device updates and restores are delivered through, the device state that
// seam is observed through, the wave-health evaluation its gates decide on, and the publisher its
// rollback announcements are announced through.
type rolloutDeps struct {
	firmware  temporal.FirmwareSource
	versions  temporal.FirmwareVersions
	targets   temporal.TargetResolver
	rollouts  temporal.RolloutRecorder
	waves     temporal.WaveRecorder
	inventory temporal.FirmwareInventory
	commands  temporal.DeviceCommander
	devices   temporal.DeviceStateReader
	health    temporal.HealthEvaluator
	notifier  temporal.RollbackNotifier
	// updateOptions adjust the per-device update activity. Production leaves them empty — the
	// activity's defaults are what a rollout drives under — and the integration smoke shrinks
	// the observation interval so a device that never reports is provable in seconds.
	updateOptions []temporal.UpdateDeviceOption
}

// newRolloutDeps wires the rollout's side effects to the fleet database, the configured health
// policy, the device signaler of this process, and the broker publisher a rollback's announcements
// are published through.
func newRolloutDeps(
	db *mongo.Database,
	commands temporal.DeviceCommander,
	deviceStates temporal.DeviceStateReader,
	health wavehealth.Settings,
	notifier temporal.RollbackNotifier,
) rolloutDeps {
	store := rollout.NewStore(db)
	// The same fleet database answers both halves of an evaluation: which devices a wave
	// targets and what their heartbeats reported.
	samples := wavehealth.NewStore(db)
	registry := firmware.NewStore(db)
	deviceRecords := devices.NewStore(db)
	return rolloutDeps{
		firmware: registry,
		// The registry resolves a device's previous firmware by version, and the device
		// records are the inventory a rollback reconciles against the devices.
		versions:  registry,
		targets:   store,
		rollouts:  store,
		waves:     store,
		inventory: deviceRecords,
		commands:  commands,
		devices:   deviceStates,
		health:    wavehealth.New(samples, samples, health),
		notifier:  notifier,
	}
}

// registerRollout registers the rollout workflow and its worker-hosted activities under their
// explicit names: the fleet-database reads and writes, the per-device update, the rollback's
// per-device downgrade and inventory reconciliation, the rollback announcement, and the
// wave-health evaluation. The one activity whose side effect is a live agent stream
// (dispatch-command) stays in the control plane: update-device and downgrade-device signal a
// device workflow and read its state query, whose command-id dedup makes a redelivery a no-op, so
// they belong beside the workflow that owns it.
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
		{temporal.UpdateDeviceActivityName, temporal.NewUpdateDeviceActivity(
			deps.commands, deps.devices, deps.updateOptions...,
		)},
		{temporal.DowngradeDeviceActivityName, temporal.NewDowngradeDeviceActivity(
			deps.commands, deps.devices, deps.versions,
		)},
		{temporal.ReconcileInventoryActivityName, temporal.NewReconcileInventoryActivity(
			deps.devices, deps.inventory,
		)},
		{temporal.AnnounceRollbackActivityName, temporal.NewAnnounceRollbackActivity(deps.notifier)},
		{temporal.EvaluateWaveHealthActivityName, temporal.NewEvaluateWaveActivity(deps.health)},
	}
	for _, a := range activities {
		w.RegisterActivityWithOptions(a.fn, activity.RegisterOptions{Name: a.name})
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

// startNotifier returns the function that runs the announcement publisher until ctx ends, together
// with a channel closed once that loop is running. The worker starts the publisher first and waits
// for ready before it polls, so an announcement a rollback publishes never races the publisher's
// startup. A broker that is unreachable at startup is a reconnect loop inside the notifier — the
// announcements fail, the worker runs — so ready says nothing about the broker being up.
func startNotifier(notifier *telemetry.Notifier, ctx context.Context) (run func() error, ready <-chan struct{}) {
	started := make(chan struct{})
	return func() error {
		close(started)
		return notifier.Run(ctx)
	}, started
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

	if err := bootstrapNamespace(ctx, tc.OperatorService(), cfg.Temporal.Namespace); err != nil {
		return err
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

	// New device run chains decide under the configured snapshot cadence, offline threshold,
	// and dispatch queue, and the rollout activities decide under the configured rollout
	// policy. This process runs the device workflow but not its dispatch activity, so the
	// queue it seeds is the one the control plane polls.
	signaler := temporal.NewSignaler(tc, cfg.Temporal.TaskQueue, temporal.DeviceSettings{
		SnapshotInterval:  cfg.Snapshots.Interval.Duration,
		OfflineThreshold:  cfg.Liveness.OfflineThreshold.Duration,
		DispatchTaskQueue: cfg.Temporal.DispatchTaskQueue,
	})

	// The announcing step runs in this process, so the publisher it publishes through is
	// hosted here too: the same broker layout the control plane declares, dialed by the worker
	// so a rollback's announcement does not depend on a second process being up.
	notifier := telemetry.NewNotifier(
		cfg.RabbitMQ.URL,
		telemetry.NewTopology(
			cfg.RabbitMQ.MaxAttempts,
			cfg.RabbitMQ.RetryBase.Duration,
			cfg.RabbitMQ.RetryMax.Duration,
		),
		slog.Default(),
	)

	// The worker keeps no replica-local state, so any replica may execute any workflow or
	// activity task and replicas can come and go without coordinating.
	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{WorkerStopTimeout: shutdownTimeout})
	registerDevice(w, devices.NewSnapshotStore(db))
	registerRollout(w, newRolloutDeps(
		db, signaler, temporal.NewDeviceStates(tc), rolloutHealthSettings(cfg.Rollout), notifier,
	))

	checks, err := health.NewDependencyChecks(cfg.MongoDB.URI, cfg.RabbitMQ.URL, cfg.Temporal.Address)
	if err != nil {
		return fmt.Errorf("dependency checks: %w", err)
	}
	srv := &http.Server{
		Addr:    cfg.Observability.HealthAddr,
		Handler: health.NewHandler(checks...),
	}

	g, gctx := errgroup.WithContext(ctx)
	// The publisher is started first and the worker waits for its loop to be running before it
	// polls, so a rollback's first announcement finds a publisher rather than racing its startup.
	notifierRun, notifierReady := startNotifier(notifier, gctx)
	g.Go(notifierRun)
	<-notifierReady
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
