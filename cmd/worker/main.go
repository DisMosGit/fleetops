// Command worker runs the FleetOps Temporal workers: it hosts the DeviceWorkflow entity and
// the workflows of later stages together with the activities whose side effects live in this
// process — the device-state snapshot on the fleet database. It loads the shared
// configuration, bootstraps the namespace's search attributes, and serves the
// liveness/readiness probes alongside the worker. Nothing it does is replica-local: running
// several copies against the shared task queue is how the entity fleet scales out.
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
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/temporal"
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

// entityRegistry is the slice of the Temporal worker registry the entity wire-up needs —
// worker.Worker satisfies it, and tests hand-write a fake.
type entityRegistry interface {
	// RegisterWorkflowWithOptions registers a workflow function under an explicit name.
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	// RegisterActivityWithOptions registers an activity function under an explicit name.
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// registerEntity registers the device entity workflow and its worker-hosted activity under
// their explicit names. Activities run beside the side effect they own: the snapshot writes
// this process's database, while dispatch-command stays in the control-plane process beside
// the agent streams it dispatches on.
func registerEntity(w entityRegistry, snapshots temporal.StateSnapshotter) {
	w.RegisterWorkflowWithOptions(temporal.DeviceWorkflow, workflow.RegisterOptions{
		Name: temporal.DeviceWorkflowName,
	})
	w.RegisterActivityWithOptions(temporal.NewSnapshotActivity(snapshots), activity.RegisterOptions{
		Name: temporal.SnapshotActivityName,
	})
}

// run serves the liveness/readiness probes on the configured health address and runs the
// Temporal worker hosting DeviceWorkflow on the configured task queue until ctx is cancelled.
// The namespace's custom search attributes are registered before the first task is polled, so
// no workflow can upsert an attribute the UI cannot filter on.
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

	// The worker keeps no replica-local state, so any replica may execute any workflow or
	// activity task and replicas can come and go without coordinating.
	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{WorkerStopTimeout: shutdownTimeout})
	registerEntity(w, devices.NewSnapshotStore(mongoClient.Database(cfg.MongoDB.Database)))

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
