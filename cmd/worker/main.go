// Command worker runs the FleetOps Temporal workers: it hosts the DeviceWorkflow entity and
// the workflows of later stages together with their activities on the shared task queue. It
// loads the shared configuration and serves the liveness/readiness probes alongside the
// worker.
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

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// shutdownTimeout bounds graceful shutdown of the probe server.
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

// run serves the liveness/readiness probes on the configured health address and runs the
// Temporal worker hosting DeviceWorkflow on the configured task queue until ctx is cancelled.
// Activities register with the workers that own their side effects: the dispatch-command
// activity joins the control-plane process beside the agent streams it dispatches on.
func run(ctx context.Context, cfg config.Config) error {
	tc, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect to temporal: %w", err)
	}
	defer tc.Close()

	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(temporal.DeviceWorkflow, workflow.RegisterOptions{
		Name: temporal.DeviceWorkflowName,
	})

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
