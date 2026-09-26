// Command controlplane is the FleetOps control plane: the gRPC agent server and the HTTP/SSE
// gateway. It loads the shared configuration and serves the liveness/readiness probes today;
// the gRPC server joins with the streaming stage and the HTTP/SSE gateway at stage 5.
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

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/health"
)

// shutdownTimeout bounds graceful shutdown of the probe server.
const shutdownTimeout = 5 * time.Second

func main() {
	configPath := flag.String(
		"config", "", "path to the YAML configuration file (defaults apply when omitted)",
	)
	httpAddr := flag.String("http-addr", ":8080", "HTTP/SSE gateway listen address (stage 5)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, *httpAddr); err != nil {
		slog.Error("controlplane stopped", "err", err)
		os.Exit(1)
	}
}

// run serves the liveness/readiness probes on the configured health address until ctx is
// cancelled. The gRPC agent server and the HTTP/SSE gateway join the same lifecycle when their
// stages land.
func run(ctx context.Context, cfg config.Config, httpAddr string) error {
	// TODO(stage 1): construct MongoDB, Temporal, and RabbitMQ clients and serve the gRPC
	// listener on cfg.GRPC.ListenAddr.
	// TODO(stage 5): serve the HTTP/SSE gateway on httpAddr.

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
