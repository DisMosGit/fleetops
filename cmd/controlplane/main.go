// Command controlplane is the FleetOps control plane: the gRPC agent server and the HTTP/SSE
// gateway. It loads the shared configuration, serves the liveness/readiness probes, and serves
// AgentService for the agent fleet; the HTTP/SSE gateway joins at stage 5.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agentserver"
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

// run serves the liveness/readiness probes on the configured health address and AgentService
// on the configured gRPC address until ctx is cancelled. The HTTP/SSE gateway joins the same
// lifecycle at stage 5.
func run(ctx context.Context, cfg config.Config, httpAddr string) error {
	// TODO(stage 1): construct MongoDB, Temporal, and RabbitMQ clients.
	// TODO(stage 5): serve the HTTP/SSE gateway on httpAddr.

	checks, err := health.NewDependencyChecks(cfg.MongoDB.URI, cfg.RabbitMQ.URL, cfg.Temporal.Address)
	if err != nil {
		return fmt.Errorf("dependency checks: %w", err)
	}
	srv := &http.Server{
		Addr:    cfg.Observability.HealthAddr,
		Handler: health.NewHandler(checks...),
	}

	// The hub is the command seam later stages send through; its sink joins the telemetry
	// pipeline at stage 3.
	hub := agentserver.NewHub(loggingSink{}, slog.Default())
	grpcServer := grpc.NewServer(agentserver.ServerOptions(slog.Default())...)
	agentv1.RegisterAgentServiceServer(grpcServer, agentserver.NewServer(hub, slog.Default()))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		slog.Info("probe server listening", "addr", cfg.Observability.HealthAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve probes on %s: %w", cfg.Observability.HealthAddr, err)
		}
		return nil
	})
	g.Go(func() error {
		lis, err := net.Listen("tcp", cfg.GRPC.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.GRPC.ListenAddr, err)
		}
		slog.Info("agent server listening", "addr", cfg.GRPC.ListenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("serve agents on %s: %w", cfg.GRPC.ListenAddr, err)
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
		// GracefulStop has no deadline of its own: give it the remaining shutdown budget
		// and fall back to a hard stop so shutdown always completes.
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
			<-stopped
		}
		return nil
	})
	return g.Wait()
}

// loggingSink is the minimal HeartbeatSink until the telemetry pipeline lands at stage 3: it
// acknowledges each routed heartbeat at the log boundary and stores nothing.
type loggingSink struct{}

// Handle logs one routed heartbeat.
func (loggingSink) Handle(_ context.Context, hb *agentv1.Heartbeat) error {
	slog.Info("heartbeat",
		"device_id", hb.GetDeviceId(),
		"event_id", hb.GetEventId(),
		"status", hb.GetStatus(),
		"health", hb.GetHealth(),
	)
	return nil
}
