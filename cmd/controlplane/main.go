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

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// shutdownTimeout bounds graceful shutdown of the probe server.
const shutdownTimeout = 5 * time.Second

// newMetrics serves the control plane's Prometheus source on a private registry: the device
// offline-transition counter the liveness sweep feeds. metricsHandler exposes the registry on
// the configured metrics address.
func newMetrics() (*prometheus.Registry, prometheus.Counter) {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "fleetops",
		Name:      "device_offline_transitions_total",
		Help:      "Devices marked offline after heartbeat staleness, counted once per transition.",
	})
	reg.MustRegister(counter)
	return reg, counter
}

// metricsHandler serves reg in the Prometheus exposition format on /metrics.
func metricsHandler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

func main() {
	configPath := flag.String(
		"config", "", "path to the YAML configuration file (defaults apply when omitted)",
	)
	httpAddr := flag.String("http-addr", ":8080", "HTTP gateway listen address (firmware upload API)")
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
// on the configured gRPC address until ctx is cancelled. Accepted streams persist device
// records and heartbeats through the batched ingest pipeline and signal the device workflow,
// the dispatch-command activity delivers the workflow's commands back onto the streams, and
// firmware binaries stream to agents over their download RPC. The HTTP gateway serves the
// firmware upload API on httpAddr; its SSE routes join the same lifecycle at stage 5.
func run(ctx context.Context, cfg config.Config, httpAddr string) error {
	log := slog.Default()

	client, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoDB.URI))
	if err != nil {
		return fmt.Errorf("connect to mongodb: %w", err)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Error("disconnect from mongodb", "err", err)
		}
	}()

	db := client.Database(cfg.MongoDB.Database)
	registry := devices.NewStore(db)
	firmwareStore := firmware.NewStore(db)
	ingest := telemetry.NewWriter(
		db.Collection("telemetry"),
		registry,
		cfg.Telemetry.BatchSize,
		cfg.Telemetry.FlushInterval.Duration,
		log,
	)
	metrics, offlineTransitions := newMetrics()
	sweeper := devices.NewSweeper(
		registry,
		cfg.Liveness.OfflineThreshold.Duration,
		cfg.Liveness.SweepInterval.Duration,
		func(transitions int64) { offlineTransitions.Add(float64(transitions)) },
		log,
	)

	checks, err := health.NewDependencyChecks(cfg.MongoDB.URI, cfg.RabbitMQ.URL, cfg.Temporal.Address)
	if err != nil {
		return fmt.Errorf("dependency checks: %w", err)
	}
	srv := &http.Server{
		Addr:    cfg.Observability.HealthAddr,
		Handler: health.NewHandler(checks...),
	}
	metricsSrv := &http.Server{
		Addr:    cfg.Observability.MetricsAddr,
		Handler: metricsHandler(metrics),
	}
	// The gateway is the operator API: firmware uploads land in the registry through it.
	// Its routes are validated before anything is stored — see internal/firmware.
	gatewaySrv := &http.Server{
		Addr:    httpAddr,
		Handler: firmware.NewHandler(firmwareStore, registry, log),
	}

	// The hub is the command seam the dispatch activity sends through; its sink is the
	// ingest pipeline and its registry records accepted registrations. Accepted heartbeats
	// and command results reach the device workflow through the signaler.
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect to temporal: %w", err)
	}
	defer tc.Close()

	// New device run chains decide under the configured snapshot cadence and offline
	// threshold; both are captured at chain start and carried with the entity state.
	signaler := temporal.NewSignaler(tc, cfg.Temporal.TaskQueue, temporal.DeviceSettings{
		SnapshotInterval: cfg.Snapshots.Interval.Duration,
		OfflineThreshold: cfg.Liveness.OfflineThreshold.Duration,
	})
	hub := agentserver.NewHub(ingest, log)
	grpcServer := grpc.NewServer(agentserver.ServerOptions(log)...)
	agentv1.RegisterAgentServiceServer(grpcServer, agentserver.NewServer(hub, registry, signaler, firmwareStore, log))

	// Activities live beside their side effects: dispatch-command needs the in-process hub,
	// so it joins this process on the same task queue the workflow worker uses.
	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{})
	w.RegisterActivityWithOptions(temporal.NewDispatchActivity(hub), activity.RegisterOptions{
		Name: temporal.DispatchActivityName,
	})

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
		slog.Info("metrics server listening", "addr", cfg.Observability.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve metrics on %s: %w", cfg.Observability.MetricsAddr, err)
		}
		return nil
	})
	g.Go(func() error {
		slog.Info("gateway server listening", "addr", httpAddr)
		if err := gatewaySrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve gateway on %s: %w", httpAddr, err)
		}
		return nil
	})
	g.Go(func() error {
		// The ingest pipeline stops with the process and flushes what it holds first.
		return ingest.Run(gctx)
	})
	g.Go(func() error {
		return sweeper.Run(gctx)
	})
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
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down probe server: %w", err)
		}
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down metrics server: %w", err)
		}
		if err := gatewaySrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down gateway server: %w", err)
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
