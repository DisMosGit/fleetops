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
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/health"
	"github.com/DisMosGit/fleetops/internal/rolloutapi"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
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

// heartbeatSink returns the sink the agent hub routes accepted heartbeats through: the ordered
// fan-out of the durable ingest write and the broker publication. Ingest decides acceptance — a
// heartbeat it refuses is never published — and broker trouble never fails a heartbeat, because
// storage stays the source of truth. newPipeline wires the real pair through it.
func heartbeatSink(ingest telemetry.HeartbeatIngest, publisher telemetry.HeartbeatPublisher) agentserver.HeartbeatSink {
	return telemetry.NewFanout(ingest, publisher)
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

// pipeline is the control plane's telemetry event pipeline: the batched ingest write, the broker
// fan-out published beside it, the alerting consumer that turns degraded heartbeats into alerts,
// and the queue-depth sampler. run starts every part of it and the end-to-end test drives the
// same wiring.
type pipeline struct {
	// sink is the ordered fan-out the agent hub routes accepted heartbeats through.
	sink      agentserver.HeartbeatSink
	ingest    *telemetry.Writer
	publisher *telemetry.Publisher
	consumer  *telemetry.Consumer
	sampler   *telemetry.QueueSampler
	metrics   *telemetry.Metrics
}

// newPipeline returns the telemetry pipeline configured by cfg over db, with its collectors
// registered on registry so a scrape sees them as soon as the metrics endpoint serves.
func newPipeline(cfg config.Config, db *mongo.Database, registry *prometheus.Registry, log *slog.Logger) (*pipeline, error) {
	store := devices.NewStore(db)
	ingest := telemetry.NewWriter(
		db.Collection("telemetry"),
		store,
		cfg.Telemetry.BatchSize,
		cfg.Telemetry.FlushInterval.Duration,
		log,
	)
	metrics := telemetry.NewMetrics(registry)

	// The broker layout, the fan-out publisher, the alerting consumer, and the queue-depth
	// sampler are all built from the same configuration, so what a running control plane
	// declares is what its configuration describes.
	topology := telemetry.NewTopology(
		cfg.RabbitMQ.MaxAttempts,
		cfg.RabbitMQ.RetryBase.Duration,
		cfg.RabbitMQ.RetryMax.Duration,
	)
	publisher := telemetry.NewPublisher(
		cfg.RabbitMQ.URL,
		topology,
		cfg.RabbitMQ.PublishBuffer,
		metrics,
		log,
	)
	alertingQueue, err := topology.WorkQueue(telemetry.HeartbeatQueue)
	if err != nil {
		return nil, fmt.Errorf("resolve the alerting work queue: %w", err)
	}
	// The consumer's side effect is one alert document per device whose health fell below the
	// configured threshold; deduplication makes redeliveries no-ops.
	consumer := telemetry.NewConsumer(
		cfg.RabbitMQ.URL,
		alertingQueue,
		topology,
		telemetry.NewLedger(db.Collection("processed_events")),
		telemetry.NewAlerting(db.Collection("device_alerts"), cfg.Alerting.HealthThreshold),
		telemetry.ConsumerOptions{
			Prefetch: cfg.RabbitMQ.Prefetch,
			Metrics:  metrics,
			Source:   publisher,
		},
		log,
	)
	sampler := telemetry.NewQueueSampler(
		cfg.RabbitMQ.URL,
		topology,
		cfg.RabbitMQ.QueueDepthInterval.Duration,
		metrics,
		log,
	)
	return &pipeline{
		sink:      heartbeatSink(ingest, publisher),
		ingest:    ingest,
		publisher: publisher,
		consumer:  consumer,
		sampler:   sampler,
		metrics:   metrics,
	}, nil
}

// rolloutSettings maps the configured canary sequence and gate timing onto the policy a rollout
// drives under: the settings the rollout start path hands to the workflow, so the sequence an
// operator configures is the sequence the rollout drives, with the approval flags they set.
//
// The mapping lives here because this process is the one that starts rollouts. It follows the
// pattern the rest of the entrypoints use: each maps the configuration sections it consumes onto
// the library types it needs, so internal/config stays free of the orchestration SDK.
func rolloutSettings(cfg config.Rollout) (temporal.RolloutSettings, error) {
	waves := make([]temporal.RolloutWave, 0, len(cfg.Waves))
	for _, wave := range cfg.Waves {
		waves = append(waves, temporal.RolloutWave{
			Percent:         wave.Percent,
			RequireApproval: wave.RequireApproval,
		})
	}
	settings, err := temporal.NewRolloutSettings(
		cfg.HealthWindow.Duration,
		cfg.DecisionTimeout.Duration,
		cfg.ResultTimeout.Duration,
		waves,
	)
	if err != nil {
		return temporal.RolloutSettings{}, fmt.Errorf("map rollout settings: %w", err)
	}
	return settings, nil
}

// rolloutClient is the slice of the Temporal client the rollout surface needs: starting a
// workflow, reading one's state query, and signalling one. *temporalclient.Client satisfies it,
// and a test hand-writes a double.
type rolloutClient interface {
	// ExecuteWorkflow starts one workflow execution.
	ExecuteWorkflow(
		ctx context.Context, options temporalclient.StartWorkflowOptions,
		workflow any, args ...any,
	) (temporalclient.WorkflowRun, error)
	// QueryWorkflow reads one workflow execution's query answer.
	QueryWorkflow(
		ctx context.Context, workflowID, queryType, runID string, queryArgs ...any,
	) (converter.EncodedValue, error)
	// SignalWorkflow delivers one signal to a workflow execution.
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
}

// newRolloutAPI builds the operator's rollout surface: the starter that begins a rollout, the
// reader that answers with the workflow's own state, and the sender that delivers the workflow's
// own signals, all over the control plane's Temporal client.
func newRolloutAPI(cfg config.Config, tc rolloutClient, log *slog.Logger) (http.Handler, error) {
	settings, err := rolloutSettings(cfg.Rollout)
	if err != nil {
		return nil, err
	}
	log.Info("rollout control configured",
		"health_window", settings.HealthWindow,
		"decision_timeout", settings.DecisionTimeout,
		"result_timeout", settings.ResultTimeout,
		"waves", len(settings.Waves),
	)
	return rolloutapi.NewHandler(
		temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, settings),
		temporal.NewRolloutStates(tc),
		temporal.NewRolloutSignals(tc),
		log,
	), nil
}

// gatewayHandler composes the HTTP gateway's APIs on one listener: each package serves its own
// routes under its own path prefix, so neither shadows the other and the mux answers a method or
// path mismatch for both.
func gatewayHandler(firmwares http.Handler, rollouts http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/firmwares", firmwares)
	mux.Handle("/api/firmwares/", firmwares)
	mux.Handle("/api/rollouts", rollouts)
	mux.Handle("/api/rollouts/", rollouts)
	return mux
}

// newWaveHealthService builds the wave health query service the control plane serves: the
// aggregation over the fleet database's rollouts, waves, and telemetry collection, evaluated at
// the configured gating policy. The served window and thresholds are logged here, before the gRPC
// listener starts, so what the process answers with is visible in its startup output.
func newWaveHealthService(cfg config.Config, db *mongo.Database, log *slog.Logger) *wavehealth.Service {
	settings := wavehealth.Settings{
		HealthWindow:          cfg.Rollout.HealthWindow.Duration,
		SampleHealthThreshold: cfg.Rollout.SampleHealthThreshold,
		MinSuccessRatio:       cfg.Rollout.MinSuccessRatio,
		MinSamples:            cfg.Rollout.MinSamples,
	}
	log.Info("wave health gating configured",
		"health_window", settings.HealthWindow,
		"sample_health_threshold", settings.SampleHealthThreshold,
		"min_success_ratio", settings.MinSuccessRatio,
		"min_samples", settings.MinSamples,
	)
	// One store serves both consumed interfaces: it resolves the wave and counts its samples.
	store := wavehealth.NewStore(db)
	return wavehealth.NewService(wavehealth.New(store, store, settings), log)
}

// registry is the slice of the Temporal worker registry the wire-up needs — worker.Worker
// satisfies it, and tests hand-write a fake. It is deliberately activity-only: the control plane
// hosts no workflow, and a seam that could not register one says so.
type registry interface {
	// RegisterActivityWithOptions registers an activity function under an explicit name.
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// registerDispatch registers the command-dispatch activity under its explicit name. It is this
// process's only registration, and that is the point: Temporal delivers a task to whichever poller
// of its queue is available rather than to one that registered its type, so the control plane
// polls a queue of its own and hosts exactly what that queue carries. Every task type belonging
// to the work queue stays with the workers, which register all of them.
func registerDispatch(w registry, dispatcher temporal.CommandDispatcher) {
	w.RegisterActivityWithOptions(temporal.NewDispatchActivity(dispatcher), activity.RegisterOptions{
		Name: temporal.DispatchActivityName,
	})
}

// workerFactory builds the poller a binary runs. worker.New satisfies it, and a test substitutes
// one that records the queue it was handed, so which queue this process polls is asserted without
// standing up a Temporal server.
type workerFactory func(temporalclient.Client, string, worker.Options) worker.Worker

// startWorker builds the control plane's Temporal worker from cfg and registers the
// command-dispatch activity on it — and nothing else, because nothing else runs in this process.
// It polls the configured control-plane queue and not the work queue: Temporal delivers a task to
// any poller of its queue rather than to one that registered its type, so a control plane on the
// work queue would be handed the workers' device, snapshot, and rollout tasks and would fail every
// one of them as an unknown type.
func startWorker(
	build workerFactory, tc temporalclient.Client, cfg config.Config,
	dispatcher temporal.CommandDispatcher,
) worker.Worker {
	w := build(tc, cfg.Temporal.DispatchTaskQueue,
		worker.Options{WorkerStopTimeout: shutdownTimeout})
	registerDispatch(w, dispatcher)
	return w
}

// run serves the liveness/readiness probes on the configured health address and AgentService
// on the configured gRPC address until ctx is cancelled. Accepted streams persist device
// records and heartbeats through the batched ingest pipeline and signal the device workflow,
// the dispatch-command activity delivers the workflow's commands back onto the streams, and
// firmware binaries stream to agents over their download RPC. RolloutService answers the
// operator's wave health query on the same listener. The HTTP gateway serves the
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
	metrics, offlineTransitions := newMetrics()
	events, err := newPipeline(cfg, db, metrics, log)
	if err != nil {
		return err
	}
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

	// The hub is the command seam the dispatch activity sends through; its registry records
	// accepted registrations, and its sink is the ordered fan-out of the durable ingest write
	// and the broker publication. Accepted heartbeats and command results reach the device
	// workflow through the signaler.
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect to temporal: %w", err)
	}
	defer tc.Close()

	// New device run chains decide under the configured snapshot cadence, offline threshold,
	// and dispatch queue; all three are captured at chain start and carried with the entity
	// state. The signaler starts workflows on the work queue — the dispatch queue is only
	// where this process later receives their commands.
	signaler := temporal.NewSignaler(tc, cfg.Temporal.TaskQueue, temporal.DeviceSettings{
		SnapshotInterval:  cfg.Snapshots.Interval.Duration,
		OfflineThreshold:  cfg.Liveness.OfflineThreshold.Duration,
		DispatchTaskQueue: cfg.Temporal.DispatchTaskQueue,
	})
	hub := agentserver.NewHub(events.sink, log)
	grpcServer := grpc.NewServer(agentserver.ServerOptions(log)...)
	agentv1.RegisterAgentServiceServer(grpcServer, agentserver.NewServer(hub, registry, signaler, firmwareStore, log))
	rolloutv1.RegisterRolloutServiceServer(grpcServer, newWaveHealthService(cfg, db, log))

	// The gateway is the operator API: firmware uploads land in the registry through it, and a
	// rollout is started, read, and commanded through it. Both APIs compose on this one listener,
	// each under its own path prefix, and the firmware routes are validated before anything is
	// stored — see internal/firmware.
	rolloutHandler, err := newRolloutAPI(cfg, tc, log)
	if err != nil {
		return err
	}
	gatewaySrv := &http.Server{
		Addr: httpAddr,
		Handler: gatewayHandler(
			firmware.NewHandler(firmwareStore, registry, log),
			rolloutHandler,
		),
	}

	// Activities live beside their side effects: dispatch-command needs the in-process hub, so
	// it joins this process on the control plane's own task queue. Polling a queue of its own
	// is what keeps this process from being handed the work queue's device, snapshot, and
	// rollout tasks, which it does not host; the workflows still start on the work queue,
	// because that is where the processes running them live.
	w := startWorker(worker.New, tc, cfg, hub)

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
		return events.ingest.Run(gctx)
	})
	g.Go(func() error {
		// The publisher reconnects on its own: a broker outage degrades the fan-out and is
		// counted, it never stops the control plane.
		return events.publisher.Run(gctx)
	})
	g.Go(func() error {
		return events.consumer.Run(gctx)
	})
	g.Go(func() error {
		return events.sampler.Run(gctx)
	})
	g.Go(func() error {
		return sweeper.Run(gctx)
	})
	g.Go(func() error {
		slog.Info("temporal worker started", "task_queue", cfg.Temporal.DispatchTaskQueue)
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
