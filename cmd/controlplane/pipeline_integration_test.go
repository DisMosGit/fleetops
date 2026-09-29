//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/telemetry"
)

// nopSignaler is a hand-written DeviceSignaler double that swallows signals: this test drives the
// telemetry pipeline, and Temporal is the worker stage's dependency.
type nopSignaler struct{}

// SignalHeartbeat accepts a heartbeat signal without a workflow.
func (nopSignaler) SignalHeartbeat(context.Context, devices.Record, *agentv1.Heartbeat) error {
	return nil
}

// SignalCommandResult accepts a command result without a workflow.
func (nopSignaler) SignalCommandResult(context.Context, *agentv1.ReportRequest) error { return nil }

// SignalUpdateStatus accepts an update status without a workflow.
func (nopSignaler) SignalUpdateStatus(context.Context, *agentv1.UpdateStatusRequest) error {
	return nil
}

// nopFirmware is a hand-written FirmwareReader double with nothing stored: this test drives the
// telemetry pipeline, not firmware delivery.
type nopFirmware struct{}

// Open reports every firmware as not found.
func (nopFirmware) Open(context.Context, string) (firmware.Record, io.ReadCloser, error) {
	return firmware.Record{}, nil, fmt.Errorf("open firmware: %w", firmware.ErrNotFound)
}

// nopCommandHandler is a hand-written agent command handler double: the emulator ignores the
// commands this test never dispatches.
type nopCommandHandler struct{}

// Handle accepts one command.
func (nopCommandHandler) Handle(context.Context, *agentv1.Command) error { return nil }

// TestEventPipelineEndToEnd drives the wired pipeline with a real emulator fleet over a real gRPC
// stream against a real broker and store: accepted heartbeats are persisted and published,
// consumed exactly once into alerts with one processed-events record each, the backlog and lag
// metrics reflect the pipeline, and a broker outage only raises the drop counter while heartbeats
// keep being persisted.
func TestEventPipelineEndToEnd(t *testing.T) {
	t.Parallel()

	store := mongotest.Start(t)
	broker := rabbittest.Start(t)

	cfg := config.Defaults()
	cfg.MongoDB.Database = "fleetops"
	cfg.RabbitMQ.URL = broker.URL
	cfg.RabbitMQ.PublishBuffer = 256
	cfg.RabbitMQ.QueueDepthInterval = config.Duration{Duration: 200 * time.Millisecond}
	cfg.Telemetry.BatchSize = 50
	cfg.Telemetry.FlushInterval = config.Duration{Duration: 100 * time.Millisecond}
	// The emulator's healthy band is 0.85-1.0 and its degraded band 0.2-0.5, so the threshold
	// separates them and the seeded fleet produces degradations.
	cfg.Alerting.HealthThreshold = 0.6

	log := slog.New(slog.DiscardHandler)
	registry := prometheus.NewRegistry()
	events, err := newPipeline(cfg, store.DB, registry, log)
	if err != nil {
		t.Fatalf("newPipeline() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return events.ingest.Run(gctx) })
	g.Go(func() error { return events.publisher.Run(gctx) })
	g.Go(func() error { return events.consumer.Run(gctx) })
	g.Go(func() error { return events.sampler.Run(gctx) })
	t.Cleanup(func() {
		cancel()
		if err := g.Wait(); err != nil {
			t.Errorf("pipeline stopped with %v, want a clean shutdown", err)
		}
	})

	// A real gRPC server routes the fleet's heartbeats through the hub into the pipeline's sink.
	hub := agentserver.NewHub(events.sink, log)
	grpcServer := grpc.NewServer(agentserver.ServerOptions(log)...)
	agentv1.RegisterAgentServiceServer(grpcServer,
		agentserver.NewServer(hub, devices.NewStore(store.DB), nopSignaler{}, nopFirmware{}, log))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := agent.Dial(listener.Addr().String())
	if err != nil {
		t.Fatalf("dial control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close agent connection: %v", err)
		}
	})

	ids, err := agent.NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen() error = %v", err)
	}
	fleet, err := agent.NewFleet(2, agent.FleetOptions{
		IDs:    ids,
		Source: agent.NewSimulation(rand.NewSource(1)),
		Period: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewFleet() error = %v", err)
	}
	client, err := agent.NewClient(agentv1.NewAgentServiceClient(conn), agent.Options{
		Devices:        fleet.Identities(),
		Handler:        nopCommandHandler{},
		IDs:            ids,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	heartbeats := make(chan *agentv1.Heartbeat, 8)
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx, heartbeats) }()
	fleetDone := make(chan error, 1)
	go func() { fleetDone <- fleet.Run(ctx, heartbeats) }()

	// Accepted heartbeats reach durable storage.
	awaitCondition(t, "heartbeats to be persisted", func() bool {
		return documentCount(t, store, "telemetry") > 0
	})
	// They are published beside the durable write.
	awaitCondition(t, "events to be published", func() bool {
		return counterValue(t, registry, "fleetops_events_published_total") >= 4
	})

	// The consumer catches up: every published event is either processed or suppressed as a
	// duplicate, so its lag returns to zero and the work queue drains.
	awaitCondition(t, "the consumer to catch up", func() bool {
		return consumedEvents(t, registry) >= counterValue(t, registry, "fleetops_events_published_total")
	})
	awaitCondition(t, "lag to return to zero", func() bool {
		return gaugeValue(t, registry, "fleetops_consumer_lag_events", map[string]string{
			"consumer": telemetry.HeartbeatQueue,
		}) == 0
	})
	awaitCondition(t, "the work queue to drain", func() bool {
		return gaugeValue(t, registry, "fleetops_queue_depth", map[string]string{
			"queue": telemetry.HeartbeatQueue,
			"kind":  string(telemetry.QueueKindWork),
		}) == 0
	})

	// A degraded heartbeat opened an alert with the identity and window an operator needs.
	awaitCondition(t, "a degraded heartbeat to open an alert", func() bool {
		return documentCount(t, store, "device_alerts") > 0
	})
	var alert struct {
		DeviceID    string    `bson:"device_id"`
		Region      string    `bson:"region"`
		Model       string    `bson:"model"`
		Threshold   float64   `bson:"threshold"`
		MinHealth   float64   `bson:"min_health"`
		FirstSeenAt time.Time `bson:"first_seen_at"`
		LastSeenAt  time.Time `bson:"last_seen_at"`
	}
	if err := store.DB.Collection("device_alerts").FindOne(context.Background(), bson.D{}).Decode(&alert); err != nil {
		t.Fatalf("read alert: %v", err)
	}
	if alert.DeviceID == "" || alert.Region == "" || alert.Model == "" {
		t.Errorf("alert identity = %+v, want the device's registered identity", alert)
	}
	if alert.Threshold != cfg.Alerting.HealthThreshold {
		t.Errorf("alert threshold = %v, want the configured %v", alert.Threshold, cfg.Alerting.HealthThreshold)
	}
	if alert.MinHealth >= alert.Threshold || alert.MinHealth <= 0 {
		t.Errorf("alert min_health = %v, want the degraded health below the threshold", alert.MinHealth)
	}
	if alert.FirstSeenAt.IsZero() || alert.LastSeenAt.Before(alert.FirstSeenAt) {
		t.Errorf("alert window = %v..%v, want a first sighting no later than the last", alert.FirstSeenAt, alert.LastSeenAt)
	}

	// Deduplication left exactly one processed-events record per processed event, each completed.
	processed := consumedByOutcome(t, registry, "processed")
	if got := documentCount(t, store, "processed_events"); got != processed {
		t.Errorf("processed-events documents = %d, want one per processed event (%d)", got, processed)
	}
	if got := incompleteClaims(t, store); got != 0 {
		t.Errorf("unfinished claims = %d, want every processed event marked done", got)
	}
	// Nothing was dead-lettered: the fleet's events are all usable.
	if got := gaugeValue(t, registry, "fleetops_queue_depth", map[string]string{
		"queue": telemetry.HeartbeatQueue + ".dlq",
		"kind":  string(telemetry.QueueKindDeadLetter),
	}); got != 0 {
		t.Errorf("dead-letter depth = %v, want no dead letters in a healthy run", got)
	}

	// A broker outage degrades the fan-out only: heartbeats keep landing in storage while the
	// drop counter rises.
	persisted := documentCount(t, store, "telemetry")
	dropped := counterValue(t, registry, "fleetops_events_dropped_total")
	broker.Stop(t)
	awaitCondition(t, "heartbeats to keep landing during the outage", func() bool {
		return documentCount(t, store, "telemetry") > persisted
	})
	awaitCondition(t, "the dropped-publication counter to rise", func() bool {
		return counterValue(t, registry, "fleetops_events_dropped_total") > dropped
	})

	// The broker returns and publication resumes without a restart.
	broker.Start(t)
	published := counterValue(t, registry, "fleetops_events_published_total")
	awaitCondition(t, "publication to resume after the broker returns", func() bool {
		return counterValue(t, registry, "fleetops_events_published_total") > published
	})
	awaitCondition(t, "the consumer to catch up again", func() bool {
		return consumedEvents(t, registry) >= counterValue(t, registry, "fleetops_events_published_total")
	})

	cancel()
	if err := <-clientDone; err != nil {
		t.Errorf("agent client Run() error = %v, want nil", err)
	}
	if err := <-fleetDone; err != nil {
		t.Errorf("agent fleet Run() error = %v, want nil", err)
	}
}

// awaitCondition waits until check holds, failing the test with what when it never does.
func awaitCondition(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// documentCount returns how many documents a collection holds.
func documentCount(t *testing.T, store *mongotest.Harness, collection string) int64 {
	t.Helper()
	got, err := store.DB.Collection(collection).CountDocuments(context.Background(), bson.D{})
	if err != nil {
		t.Fatalf("count %s documents: %v", collection, err)
	}
	return got
}

// incompleteClaims returns how many ledger records exist without a completion time.
func incompleteClaims(t *testing.T, store *mongotest.Harness) int64 {
	t.Helper()
	got, err := store.DB.Collection("processed_events").CountDocuments(context.Background(), bson.D{
		{Key: "processed_at", Value: bson.D{{Key: "$exists", Value: false}}},
	})
	if err != nil {
		t.Fatalf("count unfinished claims: %v", err)
	}
	return got
}

// counterValue sums a counter family across every label set.
func counterValue(t *testing.T, reg *prometheus.Registry, family string) float64 {
	t.Helper()
	var total float64
	for _, candidate := range gatherMetrics(t, reg) {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
	}
	return total
}

// consumedEvents sums the outcomes that mean an event reached the end of the consumer's path:
// processed and duplicate.
func consumedEvents(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	var total float64
	for _, candidate := range gatherMetrics(t, reg) {
		if candidate.GetName() != "fleetops_events_consumed_total" {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != "outcome" {
					continue
				}
				if label.GetValue() == "processed" || label.GetValue() == "duplicate" {
					total += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// consumedByOutcome returns one consumption outcome's counter value.
func consumedByOutcome(t *testing.T, reg *prometheus.Registry, outcome string) int64 {
	t.Helper()
	var total int64
	for _, candidate := range gatherMetrics(t, reg) {
		if candidate.GetName() != "fleetops_events_consumed_total" {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" && label.GetValue() == outcome {
					total += int64(metric.GetCounter().GetValue())
				}
			}
		}
	}
	return total
}

// gaugeValue returns a gauge family's value for one label set.
func gaugeValue(t *testing.T, reg *prometheus.Registry, family string, labels map[string]string) float64 {
	t.Helper()
	for _, candidate := range gatherMetrics(t, reg) {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			matched := true
			for name, want := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == name && label.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no %s metric with labels %v", family, labels)
	return 0
}

// gatherMetrics returns every metric family a registry exposes.
func gatherMetrics(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	return families
}
