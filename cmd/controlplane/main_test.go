package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/telemetry"
)

// fakeMarker is a hand-written StaleMarker double replaying scripted sweep results.
type fakeMarker struct {
	mu      sync.Mutex
	results []int64
	calls   chan struct{}
}

func newFakeMarker(results ...int64) *fakeMarker {
	return &fakeMarker{results: results, calls: make(chan struct{}, 64)}
}

func (f *fakeMarker) MarkStale(context.Context, time.Time) (int64, error) {
	f.mu.Lock()
	var result int64
	if len(f.results) > 0 {
		result = f.results[0]
		f.results = f.results[1:]
	}
	f.mu.Unlock()
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return result, nil
}

// waitCalls waits for n more sweep passes.
func waitCalls(t *testing.T, calls chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sweep pass %d of %d", i+1, n)
		}
	}
}

// scrape /metrics on the handler around reg.
func scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close scrape body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus exposition format", got)
	}
	return string(body)
}

func TestMetricsHandlerExposesOfflineTransitions(t *testing.T) {
	t.Parallel()

	reg, counter := newMetrics()
	counter.Add(1)

	body := scrape(t, metricsHandler(reg))
	if !strings.Contains(body, "fleetops_device_offline_transitions_total 1") {
		t.Errorf("scrape output = %q, want fleetops_device_offline_transitions_total with value 1", body)
	}
}

func TestOfflineTransitionsCountedOncePerTransition(t *testing.T) {
	t.Parallel()

	reg, counter := newMetrics()
	// Three transitions across three passes; the pass that finds nothing must not count.
	marker := newFakeMarker(3, 0, 2)
	sweeper := devices.NewSweeper(marker, time.Minute, 5*time.Millisecond,
		func(transitions int64) { counter.Add(float64(transitions)) },
		slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	waitCalls(t, marker.calls, 3)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("sweeper Run() error = %v, want nil", err)
	}

	if got := testutil.ToFloat64(counter); got != 5 {
		t.Errorf("fleetops_device_offline_transitions_total = %v, want 5", got)
	}
	body := scrape(t, metricsHandler(reg))
	if !strings.Contains(body, "fleetops_device_offline_transitions_total 5") {
		t.Errorf("scrape output = %q, want fleetops_device_offline_transitions_total with value 5", body)
	}
}

// fakeIngestSink is a hand-written HeartbeatIngest double recording what the fan-out stored,
// with a scripted refusal.
type fakeIngestSink struct {
	mu     sync.Mutex
	stored []string
	err    error
}

// Handle records the heartbeat's event id and returns the scripted error.
func (f *fakeIngestSink) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stored = append(f.stored, hb.GetEventId())
	return f.err
}

// events returns the recorded event ids.
func (f *fakeIngestSink) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stored...)
}

// fakeEventPublisher is a hand-written HeartbeatPublisher double recording what was queued for
// publication.
type fakeEventPublisher struct {
	mu        sync.Mutex
	published []string
}

// Handle records the heartbeat's event id.
func (f *fakeEventPublisher) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, hb.GetEventId())
}

// events returns the recorded event ids.
func (f *fakeEventPublisher) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.published...)
}

// TestHeartbeatSinkFansOutToIngestThenPublisher pins the sink the agent hub is wired with: the
// ordered fan-out, so an accepted heartbeat is stored before it is published and a refused one
// is never published at all.
func TestHeartbeatSinkFansOutToIngestThenPublisher(t *testing.T) {
	t.Parallel()

	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	hb := &agentv1.Heartbeat{EventId: "ev-1", DeviceId: rec.ID, Ts: timestamppb.Now()}

	ingest := &fakeIngestSink{}
	publisher := &fakeEventPublisher{}
	sink := heartbeatSink(ingest, publisher)
	if _, ok := sink.(*telemetry.Fanout); !ok {
		t.Fatalf("heartbeatSink() returned %T, want the ordered *telemetry.Fanout", sink)
	}
	if err := sink.Handle(context.Background(), hb, rec); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if got := ingest.events(); len(got) != 1 || got[0] != "ev-1" {
		t.Errorf("stored events = %v, want the accepted heartbeat", got)
	}
	if got := publisher.events(); len(got) != 1 || got[0] != "ev-1" {
		t.Errorf("published events = %v, want the accepted heartbeat", got)
	}

	refusal := errors.New("ingest heartbeat: measurement time required")
	ingest.err = refusal
	if err := sink.Handle(context.Background(), &agentv1.Heartbeat{EventId: "ev-2", DeviceId: rec.ID}, rec); !errors.Is(err, refusal) {
		t.Errorf("Handle() error = %v, want the ingest refusal", err)
	}
	if got := publisher.events(); len(got) != 1 {
		t.Errorf("published events = %v, want no publication for a refused heartbeat", got)
	}
}

// TestMetricsHandlerExposesTheEventPipeline pins that the pipeline's collectors are registered
// on the registry the control plane serves, before the endpoint starts serving: a scrape finds
// every family this change adds.
func TestMetricsHandlerExposesTheEventPipeline(t *testing.T) {
	t.Parallel()

	reg, _ := newMetrics()
	pipeline := telemetry.NewMetrics(reg)
	pipeline.QueueDepth.WithLabelValues(telemetry.HeartbeatQueue, string(telemetry.QueueKindWork)).Set(0)
	pipeline.Published.WithLabelValues(telemetry.HeartbeatEventType).Add(0)
	pipeline.Dropped.WithLabelValues(telemetry.HeartbeatEventType, "buffer_full").Add(0)
	pipeline.Consumed.WithLabelValues(telemetry.HeartbeatQueue, "processed").Add(0)
	pipeline.RegisterLag(telemetry.HeartbeatQueue, func() float64 { return 0 })

	body := scrape(t, metricsHandler(reg))
	want := []string{
		"fleetops_queue_depth",
		"fleetops_consumer_lag_events",
		"fleetops_events_published_total",
		"fleetops_events_dropped_total",
		"fleetops_events_consumed_total",
		"fleetops_device_offline_transitions_total",
	}
	for _, name := range want {
		if !strings.Contains(body, name) {
			t.Errorf("scrape output is missing %s:\n%s", name, body)
		}
	}
}
