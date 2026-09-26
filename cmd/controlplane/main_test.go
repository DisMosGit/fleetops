package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/DisMosGit/fleetops/internal/devices"
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
