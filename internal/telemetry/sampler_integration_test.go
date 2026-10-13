//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestSamplerReportsQueueDepths verifies the depth metric against a real broker: a dead-lettered
// message raises the depth reported with kind dead_letter, and a drained work queue reports zero.
func TestSamplerReportsQueueDepths(t *testing.T) {
	// Deliberately not parallel: this test consumes the shared work queue and asserts on the
	// depths its own message leaves behind, and a sequential test never overlaps a parallel one.
	h := rabbittest.For(t)
	topology := NewTopology(3, time.Second, time.Minute)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	sampler := NewQueueSampler(h.URL, topology, 100*time.Millisecond, metrics, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("sampler Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(testexec.Get(t).Wait(30 * time.Second)):
			t.Error("sampler Run() did not return after shutdown")
		}
	})

	dlq := topology.DeadLetterQueue(HeartbeatQueue)
	awaitCondition(t, "the queue-depth metric to be sampled", func() bool {
		return testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue, string(QueueKindWork))) == 0
	})

	// Move one message onto the dead-letter queue the way a rejecting consumer does.
	ch := h.Channel(t)
	publisher := newBrokerPublisher(t, h, topology)
	// The shared broker keeps what another test left behind; this test asserts on the depths its
	// own message produces, so both queues it measures are emptied first.
	h.Purge(t, ch, HeartbeatQueue)
	h.Purge(t, ch, dlq)
	publisher.publish(t, heartbeatEvent("ev-dead-letter", "dev-dead-letter", 0.3, time.Now()), 1)
	deliveries, err := ch.Consume(HeartbeatQueue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if err := delivery.Nack(false, false); err != nil {
			t.Fatalf("nack delivery: %v", err)
		}
	case <-time.After(testexec.Get(t).Wait(10 * time.Second)):
		t.Fatal("timed out waiting for the published delivery")
	}

	awaitCondition(t, "the dead-letter depth to rise", func() bool {
		return testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(dlq, string(QueueKindDeadLetter))) == 1
	})
	awaitCondition(t, "the work queue to drain", func() bool {
		return testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue, string(QueueKindWork))) == 0
	})
}
