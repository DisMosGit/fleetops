//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
)

// TestPublisherAgainstBroker verifies publication end to end: an accepted heartbeat yields one
// persistent event on the heartbeat work queue with the expected envelope, properties, and
// routing key, and events keep flowing after the broker restarts.
func TestPublisherAgainstBroker(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	topology := NewTopology(3, time.Second, time.Minute)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	publisher := NewPublisher(h.URL, topology, 16, metrics, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- publisher.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("publisher Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("publisher Run() did not return after shutdown")
		}
	})

	rec := devices.Record{ID: "dev-published", Region: "eu-west", Model: "v3"}
	occurred := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	assertPublished := func(t *testing.T, eventID string) {
		t.Helper()
		before := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType))
		publisher.Handle(ctx, heartbeat(eventID, rec.ID, occurred), rec)
		awaitCondition(t, "the event on the heartbeat work queue", func() bool {
			return testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)) > before
		})
		delivery := nextDelivery(t, h, HeartbeatQueue)
		if got := delivery.MessageId; got != eventID {
			t.Errorf("message id = %q, want the event id %q", got, eventID)
		}
		if got := delivery.Type; got != HeartbeatEventType {
			t.Errorf("message type = %q, want %q", got, HeartbeatEventType)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Errorf("delivery mode = %d, want persistent", delivery.DeliveryMode)
		}
		if delivery.ContentType != "application/json" {
			t.Errorf("content type = %q, want application/json", delivery.ContentType)
		}
		if got := delivery.RoutingKey; got != HeartbeatRoutingKey("eu-west", "v3") {
			t.Errorf("routing key = %q, want %q", got, HeartbeatRoutingKey("eu-west", "v3"))
		}
		if got := delivery.Headers[originalRoutingKeyHeader]; got != HeartbeatRoutingKey("eu-west", "v3") {
			t.Errorf("original routing key header = %v, want the published key", got)
		}
		event := mustDecode(t, delivery.Body)
		if event.EventID != eventID || event.DeviceID != rec.ID || event.Region != "eu-west" || event.Model != "v3" {
			t.Errorf("envelope identity = %+v, want the published heartbeat", event)
		}
		if !event.OccurredAt.Equal(occurred) {
			t.Errorf("envelope occurred_at = %v, want the measurement time %v", event.OccurredAt, occurred)
		}
		if event.PublishedAt.Before(event.OccurredAt) || event.Sequence == 0 {
			t.Errorf("envelope timing = %+v, want a publish time after the measurement and a sequence", event)
		}
	}

	published := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType))
	assertPublished(t, "ev-before-restart")
	// A broker restart must not stop publication: the publisher reconnects, re-declares, and
	// keeps publishing.
	h.Restart(t)
	publisher.Handle(ctx, heartbeat("ev-after-restart", rec.ID, occurred), rec)
	awaitCondition(t, "publication to resume after the broker restart", func() bool {
		return testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)) > published+1
	})
	delivery := nextDelivery(t, h, HeartbeatQueue)
	if got := delivery.MessageId; got != "ev-after-restart" {
		t.Errorf("message id after the restart = %q, want ev-after-restart", got)
	}
}

// TestUnroutablePublishIsReturnedAndCountable verifies the mandatory-publish contract against a
// real broker: an event no binding matches comes back instead of vanishing, so the publisher can
// count it as a failure and name the key in a log line.
func TestUnroutablePublishIsReturnedAndCountable(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	topology := NewTopology(3, time.Second, time.Minute)
	session, err := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler)).dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Logf("close session: %v", err)
		}
	})
	publisher, err := newSessionPublisher(session)
	if err != nil {
		t.Fatalf("newSessionPublisher() error = %v", err)
	}

	// No binding matches this family: the heartbeat work queue listens on heartbeat.# and the
	// rollout work queue on rollout.task.#.
	key := "unknown.family.event"
	msg, err := testEvent().Message(1, key)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	outcome, err := publisher.Publish(context.Background(), EventsExchange, key, msg)
	if err != nil {
		t.Fatalf("Publish() error = %v, want a verdict", err)
	}
	if outcome != publishUnroutable {
		t.Errorf("publish outcome = %s, want unroutable", outcome)
	}

	// A routable key on the same session still publishes.
	routable := HeartbeatRoutingKey("eu-west", "v3")
	msg, err = testEvent().Message(1, routable)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	outcome, err = publisher.Publish(context.Background(), EventsExchange, routable, msg)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if outcome != publishConfirmed {
		t.Errorf("publish outcome for %s = %s, want confirmed", routable, outcome)
	}
}
