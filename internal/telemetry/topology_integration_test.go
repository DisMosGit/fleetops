//go:build integration

package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DisMosGit/fleetops/internal/rabbittest"
)

// TestTopologyAgainstBroker verifies the layout against a real broker: declaration is
// repeatable, events reach only the queues bound to them, a narrow binding selects one region,
// the retry path returns a message to its work queue after the backoff, and a rejected delivery
// reaches the dead-letter queue.
func TestTopologyAgainstBroker(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	ctx := context.Background()
	// One-second first retry with a two-second cap keeps the ladder observable in a test.
	topology := NewTopology(4, time.Second, 2*time.Second)
	sup := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler))

	sess, err := sup.dial(ctx)
	if err != nil {
		t.Fatalf("dial and declare: %v", err)
	}
	t.Cleanup(func() {
		if err := sess.Close(); err != nil {
			t.Logf("close session: %v", err)
		}
	})
	dlq := topology.DeadLetterQueue(HeartbeatQueue)

	t.Run("declaration is repeatable and keeps waiting messages", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, HeartbeatQueue)
		publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "first-declaration", nil)
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)

		// A second process declaring the same layout must not purge or fail.
		resecond, err := sup.dial(ctx)
		if err != nil {
			t.Fatalf("second declaration: %v", err)
		}
		t.Cleanup(func() {
			if err := resecond.Close(); err != nil {
				t.Logf("close second session: %v", err)
			}
		})
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
	})

	t.Run("each family reaches only its own work queue", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, HeartbeatQueue)
		h.Purge(t, ch, RolloutQueue)

		publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "heartbeat-event", nil)
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
		if got := h.Depth(t, ch, RolloutQueue, true); got != 0 {
			t.Errorf("rollout queue depth = %d, want 0 for a heartbeat event", got)
		}

		publish(t, sess, EventsExchange, RolloutRoutingKey("start"), "rollout-event", nil)
		h.AwaitDepth(t, ch, RolloutQueue, true, 1)
		if got := h.Depth(t, ch, HeartbeatQueue, true); got != 1 {
			t.Errorf("heartbeat queue depth = %d, want the single heartbeat event", got)
		}
	})

	t.Run("a narrow binding selects one region", func(t *testing.T) {
		const narrow = "telemetrytest.heartbeat.eu-west"
		ch := h.Channel(t)
		if _, err := ch.QueueDeclare(narrow, false, true, false, false, nil); err != nil {
			t.Fatalf("declare narrow queue: %v", err)
		}
		if err := ch.QueueBind(narrow, "heartbeat.eu-west.#", EventsExchange, false, nil); err != nil {
			t.Fatalf("bind narrow queue: %v", err)
		}
		h.Purge(t, ch, HeartbeatQueue)
		h.AwaitDepth(t, ch, narrow, false, 0)

		publish(t, sess, EventsExchange, HeartbeatRoutingKey("us-east", "v3"), "east-event", nil)
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
		if got := h.Depth(t, ch, narrow, false); got != 0 {
			t.Errorf("narrow queue depth = %d, want 0 for another region", got)
		}

		publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "west-event", nil)
		h.AwaitDepth(t, ch, narrow, false, 1)
		if got := h.Depth(t, ch, HeartbeatQueue, true); got != 2 {
			t.Errorf("family queue depth = %d, want both events", got)
		}
	})

	t.Run("the retry path returns a message after its backoff", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, HeartbeatQueue)
		retry := topology.RetryQueue(HeartbeatQueue, 1)
		h.Purge(t, ch, retry)

		publish(t, sess, RetryExchange, retry, "retry-me", amqp.Table{attemptHeader: int32(2)})
		h.AwaitDepth(t, ch, retry, true, 1)
		if got := h.Depth(t, ch, HeartbeatQueue, true); got != 0 {
			t.Fatalf("work queue depth = %d, want the retry to wait for its delay", got)
		}

		// The first retry queue waits the base delay (one second) before returning the event.
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
		delivery := nextDelivery(t, h, HeartbeatQueue)
		if got := string(delivery.Body); got != "retry-me" {
			t.Errorf("returned payload = %q, want the published payload", got)
		}
		if got := attemptOf(delivery); got != 2 {
			t.Errorf("returned attempt = %d, want the incremented attempt 2", got)
		}
	})

	t.Run("a late retry queue is capped by retry max", func(t *testing.T) {
		// retry.3 would be four seconds uncapped and two seconds with the cap, so a message that
		// is back on the work queue three seconds later proves the cap applied.
		retry := topology.RetryQueue(HeartbeatQueue, 3)
		ch := h.Channel(t)
		h.Purge(t, ch, HeartbeatQueue)
		h.Purge(t, ch, retry)

		publish(t, sess, RetryExchange, retry, "capped-retry", amqp.Table{attemptHeader: int32(4)})
		h.AwaitDepth(t, ch, retry, true, 1)
		time.Sleep(time.Second)
		if got := h.Depth(t, ch, HeartbeatQueue, true); got != 0 {
			t.Fatalf("work queue depth = %d after one second, want the retry still waiting", got)
		}
		h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
	})

	t.Run("a rejected delivery is dead-lettered", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, HeartbeatQueue)
		h.Purge(t, ch, dlq)

		publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "poison", nil)
		if err := ch.Qos(1, 0, false); err != nil {
			t.Fatalf("set prefetch: %v", err)
		}
		deliveries, err := ch.Consume(HeartbeatQueue, "", false, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		select {
		case d := <-deliveries:
			if err := d.Nack(false, false); err != nil {
				t.Fatalf("nack delivery: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for the published delivery")
		}
		h.AwaitDepth(t, ch, dlq, true, 1)
	})
}

// TestTopologyDeclarationConflictFailsLoudly verifies a broker element that exists with
// different properties stops the declaring process with an error naming it, rather than being
// silently adapted to.
func TestTopologyDeclarationConflictFailsLoudly(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	ctx := context.Background()

	// A pre-existing direct exchange under the events name conflicts with the declared topic
	// exchange.
	ch := h.Channel(t)
	if err := ch.ExchangeDeclare(EventsExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		t.Fatalf("pre-declare conflicting exchange: %v", err)
	}

	sup := NewSupervisor(h.URL, NewTopology(3, time.Second, 4*time.Second), slog.New(slog.DiscardHandler))
	sess, err := sup.dial(ctx)
	if sess != nil {
		t.Fatal("dial() returned a session for a conflicted topology")
	}
	if !errors.Is(err, errTopologyConflict) {
		t.Fatalf("dial() error = %v, want a topology conflict", err)
	}
	if !strings.Contains(err.Error(), EventsExchange) {
		t.Errorf("dial() error = %q, want it to name %q", err, EventsExchange)
	}
}

// TestSupervisorRecoversFromBrokerRestart verifies a supervised connection outlives a broker
// restart: the session is re-established, the topology is re-declared, and the component keeps
// running without a process restart.
func TestSupervisorRecoversFromBrokerRestart(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	topology := NewTopology(3, time.Second, 4*time.Second)
	sup := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler))

	sessions := make(chan Session, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx, "publisher", func(ctx context.Context, sess Session) error {
			sessions <- sess
			select {
			case <-sess.Lost():
				return errSessionLost
			case <-ctx.Done():
				return nil
			}
		})
	}()

	_ = awaitSession(t, sessions, "the initial session")

	// Deleting a declared queue proves the reconnect re-declares the topology rather than
	// assuming the broker still carries it.
	dlq := topology.DeadLetterQueue(HeartbeatQueue)
	ch := h.Channel(t)
	if _, err := ch.QueueDelete(dlq, false, false, false); err != nil {
		t.Fatalf("delete %s: %v", dlq, err)
	}

	h.Restart(t)
	second := awaitSession(t, sessions, "the session re-established after the restart")
	if _, err := second.QueueDeclarePassive(dlq, true, false, false, false, nil); err != nil {
		t.Errorf("queue %s missing after reconnect, want the topology re-declared: %v", dlq, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil on shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after ctx was cancelled")
	}
}

// awaitSession waits for the next supervised session.
func awaitSession(t *testing.T, sessions <-chan Session, what string) Session {
	t.Helper()
	select {
	case sess := <-sessions:
		return sess
	case <-time.After(90 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// publish sends one persistent message.
func publish(t *testing.T, sess Session, exchange, key, body string, headers amqp.Table) {
	t.Helper()
	err := sess.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(body),
	})
	if err != nil {
		t.Fatalf("publish to %s under %s: %v", exchange, key, err)
	}
}

// nextDelivery consumes one message from a queue on a connection of its own, so the consumer is
// gone — and the queue's depth meaningful again — by the time this returns.
func nextDelivery(t *testing.T, h *rabbittest.Harness, queue string) amqp.Delivery {
	t.Helper()
	conn, err := amqp.Dial(h.URL)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("close delivery connection: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open delivery channel: %v", err)
	}
	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume %s: %v", queue, err)
	}
	select {
	case d := <-deliveries:
		return d
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for a delivery from %s", queue)
		return amqp.Delivery{}
	}
}
