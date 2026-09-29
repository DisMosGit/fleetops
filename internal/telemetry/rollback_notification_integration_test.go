//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DisMosGit/fleetops/internal/rabbittest"
)

// TestRollbackNotificationsAgainstBroker verifies the notification family against a real broker:
// the declared queue exists and is bound before any consumer, both phases of one rollback reach it
// under their own keys and reach no other queue, a narrower binding selects rollback announcements
// alone, the queue has the retry and dead-letter paths every work queue gets, and a broker restart
// leaves the layout and its waiting events intact.
func TestRollbackNotificationsAgainstBroker(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	ctx := context.Background()
	topology := NewTopology(3, time.Second, time.Minute)
	notifier := NewNotifier(h.URL, topology, slog.New(slog.DiscardHandler))
	session, err := notifier.supervisor.dial(ctx)
	if err != nil {
		t.Fatalf("dial and declare: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Logf("close session: %v", err)
		}
	})

	t.Run("the queue is declared and bound before a consumer exists", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, RolloutNotificationQueue)
		// Depth 0 on a passive declaration proves the queue exists: an undeclared queue is
		// refused by the broker.
		if got := h.Depth(t, ch, RolloutNotificationQueue, true); got != 0 {
			t.Errorf("notification queue depth = %d, want it empty and existing", got)
		}
		for _, queue := range []string{
			topology.RetryQueue(RolloutNotificationQueue, 1),
			topology.RetryQueue(RolloutNotificationQueue, 2),
			topology.DeadLetterQueue(RolloutNotificationQueue),
		} {
			if got := h.Depth(t, ch, queue, true); got != 0 {
				t.Errorf("queue %s depth = %d, want it empty and existing", queue, got)
			}
		}
	})

	t.Run("both phases reach the notification queue and no rollout queue", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, RolloutNotificationQueue)
		h.Purge(t, ch, RolloutQueue)
		h.Purge(t, ch, HeartbeatQueue)

		announcement := rollbackAnnouncement(RollbackStarted)
		msg, err := announcement.Message(RollbackRoutingKey(RollbackStarted))
		if err != nil {
			t.Fatalf("build message: %v", err)
		}
		if _, err := newSessionPublisher(session); err != nil {
			t.Fatalf("enable confirms: %v", err)
		}
		if err := session.PublishWithContext(ctx, EventsExchange,
			RollbackRoutingKey(RollbackStarted), false, false, msg); err != nil {
			t.Fatalf("publish started announcement: %v", err)
		}
		completed := rollbackAnnouncement(RollbackCompleted)
		msg, err = completed.Message(RollbackRoutingKey(RollbackCompleted))
		if err != nil {
			t.Fatalf("build message: %v", err)
		}
		if err := session.PublishWithContext(ctx, EventsExchange,
			RollbackRoutingKey(RollbackCompleted), false, false, msg); err != nil {
			t.Fatalf("publish completed announcement: %v", err)
		}

		h.AwaitDepth(t, ch, RolloutNotificationQueue, true, 2)
		if got := h.Depth(t, ch, RolloutQueue, true); got != 0 {
			t.Errorf("rollout work queue depth = %d, want 0 for an announcement", got)
		}
		if got := h.Depth(t, ch, HeartbeatQueue, true); got != 0 {
			t.Errorf("heartbeat queue depth = %d, want 0 for an announcement", got)
		}

		// The events wait for a consumer, and are taken in publication order.
		deliveries := nextDeliveries(t, h, RolloutNotificationQueue, 2)
		first, second := deliveries[0], deliveries[1]
		decoded, err := DecodeRollbackEvent(first.Body)
		if err != nil {
			t.Fatalf("decode the first announcement: %v", err)
		}
		if decoded.Phase != RollbackStarted {
			t.Errorf("first announcement phase = %s, want started", decoded.Phase)
		}
		if _, err := DecodeRollbackEvent(second.Body); err != nil {
			t.Fatalf("decode the second announcement: %v", err)
		}
		if first.MessageId == second.MessageId {
			t.Error("the two phases share a message id, want distinct event identities")
		}
	})

	t.Run("a narrow binding selects rollback announcements alone", func(t *testing.T) {
		ch := h.Channel(t)
		const narrow = "telemetrytest.rollback.notifications"
		if _, err := ch.QueueDeclare(narrow, false, true, true, false, nil); err != nil {
			t.Fatalf("declare the test queue: %v", err)
		}
		if err := ch.QueueBind(narrow, rollbackNotificationsKey, EventsExchange, false, nil); err != nil {
			t.Fatalf("bind the test queue: %v", err)
		}
		t.Cleanup(func() { h.Purge(t, ch, narrow) })

		publish(t, session, EventsExchange, RollbackRoutingKey(RollbackStarted), "rollback-event", nil)
		h.AwaitDepth(t, ch, narrow, false, 1)
		if got := h.Depth(t, ch, RolloutNotificationQueue, true); got < 1 {
			t.Errorf("family queue depth = %d, want the announcement enqueued on it too", got)
		}
	})

	t.Run("the layout and its waiting events survive a broker restart", func(t *testing.T) {
		ch := h.Channel(t)
		h.Purge(t, ch, RolloutNotificationQueue)
		publish(t, session, EventsExchange, RollbackRoutingKey(RollbackCompleted), "before-restart", nil)
		h.AwaitDepth(t, ch, RolloutNotificationQueue, true, 1)

		h.Restart(t)

		reconnected, err := notifier.supervisor.dial(ctx)
		if err != nil {
			t.Fatalf("dial after the restart: %v", err)
		}
		t.Cleanup(func() {
			if err := reconnected.Close(); err != nil {
				t.Logf("close reconnected session: %v", err)
			}
		})
		after := h.Channel(t)
		// The queue was declared again by the reconnecting process and the persistent message
		// it held is still there: a broker restart is not a lost announcement.
		h.AwaitDepth(t, after, RolloutNotificationQueue, true, 1)
		delivery := nextDelivery(t, h, RolloutNotificationQueue)
		if string(delivery.Body) != "before-restart" {
			t.Errorf("message body after the restart = %q, want before-restart", delivery.Body)
		}
	})
}

// nextDeliveries consumes n messages from a queue on one connection, so the events a test expects
// on one queue are all read before the consumer goes away: a consumer with no prefetch limit is
// handed more than one message, and the ones it never read die with its connection.
func nextDeliveries(t *testing.T, h *rabbittest.Harness, queue string, n int) []amqp.Delivery {
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
	if err := ch.Qos(n, 0, false); err != nil {
		t.Fatalf("set prefetch: %v", err)
	}
	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume %s: %v", queue, err)
	}
	out := make([]amqp.Delivery, 0, n)
	for len(out) < n {
		select {
		case d := <-deliveries:
			out = append(out, d)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out after %d of %d deliveries from %s", len(out), n, queue)
		}
	}
	return out
}

// TestNotifierAgainstBroker verifies the announcing path end to end against a real broker: a
// confirmed publication returns once the broker has the event, the event is on the declared
// notification queue, and the notifier reconnects after a broker restart without a process
// restart.
func TestNotifierAgainstBroker(t *testing.T) {
	t.Parallel()

	h := rabbittest.Start(t)
	topology := NewTopology(3, time.Second, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Declaration is what creates the layout, so a session is dialed — and the topology declared
	// — before anything is asserted about a queue.
	declared, err := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler)).dial(ctx)
	if err != nil {
		t.Fatalf("dial and declare: %v", err)
	}
	t.Cleanup(func() {
		if err := declared.Close(); err != nil {
			t.Logf("close declaring session: %v", err)
		}
	})

	notifier := NewNotifier(h.URL, topology, slog.New(slog.DiscardHandler))
	done := make(chan error, 1)
	go func() { done <- notifier.Run(ctx) }()

	ch := h.Channel(t)
	h.Purge(t, ch, RolloutNotificationQueue)

	started := rollbackAnnouncement(RollbackStarted)
	if err := notifier.Announce(ctx, started); err != nil {
		t.Fatalf("Announce() error = %v", err)
	}
	h.AwaitDepth(t, ch, RolloutNotificationQueue, true, 1)
	delivery := nextDelivery(t, h, RolloutNotificationQueue)
	if delivery.MessageId != started.EventID {
		t.Errorf("delivered message id = %q, want %q", delivery.MessageId, started.EventID)
	}
	decoded, err := DecodeRollbackEvent(delivery.Body)
	if err != nil {
		t.Fatalf("decode the delivered announcement: %v", err)
	}
	if diff := cmp.Diff(started, decoded); diff != "" {
		t.Errorf("delivered announcement mismatch (-want +got):\n%s", diff)
	}

	// A broker restart is a reconnect, not the end of the notifier: the next announcement is
	// published through the session that replaced the lost one. Until that reconnect has
	// re-declared the layout the queue does not exist, so the announcement is retried rather
	// than purged against a stale channel.
	h.Restart(t)
	completed := rollbackAnnouncement(RollbackCompleted)
	awaitCondition(t, "the notifier to publish after the broker restart", func() bool {
		return notifier.Announce(ctx, completed) == nil
	})

	after := h.Channel(t)
	h.AwaitDepth(t, after, RolloutNotificationQueue, true, 1)
	redelivered := nextDelivery(t, h, RolloutNotificationQueue)
	if redelivered.MessageId != completed.EventID {
		t.Errorf("message id after the restart = %q, want %q", redelivered.MessageId, completed.EventID)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil on shutdown", err)
	}
}
