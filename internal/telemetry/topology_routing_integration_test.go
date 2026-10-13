//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/DisMosGit/fleetops/internal/rabbittest"
)

// declareSharedTopology declares the layout on a broker and returns a session to publish through,
// so a routing test measures the bindings rather than racing the declaration of the layout.
func declareSharedTopology(t *testing.T, h *rabbittest.Harness, topology Topology) Session {
	t.Helper()

	sess, err := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler)).dial(context.Background())
	if err != nil {
		t.Fatalf("dial and declare: %v", err)
	}
	t.Cleanup(func() {
		if err := sess.Close(); err != nil {
			t.Logf("close declaring session: %v", err)
		}
	})
	return sess
}

// TestTopologyNarrowedNotificationBinding backs the scenario "A consumer narrows to one kind of
// notification" (message-broker-topology): a queue bound to the rollback announcements receives
// them alongside the family-wide notification queue, and a notification of another kind reaches
// the family-wide queue alone.
func TestTopologyNarrowedNotificationBinding(t *testing.T) {
	// Deliberately not parallel: it publishes into the package's shared notification queue and
	// asserts on its depth.
	h := rabbittest.For(t)
	topology := NewTopology(3, time.Second, time.Minute)
	sess := declareSharedTopology(t, h, topology)
	ch := h.Channel(t)

	// The narrower binding a consumer attaches with, beside the layout's own family-wide
	// notification queue and without any change to the publisher.
	const narrow = "telemetrytest.rollback.notifications"
	if _, err := ch.QueueDeclare(narrow, false, true, true, false, nil); err != nil {
		t.Fatalf("declare the narrow queue: %v", err)
	}
	if err := ch.QueueBind(narrow, rollbackNotificationsKey, EventsExchange, false, nil); err != nil {
		t.Fatalf("bind the narrow queue to %s: %v", rollbackNotificationsKey, err)
	}
	h.Purge(t, ch, narrow)
	h.Purge(t, ch, RolloutNotificationQueue)

	publish(t, sess, EventsExchange, RollbackRoutingKey(RollbackStarted), "rollback-started", nil)
	h.AwaitDepth(t, ch, narrow, false, 1)
	h.AwaitDepth(t, ch, RolloutNotificationQueue, true, 1)

	// Another kind of notification, under the same key grammar: it belongs to the family-wide
	// queue and to no narrower queue.
	publish(t, sess, EventsExchange, "rollout.notification.downgrade.started", "other-kind", nil)
	h.AwaitDepth(t, ch, RolloutNotificationQueue, true, 2)
	if got := h.Depth(t, ch, narrow, false); got != 1 {
		t.Errorf("rollback-only queue depth = %d, want no notification of another kind on it", got)
	}
}

// TestTopologyEventFamiliesHaveDisjointKeySpaces backs the scenario "Every event family has its
// own key space" (message-broker-topology): a heartbeat event cannot be delivered to a queue bound
// only to rollout work or only to rollout notifications.
func TestTopologyEventFamiliesHaveDisjointKeySpaces(t *testing.T) {
	// Deliberately not parallel: it asserts on the depths of the package's shared queues.
	h := rabbittest.For(t)
	topology := NewTopology(3, time.Second, time.Minute)
	sess := declareSharedTopology(t, h, topology)
	ch := h.Channel(t)
	for _, queue := range []string{HeartbeatQueue, RolloutQueue, RolloutNotificationQueue} {
		h.Purge(t, ch, queue)
	}

	publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "heartbeat-event", nil)
	// The event reached the queue bound to its own family, so an empty rollout queue is a disjoint
	// key space rather than an event that went nowhere.
	h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
	if got := h.Depth(t, ch, RolloutQueue, true); got != 0 {
		t.Errorf("%s depth = %d, want no heartbeat event on the rollout work queue", RolloutQueue, got)
	}
	if got := h.Depth(t, ch, RolloutNotificationQueue, true); got != 0 {
		t.Errorf("%s depth = %d, want no heartbeat event on the notification queue",
			RolloutNotificationQueue, got)
	}
}
