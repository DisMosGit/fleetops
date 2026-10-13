//go:build integration

package rabbittest

import (
	"context"
	"os"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's broker once, before the first test body, and releases it when the
// run ends.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m, SharedStack()))
}

// TestSharedBrokerIsPurgedBetweenTests asserts what a shared broker is for: one broker serves the
// whole package, what one test publishes is still there for the next one, and purging is what
// keeps a test from reading another's messages.
func TestSharedBrokerIsPurgedBetweenTests(t *testing.T) {
	t.Parallel()
	const queue = "rabbittest.shared.isolation"

	var firstURL string
	t.Run("the first test seeds the queue", func(t *testing.T) {
		h := Shared(t)
		firstURL = h.URL
		ch := h.Channel(t)
		declare(t, ch, queue)
		publish(t, ch, queue, 2)
		h.AwaitDepth(t, ch, queue, true, 2)
	})

	t.Run("the second test purges what it is about to assert on", func(t *testing.T) {
		h := Shared(t)
		if h.URL != firstURL {
			t.Fatalf("the two tests reached different brokers: %s and %s", firstURL, h.URL)
		}
		ch := h.Channel(t)
		declare(t, ch, queue)
		// The previous test's messages are still waiting: a shared broker keeps its queues, and
		// that is why isolation is a purge rather than a fresh container.
		h.AwaitDepth(t, ch, queue, true, 2)

		h.Purge(t, ch, queue)
		h.AwaitDepth(t, ch, queue, true, 0)

		publish(t, ch, queue, 3)
		h.AwaitDepth(t, ch, queue, true, 3)
	})
}

// TestForHonoursTheContainerUse asserts the profile decides how a test obtains a broker: a run
// that shares reaches the package's broker, and a run that asks for a private stack per test gets
// a broker of its own, which starts empty.
func TestForHonoursTheContainerUse(t *testing.T) {
	shared := For(t)
	if !testexec.Get(t).SharesContainers() {
		t.Fatal("an unconfigured run did not share the package's broker")
	}

	t.Run("a run asking for a private broker gets one", func(t *testing.T) {
		t.Setenv(testexec.EnvContainerUse, testexec.ContainersPerTest)
		private := For(t)
		if private.URL == shared.URL {
			t.Error("the private broker reused the shared broker's address")
		}
		if private.container == shared.container {
			t.Error("the private broker reused the shared container")
		}
		ch := private.Channel(t)
		declare(t, ch, "rabbittest.private")
		private.AwaitDepth(t, ch, "rabbittest.private", true, 0)
	})
}

// TestSharedBrokerKeepsItsAddressAcrossARestart asserts the restart support a shared broker still
// has to offer: the explicit host port survives, so a test can observe a connection reconnecting
// to the same address.
func TestSharedBrokerKeepsItsAddressAcrossARestart(t *testing.T) {
	// Deliberately not parallel: restarting the broker is visible to every test that shares it,
	// and a sequential test never overlaps a parallel one.
	h := Shared(t)
	before := h.URL
	ch := h.Channel(t)
	declare(t, ch, "rabbittest.restart")
	h.Purge(t, ch, "rabbittest.restart")
	publish(t, ch, "rabbittest.restart", 1)
	h.AwaitDepth(t, ch, "rabbittest.restart", true, 1)

	h.Restart(t)

	if h.URL != before {
		t.Errorf("broker address after restart = %s, want the same %s", h.URL, before)
	}
	after := h.Channel(t)
	if got := h.Depth(t, after, "rabbittest.restart", true); got != 1 {
		t.Errorf("a persistent message did not survive the restart: depth = %d, want 1", got)
	}
	h.Purge(t, after, "rabbittest.restart")
}

// declare creates the durable queue a test publishes to.
func declare(t *testing.T, ch *amqp.Channel, queue string) {
	t.Helper()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue %s: %v", queue, err)
	}
}

// publish writes count persistent messages to queue.
func publish(t *testing.T, ch *amqp.Channel, queue string, count int) {
	t.Helper()
	for i := range count {
		if err := ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         []byte(`{"ping":true}`),
		}); err != nil {
			t.Fatalf("publish %d to %s: %v", i, queue, err)
		}
	}
}
