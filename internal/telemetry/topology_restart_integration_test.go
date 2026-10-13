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
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestTopologySurvivesBrokerRestartWithWaitingMessages backs the scenario "Layout survives a broker
// restart" (message-broker-topology): a message waiting on a work queue and one waiting on a retry
// queue are both still there, in queues that still exist, once the broker is back.
func TestTopologySurvivesBrokerRestartWithWaitingMessages(t *testing.T) {
	t.Parallel()

	// A broker of its own: the state the scenario describes is built here rather than inherited
	// from another test's declarations.
	h := rabbittest.Start(t)
	// A retry delay far longer than this test: the retry message must still be waiting when the
	// broker comes back, rather than having expired onto the work queue.
	topology := NewTopology(3, time.Hour, 2*time.Hour)
	sess, err := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler)).dial(context.Background())
	if err != nil {
		t.Fatalf("dial and declare: %v", err)
	}
	t.Cleanup(func() {
		if err := sess.Close(); err != nil {
			t.Logf("close declaring session: %v", err)
		}
	})

	retry := topology.RetryQueue(HeartbeatQueue, 1)
	publish(t, sess, EventsExchange, HeartbeatRoutingKey("eu-west", "v3"), "work-before-restart", nil)
	publish(t, sess, RetryExchange, retry, "retry-before-restart", amqp.Table{attemptHeader: int32(2)})

	ch := h.Channel(t)
	h.AwaitDepth(t, ch, HeartbeatQueue, true, 1)
	h.AwaitDepth(t, ch, retry, true, 1)

	h.Restart(t)

	// Both queues still exist — a missing one fails the depth probe — and both messages are still
	// waiting in them.
	after := h.Channel(t)
	h.AwaitDepth(t, after, HeartbeatQueue, true, 1)
	h.AwaitDepth(t, after, retry, true, 1)
	if got := string(nextDelivery(t, h, HeartbeatQueue).Body); got != "work-before-restart" {
		t.Errorf("work queue message after the restart = %q, want the message published before it", got)
	}
	if got := string(nextDelivery(t, h, retry).Body); got != "retry-before-restart" {
		t.Errorf("retry queue message after the restart = %q, want the message published before it", got)
	}
}

// brokerElement is one element of the layout as the process that uses it declares it: an exchange
// with its type, or a durable queue.
type brokerElement struct {
	name  string
	kind  string
	queue bool
}

// declaredElements lists every exchange, work queue, retry queue, and dead-letter queue the layout
// declares, in declaration order.
func declaredElements(topology Topology) []brokerElement {
	elements := []brokerElement{
		{name: EventsExchange, kind: amqp.ExchangeTopic},
		{name: RetryExchange, kind: amqp.ExchangeDirect},
		{name: DeadLetterExchange, kind: amqp.ExchangeDirect},
	}
	for _, queue := range topology.Queues() {
		elements = append(elements, brokerElement{name: queue.Name, queue: true})
	}
	return elements
}

// elementPresent reports whether the broker already carries one element. Every probe opens a
// channel of its own, because the broker closes the channel a passive declaration of a missing
// element was made on.
func elementPresent(t *testing.T, conn *amqp.Connection, el brokerElement) bool {
	t.Helper()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe channel: %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			t.Logf("close probe channel: %v", err)
		}
	}()
	if el.queue {
		_, err = ch.QueueDeclarePassive(el.name, true, false, false, false, nil)
		return err == nil
	}
	return ch.ExchangeDeclarePassive(el.name, el.kind, true, false, false, false, nil) == nil
}

// TestTopologyDeclaredBeforeFirstUse backs the scenario "Declaration precedes use"
// (message-broker-topology): a process started against a broker that carries no FleetOps topology
// has declared every exchange, work queue, retry queue, and dead-letter queue it uses before its
// first publish.
func TestTopologyDeclaredBeforeFirstUse(t *testing.T) {
	t.Parallel()

	// A broker of its own: the premise is a broker carrying no FleetOps topology, which the
	// package's shared broker — declared by every test that touches it — never is.
	h := rabbittest.Start(t)
	topology := NewTopology(3, time.Second, time.Minute)
	elements := declaredElements(topology)

	conn, err := amqp.Dial(h.URL)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close probe connection: %v", err)
		}
	})
	for _, el := range elements {
		if elementPresent(t, conn, el) {
			t.Fatalf("the fresh broker already carries %s", el.name)
		}
	}

	metrics := NewMetrics(prometheus.NewRegistry())
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
		case <-time.After(testexec.Get(t).Wait(30 * time.Second)):
			t.Error("publisher Run() did not return after shutdown")
		}
	})

	// Wait for the starting process to declare the layout, checking all the while that it has
	// published nothing: what it declares exists before its first publish.
	deadline := time.Now().Add(testexec.Get(t).Wait(30 * time.Second))
	for {
		var missing string
		for _, el := range elements {
			if !elementPresent(t, conn, el) {
				missing = el.name
				break
			}
		}
		if missing == "" {
			break
		}
		if got := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)); got != 0 {
			t.Fatalf("published %v events while %s was still missing", got, missing)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was never declared before the first publish", missing)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)); got != 0 {
		t.Fatalf("published %v events before the layout was complete", got)
	}

	// The layout is complete and nothing has been published, so this first publish — confirmed by
	// the broker, on a key only the declared bindings make routable — proves the order.
	rec := devices.Record{ID: "dev-declared", Region: "eu-west", Model: "v3"}
	publisher.Handle(ctx, heartbeat("ev-declared", rec.ID, time.Now()), rec)
	awaitConditionWithin(t, 30*time.Second, "the first confirmed publish", func() bool {
		return testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)) == 1
	})
}
