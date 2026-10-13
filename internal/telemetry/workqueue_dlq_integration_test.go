//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestRolloutWorkQueueDeadLettersAPoisonMessage covers the terminal path of the rollout work queue:
// the layout declares it with a dead-letter exchange and binds its dead-letter queue, so a delivery
// a consumer cannot process is refused into that queue with the broker's record of why rather than
// being requeued forever or dropped silently.
//
// The rollout workflow is both the producer and the consumer of this queue, so no production
// consumer exists yet; the consumer here stands in for the one the workflow stage adds. What is
// under test is the binding the layout declares, which is why the assertion follows the refused
// message all the way to the dead-letter queue with its payload, its attempt, and its death record
// intact.
func TestRolloutWorkQueueDeadLettersAPoisonMessage(t *testing.T) {
	// Deliberately not parallel: it consumes from a queue every test of the package shares and
	// asserts on the depths it leaves behind, and a sequential test never overlaps a parallel one.
	h := rabbittest.For(t)
	budget := testexec.Get(t).Wait(10 * time.Second)
	ctx := context.Background()

	topology := NewTopology(3, time.Second, 2*time.Second)
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

	dlq := topology.DeadLetterQueue(RolloutQueue)
	ch := h.Channel(t)
	h.Purge(t, ch, RolloutQueue)
	h.Purge(t, ch, dlq)

	const payload = `{"rollout_id":"ro-poison","device_id":"device-7"}`
	publish(t, sess, EventsExchange, RolloutRoutingKey("start"), payload, amqp.Table{attemptHeader: int32(1)})
	h.AwaitDepth(t, ch, RolloutQueue, true, 1)

	// The poison delivery: the consumer refuses it without requeue, which is the broker path the
	// layout binds to the dead-letter exchange.
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("set prefetch: %v", err)
	}
	deliveries, err := ch.Consume(RolloutQueue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume %s: %v", RolloutQueue, err)
	}
	select {
	case delivery := <-deliveries:
		if got := string(delivery.Body); got != payload {
			t.Errorf("consumed payload = %q, want the published %q", got, payload)
		}
		if err := delivery.Nack(false, false); err != nil {
			t.Fatalf("nack delivery: %v", err)
		}
	case <-time.After(budget):
		t.Fatal("timed out waiting for the rollout delivery")
	}

	h.AwaitDepth(t, ch, dlq, true, 1)
	// The work queue holds nothing afterwards: the delivery was refused, not requeued.
	if got := h.Depth(t, ch, RolloutQueue, true); got != 0 {
		t.Errorf("%s depth = %d after the refusal, want 0", RolloutQueue, got)
	}

	dead := nextDelivery(t, h, dlq)
	if got := string(dead.Body); got != payload {
		t.Errorf("dead-lettered payload = %q, want the published %q", got, payload)
	}
	// The message is the delivery that was refused, not a republish of its body: the attempt the
	// producer stamped survives the move.
	if got := dead.Headers[attemptHeader]; got != int32(1) {
		t.Errorf("dead-lettered attempt = %v, want the refused delivery's attempt 1", got)
	}
	record := deathRecord(t, dead)
	if got := record["reason"]; got != "rejected" {
		t.Errorf("death reason = %v, want %q", got, "rejected")
	}
	if got := record["queue"]; got != RolloutQueue {
		t.Errorf("death queue = %v, want %q", got, RolloutQueue)
	}
	if got := deathCount(record["count"]); got != 1 {
		t.Errorf("death count = %v, want 1", record["count"])
	}
}

// deathRecord returns the broker's first death record of a dead-lettered delivery: the entry that
// names why the broker moved the message and from where, which is what makes a dead-lettered
// message diagnosable instead of merely gone.
func deathRecord(t *testing.T, delivery amqp.Delivery) amqp.Table {
	t.Helper()

	deaths, ok := delivery.Headers["x-death"].([]any)
	if !ok || len(deaths) == 0 {
		t.Fatalf("dead-lettered delivery carries x-death %#v, want the broker's death record",
			delivery.Headers["x-death"])
	}
	record, ok := deaths[0].(amqp.Table)
	if !ok {
		t.Fatalf("x-death entry = %T, want the broker's field table", deaths[0])
	}
	return record
}

// deathCount returns a death record's count, whichever integer width the client decoded it as.
func deathCount(value any) int64 {
	switch n := value.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	default:
		return -1
	}
}
