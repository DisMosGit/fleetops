//go:build integration

package rabbittest

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestHarnessRoundTrip proves the harness hands out a usable broker: declare, publish, observe
// the depth, consume the message, and see the queue drain.
func TestHarnessRoundTrip(t *testing.T) {
	t.Parallel()

	h := Start(t)
	ctx := context.Background()
	const queue = "rabbittest.round-trip"

	ch := h.Channel(t)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         []byte(`{"ping":true}`),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	h.AwaitDepth(t, ch, queue, true, 1)

	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case d := <-deliveries:
		if got := string(d.Body); got != `{"ping":true}` {
			t.Errorf("delivered body = %q, want the published payload", got)
		}
		if d.DeliveryMode != amqp.Persistent {
			t.Errorf("delivery mode = %d, want persistent", d.DeliveryMode)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the published message")
	}
	h.AwaitDepth(t, ch, queue, true, 0)
}
