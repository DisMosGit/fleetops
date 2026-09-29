//go:build integration

package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
)

// scriptedHandler records when each event was applied and fails the calls a test scripts.
type scriptedHandler struct {
	// fail decides an attempt's outcome from the event id and the one-based call number.
	fail func(eventID string, call int) error

	mu    sync.Mutex
	calls map[string][]time.Time
}

// newScriptedHandler returns a handler that succeeds every call.
func newScriptedHandler() *scriptedHandler {
	return &scriptedHandler{calls: make(map[string][]time.Time)}
}

// Apply records the call and returns the scripted error.
func (h *scriptedHandler) Apply(_ context.Context, env Envelope) error {
	h.mu.Lock()
	h.calls[env.EventID] = append(h.calls[env.EventID], time.Now())
	call := len(h.calls[env.EventID])
	h.mu.Unlock()
	if h.fail == nil {
		return nil
	}
	return h.fail(env.EventID, call)
}

// callCount returns how often an event was applied.
func (h *scriptedHandler) callCount(eventID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls[eventID])
}

// attempts returns the times an event was applied, oldest first.
func (h *scriptedHandler) attempts(eventID string) []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Time(nil), h.calls[eventID]...)
}

// consumerFixture is a running consumer with the real broker, store, and metrics behind it.
type consumerFixture struct {
	rabbit    *rabbittest.Harness
	store     *mongotest.Harness
	topology  Topology
	metrics   *Metrics
	consumer  *Consumer
	ledger    *Ledger
	publisher *brokerPublisher
}

// startConsumer runs a consumer of the heartbeat work queue against a real broker and a real
// store, with the configured attempt limit and retry ladder.
func startConsumer(t *testing.T, maxAttempts int, retryBase, retryMax time.Duration, handler Handler) *consumerFixture {
	t.Helper()

	store := mongotest.Start(t)
	rabbit := rabbittest.Start(t)
	topology := NewTopology(maxAttempts, retryBase, retryMax)
	work, err := topology.WorkQueue(HeartbeatQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", HeartbeatQueue, err)
	}
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	ledger := NewLedger(store.DB.Collection("processed_events"))
	consumer := NewConsumer(
		rabbit.URL,
		work,
		topology,
		ledger,
		handler,
		ConsumerOptions{Prefetch: 4, Metrics: metrics, Source: &Sequences{}},
		slog.New(slog.DiscardHandler),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("consumer Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("consumer Run() did not return after shutdown")
		}
	})

	return &consumerFixture{
		rabbit:    rabbit,
		store:     store,
		topology:  topology,
		metrics:   metrics,
		consumer:  consumer,
		ledger:    ledger,
		publisher: newBrokerPublisher(t, rabbit, topology),
	}
}

// brokerPublisher publishes events the way the control plane does: on a channel whose topology is
// already declared, and with the mandatory flag set so an unroutable event fails the test
// instead of waiting for a message that went nowhere.
type brokerPublisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

// newBrokerPublisher declares the topology up front — no publish may race a consumer's own
// declaration — and returns a publisher over a channel that reports returned messages.
func newBrokerPublisher(t *testing.T, h *rabbittest.Harness, topology Topology) *brokerPublisher {
	t.Helper()
	sess, err := NewSupervisor(h.URL, topology, slog.New(slog.DiscardHandler)).dial(context.Background())
	if err != nil {
		t.Fatalf("declare the topology: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Logf("close declaration session: %v", err)
	}
	ch := h.Channel(t)
	return &brokerPublisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 8))}
}

// publish sends one event to the events exchange.
func (p *brokerPublisher) publish(t *testing.T, env Envelope, attempt int) {
	t.Helper()
	key := HeartbeatRoutingKey(env.Region, env.Model)
	msg, err := env.Message(attempt, key)
	if err != nil {
		t.Fatalf("build message for %s: %v", env.EventID, err)
	}
	if err := p.ch.PublishWithContext(context.Background(), EventsExchange, key, true, false, msg); err != nil {
		t.Fatalf("publish %s: %v", env.EventID, err)
	}
	p.assertRouted(t, env.EventID)
}

// publishRaw sends an arbitrary payload, so a test can hand the consumer something it cannot
// decode.
func (p *brokerPublisher) publishRaw(t *testing.T, body string) {
	t.Helper()
	err := p.ch.PublishWithContext(context.Background(), EventsExchange,
		HeartbeatRoutingKey("eu-west", "v3"), true, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    "ev-raw",
			Type:         HeartbeatEventType,
			Body:         []byte(body),
		})
	if err != nil {
		t.Fatalf("publish raw payload: %v", err)
	}
	p.assertRouted(t, "ev-raw")
}

// assertRouted fails the test when the broker returned the message as unroutable.
func (p *brokerPublisher) assertRouted(t *testing.T, eventID string) {
	t.Helper()
	select {
	case returned := <-p.returns:
		t.Fatalf("event %s was unroutable under %s", eventID, returned.RoutingKey)
	case <-time.After(100 * time.Millisecond):
	}
}

// awaitCondition waits until check holds, failing the test with what when it never does.
func awaitCondition(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitOutcome waits until an outcome counter reaches want.
func awaitOutcome(t *testing.T, f *consumerFixture, outcome string, want float64) {
	t.Helper()
	awaitCondition(t, "the "+outcome+" outcome counter", func() bool {
		return testutil.ToFloat64(f.metrics.Consumed.WithLabelValues(HeartbeatQueue, outcome)) == want
	})
}

// awaitDepth waits until a queue of the topology holds want messages.
func awaitDepth(t *testing.T, f *consumerFixture, ch *amqp.Channel, queue string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got int
	for {
		got = f.rabbit.Depth(t, ch, queue, true)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s depth = %d, want %d", queue, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// alertDoc reads one device's alert document, reporting whether it exists.
func alertDoc(t *testing.T, f *consumerFixture, deviceID string) (alertDocument, bool) {
	t.Helper()
	var got alertDocument
	err := f.store.DB.Collection("device_alerts").
		FindOne(context.Background(), bson.D{{Key: "_id", Value: deviceID}}).Decode(&got)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return alertDocument{}, false
	}
	if err != nil {
		t.Fatalf("find alert for %s: %v", deviceID, err)
	}
	return got, true
}

// ledgerDoc reads one consumer's ledger record, reporting whether it exists.
func ledgerDoc(t *testing.T, f *consumerFixture, eventID string) (ledgerRecord, bool) {
	t.Helper()
	var got ledgerRecord
	err := f.store.DB.Collection("processed_events").
		FindOne(context.Background(), bson.D{{Key: "_id", Value: LedgerID(HeartbeatQueue, eventID)}}).Decode(&got)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ledgerRecord{}, false
	}
	if err != nil {
		t.Fatalf("find ledger record for %s: %v", eventID, err)
	}
	return got, true
}

// ledgerRecord mirrors the processed_events fields these tests assert on.
type ledgerRecord struct {
	DeviceID    string     `bson:"device_id"`
	ClaimedAt   time.Time  `bson:"claimed_at"`
	ProcessedAt *time.Time `bson:"processed_at"`
}

// heartbeatEvent returns a heartbeat event for one device with the given health.
func heartbeatEvent(eventID, deviceID string, health float64, observedAt time.Time) Envelope {
	env := testEvent()
	env.EventID = eventID
	env.DeviceID = deviceID
	env.OccurredAt = observedAt
	env.PublishedAt = observedAt.Add(time.Millisecond)
	env.Sequence = uint64(observedAt.UnixNano())
	env.Payload.Health = health
	return env
}

// TestConsumerProcessesDeduplicatesAndAlerts verifies the consumer end to end against the real
// broker and store: a degraded heartbeat becomes one alert with one processed ledger record, a
// redelivery changes nothing and is counted as a duplicate, and a healthy heartbeat writes no
// alert.
func TestConsumerProcessesDeduplicatesAndAlerts(t *testing.T) {
	t.Parallel()

	store := mongotest.Start(t)
	rabbit := rabbittest.Start(t)
	topology := NewTopology(2, time.Second, 2*time.Second)
	work, err := topology.WorkQueue(HeartbeatQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", HeartbeatQueue, err)
	}
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	consumer := NewConsumer(
		rabbit.URL,
		work,
		topology,
		NewLedger(store.DB.Collection("processed_events")),
		NewAlerting(store.DB.Collection("device_alerts"), 0.6),
		ConsumerOptions{Prefetch: 4, Metrics: metrics, Source: &Sequences{}},
		slog.Default(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("consumer Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("consumer Run() did not return after shutdown")
		}
	})

	fixture := &consumerFixture{
		rabbit:    rabbit,
		store:     store,
		topology:  topology,
		metrics:   metrics,
		consumer:  consumer,
		publisher: newBrokerPublisher(t, rabbit, topology),
	}
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	degraded := heartbeatEvent("ev-degraded", "dev-degraded", 0.3, observedAt)

	fixture.publisher.publish(t, degraded, 1)
	awaitOutcome(t, fixture, outcomeProcessed, 1)

	alert, ok := alertDoc(t, fixture, "dev-degraded")
	if !ok {
		t.Fatal("no alert document for the degraded heartbeat")
	}
	if alert.MinHealth != 0.3 || alert.Threshold != 0.6 || !alert.FirstSeenAt.Equal(observedAt) {
		t.Errorf("alert = %+v, want the degraded observation recorded with the threshold", alert)
	}
	record, ok := ledgerDoc(t, fixture, "ev-degraded")
	if !ok {
		t.Fatal("no processed-events record for the degraded heartbeat")
	}
	if record.ProcessedAt == nil || record.DeviceID != "dev-degraded" {
		t.Errorf("ledger record = %+v, want a completed record for the device", record)
	}

	// The same event again: the side effect is not repeated, and the delivery is counted as a
	// duplicate.
	fixture.publisher.publish(t, degraded, 1)
	awaitOutcome(t, fixture, outcomeDuplicate, 1)
	if again, _ := alertDoc(t, fixture, "dev-degraded"); again != alert {
		t.Errorf("alert after redelivery = %+v, want it unchanged from %+v", again, alert)
	}
	if got := countDocuments(t, store, "processed_events"); got != 1 {
		t.Errorf("processed-events documents = %d, want exactly 1", got)
	}
	if got := countDocuments(t, store, "device_alerts"); got != 1 {
		t.Errorf("alert documents = %d, want exactly 1", got)
	}

	// A healthy heartbeat is processed but records no alert.
	fixture.publisher.publish(t, heartbeatEvent("ev-healthy", "dev-healthy", 0.95, observedAt), 1)
	awaitOutcome(t, fixture, outcomeProcessed, 2)
	if _, ok := alertDoc(t, fixture, "dev-healthy"); ok {
		t.Error("a healthy heartbeat recorded an alert, want none")
	}
	if record, ok := ledgerDoc(t, fixture, "ev-healthy"); !ok || record.ProcessedAt == nil {
		t.Errorf("ledger record for the healthy heartbeat = %+v (present %t), want it processed", record, ok)
	}
}

// TestConsumerRetriesThenDeadLetters verifies the failure paths against the real broker: a
// handler that fails twice then succeeds returns through the retry ladder with its delays, a
// handler that always fails ends on the dead-letter queue with its reason and attempt count, and
// an unusable event is dead-lettered without burning an attempt.
func TestConsumerRetriesThenDeadLetters(t *testing.T) {
	t.Parallel()

	handler := newScriptedHandler()
	handler.fail = func(eventID string, call int) error {
		switch {
		case eventID == "ev-flaky" && call <= 2:
			return errors.New("transient failure")
		case eventID == "ev-poison":
			return errors.New("poison message")
		default:
			return nil
		}
	}
	// Three attempts with a 200ms first backoff keep the ladder observable without a long test.
	fixture := startConsumer(t, 3, 200*time.Millisecond, time.Second, handler)
	ch := fixture.rabbit.Channel(t)
	dlq := fixture.topology.DeadLetterQueue(HeartbeatQueue)
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)

	start := time.Now()
	fixture.publisher.publish(t, heartbeatEvent("ev-flaky", "dev-flaky", 0.3, observedAt), 1)
	awaitCondition(t, "the flaky event to be applied three times", func() bool {
		return handler.callCount("ev-flaky") == 3
	})
	elapsed := time.Since(start)
	// The first retry waits 200ms and the second 400ms: anything faster means the ladder did not
	// hold the message for its backoff.
	if minimum := 600 * time.Millisecond; elapsed < minimum {
		t.Errorf("retry ladder took %s, want at least %s", elapsed, minimum)
	}
	awaitOutcome(t, fixture, outcomeProcessed, 1)
	if got := testutil.ToFloat64(fixture.metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 2 {
		t.Errorf("retry outcomes = %v, want 2", got)
	}
	if record, ok := ledgerDoc(t, fixture, "ev-flaky"); !ok || record.ProcessedAt == nil {
		t.Errorf("ledger record for the retried event = %+v (present %t), want it completed", record, ok)
	}

	// A message that keeps failing ends on the dead-letter queue with what an operator needs.
	fixture.publisher.publish(t, heartbeatEvent("ev-poison", "dev-poison", 0.1, observedAt), 1)
	dead := nextDelivery(t, fixture.rabbit, dlq)
	if got := dead.MessageId; got != "ev-poison" {
		t.Errorf("dead letter message id = %q, want ev-poison", got)
	}
	if got := attemptOf(dead); got != 3 {
		t.Errorf("dead letter attempt = %d, want the attempts made (3)", got)
	}
	reason, _ := dead.Headers[deadLetterReasonHeader].(string)
	if !strings.Contains(reason, "poison message") {
		t.Errorf("dead letter reason = %q, want the failure reason", reason)
	}
	awaitOutcome(t, fixture, outcomeDeadLetter, 1)
	if record, ok := ledgerDoc(t, fixture, "ev-poison"); !ok || record.ProcessedAt != nil {
		t.Errorf("ledger record for the poison event = %+v (present %t), want an unfinished claim", record, ok)
	}

	// An unusable payload is dead-lettered immediately: no retry, no ledger entry.
	fixture.publisher.publishRaw(t, "{not json")
	malformed := nextDelivery(t, fixture.rabbit, dlq)
	reason, _ = malformed.Headers[deadLetterReasonHeader].(string)
	if !strings.Contains(reason, "decode event payload") {
		t.Errorf("malformed dead-letter reason = %q, want the decode failure", reason)
	}

	// An unsupported schema version is refused the same way.
	unsupported := heartbeatEvent("ev-unsupported", "dev-unsupported", 0.2, observedAt)
	unsupported.SchemaVersion = 9
	fixture.publisher.publish(t, unsupported, 1)
	refused := nextDelivery(t, fixture.rabbit, dlq)
	reason, _ = refused.Headers[deadLetterReasonHeader].(string)
	if !strings.Contains(reason, "unsupported schema version 9") {
		t.Errorf("schema-version dead-letter reason = %q, want the unsupported version named", reason)
	}

	awaitOutcome(t, fixture, outcomeDeadLetter, 3)
	// Two retries for the flaky event and two for the poison event, and none for the two
	// unusable events.
	if got := testutil.ToFloat64(fixture.metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 4 {
		t.Errorf("retry outcomes = %v, want 4", got)
	}
	for _, eventID := range []string{"ev-raw", "ev-unsupported"} {
		if record, ok := ledgerDoc(t, fixture, eventID); ok {
			t.Errorf("ledger record for unusable event %s = %+v, want none", eventID, record)
		}
	}
	// No unusable event may sit in a retry queue waiting for an attempt it must not get.
	for _, queue := range fixture.topology.Queues() {
		if queue.Kind != QueueKindRetry {
			continue
		}
		if got := fixture.rabbit.Depth(t, ch, queue.Name, true); got != 0 {
			t.Errorf("retry queue %s depth = %d, want an unusable event not retried", queue.Name, got)
		}
	}
}

// countDocuments returns how many documents a collection holds.
func countDocuments(t *testing.T, store *mongotest.Harness, collection string) int64 {
	t.Helper()
	got, err := store.DB.Collection(collection).CountDocuments(context.Background(), bson.D{})
	if err != nil {
		t.Fatalf("count %s documents: %v", collection, err)
	}
	return got
}
