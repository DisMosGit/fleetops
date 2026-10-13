//go:build integration

package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// runConsumer starts a consumer's loop and stops it when the test ends, so a consumer left running
// would fail the test rather than outlive it.
func runConsumer(t *testing.T, consumer *Consumer) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Consumer.Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(testexec.Get(t).Wait(30 * time.Second)):
			t.Error("Consumer.Run() did not return after shutdown")
		}
	})
}

// durableAcker is the acknowledgement seam of one delivery: it checks the store at the moment the
// consumer acknowledges that the side effect and the processed-events entry are already durable
// there, which is what makes the ordering an assertion rather than an assumption.
type durableAcker struct {
	t        *testing.T
	ctx      context.Context
	alerts   *mongo.Collection
	ledger   *mongo.Collection
	deviceID string
	eventID  string
	acks     int
}

// Ack checks the store before recording the acknowledgement.
func (a *durableAcker) Ack(uint64, bool) error {
	a.acks++
	count, err := a.alerts.CountDocuments(a.ctx, bson.D{{Key: "_id", Value: a.deviceID}})
	if err != nil {
		a.t.Errorf("read the side effect at acknowledgement: %v", err)
		return nil
	}
	if count != 1 {
		a.t.Errorf("alert documents at acknowledgement = %d, want the durable side effect first", count)
	}

	var record ledgerRecord
	err = a.ledger.FindOne(a.ctx,
		bson.D{{Key: "_id", Value: LedgerID(HeartbeatQueue, a.eventID)}}).Decode(&record)
	if err != nil {
		a.t.Errorf("read the processed-events entry at acknowledgement: %v", err)
		return nil
	}
	if record.ProcessedAt == nil {
		a.t.Error("processed_at at acknowledgement = nil, want the entry completed before the ack")
	}
	return nil
}

// Nack records a settlement the successful path must never make.
func (a *durableAcker) Nack(uint64, bool, bool) error {
	a.t.Error("delivery nacked, want the processed delivery acknowledged")
	return nil
}

// Reject records a settlement the successful path must never make.
func (a *durableAcker) Reject(uint64, bool) error {
	a.t.Error("delivery rejected, want the processed delivery acknowledged")
	return nil
}

// TestConsumerAcknowledgesOnlyAfterTheDurableWrite backs the scenario "Acknowledgement after the
// write" (heartbeat-consumption): the alert document and the completed processed-events record are
// both readable in the store at the moment the delivery is acknowledged.
func TestConsumerAcknowledgesOnlyAfterTheDurableWrite(t *testing.T) {
	t.Parallel()

	store := mongotest.For(t)
	ctx := context.Background()
	alerts := store.DB.Collection("device_alerts")
	ledgerDocs := store.DB.Collection("processed_events")
	topology := NewTopology(3, time.Second, time.Minute)
	work, err := topology.WorkQueue(HeartbeatQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", HeartbeatQueue, err)
	}
	consumer := NewConsumer(
		"amqp://guest:guest@localhost:5672/",
		work,
		topology,
		NewLedger(ledgerDocs),
		NewAlerting(alerts, 0.6),
		ConsumerOptions{Prefetch: 4, Metrics: NewMetrics(prometheus.NewRegistry()), Source: &Sequences{}},
		slog.New(slog.DiscardHandler),
	)

	// The broker side is a scripted channel: what this test observes is the order of the durable
	// write and the acknowledgement, not the transport.
	session := newFakeSession()
	pub, err := newSessionPublisher(session)
	if err != nil {
		t.Fatalf("newSessionPublisher() error = %v", err)
	}
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	event := heartbeatEvent("ev-durable", "dev-durable", 0.3, observedAt)
	delivery := deliveryFor(t, session, event, 1, HeartbeatRoutingKey(event.Region, event.Model))
	acker := &durableAcker{
		t: t, ctx: ctx, alerts: alerts, ledger: ledgerDocs,
		deviceID: event.DeviceID, eventID: event.EventID,
	}
	delivery.Acknowledger = acker

	if err := consumer.handle(ctx, pub, delivery); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if acker.acks != 1 {
		t.Fatalf("acknowledgements = %d, want the processed delivery acknowledged once", acker.acks)
	}
	if got := session.settled(); len(got) != 0 {
		t.Errorf("fake settlements = %+v, want the delivery's own acknowledger used", got)
	}
	if got := session.publishes(); len(got) != 0 {
		t.Errorf("publishes = %d, want none for a processed event", len(got))
	}

	// The order was checked at acknowledgement time; the store still holds both writes.
	if got := countDocuments(t, store, "device_alerts"); got != 1 {
		t.Errorf("alert documents = %d, want the side effect still durable", got)
	}
	record, ok := ledgerDocFor(t, ledgerDocs, HeartbeatQueue, event.EventID)
	if !ok || record.ProcessedAt == nil {
		t.Errorf("processed-events record = %+v (present %t), want it completed", record, ok)
	}
}

// ledgerDocFor reads one consumer's processed-events record, reporting whether it exists.
func ledgerDocFor(t *testing.T, ledger *mongo.Collection, consumer, eventID string) (ledgerRecord, bool) {
	t.Helper()

	var got ledgerRecord
	err := ledger.FindOne(context.Background(),
		bson.D{{Key: "_id", Value: LedgerID(consumer, eventID)}}).Decode(&got)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ledgerRecord{}, false
	}
	if err != nil {
		t.Fatalf("find ledger record for %s: %v", LedgerID(consumer, eventID), err)
	}
	return got, true
}

// TestTwoConsumersEachApplyTheEventOnce backs the scenario "Each consumer applies the event once"
// (heartbeat-consumption): two consumers bound to the same heartbeat events each apply their own
// side effect exactly once, and each keeps its own processed-events record.
func TestTwoConsumersEachApplyTheEventOnce(t *testing.T) {
	t.Parallel()

	store := mongotest.For(t)
	rabbit := rabbittest.For(t)
	topology := NewTopology(3, time.Second, time.Minute)

	// Two queues of the heartbeat family, one consumer each: the ledger is keyed by the consumer,
	// so one event delivered to both is applied once by each rather than once in total.
	const (
		alertingQueue  = "telemetrytest.heartbeat.alerting"
		analyticsQueue = "telemetrytest.heartbeat.analytics"
	)
	queues := []string{alertingQueue, analyticsQueue}
	ch := rabbit.Channel(t)
	for _, queue := range queues {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", queue, err)
		}
		if err := ch.QueueBind(queue, heartbeatFamilyKey, EventsExchange, false, nil); err != nil {
			t.Fatalf("bind %s to %s: %v", queue, EventsExchange, err)
		}
		rabbit.Purge(t, ch, queue)
		t.Cleanup(func() { rabbit.Purge(t, ch, queue) })
	}

	handlers := make(map[string]*scriptedHandler, len(queues))
	metrics := make(map[string]*Metrics, len(queues))
	for _, queue := range queues {
		handler := newScriptedHandler()
		handlers[queue] = handler
		metrics[queue] = NewMetrics(prometheus.NewRegistry())
		consumer := NewConsumer(
			rabbit.URL,
			WorkQueue{Name: queue, EventType: HeartbeatEventType},
			topology,
			NewLedger(store.DB.Collection("processed_events")),
			handler,
			ConsumerOptions{Prefetch: 4, Metrics: metrics[queue], Source: &Sequences{}},
			slog.New(slog.DiscardHandler),
		)
		runConsumer(t, consumer)
	}

	publisher := newBrokerPublisher(t, rabbit, topology)
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	event := heartbeatEvent("ev-two-consumers", "dev-two-consumers", 0.3, observedAt)
	publisher.publish(t, event, 1)

	for _, queue := range queues {
		handler := handlers[queue]
		awaitCondition(t, "the event to be applied by "+queue, func() bool {
			return handler.callCount(event.EventID) == 1
		})
	}

	// Each consumer keeps its own record of the event, completed once its side effect was durable.
	records := store.DB.Collection("processed_events")
	if got := countDocuments(t, store, "processed_events"); got != int64(len(queues)) {
		t.Errorf("processed-events documents = %d, want one per consumer", got)
	}
	for _, queue := range queues {
		record, ok := ledgerDocFor(t, records, queue, event.EventID)
		if !ok {
			t.Errorf("no processed-events record for consumer %s", queue)
			continue
		}
		if record.ProcessedAt == nil || record.DeviceID != event.DeviceID {
			t.Errorf("record for %s = %+v, want the event completed for the device", queue, record)
		}
	}

	// Applying the event is done: a second application would need a redelivery the ledger's
	// completed record suppresses.
	time.Sleep(testexec.Get(t).Wait(300 * time.Millisecond))
	for _, queue := range queues {
		if got := handlers[queue].callCount(event.EventID); got != 1 {
			t.Errorf("consumer %s applied the event %d times, want exactly 1", queue, got)
		}
		if got := testutil.ToFloat64(metrics[queue].Consumed.WithLabelValues(queue, outcomeProcessed)); got != 1 {
			t.Errorf("consumer %s processed outcomes = %v, want 1", queue, got)
		}
		if got := testutil.ToFloat64(metrics[queue].Consumed.WithLabelValues(queue, outcomeDuplicate)); got != 0 {
			t.Errorf("consumer %s duplicate outcomes = %v, want the event applied rather than skipped", queue, got)
		}
	}
}
