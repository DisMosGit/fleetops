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
)

// settled returns the settlements the consumer recorded on the fake session.
func (f *fakeSession) settled() []settlement {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]settlement(nil), f.settlements...)
}

// prefetch returns the last prefetch count the consumer set.
func (f *fakeSession) prefetch() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.qos
}

// fakeLedger scripts the dedup ledger's answers and records what the consumer asked it.
type fakeLedger struct {
	state    ClaimState
	claimErr error
	markErr  error

	mu     sync.Mutex
	claims []string
	marked []string
}

// Claim records the claim and reports the scripted state.
func (l *fakeLedger) Claim(_ context.Context, consumer, eventID, deviceID string, _ time.Time) (ClaimState, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claims = append(l.claims, consumer+":"+eventID+":"+deviceID)
	return l.state, l.claimErr
}

// MarkProcessed records the completion and returns the scripted error.
func (l *fakeLedger) MarkProcessed(_ context.Context, consumer, eventID string, _ time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.marked = append(l.marked, consumer+":"+eventID)
	return l.markErr
}

// claimed returns the recorded claims.
func (l *fakeLedger) claimed() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.claims...)
}

// completed returns the recorded completions.
func (l *fakeLedger) completed() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.marked...)
}

// fakeHandler records applied events and can fail them.
type fakeHandler struct {
	err error

	mu      sync.Mutex
	applied []Envelope
}

// Apply records the event and returns the scripted error.
func (h *fakeHandler) Apply(_ context.Context, env Envelope) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.applied = append(h.applied, env)
	return h.err
}

// events returns the applied events.
func (h *fakeHandler) events() []Envelope {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Envelope(nil), h.applied...)
}

// fakeSource reports a scripted latest published sequence per event type.
type fakeSource struct {
	latest map[string]uint64
}

// LatestSequence returns the scripted sequence of a type.
func (s fakeSource) LatestSequence(eventType string) uint64 { return s.latest[eventType] }

// newTestConsumer returns a consumer of the heartbeat work queue with a discard logger and
// metrics of its own, plus the metrics and their registry so a test can assert what was exposed.
func newTestConsumer(t *testing.T, ledger ClaimLedger, handler Handler, source SequenceSource) (*Consumer, *Metrics, *prometheus.Registry) {
	t.Helper()
	metrics, registry := newTestMetrics()
	work, err := NewTopology(3, time.Second, time.Minute).WorkQueue(HeartbeatQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", HeartbeatQueue, err)
	}
	consumer := NewConsumer(
		"amqp://guest:guest@localhost:5672/",
		work,
		NewTopology(3, time.Second, time.Minute),
		ledger,
		handler,
		ConsumerOptions{Prefetch: 8, Metrics: metrics, Source: source},
		slog.New(slog.DiscardHandler),
	)
	return consumer, metrics, registry
}

// handleOne builds one delivery of the test event and hands it to the consumer, returning the
// consumer's error and the session it settled on.
func handleOne(t *testing.T, consumer *Consumer, session *fakeSession, event Envelope, attempt int) error {
	t.Helper()
	pub, err := newSessionPublisher(session)
	if err != nil {
		t.Fatalf("newSessionPublisher() error = %v", err)
	}
	delivery := deliveryFor(t, session, event, attempt, HeartbeatRoutingKey(event.Region, event.Model))
	return consumer.handle(context.Background(), pub, delivery)
}

// TestConsumerProcessesAClaimedEvent pins the happy path: claim, apply, complete, and only then
// acknowledge.
func TestConsumerProcessesAClaimedEvent(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	if got := ledger.claimed(); len(got) != 1 || got[0] != HeartbeatQueue+":"+testEvent().EventID+":"+testEvent().DeviceID {
		t.Errorf("claims = %v, want one claim of the event for this consumer", got)
	}
	if got := ledger.completed(); len(got) != 1 {
		t.Errorf("completions = %v, want the claim marked processed", got)
	}
	if got := handler.events(); len(got) != 1 {
		t.Errorf("applied events = %d, want 1", len(got))
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the delivery acknowledged", got)
	}
	if got := session.publishes(); len(got) != 0 {
		t.Errorf("publishes = %d, want none for a processed event", len(got))
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeProcessed)); got != 1 {
		t.Errorf("processed outcomes = %v, want 1", got)
	}
}

// TestConsumerAcknowledgesDuplicatesWithoutApplying pins deduplication: an event the ledger
// already processed is acknowledged and counted as a duplicate, with no second side effect.
func TestConsumerAcknowledgesDuplicatesWithoutApplying(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimProcessed}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	if got := handler.events(); len(got) != 0 {
		t.Errorf("applied events = %d, want none for a duplicate", len(got))
	}
	if got := ledger.completed(); len(got) != 0 {
		t.Errorf("completions = %v, want none for a duplicate", got)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the duplicate acknowledged", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDuplicate)); got != 1 {
		t.Errorf("duplicate outcomes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeProcessed)); got != 0 {
		t.Errorf("processed outcomes = %v, want none for a duplicate", got)
	}
}

// TestConsumerResumesAnUnfinishedClaim pins that a claim without a completion is work to
// resume, not a duplicate to skip.
func TestConsumerResumesAnUnfinishedClaim(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimUnfinished}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 2); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	if got := handler.events(); len(got) != 1 {
		t.Errorf("applied events = %d, want the unfinished event applied again", len(got))
	}
	if got := ledger.completed(); len(got) != 1 {
		t.Errorf("completions = %v, want the claim completed", got)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the delivery acknowledged", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeProcessed)); got != 1 {
		t.Errorf("processed outcomes = %v, want 1", got)
	}
}

// TestConsumerReportsACompletionFailureIntoTheRetryPath pins that a completion that did not
// land is retried rather than acknowledged: the side effect may be durable, but the ledger does
// not know it yet.
func TestConsumerReportsACompletionFailureIntoTheRetryPath(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimNew, markErr: errors.New("mongo unavailable")}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the delivery acknowledged after the retry republish", got)
	}
	publishes := session.publishes()
	if len(publishes) != 1 {
		t.Fatalf("publishes = %d, want one retry republish", len(publishes))
	}
	if publishes[0].exchange != RetryExchange {
		t.Errorf("retry exchange = %q, want %q", publishes[0].exchange, RetryExchange)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 1 {
		t.Errorf("retry outcomes = %v, want 1", got)
	}
}

// TestConsumerRetriesAFailedDeliveryIntoTheAttemptsQueue pins the retry path: the failed attempt
// is republished to its own retry queue with the next attempt stamped on it, and the delivery is
// acknowledged only after that republish is confirmed.
func TestConsumerRetriesAFailedDeliveryIntoTheAttemptsQueue(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("handler failed")}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	publishes := session.publishes()
	if len(publishes) != 1 {
		t.Fatalf("publishes = %d, want one retry republish", len(publishes))
	}
	if publishes[0].exchange != RetryExchange {
		t.Errorf("retry exchange = %q, want %q", publishes[0].exchange, RetryExchange)
	}
	if want := HeartbeatQueue + ".retry.1"; publishes[0].key != want {
		t.Errorf("retry key = %q, want the queue of the attempt that failed (%q)", publishes[0].key, want)
	}
	if got := attemptOf(amqp.Delivery{Headers: publishes[0].msg.Headers}); got != 2 {
		t.Errorf("retry attempt header = %d, want the next attempt 2", got)
	}
	if got := mustDecode(t, publishes[0].msg.Body).EventID; got != testEvent().EventID {
		t.Errorf("retry event id = %q, want the original %q", got, testEvent().EventID)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the delivery acknowledged after the confirmed republish", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 1 {
		t.Errorf("retry outcomes = %v, want 1", got)
	}
}

// TestConsumerDeadLettersAnExhaustedDelivery pins the terminal path: on the final permitted
// attempt the event goes to the dead-letter queue with its attempt count and reason.
func TestConsumerDeadLettersAnExhaustedDelivery(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("handler failed")}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 3); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}

	publishes := session.publishes()
	if len(publishes) != 1 {
		t.Fatalf("publishes = %d, want one dead letter", len(publishes))
	}
	if publishes[0].exchange != DeadLetterExchange {
		t.Errorf("dead-letter exchange = %q, want %q", publishes[0].exchange, DeadLetterExchange)
	}
	if want := HeartbeatQueue + ".dlq"; publishes[0].key != want {
		t.Errorf("dead-letter key = %q, want %q", publishes[0].key, want)
	}
	if got := publishes[0].msg.Headers[attemptHeader]; got != int32(3) {
		t.Errorf("dead-letter attempt header = %v, want the attempts made (3)", got)
	}
	if reason, _ := publishes[0].msg.Headers[deadLetterReasonHeader].(string); !strings.Contains(reason, "handler failed") {
		t.Errorf("dead-letter reason = %v, want the failure reason", publishes[0].msg.Headers[deadLetterReasonHeader])
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
		t.Errorf("settlements = %+v, want the delivery acknowledged", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDeadLetter)); got != 1 {
		t.Errorf("dead-letter outcomes = %v, want 1", got)
	}
}

// TestConsumerDeadLettersMalformedPayloadsWithoutRetrying pins that an event which cannot be
// understood is dead-lettered immediately: no retry attempts, no side effect, no ledger entry.
func TestConsumerDeadLettersMalformedPayloadsWithoutRetrying(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		attempt int
		want    string
	}{
		{name: "payload that does not decode", body: "{not json", attempt: 1, want: "decode event payload"},
		{
			name:    "payload without an event id",
			body:    `{"schema_version":1,"event_type":"heartbeat","occurred_at":"2026-03-04T05:06:07Z"}`,
			attempt: 2,
			want:    "event id required",
		},
		{
			name:    "unsupported schema version",
			body:    `{"schema_version":9,"event_id":"ev-1","event_type":"heartbeat","occurred_at":"2026-03-04T05:06:07Z"}`,
			attempt: 1,
			want:    "unsupported schema version 9",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := &fakeHandler{}
			ledger := &fakeLedger{state: ClaimNew}
			consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
			session := newFakeSession()
			pub, err := newSessionPublisher(session)
			if err != nil {
				t.Fatalf("newSessionPublisher() error = %v", err)
			}
			delivery := deliveryFor(t, session, testEvent(), tc.attempt, "heartbeat.eu-west.v3")
			delivery.Body = []byte(tc.body)

			if err := consumer.handle(context.Background(), pub, delivery); err != nil {
				t.Fatalf("handle() error = %v, want nil", err)
			}

			publishes := session.publishes()
			if len(publishes) != 1 {
				t.Fatalf("publishes = %d, want exactly one dead letter and no retry", len(publishes))
			}
			if publishes[0].exchange != DeadLetterExchange {
				t.Errorf("exchange = %q, want the dead-letter exchange", publishes[0].exchange)
			}
			reason, _ := publishes[0].msg.Headers[deadLetterReasonHeader].(string)
			if !strings.Contains(reason, tc.want) {
				t.Errorf("dead-letter reason = %q, want it to contain %q", reason, tc.want)
			}
			if got := ledger.claimed(); len(got) != 0 {
				t.Errorf("claims = %v, want no ledger entry for an unusable event", got)
			}
			if got := handler.events(); len(got) != 0 {
				t.Errorf("applied events = %d, want none for an unusable event", len(got))
			}
			if got := session.settled(); len(got) != 1 || got[0].kind != "ack" {
				t.Errorf("settlements = %+v, want the dead-lettered delivery acknowledged", got)
			}
			if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDeadLetter)); got != 1 {
				t.Errorf("dead-letter outcomes = %v, want 1", got)
			}
		})
	}
}

// TestConsumerRejectsWhenTheRetryRepublishIsRefused pins that a delivery which cannot be
// republished is rejected so the broker's dead-letter path takes it: no requeue loop, no loss.
func TestConsumerRejectsWhenTheRetryRepublishIsRefused(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("handler failed")}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession().script(verdict{ack: false})

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Errorf("handle() error = %v, want nil while the session still works", err)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "reject" {
		t.Errorf("settlements = %+v, want the delivery rejected onto the dead-letter path", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDeadLetter)); got != 1 {
		t.Errorf("dead-letter outcomes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 0 {
		t.Errorf("retry outcomes = %v, want none", got)
	}
}

// TestConsumerReportsALostSessionWhenTheRetryRepublishCannotBePublished pins the difference
// between a broker refusal and a dead channel: the refused delivery still goes to the
// dead-letter path, and the dead session ends the consumer's loop.
func TestConsumerReportsALostSessionWhenTheRetryRepublishCannotBePublished(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("handler failed")}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession().script(verdict{err: errors.New("channel closed")})

	err := handleOne(t, consumer, session, testEvent(), 1)
	if !errors.Is(err, errSessionLost) {
		t.Errorf("handle() error = %v, want the lost session reported", err)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "reject" {
		t.Errorf("settlements = %+v, want the delivery rejected onto the dead-letter path", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDeadLetter)); got != 1 {
		t.Errorf("dead-letter outcomes = %v, want 1", got)
	}
}

// TestConsumerRejectsWhenTheDeadLetterPublishFails pins the last resort: even a failed dead
// letter leaves the delivery to the broker's own dead-letter path.
func TestConsumerRejectsWhenTheDeadLetterPublishFails(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("handler failed")}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession().script(verdict{ack: false})

	if err := handleOne(t, consumer, session, testEvent(), 3); err != nil {
		t.Fatalf("handle() error = %v, want nil while the session still works", err)
	}
	if got := session.settled(); len(got) != 1 || got[0].kind != "reject" {
		t.Errorf("settlements = %+v, want the delivery rejected", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDeadLetter)); got != 1 {
		t.Errorf("dead-letter outcomes = %v, want 1", got)
	}
}

// TestConsumerRetriesAClaimFailure pins that a ledger failure is a transient failure: the event
// is retried rather than applied without a claim.
func TestConsumerRetriesAClaimFailure(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{claimErr: errors.New("mongo unavailable")}
	consumer, metrics, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	if err := handleOne(t, consumer, session, testEvent(), 1); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	if got := handler.events(); len(got) != 0 {
		t.Errorf("applied events = %d, want none without a claim", len(got))
	}
	if got := session.publishes(); len(got) != 1 || got[0].exchange != RetryExchange {
		t.Errorf("publishes = %+v, want one retry republish", got)
	}
	if got := testutil.ToFloat64(metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeRetry)); got != 1 {
		t.Errorf("retry outcomes = %v, want 1", got)
	}
}

// TestConsumerServeBoundsInFlightWorkAndStopsOnLoss pins the loop: prefetch is set from the
// configuration, and a lost session ends the loop so the supervisor can reconnect.
func TestConsumerServeBoundsInFlightWorkAndStopsOnLoss(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{state: ClaimNew}
	consumer, _, _ := newTestConsumer(t, ledger, &fakeHandler{}, fakeSource{})
	session := newFakeSession()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.serve(ctx, session) }()

	deadline := time.Now().Add(5 * time.Second)
	for session.prefetch() != 8 {
		if time.Now().After(deadline) {
			t.Fatalf("prefetch = %d, want the configured 8", session.prefetch())
		}
		time.Sleep(time.Millisecond)
	}

	session.lose()
	select {
	case err := <-done:
		if !errors.Is(err, errSessionLost) {
			t.Errorf("serve() error = %v, want the lost session reported", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve() did not return after the session was lost")
	}
}

// TestConsumerServeConsumesAndAcknowledges pins the wired loop end to end on a fake channel: a
// delivery fed to the consumer is applied and acknowledged.
func TestConsumerServeConsumesAndAcknowledges(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimNew}
	consumer, _, _ := newTestConsumer(t, ledger, handler, fakeSource{})
	session := newFakeSession()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.serve(ctx, session) }()

	delivery := deliveryFor(t, session, testEvent(), 1, "heartbeat.eu-west.v3")
	session.deliveries <- delivery

	deadline := time.Now().Add(5 * time.Second)
	for len(session.settled()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the delivered event was never settled")
		}
		time.Sleep(time.Millisecond)
	}
	if got := handler.events(); len(got) != 1 {
		t.Errorf("applied events = %d, want 1", len(got))
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serve() error = %v, want nil on shutdown", err)
	}
}

// TestConsumerLagPinsTheDistanceToTheLatestPublishedEvent pins the lag arithmetic: zero on
// catch-up, monotone under duplicates and older events, and reported for this consumer.
func TestConsumerLagPinsTheDistanceToTheLatestPublishedEvent(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	ledger := &fakeLedger{state: ClaimNew}
	source := fakeSource{latest: map[string]uint64{HeartbeatEventType: 10}}
	consumer, _, registry := newTestConsumer(t, ledger, handler, source)

	if got := consumer.Lag(); got != 10 {
		t.Errorf("Lag() before any event = %v, want every published event counted", got)
	}

	events := []struct {
		sequence uint64
		wantLag  float64
	}{
		{sequence: 4, wantLag: 6},
		{sequence: 7, wantLag: 3},
		{sequence: 7, wantLag: 3},
		{sequence: 2, wantLag: 3},
		{sequence: 10, wantLag: 0},
		{sequence: 12, wantLag: 0},
	}
	for _, tc := range events {
		consumer.observe(tc.sequence)
		if got := consumer.Lag(); got != tc.wantLag {
			t.Errorf("Lag() after sequence %d = %v, want %v", tc.sequence, got, tc.wantLag)
		}
	}

	// The gauge is registered per consumer and reports the same value at scrape time.
	if got := lagFrom(t, registry, HeartbeatQueue); got != 0 {
		t.Errorf("exposed lag = %v, want the consumer's lag", got)
	}
	source.latest[HeartbeatEventType] = 20
	if got := consumer.Lag(); got != 8 {
		t.Errorf("Lag() after production advanced = %v, want 8", got)
	}
	if got := lagFrom(t, registry, HeartbeatQueue); got != 8 {
		t.Errorf("exposed lag = %v, want the lag read at scrape time", got)
	}
}

// TestLedgerIDIsPerConsumerAndEvent pins the ledger key: one document per consumer and event,
// and two consumers never share one.
func TestLedgerIDIsPerConsumerAndEvent(t *testing.T) {
	t.Parallel()

	first := LedgerID("fleetops.heartbeat.alerting", "ev-1")
	second := LedgerID("fleetops.heartbeat.analytics", "ev-1")
	if first == second {
		t.Errorf("LedgerID() collides across consumers: %q", first)
	}
	if !strings.Contains(first, "ev-1") || !strings.HasPrefix(first, "fleetops.heartbeat.alerting") {
		t.Errorf("LedgerID() = %q, want the consumer and the event id", first)
	}
}

// TestClaimStateNames pins the states a claim attempt can report.
func TestClaimStateNames(t *testing.T) {
	t.Parallel()

	states := map[ClaimState]string{
		ClaimNew:        "new",
		ClaimUnfinished: "unfinished",
		ClaimProcessed:  "processed",
		ClaimState(42):  "unknown",
	}
	for state, want := range states {
		if got := state.String(); got != want {
			t.Errorf("ClaimState(%d).String() = %q, want %q", state, got, want)
		}
	}
}

// TestConsumerWiredWorkQueueIsTheHeartbeatQueue pins the identity the ledger and metrics use.
func TestConsumerWiredWorkQueueIsTheHeartbeatQueue(t *testing.T) {
	t.Parallel()

	consumer, _, _ := newTestConsumer(t, &fakeLedger{state: ClaimNew}, &fakeHandler{}, fakeSource{})
	if consumer.work.Name != HeartbeatQueue {
		t.Errorf("consumer work queue = %q, want %q", consumer.work.Name, HeartbeatQueue)
	}
	if consumer.work.EventType != HeartbeatEventType {
		t.Errorf("consumer event type = %q, want %q", consumer.work.EventType, HeartbeatEventType)
	}
	if consumer.prefetch != 8 {
		t.Errorf("consumer prefetch = %d, want the configured 8", consumer.prefetch)
	}
}
