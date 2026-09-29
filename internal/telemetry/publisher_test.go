package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"

	"github.com/DisMosGit/fleetops/internal/devices"
)

// newTestPublisher returns a publisher with a discard logger.
func newTestPublisher(bufferSize int, metrics *Metrics) *Publisher {
	return NewPublisher(
		"amqp://guest:guest@localhost:5672/",
		NewTopology(3, time.Second, time.Minute),
		bufferSize,
		metrics,
		slog.New(slog.DiscardHandler),
	)
}

// awaitPublishes waits until a fake session has recorded want publishes.
func awaitPublishes(t *testing.T, session *fakeSession, want int) []sentMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		publishes := session.publishes()
		if len(publishes) == want {
			return publishes
		}
		if time.Now().After(deadline) {
			t.Fatalf("published %d events, want %d", len(publishes), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPublisherPublishesAcceptedHeartbeats pins the published message: the sequence orders
// events, the routing key follows the key grammar, and every event is persistent JSON whose
// identity rides on the message properties.
func TestPublisherPublishesAcceptedHeartbeats(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	publisher := newTestPublisher(8, metrics)
	session := newFakeSession()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- publisher.serve(ctx, session) }()

	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	occurred := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	for i := range 3 {
		publisher.Handle(ctx, heartbeat(fmt.Sprintf("ev-%d", i), rec.ID, occurred), rec)
	}
	publishes := awaitPublishes(t, session, 3)

	for i, sent := range publishes {
		if want := HeartbeatRoutingKey("eu-west", "v3"); sent.key != want {
			t.Errorf("publish %d routing key = %q, want %q", i, sent.key, want)
		}
		if sent.exchange != EventsExchange {
			t.Errorf("publish %d exchange = %q, want %q", i, sent.exchange, EventsExchange)
		}
		if !sent.mandatory {
			t.Errorf("publish %d was not mandatory, want unroutable events returned", i)
		}
		event := mustDecode(t, sent.msg.Body)
		if want := uint64(i + 1); event.Sequence != want {
			t.Errorf("publish %d sequence = %d, want %d", i, event.Sequence, want)
		}
		if event.EventID != fmt.Sprintf("ev-%d", i) {
			t.Errorf("publish %d event id = %q, want ev-%d", i, event.EventID, i)
		}
		if event.PublishedAt.Before(event.OccurredAt) {
			t.Errorf("publish %d published_at %v is before occurred_at %v", i, event.PublishedAt, event.OccurredAt)
		}
		if want := (Payload{CPU: 0.5, Mem: 0.4, Health: 0.9, CurrentFW: "2.0.0"}); event.Payload != want {
			t.Errorf("publish %d payload = %+v, want %+v", i, event.Payload, want)
		}
		if got := sent.msg.Headers[attemptHeader]; got != int32(1) {
			t.Errorf("publish %d attempt header = %v, want the first attempt", i, got)
		}
	}

	if got := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)); got != 3 {
		t.Errorf("fleetops_events_published_total = %v, want 3", got)
	}
	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonBufferFull)); got != 0 {
		t.Errorf("buffer-full drops = %v, want none", got)
	}
	if got := publisher.LatestSequence(HeartbeatEventType); got != 3 {
		t.Errorf("LatestSequence() = %d, want 3", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serve() error = %v, want nil on shutdown", err)
	}
}

// TestPublisherShedsWhenBufferFull pins the load-shedding contract: Handle never blocks and
// never fails, and every shed event is counted rather than dropped silently.
func TestPublisherShedsWhenBufferFull(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	publisher := newTestPublisher(1, metrics)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	ctx := context.Background()

	// No session is running, so the buffer of one holds the first event and sheds the rest.
	start := time.Now()
	for i := range 3 {
		publisher.Handle(ctx, heartbeat(fmt.Sprintf("ev-%d", i), rec.ID, time.Now()), rec)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Handle() took %s with an unavailable broker, want an immediate return", elapsed)
	}

	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonBufferFull)); got != 2 {
		t.Errorf("buffer-full drops = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)); got != 0 {
		t.Errorf("published events = %v, want none without a broker", got)
	}
}

// TestPublisherCountsBrokerRefusals pins the three ways a publish is known to have failed, each
// with its own reason, so a confirmed publish is never confused with one the broker refused.
func TestPublisherCountsBrokerRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		answer     verdict
		wantReason string
		wantLost   bool
	}{
		{
			name:       "broker rejects the publish",
			answer:     verdict{ack: false},
			wantReason: dropReasonRejected,
		},
		{
			name:       "no queue is bound to the routing key",
			answer:     verdict{ack: true, returned: true},
			wantReason: dropReasonUnroutable,
		},
		{
			name:       "the channel dies before the verdict",
			answer:     verdict{err: errors.New("channel closed")},
			wantReason: dropReasonUnreachable,
			wantLost:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metrics, _ := newTestMetrics()
			publisher := newTestPublisher(4, metrics)
			session := newFakeSession().script(tc.answer)
			rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- publisher.serve(ctx, session) }()
			publisher.Handle(ctx, heartbeat("ev-1", rec.ID, time.Now()), rec)
			awaitPublishes(t, session, 1)

			if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, tc.wantReason)); got != 1 {
				t.Errorf("drops with reason %s = %v, want 1", tc.wantReason, got)
			}
			if got := testutil.ToFloat64(metrics.Published.WithLabelValues(HeartbeatEventType)); got != 0 {
				t.Errorf("published events = %v, want none", got)
			}

			if tc.wantLost {
				select {
				case err := <-done:
					if !errors.Is(err, errSessionLost) {
						t.Errorf("serve() error = %v, want the session to be reported lost", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("serve() did not return after a lost publish")
				}
				return
			}
			cancel()
			if err := <-done; err != nil {
				t.Errorf("serve() error = %v, want nil on shutdown", err)
			}
		})
	}
}

// TestPublisherKeepsBufferedEventsForTheNextSession pins that a broker outage delays events
// rather than losing them: buffered events are published once a session is available again.
func TestPublisherKeepsBufferedEventsForTheNextSession(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	publisher := newTestPublisher(4, metrics)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	ctx := context.Background()

	// With no session at all the events wait in the buffer instead of being dropped.
	for i := range 3 {
		publisher.Handle(ctx, heartbeat(fmt.Sprintf("ev-%d", i), rec.ID, time.Now()), rec)
	}
	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonBufferFull)); got != 0 {
		t.Fatalf("buffer-full drops = %v, want none while the buffer has room", got)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	session := newFakeSession()
	done := make(chan error, 1)
	go func() { done <- publisher.serve(runCtx, session) }()
	awaitPublishes(t, session, 3)

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serve() error = %v, want nil on shutdown", err)
	}
}

// TestPublisherPublishesWhatTheLostSessionLeft pins that a lost session does not strand the
// buffer: events accepted while the publisher had no session are published by the next one.
func TestPublisherPublishesWhatTheLostSessionLeft(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	publisher := newTestPublisher(4, metrics)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newFakeSession()
	firstDone := make(chan error, 1)
	go func() { firstDone <- publisher.serve(ctx, first) }()
	publisher.Handle(ctx, heartbeat("ev-1", rec.ID, time.Now()), rec)
	awaitPublishes(t, first, 1)
	first.lose()
	if err := <-firstDone; !errors.Is(err, errSessionLost) {
		t.Fatalf("serve() error = %v, want the lost session reported", err)
	}

	// Two more events are accepted while the publisher has no session; the next session must
	// publish them instead of starting from an empty buffer.
	publisher.Handle(ctx, heartbeat("ev-2", rec.ID, time.Now()), rec)
	publisher.Handle(ctx, heartbeat("ev-3", rec.ID, time.Now()), rec)

	second := newFakeSession()
	secondDone := make(chan error, 1)
	go func() { secondDone <- publisher.serve(ctx, second) }()
	publishes := awaitPublishes(t, second, 2)

	var ids []string
	for _, sent := range publishes {
		ids = append(ids, mustDecode(t, sent.msg.Body).EventID)
	}
	if got := strings.Join(ids, ","); got != "ev-2,ev-3" {
		t.Errorf("published event ids = %q, want ev-2,ev-3", got)
	}

	cancel()
	if err := <-secondDone; err != nil {
		t.Errorf("serve() error = %v, want nil on shutdown", err)
	}
}

// TestPublisherLogsTheFirstDropOfABurst keeps a broker outage visible without one log line per
// event: the first drop is logged, the rest of the burst is counted silently, and a confirmed
// publish starts a new burst.
func TestPublisherLogsTheFirstDropOfABurst(t *testing.T) {
	t.Parallel()

	var logged bytes.Buffer
	metrics, _ := newTestMetrics()
	publisher := NewPublisher(
		"amqp://guest:guest@localhost:5672/",
		NewTopology(3, time.Second, time.Minute),
		1,
		metrics,
		slog.New(slog.NewTextHandler(&logged, nil)),
	)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	ctx := context.Background()
	for i := range 4 {
		publisher.Handle(ctx, heartbeat(fmt.Sprintf("ev-%d", i), rec.ID, time.Now()), rec)
	}
	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonBufferFull)); got != 3 {
		t.Fatalf("buffer-full drops = %v, want 3", got)
	}
	if got := strings.Count(logged.String(), "dropping an event"); got != 1 {
		t.Errorf("drop log lines = %d, want exactly one for the burst", got)
	}
}

// TestPublisherReportsUnusableHeartbeats pins that a heartbeat the fan-out cannot represent is
// counted rather than published as an empty event.
func TestPublisherReportsUnusableHeartbeats(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	publisher := newTestPublisher(4, metrics)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	ctx := context.Background()

	publisher.Handle(ctx, heartbeat("", rec.ID, time.Now()), rec)
	// A heartbeat without a measurement time is what the ingest writer refuses; reaching the
	// publisher directly it must be counted rather than published.
	publisher.Handle(ctx, &agentv1.Heartbeat{EventId: "ev-no-ts", DeviceId: rec.ID}, rec)

	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonUnusable)); got != 2 {
		t.Errorf("unusable drops = %v, want 2", got)
	}
}
