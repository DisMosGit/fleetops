package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// rollbackAnnouncement returns a complete announcement of one phase, the shape the announcing
// activity builds.
func rollbackAnnouncement(phase RollbackPhase) RollbackEvent {
	occurred := time.Unix(1700000000, 0).UTC()
	event := NewRollbackEvent("ro-1", phase, occurred)
	event.Rollout = RolloutAnnouncement{
		RolloutID:       "ro-1",
		FirmwareID:      "fw-1",
		FirmwareVersion: "2.0.0",
		Region:          "eu-west",
		Model:           "oak-s3",
		WaveID:          "ro-1-w1-5",
		Outcome:         "unhealthy_wave",
		Decision: &RollbackDecision{
			Verdict: "unhealthy", SuccessRatio: 0.4, SampleSize: 120,
			WindowStart: occurred.Add(-5 * time.Minute), WindowEnd: occurred,
		},
		PlanSteps: 4, PlanDevices: 10,
	}
	if phase == RollbackCompleted {
		event.Progress = &RollbackProgress{
			Restored:            9,
			Unreported:          1,
			Corrected:           9,
			Inventory:           []FirmwareInventoryEntry{{Version: "1.0.0", Devices: 9}},
			UnrestoredDeviceIDs: []string{"dev-9"},
			Steps: []RollbackStepOutcome{{
				Kind: "downgrade", WaveID: "ro-1-w1-5", Status: "completed",
				Devices: 10, Restored: 9, Unreported: 1,
			}},
		}
	}
	return event
}

// TestRollbackEventIdentity pins the identity a republished announcement repeats: it is derived from
// the rollout and the phase, so the two phases of one rollback are distinct events and a retry of
// one phase is the same event rather than a second one.
func TestRollbackEventIdentity(t *testing.T) {
	t.Parallel()

	started := NewRollbackEvent("ro-1", RollbackStarted, time.Unix(1700000000, 0).UTC())
	completed := NewRollbackEvent("ro-1", RollbackCompleted, time.Unix(1700000000, 0).UTC())

	if started.EventID != "rollback-ro-1-started" {
		t.Errorf("started event id = %q, want rollback-ro-1-started", started.EventID)
	}
	if completed.EventID != "rollback-ro-1-completed" {
		t.Errorf("completed event id = %q, want rollback-ro-1-completed", completed.EventID)
	}
	if started.EventID == completed.EventID {
		t.Error("the two phases of one rollback share an event id")
	}
	if started.Rollout.RolloutID != "ro-1" || completed.Rollout.RolloutID != "ro-1" {
		t.Error("the two phases name different rollouts")
	}
	// A second construction of the same announcement — what a retried step does — repeats the
	// identity, so a consumer that deduplicates by it applies the event once.
	if again := NewRollbackEvent("ro-1", RollbackStarted, time.Now()); again.EventID != started.EventID {
		t.Errorf("rebuilt event id = %q, want the stable %q", again.EventID, started.EventID)
	}
}

// TestRollbackEventRoundTrip pins the event's wire contract: it encodes and decodes losslessly,
// carries its contract version, and travels as a persistent message whose properties name it
// without decoding the body.
func TestRollbackEventRoundTrip(t *testing.T) {
	t.Parallel()

	for _, phase := range []RollbackPhase{RollbackStarted, RollbackCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()

			event := rollbackAnnouncement(phase)
			data, err := event.Encode()
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			decoded, err := DecodeRollbackEvent(data)
			if err != nil {
				t.Fatalf("DecodeRollbackEvent() error = %v", err)
			}
			if diff := cmp.Diff(event, decoded); diff != "" {
				t.Errorf("round trip lost the event (-want +got):\n%s", diff)
			}
			if decoded.SchemaVersion != RollbackEventSchemaVersion {
				t.Errorf("schema version = %d, want %d", decoded.SchemaVersion, RollbackEventSchemaVersion)
			}

			key := RollbackRoutingKey(phase)
			msg, err := event.Message(key)
			if err != nil {
				t.Fatalf("Message() error = %v", err)
			}
			if msg.MessageId != event.EventID || msg.Type != RollbackEventType {
				t.Errorf("message identity = %q/%q, want %q/%q",
					msg.MessageId, msg.Type, event.EventID, RollbackEventType)
			}
			if msg.DeliveryMode != 2 { // amqp.Persistent
				t.Errorf("delivery mode = %d, want persistent", msg.DeliveryMode)
			}
			if msg.Timestamp != event.OccurredAt {
				t.Errorf("message timestamp = %v, want the announcement's %v", msg.Timestamp, event.OccurredAt)
			}
			if got := msg.Headers[originalRoutingKeyHeader]; got != key {
				t.Errorf("original routing key header = %v, want %q", got, key)
			}
			if got := msg.Headers[schemaVersionHeader]; got != int32(RollbackEventSchemaVersion) {
				t.Errorf("schema version header = %v, want %d", got, RollbackEventSchemaVersion)
			}
		})
	}

	t.Run("the started phase reports nothing achieved", func(t *testing.T) {
		t.Parallel()

		event := rollbackAnnouncement(RollbackStarted)
		if event.Progress != nil {
			t.Errorf("started event carries progress %+v, want none", event.Progress)
		}
	})
}

// TestRollbackEventValidation pins what the notifier refuses before it publishes: an event without
// an identity, a rollout, a known phase, or an occurrence time is not a rollback announcement.
func TestRollbackEventValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		corrupt func(*RollbackEvent)
		want    string
	}{
		{
			name:    "a future contract version is refused",
			corrupt: func(e *RollbackEvent) { e.SchemaVersion = RollbackEventSchemaVersion + 1 },
			want:    "unsupported rollback schema version",
		},
		{
			name:    "a missing event id is refused",
			corrupt: func(e *RollbackEvent) { e.EventID = "" },
			want:    "event id required",
		},
		{
			name:    "a missing rollout is refused",
			corrupt: func(e *RollbackEvent) { e.Rollout.RolloutID = "" },
			want:    "rollout id required",
		},
		{
			name:    "an unknown phase is refused",
			corrupt: func(e *RollbackEvent) { e.Phase = "halfway" },
			want:    "unknown rollback phase",
		},
		{
			name:    "a missing occurrence time is refused",
			corrupt: func(e *RollbackEvent) { e.OccurredAt = time.Time{} },
			want:    "occurrence time required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			event := rollbackAnnouncement(RollbackStarted)
			tc.corrupt(&event)
			err := event.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() error = %v, want it to name %q", err, tc.want)
			}
			if _, err := event.Message(RollbackRoutingKey(event.Phase)); err == nil {
				t.Error("Message() on an invalid event = nil error, want it refused")
			}
			if _, err := DecodeRollbackEvent([]byte(`{"event_id":"x"}`)); err == nil {
				t.Error("DecodeRollbackEvent() on an incomplete payload = nil error, want it refused")
			}
			if _, err := DecodeRollbackEvent([]byte(`not json`)); err == nil {
				t.Error("DecodeRollbackEvent() on a non-JSON payload = nil error, want it refused")
			}
		})
	}
}

// TestNotifierPublishesConfirmedAnnouncements pins the notifier's successful path: the event travels
// to the events exchange under its phase's key, mandatory and persistent, and the caller learns the
// broker took it.
func TestNotifierPublishesConfirmedAnnouncements(t *testing.T) {
	t.Parallel()

	notifier := newTestNotifier()
	session := newFakeSession().script(verdict{ack: true})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- notifier.serve(ctx, session) }()

	event := rollbackAnnouncement(RollbackStarted)
	if err := notifier.Announce(ctx, event); err != nil {
		t.Fatalf("Announce() error = %v", err)
	}

	publishes := session.publishes()
	if len(publishes) != 1 {
		t.Fatalf("publishes = %d, want 1", len(publishes))
	}
	sent := publishes[0]
	if sent.exchange != EventsExchange {
		t.Errorf("exchange = %q, want %q", sent.exchange, EventsExchange)
	}
	if sent.key != RollbackRoutingKey(RollbackStarted) {
		t.Errorf("routing key = %q, want %q", sent.key, RollbackRoutingKey(RollbackStarted))
	}
	if !sent.mandatory {
		t.Error("publish was not mandatory: an unroutable announcement would vanish")
	}
	if sent.msg.DeliveryMode != 2 { // amqp.Persistent
		t.Errorf("delivery mode = %d, want persistent", sent.msg.DeliveryMode)
	}
	if sent.msg.MessageId != event.EventID || sent.msg.Type != RollbackEventType {
		t.Errorf("message identity = %q/%q, want %q/%q",
			sent.msg.MessageId, sent.msg.Type, event.EventID, RollbackEventType)
	}
	decoded, err := DecodeRollbackEvent(sent.msg.Body)
	if err != nil {
		t.Fatalf("decode the published body: %v", err)
	}
	if diff := cmp.Diff(event, decoded); diff != "" {
		t.Errorf("published event mismatch (-want +got):\n%s", diff)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serve() error = %v, want nil on shutdown", err)
	}
}

// TestNotifierReportsWhatTheBrokerDidWithTheEvent pins that a verdict the announcing step must
// retry is an error: a rejection, a message no queue is bound to, and a session lost under the
// publish are all reported rather than swallowed.
func TestNotifierReportsWhatTheBrokerDidWithTheEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		verdict verdict
		want    string
	}{
		{
			name:    "a rejected publication is an error",
			verdict: verdict{ack: false},
			want:    "broker rejected rollback announcement",
		},
		{
			name:    "an unroutable publication is an error",
			verdict: verdict{ack: true, returned: true},
			want:    "no queue is bound to rollout.notification.rollback.started",
		},
		{
			name:    "a lost session is an error",
			verdict: verdict{err: errors.New("connection closed")},
			want:    "connection closed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			notifier := newTestNotifier()
			session := newFakeSession().script(tc.verdict)
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { done <- notifier.serve(ctx, session) }()

			err := notifier.Announce(ctx, rollbackAnnouncement(RollbackStarted))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Announce() error = %v, want it to name %q", err, tc.want)
			}
			cancel()
			// A transport failure is the end of the session: serve reports it so the supervisor
			// reconnects, while a broker verdict leaves the session alive.
			serveErr := <-done
			if tc.verdict.err != nil && !errors.Is(serveErr, errSessionLost) {
				t.Errorf("serve() error = %v, want the lost session", serveErr)
			}
			if tc.verdict.err == nil && serveErr != nil {
				t.Errorf("serve() error = %v, want nil after a broker verdict", serveErr)
			}
		})
	}
}

// TestNotifierRefusesAnEventItCannotPublish pins that an event the notifier cannot represent never
// reaches the broker: nothing is published and the caller is told why.
func TestNotifierRefusesAnEventItCannotPublish(t *testing.T) {
	t.Parallel()

	notifier := newTestNotifier()
	session := newFakeSession()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = notifier.serve(ctx, session) }()

	event := rollbackAnnouncement(RollbackStarted)
	event.EventID = ""
	err := notifier.Announce(ctx, event)
	if err == nil || !strings.Contains(err.Error(), "event id required") {
		t.Fatalf("Announce() error = %v, want the refusal", err)
	}
	if publishes := session.publishes(); len(publishes) != 0 {
		t.Errorf("publishes = %d, want none for a refused event", len(publishes))
	}
}

// TestNotifierAnnounceWithoutASession pins that an announcement waits for the publishing goroutine
// and gives up when its own context ends: a caller is never left hanging by a notifier that has not
// established a session.
func TestNotifierAnnounceWithoutASession(t *testing.T) {
	t.Parallel()

	notifier := newTestNotifier()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := notifier.Announce(ctx, rollbackAnnouncement(RollbackStarted))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Announce() error = %v, want the caller's deadline", err)
	}
}

// newTestNotifier returns a notifier whose supervisor dials nothing: a test either drives its serve
// loop with a scripted session or scripts the sessions the supervisor opens.
func newTestNotifier() *Notifier {
	return NewNotifier(
		"amqp://guest:guest@localhost:5672/",
		NewTopology(3, time.Second, time.Minute),
		slog.New(slog.DiscardHandler),
	)
}

// TestNotifierPublishesThroughTheSessionThatReplacedALostOne pins the reconnect contract: a session
// lost under a publish makes that announcement fail, the supervisor dials a new session, and the
// next announcement is published through it.
func TestNotifierPublishesThroughTheSessionThatReplacedALostOne(t *testing.T) {
	t.Parallel()

	notifier := newTestNotifier()
	first := newFakeSession().script(verdict{err: errors.New("connection reset")})
	second := newFakeSession().script(verdict{ack: true})
	sessions := []*fakeSession{first, second}
	var opened int
	notifier.supervisor.open = func(context.Context) (Session, error) {
		if opened >= len(sessions) {
			return nil, errors.New("test opened more sessions than it scripted")
		}
		session := sessions[opened]
		opened++
		return session, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- notifier.Run(ctx) }()

	// The first announcement is lost with the session it was published on.
	err := notifier.Announce(ctx, rollbackAnnouncement(RollbackStarted))
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("Announce() on a lost session = %v, want the failure reported", err)
	}
	if !first.isClosed() {
		t.Error("the lost session was never released")
	}

	// The supervisor reconnects, and the next announcement goes through the new session.
	if err := notifier.Announce(ctx, rollbackAnnouncement(RollbackCompleted)); err != nil {
		t.Fatalf("Announce() after the reconnect = %v", err)
	}
	publishes := second.publishes()
	if len(publishes) != 1 {
		t.Fatalf("publishes on the second session = %d, want 1", len(publishes))
	}
	if publishes[0].key != RollbackRoutingKey(RollbackCompleted) {
		t.Errorf("routing key after the reconnect = %q, want %q",
			publishes[0].key, RollbackRoutingKey(RollbackCompleted))
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil on shutdown", err)
	}
}
