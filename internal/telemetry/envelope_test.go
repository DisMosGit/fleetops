package telemetry

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	amqp "github.com/rabbitmq/amqp091-go"
)

// testEvent returns a fully populated heartbeat event.
func testEvent() Envelope {
	occurred := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return NewHeartbeatEvent(
		"ev-1", "dev-1", "eu-west", "v3",
		occurred, occurred.Add(time.Millisecond), 7,
		Payload{CPU: 0.5, Mem: 0.25, Health: 0.4, CurrentFW: "1.0.0", Status: "online"},
	)
}

// TestEnvelopeRoundTrip pins the wire form: an encoded event decodes back into the same
// envelope, without the gRPC messages being involved at either end.
func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	event := testEvent()
	encoded, err := event.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"schema_version":1`) {
		t.Errorf("encoded event = %s, want the schema version on the wire", encoded)
	}

	got, err := DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent() error = %v", err)
	}
	if diff := cmp.Diff(event, got); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

// TestEnvelopeMessageProperties pins the message the publisher sends: persistent, JSON, and
// carrying the event's identity on properties a consumer can read without decoding the body.
func TestEnvelopeMessageProperties(t *testing.T) {
	t.Parallel()

	event := testEvent()
	msg, err := event.Message(1, "heartbeat.eu-west.v3")
	if err != nil {
		t.Fatalf("Message() error = %v", err)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent", msg.DeliveryMode)
	}
	if msg.ContentType != "application/json" {
		t.Errorf("content type = %q, want application/json", msg.ContentType)
	}
	if msg.MessageId != event.EventID {
		t.Errorf("message id = %q, want the event id %q", msg.MessageId, event.EventID)
	}
	if msg.Type != HeartbeatEventType {
		t.Errorf("message type = %q, want %q", msg.Type, HeartbeatEventType)
	}
	if !msg.Timestamp.Equal(event.PublishedAt) {
		t.Errorf("message timestamp = %v, want the publish time %v", msg.Timestamp, event.PublishedAt)
	}
	wantHeaders := amqp.Table{
		schemaVersionHeader:      int32(SchemaVersion),
		attemptHeader:            int32(1),
		originalRoutingKeyHeader: "heartbeat.eu-west.v3",
	}
	if diff := cmp.Diff(wantHeaders, msg.Headers); diff != "" {
		t.Errorf("headers mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(event, mustDecode(t, msg.Body)); diff != "" {
		t.Errorf("body mismatch (-want +got):\n%s", diff)
	}
}

// TestEnvelopeValidation pins every reason an envelope is refused, so an unusable event is
// dead-lettered for a stated defect rather than applied.
func TestEnvelopeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Envelope)
		payload string
		want    string
	}{
		{name: "valid event", mutate: func(*Envelope) {}},
		{name: "missing event id", mutate: func(e *Envelope) { e.EventID = "" }, want: "event id required"},
		{name: "missing event type", mutate: func(e *Envelope) { e.EventType = "" }, want: "event type required"},
		{name: "unknown schema version", mutate: func(e *Envelope) { e.SchemaVersion = 2 }, want: "unsupported schema version 2"},
		{name: "missing measurement time", mutate: func(e *Envelope) { e.OccurredAt = time.Time{} }, want: "measurement time required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			event := testEvent()
			tc.mutate(&event)
			err := event.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestDecodeEventRejectsMalformedPayload pins that an undecodable payload is an error rather
// than a partly populated envelope.
func TestDecodeEventRejectsMalformedPayload(t *testing.T) {
	t.Parallel()

	if _, err := DecodeEvent([]byte("{not json")); err == nil {
		t.Fatal("DecodeEvent(malformed) = nil error, want an error")
	}
	if _, err := DecodeEvent([]byte(`{"event_id":"ev-1"}`)); err == nil {
		t.Fatal("DecodeEvent(missing schema version) = nil error, want an error")
	}
}

// TestForwardMessage pins what a retry or dead letter carries: the payload and identity of the
// original event, the attempt it reached, and — for a dead letter — the reason, without any
// broker-managed header being republished.
func TestForwardMessage(t *testing.T) {
	t.Parallel()

	session := newFakeSession()
	delivery := deliveryFor(t, session, testEvent(), 2, "heartbeat.eu-west.v3")
	delivery.Headers["x-death"] = amqp.Table{"count": int64(1)}

	retry := forwardMessage(delivery, 3, "")
	if got := attemptOf(amqp.Delivery{Headers: retry.Headers}); got != 3 {
		t.Errorf("retry attempt = %d, want 3", got)
	}
	if _, ok := retry.Headers[deadLetterReasonHeader]; ok {
		t.Error("retry message carries a dead-letter reason, want none")
	}
	if _, ok := retry.Headers["x-death"]; ok {
		t.Error("retry message republishes the broker's x-death header")
	}
	if diff := cmp.Diff(string(delivery.Body), string(retry.Body)); diff != "" {
		t.Errorf("retry payload mismatch (-want +got):\n%s", diff)
	}
	if retry.MessageId != delivery.MessageId {
		t.Errorf("retry message id = %q, want the original %q", retry.MessageId, delivery.MessageId)
	}
	if retry.DeliveryMode != amqp.Persistent {
		t.Errorf("retry delivery mode = %d, want persistent", retry.DeliveryMode)
	}

	dead := forwardMessage(delivery, 2, "apply event: boom")
	if got := dead.Headers[deadLetterReasonHeader]; got != "apply event: boom" {
		t.Errorf("dead letter reason = %v, want the failure reason", got)
	}
	if got := dead.Headers[originalRoutingKeyHeader]; got != "heartbeat.eu-west.v3" {
		t.Errorf("dead letter original routing key = %v, want the first published key", got)
	}
}

// TestAttemptAndRoutingKeyAccessors pins the fallbacks a foreign or first-attempt delivery
// relies on.
func TestAttemptAndRoutingKeyAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headers     amqp.Table
		routingKey  string
		wantAttempt int
		wantKey     string
	}{
		{
			name:        "first attempt without headers",
			routingKey:  "heartbeat.eu-west.v3",
			wantAttempt: 1,
			wantKey:     "heartbeat.eu-west.v3",
		},
		{
			name:        "stamped identity",
			headers:     amqp.Table{attemptHeader: int32(4), originalRoutingKeyHeader: "heartbeat.us-east.v2"},
			routingKey:  "fleetops.heartbeat.alerting.retry.3",
			wantAttempt: 4,
			wantKey:     "heartbeat.us-east.v2",
		},
		{
			name:        "zero attempt falls back to the first",
			headers:     amqp.Table{attemptHeader: int32(0)},
			routingKey:  "heartbeat.eu-west.v3",
			wantAttempt: 1,
			wantKey:     "heartbeat.eu-west.v3",
		},
		{
			name:        "wrong header type falls back",
			headers:     amqp.Table{attemptHeader: "two", originalRoutingKeyHeader: int32(7)},
			routingKey:  "heartbeat.eu-west.v3",
			wantAttempt: 1,
			wantKey:     "heartbeat.eu-west.v3",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delivery := amqp.Delivery{Headers: tc.headers, RoutingKey: tc.routingKey}
			if got := attemptOf(delivery); got != tc.wantAttempt {
				t.Errorf("attemptOf() = %d, want %d", got, tc.wantAttempt)
			}
			if got := originalRoutingKey(delivery); got != tc.wantKey {
				t.Errorf("originalRoutingKey() = %q, want %q", got, tc.wantKey)
			}
		})
	}
}

// mustDecode decodes an event or fails the test.
func mustDecode(t *testing.T, body []byte) Envelope {
	t.Helper()
	event, err := DecodeEvent(body)
	if err != nil {
		t.Fatalf("DecodeEvent() error = %v", err)
	}
	return event
}
