package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SchemaVersion is the envelope revision this build writes and understands. Within one version
// a field is never renamed, repurposed, or given a different meaning: a changed shape is
// published under a new version instead.
const SchemaVersion = 1

// Message properties and headers carrying the envelope's identity. MessageId and Type hold the
// event id and event type, so a delivery — including a dead letter — is diagnosable without
// decoding its payload.
const (
	// schemaVersionHeader repeats the envelope's schema version on the message.
	schemaVersionHeader = "x-schema-version"
	// attemptHeader is the processing attempt the message is on, one-based. The publisher stamps
	// the first attempt and every retry increments it, so the count is deterministic rather than
	// inferred from broker bookkeeping.
	attemptHeader = "x-attempt"
	// originalRoutingKeyHeader is the key the event was first published under, so a retry or a
	// dead letter still says where the event came from.
	originalRoutingKeyHeader = "x-original-routing-key"
	// deadLetterReasonHeader is why a message was dead-lettered.
	deadLetterReasonHeader = "x-death-reason"

	// contentTypeJSON is the content type of every published event.
	contentTypeJSON = "application/json"
)

// Payload is the measurement payload of a heartbeat event: the samples the agent reported.
type Payload struct {
	// CPU is the reported CPU utilisation.
	CPU float64 `json:"cpu"`
	// Mem is the reported memory utilisation.
	Mem float64 `json:"mem"`
	// Health is the reported health score.
	Health float64 `json:"health"`
	// CurrentFW is the firmware version the device runs.
	CurrentFW string `json:"current_fw"`
	// Status is the status the device reported.
	Status string `json:"status"`
}

// Envelope is the versioned JSON contract every FleetOps event travels in. JSON keeps events
// readable in the broker UI, which is this stack's debugging surface; the protocol buffer
// messages of the gRPC contract are deliberately not reused here, so consumers never couple to
// a transport message of another bounded context.
type Envelope struct {
	// SchemaVersion is the envelope revision; see SchemaVersion.
	SchemaVersion int `json:"schema_version"`
	// EventID is the event's stable identifier: the heartbeat id the agent minted, reused on
	// redelivery, and the key consumers deduplicate on.
	EventID string `json:"event_id"`
	// EventType is the event family: HeartbeatEventType, or RolloutEventType for rollout work.
	EventType string `json:"event_type"`
	// DeviceID is the device the event belongs to.
	DeviceID string `json:"device_id"`
	// Region is the device's registered region, also part of the routing key.
	Region string `json:"region"`
	// Model is the device's registered model, also part of the routing key.
	Model string `json:"model"`
	// OccurredAt is the moment the measurement was taken.
	OccurredAt time.Time `json:"occurred_at"`
	// PublishedAt is the moment the control plane published the event.
	PublishedAt time.Time `json:"published_at"`
	// Sequence is the publisher's per-event-type monotonic counter, which is what consumer lag
	// is measured against.
	Sequence uint64 `json:"sequence"`
	// Payload is the event's measurements.
	Payload Payload `json:"payload"`
}

// NewHeartbeatEvent returns the envelope of one accepted heartbeat: the ingest path's view of
// the heartbeat, with the device identity its registration established.
func NewHeartbeatEvent(
	eventID, deviceID, region, model string,
	occurredAt, publishedAt time.Time,
	sequence uint64,
	payload Payload,
) Envelope {
	return Envelope{
		SchemaVersion: SchemaVersion,
		EventID:       eventID,
		EventType:     HeartbeatEventType,
		DeviceID:      deviceID,
		Region:        region,
		Model:         model,
		OccurredAt:    occurredAt,
		PublishedAt:   publishedAt,
		Sequence:      sequence,
		Payload:       payload,
	}
}

// Validate reports why an envelope cannot be understood. Every reason is a fixed string naming
// the defect, so a dead letter's reason is diagnosable and never echoes a payload.
func (e Envelope) Validate() error {
	var errs []error
	if e.SchemaVersion != SchemaVersion {
		errs = append(errs, fmt.Errorf("unsupported schema version %d", e.SchemaVersion))
	}
	if e.EventID == "" {
		errs = append(errs, errors.New("event id required"))
	}
	if e.EventType == "" {
		errs = append(errs, errors.New("event type required"))
	}
	if e.OccurredAt.IsZero() {
		errs = append(errs, errors.New("measurement time required"))
	}
	return errors.Join(errs...)
}

// Encode returns the envelope's JSON wire form.
func (e Envelope) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode event %s: %w", e.EventID, err)
	}
	return data, nil
}

// DecodeEvent parses one event payload and validates it, so a consumer never applies an event
// it could not fully understand.
func DecodeEvent(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode event payload: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Message builds the persistent AMQP message for one processing attempt of an event published
// under routingKey. The identity fields ride on the message properties, so a consumer — or an
// operator looking at the dead-letter queue — can read them without decoding the body.
func (e Envelope) Message(attempt int, routingKey string) (amqp.Publishing, error) {
	body, err := e.Encode()
	if err != nil {
		return amqp.Publishing{}, err
	}
	headers := amqp.Table{
		schemaVersionHeader:      int32(e.SchemaVersion),
		attemptHeader:            int32(attempt),
		originalRoutingKeyHeader: routingKey,
	}
	msg := amqp.Publishing{
		Headers:      headers,
		ContentType:  contentTypeJSON,
		DeliveryMode: amqp.Persistent,
		MessageId:    e.EventID,
		Type:         e.EventType,
		Timestamp:    e.PublishedAt,
		Body:         body,
	}
	return msg, nil
}

// forwardMessage rebuilds a delivery as a fresh persistent message for the next stage of its
// life — a retry, or a dead letter. Only the envelope's own identity headers are carried over:
// the broker's bookkeeping headers (x-death and friends) are its own and must never be
// republished. attempts is the attempt count to record, and reason is empty for a retry.
func forwardMessage(d amqp.Delivery, attempts int, reason string) amqp.Publishing {
	headers := amqp.Table{
		attemptHeader:            int32(attempts),
		originalRoutingKeyHeader: originalRoutingKey(d),
	}
	if version, ok := intHeader(d.Headers, schemaVersionHeader); ok {
		headers[schemaVersionHeader] = version
	}
	if reason != "" {
		headers[deadLetterReasonHeader] = reason
	}
	contentType := d.ContentType
	if contentType == "" {
		contentType = contentTypeJSON
	}
	return amqp.Publishing{
		Headers:      headers,
		ContentType:  contentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Type:         d.Type,
		Timestamp:    d.Timestamp,
		Body:         d.Body,
	}
}

// attemptOf returns the processing attempt a delivery is on; a delivery without the header is
// on its first attempt.
func attemptOf(d amqp.Delivery) int {
	if attempt, ok := intHeader(d.Headers, attemptHeader); ok && attempt > 0 {
		return int(attempt)
	}
	return 1
}

// originalRoutingKey returns the key the event was first published under, so a retry or a dead
// letter keeps saying where the event came from even though it was republished under the key of
// a retry or dead-letter queue.
func originalRoutingKey(d amqp.Delivery) string {
	if key, ok := d.Headers[originalRoutingKeyHeader].(string); ok && key != "" {
		return key
	}
	return d.RoutingKey
}

// intHeader reads one integer header, reporting false when it is absent or of another type.
func intHeader(headers amqp.Table, name string) (int32, bool) {
	value, ok := headers[name].(int32)
	return value, ok
}
