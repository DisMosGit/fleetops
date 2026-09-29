package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RollbackEventSchemaVersion is the revision of the rollback announcement contract this build
// writes. Within one version a field is never renamed, repurposed, or given a different meaning: a
// changed shape is published under a new version instead. It is deliberately separate from
// SchemaVersion, which stamps the heartbeat envelope: the two are different contracts, and a
// consumer of one is not a consumer of the other.
const RollbackEventSchemaVersion = 1

// RollbackEventType is the event family of rollback announcements. It rides on the message's type
// property, so a delivery is diagnosable without decoding its body.
const RollbackEventType = "rollback_notification"

// RollbackPhase names the moment of a rollback an announcement is published for.
type RollbackPhase string

// The two moments one rollback announces.
const (
	// RollbackStarted is published when the rollback enters its compensating phase, before any
	// compensation runs.
	RollbackStarted RollbackPhase = "started"
	// RollbackCompleted is published once every compensation has run.
	RollbackCompleted RollbackPhase = "completed"
)

// RollbackEventID returns the stable identity of one rollback announcement. It is derived from the
// rollout and the phase, so a retried publication repeats it and a consumer that deduplicates by
// event identity applies the event once. A rollout rolls back once, so the phase is the only
// discriminator the identity needs.
func RollbackEventID(rolloutID string, phase RollbackPhase) string {
	return "rollback-" + rolloutID + "-" + string(phase)
}

// RollbackEvent is one rollback announcement: a persistent JSON event published to the events
// exchange under the notification key space. It carries no device and no measurement — it is a
// statement about a rollout, not a sample — which is why it is its own contract with its own schema
// version rather than a widened heartbeat envelope.
type RollbackEvent struct {
	// SchemaVersion is the event contract revision; see RollbackEventSchemaVersion.
	SchemaVersion int `json:"schema_version"`
	// EventID is the event's stable identity; see RollbackEventID.
	EventID string `json:"event_id"`
	// EventType is the event family: RollbackEventType.
	EventType string `json:"event_type"`
	// Phase is the moment of the rollback being announced.
	Phase RollbackPhase `json:"phase"`
	// OccurredAt is the workflow time the announcement was decided at, which is what makes a
	// replayed run announce the same moment.
	OccurredAt time.Time `json:"occurred_at"`
	// Rollout identifies the rollout, what it deployed, and the wave that ended it.
	Rollout RolloutAnnouncement `json:"rollout"`
	// Progress is what the compensations achieved. It is set on the completed announcement and
	// nil on the started one, which precedes every compensation.
	Progress *RollbackProgress `json:"progress,omitempty"`
}

// RolloutAnnouncement identifies the rollout a rollback belongs to and why it happened.
type RolloutAnnouncement struct {
	// RolloutID is the rollout that is rolling back.
	RolloutID string `json:"rollout_id"`
	// FirmwareID is the firmware the rollout deployed.
	FirmwareID string `json:"firmware_id"`
	// FirmwareVersion is the deployed firmware's version.
	FirmwareVersion string `json:"firmware_version,omitempty"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
	// WaveID is the wave whose failure ended the rollout.
	WaveID string `json:"wave_id,omitempty"`
	// Outcome is the outcome that ended the rollout — why the compensations ran.
	Outcome string `json:"outcome"`
	// Decision is the measurement that ended the rollout. It is nil for a wave that ended the
	// rollout before it could be measured — a wave whose commands could not be delivered — rather
	// than a fabricated measurement.
	Decision *RollbackDecision `json:"decision,omitempty"`
	// PlanSteps is how many steps the rollback's plan has.
	PlanSteps int `json:"plan_steps"`
	// PlanDevices is how many devices the rollback's plan compensates.
	PlanDevices int `json:"plan_devices"`
}

// RollbackDecision is the health measurement a gate ended a rollout on.
type RollbackDecision struct {
	// Verdict is the decision the measurement produced.
	Verdict string `json:"verdict"`
	// SuccessRatio is the share of samples that succeeded, in [0, 1].
	SuccessRatio float64 `json:"success_ratio"`
	// SampleSize is the number of samples the ratio was computed over.
	SampleSize int64 `json:"sample_size"`
	// WindowStart is the start of the window the measurement read.
	WindowStart time.Time `json:"window_start"`
	// WindowEnd is the end of the window.
	WindowEnd time.Time `json:"window_end"`
}

// RollbackProgress is what a rollback's compensations achieved: the device outcomes of its
// downgrades, the record outcomes of its reconciliation, the inventory that reconciliation
// established, and the devices it could not restore. It also lists each step's own outcome, so a
// consumer can see where a partial rollback fell short without reading a workflow.
type RollbackProgress struct {
	// Restored is how many devices reported their restore concluded successfully.
	Restored int `json:"restored"`
	// Failed is how many devices reported their restore concluded unsuccessfully.
	Failed int `json:"failed"`
	// Unreported is how many devices never concluded their restore before the step stopped
	// waiting.
	Unreported int `json:"unreported"`
	// Skipped is how many devices had nothing to restore.
	Skipped int `json:"skipped"`
	// Unavailable is how many devices could not be restored at all.
	Unavailable int `json:"unavailable"`
	// Agreed is how many device records already matched the version their device holds.
	Agreed int `json:"agreed"`
	// Corrected is how many device records were corrected to the version their device reports.
	Corrected int `json:"corrected"`
	// Unverified is how many device records could not be verified.
	Unverified int `json:"unverified"`
	// Inventory is the reconciled inventory: how many devices were found on each firmware
	// version.
	Inventory []FirmwareInventoryEntry `json:"inventory"`
	// UnrestoredDeviceIDs are the devices the rollback could not restore.
	UnrestoredDeviceIDs []string `json:"unrestored_device_ids"`
	// Steps are the plan's steps in plan order with what each achieved.
	Steps []RollbackStepOutcome `json:"steps"`
}

// FirmwareInventoryEntry is one firmware version's share of a reconciled inventory.
type FirmwareInventoryEntry struct {
	// Version is the firmware version devices were found on.
	Version string `json:"version"`
	// Devices is how many devices were found on it.
	Devices int `json:"devices"`
}

// RollbackStepOutcome is what one compensating step achieved, as the completion announcement
// reports it.
type RollbackStepOutcome struct {
	// Kind is what the step does.
	Kind string `json:"kind"`
	// WaveID is the wave a compensating step compensates; empty for a step that compensates no
	// wave.
	WaveID string `json:"wave_id,omitempty"`
	// Status is where the step stands.
	Status string `json:"status"`
	// Devices is how many devices the step targeted.
	Devices int `json:"devices"`
	// Restored, Failed, Unreported, Skipped, and Unavailable are the outcomes of a downgrade
	// step's devices.
	Restored    int `json:"restored"`
	Failed      int `json:"failed"`
	Unreported  int `json:"unreported"`
	Skipped     int `json:"skipped"`
	Unavailable int `json:"unavailable"`
	// Agreed, Corrected, and Unverified are the outcomes of a reconciliation step's device
	// records.
	Agreed     int `json:"agreed"`
	Corrected  int `json:"corrected"`
	Unverified int `json:"unverified"`
	// Detail is why a step failed; empty otherwise.
	Detail string `json:"detail,omitempty"`
}

// NewRollbackEvent returns one rollback announcement carrying its deterministic identity and the
// contract version it is written under. A caller fills in what the event reports.
func NewRollbackEvent(rolloutID string, phase RollbackPhase, occurredAt time.Time) RollbackEvent {
	return RollbackEvent{
		SchemaVersion: RollbackEventSchemaVersion,
		EventID:       RollbackEventID(rolloutID, phase),
		EventType:     RollbackEventType,
		Phase:         phase,
		OccurredAt:    occurredAt,
		Rollout:       RolloutAnnouncement{RolloutID: rolloutID},
	}
}

// Validate reports why a rollback event cannot be understood. Every reason is a fixed string naming
// the defect, so nothing echoes a payload.
func (e RollbackEvent) Validate() error {
	var errs []error
	if e.SchemaVersion != RollbackEventSchemaVersion {
		errs = append(errs, fmt.Errorf("unsupported rollback schema version %d", e.SchemaVersion))
	}
	if e.EventID == "" {
		errs = append(errs, errors.New("event id required"))
	}
	if e.Rollout.RolloutID == "" {
		errs = append(errs, errors.New("rollout id required"))
	}
	if e.Phase != RollbackStarted && e.Phase != RollbackCompleted {
		errs = append(errs, fmt.Errorf("unknown rollback phase %q", e.Phase))
	}
	if e.OccurredAt.IsZero() {
		errs = append(errs, errors.New("occurrence time required"))
	}
	return errors.Join(errs...)
}

// Encode returns the event's JSON wire form.
func (e RollbackEvent) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode rollback event %s: %w", e.EventID, err)
	}
	return data, nil
}

// Message builds the persistent AMQP message for one rollback announcement published under
// routingKey. The identity fields ride on the message properties, so an operator reading the
// notification queue — or its dead letters — can tell what an event is without decoding its body.
func (e RollbackEvent) Message(routingKey string) (amqp.Publishing, error) {
	if err := e.Validate(); err != nil {
		return amqp.Publishing{}, err
	}
	body, err := e.Encode()
	if err != nil {
		return amqp.Publishing{}, err
	}
	return amqp.Publishing{
		Headers: amqp.Table{
			schemaVersionHeader: int32(e.SchemaVersion),
			// The announcement is published once by the process that ran the rollback, so it
			// starts on its first processing attempt like every other event family.
			attemptHeader:            int32(1),
			originalRoutingKeyHeader: routingKey,
		},
		ContentType:  contentTypeJSON,
		DeliveryMode: amqp.Persistent,
		MessageId:    e.EventID,
		Type:         e.EventType,
		Timestamp:    e.OccurredAt,
		Body:         body,
	}, nil
}

// DecodeRollbackEvent parses one announcement and validates it, so a consumer never reacts to an
// event it could not fully understand.
func DecodeRollbackEvent(data []byte) (RollbackEvent, error) {
	var event RollbackEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return RollbackEvent{}, fmt.Errorf("decode rollback event payload: %w", err)
	}
	if err := event.Validate(); err != nil {
		return RollbackEvent{}, err
	}
	return event, nil
}
