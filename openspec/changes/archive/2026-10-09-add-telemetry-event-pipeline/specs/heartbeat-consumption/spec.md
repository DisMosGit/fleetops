# Spec Delta

## Purpose

Consumes heartbeat events so that each consumer applies every event at most once: events are
deduplicated by their stable identifier against a uniqueness-constrained processed-events ledger,
acknowledged only after their side effect is durable, and driven through a bounded retry ladder into
a dead-letter queue when they keep failing. The alerting consumer turns degraded heartbeats into
durable alerts.

## ADDED Requirements

### Requirement: Deduplication by event identifier
Each consumer SHALL deduplicate deliveries by the pair (consumer name, event id) recorded in the
`processed_events` collection, one document per pair, whose uniqueness constraint SHALL make a
second document for the same pair impossible. A record SHALL name the consumer, the event id, the
device, the time the event was first claimed, and the time its side effect became durable; only a
record whose side effect is durable SHALL count as processed, and a record without that time SHALL
NOT be treated as evidence that the event was applied. A delivery whose event id is already recorded
as processed by that consumer SHALL be acknowledged as a duplicate without repeating the side effect
and without being counted as newly processed, while a delivery whose record exists but is not yet
processed SHALL be treated as unfinished work and applied again. Deduplication SHALL be per
consumer: two consumers processing the same event each apply it once.

#### Scenario: First delivery is processed once
- **WHEN** a consumer receives an event whose id it has not recorded
- **THEN** it records the claim, applies the side effect, marks that record processed, and
  acknowledges the delivery

#### Scenario: Redelivery is a counted no-op
- **WHEN** the same event is delivered to that consumer again, whether immediately or after a
  restart
- **THEN** no second side effect happens, no second processed-events record exists, and the delivery
  is acknowledged as a duplicate

#### Scenario: Interrupted work is resumed, not skipped
- **WHEN** a delivery arrives for an event whose record exists but whose side effect never became
  durable, because the previous attempt failed or the consumer stopped mid-processing
- **THEN** the event is applied again and no event is lost behind a stale claim

#### Scenario: Each consumer applies the event once
- **WHEN** two consumers bound to the same events each receive one copy
- **THEN** each records its own processed-events entry and applies its own side effect exactly once

#### Scenario: A second record for one pair is refused
- **WHEN** a consumer attempts to record a second processed-events document for a pair it already
  recorded
- **THEN** the uniqueness constraint refuses it, and the consumer treats the refusal as a duplicate
  rather than an error

### Requirement: Acknowledgement follows the durable side effect
A consumer SHALL acknowledge a delivery only after its side effect is durable, and SHALL NOT
acknowledge a delivery whose processing failed — such a delivery is retried or dead-lettered
through the paths below. A consumer SHALL NOT acknowledge an event it did not apply.

#### Scenario: Acknowledgement after the write
- **WHEN** a consumer processes an event successfully
- **THEN** the underlying storage holds the side effect and the processed-events entry before the
  delivery is acknowledged

#### Scenario: Failed side effect is not acknowledged
- **WHEN** the side effect fails
- **THEN** the delivery is not acknowledged as processed and instead enters the retry path

### Requirement: Alerting consumer turns degraded heartbeats into alerts
The `fleetops.heartbeat.alerting` consumer SHALL record a durable alert for every heartbeat whose
`health` is below `alerting.health_threshold`, and SHALL write nothing for a heartbeat at or above
it. There SHALL be at most one alert document per device, identified by the device id and refreshed
in place — never a second document for the same device — carrying the device's region and model, the
lowest health observed during the degradation, the first and last time degraded health was observed,
and the threshold that triggered it. Repeated degraded heartbeats SHALL widen that window and lower
that minimum without creating a new alert, and re-applying the same event SHALL leave the document
unchanged.

#### Scenario: Degraded heartbeat opens an alert
- **WHEN** a heartbeat with health below the configured threshold is consumed
- **THEN** an alert document exists for that device, with its region and model, that health value,
  the threshold, and the observation time

#### Scenario: Repeated degradation refreshes one alert
- **WHEN** further degraded heartbeats arrive for the same device
- **THEN** the existing alert's last-observed time advances, its lowest observed health never
  increases, its first-observed time stays the earliest observation, and no second alert document
  appears

#### Scenario: Healthy heartbeat writes nothing
- **WHEN** a heartbeat at or above the threshold is consumed
- **THEN** no alert document is created or modified for that device

#### Scenario: Alert write is idempotent
- **WHEN** the same degraded event is applied twice
- **THEN** the alert document is identical after the second application

### Requirement: Failed deliveries retry with backoff then dead-letter
A delivery whose processing fails SHALL be republished to the retry path with its attempt count
incremented, and its original delivery SHALL be acknowledged only after that republish is confirmed,
so a crash between the two cannot lose the event. A delivery that has reached
`rabbitmq.max_attempts` SHALL be published to the work queue's dead-letter queue with its failure
reason and attempt count, and a delivery that cannot be republished SHALL be rejected so the
broker's dead-letter path takes it. A failing delivery SHALL NOT be requeued indefinitely on the
work queue.

#### Scenario: Failure enters the retry ladder
- **WHEN** processing an event fails on its first attempt
- **THEN** the event is republished to the retry queue for the next attempt and the original
  delivery is acknowledged only after the republish is confirmed

#### Scenario: Exhausted attempts dead-letter with a reason
- **WHEN** processing fails on the final permitted attempt
- **THEN** the event is on the dead-letter queue carrying its attempt count and failure reason, and
  the delivery is acknowledged

#### Scenario: Unrepublishable failure cannot loop
- **WHEN** the retry republish itself fails
- **THEN** the delivery is rejected without requeue and reaches the dead-letter queue through the
  broker's dead-letter path

### Requirement: Unusable events are dead-lettered without burning attempts
A delivery that cannot be understood — a payload that does not decode, a missing event identifier,
or a schema version this consumer does not support — SHALL be dead-lettered immediately with a
reason naming the defect, without consuming retry attempts and without invoking the side effect.
Such an event SHALL NOT be retried as if it were a transient failure.

#### Scenario: Malformed payload goes straight to the dead-letter queue
- **WHEN** a delivery carries a payload that does not decode into an event envelope
- **THEN** it lands on the dead-letter queue with a reason naming the malformed payload, and no
  retry delivery is made

#### Scenario: Unsupported schema version is refused
- **WHEN** an event declares a schema version the consumer does not support
- **THEN** it is dead-lettered with that reason and no side effect is applied

#### Scenario: Event missing its identifier is refused
- **WHEN** an event envelope carries no event id
- **THEN** it is dead-lettered with that reason and no processed-events entry is written

### Requirement: Consumption is bounded and survives broker loss
A consumer SHALL bound its in-flight work by `rabbitmq.prefetch` unacknowledged deliveries, so a slow
side effect applies backpressure instead of accumulating unbounded work. When its connection or
channel is lost it SHALL reconnect with backoff, re-declare the topology, and resume consuming, and
an event redelivered after such a reconnect SHALL be deduplicated rather than applied twice.

#### Scenario: In-flight work is bounded
- **WHEN** a consumer's side effect is slower than the arrival rate
- **THEN** the broker delivers no more than `rabbitmq.prefetch` unacknowledged messages at a time

#### Scenario: Consumer resumes after a broker restart
- **WHEN** the broker restarts while a consumer is running
- **THEN** the consumer reconnects with backoff and continues consuming without a process restart

#### Scenario: Redelivery after reconnect is deduplicated
- **WHEN** an event already processed before the reconnect is redelivered after it
- **THEN** the side effect is not applied twice
