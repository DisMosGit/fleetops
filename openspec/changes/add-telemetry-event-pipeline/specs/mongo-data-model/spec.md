# Spec Delta

## ADDED Requirements

### Requirement: Processed-event deduplication ledger
The system SHALL store the events each consumer has processed in a `processed_events` collection,
identified by `_id` = `<consumer>:<event id>`, and SHALL enforce a unique constraint on the pair
(`consumer`, `event_id`) so a second document for one consumer and event cannot exist. Every
document MUST contain `consumer`, `event_id`, `device_id`, `claimed_at` (the time the consumer first
took the event for processing), and `processed_at` (the time the event's side effect became durable,
present only once it did). A document without `processed_at` MUST NOT be read as an applied event.
The ledger SHALL have bounded retention: documents are removed automatically after the retention
period, which SHALL be a single named configuration value shared by the ledger's writes and its
index definition. `processed_events` is pipeline bookkeeping, not a domain record.

#### Scenario: One ledger document per consumer and event
- **WHEN** a consumer processes an event
- **THEN** exactly one `processed_events` document exists for that consumer and event id, carrying
  `device_id`, `claimed_at`, and `processed_at`

#### Scenario: Uniqueness is enforced by the store
- **WHEN** a write attempts to store a second document for a consumer and event id already recorded
- **THEN** the write is rejected as a duplicate and no second document exists

#### Scenario: Retention is bounded and configurable in one place
- **WHEN** a ledger document ages past the retention period
- **THEN** the database removes it automatically through its TTL index, and changing the single
  named retention value is enough for new deployments to pick up the new period

#### Scenario: Ledger documents never expire early
- **WHEN** a ledger document is inside the retention period
- **THEN** it is still present and a redelivery of its event is still recognised as processed

### Requirement: Device alert records
The system SHALL store at most one alert document per device in a `device_alerts` collection,
identified by `_id` = device id and updated in place, never a second document for one device. Every
alert document MUST contain `device_id`, `region`, `model`, `threshold` (the health threshold that
triggered the alert), `min_health` (the lowest health observed since the alert opened),
`first_seen_at`, and `last_seen_at`, and MUST NOT contain the raw heartbeat payload. Alert documents
are domain records and MUST NOT expire.

#### Scenario: Alert record content
- **WHEN** a degraded heartbeat is recorded as an alert
- **THEN** its `device_alerts` document is retrievable by the device id and carries `region`,
  `model`, `threshold`, `min_health`, `first_seen_at`, and `last_seen_at`

#### Scenario: One alert per device
- **WHEN** many degraded heartbeats arrive for one device
- **THEN** exactly one document exists for that device, holding the earliest `first_seen_at`, the
  lowest `min_health`, and the newest `last_seen_at`

#### Scenario: Required fields enforced
- **WHEN** a write attempts to store an alert document missing a required field
- **THEN** the write is rejected and no partial document is stored

## MODIFIED Requirements

### Requirement: Indexing strategy
Every collection in this data model SHALL carry indexes matching its access patterns so that no
hot query requires a collection scan: `devices` by device identity (unique `_id`), by
`region` + `model`, and by `status`; firmware metadata by `version` (unique) and `_id`;
`rollouts` by `status` and by `firmware_id`; `waves` by `rollout_id` (+ wave order); `telemetry`
by `device_id` + `ts` and by `ts` within `region` + `model` meta; `device_state_snapshots` by
device identity (unique `_id`), by `region` + `model`, and by `online`; `processed_events` by
(`consumer`, `event_id`) unique and by `device_id`; and `device_alerts` by device identity
(unique `_id`) and by `region` + `model`. Writes (including heartbeat ingest at fleet scale) SHALL
NOT be rejected because of index conflicts on redelivery, and the duplicate refusal the
`processed_events` uniqueness constraint produces SHALL be distinguishable from every other write
error so a duplicate is never mistaken for a failure.

#### Scenario: Device identity lookup
- **WHEN** code resolves a device by its identity
- **THEN** the lookup is served by the unique `_id` index on `devices`, and a second device
  document with the same identity cannot be inserted

#### Scenario: Region and model pool queries
- **WHEN** the eligible-device pool is recalculated or a rollout resolves its target group
- **THEN** the `region` + `model` query on `devices` and the `region` + `model` time-range query on
  `telemetry` are each served by an index

#### Scenario: Status queries
- **WHEN** code lists devices or rollouts filtered by `status`, or state snapshots filtered by
  `online`
- **THEN** the query is served by a `status` index on the queried collection

#### Scenario: Dedup lookup is served by an index
- **WHEN** a consumer checks whether it has already processed an event, and an operator inspects the
  events of one device
- **THEN** both lookups are served by `processed_events` indexes rather than a collection scan

#### Scenario: Duplicate refusal is distinguishable
- **WHEN** the uniqueness constraint refuses a duplicate `processed_events` write
- **THEN** the caller can tell that refusal apart from any other write failure
