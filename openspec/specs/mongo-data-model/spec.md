# mongo-data-model Specification

## Purpose
Defines the MongoDB persistence contract for the fleet: the `devices`, firmware metadata,
`rollouts`, `waves`, and `telemetry` collections with their required fields, the indexing strategy
that serves device identity, region+model, and status lookups, and the retention policy for
telemetry.

## Requirements

### Requirement: Device records

The system SHALL store exactly one heterogeneous document per device in the `devices` collection,
identified by `_id` = device identity. Every device document MUST contain the fields `model`,
`region`, `current_fw` (the firmware version the device runs), `status`, and `last_heartbeat`.
Documents missing any required field MUST be rejected at write time.

`current_fw` SHALL be the fleet's record of the firmware version a device runs, kept from the
device's own reports and from the registration that enrolled it. It is a copy rather than the
authority: the device's workflow owns the version the device runs, and where the recorded version
disagrees with it the workflow's value is the correct one. A rollback MAY therefore correct
`current_fw` to the version the device's workflow reports, and SHALL do so only by writing a version
a device reported — the field is never derived, guessed, or cleared.

#### Scenario: Device record content

- **WHEN** a device enrols and emits state
- **THEN** its `devices` document is retrievable by `_id` and carries `model`, `region`,
  `current_fw`, `status`, and `last_heartbeat`

#### Scenario: Required fields enforced

- **WHEN** a write attempts to store a device document missing a required field
- **THEN** the write is rejected and no partial document is stored

#### Scenario: Device state updates in place

- **WHEN** a heartbeat or command result changes a device's state
- **THEN** the existing `devices` document is updated in place and no second document appears for
  the same device identity

#### Scenario: A stale recorded version is corrected against the device

- **WHEN** a rollback finds a device's recorded `current_fw` disagreeing with the version that
  device's workflow holds
- **THEN** the recorded version is corrected to the version the workflow holds, in place, with no
  second document for that device

#### Scenario: A record that already agrees is left alone

- **WHEN** a rollback finds a device's recorded `current_fw` already equal to the version that
  device's workflow holds
- **THEN** the document is unchanged

### Requirement: Firmware metadata records
The system SHALL store one document per firmware in a firmware metadata collection, identified by
`_id` = firmware id. Every firmware document MUST contain `version`, a non-empty `models` list
naming the device models the firmware targets, `checksum`, `size`, `created_at`, and `gridfs_id`
referencing the binary stored in GridFS, and MUST NOT contain the binary payload itself. A
firmware document whose `gridfs_id` does not resolve to a GridFS object MUST be rejected at write
time.

#### Scenario: Firmware metadata with binary reference
- **WHEN** an operator uploads a firmware binary
- **THEN** the binary lands in GridFS and the metadata document records `version`, `models`,
  `checksum`, `size`, `created_at`, and the `gridfs_id` of the stored object

#### Scenario: Metadata never holds payloads
- **WHEN** a firmware document is read
- **THEN** it exposes `version`, `models`, `checksum`, `size`, `created_at`, and `gridfs_id` but no
  inline binary content

#### Scenario: Target models are recorded
- **WHEN** a firmware record is read after an upload declaring its target device models
- **THEN** the record's `models` list is exactly the declared target models

### Requirement: Rollout records

The system SHALL store one document per rollout in the `rollouts` collection, identified by
`_id` = rollout id. Every rollout document MUST contain `firmware_id`, `status`, and
`temporal_wf_id` (the id of the workflow execution driving the rollout), and MUST record the
target group as `region` plus `model`. The document SHALL be written when the rollout workflow
starts and updated by that workflow as the rollout progresses; `status` SHALL be one of the
lifecycle values `running`, `paused`, `awaiting_approval`, `rolling_back`, `rolled_back`,
`completed`, or `failed`, where `rolled_back`, `completed`, and `failed` are terminal and are never
left once recorded, and `running`, `paused`, `awaiting_approval`, and `rolling_back` are never
terminal. Every write SHALL be idempotent: a repeated write of a state the document already holds
MUST leave it unchanged.

A rollout that enters rollback SHALL carry the rollback as it compensates: the outcome that ended the
rollout, the ordered steps with the kind of each step, the wave a compensating step compensates, its
status, and what it achieved (the devices restored, failed, unreported, skipped, and unavailable; the
device records agreed, corrected, and unverified), the inventory the reconciliation established as the
number of devices on each firmware version, and the devices the rollback could not restore. These
fields MAY be absent on a document written before they existed, and SHALL be written by every write
that records a rollout in rollback or afterwards.

#### Scenario: Rollout record content

- **WHEN** a rollout workflow starts for a firmware and a target group
- **THEN** its `rollouts` document is retrievable by `_id` and carries `firmware_id`, `status`,
  `temporal_wf_id`, `region`, and `model`

#### Scenario: Rollout status transitions are recorded

- **WHEN** the rollout workflow advances a wave, waits for approval, is paused, is resumed, enters
  rollback, completes its rollback, completes, or fails
- **THEN** the `status` field of the existing rollout document reflects the new state

#### Scenario: A paused rollout is recorded as paused

- **WHEN** a running rollout is paused and later resumed
- **THEN** its document carries `paused` while it is paused and a non-terminal status again once it
  is resumed

#### Scenario: A compensating rollout is recorded as rolling back

- **WHEN** a rollout has entered rollback and its compensations are still running
- **THEN** its document carries `rolling_back`, which is not terminal, together with the outcome
  that ended the rollout

#### Scenario: A concluded rollback carries what it achieved

- **WHEN** a rollout's rollback has run
- **THEN** its document carries the terminal status `rolled_back` and the rollback record: the outcome
  that ended the rollout, each step's kind, the wave it compensates, its status and counts, the
  reconciled inventory with its correction count, and the devices that could not be restored

#### Scenario: A rollout that could not start is recorded as failed

- **WHEN** a rollout workflow cannot resolve the firmware it was given, or the firmware does not
  target the selector's model
- **THEN** its rollout document carries the terminal status `failed`

#### Scenario: A terminal status is not overwritten

- **WHEN** a write attempts to move a rollout document out of `rolled_back`, `completed`, or
  `failed`
- **THEN** the stored status is unchanged

#### Scenario: A retried rollback write converges

- **WHEN** a rollback record already written is written again
- **THEN** the rollout document's rollback record is unchanged rather than duplicated or regressed

### Requirement: Wave records

The system SHALL store one document per wave in the `waves` collection, identified by `_id` =
wave id. Every wave document MUST contain its parent `rollout_id`, `percent` (the canary share of
the target group), `status`, `success_rate` (health measured over the wave's health window), the
target device set the wave was dispatched to (`device_ids`, an array of device identities that MAY
be empty), and `started_at` (the time the wave started, when its health window opened), and waves
of one rollout SHALL be retrievable in rollout order. Heartbeats recorded before a wave's
`started_at` are outside that wave's health window.

The document SHALL also record the wave's device outcomes once they have been collected: the
devices that reported a failed update (`failed_device_ids`) and the devices that never reported a
result before the wave stopped waiting (`unreported_device_ids`), each an array of device
identities that MAY be empty and that is written by every wave write. A document stored before
these fields existed MAY omit them.

The wave id SHALL be determined by the rollout and the wave's position in the configured sequence,
so that a re-resolved or retried wave write addresses the same document instead of creating a
second one. `status` SHALL be one of `dispatching` (membership recorded, update commands being
delivered), `evaluating` (every target has settled or run out of time, and the wave is inside its
health window or being re-measured), `skipped` (a share that added no device), `healthy`,
`unhealthy`, or `failed` (its update commands could not be delivered). `success_rate` SHALL be `0`
until the wave's health has been evaluated and SHALL then carry the evaluated success ratio; a
skipped wave is never evaluated and keeps `0`.

#### Scenario: Wave record content

- **WHEN** a rollout starts one of its canary waves (1%, 5%, 25%, or 100%)
- **THEN** a `waves` document exists with `rollout_id`, `percent`, `status`, a `success_rate` that
  is filled in once the health window has been evaluated, the target `device_ids`, `started_at`,
  and the `failed_device_ids` and `unreported_device_ids` sets

#### Scenario: Wave membership is the record of who was targeted

- **WHEN** a wave's target devices are resolved as the wave starts
- **THEN** the wave document records exactly those device identities, so the wave's health is
  evaluated over the devices it dispatched to rather than over a later re-resolution of the target
  group

#### Scenario: A wave records the devices that did not take the update

- **WHEN** a wave's devices have settled with failures and unreported devices among them
- **THEN** its document names those devices in `failed_device_ids` and `unreported_device_ids`, and
  both arrays are empty for a wave whose devices all succeeded

#### Scenario: A wave that targets no devices is still a record

- **WHEN** a wave resolves an empty target set (for example a canary share smaller than one device)
- **THEN** its document is stored with an empty `device_ids` array and a `started_at`, and its
  health evaluation reports an empty window rather than a healthy wave

#### Scenario: Wave status reflects the wave's life

- **WHEN** a wave is resolved, dispatched, evaluated, and decided
- **THEN** its document's status moves from `dispatching` to `evaluating` to `healthy`,
  `unhealthy`, or `failed`, or is recorded as `skipped` without being dispatched

#### Scenario: A retried wave write converges

- **WHEN** a wave write is retried after the wave's membership or status was already recorded
- **THEN** exactly one document exists for that wave id and its stored fields are the recorded ones

#### Scenario: Waves roll up to their rollout

- **WHEN** a consumer lists the waves of a rollout
- **THEN** exactly the waves belonging to that `rollout_id` are returned, in wave order

#### Scenario: Required fields enforced

- **WHEN** a write attempts to store a wave document missing `rollout_id`, `percent`, `status`,
  `success_rate`, `device_ids`, or `started_at`
- **THEN** the write is rejected and no partial document is stored

### Requirement: Heartbeat telemetry time-series
The system SHALL store heartbeat telemetry in a MongoDB time-series collection named `telemetry`,
keyed by measurement time `ts`, with `device_id` and per-sample metrics (`cpu`, `mem`, `health`)
per document. Telemetry ingestion SHALL be idempotent: each event document is identified by
`_id` = event id, and a redelivered event with an already-stored id MUST be a no-op rather than a
duplicate row or an error.

#### Scenario: Telemetry document content
- **WHEN** a device heartbeat is ingested
- **THEN** a time-series document exists with `ts`, `device_id`, `cpu`, `mem`, and `health`, and is
  retrievable by time range for its device

#### Scenario: Redelivered events are no-ops
- **WHEN** the same telemetry event is delivered twice (for example after a consumer redelivery)
- **THEN** exactly one document exists for that `_id` and the second delivery succeeds as a no-op

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

### Requirement: Telemetry retention policy
Telemetry SHALL have a bounded retention: time-series documents older than the configured
retention period (default: 7 days after their `ts`) MUST be removed automatically by a TTL
index on `ts` without any background job, and the retention period SHALL be a single named
configuration value shared by the ingestion path and the index definition. Domain records —
`devices`, firmware metadata, `rollouts`, `waves`, and `device_state_snapshots` — MUST NOT
expire, and a GridFS firmware binary MUST remain stored as long as its metadata document
references it.

#### Scenario: Old telemetry expires automatically
- **WHEN** a telemetry document's `ts` falls outside the retention period
- **THEN** the database removes it automatically via the TTL index, with no cleanup job running

#### Scenario: Retention is configurable in one place
- **WHEN** the retention period is changed
- **THEN** changing the single named configuration value is sufficient for new deployments to
  pick up the new period

#### Scenario: Domain records never expire
- **WHEN** the retention period elapses many times
- **THEN** `devices`, firmware metadata, `rollouts`, `waves`, and `device_state_snapshots`
  documents and referenced GridFS binaries are all still present

### Requirement: Device workflow state snapshots
The system SHALL store exactly one state snapshot document per device in the
`device_state_snapshots` collection, identified by `_id` = device identity and updated in place —
never a second document per device identity. Every snapshot document MUST contain the fields
`region`, `model`, `current_fw`, `online` (boolean), `last_heartbeat` (date), `config` (the
configuration snapshot with its version), and `snapshot_at` (the time the snapshot was taken);
`pending` (the outstanding command with its delivery state) is present only while a command is
outstanding. Documents missing any required field MUST be rejected at write time. Snapshot
documents are domain records and MUST NOT expire.

#### Scenario: Snapshot document content
- **WHEN** a device workflow's state is snapshotted
- **THEN** its `device_state_snapshots` document is retrievable by `_id` and carries `region`,
  `model`, `current_fw`, `online`, `last_heartbeat`, `config` with its version, and `snapshot_at`

#### Scenario: Snapshots update in place
- **WHEN** one device is snapshotted many times
- **THEN** exactly one document exists for that device identity, holding the newest snapshot's
  content

#### Scenario: Required fields enforced
- **WHEN** a write attempts to store a snapshot document missing a required field
- **THEN** the write is rejected and no partial document is stored

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
