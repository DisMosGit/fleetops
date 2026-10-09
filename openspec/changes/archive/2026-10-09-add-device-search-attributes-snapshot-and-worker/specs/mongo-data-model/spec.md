# Spec Delta

## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Indexing strategy
Every collection in this data model SHALL carry indexes matching its access patterns so that no
hot query requires a collection scan: `devices` by device identity (unique `_id`), by
`region` + `model`, and by `status`; firmware metadata by `version` (unique) and `_id`;
`rollouts` by `status` and by `firmware_id`; `waves` by `rollout_id` (+ wave order); `telemetry`
by `device_id` + `ts` and by `ts` within `region` + `model` meta; and `device_state_snapshots` by
device identity (unique `_id`), by `region` + `model`, and by `online`. Writes (including
heartbeat ingest at fleet scale) SHALL NOT be rejected because of index conflicts on redelivery.

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
