# Spec Delta

## Purpose

Defines the MongoDB persistence contract for the fleet: the `devices`, firmware metadata,
`rollouts`, `waves`, and `telemetry` collections with their required fields, the indexing strategy
that serves device identity, region+model, and status lookups, and the retention policy for
telemetry.

## ADDED Requirements

### Requirement: Device records
The system SHALL store exactly one heterogeneous document per device in the `devices` collection,
identified by `_id` = device identity. Every device document MUST contain the fields `model`,
`region`, `current_fw` (the firmware version the device runs), `status`, and `last_heartbeat`.
Documents missing any required field MUST be rejected at write time.

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

### Requirement: Firmware metadata records
The system SHALL store one document per firmware in a firmware metadata collection, identified by
`_id` = firmware id. Every firmware document MUST contain `version`, `checksum`, and `gridfs_id`
referencing the binary stored in GridFS, and MUST NOT contain the binary payload itself. A
firmware document whose `gridfs_id` does not resolve to a GridFS object MUST be rejected at write
time.

#### Scenario: Firmware metadata with binary reference
- **WHEN** an operator uploads a firmware binary
- **THEN** the binary lands in GridFS and the metadata document records `version`, `checksum`, and
  the `gridfs_id` of the stored object

#### Scenario: Metadata never holds payloads
- **WHEN** a firmware document is read
- **THEN** it exposes `version`, `checksum`, and `gridfs_id` but no inline binary content

### Requirement: Rollout records
The system SHALL store one document per rollout in the `rollouts` collection, identified by
`_id` = rollout id. Every rollout document MUST contain `firmware_id`, `status`, and
`temporal_wf_id` (the id of the workflow execution driving the rollout), and MUST record the
target group as `region` plus `model`.

#### Scenario: Rollout record content
- **WHEN** an operator starts a rollout for a target group
- **THEN** its `rollouts` document is retrievable by `_id` and carries `firmware_id`, `status`,
  `temporal_wf_id`, `region`, and `model`

#### Scenario: Rollout status transitions are recorded
- **WHEN** the rollout workflow advances or pauses the rollout
- **THEN** the `status` field of the existing rollout document reflects the new state

### Requirement: Wave records
The system SHALL store one document per wave in the `waves` collection, identified by `_id` =
wave id. Every wave document MUST contain its parent `rollout_id`, `percent` (the canary share of
the target group), `status`, and `success_rate` (health measured over the wave's health window),
and waves of one rollout SHALL be retrievable in rollout order.

#### Scenario: Wave record content
- **WHEN** a rollout starts one of its canary waves (1%, 5%, 25%, or 100%)
- **THEN** a `waves` document exists with `rollout_id`, `percent`, `status`, and a
  `success_rate` that is filled in once the health window has been evaluated

#### Scenario: Waves roll up to their rollout
- **WHEN** a consumer lists the waves of a rollout
- **THEN** exactly the waves belonging to that `rollout_id` are returned, in wave order

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
Every collection above SHALL carry indexes matching its access patterns so that no hot query
requires a collection scan: `devices` by device identity (unique `_id`), by `region` + `model`, and
by `status`; firmware metadata by `version` (unique) and `_id`; `rollouts` by `status` and by
`firmware_id`; `waves` by `rollout_id` (+ wave order); and `telemetry` by `device_id` + `ts` and by
`ts` within `region` + `model` meta. Writes (including heartbeat ingest at fleet scale) SHALL NOT
be rejected because of index conflicts on redelivery.

#### Scenario: Device identity lookup
- **WHEN** code resolves a device by its identity
- **THEN** the lookup is served by the unique `_id` index on `devices`, and a second device
  document with the same identity cannot be inserted

#### Scenario: Region and model pool queries
- **WHEN** the eligible-device pool is recalculated or a rollout resolves its target group
- **THEN** the `region` + `model` query on `devices` and the `region` + `model` time-range query on
  `telemetry` are each served by an index

#### Scenario: Status queries
- **WHEN** code lists devices or rollouts filtered by `status`
- **THEN** the query is served by a `status` index on the queried collection

### Requirement: Telemetry retention policy
Telemetry SHALL have a bounded retention: time-series documents older than the configured
retention period (default: 7 days after their `ts`) MUST be removed automatically by a TTL
index on `ts` without any background job, and the retention period SHALL be a single named
configuration value shared by the ingestion path and the index definition. Domain records —
`devices`, firmware metadata, `rollouts`, and `waves` — MUST NOT expire, and a GridFS firmware
binary MUST remain stored as long as its metadata document references it.

#### Scenario: Old telemetry expires automatically
- **WHEN** a telemetry document's `ts` falls outside the retention period
- **THEN** the database removes it automatically via the TTL index, with no cleanup job running

#### Scenario: Retention is configurable in one place
- **WHEN** the retention period is changed
- **THEN** changing the single named configuration value is sufficient for new deployments to
  pick up the new period

#### Scenario: Domain records never expire
- **WHEN** the retention period elapses many times
- **THEN** `devices`, firmware metadata, `rollouts`, `waves` documents and referenced GridFS
  binaries are all still present