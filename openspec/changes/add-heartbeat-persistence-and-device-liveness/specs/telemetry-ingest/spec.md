# Spec Delta

## Purpose

Persists heartbeat telemetry durably: every accepted heartbeat lands in the `telemetry`
time-series collection with its measurement time, device meta fields, and samples, through a
batched, idempotent write path that redeliveries cannot corrupt.

## ADDED Requirements

### Requirement: Heartbeat event persistence
Every heartbeat accepted for a registered device SHALL be stored as exactly one document in the
`telemetry` collection, keyed by `_id` = the heartbeat's event id, with `ts` set to the
heartbeat's measurement time, the meta fields `meta.device_id`, `meta.region`, and `meta.model`
set from the device's registered identity, and the samples `cpu`, `mem`, and `health` stored
as top-level fields. An event is stored once no matter how many times it is delivered.

#### Scenario: Heartbeat lands with timestamp and meta
- **WHEN** a registered device sends a heartbeat with event id, measurement time, and samples
- **THEN** one `telemetry` document exists with that `_id`, `ts` equal to the heartbeat's
  measurement time, `meta` carrying the device id and the region and model recorded at
  registration, and `cpu`, `mem`, `health` as reported

#### Scenario: Meta comes from the registered identity
- **WHEN** a heartbeat is persisted for a device registered in region `eu-west`, model `v3`
- **THEN** the stored document's `meta.region` is `eu-west` and `meta.model` is `v3`,
  regardless of anything else the event carries

#### Scenario: Heartbeats are queryable by device and time range
- **WHEN** a consumer queries `telemetry` for one device over a time range
- **THEN** exactly that device's events within the range are returned in measurement-time order

#### Scenario: Missing measurement time is not stored
- **WHEN** a heartbeat arrives without a measurement time
- **THEN** it is rejected before persistence, and neither a telemetry document nor a last-seen
  refresh results from it

### Requirement: Idempotent ingestion
Redelivery of an already-stored event SHALL be a no-op: the second delivery stores no second
document, fails no batch, and surfaces no error to the stream. This SHALL hold when a redelivered
event arrives inside a batch that also carries new events — the new events are stored and the
duplicate is ignored. Ingestion SHALL tolerate already-stored event ids in the write path rather
than filtering them out with a prior read.

#### Scenario: Redelivered event is a no-op
- **WHEN** an event id already present in `telemetry` is delivered again
- **THEN** exactly one document exists for that `_id` and the delivery is reported successful

#### Scenario: Mixed batch succeeds partially
- **WHEN** one write batch contains both already-stored and never-stored event ids
- **THEN** the new events are stored, the duplicates change nothing, and the batch completes
  without error

#### Scenario: Redelivery after a control-plane restart
- **WHEN** an agent resends heartbeats with their original event ids after the control plane
  restarted mid-stream
- **THEN** the stored collection holds exactly one document per event id

### Requirement: Batched telemetry writes
Telemetry SHALL reach the database in batches rather than one round trip per heartbeat: each
flush writes the accumulated events in a single write, bounded by a configurable batch size and
a configurable flush interval so a quiet fleet still persists promptly. The pipeline between
accepting a heartbeat and its batched write SHALL be bounded — a slow database applies
backpressure to the accepting path instead of growing an unbounded buffer or silently dropping
accepted events.

#### Scenario: Steady heartbeats are written in batches
- **WHEN** heartbeats arrive steadily above the flush rate
- **THEN** they are persisted in writes carrying many events each, not one write per heartbeat

#### Scenario: Quiet fleet still persists promptly
- **WHEN** fewer events than the batch size arrive within the flush interval
- **THEN** the pending events are written when the flush interval elapses

#### Scenario: Slow database does not grow unbounded memory
- **WHEN** the database stalls while heartbeats keep arriving
- **THEN** the ingest pipeline stops accepting beyond its bound (the stream feels backpressure)
  and no accepted event is dropped silently
