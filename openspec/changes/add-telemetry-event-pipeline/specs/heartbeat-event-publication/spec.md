# Spec Delta

## Purpose

Turns every accepted heartbeat into a persistent broker event with a stable identifier, a versioned
envelope, and a monotonic sequence, published beside the durable ingest path so consumers can
deduplicate and route events without the fleet's heartbeat acceptance depending on the broker.

## ADDED Requirements

### Requirement: Accepted heartbeats are published
Every heartbeat accepted for a registered device SHALL be published to `fleetops.events` with
routing key `heartbeat.<region>.<model>` built from the device's registered region and model, using
the heartbeat's own event identifier. Publication SHALL happen in addition to the durable ingest
write and SHALL NOT replace it: the publisher receives a heartbeat only after the ingest path has
accepted it for storage, and a heartbeat the ingest path refuses SHALL NOT be published.

#### Scenario: One accepted heartbeat publishes one event
- **WHEN** a registered device sends a heartbeat that the ingest path accepts
- **THEN** exactly one event is published, on the routing key for that device's region and model,
  carrying the heartbeat's event id

#### Scenario: Publication follows ingest acceptance
- **WHEN** the ingest path refuses a heartbeat
- **THEN** no event is published for it

#### Scenario: Redelivered heartbeats are published with their original identity
- **WHEN** an agent resends a heartbeat with its original event id after a stream reconnect
- **THEN** the published event carries that same event id, so a consumer can recognise it as an
  event it has already processed

### Requirement: Versioned event envelope
Every published event SHALL carry a consistent envelope: `schema_version` (an integer, `1` for this
revision), `event_id` (the heartbeat's stable identifier), `event_type` (`heartbeat`, and the
routing family for rollout events), the device identity (`device_id`, `region`, `model`),
`occurred_at` (the heartbeat's measurement time), `published_at` (the control plane's publish time),
a publisher-monotonic `sequence`, and a `payload` object with the heartbeat samples (`cpu`, `mem`,
`health`), `current_fw`, and `status`. Within one `schema_version` a field SHALL NOT be renamed,
repurposed, or given a different meaning; a changed shape SHALL be published under a new
`schema_version`. Messages SHALL be persistent and SHALL declare a JSON content type, and the event
id, event type, schema version, and attempt count SHALL additionally be readable from message
properties without decoding the payload.

#### Scenario: Envelope carries identity, timing, and samples
- **WHEN** a heartbeat for a device registered in region `eu-west`, model `v3` is published
- **THEN** the event carries `schema_version` 1, the event id, `event_type` `heartbeat`,
  `device_id`, `region` `eu-west`, `model` `v3`, the heartbeat's measurement time as `occurred_at`,
  a `published_at` not earlier than it, and the reported `cpu`, `mem`, `health`, `current_fw`, and
  `status` in its payload

#### Scenario: Sequence orders events within a publisher
- **WHEN** one publisher publishes several events
- **THEN** each event's `sequence` is greater than the previous event's, and two events never share
  a sequence

#### Scenario: Message properties identify the event
- **WHEN** a consumer inspects a delivery without decoding its payload
- **THEN** the event id, event type, schema version, and attempt count are readable from the
  delivery's properties

### Requirement: Broker unavailability does not break heartbeat acceptance
Publication SHALL be bounded and non-blocking for the heartbeat path: when the broker is unreachable
or the publish buffer is full, the heartbeat SHALL still be accepted and persisted, and heartbeat
acceptance SHALL NOT block on, nor fail because of, broker availability. Every event that could not
be published SHALL be counted rather than dropped silently, and the first drop of a burst SHALL be
logged with the reason; once the broker is reachable again, subsequent heartbeats SHALL be published
again without a restart.

#### Scenario: Unreachable broker degrades the fan-out only
- **WHEN** the broker is unreachable while heartbeats keep arriving
- **THEN** heartbeats are still accepted and persisted, the dropped-publication counter rises, and
  the heartbeat path does not block waiting for the broker

#### Scenario: Full publish buffer sheds load instead of stalling streams
- **WHEN** events are published faster than the broker accepts them and the buffer fills
- **THEN** further events are counted as dropped instead of blocking or growing memory without
  bound

#### Scenario: Publication resumes after the broker returns
- **WHEN** the broker becomes reachable again
- **THEN** subsequent heartbeats are published again, with no restart of the control plane

### Requirement: Publish outcome is confirmed, not assumed
The publisher SHALL use publisher confirms and SHALL treat a broker-side rejection (a nack) and a
message returned as unroutable as publication failures, each counted and logged with the event's
routing key. Only a confirmed publish SHALL count as a published event. The publisher SHALL NOT
abort the process on a failed publish, a returned message, or a lost broker connection; it SHALL
reconnect with backoff and re-establish its topology before publishing again.

#### Scenario: Confirmed publish counts as published
- **WHEN** the broker confirms an event's publish
- **THEN** the published counter for that event type increases by one

#### Scenario: Unroutable event is reported, not swallowed
- **WHEN** an event is published with a routing key that matches no binding
- **THEN** the broker returns it, the publisher counts it as a failed publication, and the routing
  key appears in a log line

#### Scenario: Lost connection is retried, not fatal
- **WHEN** the connection to the broker drops
- **THEN** the publisher reconnects with backoff, re-declares the topology, and keeps running
