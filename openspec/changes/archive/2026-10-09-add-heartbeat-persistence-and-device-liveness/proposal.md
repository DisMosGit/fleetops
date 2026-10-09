# Proposal

## Why

Agents already open `AgentService.Connect` streams and emit heartbeats, but the control plane
keeps that state in memory only: a device's record is never written to MongoDB, its heartbeat
samples never reach the `telemetry` collection, and a device that silently disappears is still
counted as online until its stream happens to close. Nothing survives a control-plane restart,
and there is no data for the health windows, dashboards, or the eligible-device pool to read.
This change makes the fleet's state durable and its liveness observable: the database becomes
the source of truth for who is online and what they reported.

## What Changes

- On a successful registration exchange over a new stream connection, the control plane
  upserts the `devices` document for that device: identity fields (`model`, `region`,
  `current_fw`), `status: "online"`, and the last-seen timestamp (`last_heartbeat`), refreshed
  on every accepted heartbeat. A rejected registration writes nothing.
- Every accepted heartbeat is persisted to the `telemetry` time-series collection with its
  measurement time (`ts`), meta fields (`meta.device_id`, `meta.region`, `meta.model`), and
  metric samples (`cpu`, `mem`, `health`), keyed by `_id` = event id so redelivery is a no-op.
- Telemetry writes are batched (size- and time-bounded flush) so ingestion does not pay one
  database round trip per heartbeat; a batch containing already-stored event ids succeeds as a
  partial no-op rather than failing.
- A background liveness sweep marks a device `offline` when no heartbeat has been accepted for
  it within a configurable threshold, and every online→offline transition increments a
  Prometheus counter served on the configured metrics address.
- Configuration grows the knobs this behavior needs (offline threshold, sweep cadence, ingest
  batch size and flush interval), validated like the existing fields and documented in
  `deploy/config.yaml`.

## Capabilities

### New Capabilities

- `device-registry`: device-record lifecycle tied to agent streams — registering or updating
  the `devices` document on a registration exchange, last-seen tracking, and online marking.
- `telemetry-ingest`: idempotent, batched persistence of heartbeat events into the `telemetry`
  time-series collection with correct timestamps and meta fields.
- `device-liveness`: heartbeat-staleness detection — marking devices offline after a
  configurable threshold and exposing the transition as a metric.

### Modified Capabilities

- `runtime-config`: the shared YAML schema gains the `liveness` and `telemetry` sections
  (`liveness.offline_threshold`, `liveness.sweep_interval`, `telemetry.batch_size`,
  `telemetry.flush_interval`) with defaults and validation, so the behavior knobs are
  configuration values rather than scattered constants.

## Impact

- **Code**: `internal/devices` (new — registry upserts, liveness sweep), `internal/telemetry`
  (batched ingest writer), `internal/agentserver` (hook registration/heartbeat handling to
  persistence through interfaces defined at the consumer), `cmd/controlplane` (wire MongoDB
  client, ingest writer, sweep loop, metrics endpoint), `internal/config` + `deploy/config.yaml`
  (new sections and validation). `internal/agent` is unchanged.
- **Dependencies**: `go.mongodb.org/mongo-driver` (MongoDB persistence) and
  `prometheus/client_golang` (the requested transition metric and its exposition) — both
  already named in the project's stack.
- **Contracts**: none. The v1 proto is untouched; the gRPC behavior of `Connect` keeps its
  current acceptance and routing semantics, persistence is additive.
- **Storage**: no schema change — `deploy/mongo/init.js` already defines `devices` and
  `telemetry` with their validators and indexes. Note that `telemetry` is deliberately a
  plain collection in time-series shape (`ts` + `meta` + top-level samples) with a TTL index,
  because native MongoDB time-series collections have no unique `_id` index and cannot enforce
  the redelivery-no-op contract (probe evidence in `add-repo-scaffold-and-data-model/notes.md`).
  This change ingests into that existing collection.
- **Ops**: the metrics listener (`observability.metrics_addr`) starts serving `/metrics`;
  liveness and ingest logs land at the gRPC/ingest boundaries per the logging rules.
