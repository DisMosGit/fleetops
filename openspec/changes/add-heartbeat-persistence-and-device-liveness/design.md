# Design

## Context

`AgentService.Connect` already accepts streams, validates registration exchanges, and routes
heartbeats to a `HeartbeatSink` seam (`internal/agentserver`), but the sink is a logging stub in
`cmd/controlplane` and nothing touches MongoDB. `deploy/mongo/init.js` already bootstraps
`devices` and `telemetry` with validators and indexes; `telemetry` is deliberately a plain
collection in time-series shape (`_id` = event id, `ts`, `meta: {device_id, region, model}`,
top-level samples) with a TTL index, because native time-series collections have no unique
`_id` index and cannot enforce the redelivery-no-op contract (probe evidence in
`openspec/changes/add-repo-scaffold-and-data-model/notes.md`). Agent heartbeats run at a 5s
default period (`internal/agent.DefaultPeriod`) for up to a few thousand devices per process.
See `proposal.md` for motivation and the four delta specs for the behavior contract.

## Goals / Non-Goals

**Goals:**

- Durable device records and heartbeat telemetry behind the existing stream handling, with no
  change to the v1 proto or the stream's acceptance/routing semantics.
- One write path that batches both kinds of writes (telemetry inserts, device-state updates),
  bounded in memory and flush latency.
- Server-side liveness timestamps only — the sweep's judgments never depend on agent clocks.
- A single, exactly-once-per-transition offline counter, scrapeable from
  `observability.metrics_addr`.

**Non-Goals:**

- RabbitMQ fan-out, DLQ, and the analytics/alerting consumers — the ingest writer is the seam
  those plug into later; nothing in this change speaks AMQP.
- Heartbeat-reported device status semantics beyond presence (`online`/`offline`); rollout
  states like `updating` belong to the Temporal stage.
- Change-stream-driven recalculation of the eligible-device pool (needs the pool first).
- Multi-instance control planes; last-write-wins on a single writer is the model.

## Decisions

1. **Persistence hooks are interfaces defined at the consumer, implemented in new packages.**
   `internal/agentserver` grows two consumed interfaces: the existing `HeartbeatSink` (its
   `Handle` gains the device's registered identity — region/model — so ingest can fill meta
   fields without a read) and a small registry interface for the registration exchange
   (`Upsert` on accepted registration, nothing on rejection). `internal/devices` implements
   the registry against MongoDB; `internal/telemetry` implements `HeartbeatSink` with its
   batched writer, whose flush also carries the per-heartbeat device-state updates.
   `cmd/controlplane` wires both and replaces the `loggingSink` stub outright — the shared
   config always carries MongoDB settings, so there is no non-database mode to keep a stub for.
   *Alternative:* pass identity by looking the device up at ingest time — rejected: one extra
   read per heartbeat at 5k/s for data the stream already has.

2. **Timestamps: agent time for measurement, server time for liveness.** `telemetry.ts` is the
   heartbeat's `ts` (it keys the measurement and the TTL index); `devices.last_heartbeat` is
   the control plane's acceptance time (seeded by the accepted registration, refreshed per
   heartbeat). Agent clock skew therefore cannot make a live device look stale. A heartbeat
   without `ts` is rejected before persistence — a zero `ts` would be TTL-expired instantly
   (silent data loss), and a fabricated one would corrupt the measurement contract. Consistent
   with the existing handler, rejection is an `InvalidArgument` stream failure.

3. **One bounded ingest pipeline, coalesced flush.** A single writer goroutine per process
   (owned by the `errgroup`, stopped by `ctx`) drains a bounded channel of pending work:
   telemetry events accumulate into `InsertMany` (unordered) batches capped by
   `telemetry.batch_size` and forced out after `telemetry.flush_interval`; device-state updates
   coalesce to one write per device per flush (`$max` on `last_heartbeat` plus `$set` of
   `status`/`current_fw` — last-write-wins, monotonic timestamp) through `internal/devices`'
   batch primitive, so one accepted heartbeat lands both its telemetry document and its
   last-seen refresh in the same flush. When the channel is full the
   gRPC receive path blocks: that is gRPC flow-control backpressure onto the agent, per the
   "block or drop with a metric" rule — blocking chosen because silently dropped heartbeats
   would corrupt both liveness and telemetry. On shutdown the writer flushes what it holds
   before returning. *Alternatives:* one `InsertOne` per event (rejected: the round trips this
   change exists to avoid); a per-device writer goroutine (rejected: fleet-scale goroutines for
   no gain).

4. **Idempotence at the write, not a pre-read.** Unordered `InsertMany` with `_id` = event id;
   a `WriteException` whose write errors are all duplicate-key (E11000) is success — the new
   rows landed and the redeliveries are no-ops. Any other write error is wrapped and retried
   with backoff until `ctx` is done (batches are small and idempotent by construction, so a
   retry cannot double-store). *Alternative:* query-then-insert filtering (rejected: doubles
   round trips and races under concurrency).

5. **Offline sweep as a periodic `UpdateMany`, not timers or change streams.** A ticker at
   `liveness.sweep_interval` runs
   `devices.updateMany({status: "online", last_heartbeat: {$lt: now - threshold}}, {$set: {status: "offline"}})`
   and feeds `ModifiedCount` to the counter — one query per sweep for the whole fleet, and the
   modified count is exactly the number of transitions. Change streams cannot detect an
   *absence* of heartbeats; per-device timers lose state on restart and don't scale. The sweep
   also covers streams that stayed open but went quiet, which a disconnect hook would miss.

6. **The transition metric is a plain Prometheus counter on a private registry.**
   `prometheus/client_golang` with a `prometheus.NewRegistry()` (not the global registry) wired
   in `cmd/controlplane`, exposed by `promhttp` on `observability.metrics_addr`. The counter
   `fleetops_device_offline_transitions_total` increments by `ModifiedCount` after each sweep,
   so it counts transitions, never sweep passes. Interceptor-level metrics remain stage-4
   scope; this change adds only the requested source-backed counter and its endpoint.

7. **Config, not constants, for the behavior knobs.** `liveness.offline_threshold`,
   `liveness.sweep_interval`, `telemetry.batch_size`, `telemetry.flush_interval` join the
   shared YAML schema (validated as positive durations/integers, documented in
   `deploy/config.yaml`), per the modified `runtime-config` spec. Threshold default `30s`
   (six missed heartbeats at the 5s agent period), sweep `10s`, batch `500`, flush `1s`.

8. **No schema or bootstrap changes.** `deploy/mongo/init.js` already defines everything this
   change writes; the store trusts that bootstrap (the local stack mounts it on first startup
   and `deploy/mongo/verify.sh` checks it). Indexes used: `devices` unique `_id` plus
   `status`, `telemetry` `meta.device_id + ts` and TTL `ts`.

## Risks / Trade-offs

- [Per-heartbeat device-state writes at fleet scale] → Coalesced to one write per device per
  flush window and batched with the telemetry writes; worst case is one `bulkWrite` per flush.
- [Blocking the receive path on a slow database stalls streams] → Bounded channel plus gRPC
  flow control is the intended backpressure; the alternative (drop-with-metric) silently
  corrupts liveness, which this change exists to make trustworthy.
- [Sweep vs. flush race on `status`] → Both write with server timestamps and `$max`
  `last_heartbeat`; the sweep only flips devices whose last-seen predates the cutoff, and a
  concurrently flushed heartbeat marks the device online again, so the record converges to the
  truth and any spurious transition is corrected by the next heartbeat.
- [Duplicate-key tolerance masks real `_id` collisions] → Only error code 11000 is treated as
  a no-op and only for event ids delivered by the same agent stream; every other write error
  fails the batch loudly and is retried.
- [Shutdown loses an unflushed batch] → The writer drains and flushes on `ctx` cancellation
  before the process exits; the agent's own redelivery after reconnect is the backstop (same
  event ids, so redelivery is a no-op at worst).
- [New dependencies: `go.mongodb.org/mongo-driver`, `prometheus/client_golang` (+ test-only
  `testcontainers-go`)] → Both are named in the project stack; the PR description carries the
  one-line justifications. No cgo.

## Migration Plan

Local single-node stack: add the four config keys to `deploy/config.yaml` (absent keys take
defaults, so existing files keep working), restart `cmd/controlplane`. The database schema is
already bootstrapped — no migration, no backfill: devices appear as agents (re)register and
heartbeats land from then on. Rollback is reverting the binary; the data written is additive
and harmless to leave in place.

Archiving note: the `runtime-config` delta is a MODIFIED operation, but that capability's main
spec is created by the older, already-complete change `add-runtime-config-and-health-endpoints`
(`openspec/specs/` is still empty). Archive that change first — or both together — otherwise
`openspec archive` refuses this delta with "target spec does not exist".

## Open Questions

- Whether the heartbeat-reported `status` string should land in `devices.status` alongside
  presence is deferred to the rollout/health stage, where its vocabulary is defined; this
  change treats `devices.status` as presence only.
