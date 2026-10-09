# Design

## Context

`AgentService.Connect` already routes accepted heartbeats to a `HeartbeatSink` seam
(`internal/agentserver`), and `cmd/controlplane` wires the batched ingest writer
(`internal/telemetry.Writer`) as that sink: telemetry lands in the `telemetry` collection and device
state is refreshed, both idempotently. Nothing in the repository speaks AMQP yet — `go.mod` has no
broker client, `deploy/config.yaml` carries only `rabbitmq.url`, and there is no RabbitMQ manifest
in `deploy/`. The `HeartbeatSink` doc comment already names its intended second implementer: the
stage-3 publisher. `RolloutWorkflow` does not exist, so nothing produces rollout events today.

Constraints the design works under: heartbeat event ids are minted by the agent and reused verbatim
on redelivery (`internal/agent.IDGen`, `Heartbeat.event_id`); the v1 proto is frozen; the control
plane runs as a single process with an `errgroup` lifecycle; the local MongoDB is a standalone node
(no multi-document transactions) and its schema is bootstrapped by `deploy/mongo/init.js`, whose
existing pattern pairs a validator with named-value-driven TTL indexes. Motivation is in
`proposal.md`; the behavior contracts are the six delta specs under `specs/`.

## Goals / Non-Goals

**Goals:**

- One declared broker topology covering heartbeat fan-out and rollout work, with an exponential
  retry ladder and a dead-letter path that are properties of the layout rather than of each
  consumer's code.
- Publication that cannot damage the heartbeat path: the durable ingest write stays the source of
  truth and the broker stays a derived fan-out.
- At-most-once side effects per consumer through a durable, uniqueness-constrained ledger, with
  ack-only-after-durable semantics that survive failures and restarts.
- Backlog and progress visible from the existing metrics endpoint: dead-letter depth, per-queue
  depth, per-consumer lag, and per-event outcomes.
- Testability without a broker for every decision (dedup, backoff, alerting, lag) plus an
  integration harness for everything that is genuinely broker behavior.

**Non-Goals:**

- No rollout producer or consumer. The rollout work queue, its retry queues, and its DLQ are
  declared and bound as the layout, but nothing publishes or consumes them until the rollout
  workflow stage lands and publishes against these names.
- No split deployment of publisher and consumers, no consumer groups or competing-consumer
  sharding (the lag metric assumes one control-plane process — see Decision 6).
- No automatic dead-letter redrive tooling; operators inspect and act.
- No analytics or rollout-health consumers, no per-region rollups — a second consumer is one queue
  declaration plus a handler away.
- No trace propagation through message headers yet: stage 4 owns OTel, and the envelope's headers
  leave room for `traceparent` without a schema change.
- No Grafana dashboard or alerts (stage 4), and no broker auth/TLS beyond what `rabbitmq.url`
  already selects.
- `docs/architecture.md` — also stage-3-triggered in `docs/README.md` — is not written here: it
  spans the stage-4 observability components, so that change owns it. This change owns
  `docs/telemetry.md`, which documents exactly this pipeline.

## Decisions

1. **Publishing is a second sink beside the ingest writer, never a replacement.** `internal/telemetry`
   gains a small ordered fan-out sink (`Fanout`) implementing the serverside `HeartbeatSink`: it
   calls the ingest writer first and the publisher second, so a heartbeat the ingest path refuses is
   never published and every published event has a durable counterpart. `cmd/controlplane` wires
   `agentserver.NewHub(telemetry.NewFanout(ingest, publisher), log)`.
   *Alternatives:* consumers own Mongo persistence (the idea's diagram) — rejected because device
   liveness and device state would start depending on broker availability, the landed ingest writer
   would be reworked, and the dedup ledger would merely duplicate `telemetry._id` idempotence; a
   broker-first publish with persistence on the consumer path — rejected for the same reason.

2. **One topic exchange for both event families, plus dedicated retry and dead-letter exchanges.**
   `fleetops.events` (topic, durable) carries `heartbeat.<region>.<model>` and `rollout.task.<kind>`;
   `fleetops.retry` and `fleetops.dead-letter` are durable direct exchanges. Work queues
   (`fleetops.heartbeat.alerting`, `fleetops.rollout.tasks`), their per-attempt retry queues
   (`<work queue>.retry.<n>`), their DLQs (`<work queue>.dlq`), and every binding are declared
   idempotently by the processes that use them, so the layout lives in code and a fresh broker needs
   no manual setup.
   *Alternatives:* an exchange per family — rejected: two declaration paths for one contract, with
   no routing benefit at this scale; default-exchange work queues — rejected: routing keys and the
   retry wiring would become per-queue special cases; quorum queues with `x-delivery-limit` —
   rejected: they bound redelivery but provide no backoff, which the retry path requires.

3. **The retry ladder is consumer-driven republish into per-attempt TTL queues.** On a handler
   failure the consumer republishes the event to `fleetops.retry` under `<work queue>.retry.<n+1>`,
   waits for that republish's confirm, and only then acknowledges the original delivery; the retry
   queue holds the message for `min(retry_base × 2^(n-1), retry_max)` and dead-letters it back to
   `fleetops.events` under `retry.<work queue>`, a key the work queue is bound to. The consumer sets
   an explicit `x-attempt` header, so the attempt count is deterministic and testable rather than
   inferred from broker bookkeeping.
   *Alternatives:* a broker-driven nack into `<work queue>.retry.<n>` — rejected because RabbitMQ
   fixes a queue's dead-letter routing key at declaration time, so one work queue cannot select
   different delays per attempt without duplicating work queues; `x-death` counting — rejected as
   opaque, broker-managed state that is awkward to assert on; `requeue=true` — rejected as the
   requeue-forever loop the rules forbid; a single retry queue with per-message `expiration` —
   rejected because per-message TTLs only expire at the queue head, so a long delay ahead of a short
   one scrambles the ladder.

4. **The dedup ledger claims first and completes after; an unfinished claim is work, not a
   duplicate.** A consumer inserts a `processed_events` claim (`_id = <consumer>:<event_id>`, with
   `device_id` and `claimed_at`), applies its side effect (idempotent by event id), sets
   `processed_at`, and only then acknowledges. A delivery whose event id is already *processed* is
   acknowledged as a duplicate; a claim *without* `processed_at` is treated as unfinished work and
   re-applied, which is what makes both a failed attempt and a crash mid-processing safe with one
   mechanism, and prevents an event from hiding behind a stale claim. The unique index on
   (`consumer`, `event_id`) makes the record single-writer, and a duplicate-key refusal is
   recognised as a duplicate rather than an error.
   *Alternatives:* marker written only after the side effect — rejected: a crash between the two
   leaves no trace and the event is silently re-applied with no claim to take over; marker without a
   completion time — rejected: a crash after the marker would silently lose the event; Mongo
   transactions pairing ledger and side effect — rejected because the local MongoDB is standalone
   (no multi-document transactions) and because idempotent side effects make them unnecessary.
   Assumption: one consumer goroutine per queue, so the same event id is never processed twice
   concurrently by one consumer — RabbitMQ never delivers one message twice concurrently on one
   channel, and a retry is republished only after the failed delivery is settled.

5. **The alerting consumer's side effect is an idempotent monotone upsert.** Degraded heartbeats
   (`health < alerting.health_threshold`) upsert one `device_alerts` document per device with
   `$min` on `first_seen_at` and `min_health`, `$max` on `last_seen_at`, and `$set` for identity
   fields and the threshold — no counters, no increments, so replaying an event converges to the
   same document. Healthy heartbeats write nothing.
   *Alternatives:* one alert document per degraded event — rejected: alert-per-heartbeat noise with
   no operator-meaningful unit, and it would make the ledger's claim redundant with the document
   key; counter increments — rejected as non-idempotent under at-least-once delivery with a
   standalone MongoDB and no transaction to pair with the ledger.

6. **Lag is measured in events, from the publisher's monotonic sequence.** The publisher stamps every
   event with a `sequence` it increments per event type; the consumer reports
   `fleetops_consumer_lag_events{consumer}` as the publisher's latest sequence for the event types
   that consumer consumes minus the sequence of the last event it processed, clamped at zero. The
   consumer reads the publisher through a one-method `SequenceSource` interface, which holds because
   publisher and consumers share the control-plane process.
   *Alternatives:* queue depth as a lag proxy — rejected: the request asks for both, and depth counts
   undelivered work rather than consumer progress; wall-clock lag (`now − occurred_at`) — rejected:
   it measures staleness of the stream, not how far behind the latest produced event a consumer is;
   broker management API or `x-stream` offsets — rejected: the management plugin is not part of the
   local stack and classic queues expose no such offsets.

7. **Publication is buffered and sheds load with counters; it never blocks the stream.** `Handle`
   enqueues into a bounded buffer (`rabbitmq.publish_buffer`) and returns; a publisher goroutine owns
   the connection, declares the topology, publishes persistent messages as `mandatory` with confirms
   and a returned-message handler, and counts published, dropped (buffer full), failed (nack,
   unroutable, channel error) events. The first drop of a burst is logged; drops are always counted.
   *Alternatives:* blocking the heartbeat path like the ingest writer does — rejected: the ingest
   write is the source of truth and deserves backpressure, while the fan-out is derived, so a broker
   outage must not stall agent streams; unbounded buffering — rejected by the
   block-or-drop-with-a-metric rule.

8. **Broker loss is handled in-process, not by process exit.** Publisher and consumer each supervise
   their own AMQP connection: dial, declare the topology, run; on connection or channel loss they
   back off (doubling, capped — the pattern `internal/telemetry.Writer` already uses for flush
   retries) and re-enter until `ctx` is done, re-declaring the topology because declarations are
   idempotent. Consumers bound in-flight work with `rabbitmq.prefetch` and resume after reconnecting.
   *Alternatives:* fail fast and let the orchestrator restart the process — rejected: a fan-out-only
   dependency restarting RabbitMQ would otherwise take down agent streams, ingest, and the Temporal
   worker with it.

9. **The envelope is a hand-written versioned JSON contract, not a reused gRPC message.** The
   envelope carries `schema_version`, `event_id`, `event_type`, device identity, `occurred_at`,
   `published_at`, `sequence`, and a `payload` object, with the message properties repeating
   `event_id`, `event_type`, `schema_version`, `attempt`, and the original routing key so dead
   letters are diagnosable without decoding. JSON keeps events readable in the RabbitMQ UI, which is
   this stack's debugging surface.
   *Alternatives:* protojson of `agentv1.Heartbeat` — rejected: it couples consumers to a transport
   message the rules say not to reuse across contexts, and freezes the payload to the proto's
   evolution; a new `events/v1` proto package — rejected as codegen and freeze ceremony for a
   payload that is JSON on the wire regardless; binary protobuf — rejected on debuggability, since
   throughput here does not need it.

10. **Behavior knobs are configuration, with defaults that fit the local stack.**
    `rabbitmq.prefetch` 32, `publish_buffer` 1024 (≈5s of slack at the 200 events/s a 1000-device
    fleet at a 5s period produces), `max_attempts` 3, `retry_base` `"5s"`, `retry_max` `"1m"`,
    `queue_depth_interval` `"15s"`, and `alerting.health_threshold` 0.6 — the simulated healthy band
    is 0.85–1.0 and the degraded band 0.2–0.5, so 0.6 separates them cleanly. `retry_max` below
    `retry_base` is rejected at startup so a misconfiguration cannot silently flatten the ladder.

11. **The MongoDB bootstrap gains two collections, and the ledger's dedup guarantee is explicitly
    bounded.** `deploy/mongo/init.js` declares `processed_events` (required fields, unique
    (`consumer`, `event_id`), TTL on `claimed_at` driven by the named
    `FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS`, default 7 — mirroring the telemetry retention
    pattern) and `device_alerts` (required fields, no TTL). Past the retention window a redelivery is
    re-applied; that is harmless and is exactly why every consumer side effect must be idempotent by
    construction, with the ledger serving as the fast path and the audit trail.

12. **Broker behavior is tested against a real broker; decisions are tested without one.** The
    consumer loop stays thin glue over `amqp091-go`: envelope encoding, dedup decisions, backoff
    selection, alert upserts, lag accounting, and retry/dead-letter routing are ordinary units tested
    with hand-written fakes, keeping domain-logic coverage at the 80% bar without a broker. Bindings,
    TTL delays, DLQ routing, persistence across a broker restart, prefetch, and depth sampling are
    covered by a new `//go:build integration` harness in `internal/rabbittest` that boots a RabbitMQ
    container with `testcontainers-go`, parallel to the existing `internal/mongotest` harness.

## Risks / Trade-offs

- [Broker outage loses derived events] → The durable ingest path remains the source of truth, every
  drop and failure is counted, the first drop of a burst is logged, and Mongo telemetry can rebuild
  derived state; the default buffer covers ~5s at the demo's fleet scale.
- [Dropping events when the buffer is full] → Deliberate load shedding, per the block-or-drop rule:
  a full buffer means the broker is far behind, and stalling agent streams would be worse. Both the
  drop counter and the drop log line make it observable rather than silent.
- [The ledger grows with every event] → TTL retention (7 days by default, one named value) bounds
  it; the hot lookup is a unique-index point read.
- [Dedup guarantee ends at the retention window] → Side effects are idempotent by construction, so a
  redelivery after expiry converges instead of corrupting; documented in the data-model spec rather
  than implied.
- [A retry storm during a partial outage] → The ladder is capped by `retry_max` and bounded by
  `max_attempts`, terminal messages stop consuming broker resources in the DLQ, and both retry-queue
  depth and dead-letter depth are metrics.
- [Head-of-line blocking in the retry path] → One retry queue per attempt, so every message in a
  queue shares the same TTL and no message is stuck behind a longer delay.
- [A poison message hides a real bug] → Dead letters carry the failure reason, attempt count, and
  original routing key; DLQ depth is a metric; an integration test asserts the whole path.
- [In-process lag ties publisher and consumers to one process] → Accepted and documented; a split
  deployment would publish the sequence elsewhere (broker-side counter or a state document) before
  the metric is trusted across processes.
- [Consumer writes to Mongo while the ingest writer does] → Different collections, one small upsert
  per degraded heartbeat only, no shared document and no cross-write ordering requirement.
- [New dependency `github.com/rabbitmq/amqp091-go`] → The AMQP 0-9-1 client maintained by the
  RabbitMQ team, named by the project stack; no cgo. `testcontainers-go` is already a test
  dependency.
- [A consumer crash between side effect and `processed_at`] → The claim stays unfinished and the
  redelivered event is re-applied; because side effects are idempotent, this converges.

## Migration Plan

- **Configuration**: the new `rabbitmq` fields and the `alerting` section are added to
  `deploy/config.yaml`; absent keys take the documented defaults, so existing files keep working
  unchanged.
- **Schema**: `deploy/mongo/init.js` gains the two collections, their validators, indexes, and the
  ledger TTL; the local stack applies it on first startup, and an existing deployment needs the same
  declarations applied once (extend `deploy/mongo/verify.sh` to check them). No backfill: the ledger
  and alerts start empty.
- **Topology**: declared by `cmd/controlplane` at startup, so there is no broker-side migration step
  and no hand-declared queues to keep in sync.
- **Rollback**: revert the binary. The additional exchanges and queues are inert once nothing
  publishes, and the two collections are additive and harmless to leave in place; deleting them is
  optional cleanup.
- **Archive ordering**: this delta MODIFIES `runtime-config` and `mongo-data-model`, whose main specs
  are created by older, still-unarchived changes (`add-runtime-config-and-health-endpoints`,
  `add-repo-scaffold-and-data-model`, and the changes that later modified those requirements).
  Archive those first — or together — otherwise `openspec archive` refuses this delta with "target
  spec does not exist", the same constraint the heartbeat-persistence change recorded.

## Open Questions

- Whether `fleetops.rollout.tasks` should carry the rollout saga's *commands* or only wave-task
  notifications is deferred until `RolloutWorkflow` exists; `rollout.task.<kind>` is the contract and
  the binding can be narrowed then without touching the publisher.
- Whether dead letters should be redriven automatically after an operator fix (a shovel or an
  administrative command) is deferred to the ops/UI stage, which owns operator tooling.
- Whether the lag metric should be published by the broker or a shared store if publisher and
  consumers are ever split across processes (see Decision 6).
