# Proposal

## Why

Heartbeats already travel from agents to durable storage, but only along one hardwired path: the
gRPC stream hands each accepted heartbeat to the ingest writer, which batches it into MongoDB.
Nothing is published to the broker, so no consumer can act on telemetry without being spliced into
the stream handler, and the project's promised event pipeline — fan-out to analytics, alerting,
and rollout-health consumers with a DLQ for poison messages — does not exist yet. That is the
remaining stage-3 work, and the health aggregation and rollout stages both depend on it: they need
heartbeat events on a topic exchange, not a callback inside the control plane.

A broker fan-out is only trustworthy if a redelivered event cannot cause a second effect and a
message that keeps failing is visible instead of looping forever. So this change delivers the whole
contract, not just a publish call: a declared topology with a retry ladder and dead-letter path, a
versioned envelope with a stable event identifier, one deduplicating consumer, and the metrics that
make backlog and dead letters observable.

## What Changes

- **Broker topology is declared, not assumed.** One topic exchange carries both event families:
  `fleetops.events` with heartbeat routing keys (`heartbeat.<region>.<model>`) and rollout work
  routing keys (`rollout.task.<kind>`). A direct retry exchange and a direct dead-letter exchange
  complete the layout, with work queues, per-attempt retry queues carrying exponential TTL
  backoff, dead-letter queues, and the bindings that return an expired retry to its work queue.
  Every element is declared idempotently at startup by the publishing and consuming processes, so
  the topology is code, not a hand-run broker command. The rollout work queue and its retry/DLQ
  wiring are part of the layout; its producer and consumer arrive with the rollout workflow, which
  does not exist yet.
- **Heartbeats are published with a stable identity and a consistent envelope.** Every accepted
  heartbeat is published to `fleetops.events` as a persistent JSON event carrying a schema
  version, the heartbeat's stable `event_id`, the event type, device identity (id, region, model),
  the measurement time, the publish time, a publisher-monotonic `sequence`, and the samples.
  Publishing runs beside — never instead of — the durable ingest write, uses publisher confirms,
  and drops with a counter rather than blocking the agent stream when the broker is unreachable or
  the publish buffer is full.
- **One consumer deduplicates by event identifier.** The `fleetops.heartbeat.alerting` consumer
  acknowledges a delivery only after its side effect is durable, and deduplicates on
  (consumer, event id) through a `processed_events` collection whose uniqueness constraint makes a
  redelivery a counted no-op. Its side effect is one durable alert document per degraded device,
  written with an idempotent monotone upsert.
- **Failures retry with backoff and then dead-letter.** A handler failure is republished into the
  consumer's retry ladder — a per-attempt queue whose message TTL is the exponential backoff delay
  — and the delivery is acknowledged only after the republish is confirmed. A message that exhausts
  `max_attempts`, and any delivery that cannot be republished at all, lands in the consumer's
  dead-letter queue instead of being requeued forever or dropped silently.
- **Broker lag and backlog are metrics.** The control plane exposes queue depth per queue (work,
  retry, and dead-letter, so DLQ depth is directly scrapeable), consumer lag as the number of
  events published after the last event each consumer processed, and event throughput counters for
  published, dropped, and consumed-by-outcome.
- **Configuration grows the knobs this behavior needs** — prefetch, publish buffer, attempt limit,
  retry base and cap, queue-depth sampling cadence, and the alerting health threshold — validated
  like the existing fields and documented in `deploy/config.yaml`.
- **The data model gains two collections** with validators and indexes: the `processed_events`
  dedup ledger (bounded retention) and `device_alerts`. `docs/telemetry.md`, whose docs-plan
  trigger milestone is stage 3, documents the topology and consumer semantics.

## Capabilities

### New Capabilities

- `message-broker-topology`: the exchange, queue, and binding layout for heartbeat and rollout
  events — routing-key grammar, the exponential retry ladder, the dead-letter path, and idempotent
  declaration of the whole topology at process startup.
- `heartbeat-event-publication`: turning an accepted heartbeat into a persistent, versioned broker
  event with a stable identifier and consistent envelope, published alongside the durable ingest
  path without letting broker unavailability break heartbeat acceptance.
- `heartbeat-consumption`: the consuming side — dedup by event identifier against a
  uniqueness-constrained processed-events ledger, acknowledge-only-after-durable-side-effect,
  retry-with-backoff and terminal dead-lettering of failed deliveries, and the alerting consumer's
  degraded-device alert behavior.
- `broker-observability`: the pipeline's Prometheus sources — dead-letter queue depth, per-queue
  backlog, per-consumer lag relative to the latest produced event, and published/consumed event
  counters.

### Modified Capabilities

- `runtime-config`: the shared YAML schema gains the `rabbitmq` pipeline fields (`prefetch`,
  `publish_buffer`, `max_attempts`, `retry_base`, `retry_max`, `queue_depth_interval`) and a new
  `alerting` section (`health_threshold`) with defaults and validation.
- `mongo-data-model`: adds the `processed_events` dedup ledger (unique per consumer and event id,
  bounded retention) and the `device_alerts` collection, and extends the indexing-strategy
  requirement to cover them.

## Impact

- **Code**: `internal/telemetry` (new — topology declaration, publisher, consumer, dedup ledger,
  alerting handler, broker metrics; the existing ingest writer and its `HeartbeatSink` seam are
  reused unchanged), `cmd/controlplane` (wire publishing and the alerting consumer into the
  existing errgroup, register the new collectors), `internal/config` + `deploy/config.yaml` (new
  fields and validation), `deploy/mongo/init.js` (two collections), `docs/telemetry.md` (new).
  `internal/agent`, `api/proto`, and the gRPC handlers are unchanged.
- **Dependencies**: `github.com/rabbitmq/amqp091-go` (AMQP 0-9-1 client — the broker client the
  project's stack names); `testcontainers-go` (already present) gains a RabbitMQ harness for
  integration tests. No cgo.
- **Contracts**: none — the v1 proto is untouched and no broker event replaces a gRPC message. The
  broker envelope is a new, separately versioned contract (`schema_version` in the payload).
- **Storage**: `deploy/mongo/init.js` gains the `processed_events` and `device_alerts` collections
  with their validators, unique indexes, and the ledger's TTL retention (a named value shared with
  the index definition, mirroring the telemetry retention pattern).
- **Ops**: RabbitMQ becomes a runtime dependency of the fan-out, not of heartbeat acceptance — a
  broker outage counts dropped publications and reconnects with backoff while devices stay online
  and telemetry keeps landing. The Grafana dashboard that reads these metrics is stage-4 scope.
