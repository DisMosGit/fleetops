# Telemetry event pipeline

How a heartbeat becomes a broker event, how a consumer applies it exactly once, and what an
operator can see from the metrics endpoint. The requirement sources are the `specs/` deltas of
the `add-telemetry-event-pipeline` change; this document is the implementation-facing view of
them.

Not covered here: change-stream-driven eligible-device-pool recalculation and the wider
observability surface (OTel traces, Grafana dashboards), which arrive with their own stages.

## Where the pipeline sits

The gRPC stream stays the only way heartbeats enter the control plane, and the batched ingest
write into MongoDB stays the only source of truth for them:

```
agent ──gRPC stream──▶ agentserver.Hub ──▶ telemetry.Fanout ──▶ telemetry.Writer ──▶ mongodb.telemetry
                                                           └──▶ telemetry.Publisher ──▶ rabbitmq
                                                                                             │
                                                       fleetops.heartbeat.alerting ◀─────────┘
                                                                     │
                                                        telemetry.Ledger (processed_events)
                                                        telemetry.Alerting (device_alerts)
```

Publication is a *second* sink beside the durable write, never a replacement: the fan-out stores a
heartbeat first and publishes it only if storage accepted it, so a broker outage degrades the
fan-out (counted, logged once per burst) while heartbeats keep being accepted and persisted.

## Exchange, queue, and binding layout

Three durable exchanges carry every FleetOps event:

| Exchange | Type | Carries |
|---|---|---|
| `fleetops.events` | topic | heartbeat events, rollout work, and rollout notifications |
| `fleetops.retry` | direct | a failed delivery, into the retry queue of the attempt that failed |
| `fleetops.dead-letter` | direct | deliveries that exhausted their attempts, and deliveries the consumer cannot republish |

Routing keys follow one grammar:

| Key | Published by | Matches |
|---|---|---|
| `heartbeat.<region>.<model>` | the control plane, for every accepted heartbeat | the heartbeat work queue, and any narrower binding such as `heartbeat.eu-west.#` |
| `rollout.task.<kind>` | rollout work (no producer yet) | `fleetops.rollout.tasks` |
| `rollout.notification.<kind>.<phase>` | a rollback's announcement | `fleetops.rollout.notifications`, and any narrower binding such as `rollout.notification.rollback.#` |
| `retry.<work queue>` | the broker, when a retry queue's TTL expires | that work queue |
| `<work queue>.dlq` | the control plane, on a terminal failure | that work queue's dead-letter queue |

Queues, one work queue per consumer family:

| Queue | Kind | Purpose |
|---|---|---|
| `fleetops.heartbeat.alerting` | work | heartbeat events for the alerting consumer |
| `fleetops.heartbeat.alerting.retry.<n>` | retry | attempt `n`'s backoff queue, for `n` = 1 … `max_attempts - 1` |
| `fleetops.heartbeat.alerting.dlq` | dead letter | terminal failures of that work queue |
| `fleetops.rollout.tasks` | work | rollout work; declared and bound |
| `fleetops.rollout.tasks.retry.<n>` | retry | as above, for rollout work |
| `fleetops.rollout.tasks.dlq` | dead letter | as above, for rollout work |
| `fleetops.rollout.notifications` | work | rollout notifications — a rollback's announcements today — declared and bound, with no consumer yet |
| `fleetops.rollout.notifications.retry.<n>` | retry | as above, for notifications |
| `fleetops.rollout.notifications.dlq` | dead letter | as above, for notifications |

Every queue and exchange is durable and every published message is persistent, so the layout and
its undelivered messages survive a broker restart. The whole topology is declared idempotently at
startup by the processes that use it — there is no broker-side setup step — and a declaration that
conflicts with an existing element stops the process with an error naming that element instead of
adapting to it silently.

## The envelope

Every event is a persistent `application/json` message. The body is a versioned envelope:

```json
{
  "schema_version": 1,
  "event_id": "device-3-1735900000000000000-2",
  "event_type": "heartbeat",
  "device_id": "device-3",
  "region": "eu-west",
  "model": "v3",
  "occurred_at": "2026-03-04T05:06:07Z",
  "published_at": "2026-03-04T05:06:07.021Z",
  "sequence": 42,
  "payload": {
    "cpu": 0.42,
    "mem": 0.61,
    "health": 0.31,
    "current_fw": "1.4.0",
    "status": "degraded"
  }
}
```

| Field | Meaning |
|---|---|
| `schema_version` | envelope revision; `1` in this build |
| `event_id` | the heartbeat's stable id, minted by the agent and reused on redelivery — the key consumers deduplicate on |
| `event_type` | `heartbeat`, or `rollout` for rollout work |
| `device_id`, `region`, `model` | the device's registered identity; region and model also build the routing key |
| `occurred_at` | the measurement time |
| `published_at` | the control plane's publish time |
| `sequence` | the publisher's per-event-type monotonic counter, which consumer lag is measured against |
| `payload` | the samples: `cpu`, `mem`, `health`, `current_fw`, `status` |

**Versioning rule:** within one `schema_version` a field is never renamed, repurposed, or given a
different meaning. A changed shape is published under a new `schema_version`, and a consumer that
does not support a version dead-letters the event with that reason instead of guessing. The
envelope is deliberately not a gRPC message: consumers never couple to a transport contract of
another bounded context, and the JSON body stays readable in the broker UI.

Message properties repeat the identity, so a delivery — including a dead letter — can be triaged
without decoding the body:

| Property / header | Value |
|---|---|
| `MessageId` | `event_id` |
| `Type` | `event_type` |
| `Timestamp` | `published_at` |
| `ContentType`, `DeliveryMode` | `application/json`, persistent |
| `x-schema-version` | envelope revision |
| `x-attempt` | processing attempt, one-based; the publisher stamps `1` and every retry increments it |
| `x-original-routing-key` | the key the event was first published under, so a retry or dead letter still says where it came from |
| `x-death-reason` | present on a dead letter: why the event was dead-lettered |

## Rollback announcements

A rollback publishes two notifications into the same events exchange, under their own key space:
`rollout.notification.rollback.started` when it enters its compensating phase and
`rollout.notification.rollback.completed` when its compensations have run. A consumer therefore
narrows with a binding alone — one phase by its key, every rollback announcement with
`rollout.notification.rollback.#`, every rollout notification with `rollout.notification.#` — and
receives no heartbeat and no rollout work, because the key spaces are disjoint. Nothing publishes
rollout *work* yet, and nothing consumes notifications yet: the queue is declared and bound so the
announcements are routable from the first rollback, and the dashboards and alerts the
observability stage adds attach to it by binding.

The notification is its own contract, with its own schema version — it carries no device and no
sample, which is not what the heartbeat envelope describes:

```json
{
  "schema_version": 1,
  "event_id": "rollback-ro-42-completed",
  "event_type": "rollback_notification",
  "phase": "completed",
  "occurred_at": "2026-03-04T05:06:07Z",
  "rollout": {
    "rollout_id": "ro-42",
    "firmware_id": "fw-9",
    "firmware_version": "2.0.0",
    "region": "eu-west",
    "model": "v3",
    "wave_id": "ro-42-w1-5",
    "outcome": "unhealthy_wave",
    "decision": {
      "verdict": "unhealthy",
      "success_ratio": 0.72,
      "sample_size": 140,
      "window_start": "2026-03-04T05:00:07Z",
      "window_end": "2026-03-04T05:06:07Z"
    },
    "plan_steps": 4,
    "plan_devices": 12
  },
  "progress": {
    "restored": 11,
    "failed": 0,
    "unreported": 1,
    "skipped": 0,
    "unavailable": 0,
    "agreed": 9,
    "corrected": 2,
    "unverified": 1,
    "inventory": [{"version": "1.0.0", "devices": 11}, {"version": "2.0.0", "devices": 1}],
    "unrestored_device_ids": ["device-7"],
    "steps": [
      {"kind": "downgrade", "wave_id": "ro-42-w1-5", "status": "completed", "devices": 12,
       "restored": 11, "unreported": 1, "skipped": 0, "failed": 0, "unavailable": 0,
       "agreed": 0, "corrected": 0, "unverified": 0},
      {"kind": "reconcile_inventory", "status": "completed", "devices": 12,
       "restored": 0, "unreported": 0, "skipped": 0, "failed": 0, "unavailable": 0,
       "agreed": 9, "corrected": 2, "unverified": 1}
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `schema_version` | the announcement contract's revision; `1` in this build, independent of the envelope's |
| `event_id` | `rollback-<rollout_id>-<phase>`: derived, so a retried publication republishes the same event and a consumer deduplicating by it applies it once |
| `event_type` | `rollback_notification` |
| `phase` | `started` or `completed` |
| `occurred_at` | the workflow time the announcement was decided at, so a replay announces the same moment |
| `rollout` | what is rolling back and why: the rollout, the firmware it deployed, its target group, the wave that ended it, the outcome, and — when the ending wave was measured — its decision |
| `rollout.plan_steps`, `rollout.plan_devices` | how many steps and how many devices the rollback compensates |
| `progress` | what the compensations achieved: the device outcomes, the record corrections, the reconciled inventory, the devices that could not be restored, and each step's own outcome. Absent on the `started` announcement, which precedes every compensation |

An announcement is published *with the broker's confirmation* (`internal/telemetry.Notifier`, a
supervised session that waits for the ack, the nack, or the returned message): the workflow records
the announcement step as complete only once the broker has taken the event, and a rejection, an
unroutable key, or a lost connection is an error the step retries. A publication that cannot
succeed after its retries is recorded against that step and changes nothing else — the rollout's
recorded rollback is authoritative, and an unannounced rollback is still a rollback.

## Consumer semantics

A consumer of a work queue (`internal/telemetry.Consumer`) applies each event at most once and
never loses one:

1. **Decode.** A payload that does not decode, is missing its event id, carries no event type, or
   declares a schema version this build does not support is dead-lettered immediately with that
   reason. Such an event consumes no retry attempt and writes no ledger entry.
2. **Claim.** Before applying anything, the consumer records a `processed_events` document keyed
   by `<consumer>:<event_id>`, whose unique (`consumer`, `event_id`) index makes a second document
   for one pair impossible. A refused insert is read back:
   - **processed** (`processed_at` set) → the delivery is a duplicate: counted, acknowledged, and
     no side effect is repeated;
   - **unfinished** (`processed_at` absent) → the previous attempt failed or the process stopped
     mid-processing: the event is applied again, so nothing hides behind a stale claim.
3. **Apply.** The consumer's handler applies an idempotent side effect (below).
4. **Complete.** `processed_at` is set. A failure here is a normal failure: the side effect is
   idempotent, so the retry re-applies and converges.
5. **Acknowledge.** Only after the side effect *and* its completion are durable. A consumer never
   acknowledges an event it did not apply.

### Retry ladder and dead-letter path

A failed delivery is republished to `fleetops.retry` under `<work queue>.retry.<n>`, where `n` is
the attempt that just failed, with `x-attempt` incremented. The original delivery is acknowledged
only after that republish is confirmed, so a crash between the two cannot lose the event.

Each retry queue holds its messages for that attempt's delay and then dead-letters them back to
the work queue under `retry.<work queue>`:

| Attempt | Retry queue | Delay |
|---|---|---|
| 1 | `<work queue>.retry.1` | `retry_base` |
| 2 | `<work queue>.retry.2` | `retry_base × 2` |
| `n` | `<work queue>.retry.n` | `min(retry_base × 2^(n-1), retry_max)` |

With the defaults (`retry_base` `5s`, `retry_max` `1m`, `max_attempts` 3) a delivery is processed
at most three times, waiting 5s and then 10s between attempts. One queue per attempt is what makes
that exact: every message in a queue shares one TTL, so a short delay can never wait behind a
longer one at the queue head.

A delivery that keeps failing lands on `<work queue>.dlq` with its `x-attempt`, its
`x-original-routing-key`, and its `x-death-reason`, and is never redelivered automatically. A
delivery that cannot even be republished (the broker is unreachable, or the republish is refused)
is rejected without requeue, so the broker's own dead-letter path takes it to the same queue
instead of losing it or looping it on the work queue forever.

### Alerting consumer

`fleetops.heartbeat.alerting` turns degradation into one durable alert document per device:

- a heartbeat with `health` at or above `alerting.health_threshold` writes nothing;
- a heartbeat below it upserts the device's single `device_alerts` document with `$min` on
  `first_seen_at` and `min_health`, `$max` on `last_seen_at`, and `$set` for identity and the
  threshold — no counters and no increments, so a redelivery or a resumed claim converges to the
  same document rather than drifting.

Consumers bound in-flight work with `rabbitmq.prefetch` unacknowledged deliveries, reconnect with
capped exponential backoff when the connection or channel is lost, and re-declare the topology on
every reconnect. Deliveries left unacknowledged by a lost connection are redelivered, and
deduplication makes that safe.

## Metrics

The pipeline's Prometheus source is registered on the registry the control plane already serves at
`observability.metrics_addr`, alongside `fleetops_device_offline_transitions_total`:

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `fleetops_queue_depth` | gauge | `queue`, `kind` (`work`, `retry`, `dead_letter`) | how many messages are waiting, sampled at `rabbitmq.queue_depth_interval`; dead-letter depth is simply the `kind="dead_letter"` series |
| `fleetops_consumer_lag_events` | gauge | `consumer` | how many events of that consumer's type were published after the last event it processed; zero when it has caught up, never negative, read at scrape time |
| `fleetops_events_published_total` | counter | `event_type` | how many events the broker confirmed |
| `fleetops_events_dropped_total` | counter | `event_type`, `reason` | how many events could not be published, split by `unreachable`, `buffer_full`, `rejected`, `unroutable`, `unusable` |
| `fleetops_events_consumed_total` | counter | `consumer`, `outcome` | what happened to each delivery: `processed`, `duplicate`, `retry`, `dead_letter` |

Every event that enters the pipeline is accounted for: it is published, or dropped with a reason;
every delivery ends in exactly one outcome.

A useful starting set of queries:

```promql
# dead letters are arriving
increase(fleetops_events_consumed_total{outcome="dead_letter"}[15m]) > 0
# the alerting consumer is falling behind
fleetops_consumer_lag_events{consumer="fleetops.heartbeat.alerting"} > 500
# the broker is unreachable or shedding
increase(fleetops_events_dropped_total[5m]) > 0
# something is waiting where nothing should wait
fleetops_queue_depth{kind="dead_letter"} > 0
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `rabbitmq.url` | `amqp://guest:guest@localhost:5672/` | broker connection URL |
| `rabbitmq.prefetch` | `32` | unacknowledged deliveries one consumer holds in flight |
| `rabbitmq.publish_buffer` | `1024` | events buffered for publication before load shedding |
| `rabbitmq.max_attempts` | `3` | processing attempts before a delivery is dead-lettered |
| `rabbitmq.retry_base` | `"5s"` | backoff delay of the first retry attempt |
| `rabbitmq.retry_max` | `"1m"` | upper bound on a retry delay; must not be below `retry_base` |
| `rabbitmq.queue_depth_interval` | `"15s"` | cadence of queue-depth sampling |
| `alerting.health_threshold` | `0.6` | health score below which a degradation alert is recorded |

The defaults sit between the emulator's bands — healthy `0.85`–`1.0`, degraded `0.2`–`0.5` — so
the threshold separates them cleanly.

## Operational notes

**Dropped events.** `fleetops_events_dropped_total` is the honest counter for everything the
fan-out could not publish. `unreachable` means the broker (or its connection) was gone,
`buffer_full` means the broker could not keep up with `rabbitmq.publish_buffer` of slack — both
are safe to ignore in short bursts and worth alerting on when sustained, because dropped events
are *derived* data: the durable write in `telemetry` still holds every heartbeat, so a consumer
state can be rebuilt from storage. The first drop of a burst is logged with its reason and event
type; the rest of the burst is counted silently.

**Inspecting the dead-letter queue.** Dead letters are ordinary messages on
`<work queue>.dlq`. Read them from the broker's management UI, or — when the management plugin is
enabled — with

```sh
rabbitmqadmin get queue=fleetops.heartbeat.alerting.dlq ackmode=ack_requeue_true count=10
```

`rabbitmqctl list_queues name messages` prints the same depths the metric reports. Each message
carries its event id in the `MessageId` property, the attempts made in `x-attempt`, the key it was
first published under in `x-original-routing-key`, and the failure in `x-death-reason`. Nothing is
redriven automatically — after fixing the cause, republish the body to `fleetops.events` under
`x-original-routing-key`, or purge the queue. Redriving is safe: consumers deduplicate by event id.

**Retention.** The dedup ledger is bookkeeping and expires: `processed_events` documents are
removed `FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS` (default 7) after `claimed_at`, the single
named value `deploy/mongo/init.js` also builds the TTL index from. Telemetry retention is the
separate `FLEETOPS_TELEMETRY_RETENTION_DAYS`. Alert documents are domain records and never expire.
Past the ledger window a redelivery is applied again — which is exactly why every consumer side
effect is idempotent by construction: the ledger is the fast path, not the guarantee.

**Broker outages.** Neither publication nor consumption stops the control plane: both reconnect
with capped exponential backoff (250ms doubling to 30s) and re-declare the topology. A broker that
is unreachable at startup delays the fan-out and the consumers but never heartbeat acceptance, and
a queue deleted by hand reappears on the next reconnect.

## Where the contracts live

- Behaviour requirements: `openspec/changes/add-telemetry-event-pipeline/specs/` (broker topology,
  publication, consumption, observability, config, data model).
- Collections and their indexes: `deploy/README.md` and `deploy/mongo/init.js`.
- The layout, ladder, and envelope in code: `internal/telemetry/topology.go`,
  `envelope.go`, `publisher.go`, `consumer.go`, `dedup.go`, `alerting.go`.
