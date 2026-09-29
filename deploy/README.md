# deploy/ — local-stack manifests and storage bootstrap

This directory owns the FleetOps local stack: compose/k3d manifests for MongoDB, RabbitMQ,
Temporal, Prometheus, and Grafana — plus the MongoDB bootstrap that defines the fleet data
model.

- `mongo/init.js` — creates the fleet collections, validators, and indexes below (mounted into
  `/docker-entrypoint-initdb.d` by the stack's MongoDB once the stage-1 manifests exist; it can
  also be applied directly: `mongosh --quiet < deploy/mongo/init.js`).
- `mongo/verify.sh` — asserts the data model against a running MongoDB: collections, required
  fields, indexes, TTL retention, and the redelivery no-op contract. Run it as
  `deploy/mongo/verify.sh` (uses `mongosh` from PATH, or the `fleetops-mongo` docker container;
  `MONGO_URI` and `FLEETOPS_TELEMETRY_RETENTION_DAYS` override the defaults). It exits non-zero
  on any drift.

**Not implemented yet:** the stack manifests themselves arrive at stage 1 (compose first, then
k3d manifests); until then `make up` / `make down` have nothing to apply.

## Configuration and probes

`config.yaml` is a commented sample of the single YAML configuration file every binary loads
via `-config` (see [cmd/README](../cmd/README.md)): simulation scale, gRPC, MongoDB, RabbitMQ,
Temporal, and observability endpoints, each shown with its default. Loading validates the whole
file — unknown keys and invalid values stop startup with the offending field named. The control
plane and the worker serve `GET /healthz` (liveness) and `GET /readyz` (readiness: connectivity
to MongoDB, RabbitMQ, and Temporal) on `observability.health_addr`.

The `temporal` section carries **two** task queues, and the two binaries poll one each:

| Field | Default | Polled by | Carries |
|---|---|---|---|
| `temporal.task_queue` | `"fleetops"` | `worker` replicas only | `device-workflow`, `snapshot-device-state`, `rollout-workflow`, and the nine rollout activities |
| `temporal.dispatch_task_queue` | `"fleetops-controlplane"` | `controlplane` only | `dispatch-command` |

They are separate because Temporal delivers a task to any poller of its queue rather than to one
that registered its task type: two processes sharing one queue would each be handed task types they
do not host, and those tasks would fail as unknown and be redelivered with retry backoff. The
dispatch activity has its own queue because its side effect — the agent connection a command is
written to — lives in the control-plane process, while every rollout activity's side effect lives in
the worker. Both binaries read this one file and ship as a pair; see
[Task queues](../cmd/README.md#task-queues).

An empty value for either queue stops startup with the field named. That the two names **differ** is
a requirement on the deployment, not a checked one: validation cannot know the topology an operator
intends, and two names that happen to collide are only a defect when both binaries are pointed at
them. Setting both to the work queue restores exactly the bouncing this separation removes.

## MongoDB data model contract

Source of truth for requirements: `openspec/specs/mongo-data-model/spec.md`. Database `fleetops`.
Field names follow the project's ER sketch (`id` → Mongo `_id`).

### `devices` — one heterogeneous document per device

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | device identity (unique) |
| `model` | string | device model (required) |
| `region` | string | deployment region (required) |
| `current_fw` | string | firmware version the device runs (required) |
| `status` | string | device status (required) |
| `last_heartbeat` | date | time of the last heartbeat (required) |

Indexes: `_id_` (unique, device identity), `idx_region_model {region, model}` (eligible-pool and
rollout target-group queries), `idx_status {status}` (UI device list filtering). No TTL — device
records never expire.

`current_fw` is the fleet's record of the firmware version a device runs, kept from the device's own
reports — the registration that enrolled it and the heartbeats that followed — and is never the
authority: the device's workflow owns the version the device runs. A rollback's inventory
reconciliation compares the two and corrects the record where they disagree, writing only a version
the device's workflow reported and only where the record differs, so a rerun writes nothing. That
correction touches `current_fw` alone; `status` and `last_heartbeat` belong to heartbeat ingestion
and the staleness sweep.

### `device_state_snapshots` — one projected device-workflow state per device

Written by the device workflow's snapshot activity; the write is monotone in `snapshot_at`, so
the document always holds the newest projected state and an out-of-order write converges instead
of regressing it. The projection is a subset of the workflow's state: fields no reader of this
collection needs — the last concluded command and the firmware version the device ran before its
current one — stay in the workflow, which owns them.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | device identity (unique) |
| `region` | string | device region (required) |
| `model` | string | device model (required) |
| `current_fw` | string | firmware version the device runs (required) |
| `online` | bool | liveness status of the device workflow (required) |
| `last_heartbeat` | date | timestamp of the newest applied heartbeat (required) |
| `pending` | object | outstanding command + delivery state (`command_id`, `device_id`, `kind`, `firmware_id`, `version`, `checksum`, `reason`, `dispatched`); absent while none is outstanding |
| `update_status` | object | latest firmware-update progress (`firmware_id`, `phase`, `progress_percent`, `detail`); absent while none was reported |
| `config` | object | configuration snapshot with its `version` (required) and `data` (any JSON value) |
| `snapshot_at` | date | workflow time the state was decided at (required) |

Indexes: `_id_` (unique, device identity), `idx_region_model {region, model}`, `idx_online
{online}`. No TTL — state snapshots never expire.

### `firmware` — firmware metadata (binary in GridFS)

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | firmware id (unique) |
| `version` | string | firmware version (required, unique) |
| `models` | string[] | target device models the upload declared (required, non-empty) |
| `checksum` | string | lowercase hex SHA-256 of the binary (required) |
| `size` | number | size of the stored binary in bytes (required) |
| `created_at` | date | time the upload was recorded (required) |
| `gridfs_id` | string | reference to the binary in GridFS (required) |

Metadata never holds the payload itself — binaries live in GridFS and persist exactly as long as
a metadata document references them. A metadata document whose `gridfs_id` does not resolve to a
GridFS object must be rejected by the write path (enforced when the firmware workflow lands;
the schema validator enforces the required fields and types).

Indexes: `_id_` (unique), `idx_version {version}` (unique). No TTL.

The registry is read by id and by version. By id, `Metadata` returns the record and `Open` returns
it together with a reader over the stored binary; by version, `MetadataByVersion` returns the record
alone, which is how a caller that knows only a version — a device reporting the firmware it ran, or
a rollback restoring a device to it — obtains the id, checksum, and target models to command it.
Because `version` is unique, a version resolves to exactly one record, and a version no document
carries is reported as not found rather than as an empty record. Neither lookup reads a payload:
binaries are streamed from GridFS only when a caller opens one by id.

### `rollouts` — one document per rollout

Written by the rollout workflow: the document is created when the rollout starts — carrying the
firmware, the target selector, and the workflow execution that owns it — and its `status` moves as
the rollout progresses.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | rollout id (unique) |
| `firmware_id` | string | firmware being rolled out (required) |
| `status` | string | rollout status (required) |
| `temporal_wf_id` | string | id of the workflow execution driving the rollout (required) |
| `region` | string | target group region (required) |
| `model` | string | target group model (required) |
| `rollback` | object | the rollback the rollout ran, written as it compensates (optional; absent on a rollout that never entered rollback) |

`status` is one of `running`, `paused` (an operator held the rollout: it starts no further wave),
`awaiting_approval` (holding at a wave that requires an operator approval), `rolling_back` (the
rollout has stopped deciding and is running the compensations it derived from the wave that failed
it), `rolled_back`, `completed`, or `failed`. The last three are terminal: a later write never moves
the document out of one, so a retried or late transition leaves a concluded rollout's record as it
stands. `paused` and `rolling_back` are never terminal — a resumed rollout reports `running` again,
and a compensating one records `rolled_back` only once every step of its plan has run. Every write
is idempotent — recording a status the document already holds changes nothing.

The `rollback` sub-document is the rollback's own record, written by every write from the moment the
rollout enters rollback:

| Field | Type | Meaning |
|---|---|---|
| `outcome` | string | why the compensations ran: the outcome that ended the rollout (`unhealthy_wave`, `decision_timeout`, `dispatch_failed`) |
| `steps` | array | the plan in plan order, each with its `kind` (`notify_started`, `downgrade`, `reconcile_inventory`, `notify_completed`), the `wave_id` a compensating step compensates (absent for the steps that compensate no wave), its `status` (`pending`, `running`, `completed`, `failed`), how many `devices` it targeted, the device outcomes of a downgrade step (`restored`, `failed`, `unreported`, `skipped`, `unavailable`), the record outcomes of the reconciliation (`agreed`, `corrected`, `unverified`), and a `detail` on a failed step |
| `inventory` | array | the inventory the reconciliation established: one `{version, devices}` entry per firmware version the rollback's devices were found on, ordered by version |
| `unrestored_device_ids` | array of string | the devices left on the rolled-back firmware: the ones that reported a failed restore, never reported, or could not be restored at all |

The steps are replaced whole by every write, so a step transition recorded twice converges on the
same document rather than appending to it, and the record of a rollout interrupted mid-plan stays
readable: the steps that ran carry their counts and the rest are still `pending`. The sub-document
is deliberately optional in the validator, so a document written before it existed stays updatable
by the new build and a document written with it stays valid for the previous validator.

Indexes: `_id_` (unique), `idx_status {status}` (rollout list filtering), `idx_firmware_id
{firmware_id}` (rollouts per firmware). No TTL.

### `waves` — one document per canary wave

Written by the rollout workflow: a wave's document is created when its membership is resolved,
before any update command is dispatched, and its `status` and `success_rate` are recorded as its
gate decides.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | wave id, derived as `<rollout_id>-w<position>-<percent>` (unique) |
| `rollout_id` | string | parent rollout (required) |
| `percent` | int | cumulative canary share of the target group (required; the default sequence is 1, 5, 25, 100) |
| `status` | string | wave status (required) |
| `success_rate` | number | success ratio measured over the wave's health window (required) |
| `device_ids` | array of string | the target devices the wave was dispatched to (required; may be empty) |
| `started_at` | date | when the wave started, opening its health window (required) |
| `failed_device_ids` | array of string | devices that reported a failed update (optional; written empty when none) |
| `unreported_device_ids` | array of string | devices that never reported a result (optional; written empty when none) |

`status` is one of `dispatching` (membership recorded, update commands being delivered),
`evaluating` (every target has settled — reported success, reported failure, or run out of time —
and the wave is inside its health window or being re-measured), `skipped` (a share that added no
device: recorded, not dispatched, and not gated on health), `healthy` or `unhealthy` (the gate's
decision), or `failed` (its update commands could not be delivered). `success_rate` is `0` until
the wave's health has been evaluated and then carries the evaluated success ratio; a skipped wave
is never evaluated and keeps `0`. Writing the same transition twice leaves the document unchanged,
and a decided wave's membership and `started_at` are never rewritten — they are the denominator and
the window its decision was measured over.

`device_ids` is the wave's membership as resolved when it started, recorded rather than
re-derived: a device that re-registers or changes model mid-rollout must not silently move the
denominator a canary decision rests on. A wave whose canary share resolves to no device is still
recorded, with an empty array. Health evaluation reads heartbeats of exactly these devices and
**never heartbeats older than `started_at`** — pre-wave samples come from devices still running
the previous firmware, so counting them would make a regressing wave look healthiest exactly when
the gate must be strictest.

`failed_device_ids` and `unreported_device_ids` are the outcomes the wave's per-device update
dispatch collected. A device is *failed* when its workflow reported that the update command
concluded unsuccessfully, and *unreported* when it never reported before the wave stopped waiting
(`rollout.result_timeout`, default five minutes). They are written by every wave write, always as
arrays — a wave whose devices all succeeded carries two empty arrays rather than a missing field.
They are deliberately optional in the validator, so a document written before they existed stays
updatable by the new build and a document written with them stays valid for the previous one.
Neither set decides a wave by itself: promotion remains the configured health gate's verdict, and
the sets record who did not take the update.

Indexes: `_id_` (unique), `idx_rollout_percent {rollout_id, percent}` (waves of a rollout in wave
order). No unique `(rollout_id, percent)` constraint — a rollback may re-run a wave size. No TTL.

### `telemetry` — heartbeat time-series

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | event id (unique — this is the idempotency key) |
| `ts` | date | measurement time (required) |
| `meta.device_id` | string | `device_id` of the emitting device (required) |
| `meta.region` | string | device region, denormalized for region health queries (required) |
| `meta.model` | string | device model, denormalized likewise (required) |
| `cpu` | number | CPU metric sample (required) |
| `mem` | number | memory metric sample (required) |
| `health` | string | health sample (required) |

Documents keep the time-series shape (`ts` + `meta` + top-level metrics) in a plain collection
with a TTL index; see `openspec/changes/add-repo-scaffold-and-data-model/notes.md` for why the
bucketed `timeseries` type was rejected (it cannot enforce the `_id` idempotency contract).

**Ingest contract (idempotent):** every event is written with `_id = event id` using
insert-or-no-op semantics — an upsert with `$setOnInsert`, or a plain insert whose duplicate-key
error (`E11000`) is treated as a successful no-op. A redelivered event therefore never becomes a
second row and never fails the consumer.

Indexes: `_id_` (unique), `idx_device_ts {meta.device_id, ts}` (per-device time ranges),
`idx_region_model_ts {meta.region, meta.model, ts}` (region+model health windows),
`idx_ts_ttl {ts}` TTL with `expireAfterSeconds` (retention below).

### `processed_events` — the consumer dedup ledger

One document per consumer and event, `_id` = `<consumer>:<event id>`; the unique
(`consumer`, `event_id`) index makes a second document for one pair impossible. `processed_at` is
absent until the consumer's side effect is durable, and a document without it is unfinished work
the consumer resumes rather than an applied event. Pipeline bookkeeping, not a domain record: it
expires (retention below).

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | `<consumer>:<event id>` (unique) |
| `consumer` | string | consumer identity, the work queue name (required) |
| `event_id` | string | the event's stable identifier (required) |
| `device_id` | string | device the event belongs to (required) |
| `claimed_at` | date | when the consumer first took the event for processing (required) |
| `processed_at` | date | when the side effect became durable; absent while the work is unfinished |

Indexes: `_id_` (unique), `idx_consumer_event {consumer, event_id}` (unique — the dedup
guarantee), `idx_device_id {device_id}`, `idx_claimed_at_ttl {claimed_at}` TTL with
`expireAfterSeconds` (retention below).

### `device_alerts` — one alert per degraded device

Written by the alerting consumer's monotone upsert: `$min` on `first_seen_at` and `min_health`,
`$max` on `last_seen_at`, `$set` for identity and threshold, so replaying an event converges to the
same document. Domain record: never expires, and it never holds the raw heartbeat payload.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | device identity (unique) |
| `device_id` | string | device identity (required) |
| `region` | string | device region (required) |
| `model` | string | device model (required) |
| `threshold` | number | health threshold that triggered the alert (required) |
| `min_health` | number | lowest health observed since the alert opened (required) |
| `first_seen_at` | date | earliest degraded observation (required) |
| `last_seen_at` | date | newest degraded observation (required) |

Indexes: `_id_` (unique, device identity), `idx_region_model {region, model}`. No TTL.

### Retention

Heartbeat telemetry is retained for **7 days** after its `ts` and then removed automatically by
the TTL index — no cleanup job. The period is the single named configuration value
`FLEETOPS_TELEMETRY_RETENTION_DAYS` (default 7): `mongo/init.js` builds the TTL index from it and
the ingestion path (stage 3) reads the same variable, so one change moves both. Domain
collections (`devices`, `firmware`, `rollouts`, `waves`, `device_state_snapshots`,
`device_alerts`) and GridFS binaries never expire.

The consumer dedup ledger is bookkeeping and expires too: `processed_events` documents are removed
**7 days** after `claimed_at`, built from the named value
`FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS` (default 7, the same pattern). Past that window a
redelivery is applied again, which is why every consumer side effect is idempotent by
construction.

### Rollout lifecycle statuses

A rollout document's `status` is a projection of the workflow's own state, so the document, the
state query an operator reads, and the `RolloutStatus` search attribute cannot disagree:

| Status | Meaning | Terminal |
|---|---|---|
| `running` | driving its sequence: dispatching a wave, waiting in its health window, or measuring it | no |
| `paused` | an operator paused it: no further wave is resolved or dispatched until it is resumed | no |
| `awaiting_approval` | holding at a wave configured to require an operator's approval | no |
| `rolled_back` | a wave failed its gate, its decision timeout, or its delivery, and the rollout stopped | yes |
| `completed` | the whole configured sequence was promoted | yes |
| `failed` | it could not start (unknown firmware, or one that does not target the selector's model) | yes |

A pause holds promotion, never safety: a wave already in flight is measured and recorded as it
would have been, and a regression still rolls a paused rollout back. The `paused` value needs no
validator change — `status` is a plain string in the schema, and the vocabulary is this document
and the `rollout.RolloutStatus` constants in code.
