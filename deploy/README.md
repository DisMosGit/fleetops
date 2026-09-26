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

### `device_state_snapshots` — one projected device-workflow state per device

Written by the device workflow's snapshot activity; the write is monotone in `snapshot_at`, so
the document always holds the newest projected state and an out-of-order write converges instead
of regressing it.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | device identity (unique) |
| `region` | string | device region (required) |
| `model` | string | device model (required) |
| `current_fw` | string | firmware version the device runs (required) |
| `online` | bool | liveness status of the device workflow (required) |
| `last_heartbeat` | date | timestamp of the newest applied heartbeat (required) |
| `pending` | object | outstanding command + delivery state (`command_id`, `device_id`, `kind`, `firmware_id`, `version`, `checksum`, `reason`, `dispatched`); absent while none is outstanding |
| `config` | object | configuration snapshot with its `version` (required) and `data` (any JSON value) |
| `snapshot_at` | date | workflow time the state was decided at (required) |

Indexes: `_id_` (unique, device identity), `idx_region_model {region, model}`, `idx_online
{online}`. No TTL — state snapshots never expire.

### `firmware` — firmware metadata (binary in GridFS)

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | firmware id (unique) |
| `version` | string | firmware version (required, unique) |
| `checksum` | string | binary checksum (required) |
| `gridfs_id` | string | reference to the binary in GridFS (required) |

Metadata never holds the payload itself — binaries live in GridFS and persist exactly as long as
a metadata document references them. A metadata document whose `gridfs_id` does not resolve to a
GridFS object must be rejected by the write path (enforced when the firmware workflow lands;
the schema validator enforces the required fields and types).

Indexes: `_id_` (unique), `idx_version {version}` (unique). No TTL.

### `rollouts` — one document per rollout

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | rollout id (unique) |
| `firmware_id` | string | firmware being rolled out (required) |
| `status` | string | rollout status (required) |
| `temporal_wf_id` | string | id of the workflow execution driving the rollout (required) |
| `region` | string | target group region (required) |
| `model` | string | target group model (required) |

Indexes: `_id_` (unique), `idx_status {status}` (rollout list filtering), `idx_firmware_id
{firmware_id}` (rollouts per firmware). No TTL.

### `waves` — one document per canary wave

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | wave id (unique) |
| `rollout_id` | string | parent rollout (required) |
| `percent` | int | canary share of the target group: 1, 5, 25, or 100 (required) |
| `status` | string | wave status (required) |
| `success_rate` | number | health over the wave's health window (required) |

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

### Retention

Heartbeat telemetry is retained for **7 days** after its `ts` and then removed automatically by
the TTL index — no cleanup job. The period is the single named configuration value
`FLEETOPS_TELEMETRY_RETENTION_DAYS` (default 7): `mongo/init.js` builds the TTL index from it and
the ingestion path (stage 3) reads the same variable, so one change moves both. Domain
collections (`devices`, `firmware`, `rollouts`, `waves`) and GridFS binaries never expire.
