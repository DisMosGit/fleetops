# Proposal

## Why

The device entity from `add-device-entity-workflow` is invisible from the outside at fleet scale:
its state can only be reached one device at a time through the `GetState` query, so the Temporal
UI cannot answer "which devices in `eu-west` run firmware `2.0.3` and are offline?", and MongoDB
— the fleet read model — never reflects the workflow's authoritative state (that change
deliberately deferred any write-back). The worker binary is also incomplete: `cmd/worker` hosts
only the workflow body while the entity's periodic work has nowhere to run, and nothing pins down
that the worker can be scaled out. This change makes the device fleet filterable in the Temporal
UI via search attributes, gives the entity a durable state projection in MongoDB, and turns
`cmd/worker` into the horizontally scalable host of the device workflow and its activities.

## What Changes

- Device workflows carry custom Temporal search attributes — `DeviceRegion`, `DeviceModel`,
  `DeviceFirmware` (Keyword) and `DeviceOnline` (Bool) — so device runs can be filtered in the
  Temporal UI by region, model, firmware version, and online status. The workflow upserts the
  attributes whenever their values change, and the values stay correct across rolling
  continuations. The custom attributes are registered on the namespace at worker startup,
  idempotently, so any replica can bootstrap them.
- The device workflow state gains the device's identity attributes (region, model) and its
  liveness status (`online`), adopted from the registration data signals already carry, with
  `online` flipping offline once the configured offline threshold passes without a heartbeat —
  the same threshold the liveness sweep uses. The `GetState` query exposes the extended state.
- A new `snapshot-device-state` activity writes the workflow's state into a new
  `device_state_snapshots` collection — one upserted document per device. The workflow schedules
  it periodically (durable timer at the configured interval) and immediately on meaningful
  transitions: firmware adoption, pending-command lifecycle, configuration change, online-status
  flip. The activity is idempotent (same state → same document), and a failed snapshot never
  kills the entity — the next scheduled snapshot retries the write. The `devices` collection and
  the ingest path are untouched: snapshots are a separate projection, so no writer races and no
  `devices` change-stream churn.
- `cmd/worker` registers the device workflow and its database-bound activity under explicit
  names, boots the search-attribute registration, and is safe to run as multiple replicas on the
  shared task queue: it keeps no replica-local state, any replica may execute any task, and each
  replica serves its own probes and shuts down gracefully.
- Configuration gains `snapshots.interval` (positive Go duration, default `"1m"`), the cadence
  the entity snapshots itself at; it is seeded into new device workflows alongside the existing
  `liveness.offline_threshold` at workflow start.

## Capabilities

### New Capabilities

- `device-search-attributes`: custom search attributes on device workflows — names, types, the
  values they mirror from device state, when the workflow upserts them, and namespace
  registration — so device runs are filterable in the Temporal UI by region, model, firmware
  version, and online status.
- `device-state-snapshot`: the periodic and transition-triggered snapshot of device workflow
  state into MongoDB — snapshot triggers, snapshot content, idempotent persistence, and failure
  isolation.
- `temporal-worker`: the Temporal worker binary — which workflows and activities it registers and
  under what names, search-attribute bootstrap at startup, and the properties that let it run as
  multiple replicas on the shared task queue.

### Modified Capabilities

- `device-workflow`: the entity's carried state grows identity attributes (region, model) and
  liveness status (`online`) alongside the four owned fields, exposed through the state query and
  preserved across rolling continuations.
- `mongo-data-model`: a new `device_state_snapshots` collection — one document per device keyed
  by `_id`, its required fields, its indexes, and its no-expiry status as a domain record.
- `runtime-config`: the shared configuration file gains `snapshots.interval`, validated as a
  positive duration, consumed by `cmd/controlplane` when seeding device workflows.

## Impact

- **Code**: `internal/temporal` (state extension, search-attribute map and upsert points,
  snapshot activity with a `StateSnapshotter` seam, durable snapshot timer and transition
  detection in `DeviceWorkflow`), `internal/devices` (snapshot store on
  `device_state_snapshots`), `internal/config` (`snapshots.interval`), `cmd/worker` (Mongo
  client for the snapshot activity, activity registration, search-attribute bootstrap), and
  `cmd/controlplane` (seed new workflows with the snapshot interval and offline threshold).
  `deploy/mongo/init.js` and `deploy/mongo/verify.sh` grow the new collection and its indexes.
  Workflow behavior via Temporal's `TestWorkflowEnvironment`; store behavior via the existing
  integration-test seam.
- **Dependencies**: none new — `go.temporal.io/sdk` (operator service for attribute
  registration) and `go.mongodb.org/mongo-driver/v2` are already pinned.
- **Contracts**: none. The frozen v1 proto is untouched; search attributes and snapshots ride
  Temporal and MongoDB, not the agent API.
- **Existing behavior**: heartbeat ingestion, `devices` persistence, the liveness sweep, and the
  dispatch activity (which stays in `cmd/controlplane` beside the hub) are unchanged. Device run
  chains created before this change carry an older state schema and are refused loudly rather
  than half-understood (see design — the stack runs against a local dev Temporal, so resetting
  run chains is the migration).
- **Out of scope**: rollout-side callers, UI or gateway consumption of the snapshots, k3d/compose
  manifests for worker replicas, and relocating the dispatch-command activity into the worker
  (it keeps living beside the in-process hub on the same task queue).
