# Design

## Context

`internal/temporal` already owns the device entity: `deviceState` (four owned fields + dedup
rings + `carry_version`), `DeviceWorkflow` (signal selector, dispatch activity, rolling
continuation at 128 signals), `Signaler` (`SignalWithStart` producers), and `NewDispatchActivity`
over the `CommandDispatcher` seam. `cmd/worker` dials Temporal, registers `DeviceWorkflow` on
`cfg.Temporal.TaskQueue`, and serves probes; the dispatch activity is registered in
`cmd/controlplane` beside the hub. The `devices` collection is written only by the ingest path
(registry upserts, coalesced refreshes, staleness sweep) and the earlier change deliberately left
any workflow→Mongo write-back out. `deploy/mongo/init.js` / `verify.sh` own the Mongo contract.
See proposal.md — Why, and the spec deltas for normative behavior.

## Goals / Non-Goals

**Goals:**
- Search attribute values that are a pure function of entity state and correct at every point of
  a run chain — including right after a rolling continuation — with no periodic repair loop.
- A state projection into MongoDB with bounded staleness (periodic) plus immediate freshness on
  meaningful transitions, convergent under retries, replays, and out-of-order writes.
- A worker binary whose replicas can be added or removed freely on the shared task queue, with
  startup that is safe to run concurrently on all of them.

**Non-Goals:**
- Snapshot history: exactly one document per device (the spec pins it); a time-stamped history
  collection is a later concern if a caller needs it.
- Reconciling the `devices` collection from snapshots — `devices` stays the ingest-written read
  model; snapshots are a separate projection of workflow truth.
- Metrics/tracing for snapshot lag or search-attribute freshness (observability stage).
- Moving the dispatch-command activity into the worker (it stays beside the in-process hub).

## Decisions

### D1. Search attributes: names, types, and derivation
Four custom attributes in the `default` namespace surface of the FleetOps namespace:
`DeviceRegion`, `DeviceModel`, `DeviceFirmware` (Keyword) and `DeviceOnline` (Bool). Keyword
gives exact-match filtering — the only kind fleet dimensions need; Bool keeps `DeviceOnline`
a real predicate instead of a string. Values come from one pure function of `deviceState`
(`searchAttributes(state)`), never from ad-hoc upsert call sites, so the attribute map cannot
drift from state. Upsert points: once at run start (full map), then whenever the derived map
differs from the previous one — identity adoption, firmware adoption, liveness flip. A heartbeat
that only moves `last_heartbeat_at` derives an unchanged map and therefore does not upsert (the
spec requires exactly this). Alternative: seed values via `StartWorkflowOptions.SearchAttributes`
at `SignalWithStart` — rejected: only the start call carries them, later transitions would still
need upserts, and there would be two sources of truth for one map.

### D2. Continuation correctness without relying on inheritance semantics
Whether a ContinueAsNew run inherits the parent's search attributes is an SDK detail we must not
build correctness on. Every run therefore upserts the full derived map once at start (idempotent,
four values), and keeps it current from there. A workflow test asserts the attributes after a
rollover; if the SDK turns out to inherit them, the run-start upsert is simply redundant.

### D3. Liveness status: derived from heartbeat recency at decision points
`deviceState` carries `Online`, but it is a cache of one deterministic judgement:
`workflow.Now - LastHeartbeatAt < Settings.OfflineThreshold`. The judgement is evaluated at two
points — after each applied signal (so a heartbeat flips a device online immediately) and at
each periodic snapshot tick (so an idle device flips offline at all). A change of the derived
value against the carried one is a meaningful transition: store it, upsert search attributes,
snapshot. `workflow.Now` keeps this deterministic; no wall-clock reads. The threshold is the same
configured value the Mongo staleness sweep uses (`liveness.offline_threshold`), so the two
judgements cannot be tuned apart (see Risks for their known small divergence).

### D4. Settings travel in the carried state
The snapshot interval and offline threshold are seeded into `deviceState` at `SignalWithStart`
(wired from `snapshots.interval` and `liveness.offline_threshold` in `cmd/controlplane`) and ride
every ContinueAsNew payload. Workflows are long-lived and config is deploy-time, so capture-at-
start is the right trade (see Risks). Alternative: a `config_changed`-style settings signal —
rejected: no caller exists, and it would make entity behavior depend on an operator ever sending
it. Alternative: workflow constants — rejected: the offline threshold must stay the single named
configuration value the liveness spec already requires.

### D5. Snapshot scheduling: one durable timer, immediate writes on transitions
The workflow arms a durable timer at `Settings.SnapshotInterval`, rearms it after each fire, and
additionally runs the snapshot activity immediately on every meaningful transition (firmware
adoption, pending-command set/superseded/concluded, config apply, liveness flip). Routine
heartbeat refreshes are deliberately not immediate writes — the periodic timer bounds their
staleness to one interval, which keeps snapshot write volume at `devices / interval` plus
transitions instead of one write per heartbeat at fleet scale. The activity runs with a
caller-supplied retry policy and its result is only logged: a failed snapshot never blocks or
kills the entity, and the next tick converges (spec: failure-isolated persistence). Before
ContinueAsNew the workflow simply abandons any in-flight snapshot — the write is idempotent and
the next run's tick refreshes it.

### D6. Monotone snapshot writes
Snapshots carry `snapshot_at` — `workflow.Now` at the moment the state was decided — and the
store write is a conditional upsert: replace the document only when the incoming `snapshot_at`
is newer than the stored one (`$max`-guarded filter or equivalent). That makes retries and
replays no-ops and makes out-of-order writes (an older state's slow write landing after a newer
one) converge to the newest state instead of regressing the projection. Two writes of the same
state carry the same `snapshot_at` and the same content, so either order is identical.

### D7. Activity seam and store
`internal/temporal/snapshot.go` defines `StateSnapshotter` at the consumer (mirroring
`CommandDispatcher`) with `NewSnapshotActivity` returning the idempotent activity registered
under the explicit name `snapshot-device-state`; `internal/devices` gains a `SnapshotStore` on
the `device_state_snapshots` collection implementing it (conditional upsert, one document per
`_id`). The snapshot payload is the state-query view plus `snapshot_at` — one struct, so the
query and the projection cannot disagree on shape. The activity takes `context.Context` first
and carries no retry policy of its own; the workflow caller supplies one.

### D8. Search-attribute bootstrap at worker startup
Before polling tasks, `cmd/worker` ensures the four attributes exist via the Temporal operator
service (`AddSearchAttributes`), treating "already exists" as success and any other error as
fatal startup failure. The operation is server-side idempotent, so replicas bootstrapping
concurrently is harmless. Alternative: a documented `temporal operator cluster search-attribute
create` step in the deploy scripts — rejected: a manual step that the worker can own safely, and
forgotten steps fail late in the UI instead of early at startup.

### D9. Worker hosting and horizontal scaling
`cmd/worker` opens the Mongo client it needs for `SnapshotStore`, registers `DeviceWorkflow` and
`snapshot-device-state` on `cfg.Temporal.TaskQueue`, bootstraps the attributes, and keeps the
existing errgroup lifecycle (worker + probe server, bounded graceful stop). Everything a task
needs is either in the task payload or in Mongo, so replicas are interchangeable: Temporal
distributes workflow and activity tasks across pollers, sticky workflow caching is per-replica
and self-healing on replica loss, and startup/stop requires no peer coordination. Probes stay
per replica on `observability.health_addr`. The dispatch activity remains registered in
`cmd/controlplane` on the same task queue — "the worker hosts the device workflow and its
activities" means the activities whose side effects the worker process owns.

### D10. State schema bump and naming discipline
`carry_version` bumps 1 → 2 (new fields: `region`, `model`, `online`, `settings`); `validate`
keeps refusing foreign carry versions loudly via `ErrUnsupportedCarryVersion`, so a pre-change
run chain fails visibly instead of running half-understood (Migration Plan). This state-shape
gate replaces a `workflow.GetVersion` branch: no execution can replay the new body with an old
payload, because the payload is refused before the body runs — the determinism guarantee the
version rule protects is preserved by the refusal, and a frozen legacy loop would be dead code
on a stack with no production chains (alternative considered; revisit if live chains ever must
be preserved). Registration names stay explicit (`device-workflow`, `snapshot-device-state`);
new files: `internal/temporal/searchattrs.go`, `internal/temporal/snapshot.go`,
`internal/devices/snapshots.go` — each well under the 400 LOC split threshold, tests beside
them.

## Risks / Trade-offs

- [Search attributes may not be inherited across ContinueAsNew] → run-start upsert of the full
  map (D2) makes correctness independent of the SDK behavior; pinned by a workflow test.
- [Out-of-order snapshot writes could regress the projection] → `$max`-guarded writes keyed by
  `snapshot_at` (D6); pinned by a store test with reversed write order.
- [Workflow liveness and the Mongo sweep can disagree transiently] → both use
  `liveness.offline_threshold`, but the workflow judges on device sample time and the sweep on
  accept time; divergence is bounded by one threshold and the snapshot interval. Per spec the
  workflow's judgement is authoritative for the snapshot and search attributes.
- [Settings are captured when a run chain starts] → a config change reaches a device only on its
  next chain; config is deploy-time in this stack, and resetting chains is cheap. If live
  reconfiguration ever matters, a settings signal is additive.
- [Custom search attributes are namespace-global] → the `Device*` prefix keeps them collision-free
  in a namespace shared with other demos; registration errors are loud at startup.
- [Snapshot write volume at fleet scale] → bounded by `devices / interval` plus meaningful
  transitions (D5); 1 000 devices at the default `1m` is ~17 writes/s, and the interval is the
  one knob if that ever matters.
- [Run chains created before this change are refused] → by design (D10); the stack runs a local
  dev Temporal, so the migration is to reset those chains (below), not to migrate payloads.

## Migration Plan

1. Apply the Mongo bootstrap (`deploy/mongo/init.js`) so `device_state_snapshots`, its validator,
   and its indexes exist; `deploy/mongo/verify.sh` asserts them.
2. Deploy `cmd/controlplane` (settings seeding) and `cmd/worker` (activity + attribute
   bootstrap) in the same release; run as many worker replicas as desired.
3. Reset pre-change device run chains on the dev Temporal (they carry carry-version-1 state and
   are refused loudly otherwise); the first signal after reset recreates each chain at
   carry version 2.
4. Rollback is binary rollback: chains created by this change then fail loudly on the old build
   the same way, and are reset the same way — nothing external is rewritten, snapshots remain a
   valid read model of the last state the entity reached.

## Open Questions

- Whether a later stage should keep snapshot history (one document per snapshot instead of one
  per device) for a device-page timeline — today's spec pins the single-document projection;
  changing it later is a data-model change, not a redesign of this one.
- Whether the rollout stage wants snapshot reads at all or keeps addressing the entity by query —
  it can adopt either without changing these specs.
