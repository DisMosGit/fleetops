# Proposal

## Why

`cmd/controlplane` and `cmd/worker` both poll the shared `fleetops` task queue, but they register
disjoint sets of task types: the worker registers `device-workflow`, `snapshot-device-state`, and
the rollout workflow with its nine activities, while the control plane registers only
`dispatch-command`. Temporal routes a task to *any* poller of its queue, not to a poller that
registered that task type, so most tasks handed to the control plane fail as unknown and are
redelivered with retry backoff until a worker replica receives them.

The damage is not theoretical. A live smoke observed snapshots more than a minute stale against a
15s snapshot interval, and a device reported `online: false` while its agent was alive — a
violation of the bounded staleness `device-state-snapshot` requires. The same bounce also hits
`rollout-workflow` and every rollout activity, including `record-wave-state` and
`evaluate-wave-health`, the tasks that decide a wave's promotion. Every workflow added to that
queue in future inherits the defect, and one already did: the rollout workflow landed on the
shared queue without the control plane being updated.

## What Changes

- The control plane stops polling the workflow task queue entirely. It gets its own task queue and
  polls only that one, registering only the activity it owns — `dispatch-command`, whose side
  effect is the in-process agent hub. A process can then no longer be handed a task type it does
  not host, so the defect cannot recur when a new workflow joins the work queue.
- Device workflows schedule `dispatch-command` on the control-plane queue rather than on the queue
  they run on, so the activity lands where the hub lives while everything else stays on the work
  queue.
- Configuration gains the control-plane queue name, seeded into new device run chains alongside the
  snapshot interval and the offline threshold, so a device entity knows where to send its commands
  for the life of its run chain.
- Worker replicas continue to poll only the work queue and are never registered on the
  control-plane queue.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `temporal-worker`: the queue topology is specified — which process polls which queue, that a
  queue is polled only by processes that host every task type it can deliver, that the dispatch
  activity is registered on the control-plane queue and never on the work queue, and that device
  workflows schedule their command dispatch there.
- `runtime-config`: the shared configuration file gains the control-plane task-queue value,
  validated as non-empty, consumed by `cmd/controlplane` and by `cmd/worker` when it seeds device
  settings.

## Impact

- **Code**: `cmd/controlplane` (poll its own queue instead of `temporal.task_queue`; pass the
  dispatch queue to the signaler), `internal/temporal` (dispatch activity options gain an explicit
  task queue; `Signaler`/`DeviceSettings` carry the dispatch queue so it rides the entity state),
  `cmd/worker` (seed the dispatch queue into device settings), `internal/config` (the new value,
  its default, and its validation).
- **Dependencies**: none new.
- **Contracts**: none. The frozen v1 proto is untouched; queue topology is Temporal-internal.
- **Existing behavior**: `dispatch-command` and the device workflow's dispatch flow keep their
  current semantics — at-least-once delivery keyed by command id, the same retry policy, the same
  non-retryable malformed-command error. Only the queue the activity is scheduled on and polled
  from changes. Rollout, snapshot, and telemetry behavior are unchanged.
- **Deployment**: the control plane's new queue is polled by the control plane alone, so an
  operator must run at least one control plane for device commands to dispatch; this is already
  true, since the hub holds the agent connections.
- **Out of scope**: the live two-replica and end-to-end smoke verification, which stays as the
  deferred tasks in `add-device-search-attributes-snapshot-and-worker` (5.3, 6.2); the
  `openspec/specs/` archive blocker; and the product decision on device failure versus wave health.
- **Archiving dependency**: the `runtime-config` delta here is a MODIFIED one, and
  `openspec/specs/` is currently empty — nothing has created the base `runtime-config` spec yet
  (every change that touches it modifies it, none adds it). `openspec validate` already reports
  that archiving would refuse this delta until that base spec exists, which is the same
  repo-wide blocker the roadmap tracks. The `temporal-worker` delta is ADDED-only and so is not
  affected. This change therefore cannot be archived before the base-spec problem is solved; the
  fix belongs to a separate change, not to this one.
