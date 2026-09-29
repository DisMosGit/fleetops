# Design

## Context

Two Temporal entrypoints poll the same queue today. `cmd/worker` polls
`cfg.Temporal.TaskQueue` (`fleetops`) and registers `device-workflow`, `snapshot-device-state`,
`rollout-workflow`, and the rollout workflow's nine activities. `cmd/controlplane` polls the same
queue and registers only `dispatch-command`, whose side effect is the in-process
`agentserver.Hub` holding the agent streams. Temporal delivers a task to whichever poller is
available, not to one that registered its type, so every task the control plane receives that is
not `dispatch-command` fails as an unknown type and is redelivered with retry backoff. See
proposal.md — Why.

Two existing constraints shape the fix. First, `add-rollback-saga` requires the worker to host the
publisher a rollback's announcements are published through, "so a rollback's announcement is
published by the process that runs the rollback rather than by a second one holding the same
connections" — so the rollout activities cannot move to the control plane. Second, the device
workflow already carries a `DeviceSettings` value (snapshot interval, offline threshold) seeded at
run-chain start and preserved across rolling continuations, and `carryVersion` already gates the
carried schema loudly via `ErrUnsupportedCarryVersion`.

## Goals / Non-Goals

**Goals:**
- No process is ever handed a task type it does not host, and the property holds for workflows and
  activities added to the work queue in future without anyone remembering to update the control
  plane.
- Device command dispatch keeps its current semantics: same activity, same at-least-once delivery
  keyed by command id, same retry policy, same non-retryable malformed-command error.
- Every process keeps running only the side effects it owns.

**Non-Goals:**
- Moving `dispatch-command` out of the control plane, or moving any rollout activity into it. The
  hub and the rollback publisher stay where they are.
- Changing the number of Temporal namespaces, or introducing per-task-type queues beyond the one
  separation this change needs.
- The live two-replica and end-to-end smoke verification of the fix (deferred in
  `add-device-search-attributes-snapshot-and-worker` as tasks 5.3 and 6.2).

## Decisions

### D1. Separate the queues rather than registering everything everywhere

The control plane stops polling the work queue and polls its own queue instead. The alternative —
having the control plane register all thirteen task types so it can serve any task it is handed —
was rejected: the rollout activities' side effects are built by `newRolloutDeps` inside
`package main` of `cmd/worker` and would have to move to an importable package, and the control
plane would have to run a `telemetry.Notifier` broker publisher. That duplicates the worker's
dependencies and connections inside the gateway process and contradicts the rollback
publisher-hosting requirement quoted above. Worse, it fixes the defect only for the task types
someone remembers to register: the rollout workflow landed on the shared queue unregistered once
already, and registration would have to be revisited for every future workflow. Separating the
queues removes the *polling relationship* that causes the defect, so a workflow added to the work
queue later is delivered only to its host by construction.

### D2. The dispatch queue is a configured value, not a constant

`temporal.dispatch_task_queue` is added to the shared configuration. The alternative — deriving
the name in code (`task_queue + "-controlplane"`) — was rejected: every other endpoint and queue in
this system is a named configuration value, and a derived name cannot be pointed at a different
server or namespace per environment without a code change. The value follows the existing
`temporal.task_queue` pattern: documented in the sample file, defaulted, validated as non-empty.

### D3. The dispatch queue travels in the carried settings

The device workflow decides where to schedule `dispatch-command`, so it must know the queue name.
It is added to `DeviceSettings`, which is seeded by `NewSignaler` callers and rides every
`ContinueAsNew` payload. The alternative — reading it from configuration inside the workflow — is
impossible without breaking determinism, and the alternative of a workflow constant contradicts
D2. Carrying it gives the setup the same deploy-time semantics as the snapshot interval and the
offline threshold: a run chain decides consistently for its whole life. Both `cmd/controlplane`
and `cmd/worker` build `DeviceSettings`, and both must set the field, or a device's commands would
be scheduled on an empty queue.

### D4. The carried schema version is bumped, and an empty queue is refused loudly

`carryVersion` bumps from 5 to 6, and `validate` refuses a carried state whose dispatch queue is
empty, alongside the existing positive-interval and positive-threshold guards. The alternative —
tolerating an empty queue and defaulting it at the point of use — was rejected: an empty task queue
does not fail a workflow, it leaves the dispatch activity unschedulable, which would surface as
commands silently never reaching devices and the retry policy backing off for the life of the run.
Refusing the payload at the top of the workflow fails visibly. The cost is intended and already
established by this state's history: a run chain created before this change is refused rather than
half-understood, and the local dev stack's migration is to reset those chains.

### D5. The control plane's worker keeps a registry seam and a bounded stop

`cmd/controlplane` gains the same small registry seam `cmd/worker` uses, so its registration is
asserted directly in a unit test instead of only through a running server, and its worker gets a
bounded `WorkerStopTimeout` matching the worker binary's. This is a testability and shutdown-
symmetry fix, not a behavior change.

## Risks / Trade-offs

- [Two queues must both be served for commands to flow] → For a device command to dispatch, a
  control plane must be running. This is not new: without the control plane there are no agent
  connections to dispatch onto, so the hub is already a hard prerequisite. The activity's existing
  retry policy (capped backoff for the life of the run, non-retryable malformed commands) already
  covers a control plane that is briefly absent.
- [An optional `dispatch_task_queue` omitted from a hand-written config file] → The field defaults
  to `fleetops-controlplane`, so an absent value is safe; only an explicitly empty value is
  rejected, and it is rejected by validation at startup, naming the field.
- [Run chains created before this change are refused] → Intended, per D4 and this state's existing
  migration story: the stack runs a local dev Temporal, so the migration is to reset those chains,
  not to migrate payloads.
- [The two binaries could drift to different queue names] → Each binary reads the same shared
  configuration file, and validation rejects an empty value. A mismatch leaves commands
  undispatched rather than corrupting state, and the deferred live smoke in the device change
  (task 5.3) is where a misconfiguration is observed end to end.
- [The control plane is now a single-queue poller, so a stopped control plane no longer silently
  helps serve work] → It never correctly served any work beyond `dispatch-command`, so nothing is
  lost; the change makes its actual role visible instead of load-dependent.

## Migration Plan

1. Apply the configuration change first (`temporal.dispatch_task_queue`, defaulted), so both
   binaries read a file that carries the value before either expects it.
2. Deploy `cmd/controlplane` and `cmd/worker` in the same release. The two must move together:
   a control plane on the new build polls only its own queue, while a worker still on the old build
   schedules dispatch onto the work queue, where nothing serves it.
3. Reset pre-change device run chains on the dev Temporal: they carry a `carryVersion` 5 payload
   and are refused loudly. The first signal after reset recreates each chain at version 6 with the
   dispatch queue in its settings.
4. Rollback is binary rollback in the same pairing: both binaries revert together, chains created
   by the new build are refused by the old one the same way, and are reset the same way.

## Open Questions

None. The remaining unknown — whether the bounce is actually gone under real load, and whether
snapshot staleness returns inside its bound — is a live check already deferred as tasks 5.3 and 6.2
in `add-device-search-attributes-snapshot-and-worker`, and it cannot change this design.
