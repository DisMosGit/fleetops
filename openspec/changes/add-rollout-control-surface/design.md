# Design

## Context

See proposal.md — Why. What shapes the approach:

- `RolloutWorkflow` (`internal/temporal/rollout_workflow.go`) drives the sequence: resolve a wave,
  dispatch it with one `dispatch-wave-update` activity that signals every target device's workflow,
  hold a durable timer for the health window, evaluate through `evaluate-wave-health`, then promote
  or roll back. `rolloutState` carries no pause concept, and the workflow's only signals are
  `approve_next_wave`. The state is written to MongoDB through `record-rollout-state` /
  `record-wave-state`, and the query `get-rollout-state` returns the view.
- `DeviceWorkflow` (`internal/temporal/device_workflow.go`) owns the pending command: it accepts
  `command_issued` (deduplicating by command id), dispatches it to the agent stream through the
  `dispatch-command` activity registered in the control plane, accepts `command_result`, and clears
  the pending command on a matching result. It carries its state across rolling continuations
  (`carryVersion` 3) and exposes it through `get-state`; `State` has no record of the outcome of a
  command that concluded.
- `internal/temporal/searchattrs.go` derives device attributes from state, detects changes against
  the previously upserted map, and bootstraps the namespace's attributes at worker startup through
  `EnsureSearchAttributes` over a typed map of names to indexed value types.
- The control plane (`cmd/controlplane`) holds the hub, the gRPC agent server, the firmware upload
  HTTP handler on the gateway listener, and a Temporal client; the worker (`cmd/worker`) hosts the
  device and rollout workflows, their worker-side activities, and the attribute bootstrap.
  `RolloutStarter` exists but only tests use it: nothing starts a rollout in production.
- The `waves` document records membership, start, status, and success rate; `wavehealth` reads
  `device_ids` and `started_at` and clips the window at the wave start.

## Goals / Non-Goals

**Goals:**

- A wave's completion rests on reported per-device results, and no wave can be held open by a
  device that never reports.
- An operator can hold a rollout and continue it without losing recorded progress or a banked
  approval, and every wait involved is durable across worker restarts.
- One operator surface: what the HTTP API answers is the workflow's own state, and what it sends
  are the workflow's own signals — no second source of truth, no second signaling path.
- Rollout runs are findable by firmware version, target region, and current status in the Temporal
  UI, with values that cannot drift from state.

**Non-Goals:**

- Rollback compensations (downgrade, inventory, notification) — still the saga, Phase 8.
- The SSE live-status stream, the React UI, and a rollout *list* endpoint (stage 5).
- Changing the health gate's arithmetic: per-device failures are recorded, not promoted into a
  second promotion boundary.
- Metrics and tracing for rollout progress (stage 4).
- Mirroring the concluded command into the `device_state_snapshots` projection: the device state
  query is what the waiting activity reads, and the wave document is where device failures are
  recorded, so the projection keeps the shape the stage-2 change gave it.
- Rollback of the API surface's dependency on Temporal being up: a rollout's state is read from its
  workflow execution, not from MongoDB, so the deployment docs keep saying what a running rollout
  needs.

## Decisions

### D1. Dispatch becomes one update activity per device; the wave-level dispatch activity is removed

A wave schedules one `update-device` activity per target device instead of one
`dispatch-wave-update` activity for the whole wave. Each activity signals `command_issued` to its
device's workflow — the entity that owns the pending command, its dedup by command id, and its
delivery retries — and then waits for that device's conclusion. `DispatchWaveUpdateActivityName`,
`DispatchWaveRequest`, and `NewDispatchWaveActivity` are deleted rather than kept beside the new
path: the wave-level activity's only remaining caller would be nothing, and two dispatch
mechanisms would make a wave's completion mean two different things.

*Alternatives:* (a) the activity sends straight to the agent hub and waits on an in-process
rendezvous in the control plane — rejected: it bypasses the entity that owns the pending command
and its dedup, needs the hub inside the worker, and an in-memory waiter loses a report that arrives
while no activity happens to be waiting (an agent reports once; the entity's state is durable).
(b) the device entity signals each concluded command back to the rollout, which waits for one
signal per target — rejected: it removes the per-device update activity this change is about, and
it would make every device workflow depend on knowing its orchestrator.

### D2. Waiting reads the device entity's state query, and the entity records what it concluded

The update activity waits by evaluating the device's `get-state` query at a bounded interval
(`updatePollInterval`, 2s) while recording activity heartbeats, so a lost attempt is retried
promptly and every attempt re-reads the recorded outcome instead of depending on live observation.
The wave's heartbeat timeout is derived from the result timeout but never falls below that interval:
an attempt can only heartbeat between two observations, so a shorter timeout would kill an attempt
that is waiting exactly as designed and retry every one of them. A wave with a result timeout
shorter than a few observation intervals is what makes the floor bite, and the smoke's three-second
result timeout is such a wave — before the floor, every wait was force-retried and every retry
re-delivered the command.
Because a cleared pending command alone cannot distinguish a success from a failure (a device
already running the commanded version would look successful either way), the device state gains the
last concluded command: `ConcludedCommand{Command, Outcome, Detail}`, set by `applyCommandResult`
when it concludes the pending command, exposed through `State`, and carried across rolling
continuations (`carryVersion` 4).

Every observation failure is treated as "not reported yet" and the wait continues to the deadline:
a query error is not evidence about the device, and a wave must not roll back because the observer
blinked. Only a *delivery* failure is an activity error, because a device the rollout could not
command is a fact about the wave (D4).

*Alternatives:* (a) poll the `device_state_snapshots` projection — rejected: it is a best-effort
projection whose write may lag or fail, and it would have to mirror the concluded command for a
read the workflow's own state already answers; (b) observe each device once after the window —
rejected: that is not "wait for the reported result", and it cannot distinguish a device that
finished in ten seconds from one that never started.

### D3. The result deadline is decided by the workflow and carried in the request

At the wave's dispatch the workflow computes `deadline = workflow.Now(ctx) + result_timeout` and
passes it in every update request. The value is a workflow clock reading recorded in history, so a
replay, a retry, or a restart recomputes the same deadline, and a worker outage cannot extend the
window a wave waits in. The activity's `StartToCloseTimeout` is `result_timeout` plus slack so a
deadline can always be reached, and its `HeartbeatTimeout` bounds how long a lost attempt waits to
be retried.

*Alternatives:* (a) `StartToCloseTimeout` alone as the bound — rejected: a retried attempt would get
a fresh timeout and could outlive the wave's result window; (b) the activity computing
`time.Now() + timeout` itself — rejected: a retry after a restart would restart the clock; (c) the
workflow enforcing the deadline with a timer and canceling the activities — rejected as more moving
parts for the same guarantee, since the deadline is already absolute and the activity already has
to stop waiting at it.

### D4. Device outcomes are recorded, the health gate still decides, and delivery failure still fails the wave

A wave is complete once every target has settled: reported success, reported failure, or
`unreported`. Failed and unreported devices are recorded on the wave document and counted in the
state view; they do not by themselves decide the wave, whose promotion stays the configured health
verdict. A command that cannot be *delivered* at all is different: it is an activity error, the
wave is recorded `failed`, and the rollout transitions into rollback — a wave the rollout could not
command is not evidence of health, and that rule already exists in the wave vocabulary.

*Alternatives:* (a) any failed device fails the wave — rejected here: it introduces a second,
unconfigured promotion boundary that the configured gate's arithmetic did not ask for (see Open
Questions); (b) treat an undeliverable command as `unreported` — rejected: it would hide a wave the
rollout never commanded behind a device-level outcome.

### D5. A wave's update fan-out is bounded, cancelable, and folded

The workflow schedules all of a wave's update activities up front, then waits for them in target
order, folding operator signals while it waits, so devices are commanded concurrently and the
workflow is never blocked on one device. The first delivery error cancels the remaining activities
and fails the wave instead of waiting out their deadlines. Waiting in target order keeps the
selector small and the history linear: a wave costs two to three events per target device (a
1000-device sequence is roughly 5k events, the largest run in this project but comfortably inside
Temporal's default history limits).

*Alternatives:* (a) `workflow.Go` per device with a result channel — rejected: hundreds of
coroutines to express "wait for all" that a future per device already expresses; (b) a selector
holding every pending future at once — rejected: the callback set would have to be rebuilt per
completion for no behavioural gain; (c) waiting for all activities even after a wave is already
lost — rejected: it would delay a rollback by up to the result timeout.

### D6. Pause and resume are state, and the status is a projection of it

`rolloutState` gains `Paused`; the rollout's status becomes a derivation rather than a field the
control flow maintains:

1. a terminal status, if the rollout concluded;
2. `paused`, if the operator paused it;
3. `awaiting_approval`, if it is holding at a gated wave;
4. `running`.

One function produces it, so the recorded document, the search attribute, and the state query
cannot disagree. Pause and resume are folded at every wait (approval, dispatch/collection, window,
gate re-measure) through one helper, so no wait path can ignore them; applying one records the
rollout when the derived status changed. A pause takes effect immediately for advancement (no wave
is resolved or dispatched) and is *visible* immediately even while a wave is in flight, because the
operator's intent is the thing the status reports.

A wave already in flight is still driven to its recorded decision: its window and decision timeout
are honoured unchanged, its health is measured over the same membership, and a failing verdict
still rolls the rollout back. When a healthy in-flight wave has been decided, the paused rollout
holds before the next wave rather than promoting into it.

*Alternatives:* (a) pause only at wave boundaries — rejected: an operator's pause would be invisible
for up to a health window and the query would report `running` while the rollout is held; (b)
suspend the in-flight wave's window and re-anchor it on resume — rejected: it would invalidate the
recorded window the health aggregation reads (samples after the pause would be mixed with samples
from before it), and the update is already on the device regardless; (c) a separate `pause_requested`
flag with `running` status — rejected: two fields describing the same thing, and every consumer
would have to know to look at both.

### D7. The HTTP surface is a thin adapter beside the firmware API, and the start path moves to the control plane

A new `internal/rolloutapi` package serves the five routes over three consumed seams — `Starter`
(satisfied by `*temporal.RolloutStarter`), `States` (a new `*temporal.RolloutStates` query adapter),
and `Signals` (a new `*temporal.RolloutSignals` adapter sending `approve_next_wave`,
`pause_rollout`, `resume_rollout`). The handler answers with the workflow's view verbatim — the
state query is the contract, so the API cannot drift from it — maps `temporal.ErrRolloutNotFound`
(a sentinel defined beside the adapters) to 404, refuses commands for a concluded rollout with 409
by reading the state first, and bounds every Temporal call with its own short deadline. Routes are
method-scoped patterns (`POST /api/rollouts`, `GET /api/rollouts/{id}`, `POST
/api/rollouts/{id}/approve|pause|resume`), so method mismatches become 405 from the mux. The
gateway composes the two APIs on one listener by mounting each package's handler under its path
prefix.

The control plane is now the process that starts rollouts, so the configuration→policy mapping
(`config.Rollout` → `temporal.RolloutSettings`) moves from `cmd/worker` — where nothing but tests
used it — to `cmd/controlplane`, beside the mapping it already does for `wavehealth`. That follows
the repo's existing pattern: each entrypoint maps the configuration sections it consumes onto the
library types it needs.

*Alternatives:* (a) mapping in `internal/config` — rejected: it would make the configuration package
depend on the Temporal SDK and pull it into the agent emulator binary; (b) mapping in
`internal/temporal` — rejected: the orchestration package should not know the configuration file's
shape; (c) reading the rollout's state from MongoDB in the API — rejected: the documents are a
projection, and a wave in flight (its outcomes, the current wave, the outstanding approval) is only
complete in the workflow's state; (d) validating the firmware in the start endpoint — rejected: the
load-firmware activity is the single decider, and its refusal is a recorded rollout outcome the
operator can read (a `failed` rollout with `firmware_unknown`), not a start-time rejection; (e)
server-generated rollout ids — rejected: the id is the workflow identity and the record key, and a
client-supplied id is what makes a retried start idempotent (409 instead of a second rollout).

### D8. Rollout search attributes mirror one pure function of rollout state

`rolloutSearchAttributes(state)` derives `RolloutFirmware` (the version, empty until loaded),
`RolloutRegion`, and `RolloutStatus`; the workflow upserts the changed map inside the same helper
that records the rollout document, comparing against the map it last upserted so a state change
that mirrors nothing does not upsert. The namespace registration extends the existing typed map in
`searchattrs.go`, so the worker's startup bootstrap covers both workflow families with one
idempotent call.

*Alternatives:* (a) seeding values through `StartWorkflowOptions.SearchAttributes` — rejected: the
firmware version and the status change after start, so upserts would be needed anyway and the values
would have two sources; (b) a second attribute set per wave — rejected: the operator filters
rollouts, not waves.

### D9. The device entity's concluded command is new state; the rollout state's version marker is not

The device state travels between runs, so its carry version is bumped to 4 and a state from an older
schema is refused loudly, as the earlier device changes established. The rollout state never travels
between runs (a rollout spends its life in one execution), so its version marker is not a
compatibility gate — the new fields are additive and default to the values a running rollout would
have had.

### D10. Testing split

- **Workflow behaviour** (`TestWorkflowEnvironment`): the existing fake world gains a scriptable
  device world (devices that succeed, fail, or never report, with commands recorded), so the real
  activity implementations are exercised end to end inside the workflow test: per-device outcomes
  recorded, mixed outcomes, an unreported device, a delivery failure failing the wave, pause and
  resume folded at each wait, and the search attributes at each status.
- **The update activity** itself: table-driven unit tests over a hand-written fake device-state
  reader with scripted conclusions, deadlines already passed, superseded commands, and query
  failures.
- **The HTTP surface**: `httptest` over fake seams for every status code in the delta spec, plus a
  handler-level test that the two APIs compose on one mux.
- **MongoDB**: the `//go:build integration` seam proves the new wave fields round-trip and that a
  document written without them stays updatable.
- **End to end**: the existing smoke (`cmd/worker/rollout_integration_test.go`) already runs a real
  MongoDB, a real Temporal dev server, and the worker's own registrations. It is extended to drive
  real rollouts through the real update activities (with the device seams faked, as they are today)
  and the real HTTP handler over real client adapters, asserting per-device outcomes, pause/resume,
  and approve over the wire — with `rollout.result_timeout` shrunk so an unreported device is
  provable in seconds. It still does not run a control-plane process, an agent emulator, or a
  worker restart; those stay with their own suites.

## Risks / Trade-offs

- [Waiting by polling the device state costs one query per target every 2 seconds] → a 750-device
  wave is roughly 375 queries/s for up to the result timeout. Accepted at demo scale and bounded by
  the deadline; the entity→rollout callback alternative (D1b) is a drop-in replacement behind the
  same wave contract if a larger fleet needs it.
- [A per-device failure does not decide a wave] → a fleet that fails every update but keeps
  heartbeating healthily could promote a wave that took nothing. Accepted: the health ratio is the
  configured promotion boundary, and the failures are recorded on the wave and counted in the state
  view; making per-device failures a gate is a policy decision, not an implementation detail (Open
  Questions).
- [History growth per wave is proportional to target count] → a 1000-device sequence is ~5k events.
  Under the default limits, and the largest run in this project; a rollout `ContinueAsNew` rollover
  is the escape hatch if wave sizes grow (Open Questions).
- [An update activity occupies a worker slot for up to the result timeout] → 750 concurrent
  activities is inside the SDK's defaults, and validation keeps `result_timeout` no longer than
  `decision_timeout`, so the window a wave waits in cannot exceed the deadline at which its verdict
  is already due.
- [`RolloutFirmware` is empty for a rollout whose firmware metadata never loaded] → an unknown- or
  mismatched-firmware rollout is findable by region and status, not by firmware version. Accepted:
  the version is not knowable from the start input.
- [The API's state read requires a worker to be polling the rollout's task queue] → a query against
  an execution nothing is running blocks until its deadline, which the handler bounds and answers
  as a failure. Accepted: the API reads the workflow's own state by design, and the deployment docs
  say a rollout needs a running worker.
- [A pause can be delivered while a wave is being dispatched] → the wave still completes and its
  decision is recorded; only promotion is withheld. Accepted and specified (D6), because the
  devices have already been commanded.
- [In-flight rollouts and device run chains cannot span this upgrade] → see Migration Plan; the
  alternative (`workflow.GetVersion` branches) keeps two dispatch mechanisms and a dead activity
  alive for runs that last minutes on a local dev stack.

## Migration Plan

No data migration. Nothing else writes `rollouts`, `waves`, or device state.

Deploy order: let in-flight rollout runs conclude, or terminate them in the Temporal UI, and expect
device run chains to be recreated, then rebuild and restart `cmd/worker` (new registrations, new
device carry version, attribute bootstrap covering seven attributes) and `cmd/controlplane` (the
gateway routes, the start path, and the new client adapters). Update `deploy/config.yaml` with
`rollout.result_timeout` if the default is not wanted, and re-apply `deploy/mongo/init.js` for the
extended `waves` validator.

Why no `workflow.GetVersion` branch: this change alters `RolloutWorkflow`'s logic in place (dispatch
shape, waits, and attribute upserts) and `DeviceWorkflow`'s carried state, and the worker stops
registering `dispatch-wave-update` and `RolloutStarter`'s wave-level request. A versioned branch
would have to keep the old dispatch activity registered and both dispatch mechanisms specified
indefinitely, for runs whose whole life is minutes on a single-node local Temporal; the repo's
established migration for in-flight workflow runs is a documented reset (the device state's carry
versions), and a rollout's recorded documents stay as historical records.

The new `waves` fields are deliberately optional in the validator while the store always writes
them, so a document written before this change remains updatable by the new build and a document
written by the new build stays valid for the old one — a mixed deploy (or a revert) neither breaks
writes nor requires a backfill. The paused status needs no validator change: `status` is a
constrained string only in the specification and documentation.

Rollback: revert both binaries once in-flight runs have concluded or been terminated. Wave
documents carrying the new arrays are still valid for the previous validator (extra properties are
allowed), and no index or collection changed.

## Open Questions

- Should a per-device update failure decide a wave by itself — for example failing the wave on any
  failed device, or on more than a configured share of unreported ones? Deferred: it is a promotion
  policy the configured health gate does not currently express, and it needs an operator-facing
  meaning ("this wave took the firmware on 97% of its targets").
- Should the operator surface expose *which* devices failed, rather than how many? The wave
  document records the ids; the state view carries counts. Deferred until the UI reads the fleet
  database.
- Should rollouts roll over with `ContinueAsNew`? A full 1000-device sequence is roughly 5k events;
  a larger fleet or a longer sequence would want a bounded run. Deferred.
- Should the update activity's poll interval, or the entity→rollout callback that replaces polling
  altogether, be configurable? Deferred: nothing tunes it yet, and the callback would change the
  device entity's contract.
- Should `POST /api/rollouts` accept a per-rollout wave sequence or gate thresholds instead of the
  deployment's configured policy? Deferred: the configuration file is this project's single policy
  surface, and a per-rollout policy needs a validation story of its own.
- Should the API expose a rollout list (and read it from MongoDB) so an operator does not need the
  Temporal UI to find a rollout id? Deferred to the UI stage.
