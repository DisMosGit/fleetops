# Design

## Context

See proposal.md — Why. What shapes the approach:

- `RolloutWorkflow` (`internal/temporal/rollout_workflow.go`) drives the sequence and ends a
  regressing rollout in `rollback()` (line 335): the helper sets the terminal state, logs, and calls
  `recordRollout` — no compensation exists. Every rollback outcome (`unhealthy_wave`,
  `decision_timeout`, `dispatch_failed`) funnels through it, and a rollout that cannot start takes a
  different path (`state.fail`) that never enters rollback.
- The workflow's state (`rollout_state.go`) carries each wave's identity, status, success rate,
  target count, and the failed/unreported device sets, but not the membership itself; the membership
  travels from the resolve activity to the dispatch and is recorded on the wave document. The state
  query's view is derived from that state in one function.
- `rollout.Store.RecordRollout` (`internal/rollout/store.go:76`) moves a rollout document's status
  under a filter that excludes the terminal statuses: a document that has concluded is never moved
  again. The saga therefore has to run *before* the terminal record, not after it.
- `DeviceWorkflow` owns the device's authoritative state (`device_state.go`): the current firmware
  version is adopted from a heartbeat report or from a successful command result, and the state is
  carried across rolling continuations under a stamped carry version. Nothing records what the device
  ran before, so a fleet has no record of what a device could be restored to.
- A firmware update is already version-agnostic: `CommandKindUpdate` carries the firmware id, version,
  and checksum; the agent fetches by id, applies by version, and the device adopts the version on a
  successful result. `agent.proto:220` states the design intent: "A rollback downgrade is the same
  command for the older version." `PhaseRolledBack` exists in the frozen contract but no producer
  emits it.
- `firmware.Store` reads metadata by id only (`Metadata`, `Open`); a version lookup exists solely as
  the unexported `versionExists` boolean the upload path uses. Versions are unique (a unique index).
- A device's firmware version is recorded in three places: the device workflow (authority),
  `devices.current_fw` (written by registration and by batched heartbeat ingestion), and
  `device_state_snapshots.current_fw` (the entity's own projection, monotone in `snapshot_at` and
  written when a firmware transition happens). No query, index, metric, or API counts devices per
  version.
- `internal/telemetry` owns the broker layout and one publishing path. Its exported publisher is
  heartbeat-shaped and fire-and-forget (`HeartbeatPublisher.Handle` buffers and returns); the
  confirm-waiting publisher (`sessionPublisher`) is unexported; `routingKey` maps only the heartbeat
  family; and publishes are `mandatory`, so a key no queue is bound to comes back unroutable. The
  declared topology binds `heartbeat.#` and `rollout.task.#` only. `cmd/worker` imports no telemetry
  package today: the rollout's activities run there and the broker pipeline runs in `cmd/controlplane`.

## Goals / Non-Goals

**Goals:**

- A regression leaves the fleet — devices, records, and the rest of the system — where the rollout
  found it, or explicitly says where it could not.
- The rollback is a plan an operator can read while it runs and after it finishes, with every step's
  outcome accounted for.
- A rollback interrupted at any point (worker death, broker outage, a device that never answers) is
  safe to continue or repeat: no double restore, no invented version, no duplicated announcement.
- The rollout's terminal record is written only once the compensations have run, so "rolled back"
  means the fleet is back, not that someone intended it.

**Non-Goals:**

- Changing the decision rules that *lead* to a rollback: the health gate, the decision timeout, and
  the dispatch-failure rule are untouched.
- Compensating anything other than firmware: a wave changes device firmware only, so nothing else is
  restored.
- An agent-side rollback mode, a new command kind, or any proto change.
- A consumer of the notification events (see proposal.md — What Changes).
- Metrics and tracing for the saga (the observability stage).

## Decisions

### D1. The plan is derived from the rollout's own recorded progress, in reverse

Entering rollback derives an ordered plan from the state the rollout already holds: the waves it
dispatched (each with its membership), the wave that ended it, and the outcome. The plan is
`notify(started)`, then one downgrade step per dispatched wave ordered most-recent-first, then the
inventory reconciliation, then `notify(completed)`.

The reverse order is what makes the plan a saga rather than a checklist: the compensating action for
the most recent forward step runs first, so the wave the rollout just moved is undone before the waves
that have been sitting on the firmware since earlier. The pairing is explicit — each downgrade step
names the wave whose dispatch it compensates, and the reconciliation names the bookkeeping — which is
the property the order alone cannot express.

Deriving the plan from state rather than from a fresh reading of the fleet is what makes a replay,
a retry, and a worker restart non-events: the same state derives the same plan, and the workflow's
control flow (not an external ledger) decides what has run. The membership therefore has to live in
the workflow's state, so `rolloutWaveProgress` gains the device set each wave was resolved to — it is
the one fact the dispatch already received and the plan must name.

*Alternatives:* (a) a fixed step list that does not name waves or devices — rejected: it cannot
address the devices a specific wave moved, and "compensate the pool" would touch devices this rollout
never changed; (b) reading the wave documents when rollback starts — rejected: it makes the plan
depend on a projection written by an activity and adds a store read seam for a fact the workflow
already holds; (c) re-resolving the target group at rollback time — rejected for the same reason wave
membership is recorded before dispatch: the pool can move under the rollout.

### D2. No step aborts the plan, and the terminal record comes last

A step whose work fails permanently is recorded with what it did and did not achieve and the plan
continues. The reconciliation and the completion announcement run over whatever the earlier steps
achieved: the reconciliation is precisely the step that makes a partial rollback legible, and the
announcement is what tells the rest of the fleet. Aborting on the first failed step would leave the
fleet's records unreconciled and the failure unannounced — the two outcomes the plan exists to
prevent.

Because `rollout.Store.RecordRollout` never moves a document out of a terminal status, the terminal
record is written after the plan has run, exactly as the rollout-workflow design anticipated ("the
saga will insert them between the decision and the terminal record"). The rollout therefore needs a
status for the interval in between, and `rolling_back` is it: non-terminal, so the saga's progress
writes are accepted, and honest, so an operator sees that the rollout has stopped deciding and is
compensating.

*Alternatives:* (a) recording `rolled_back` first and running the saga after — rejected: the store
would refuse every progress write, so the record of the compensations could not be stored at all;
(b) aborting the plan on the first failed step — rejected above; (c) retrying a failed step forever —
rejected: a rollout's terminal record would depend on a device that may never answer, and the
unrestored devices are the actionable output, not a hang.

### D3. The device entity owns the previous firmware version

`deviceState` gains `PreviousFw`, and one rule maintains it: whenever the version the device runs
changes, the version being left becomes the previous one; a report or a command conclusion that does
not change it moves nothing. The rule covers both adoption paths and is order-independent — an agent
that reports the new version in its first heartbeat after applying it and a command result that
arrives later produce the same state, whichever lands first. The field is carried with the state
(carry version 5) and exposed through the state query.

*Alternatives:* (a) the rollout captures each device's version before dispatching to it and records
it on the wave — rejected: it costs one state read per target device per wave before anything is
dispatched, races the command (the device can apply before the capture completes), and stores a
second copy of a fact the device already owns; (b) the downgrade activity reads the device's current
version at rollback time and treats it as the restore target — rejected: it is the *new* version by
then, and a retried attempt certainly reads the new one; (c) a speculatively "previous" version
derived from the firmware registry's creation order — rejected: version strings do not imply order,
and the device's own history is the only truthful source.

### D4. A downgrade is an ordinary update command, addressed by a rollback command id

The downgrade activity resolves the device's previous version through a new version-keyed metadata
read on the firmware registry (`version` is unique, so the lookup is unambiguous), checks that the
resolved firmware targets the device's model, and delivers a `CommandKindUpdate` for it under a
command id derived from the rollback and the device. Nothing else changes: the agent downloads by
firmware id, applies by version, and the device adopts the restored version through the same
`applyCommandResult` path that adopted the new one — and deduplicates the command by the same command
id, which is what makes a retried restore a no-op.

The command id has to differ from the wave's update command id for the same device (the wave's is
`waveID-deviceID`), because the two commands mean different things and the device's dedup ring would
otherwise collapse them. The rollback's is derived from the rollout (a rollout rolls back once) and
the device.

*Alternatives:* (a) a new command kind or an agent-side rollback mode — rejected: the frozen contract
already describes a downgrade as an update command, the agent has no rollback state to keep, and a new
kind would need a proto addition and an agent change for no behavioural gain; (b) reusing the wave's
command id — rejected: the device would treat the restore as a redelivery of the update it already
concluded; (c) emitting `PhaseRolledBack` for the restore — rejected: the device did complete an
update, and the placeholder phase has no producer or consumer to serve.

### D5. The downgrade step records one outcome per device, and never fails the step

The activity returns one of five outcomes per device: `restored`, `failed` (the device's own detail),
`unreported` (the deadline passed), `skipped` (the device does not run the firmware being rolled
back), `unavailable` (the restore could not be attempted — no previous version, an unknown version, a
model mismatch, an undeliverable command — with the reason). A transient failure inside the activity
(a state read, a registry lookup) is left to the activity's retry policy; a failure that outlives the
retries reaches the step as an activity error and is recorded as that device's `unavailable`, so the
step still records exactly one outcome per device.

Two consequences are deliberate. First, one unreachable or unrestorable device cannot cancel the
restore of its peers or abort the plan (D2). Second, the decision whether a restore is needed is the
device's own: the activity compares the version the device runs with the firmware being rolled back
and skips a device that does not run it. That guard is the whole idempotency story for the step — a
retried attempt, a repeated rollback, and a device that never took the firmware all converge on
"nothing to do" without a separate compensation ledger.

An `unreported` restore is not a lost one. The device entity keeps the command pending and its
dispatch activity retries for the life of the device's run chain, so an offline device is restored
when its agent returns; what was missing at the time is only the *report*, which the record states.

*Alternatives:* (a) a delivery failure as an activity error that fails the step (as a wave's update
delivery failure does) — rejected: a wave must not promote on an undelivered command, but a
compensation must not stop on one; the fact is recorded for the operator either way; (b) letting the
`update-device` activity do the work by passing it an older firmware — rejected: its contract is
"command what the rollout deploys" and it has no notion of a restore target or of skipping; (c)
retrying an unrestorable device until it works — rejected in D2.

### D6. The inventory is `devices.current_fw`, reconciled one device per activity against the authority

The reconciliation compares, per device, the version the device's workflow holds with the version the
fleet records for it in the `devices` collection, and corrects the record when they disagree. That
collection is the fleet's firmware inventory: it is the per-device version record the rollout's
eligible-pool query already reads, and it is the one that can stay stale indefinitely, because it
follows heartbeats and a downgraded device that goes quiet never sends another. The
`device_state_snapshots` projection is not reconciled: it is the entity's own projection and the
command-result transition schedules a snapshot immediately, so it self-heals; the `firmware`
collection is a catalog with no device references; rollout and wave documents record intent, not
reality.

The step runs one activity per device, mirroring the wave dispatch and the downgrade fan-out. That
keeps each activity small and uniformly retryable, needs no concurrency budget inside an activity, and
avoids an activity timeout that has to scale with a wave (a 750-device wave reconciled in one call
would). The workflow aggregates the per-device results into the inventory the rollback reports, so the
counts come from the versions the reconciliation actually read rather than from an aggregation over
the records it was correcting.

The correction is a conditional write (`current_fw` updated where it does not already equal the
authoritative version), so a rerun is a no-op that reports `agreed`, and a concurrent heartbeat write
is never regressed — both values are versions a device reported, and the next report corrects the
record again if it moved.

*Alternatives:* (a) one activity per wave with a bounded worker pool — rejected: its timeout would
depend on wave size and the pool size would be a knob nothing tunes; (b) a Mongo aggregation counting
devices per version — rejected: it reads the records the step exists to correct, and it cannot report
which device was unverifiable; (c) reconciling the snapshots projection as well — rejected: it has its
own writer, its own monotone write rule, and no reader that needs it.

### D7. Notifications are their own event family, published with confirmations through a notifier seam

The notification is a second event family in `internal/telemetry` with its own type,
`RollbackEvent`, its own routing keys (`rollout.notification.rollback.started` and
`rollout.notification.rollback.completed`) and a declared work queue the family routes to. Three
properties drive that:

- *Its own type.* `Envelope.Payload` is a concrete heartbeat sample struct, and the telemetry
  contract forbids repurposing a field within a schema version; a rollback event has no device, no
  sample, and no region-per-device. A separate type with its own schema version keeps both contracts
  readable.
- *A declared queue.* Publishes are `mandatory`, so a key with no binding is returned as unroutable
  and counted as a drop. The family therefore needs a queue bound to it before its first event, even
  though the consumer that reads it (dashboards, alerts) lands with the observability stage. The
  queue gets the retry queues and dead-letter queue every work queue gets, because it is declared
  through the same layout.
- *A notifier that waits.* The heartbeat publisher is fire-and-forget by contract: it buffers, counts
  drops, and returns nothing. An announcing step's contract is the opposite — the broker must have
  taken the event before the step is recorded complete. The notifier reuses the package's
  confirm-waiting publisher over a supervised session and blocks until the broker's verdict, so a nack,
  an unroutable message, or a lost connection is an error the step retries.

The event identity is derived from the rollout and the phase (`rollback-<rollout_id>-<phase>`), so a
retried publication repeats the identity and a consumer that deduplicates by it applies the event
once — the same at-least-once-plus-idempotent-consumer contract the heartbeat family uses. A rollout
rolls back once, so the phase is the only discriminator the identity needs.

The started event carries the failing wave's measured decision, and carries none when the wave ended
the rollout before it could be measured: a wave whose commands could not be delivered has no
measurement, and a fabricated one would misreport why the fleet is being restored.

The announcing steps carry a deliberately short retry budget: an announcement is not safety-critical,
and the started announcement runs before the downgrades, so a broker outage must cost the rollback
seconds rather than its deadline.

*Alternatives:* (a) broadening `Envelope.Payload` to an untyped payload — rejected: it weakens the
heartbeat contract that existing consumers decode and validates nothing; (b) publishing from the
control plane's existing publisher — rejected: the saga runs in the worker, and a hop to another
process for one publish adds a failure mode, a second connection to the same exchange, and an
interface whose contract is fire-and-forget; (c) publishing without a queue and treating the
unroutable outcome as acceptable — rejected: the announce step would fail permanently and the events
would go nowhere; (d) a device-scoped notification reusing the heartbeat enqueue path — rejected: a
rollback is not a device event.

### D8. The rollback's record lives in the workflow's state and in the rollout document

The workflow's state is authoritative and the state query reads it; the rollout document carries the
same record so that the outcome outlives the workflow's retention (a run's history expires on the
namespace's retention, the document does not) and so the fleet database holds the durable record the
API's later stages read. The write rides the existing `record-rollout-state` activity, extended with
the rollback record, and every step transition writes it — so an operator reading the collection sees
a rollback progress rather than only its end, and a rollout interrupted mid-plan leaves what it
achieved visible.

The record holds counts, not device lists, except for the one list that is actionable: the devices the
rollback could not restore. It also carries the outcome that ended the rollout — the reason the
compensations ran — so the document answers why the rollback happened without a field beside it. The
wave documents continue to hold the membership and the update
outcomes; duplicating them into the rollout document would give one fact two homes.

*Alternatives:* (a) a `rollbacks` collection — rejected: one document per rollout is the model, and a
second collection would need its own lifecycle, index, and reader for a sub-document; (b) recording
only the final outcome — rejected: the mid-flight view and the interrupted-run record are exactly what
makes a long rollback operable; (c) writing the record only in the workflow's state — rejected: it
disappears with the run's retention, and the document is the durable record.

### D9. The saga is not interruptible, and its waits are activities

Once a rollout has entered rollback, pause, resume, and approval change nothing: a compensation is not
interruptible, and a pause that could hold a rollback would hold the thing that makes the fleet safe.
The state's control transitions therefore guard on having entered rollback as they already guard on
being terminal, and the plan's waits are activity futures rather than timers — there is no window to
fold an operator signal into, and folding one would only record an intent the saga cannot honour.

The operator API is untouched: `rolling_back` is not a concluded status, so the API's rule for
refusing commands on a concluded rollout still describes its behaviour, and the state query is what
tells the operator why a command did nothing.

*Alternatives:* (a) letting a pause hold the plan at a step boundary — rejected: it would delay
exactly the work that makes the fleet consistent, and a resumed pause would not change what the
compensations do; (b) folding the signals and reporting them — rejected: an intent that is never
honoured is worse than an ignored one, and the state query reports the phase instead.

### D10. Idempotency comes from derivation, deterministic ids, and conditional writes

Every step is idempotent by construction rather than by a ledger: the plan is a pure function of the
rollout's state, so a replay derives it identically and Temporal's history guarantees a completed
activity is not re-executed; a restore is addressed by a command id derived from the rollback and the
device, and the device deduplicates it; the reconciliation writes only where the record disagrees; and
each announcement repeats its event identity. Nothing records an outcome it did not observe: the
five restore outcomes and the three reconciliation outcomes are total, and a failure that could not be
attributed becomes `unavailable` or `unverified` with its reason rather than a success.

*Alternatives:* (a) a compensation ledger in Mongo recording which steps ran — rejected: it is a
second source of truth beside the run's own history, and it would need its own crash semantics; (b)
relying on the device's dedup ring alone — rejected: the ring is bounded, so a long-delayed retry
could re-open a concluded command; the skip guard (D5) is the structural backstop, and it holds
regardless of the ring.

### D11. Testing split

- **Workflow behaviour** (`TestWorkflowEnvironment`): the plan's derivation and reverse order, the
  step recording, the terminal record's ordering, the non-interruptibility of the phase, a
  permanently failed step leaving the plan running, the reconciled inventory the view reports, and a
  re-run of a finished plan converging.
- **The downgrade activity**: table-driven unit tests over the device-state and registry fakes for
  every outcome, the skip guard, the retried command id, a deadline already passed, and a lookup that
  fails transiently.
- **The reconciliation activity**: table-driven unit tests for `agreed`, `corrected`, `unverified`,
  the no-write case, and an unreadable device.
- **Telemetry**: unit tests over the package's existing session fake for the routing keys, the
  confirm-before-return behaviour, the drop counting, and the event identity; a `//go:build
  integration` case against a real broker for delivery to the declared notification queue, including
  the queue's retry and dead-letter paths.
- **MongoDB** (`//go:build integration`): the rollback record's round trip, a document written without
  it staying updatable, and the conditional correction against a real `devices` document.
- **End to end**: the existing `cmd/worker/rollout_integration_test.go` smoke drives a real rollback
  through the real activities with a scripted device world, asserting the restored devices, the
  recorded record, the terminal ordering, and the announcements published to a real broker.

The device-world fake (`internal/temporal/rollout_deviceworld_test.go`) hardcodes `CurrentFw` and
needs a settable current and previous version, and the registry fake needs a version-keyed answer,
before any of this is testable.

## Risks / Trade-offs

- [A large rollback schedules two activities per device] → the plan compensates only the waves that
  were dispatched, and a canary usually fails early with a small membership; the per-wave fan-out
  already establishes the pattern and its history budget, and a rollout `ContinueAsNew` rollover is
  the escape hatch if a full-fleet rollback ever approaches the limits.
- [The notification queue accumulates events while no consumer is attached] → the events are small
  and the queue is the integration point the observability stage attaches to; an operator can purge
  it, and a retention TTL is an open question rather than a hidden default.
- [A broker outage delays the rollback by the started announcement's retry budget] → the budget is
  short by design (D7) and the compensations are never gated on the announcements, so the worst case
  is a few seconds before the downgrades begin.
- [A device that is offline during a rollback is recorded as unrestored] → the entity keeps the
  command pending and delivers it when the agent returns, and the unrestored list on the rollout
  document, in the state query, and in the completion announcement tells an operator exactly which
  devices are still on the rolled-back firmware.
- [The reconciliation writes `current_fw` while heartbeat ingestion may be writing it too] → both
  write a version a device reported, the conditional update never regresses a record to a value that
  was not read from a device, and the next heartbeat or snapshot corrects the record if reality moved
  again.
- [Rollout runs lengthen: a rollback now takes as long as its compensations] → the waits are the
  configured result timeout per device, the steps are bounded by their retry policies, and the
  alternative (declaring the rollout over while devices still run the bad firmware) is the problem
  this change exists to fix.
- [`rolling_back` is a new status value for every consumer of the vocabulary] → it is additive, the
  Mongo field is an unconstrained string, the existing search attribute carries it automatically, and
  the deploy documentation states the vocabulary in one place.
- [The API still answers `202` to a control command during a rollback, which the workflow ignores] →
  accepted and specified: the state query reports the phase, and refusing the command at the surface
  is an open question rather than an unstated behaviour.

## Migration Plan

No data migration. Nothing else writes `rollouts` or the device's carried state.

Deploy order: let in-flight rollout runs and device run chains conclude, or terminate them in the
Temporal UI (device state's carry version moves to 5, and this change alters `RolloutWorkflow` in
place), then rebuild and restart `cmd/worker` (new registrations, the notifier and its broker
session) and re-apply `deploy/mongo/init.js` for the extended `rollouts` validator. No configuration
change: the downgrade waits under the existing `rollout.result_timeout`, and the new settings are
code constants.

Why no `workflow.GetVersion` branch: this changes the rollout's control flow around its terminal
record and the device state's carried schema. A versioned branch would have to keep both the
"record and stop" and the saga paths alive, and both device state schemas, for runs whose whole life
is minutes on a single-node local Temporal — the same reasoning the earlier workflow changes recorded,
and the repo's established migration for in-flight runs is a documented drain.

Rollback: revert both binaries. `rollouts` documents written with `rolling_back` or a rollback record
remain valid for the previous validator (the status is a plain string and extra properties are
allowed), but a rollout left mid-rollback by the revert is a run the previous build cannot continue —
terminate it in the Temporal UI and re-drive the firmware if needed.

## Open Questions

- Should the operator API refuse pause, resume, and approve once a rollout has entered rollback
  (answering `409`), rather than accepting a command the workflow ignores? Deferred: the state query
  answers why, and the API's contract is unchanged by this change.
- Should the unrestored devices be re-driven automatically — a follow-up rollout of the restored
  firmware, or a bounded retry loop inside the saga? Deferred: the unrestored list is recorded and
  announced, an operator can deploy the older firmware with the existing API, and an automatic
  re-drive needs its own policy for how long to keep trying.
- Should the notification queue carry a retention TTL while it has no consumer? Deferred until the
  observability stage decides who reads it.
- Should the rollback be reachable as an operator action (a manual rollback of a healthy but unwanted
  rollout) rather than only as the consequence of a failed gate? Deferred: it needs an operator-facing
  meaning for "roll back a rollout that did not fail".
- Should the rollout's search attributes expose the compensating phase as a distinct filter value?
  They already carry the status, so the phase is filterable; a dedicated attribute is only worth it if
  an operator asks for one.
