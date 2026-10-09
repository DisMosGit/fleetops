# Proposal

## Why

A canary that can fail without being able to undo itself is only half a safety mechanism. Today the
rollout workflow *decides* to roll back and stops there: `rollback()` records the failing wave's
decision and moves the rollout to its terminal status. Nothing puts the devices that already took
the bad firmware back on the version they ran before, nothing repairs the fleet's record of which
device runs which firmware, and nothing outside the workflow learns that a rollback happened — an
operator watching Grafana or the broker sees the health dip and nothing after it. The rollout
proposal and the control-surface proposal both deferred exactly this work as "the saga, Phase 8";
this change is that stage.

## What Changes

- **Entering rollback derives an ordered plan of compensations and runs it.** The plan is built from
  the rollout's own recorded progress, walking it in reverse: a notification announcing the
  rollback, one downgrade step per wave the rollout dispatched — most recently dispatched wave
  first, so `100%` is compensated before `25%` — each paired with the forward step it compensates,
  then the inventory reconciliation that compensates the fleet's bookkeeping, then the completion
  notification. The plan is a pure function of recorded state, so a replay derives the same plan, and
  it is published through the state query as it progresses.
- **Every device that took the firmware is downgraded to the version it ran before.** A new
  `downgrade-device` activity restores one device: the device entity gains the firmware version it
  ran before the current one (carried state, exposed through the state query), the activity resolves
  that version in the firmware registry, and commands the restore through the same device command
  seam a wave uses — under a command id derived from the rollback and the device, which the device
  deduplicates the same way it deduplicates a wave's command — reporting `restored`, `failed`,
  `unreported`, `skipped` (nothing to restore), or `unavailable` (the restore could not be attempted,
  with why). No proto change and no agent change: a downgrade is an ordinary update command for an
  older version, which is what the frozen `agent.proto` already says it is.
- **The fleet's firmware inventory is reconciled against the devices' authoritative state.** A new
  `reconcile-device-inventory` activity verifies each device the rollback touched: it reads the
  version the device workflow — the authority — holds, compares it with the `devices` document's
  `current_fw`, and corrects the record where it disagrees, reporting `agreed`, `corrected`, or
  `unverified`. The step reports the reconciled inventory (how many devices ended on each version)
  alongside the number of corrections, so after a rollback the bookkeeping matches reality instead of
  waiting for a heartbeat that a downgraded device may never send.
- **A rollback is announced when it starts and when it completes.** Two events are published into the
  broker's events exchange under a new notification key space — `rollout.notification.rollback.started`
  and `rollout.notification.rollback.completed` — carrying the rollout, the firmware, the wave that
  ended it, the outcome, and (on completion) the plan's per-step counts and the reconciled inventory.
  They are published with the broker's confirmation before the step is recorded, under deterministic
  event ids, so a retried publication republishes the same event and a consumer deduplicates it. The
  publisher is a new seam in `internal/telemetry`; the worker hosts it.
- **Every step is idempotent, so a partial rollback cannot corrupt state.** A downgrade command id is
  deterministic per rollback and device and a device already off the rolled-back firmware is skipped; the
  reconciliation writes only where the record disagrees and always copies the device's authoritative
  version; notifications repeat their event id. A step whose work fails permanently is recorded with
  what it could and could not do, and the plan still runs to its end — the reconciliation and the
  completion notification are exactly what make a partial rollback legible — after which the rollout
  records its terminal `rolled_back` status with the saga's outcome. A new non-terminal `rolling_back`
  status makes the compensating phase visible while it runs, and the rollout stops accepting operator
  control once it has begun.

Explicitly **not** in this change: a consumer of the notification events (dashboards and alerts attach
to the declared key space in the observability stage), an agent-side "rolled back" phase report (the
unused `PhaseRolledBack` placeholder stays unused — a downgrade is an update command), rollback HTTP
endpoints or UI, metrics and tracing for the saga, and any change to the health gate's arithmetic or
to the decision rules that *lead* to a rollback.

## Capabilities

### New Capabilities

- `rollback-saga`: the rollback's spine — the ordered plan of compensating steps and what each one
  compensates, its derivation from recorded progress in reverse, the execution and recording of every
  step, the interim `rolling_back` status, the terminal ordering, and the idempotency that makes a
  partially failed rollback safe to retry.
- `device-downgrade`: restoring one device to the firmware it ran before — the downgrade activity, the
  previous firmware the device entity owns, the registry lookup that resolves it, the command identity
  it reuses, the wait for the device's reported result, the five outcomes it reports, and why one
  device's failure never aborts the step.
- `firmware-inventory`: the fleet's record of which device runs which firmware version and the
  reconciliation step that makes it match the devices after a rollback — what is compared against
  what, what is corrected, what is reported, and why a device that cannot be verified is never
  assumed correct.
- `rollback-notification`: announcing a rollback into the broker — the two events, their routing keys
  and content, their deterministic identity, the confirmed publication that precedes recording the
  step complete, and their relationship to the authoritative record.

### Modified Capabilities

- `rollout-workflow`: entering rollback runs the plan before the terminal record, the rollout's
  recorded progress and status vocabulary gain the compensating phase and the rollback record, and
  the state query reports the plan, its per-step outcomes, the reconciled inventory, and the devices
  the rollback could not restore.
- `device-workflow`: the entity's authoritative state gains the firmware version the device ran
  before its current one, adopted whenever its firmware changes and readable through the state query,
  which is what a downgrade restores.
- `firmware-registry`: firmware metadata becomes addressable by version, not only by id, because a
  device's previous firmware is known by version.
- `message-broker-topology`: a third key space — `rollout.notification.<kind>.<phase>` — and a work
  queue bound to it, so a notification is routable and a dashboard or alerting consumer attaches by
  binding alone.
- `mongo-data-model`: the `rollouts` status vocabulary gains the non-terminal `rolling_back`, the
  rollout document carries the rollback record, and the `devices` document's `current_fw` is defined
  as the reconciled copy of the device workflow's authoritative version.
- `temporal-worker`: the worker registers the downgrade and reconciliation activities beside its
  existing rollout activities, and hosts the notification publisher those steps publish through.

## Impact

- **Code**: `internal/temporal` (the rollback plan, its steps and their recording, the state and view,
  the `downgrade-device` and `reconcile-device-inventory` activities, the notification activity, the
  device entity's previous firmware, and the registry lookup seam), `internal/rollout` (the rollback
  record the rollout document carries), `internal/devices` (the reconciliation write on the `devices`
  collection), `internal/firmware` (the version-keyed metadata read), `internal/telemetry` (the
  notification event, its routing key, and a publisher that waits for the broker's confirmation),
  `cmd/worker` (registration and the publisher's lifecycle), `deploy/config.yaml` is unchanged,
  `deploy/mongo/init.js` + `verify.sh`, `deploy/README.md`, `cmd/README.md`, `docs/telemetry.md`.
- **Dependencies**: none — the Temporal SDK, the Mongo driver, the AMQP client, and the existing
  packages cover this.
- **Contracts**: no proto change. The frozen `fleetops.agent.v1` and `fleetops.rollout.v1` contracts
  are untouched: a downgrade is a `StartUpdate` for an older version, which the contract already
  describes. Two broker routing keys and one queue are added to the declared layout.
- **Storage**: no new collection and no new index. `rollouts` accepts one more status value and one
  more sub-document; `devices` documents gain a third writer of `current_fw` (the reconciliation),
  which writes only when the record disagrees with the device.
- **Behaviour**: a rollout that regresses now takes as long as its compensations do before it records
  its terminal status — bounded by the configured `rollout.result_timeout` per restored device and by
  the activities' retry policies — and its status is `rolling_back` while they run. A compensation
  that cannot restore a device does not fail the rollout: the device is recorded as unrestored and the
  plan continues.
- **Compatibility**: `RolloutWorkflow` changes in place and `DeviceWorkflow`'s carried state gains a
  field (carry version 5), so in-flight rollout runs and device run chains must be drained or
  terminated before the new worker build polls — the same local-stack migration the earlier workflow
  changes documented (see design — Migration Plan, which also records why no `workflow.GetVersion`
  branch is kept).
