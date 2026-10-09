# rollout-workflow Specification

## Purpose
Owns one canary rollout from start to finish: it drives the configured wave sequence, resolves and
records what each wave targets, dispatches the firmware update to those devices, records the
rollout's progress and terminal outcome, and answers a state query about all of it.

## Requirements

### Requirement: One rollout workflow owns a rollout from start to finish

Each rollout SHALL be driven by exactly one workflow execution, addressed by the stable workflow
identity `rollout-<rollout_id>`, which owns the rollout's whole life: the wave sequence position,
the devices each wave targeted, the gate outcome of each wave, and the rollout's terminal state.
The workflow's input SHALL be the firmware to deploy (a firmware id) and the target selector (a
region and a device model); both are fixed for the life of the rollout and SHALL NOT be changed by
a signal. A rollout SHALL conclude in exactly one terminal state — completed (the configured
sequence ran to its end with every wave promoted), rolled back (a wave failed its gate), or failed
(the rollout could not start) — and SHALL NOT dispatch anything after reaching one.

#### Scenario: Starting a rollout records it as running

- **WHEN** a rollout workflow starts for a firmware id and a region-plus-model selector
- **THEN** exactly one rollout document exists for that rollout id, carrying the firmware id, the
  selector, the id of the driving workflow execution, and a status of running

#### Scenario: One workflow drives a rollout

- **WHEN** a rollout is started for a rollout id that already has a workflow execution
- **THEN** no second workflow execution drives that rollout and the existing execution's state is
  authoritative

#### Scenario: The selector does not move under a running rollout

- **WHEN** devices matching the selector register, re-register, or change model while the rollout
  runs
- **THEN** the rollout's waves still target the membership resolved when each wave started

#### Scenario: A concluded rollout is terminal

- **WHEN** a rollout has reached completed, rolled back, or failed
- **THEN** it dispatches no further commands and its recorded status does not change again

### Requirement: The rollout drives the configured wave sequence in order

The rollout SHALL drive the waves of the configured canary sequence in sequence order, starting one
wave only after the previous wave has been promoted by its gate, and SHALL hold at most one wave in
flight at a time. The rollout SHALL reach its completed state only after the last wave of the
sequence has been promoted; a sequence that is exhausted without a failed wave is a completed
rollout.

#### Scenario: Waves run in sequence order

- **WHEN** a rollout with the sequence 1%, 5%, 25%, 100% runs to completion
- **THEN** its waves start in that order, each after the previous one was promoted

#### Scenario: Only one wave is in flight

- **WHEN** a wave is being dispatched or is inside its health window
- **THEN** the next wave of the sequence has not been resolved or dispatched

#### Scenario: Exhausting the sequence completes the rollout

- **WHEN** the last wave of the configured sequence is promoted
- **THEN** the rollout is recorded as completed and dispatches nothing further

### Requirement: A wave resolves and records its target membership before dispatch

Each wave SHALL target the devices its configured percentage adds to the shares the rollout's
earlier waves already targeted: the eligible pool is the devices matching the rollout's target
selector, the percentage is a share of the whole pool, and a wave's target set is the difference
between its own share and the shares of the waves before it. The wave's target set SHALL be
resolved once, before any command is dispatched, recorded on the wave document together with the
time the wave started, and SHALL NOT be re-derived later — not by re-reading the pool, and not by a
retry of the resolution itself, which SHALL converge on the recorded membership. Wave target sets
of one rollout SHALL be disjoint, and their union SHALL be the pool's first through the last wave's
share.

#### Scenario: Each wave targets its own added share

- **WHEN** a rollout whose selector matches 1000 devices runs the sequence 1%, 5%, 25%, 100%
- **THEN** the first wave targets 10 devices, the second 40, the third 200, the fourth 750, and no
  device appears in more than one wave

#### Scenario: Membership is recorded before anything is dispatched

- **WHEN** a wave starts
- **THEN** a wave document exists carrying its rollout id, percentage, target device set, and start
  time before any update command for that wave is delivered

#### Scenario: A retried resolution converges

- **WHEN** the resolution of a wave's membership is retried
- **THEN** the wave's recorded target set and start time are unchanged

#### Scenario: A share that resolves to no device is recorded and skipped

- **WHEN** a wave's percentage adds no device to the shares already targeted (for example 1% of a
  pool of ten devices)
- **THEN** the wave is recorded with an empty target set, is not dispatched and is not gated on
  health, and the rollout advances to the next wave

### Requirement: Waves dispatch the firmware update through the device command seam

For every device a wave targets, the rollout SHALL deliver an update command carrying the
firmware's id, version, and checksum to that device's workflow through the device command seam,
under a command id determined by the rollout, the wave, and the device, so that a redelivered
command repeats the same id and is a no-op for the device. Delivery SHALL be one activity per
target device, and each such activity SHALL also wait for that device's reported result within the
configured result timeout, so a wave's dispatch is complete only once every device it targets has
settled: reported success, reported failure, or run out of time without reporting. The wave SHALL
record which of its devices failed and which never reported; a device that failed or never reported
SHALL NOT by itself decide the wave, whose promotion stays the health gate's verdict. The rollout
SHALL dispatch nothing until it has resolved the firmware's metadata and confirmed that the
selector's model is one the firmware targets; a firmware that is unknown or does not target the
selector's model SHALL fail the rollout before any command is dispatched. A wave whose update
commands cannot be delivered to its target devices SHALL be recorded as failed and the rollout SHALL
transition into rollback instead of advancing.

#### Scenario: Every target device receives its command

- **WHEN** a wave targeting ten devices is dispatched
- **THEN** each of those devices' workflows receives an update command naming that wave's firmware
  id, version, and checksum

#### Scenario: The wave waits for its devices' results

- **WHEN** a wave has delivered its commands and some of its devices have not yet reported
- **THEN** the wave is not complete and is not gated until every device has settled

#### Scenario: A device that never reports stops the wait

- **WHEN** a device has not reported its result once the configured result timeout has passed since
  the wave's dispatch
- **THEN** that device counts as unreported, the wave's dispatch completes, and its document records
  the device as unreported

#### Scenario: Mixed device outcomes are recorded

- **WHEN** a wave's devices conclude with a mixture of successes and failures
- **THEN** the wave document records the failed and unreported devices and the wave is still judged
  by its health gate

#### Scenario: Redelivery repeats the command id

- **WHEN** the delivery of a wave's update command to a device is retried
- **THEN** the device receives the same command id it received before, and a device that already
  accepted the command is unaffected

#### Scenario: Incompatible firmware fails the rollout at start

- **WHEN** a rollout names a firmware whose target models do not include the selector's model, or
  names a firmware that does not exist
- **THEN** the rollout is recorded as failed and no device receives an update command

#### Scenario: A wave that cannot be delivered is not promoted

- **WHEN** a wave's update commands cannot be delivered to its target devices after the delivery
  attempts are exhausted
- **THEN** the wave is recorded as failed and the rollout transitions into rollback instead of
  advancing

### Requirement: Rollout and wave progress is recorded as it happens

The rollout SHALL record each meaningful transition in the fleet database: the rollout document's
status (one of `running`, `paused`, `awaiting_approval`, `rolling_back`, `rolled_back`, `completed`,
or `failed`) and each wave document's status, target set, start time, evaluated success rate, and —
once its update outcomes have been collected — the devices that failed and the devices that never
reported. A rollout that has entered rollback SHALL additionally record the rollback as it
compensates: the outcome that ended the rollout, the plan's steps with the wave each compensating
step compensates and what each step achieved, the inventory the reconciliation established with the
corrections it made, and the devices the rollback could not restore. `rolling_back` is not a terminal status: the terminal status
`rolled_back` SHALL be recorded only once the plan has run. Every write SHALL be idempotent — a
retried write for a transition already recorded SHALL leave the stored record unchanged rather than
duplicating or regressing it — so the recorded picture is the workflow's state, not an artifact of
when an activity happened to run.

#### Scenario: Status transitions are recorded

- **WHEN** a rollout starts, waits for approval, is paused, is resumed, enters rollback, rolls back,
  completes, or fails
- **THEN** the rollout document's status reflects the state the rollout is in

#### Scenario: A compensating rollout is recorded while it compensates

- **WHEN** a rollout is running its rollback steps
- **THEN** its document carries the status `rolling_back` and the outcome that ended the rollout, and
  its terminal status is not yet recorded

#### Scenario: A concluded rollback carries what it achieved

- **WHEN** a rollout's rollback plan has run
- **THEN** its document carries the terminal status `rolled_back`, each step's outcome, the reconciled
  inventory with its correction count, and the devices that could not be restored

#### Scenario: A decided wave carries its measured health

- **WHEN** a wave's health has been decided
- **THEN** its wave document carries a success rate equal to the evaluated success ratio

#### Scenario: A wave carries its device outcomes

- **WHEN** every device of a wave has settled
- **THEN** its wave document names the devices that reported a failed update and the devices that
  never reported, each set empty when there were none

#### Scenario: A retried write does not regress the record

- **WHEN** a recorded transition is written again
- **THEN** the stored rollout and wave documents are unchanged

### Requirement: A rollout answers a state query

The rollout workflow SHALL serve a state query that reports, without blocking the rollout: the
rollout id, its current status (including whether it is paused), the firmware it deploys with the
version once its metadata has been loaded, its target selector, the configured wave sequence with
each wave's percentage, status, success rate, target count, and how many of its devices failed or
never reported, which wave is current, whether an operator approval is outstanding, — once it has
entered rollback — the rollback plan with each step's kind, the wave it compensates, its status and
its counts, the reconciled inventory, and the devices the rollback could not restore, and — once the
rollout has concluded — its terminal outcome with the wave that ended it.

#### Scenario: A running rollout reports its position

- **WHEN** the state query is evaluated while the second wave of a rollout is inside its health
  window
- **THEN** it reports status running, the first wave's recorded outcome, and the second wave as the
  current one

#### Scenario: A wave's device outcomes are reported

- **WHEN** the state query is evaluated after a wave's devices have settled with failures and
  unreported devices among them
- **THEN** that wave reports how many of its devices failed and how many never reported

#### Scenario: A waiting rollout reports what it waits for

- **WHEN** the state query is evaluated while the rollout waits for an operator approval
- **THEN** it reports status awaiting approval, the wave that may not start yet, and that no
  approval is outstanding

#### Scenario: A paused rollout reports the pause

- **WHEN** the state query is evaluated while the rollout is paused
- **THEN** it reports status paused and the wave it will start when it is resumed

#### Scenario: A compensating rollout answers without waiting for its compensations

- **WHEN** the state query is evaluated while a rollback's steps are still running
- **THEN** it answers immediately with the status `rolling_back`, the plan with each step's recorded
  or pending state, and the reconciled inventory as far as it has been established

#### Scenario: A concluded rollout reports why

- **WHEN** the state query is evaluated for a rolled-back rollout
- **THEN** it reports the terminal outcome and the wave whose health ended the rollout
