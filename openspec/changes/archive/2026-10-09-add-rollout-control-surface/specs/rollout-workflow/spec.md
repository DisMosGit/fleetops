# Spec Delta

## MODIFIED Requirements

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
status (running, paused, awaiting approval, rolled back, completed, failed) and each wave document's
status, target set, start time, evaluated success rate, and — once its update outcomes have been
collected — the devices that failed and the devices that never reported. Every write SHALL be
idempotent — a retried write for a transition already recorded SHALL leave the stored record
unchanged rather than duplicating or regressing it — so the recorded picture is the workflow's
state, not an artifact of when an activity happened to run.

#### Scenario: Status transitions are recorded

- **WHEN** a rollout starts, waits for approval, is paused, is resumed, rolls back, completes, or
  fails
- **THEN** the rollout document's status reflects the state the rollout is in

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
never reported, which wave is current, whether an operator approval is outstanding, and — once the
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

#### Scenario: A concluded rollout reports why

- **WHEN** the state query is evaluated for a rolled-back rollout
- **THEN** it reports the terminal outcome and the wave whose health ended the rollout
