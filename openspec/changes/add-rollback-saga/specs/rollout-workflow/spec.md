# Spec Delta

## MODIFIED Requirements

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
