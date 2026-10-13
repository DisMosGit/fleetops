# Spec Delta

## Purpose

Makes a rollout's full timeline one trace: the workflow that drives it, every activity it
schedules, and the saga compensations it runs on regression, with the workflow owning the trace
context its activities inherit.

## ADDED Requirements

### Requirement: The workflow owns the trace of the work it drives

A rollout workflow SHALL create a span that covers the execution of the workflow itself, and the
spans of the activities it schedules SHALL be children of the workflow's context rather than
starting unrelated traces. One rollout therefore has one trace covering its whole timeline, from
its first wave to its terminal status.

#### Scenario: One rollout is one trace
- **WHEN** a rollout runs through its whole wave sequence to completion
- **THEN** its wave dispatch, health evaluation, and status writes all appear in one trace, rooted at the workflow's span

#### Scenario: A rollout's span outlives its individual waves
- **WHEN** a rollout advances from one wave to the next
- **THEN** the second wave's activity spans belong to the same trace as the first wave's, not to a new one

#### Scenario: Two rollouts are separate traces
- **WHEN** two rollouts run concurrently
- **THEN** each has its own trace, and neither's activities are children of the other's workflow span

### Requirement: Activity execution is traced as one step of the workflow

Every activity SHALL be traced, carrying the activity's type and its outcome, and a retried
activity SHALL be visible as a retry of one step rather than as an independent execution that
happened to fail. An activity that exhausts its retries SHALL record its failure on its own span
and in the workflow's trace.

#### Scenario: An activity is a child of the workflow
- **WHEN** the rollout workflow schedules a wave dispatch activity
- **THEN** that activity's span names the workflow's span as its parent

#### Scenario: A retry is visible as a retry
- **WHEN** an activity fails once and succeeds on its next attempt
- **THEN** both attempts are visible in the rollout's trace, attributable to the same step

#### Scenario: Permanent activity failure is recorded
- **WHEN** an activity exhausts its retry policy
- **THEN** its span records the failure and the workflow's trace shows which step could not complete

### Requirement: Domain identifiers are attached to spans

Spans created for workflow and activity execution SHALL carry the domain identifiers of the work
they cover — `rollout_id`, `wave_id`, `device_id`, and `firmware_id` where the work concerns them —
so a trace can be found by the entity an operator already knows.

#### Scenario: A rollout's spans carry its identifiers
- **WHEN** a rollout's activities are traced
- **THEN** their spans carry the rollout id, and a wave-specific activity also carries the wave id

#### Scenario: A device's work carries the device id
- **WHEN** the per-device update activity runs for one device
- **THEN** its span carries that device's id, so the trace can be filtered by device

### Requirement: Workflow determinism is preserved

Attaching trace context to workflow and activity execution SHALL NOT make workflow code
non-deterministic. Workflow code SHALL NOT read wall-clock time, generate randomness, or perform
I/O to produce trace data, and a replay of a workflow's history SHALL produce the same decisions it
produced originally.

#### Scenario: Traced workflow replays cleanly
- **WHEN** a traced rollout workflow's completed history is replayed
- **THEN** the replay completes without a determinism error and reaches the same decisions

#### Scenario: Tracing adds no non-deterministic reads
- **WHEN** the workflow and its tracing helper are inspected
- **THEN** no wall-clock time, randomness, or direct I/O is read in workflow code to produce trace data

### Requirement: The saga's compensations belong to the rollout's trace

When a rollout rolls back, the compensating activities SHALL be traced as children of the same
workflow trace, each identifiable by the step it compensates, so the whole regression — downgrade,
inventory reconciliation, and notification — reads as one timeline with the failure that caused it.

#### Scenario: Compensation spans join the rollout's trace
- **WHEN** a wave fails its gate and the rollout runs its compensations
- **THEN** the compensating activities' spans are in the same trace as the wave that failed

#### Scenario: The failing wave is identifiable in the trace
- **WHEN** an operator opens the trace of a rolled-back rollout
- **THEN** the trace shows which wave failed and which compensations followed it

### Requirement: Tracing never changes what the workflow does

Trace propagation SHALL NOT alter scheduling, retry policy, timers, signals, or recorded state. A
workflow SHALL reach the same terminal status and write the same records whether tracing is enabled
or disabled.

#### Scenario: Same outcome with tracing off
- **WHEN** the same rollout runs with tracing disabled
- **THEN** it reaches the same terminal status and records the same waves, success rates, and rollback steps

#### Scenario: A collector outage does not fail a rollout
- **WHEN** the trace collector is unreachable while a rollout runs
- **THEN** the rollout proceeds to its normal conclusion and the export failure does not fail any activity
