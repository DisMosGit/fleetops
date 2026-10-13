# Spec Delta

## Purpose

Makes the questions an operator asks during a canary answerable from metrics: how far a rollout has
progressed, whether its current wave is healthy, how much of the fleet is online, how fast
heartbeats are arriving, and whether anything is rolling back.

## ADDED Requirements

### Requirement: Rollout progress is observable while it runs

The system SHALL expose the number of rollouts in each non-terminal and terminal status, and for
each active rollout its progress through the configured wave sequence — the waves concluded and the
share of the target group the rollout has reached. Progress SHALL be derived from the rollout's
recorded state rather than from an independently maintained counter, so a restarted worker does not
reset or double-count it.

#### Scenario: Active rollouts are counted by status
- **WHEN** rollouts are running, paused, and awaiting approval at the same time
- **THEN** each status's gauge reflects how many rollouts currently hold it

#### Scenario: Progress advances as waves conclude
- **WHEN** a rollout promotes a wave and moves to the next
- **THEN** its progress reflects the concluded wave and the reached share of the target group

#### Scenario: A concluded rollout leaves the active measure
- **WHEN** a rollout completes, fails, or is rolled back
- **THEN** it no longer counts as active and its terminal status is counted instead

#### Scenario: A restarted worker does not reset progress
- **WHEN** the worker restarts while rollouts are in flight
- **THEN** the metrics it exposes on the next scrape still reflect those rollouts' true progress

### Requirement: Wave health is observable

The system SHALL expose, per wave, the measured success ratio, the sample size the measurement
rested on, and the wave's status, so an operator can tell a wave that is healthy on thin evidence
from one that is healthy on real evidence, and can see a wave that is still undecided.

#### Scenario: A measured wave reports its ratio and evidence
- **WHEN** a wave's health window is evaluated
- **THEN** its success ratio and the number of samples behind it are both exposed

#### Scenario: An undecided wave is distinguishable from an unhealthy one
- **WHEN** a wave holds fewer samples than the configured minimum
- **THEN** its undecided state is visible and it is not reported as a failing ratio

#### Scenario: A regressing wave is visible before it is acted on
- **WHEN** a wave's measured success ratio falls below the configured boundary
- **THEN** the drop is exposed as the gate decides, so the dashboards show the regression and the rollback that follows

#### Scenario: Wave metrics do not outlive their wave
- **WHEN** a rollout reaches a terminal status
- **THEN** its waves' series stop being reported as current rather than lingering as if the wave were still being evaluated

### Requirement: Fleet state is observable

The system SHALL expose the ratio of online to known devices, computed from the liveness state the
system already maintains, and SHALL expose heartbeat throughput — heartbeats accepted per unit time
— so a fleet that is going silent is distinguishable from one whose telemetry pipeline has stalled.

#### Scenario: Online ratio reflects liveness
- **WHEN** devices cross the configured offline threshold
- **THEN** the online ratio falls to reflect the devices that are no longer live

#### Scenario: The ratio is bounded and defined when empty
- **WHEN** no devices are known
- **THEN** the ratio is reported only as a defined value rather than as a division by zero or a missing series

#### Scenario: Heartbeat throughput is measured where heartbeats are accepted
- **WHEN** heartbeats are accepted by ingestion
- **THEN** the throughput metric rises with them, separately from the broker publication path, so a broker failure does not read as a silent fleet

#### Scenario: Throughput falls when the fleet goes quiet
- **WHEN** the devices stop reporting
- **THEN** throughput falls toward zero rather than holding its last rate

### Requirement: Rollbacks are observable

The system SHALL expose a counter of rollbacks by the outcome that caused them, and per rollback
the progress of its compensation plan — steps completed against steps planned — so an operator can
see both how often rollbacks happen and why, and whether one is currently stuck midway.

#### Scenario: Rollbacks are counted by cause
- **WHEN** a rollout rolls back because a wave failed its gate
- **THEN** the rollback counter for that cause increases and the counters for other causes do not

#### Scenario: A rollback's progress is visible
- **WHEN** a rollback has completed some compensations and not others
- **THEN** its completed and planned step counts are both exposed

#### Scenario: A stuck compensation is distinguishable from a slow one
- **WHEN** a compensation takes longer than its usual duration
- **THEN** the step's duration is observable without the plan appearing to have completed

#### Scenario: Unrestorable devices are visible
- **WHEN** a rollback concludes with devices it could not restore
- **THEN** that count is exposed rather than being visible only in MongoDB

### Requirement: Rollout metrics have real producers and never fabricate values

Every metric added by this capability SHALL be fed by the component that owns the underlying fact —
the workflow for rollout and wave state, the liveness sweep for device liveness, ingestion for
heartbeat acceptance, and the rollback's own record for compensations. No metric SHALL report a
plausible value when its source is unavailable: a failed read SHALL leave the series absent or
unchanged rather than publishing a fabricated zero.

#### Scenario: A failed source read does not publish a fabricated value
- **WHEN** the component that owns a metric's source cannot read it
- **THEN** the metric does not report a zero or stale value as if it were current, and the failure is logged

#### Scenario: Every dashboard panel has a real source
- **WHEN** a metric this capability exposes is queried after the work it describes has happened
- **THEN** it returns the value the underlying records support

#### Scenario: Metrics survive a process restart
- **WHEN** the process exposing these metrics restarts
- **THEN** metrics derived from recorded state are re-established from that state rather than staying at zero

### Requirement: Metrics never gate the rollout

Observing a rollout SHALL NOT alter it. A metrics failure SHALL NOT fail a wave, delay a gate
decision, change a rollback's plan, or alter any recorded state.

#### Scenario: A rollout completes while metrics are failing
- **WHEN** metric publication fails throughout a rollout
- **THEN** the rollout reaches the same terminal status and records the same waves and rollback steps

#### Scenario: Metric reads do not slow the gate
- **WHEN** the health gate evaluates a wave while its metrics are being scraped
- **THEN** the gate's decision path does not wait on metric collection
