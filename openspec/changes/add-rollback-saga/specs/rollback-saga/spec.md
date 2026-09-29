# Spec Delta

## Purpose

Owns the rollback of a regressing rollout: it derives, from the rollout's own recorded progress, the
ordered plan of compensations that undoes what the rollout did — the devices it moved onto a firmware,
the fleet bookkeeping it advanced, and the announcement of what happened — runs that plan, records
what every step achieved, and concludes the rollout once the plan has run.

## ADDED Requirements

### Requirement: Entering rollback derives an ordered plan of compensations

When a rollout's wave fails — a decided unhealthy verdict, an undecided wave past its decision
timeout, or a wave whose update commands could not be delivered — the rollout SHALL derive a rollback
plan before any compensation runs, and SHALL derive it from the rollout's own recorded progress
rather than from a fresh reading of the fleet, so that a replay of the run derives the same plan and
a retry or a worker restart neither extends, reorders, nor re-derives it.

The plan SHALL be an ordered list of steps in which every compensating step names the forward step it
compensates, derived by walking the rollout's forward progress in reverse:

1. a step announcing the rollback into the broker;
2. one downgrade step per wave the rollout dispatched, ordered so that the most recently dispatched
   wave is compensated first, each compensating that wave's firmware updates and naming that wave;
3. a step reconciling the fleet's firmware inventory, compensating the bookkeeping the rollout
   advanced;
4. a step announcing the rollback's completion.

A wave the rollout never dispatched — a pending wave, or a share that resolved to no device — SHALL
contribute no step. A rollout that could not start, because its firmware is unknown or does not target
the selector's model, SHALL have no plan and SHALL run no compensation: nothing was dispatched, so
there is nothing to compensate and no rollback to announce.

#### Scenario: The plan compensates in reverse order

- **WHEN** a rollout rolls back on its third wave after promoting its first two
- **THEN** its plan downgrades the third wave's devices before the second wave's, the second wave's
  before the first wave's, and reconciles the inventory after all of them

#### Scenario: A replay derives the same plan

- **WHEN** a run that has derived a rollback plan is replayed from its history
- **THEN** the plan it derives is identical, step for step, and no step is executed twice

#### Scenario: A wave that was never dispatched contributes no step

- **WHEN** a rollout whose sequence contains a wave that targeted no device, or a wave it never
  reached, rolls back on an earlier wave
- **THEN** its plan contains no downgrade step for that wave

#### Scenario: A rollout that could not start runs no compensation

- **WHEN** a rollout concludes as failed because its firmware is unknown or does not target the
  selector's model
- **THEN** no compensation step runs and no rollback notification is published

### Requirement: The plan runs in order and every step is recorded

The rollout SHALL execute the plan's steps in plan order and SHALL record each step's outcome as it
happens, in the fleet database and in the state its query reports: the step's kind, the wave it
compensates where it compensates one, and what it achieved — for a downgrade step, how many of its
devices were restored, failed to restore, never reported, had nothing to restore, or could not be
restored at all; for the reconciliation, how many device records agreed, were corrected, and could
not be verified. The recording SHALL be idempotent: re-recording an outcome already recorded SHALL
leave the stored record unchanged rather than duplicating or regressing it.

A step whose work fails permanently SHALL be recorded with what it did and did not achieve and SHALL
NOT abort the plan: the remaining steps SHALL still run, because the reconciliation and the completion
announcement are what make a partial rollback legible and complete the fleet's picture. The rollout
SHALL NOT record its terminal status until every step of the plan has been executed or found
permanently failed.

#### Scenario: Steps run and are recorded in order

- **WHEN** a rollback's plan runs
- **THEN** each step's outcome is recorded before the next step starts, and the recorded order is the
  plan's order

#### Scenario: A permanently failed step does not stop the plan

- **WHEN** a downgrade step cannot restore some of its devices, and those failures outlive the
  step's retries
- **THEN** the step is recorded with those devices unrestored, and the remaining downgrade steps, the
  reconciliation, and the completion announcement still run

#### Scenario: The terminal record comes last

- **WHEN** a rollout's plan has not finished
- **THEN** the rollout is not recorded as rolled back, and its terminal record is written only once
  every step has been executed or found permanently failed

#### Scenario: A re-recorded step converges

- **WHEN** a recorded step's outcome is recorded again
- **THEN** the stored rollback record is unchanged

### Requirement: A compensating rollout reports its plan and its progress

From the moment it enters rollback until its plan has finished, the rollout SHALL report the
non-terminal status `rolling_back`, and its state query SHALL report, without blocking: the plan with
each step's kind, the wave it compensates, its status, and its counts; the reconciled inventory, as
the number of devices it accounts for on each firmware version; and the devices the rollback could
not restore. Once the plan has run, the rollout SHALL record the terminal status `rolled_back` with
the outcome that ended it and SHALL conclude in exactly one terminal state as before.

Operator control SHALL change nothing once a rollout has entered rollback: a pause, a resume, or an
approval delivered then SHALL leave the rollout's state and its recorded progress unchanged, because
a rollout that has begun compensating has already stopped deciding and its compensations are not
interruptible.

#### Scenario: The compensating phase is reported

- **WHEN** the state query is evaluated while a rollback's second downgrade step runs
- **THEN** it reports status `rolling_back`, the first step's recorded outcome, the second step as the
  one in progress, and the wave each step compensates

#### Scenario: A concluded rollback reports what it achieved

- **WHEN** the state query is evaluated for a rollout whose plan has finished
- **THEN** it reports the terminal status `rolled_back`, the outcome that ended it, the reconciled
  inventory, and the devices that could not be restored

#### Scenario: The inventory accounts for every device the rollback touched

- **WHEN** the reconciliation step has run
- **THEN** every device the rollback touched is reported either on the firmware version it was found
  to run or as a device whose record could not be verified

#### Scenario: Control signals do nothing during a rollback

- **WHEN** a pause, a resume, or an approval is delivered after a rollout has entered rollback
- **THEN** the rollout's reported status and recorded progress are unchanged and no step is held

### Requirement: Every rollback step is safe to retry

Every step of the plan SHALL be idempotent, so a partially applied rollback can be retried without
corrupting state: a downgrade step SHALL address each of its devices under a command id derived from
the rollback and that device, so a redelivered or retried restore repeats the same id and is a no-op
for a device that already accepted it, and SHALL leave a device that does not run the firmware being
rolled back untouched; the reconciliation SHALL read each device's authoritative version, write only
where the recorded version disagrees, and never write a version the device did not report; and each
announcement SHALL carry an event identity derived from the rollout and the phase, so a republished
announcement is the same event rather than a second one.

No step SHALL record an outcome it did not observe: a device that never reported its restore SHALL be
recorded as unreported rather than restored, a device whose restore could not be attempted SHALL be
recorded as such with the reason, and a device record that could not be verified SHALL be reported as
unverified rather than assumed correct.

#### Scenario: A retried restore repeats its command id

- **WHEN** a downgrade step is retried after its device already accepted the restore command
- **THEN** the device receives the same command id and its state is unchanged

#### Scenario: Re-running a finished plan changes nothing

- **WHEN** a rollout whose plan has completed is made to run its steps again
- **THEN** every device is either still on the version the plan restored or restored again under the
  same command id, no device is commanded with a new id, and the recorded rollback does not regress

#### Scenario: A retried reconciliation writes nothing

- **WHEN** the reconciliation runs again for a device whose recorded version already matches the
  device's authoritative version
- **THEN** the record is unchanged and the device is reported as having agreed

#### Scenario: A republished announcement is the same event

- **WHEN** an announcement step is retried after a publication the broker already accepted
- **THEN** the event carries the same event identity as the first publication

#### Scenario: Nothing is recorded as achieved that was not observed

- **WHEN** a device's restore is never reported before the wait ends
- **THEN** that device is recorded as unreported and is counted among the devices the rollback did not
  restore, never among the restored ones
