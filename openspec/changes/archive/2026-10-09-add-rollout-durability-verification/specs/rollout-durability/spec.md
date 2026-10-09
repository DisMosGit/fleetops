# Spec Delta

## Purpose

Holds the rollout's contract with the worker process that drives it: what survives when that worker
stops and another starts, where the rollout resumes, and why a restart can never apply an effect the
rollout has already applied.

## ADDED Requirements

### Requirement: A worker restart does not disturb a rollout in flight

A rollout SHALL be driven entirely from the state Temporal records, so that a restart of the worker
executing it — the process stops and a fresh worker starts polling the same task queue — resumes it
from exactly the position it had reached: the wave it was dispatching, the health window it was
waiting out, the approval it was awaiting, the pause it was holding, or the rollback step it was
running. A restart SHALL NOT resolve a wave's membership again, SHALL NOT restart, shorten, or
extend a health window, SHALL NOT lose, duplicate, or move an approval, pause, or resume that
arrived while no worker was executing the rollout, SHALL NOT re-dispatch a wave's update commands or
re-run a compensating step that had completed, and SHALL NOT duplicate, skip, or regress a recorded
wave outcome or rollout status. A rollout that had reached a terminal state SHALL stay concluded and
SHALL apply no further effect.

#### Scenario: A restart between two waves resumes with the next wave

- **WHEN** a rollout's worker stops after a wave was promoted and a fresh worker starts polling
- **THEN** the promoted wave's record is unchanged and the rollout resolves and dispatches the next
  wave of its sequence, with no wave resolved or dispatched twice

#### Scenario: A restart inside a health window does not move the window

- **WHEN** the worker stops and restarts while a wave is inside its health window
- **THEN** the wave is judged at the deadline its recorded start and the configured window always
  implied, without restarting, shortening, or extending it, and the wave's recorded start time is
  unchanged

#### Scenario: A restart between the approval and the gated wave

- **WHEN** a rollout awaits approval, its worker stops, an approval arrives while no worker is
  executing the rollout, and a fresh worker starts
- **THEN** the gated wave is resolved and dispatched on that approval, and another wave that
  requires approval still waits for its own

#### Scenario: A restart while paused mid-wave

- **WHEN** a rollout is paused while a wave is in flight, its worker stops, and a fresh worker
  starts
- **THEN** the rollout is still paused, that wave is still driven to its recorded decision, and no
  further wave starts until a resume arrives

#### Scenario: A restart inside the rollback resumes the plan where it stopped

- **WHEN** the worker stops between two steps of a rollback's plan and a fresh worker starts
- **THEN** the plan continues at the step it had reached, no completed step is compensated again,
  and the order of the steps that remain is unchanged

#### Scenario: A concluded rollout stays concluded

- **WHEN** a worker restarts after a rollout has completed, rolled back, or failed
- **THEN** the rollout records no further transition and applies no further effect

### Requirement: A restart never applies one effect twice

An activity whose result the rollout recorded SHALL NOT be executed again by the worker that
resumes the rollout. An activity whose attempt ends before its result is recorded — because the
worker stopped, or because the attempt failed after its side effect had already landed — SHALL be
retried, and the retry SHALL converge on the effect the first attempt intended rather than add a
second one. Every effect SHALL therefore carry an identity derived from the rollout's own recorded
state — the update command id a wave and device derive, the restore command id a rollback and device
derive, the event id a rollout and notification phase derive — and every write SHALL be idempotent,
so that after any number of restarts, including one between and one inside every activity, each
targeted device was commanded for its wave under exactly one update command id, each compensated
device was restored under exactly one restore command id, each compensated device's version was
reconciled against that device, and each rollback phase was announced under exactly one event id,
so a redelivered announcement repeats that id rather than minting a second event.

#### Scenario: A completed activity is not executed again

- **WHEN** a restart happens after an activity's result has been recorded
- **THEN** the resuming worker executes the rollout's remaining work and never executes the recorded
  activity again

#### Scenario: An effect applied before a failed attempt is not applied twice

- **WHEN** an activity's attempt applies its side effect and then ends without a result being
  recorded, and its retry runs on a fresh worker
- **THEN** the retry repeats the same identity and the effect is applied once in total

#### Scenario: The effect ledger stays exactly-once across the restart matrix

- **WHEN** a rollout — dispatch, health windows, and rollback included — runs with the worker
  restarted between every pair of its activities
- **THEN** every targeted device was commanded under exactly one update command id per wave, every
  compensated device was restored under exactly one restore command id, every compensated device's
  recorded version was reconciled against that device, and each rollback phase was announced under
  exactly one event id, so every copy of an announcement on the broker carries the id of the phase
  it announces and no phase is announced under two ids

#### Scenario: The recorded state converges instead of accumulating

- **WHEN** the fleet database is read after such a run
- **THEN** the rollout document, its wave documents, the devices' recorded firmware versions, and
  the reconciliation's counts each describe one run: no duplicated wave, no counter that grew with
  the number of restarts, and no device whose recorded version disagrees with the version its
  workflow holds
