# Spec Delta

## Purpose

Decides whether a canary wave may be promoted: it holds the wave open for its durable health
window, judges the wave's measured health against the configured boundary, transitions a regressing
rollout into rollback, and holds a wave that requires human review until an operator approves it.

## ADDED Requirements

### Requirement: A wave is held open by a durable health-window timer

Every dispatched wave SHALL be held open for the configured health window, measured from the time
the wave started as recorded on the wave, before its health is judged. The wait SHALL be a durable
timer on the workflow clock — never a wall-clock reading, a sleep, or a poll — so that it survives
worker restarts: a restart inside the window SHALL resume the remainder of the window, SHALL NOT
restart it, and SHALL NOT shorten it; a wave whose window has already elapsed SHALL not be waited
on again.

#### Scenario: The window opens at the wave's start

- **WHEN** a wave starts and is held for a five-minute health window
- **THEN** its health is not judged before five minutes have passed from the recorded wave start

#### Scenario: A worker restart mid-window resumes the remainder

- **WHEN** the worker executing a rollout stops two minutes into a wave's five-minute window and
  another worker resumes the workflow
- **THEN** the wave is judged three minutes later, not five, and not immediately

#### Scenario: An elapsed window is not re-waited

- **WHEN** a rollout resumes after a worker outage that outlasted the wave's health window
- **THEN** the gate proceeds to judge the wave's health without waiting another window

### Requirement: A wave's health is judged over its recorded membership

The gate SHALL evaluate a wave's health through the fleet's wave-health aggregation, over the
devices and the start time recorded on the wave document, at the moment the workflow decides the
evaluation happens — never by re-resolving the target group and never by reading the wall clock
inside the workflow. The evaluated success ratio, sample size, effective window, and verdict SHALL
be what the gate decides on, and the verdict and ratio SHALL be recorded on the wave when the wave
is decided. An evaluation that fails SHALL be retried and SHALL NOT itself decide a wave: only a
returned verdict does.

#### Scenario: Only the wave's recorded devices are counted

- **WHEN** the gate evaluates a wave whose document records ten devices
- **THEN** the evaluation covers exactly those ten devices over a window no earlier than the wave's
  recorded start

#### Scenario: The evaluation moment is the workflow's decision time

- **WHEN** the gate evaluates a wave
- **THEN** the evaluation's end is the workflow's clock reading for that decision, so a replay of
  the workflow evaluates the same window

#### Scenario: A failed evaluation is retried rather than treated as a regression

- **WHEN** an evaluation attempt fails transiently
- **THEN** the gate retries it and the wave is not rolled back on the failure

### Requirement: The gate promotes a healthy wave and rolls back an unhealthy one

On a decided verdict the gate SHALL either promote the wave — a healthy verdict records the wave as
healthy and lets the rollout advance to the next wave — or fail it: an unhealthy verdict records
the wave as unhealthy and transitions the rollout into rollback. A verdict of undecided SHALL keep
the gate measuring: it SHALL wait another health window, re-evaluate with its window slid forward,
and repeat until the wave is decided or until the wave has been undecided for the configured
decision timeout measured from the wave's recorded start, at which point the wave SHALL be treated
as unhealthy. A wave SHALL never be promoted on anything but a healthy verdict, and a rule exactly
at the configured minimum success ratio is healthy.

#### Scenario: A healthy wave is promoted

- **WHEN** a wave's evaluation returns a healthy verdict
- **THEN** the wave is recorded healthy with its evaluated success ratio and the rollout advances to
  the next wave of the sequence

#### Scenario: Health at the boundary promotes

- **WHEN** a decided wave's success ratio equals the configured minimum success ratio
- **THEN** the wave is healthy and the rollout advances

#### Scenario: An unhealthy wave rolls the rollout back

- **WHEN** a wave's evaluation returns an unhealthy verdict
- **THEN** the wave is recorded unhealthy with its evaluated success ratio and the rollout
  transitions into rollback instead of advancing

#### Scenario: An undecided wave is measured again, not promoted

- **WHEN** a wave's evaluation returns an undecided verdict
- **THEN** no next wave starts, the wave is evaluated again after another health window, and the
  wave is not recorded as healthy

#### Scenario: A wave that never gathers evidence is treated as unhealthy

- **WHEN** a wave is still undecided once the configured decision timeout has passed since its
  recorded start
- **THEN** the wave is recorded unhealthy and the rollout transitions into rollback

### Requirement: A regressing rollout is recorded as rolled back and concludes

When a wave fails — an unhealthy verdict, an undecided wave past the decision timeout, or a wave
whose update commands could not be delivered — the rollout SHALL record the rollback with the wave
that ended it and that wave's decision (its verdict, success ratio, sample size, and effective
window), SHALL start no further wave, and SHALL conclude as rolled back. Waves already promoted
before the failing one SHALL keep their recorded outcomes.

#### Scenario: The rollback names the failing wave

- **WHEN** a rollout's third wave fails its gate
- **THEN** the rollout is recorded as rolled back, the third wave is recorded unhealthy, and the
  rollout's state reports that wave as the one that ended it

#### Scenario: No wave starts after a rollback

- **WHEN** a rollout has transitioned into rollback
- **THEN** no further wave is resolved, dispatched, or gated and no update command is delivered

#### Scenario: Promoted waves keep their outcome

- **WHEN** a rollout rolls back on its third wave after promoting its first two
- **THEN** the first two waves' recorded statuses and success rates are unchanged

### Requirement: A wave that requires approval waits for an operator signal

When the configured sequence entry for a wave requires approval and no approval is outstanding, the
rollout SHALL record that it is awaiting approval, SHALL wait for the operator's `approve_next_wave`
signal before resolving or dispatching that wave, and SHALL report the wait through its state
query. The
wait SHALL be durable: an approval delivered while no worker is executing the rollout is held and
applied when it resumes. One approval SHALL authorize one wave: the approval is consumed by the
next wave that requires it, a wave that does not require approval never consumes one, a repeated
approval while one is already outstanding changes nothing, and an approval arriving after the
rollout has concluded changes nothing.

#### Scenario: A gated wave starts only after approval

- **WHEN** a rollout reaches a wave whose sequence entry requires approval and no approval is
  outstanding
- **THEN** the rollout reports that it is awaiting approval, and that wave's target membership is
  not resolved and no update command is delivered until an approval is received

#### Scenario: An approval lets the gated wave start

- **WHEN** the approval signal is received while the rollout is awaiting approval
- **THEN** the rollout returns to running and that wave is resolved and dispatched

#### Scenario: One approval passes one gated wave

- **WHEN** a rollout with two approval-requiring waves receives a single approval
- **THEN** the first of them proceeds and the second still waits for its own approval

#### Scenario: An early approval is held and reported

- **WHEN** the approval signal arrives while the rollout is running and no wave is awaiting approval
- **THEN** the rollout records an outstanding approval, its state query reports it, and the next
  approval-requiring wave starts without waiting

#### Scenario: Approval survives a worker restart

- **WHEN** a rollout is awaiting approval, its worker stops, and the approval signal arrives before
  another worker resumes the rollout
- **THEN** the rollout resumes and the approved wave starts

#### Scenario: A concluded rollout ignores approval

- **WHEN** the approval signal arrives after the rollout has completed, rolled back, or failed
- **THEN** the rollout's recorded state is unchanged
