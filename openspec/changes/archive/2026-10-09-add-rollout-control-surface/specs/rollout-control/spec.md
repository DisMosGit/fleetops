# Spec Delta

## Purpose

Holds and continues a running rollout: the pause and resume signals an operator uses to stop a
rollout from advancing and to let it continue, what a paused rollout still does, and the `paused`
lifecycle status it reports.

## ADDED Requirements

### Requirement: A running rollout can be paused and resumed

The rollout SHALL accept a `pause_rollout` signal and a `resume_rollout` signal. A paused rollout
SHALL start no further wave: it SHALL resolve no membership, dispatch no update command, and enter
no wave it had not already started. A resumed rollout SHALL return to running and continue from
exactly where it stopped, with every recorded wave outcome, promoted wave, and banked approval
intact. A repeated pause while paused and a resume while not paused SHALL change nothing, and a
pause or resume delivered after the rollout concluded SHALL leave its recorded state unchanged. The
rollout SHALL report being paused through its status and its state query.

#### Scenario: A pause stops the rollout at the next wave boundary

- **WHEN** a rollout is paused while no wave is in flight
- **THEN** no further wave is resolved or dispatched until it is resumed, and its status reports
  the pause

#### Scenario: A resume continues where the rollout stopped

- **WHEN** a paused rollout is resumed
- **THEN** it returns to running and starts the next wave of its sequence, with the waves promoted
  before the pause still recorded as promoted

#### Scenario: Pausing a rollout that waits for approval keeps the approval

- **WHEN** a rollout awaiting approval is paused and then resumed
- **THEN** it is still awaiting approval, an approval that arrived while it was paused is still
  outstanding, and the gated wave starts on that approval

#### Scenario: Repeated signals change nothing

- **WHEN** a rollout receives a pause while it is paused, or a resume while it is running
- **THEN** its state and recorded documents are unchanged

#### Scenario: A concluded rollout ignores pause and resume

- **WHEN** a pause or a resume arrives after the rollout has completed, rolled back, or failed
- **THEN** its recorded state is unchanged

### Requirement: A pause holds promotion, not safety

A wave already in flight when the rollout is paused SHALL be driven to its recorded decision: its
health window and decision timeout SHALL be honoured unchanged, it SHALL be measured over the same
window and membership, and its decision SHALL be recorded. A wave that fails — an unhealthy
verdict, an undecided wave past its decision timeout, or update commands that could not be
delivered — SHALL still roll the rollout back while the rollout is paused, because a pause may
withhold a promotion but SHALL NOT withhold a rollback. A paused rollout SHALL NOT be recorded as
completed, and SHALL NOT promote the wave it was driving into the next one.

#### Scenario: A pause does not suppress the wave in flight

- **WHEN** a rollout is paused while a wave is inside its health window
- **THEN** that wave is still judged when its window elapses and its decision is recorded

#### Scenario: A regression still rolls back a paused rollout

- **WHEN** the wave in flight when the rollout was paused fails its gate
- **THEN** the rollout is recorded as rolled back and concludes, rather than staying paused

#### Scenario: A healthy wave does not advance a paused rollout

- **WHEN** the wave in flight when the rollout was paused is promoted
- **THEN** the wave is recorded as healthy and the rollout still starts no further wave until it is
  resumed

### Requirement: Control signals are applied while the rollout waits

Pause and resume signals SHALL be applied at every point the rollout waits — while it holds for an
approval, while its wave's update commands are being delivered and their results collected, while a
health window elapses, and while a gate measures an undecided wave. Applying a control signal SHALL
NOT disturb what the rollout is waiting for: it SHALL NOT restart, shorten, or extend a health
window, SHALL NOT resolve a membership again, SHALL NOT dispatch anything twice, and SHALL NOT
consume or drop a banked approval. The pause SHALL be durable: a signal delivered while no worker is
executing the rollout SHALL be applied when it resumes, and the rollout SHALL still be paused after a
worker restart.

#### Scenario: A pause during the dispatch wait is applied

- **WHEN** a pause signal arrives while a wave's update commands are being delivered and their
  results collected
- **THEN** the wave's collection finishes as it would have, the wave's decision is recorded, and no
  next wave starts

#### Scenario: A pause during a window is applied without moving the window

- **WHEN** a pause signal arrives while a wave's health window is elapsing
- **THEN** the window still ends at its recorded deadline and the wave is judged then

#### Scenario: A pause survives a worker restart

- **WHEN** a rollout is paused, its worker stops, and another worker resumes the rollout
- **THEN** the rollout is still paused and starts no wave until a resume arrives

#### Scenario: An approval arriving while paused is banked

- **WHEN** an operator approves a gated rollout that is paused
- **THEN** the approval is outstanding and the gated wave starts when the rollout is resumed
