# Spec Delta

## Purpose

Tells the rest of the fleet that a rollout is rolling back: it publishes one event into the broker
when the rollback starts and one when its compensations have run, carrying what happened so a
dashboard or an alerting consumer can react without reading a workflow or a database.

## ADDED Requirements

### Requirement: A rollback is announced when it starts and when it completes

A rollback SHALL publish two events into the broker's events exchange — one when it starts, before
any compensation runs, and one when its compensations have run — under the notification key space, on
the routing keys `rollout.notification.rollback.started` and `rollout.notification.rollback.completed`
respectively, so that a consumer may subscribe to rollbacks alone (`rollout.notification.rollback.#`)
or to every notification a rollout publishes (`rollout.notification.#`).

Both events SHALL identify the rollout, the firmware it deploys (its id and version), the rollout's
target selector (region and model), the wave whose failure ended the rollout, and the outcome that
ended it. The event published when the rollback starts SHALL additionally carry the decision that
ended the rollout — the verdict, the evaluated success ratio, the sample size, and the window it was
measured over — for an ending wave that was measured; a wave that ended the rollout before it could
be measured SHALL carry no decision rather than a fabricated one. The started event SHALL also carry
how many steps and how many devices the rollback will compensate. The event published when the
compensations have run SHALL additionally carry what they achieved: how many devices were restored,
failed to restore, never reported, had nothing to restore, and could not be restored, how many
device records were corrected and how many could not be verified, and the reconciled inventory as
the number of devices on each firmware version.

#### Scenario: A rollback is announced before it compensates

- **WHEN** a rollout enters rollback
- **THEN** an event on `rollout.notification.rollback.started` names the rollout, the firmware, the
  wave that ended it, the outcome, and the size of the plan, and it is published before any downgrade
  step runs

#### Scenario: A rollback is announced when its compensations have run

- **WHEN** a rollback's last step has run
- **THEN** an event on `rollout.notification.rollback.completed` names what the plan achieved,
  including the restored and unrestored devices, the corrections, and the reconciled inventory

#### Scenario: A consumer can narrow to rollback announcements

- **WHEN** a queue is bound to the events exchange with `rollout.notification.rollback.#`
- **THEN** both a started and a completed announcement are delivered to it, and a heartbeat event is
  not

#### Scenario: A rollout that could not start announces nothing

- **WHEN** a rollout concludes as failed because its firmware is unknown or does not target the
  selector's model
- **THEN** no rollback announcement is published

### Requirement: An announcement is published with the broker's confirmation

An announcement SHALL be published as a persistent event that waits for the broker's confirmation
before the step that publishes it is recorded as having completed. A publication the broker rejects,
cannot route, or loses with its connection SHALL be an error the announcing step retries, so a
rollback is never recorded as announced when the broker never took the event. An announcement that
cannot be published after its retries are exhausted SHALL be recorded as a failed step and SHALL NOT
change the rollout's outcome, its recorded rollback, or the compensations that already ran: the
rollout's own record is authoritative and the announcement only reports it.

#### Scenario: The step completes only once the broker accepted the event

- **WHEN** the broker confirms the publication
- **THEN** the announcement step is recorded as completed

#### Scenario: A rejected publication is retried

- **WHEN** the broker rejects, cannot route, or loses the connection under an announcement
- **THEN** the publication is retried and the step is not recorded as completed on the strength of the
  failed attempt

#### Scenario: An unannounced rollback is still a rollback

- **WHEN** an announcement cannot be published after its retries are exhausted
- **THEN** the failure is recorded against that step, the rollout still concludes as rolled back with
  the outcome that ended it, and the compensations that ran are still recorded

### Requirement: An announcement carries a stable identity

Each announcement SHALL carry an event identity derived from the rollout and the phase it announces,
and SHALL carry the version of the event contract it is written under. A republished announcement
SHALL therefore repeat the same event identity, so a consumer that deduplicates by event identity
applies it once. A rollout rolls back once, so the phase is what tells one rollback's two
announcements apart, and a second publication of one phase is a redelivery rather than a second
event.

#### Scenario: A republished announcement is the same event

- **WHEN** an announcement step is retried after a publication the broker already accepted
- **THEN** the event carries the same event identity as the first publication

#### Scenario: The two phases of one rollback are distinct events

- **WHEN** a rollout's started and completed announcements are compared
- **THEN** they carry different event identities and the same rollout identity

#### Scenario: An announcement names its contract version

- **WHEN** an announcement is decoded
- **THEN** it carries the version of the event contract it was written under
