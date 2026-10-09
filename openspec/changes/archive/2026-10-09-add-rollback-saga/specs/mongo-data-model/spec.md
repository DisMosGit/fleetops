# Spec Delta

## MODIFIED Requirements

### Requirement: Device records

The system SHALL store exactly one heterogeneous document per device in the `devices` collection,
identified by `_id` = device identity. Every device document MUST contain the fields `model`,
`region`, `current_fw` (the firmware version the device runs), `status`, and `last_heartbeat`.
Documents missing any required field MUST be rejected at write time.

`current_fw` SHALL be the fleet's record of the firmware version a device runs, kept from the
device's own reports and from the registration that enrolled it. It is a copy rather than the
authority: the device's workflow owns the version the device runs, and where the recorded version
disagrees with it the workflow's value is the correct one. A rollback MAY therefore correct
`current_fw` to the version the device's workflow reports, and SHALL do so only by writing a version
a device reported — the field is never derived, guessed, or cleared.

#### Scenario: Device record content

- **WHEN** a device enrols and emits state
- **THEN** its `devices` document is retrievable by `_id` and carries `model`, `region`,
  `current_fw`, `status`, and `last_heartbeat`

#### Scenario: Required fields enforced

- **WHEN** a write attempts to store a device document missing a required field
- **THEN** the write is rejected and no partial document is stored

#### Scenario: Device state updates in place

- **WHEN** a heartbeat or command result changes a device's state
- **THEN** the existing `devices` document is updated in place and no second document appears for
  the same device identity

#### Scenario: A stale recorded version is corrected against the device

- **WHEN** a rollback finds a device's recorded `current_fw` disagreeing with the version that
  device's workflow holds
- **THEN** the recorded version is corrected to the version the workflow holds, in place, with no
  second document for that device

#### Scenario: A record that already agrees is left alone

- **WHEN** a rollback finds a device's recorded `current_fw` already equal to the version that
  device's workflow holds
- **THEN** the document is unchanged

### Requirement: Rollout records

The system SHALL store one document per rollout in the `rollouts` collection, identified by
`_id` = rollout id. Every rollout document MUST contain `firmware_id`, `status`, and
`temporal_wf_id` (the id of the workflow execution driving the rollout), and MUST record the
target group as `region` plus `model`. The document SHALL be written when the rollout workflow
starts and updated by that workflow as the rollout progresses; `status` SHALL be one of the
lifecycle values `running`, `paused`, `awaiting_approval`, `rolling_back`, `rolled_back`,
`completed`, or `failed`, where `rolled_back`, `completed`, and `failed` are terminal and are never
left once recorded, and `running`, `paused`, `awaiting_approval`, and `rolling_back` are never
terminal. Every write SHALL be idempotent: a repeated write of a state the document already holds
MUST leave it unchanged.

A rollout that enters rollback SHALL carry the rollback as it compensates: the outcome that ended the
rollout, the ordered steps with the kind of each step, the wave a compensating step compensates, its
status, and what it achieved (the devices restored, failed, unreported, skipped, and unavailable; the
device records agreed, corrected, and unverified), the inventory the reconciliation established as the
number of devices on each firmware version, and the devices the rollback could not restore. These
fields MAY be absent on a document written before they existed, and SHALL be written by every write
that records a rollout in rollback or afterwards.

#### Scenario: Rollout record content

- **WHEN** a rollout workflow starts for a firmware and a target group
- **THEN** its `rollouts` document is retrievable by `_id` and carries `firmware_id`, `status`,
  `temporal_wf_id`, `region`, and `model`

#### Scenario: Rollout status transitions are recorded

- **WHEN** the rollout workflow advances a wave, waits for approval, is paused, is resumed, enters
  rollback, completes its rollback, completes, or fails
- **THEN** the `status` field of the existing rollout document reflects the new state

#### Scenario: A paused rollout is recorded as paused

- **WHEN** a running rollout is paused and later resumed
- **THEN** its document carries `paused` while it is paused and a non-terminal status again once it
  is resumed

#### Scenario: A compensating rollout is recorded as rolling back

- **WHEN** a rollout has entered rollback and its compensations are still running
- **THEN** its document carries `rolling_back`, which is not terminal, together with the outcome
  that ended the rollout

#### Scenario: A concluded rollback carries what it achieved

- **WHEN** a rollout's rollback has run
- **THEN** its document carries the terminal status `rolled_back` and the rollback record: the outcome
  that ended the rollout, each step's kind, the wave it compensates, its status and counts, the
  reconciled inventory with its correction count, and the devices that could not be restored

#### Scenario: A rollout that could not start is recorded as failed

- **WHEN** a rollout workflow cannot resolve the firmware it was given, or the firmware does not
  target the selector's model
- **THEN** its rollout document carries the terminal status `failed`

#### Scenario: A terminal status is not overwritten

- **WHEN** a write attempts to move a rollout document out of `rolled_back`, `completed`, or
  `failed`
- **THEN** the stored status is unchanged

#### Scenario: A retried rollback write converges

- **WHEN** a rollback record already written is written again
- **THEN** the rollout document's rollback record is unchanged rather than duplicated or regressed
