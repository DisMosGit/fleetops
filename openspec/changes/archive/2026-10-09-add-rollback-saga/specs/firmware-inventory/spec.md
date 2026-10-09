# Spec Delta

## Purpose

Keeps the fleet's record of which firmware version each device runs true after a rollback: it
verifies the devices a rollback touched against the version each device's workflow — the authority on
its own firmware — holds, corrects the recorded version where it disagrees, and reports the inventory
that stands once the rollback has run.

## ADDED Requirements

### Requirement: A rollback reconciles the fleet's firmware inventory against the devices

After its downgrade steps, a rollback SHALL reconcile the fleet's recorded firmware version for every
device it touched — the devices of every wave the plan compensates — against the version those devices
actually run. For each such device the reconciliation SHALL read the version the device's workflow
holds as authoritative, SHALL compare it with the version the fleet records for that device, and SHALL
correct the record when the two disagree. Each device SHALL be reported as `agreed` when the recorded
version already matched, `corrected` when the record was wrong and now holds the version the device
reports, or `unverified` when the comparison could not be made, with the reason.

The reconciliation SHALL be the last compensating step before the rollout's outcome is announced, so
that what the fleet records about firmware versions is what the devices report at the moment the
rollback concludes, rather than what a heartbeat that a downgraded device may never send would
eventually make it.

#### Scenario: A record that disagrees with the device is corrected

- **WHEN** a device's workflow holds version `1.0.0` while the fleet records the device as running
  `2.0.0`
- **THEN** the recorded version becomes `1.0.0` and the device is reported as corrected

#### Scenario: A record that already agrees is left alone

- **WHEN** the recorded version matches the version the device's workflow holds
- **THEN** the record is not written and the device is reported as having agreed

#### Scenario: Every device the rollback touched is reconciled

- **WHEN** a rollback's plan compensates three waves
- **THEN** every device of those three waves is reconciled, including the devices that never took the
  deployed firmware

#### Scenario: The reconciliation follows the downgrades

- **WHEN** a rollback's steps run
- **THEN** no device's record is reconciled before the downgrade step that compensates its wave has
  run, and the reconciliation runs before the completion of the rollback is announced

### Requirement: Reconciliation is idempotent and copies only what a device reports

The reconciliation SHALL write a device's recorded firmware version only when it disagrees with the
version the device's workflow holds, and SHALL write exactly that version: it SHALL never invent,
guess, or derive a version, and SHALL never write a version no device reported. A repeated
reconciliation of a device whose record already matches SHALL write nothing and SHALL report the
device as having agreed, so a reconciliation retried after a partial failure converges on the same
records rather than duplicating or regressing them.

A device whose authoritative state cannot be read, a device whose workflow holds no firmware version
at all — one that has never reported a version and never concluded a command — and a device for which
the fleet holds no record at all SHALL each be reported as unverified with the reason and SHALL NOT be
counted as running any version, so an unverified device is visible as exactly that rather than
silently assumed correct, and no record is ever corrected to a version no device reported.

#### Scenario: A retried reconciliation writes nothing

- **WHEN** the reconciliation runs again for a device whose recorded version already matches
- **THEN** the record is unchanged and the device is reported as having agreed

#### Scenario: Only a reported version is written

- **WHEN** the reconciliation corrects a device's record
- **THEN** the value written is the version the device's workflow holds, unchanged and uninterpreted

#### Scenario: An unreadable device is never assumed correct

- **WHEN** a device's authoritative state cannot be read
- **THEN** its record is left as it stands and the device is reported as unverified with the reason

#### Scenario: A device with no record is reported rather than invented

- **WHEN** the reconciliation runs for a device the fleet holds no record for
- **THEN** no record is created, and the device is reported as unverified

#### Scenario: A device that holds no version is never used to clear a record

- **WHEN** the reconciliation runs for a device whose workflow holds no firmware version while the
  fleet records one
- **THEN** the record is left as it stands and the device is reported as unverified with that reason
  rather than corrected to an empty version

### Requirement: The rollback reports the inventory it reconciled

The reconciliation step SHALL report the inventory that stands after the rollback: how many of the
devices it touched were found on each firmware version, how many records it corrected, and how many
devices it could not verify. Every device the rollback touched SHALL be accounted for exactly once —
either on the version it was found to run or among the devices that could not be verified — and the
reported inventory SHALL be carried by the rollback's record and by the announcement that its
compensations completed.

#### Scenario: The inventory accounts for every device exactly once

- **WHEN** the reconciliation has run over the devices a rollback touched
- **THEN** the number of devices it reports on the versions it found plus the number it reports as
  unverified equals the number of devices it was given

#### Scenario: Corrections are reported alongside the inventory

- **WHEN** the reconciliation corrected some records
- **THEN** the rollback's record and its completion announcement both carry how many records were
  corrected, distinct from how many devices were found on each version
