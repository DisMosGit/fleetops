# Spec Delta

## Purpose

Restores one device to the firmware it ran before a rollout moved it: it reads the previous firmware
the device's own workflow remembers, resolves it in the firmware registry, commands the restore
through the same device command seam a wave uses, waits for the device's reported result, and reports
what happened to the rollback step that dispatched it.

## ADDED Requirements

### Requirement: One downgrade activity restores one device and reports the outcome

For a single device, and for the firmware a rollout deployed, the rollback SHALL run one
`downgrade-device` activity whose request names the rollout, the wave being compensated, the device,
and the firmware the device is being rolled back from. The activity SHALL read the device's
authoritative state to learn the firmware the device records as the one it ran before, SHALL resolve
that version in the firmware registry, and — when it resolves to a firmware that targets the device's
model — SHALL deliver a firmware command for that version to the device's workflow through the device
command seam, then wait for the device to report that command concluded, within the configured result
timeout.

The activity SHALL report exactly one outcome for its device: `restored` when the device reported the
command concluded successfully, `failed` carrying the device's own failure detail when it concluded
unsuccessfully, `unreported` when the wait ended without the device concluding the command, `skipped`
when the device does not run the firmware being rolled back, and `unavailable` when the restore could
not be attempted or its outcome could not be determined, carrying why.

A restore SHALL be an ordinary firmware command for an older version: the device's own command
conclusion adopts the restored version, and no new command kind, no changed device contract, and no
agent-side rollback mode is involved.

#### Scenario: A device that took the firmware is restored

- **WHEN** a device running the deployed firmware records an earlier version as the firmware it ran
  before, and the rollback runs its downgrade activity
- **THEN** that device's workflow receives a firmware command for the earlier version and the activity
  reports `restored` once the device concludes it successfully

#### Scenario: A device that reported a failed restore is reported as failed

- **WHEN** a device concludes the restore command with a failed outcome and a detail
- **THEN** the activity reports `failed` for that device, carrying that detail

#### Scenario: A device that never reports runs out of time

- **WHEN** the configured result timeout passes while the device has not concluded the restore command
- **THEN** the activity reports `unreported` for that device rather than waiting longer

#### Scenario: A device that never took the firmware is skipped

- **WHEN** the rollback's downgrade activity runs for a device that does not run the firmware being
  rolled back
- **THEN** the activity delivers no command and reports `skipped` for that device

#### Scenario: A device with nothing to restore is reported as unavailable

- **WHEN** the device records no previous firmware version, or the version it records is unknown to
  the registry, or resolves to a firmware that does not target the device's model
- **THEN** the activity delivers no command for it and reports `unavailable` with that reason

#### Scenario: A restore that cannot be delivered is reported, not raised

- **WHEN** the device command seam refuses every delivery attempt for one device's restore
- **THEN** that device is reported as unrestored with the delivery failure as its reason, and no other
  device's restore is affected

### Requirement: One device's unrestored firmware never aborts the step

The downgrade activity SHALL report a device's restore where it can determine one, and the step that
dispatched it SHALL record exactly one outcome per device it targeted. A transient failure — a device
state read that failed, a registry lookup that failed — SHALL be left to the activity's retry policy
rather than recorded as a device outcome, and a failure that outlives those retries SHALL be recorded
for that device as an unrestored device with its reason. No device's unrestored firmware SHALL fail
the step, cancel the restore of the step's other devices, or abort the rollback plan.

#### Scenario: A failed read is retried rather than recorded

- **WHEN** reading a device's state fails transiently
- **THEN** the activity is retried, and no outcome is recorded for that device on the strength of the
  failure

#### Scenario: A resolved failure is recorded for the device alone

- **WHEN** a device's restore fails after the activity's retries are exhausted
- **THEN** that device is recorded as unrestored with the failure as its reason and every other device
  of the step is still restored

### Requirement: A device already restored is never commanded twice

Every attempt of one device's restore SHALL address the device under the same command id, derived from
the rollback and the device, so that a retried or duplicated delivery is a no-op for a device that
already accepted the command. The activity SHALL determine from the device's authoritative state
whether a restore is still needed, so that a device already back on its previous firmware — because
an earlier attempt restored it, or because it never took the deployed firmware at all — is reported as
`skipped` rather than commanded again, and a redelivered command result cannot restore a device twice.

#### Scenario: A retried attempt repeats the command id

- **WHEN** a device's downgrade activity is retried after the device already accepted the restore
  command
- **THEN** the device receives the same command id again and its recorded state is unchanged

#### Scenario: An already restored device is left alone

- **WHEN** the downgrade activity runs again for a device that an earlier attempt already restored
- **THEN** no command is delivered and the device is reported as `skipped`

#### Scenario: A superseded restore is not a success

- **WHEN** a device's pending restore command is superseded by a newer command before it concludes
- **THEN** the superseded restore is reported as `unreported` when its wait ends, never as `restored`
