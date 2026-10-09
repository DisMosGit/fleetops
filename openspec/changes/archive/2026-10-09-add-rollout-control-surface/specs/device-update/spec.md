# Spec Delta

## Purpose

Updates one device and confirms it: the per-device activity that commands a single device through
its workflow and waits for that device's reported result within the configured timeout, and the
outcomes it hands back to the wave that dispatched it.

## ADDED Requirements

### Requirement: One update activity commands one device and waits for its result

For a single target device the rollout SHALL run one `update-device` activity whose request names
the rollout, the wave, the device, the firmware to deploy (its id, version, and checksum), and the
deadline by which a reported result is still useful. The activity SHALL deliver the update command
to that device's workflow through the device command seam under the command id derived from the
wave and the device, SHALL then wait until the device's workflow reports that command concluded,
and SHALL return the device's outcome: `succeeded` when the command concluded successfully,
`failed` carrying the device's own failure detail when it concluded unsuccessfully, and
`unreported` when the deadline passed without a concluded result. The wait SHALL be bounded by the
requested deadline — no update activity outlives it — and SHALL observe the device's state through
the device workflow's state query rather than by holding an open connection or reading a database
projection, so a result reported before the first observation is still found.

#### Scenario: A device that applies the update reports success

- **WHEN** a device's workflow concludes its update command successfully before the deadline
- **THEN** the activity returns the outcome `succeeded` for that device

#### Scenario: A device that fails the update reports the failure

- **WHEN** a device's workflow concludes its update command with a failed outcome and a detail
- **THEN** the activity returns the outcome `failed` carrying that detail

#### Scenario: A device that never reports runs out of time

- **WHEN** the deadline passes while the device's workflow has not concluded the command
- **THEN** the activity returns the outcome `unreported` for that device rather than waiting longer

#### Scenario: A result reported before the first observation is found

- **WHEN** a device concludes its command before the activity's first state observation
- **THEN** the activity returns that device's recorded outcome instead of waiting for a fresh report

#### Scenario: The wait survives an attempt that dies

- **WHEN** the process running an update activity dies while it waits and the activity is retried
- **THEN** the retry still returns the device's recorded outcome, or `unreported` once the deadline
  has passed, without commanding the device twice

### Requirement: Update delivery is idempotent under retries

Every attempt of one device's update SHALL use the same command id, so a retried or duplicated
delivery is a no-op for a device that already accepted the command. A command that a newer command
superseded, and a command whose result never arrives, SHALL both surface as `unreported` rather
than as a success or a failure — the device never concluded the command, and the wave is told
exactly that.

#### Scenario: A retried attempt repeats the command id

- **WHEN** an update activity is retried after its command was already accepted
- **THEN** the device receives the same command id again and its recorded state is unchanged

#### Scenario: A superseded command is never a success

- **WHEN** a device's pending command is superseded before it concludes
- **THEN** the superseded command's activity reports `unreported` when its deadline passes

### Requirement: A command that cannot be delivered is an error

An update command that cannot be delivered to its device's workflow at all — the device command
seam failing for every attempt — SHALL be reported as an activity error rather than as one of the
three outcomes, so the wave that dispatched it can record the failure and roll the rollout back
instead of promoting a wave it could not command.

#### Scenario: An undeliverable command fails the activity

- **WHEN** the device command seam refuses every delivery attempt for one target device
- **THEN** that device's update activity returns an error and no outcome, and no device is reported
  as updated on the strength of it
