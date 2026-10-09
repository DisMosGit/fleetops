# Spec Delta

## Purpose

Owns the authoritative state of a single device — current firmware, last heartbeat timestamp,
pending command, configuration snapshot — in one long-lived per-device workflow that applies
idempotent signals and stays bounded through rolling continuation.

## ADDED Requirements

### Requirement: One workflow per device owns authoritative device state
Each device SHALL be represented by exactly one long-lived workflow, addressed by the stable
workflow identity `device-<device_id>`, which owns the device's authoritative state: the current
firmware version, the last heartbeat timestamp, the pending command (its id, kind, and delivery
state), and the configuration snapshot (its content and version). This state SHALL be readable
through a state query returning all four fields together. Where any stored copy of device state
(for example a persisted device record) disagrees with the workflow's state on these fields,
the workflow's state SHALL be the correct one. The workflow SHALL NOT terminate as part of
normal operation: it exists for the life of the device.

#### Scenario: First signal brings the workflow into existence
- **WHEN** a signal for a device with no running workflow arrives
- **THEN** the workflow for `device-<device_id>` is created and applies that signal, and no
  second workflow for the same device id exists

#### Scenario: State query returns the complete picture
- **WHEN** the state query is evaluated for a device
- **THEN** it returns the current firmware version, the last heartbeat timestamp, the pending
  command (or none), and the configuration snapshot with its version

#### Scenario: Workflow state wins over stored copies
- **WHEN** a persisted device record disagrees with the workflow state on one of the four owned
  fields
- **THEN** the workflow's value is treated as correct

### Requirement: Heartbeat signal updates liveness and firmware
The workflow SHALL accept a heartbeat signal carrying the heartbeat's event id, the reported
firmware version, and the heartbeat timestamp. Applying it SHALL move the last heartbeat
timestamp forward to the signal's timestamp and update the current firmware version when the
reported version differs from the owned one. A heartbeat whose timestamp is not newer than the
recorded last heartbeat SHALL change nothing.

#### Scenario: Heartbeat refreshes liveness
- **WHEN** a heartbeat signal with a newer timestamp than the recorded last heartbeat arrives
- **THEN** the last heartbeat timestamp equals the signal's timestamp

#### Scenario: Heartbeat reports a firmware change
- **WHEN** a heartbeat signal reports a firmware version different from the owned one
- **THEN** the owned current firmware version becomes the reported one

#### Scenario: Stale heartbeat changes nothing
- **WHEN** a heartbeat signal arrives whose timestamp is not newer than the recorded last
  heartbeat
- **THEN** the device state is unchanged

### Requirement: Command issuance sets the pending command and triggers delivery
The workflow SHALL accept a command-issued signal carrying the command id, its kind, and its
targets, and SHALL record it as the pending command and deliver it to the device's agent at
least once through the control plane's command dispatch. While a command is pending, a newer
command-issued signal SHALL supersede it: the pending command becomes the newer one, and a
command result for the superseded command SHALL NOT clear it. A command-issued signal repeating
a command id already accepted SHALL change nothing.

#### Scenario: Issued command becomes pending and is delivered
- **WHEN** a command-issued signal for a device with no pending command arrives
- **THEN** the command is recorded as the pending command and delivered to the device's agent
  with its command id and device id

#### Scenario: Newer command supersedes the pending one
- **WHEN** a command is pending and a command-issued signal with a different command id arrives
- **THEN** the newer command is the pending command and the superseded one is no longer pending

#### Scenario: Repeated issuance of the same command is a no-op
- **WHEN** a command-issued signal repeats a command id already accepted
- **THEN** the pending command and its delivery state are unchanged

### Requirement: Command result signal concludes the pending command
The workflow SHALL accept a command-result signal carrying the command id and the terminal
outcome. It SHALL clear the pending command only when the result's command id matches the
pending command's id; a result for any other command id SHALL change nothing. On a successful
outcome the owned current firmware version SHALL become the version the concluded command
targeted; on a failed outcome the firmware version SHALL be unchanged.

#### Scenario: Matching result clears the pending command
- **WHEN** a command-result signal matches the pending command's id
- **THEN** the device has no pending command

#### Scenario: Success adopts the commanded firmware
- **WHEN** a firmware command concludes successfully
- **THEN** the owned current firmware version is the version that command targeted

#### Scenario: Failure leaves firmware unchanged
- **WHEN** a firmware command concludes with a failed outcome
- **THEN** the owned current firmware version is unchanged and the device has no pending command

#### Scenario: Result for a non-pending command changes nothing
- **WHEN** a command-result signal arrives for a command that is not the pending command
- **THEN** the device state is unchanged

### Requirement: Configuration changed signal applies versioned snapshots
The workflow SHALL accept a configuration-changed signal carrying a complete configuration
snapshot and its monotonically increasing version, and SHALL replace the owned configuration
snapshot only when the signal's version is strictly newer than the owned snapshot's version. A
configuration-changed signal at or below the owned version SHALL change nothing.

#### Scenario: Newer configuration version is applied
- **WHEN** a configuration-changed signal carries a version newer than the owned snapshot's
- **THEN** the owned configuration snapshot and its version are the signal's

#### Scenario: Older or repeated configuration version is ignored
- **WHEN** a configuration-changed signal carries a version at or below the owned snapshot's
- **THEN** the owned configuration snapshot is unchanged

### Requirement: Signals are idempotent when delivered twice
Every signal the workflow accepts SHALL produce at most one state transition per delivery key
(heartbeat event id, command id, configuration version): delivering the same signal twice SHALL
leave the device state exactly as after the first delivery. This SHALL hold across rolling
continuations, and SHALL hold even for a duplicate arriving after the dedup memory of a run has
rolled over, by construction of the state transitions themselves.

#### Scenario: Duplicate heartbeat is dropped
- **WHEN** two heartbeat signals with the same event id are delivered
- **THEN** the device state reflects exactly one of them

#### Scenario: Duplicate command result is dropped
- **WHEN** two command-result signals with the same command id are delivered
- **THEN** the pending command is cleared once and no further state change occurs

#### Scenario: Duplicate signals across a continuation are dropped
- **WHEN** a signal is delivered, the workflow rolls over to a new run, and the same signal is
  delivered again
- **THEN** the second delivery causes no state change beyond the first

### Requirement: Rolling continuation bounds event history
The workflow SHALL keep its event history bounded under sustained signal load: after a bounded
number of applied signals it SHALL roll over to a fresh run (continue its run chain) carrying
the complete device state — including the dedup memory the idempotency requirement depends on —
so the visible state after the rollover is exactly the state before it. Signals delivered
around a rollover SHALL be applied exactly once: none lost, none applied twice.

#### Scenario: State survives the rollover unchanged
- **WHEN** the workflow rolls over to a fresh run
- **THEN** a state query before and after the rollover returns equal device state, including the
  dedup-relevant fields

#### Scenario: Signals in flight around a rollover are applied exactly once
- **WHEN** signals are delivered while the workflow is rolling over
- **THEN** each signal is applied exactly once by the continuing run chain

#### Scenario: History stays bounded under sustained load
- **WHEN** the workflow applies signals continuously over a long period
- **THEN** no single run's event history grows without bound; the run chain rolls over instead
