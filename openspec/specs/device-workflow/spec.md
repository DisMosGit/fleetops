# device-workflow Specification

## Purpose
Owns the authoritative state of a single device — current firmware, last heartbeat timestamp,
pending command, configuration snapshot — in one long-lived per-device workflow that applies
idempotent signals and stays bounded through rolling continuation.

## Requirements

### Requirement: One workflow per device owns authoritative device state

Each device SHALL be represented by exactly one long-lived workflow, addressed by the stable
workflow identity `device-<device_id>`, which owns the device's authoritative state: the current
firmware version, the firmware version it ran before the current one, the last heartbeat timestamp,
the pending command (its id, kind, and delivery state), the last concluded command (the command as
issued, its terminal outcome, and its failure detail), and the configuration snapshot (its content
and version) — carried alongside the device's identity attributes (region, model) and its liveness
status. This state SHALL be readable through a state query returning the complete carried state
together, including the previous firmware version. The previous firmware version SHALL be the version
the device ran immediately before its current one, SHALL change only when the current firmware version
changes — the version being left becomes the previous one, whether the change is adopted from the
device's own report or from the conclusion of a successful firmware command — and SHALL be carried
with the device state across the workflow's continuations, so a device whose firmware changes records
what it can be restored to without keeping its own history. A device whose firmware has never changed
SHALL record no previous firmware version. Where any stored copy of device state (for example a
persisted device record or a persisted state snapshot) disagrees with the workflow's state on these
fields, the workflow's state SHALL be the correct one. The workflow SHALL NOT terminate as part of
normal operation: it exists for the life of the device.

#### Scenario: First signal brings the workflow into existence

- **WHEN** a signal for a device with no running workflow arrives
- **THEN** the workflow for `device-<device_id>` is created and applies that signal, and no second
  workflow for the same device id exists

#### Scenario: State query returns the complete picture

- **WHEN** the state query is evaluated for a device
- **THEN** it returns the current firmware version, the previous firmware version, the last heartbeat
  timestamp, the pending command (or none), the last concluded command (or none), the configuration
  snapshot with its version, the region and model, and the liveness status

#### Scenario: A device that never changed firmware records no previous version

- **WHEN** the state query is evaluated for a device whose firmware has only ever been reported
- **THEN** it records the version the device runs and no previous firmware version

#### Scenario: The version being left becomes the previous one

- **WHEN** a device running version `1.0.0` adopts version `2.0.0`, from its own report or from a
  successful firmware command
- **THEN** its state records `2.0.0` as the current firmware version and `1.0.0` as the previous one

#### Scenario: A report that changes nothing moves nothing

- **WHEN** a device's report or command conclusion names the firmware version the device already runs
- **THEN** neither its current nor its previous firmware version changes

#### Scenario: The previous version survives a continuation

- **WHEN** a device that changed firmware is carried into a new run of its workflow
- **THEN** its previous firmware version is still recorded and readable

#### Scenario: Workflow state wins over stored copies

- **WHEN** a persisted device record or state snapshot disagrees with the workflow state on one of
  the carried fields
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
outcome. It SHALL clear the pending command only when the result's command id matches the pending
command's id; a result for any other command id SHALL change nothing. Concluding a command SHALL
record that command as the device's last concluded command, with its terminal outcome and, for a
failed outcome, the reported detail, so a caller waiting for the device's result can read what
happened from the state query. On a successful outcome the owned current firmware version SHALL
become the version the concluded command targeted, and the version it replaced SHALL become the
device's previous firmware version; on a failed outcome the firmware version SHALL be unchanged. A
command that targets the version the device already runs SHALL leave both firmware versions as they
are.

#### Scenario: Matching result clears the pending command

- **WHEN** a command-result signal matches the pending command's id
- **THEN** the device has no pending command

#### Scenario: The concluded command is recorded and readable

- **WHEN** the state query is evaluated after a command concluded
- **THEN** it reports that command's id, its outcome, and — for a failure — the reported detail as
  the last concluded command

#### Scenario: Success adopts the commanded firmware

- **WHEN** a firmware command concludes successfully
- **THEN** the owned current firmware version is the version that command targeted, and the version
  the device ran before it is the owned previous firmware version

#### Scenario: A restore moves the previous version too

- **WHEN** a device running `2.0.0` with previous version `1.0.0` concludes a command for `1.0.0`
  successfully
- **THEN** it records `1.0.0` as the current firmware version and `2.0.0` as the previous one

#### Scenario: Failure leaves firmware unchanged

- **WHEN** a firmware command concludes with a failed outcome
- **THEN** the owned current firmware version and the owned previous firmware version are unchanged
  and the device has no pending command

#### Scenario: Result for a non-pending command changes nothing

- **WHEN** a command-result signal arrives for a command that is not the pending command
- **THEN** the device state is unchanged, including its last concluded command and its firmware
  versions

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

### Requirement: Update status signals
The device workflow SHALL accept an `update_status` signal carrying the firmware id, the update
phase, the progress percentage, and operator-safe failure detail, and SHALL record the latest
reported update status in its entity state so it is carried in state snapshots. Update status
signals SHALL apply in arrival order like every other signal and MUST NOT conclude or supersede
the pending command — command conclusion stays with the command-result signal.

#### Scenario: Reported phase lands in device state
- **WHEN** a device's workflow receives an update status signal reporting the failed phase with
  detail
- **THEN** the workflow state records the firmware id, the failed phase, and the detail, and the
  next state snapshot carries them

#### Scenario: Latest progress wins
- **WHEN** several update status signals arrive for one update
- **THEN** the workflow state reflects the most recently reported phase and progress

#### Scenario: Update status does not conclude the command
- **WHEN** update status signals arrive while a command is pending
- **THEN** the command stays pending until its command result is signaled

### Requirement: Identity attributes and liveness status are carried in state
The device workflow SHALL carry the device's identity attributes (region and model), adopting
the registration data its signals carry whenever a signal supplies them, and SHALL carry the
device's liveness status. The liveness status SHALL be online while the device reports in: it
flips to online whenever a heartbeat is applied, and flips to offline when the last heartbeat
timestamp falls more than the configured offline threshold behind the workflow's current time —
a judgement the workflow evaluates whenever it schedules its periodic work. Both the identity
attributes and the liveness status SHALL travel with the rest of the state across rolling
continuations.

#### Scenario: Identity attributes are adopted
- **WHEN** a signal carrying the device's region `eu-west` and model `sensor-2` is applied
- **THEN** the state's identity attributes are `eu-west` and `sensor-2`

#### Scenario: Silence past the threshold flips the status offline
- **WHEN** no heartbeat has been applied for longer than the configured offline threshold
- **THEN** the liveness status becomes offline

#### Scenario: A new heartbeat flips the status back online
- **WHEN** a device that is offline applies a heartbeat
- **THEN** the liveness status becomes online

#### Scenario: Status survives the rollover
- **WHEN** the workflow rolls over to a fresh run while the device is online
- **THEN** the carried state still reports the device as online
