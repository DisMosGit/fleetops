# Spec Delta

## MODIFIED Requirements

### Requirement: One workflow per device owns authoritative device state

Each device SHALL be represented by exactly one long-lived workflow, addressed by the stable
workflow identity `device-<device_id>`, which owns the device's authoritative state: the current
firmware version, the last heartbeat timestamp, the pending command (its id, kind, and delivery
state), the last concluded command (the command as issued, its terminal outcome, and its failure
detail), and the configuration snapshot (its content and version) — carried alongside the device's
identity attributes (region, model) and its liveness status. This state SHALL be readable through a
state query returning the complete carried state together. Where any stored copy of device state
(for example a persisted device record or a persisted state snapshot) disagrees with the workflow's
state on these fields, the workflow's state SHALL be the correct one. The workflow SHALL NOT
terminate as part of normal operation: it exists for the life of the device.

#### Scenario: First signal brings the workflow into existence

- **WHEN** a signal for a device with no running workflow arrives
- **THEN** the workflow for `device-<device_id>` is created and applies that signal, and no second
  workflow for the same device id exists

#### Scenario: State query returns the complete picture

- **WHEN** the state query is evaluated for a device
- **THEN** it returns the current firmware version, the last heartbeat timestamp, the pending
  command (or none), the last concluded command (or none), the configuration snapshot with its
  version, the region and model, and the liveness status

#### Scenario: Workflow state wins over stored copies

- **WHEN** a persisted device record or state snapshot disagrees with the workflow state on one of
  the carried fields
- **THEN** the workflow's value is treated as correct

### Requirement: Command result signal concludes the pending command

The workflow SHALL accept a command-result signal carrying the command id and the terminal
outcome. It SHALL clear the pending command only when the result's command id matches the pending
command's id; a result for any other command id SHALL change nothing. Concluding a command SHALL
record that command as the device's last concluded command, with its terminal outcome and, for a
failed outcome, the reported detail, so a caller waiting for the device's result can read what
happened from the state query. On a successful outcome the owned current firmware version SHALL
become the version the concluded command targeted; on a failed outcome the firmware version SHALL
be unchanged.

#### Scenario: Matching result clears the pending command

- **WHEN** a command-result signal matches the pending command's id
- **THEN** the device has no pending command

#### Scenario: The concluded command is recorded and readable

- **WHEN** the state query is evaluated after a command concluded
- **THEN** it reports that command's id, its outcome, and — for a failure — the reported detail as
  the last concluded command

#### Scenario: Success adopts the commanded firmware

- **WHEN** a firmware command concludes successfully
- **THEN** the owned current firmware version is the version that command targeted

#### Scenario: Failure leaves firmware unchanged

- **WHEN** a firmware command concludes with a failed outcome
- **THEN** the owned current firmware version is unchanged and the device has no pending command

#### Scenario: Result for a non-pending command changes nothing

- **WHEN** a command-result signal arrives for a command that is not the pending command
- **THEN** the device state is unchanged, including its last concluded command
