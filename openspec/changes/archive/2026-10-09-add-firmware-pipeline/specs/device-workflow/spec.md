# Spec Delta

## ADDED Requirements

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
