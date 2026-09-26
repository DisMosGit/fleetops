# Spec Delta

## MODIFIED Requirements

### Requirement: One workflow per device owns authoritative device state
Each device SHALL be represented by exactly one long-lived workflow, addressed by the stable
workflow identity `device-<device_id>`, which owns the device's authoritative state: the current
firmware version, the last heartbeat timestamp, the pending command (its id, kind, and delivery
state), and the configuration snapshot (its content and version) — carried alongside the
device's identity attributes (region, model) and its liveness status. This state SHALL be
readable through a state query returning the complete carried state together. Where any stored
copy of device state (for example a persisted device record or a persisted state snapshot)
disagrees with the workflow's state on these fields, the workflow's state SHALL be the correct
one. The workflow SHALL NOT terminate as part of normal operation: it exists for the life of the
device.

#### Scenario: First signal brings the workflow into existence
- **WHEN** a signal for a device with no running workflow arrives
- **THEN** the workflow for `device-<device_id>` is created and applies that signal, and no
  second workflow for the same device id exists

#### Scenario: State query returns the complete picture
- **WHEN** the state query is evaluated for a device
- **THEN** it returns the current firmware version, the last heartbeat timestamp, the pending
  command (or none), the configuration snapshot with its version, the region and model, and the
  liveness status

#### Scenario: Workflow state wins over stored copies
- **WHEN** a persisted device record or state snapshot disagrees with the workflow state on one
  of the carried fields
- **THEN** the workflow's value is treated as correct

## ADDED Requirements

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
