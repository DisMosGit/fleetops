# Spec Delta

## Purpose

Tracks the fleet's device records in MongoDB as agents connect: registering or updating a
device document when a stream's registration exchange is accepted, and keeping its last-seen
timestamp current as the device reports in.

## ADDED Requirements

### Requirement: Device record registration on stream connection
When a stream's registration exchange is accepted for a device, the system SHALL register or
update that device's record in the `devices` collection — exactly one document per device
identity — carrying the reported identity fields (`model`, `region`, `current_fw`), the
resulting status `online`, and the last-seen timestamp. A device already known is updated in
place: a later registration (for example after a reconnect, or after the device changed its
firmware) refreshes its identity fields and last-seen timestamp and MUST NOT create a second
document. A registration exchange that is rejected SHALL write no device record at all.

#### Scenario: First connection creates the device record
- **WHEN** an agent's registration exchange is accepted for a device not yet in the fleet
- **THEN** one `devices` document exists for that identity, carrying `model`, `region`,
  `current_fw`, status `online`, and a last-seen timestamp of the acceptance time

#### Scenario: Reconnection updates the record in place
- **WHEN** an already-known device is accepted again on a new stream connection with new
  identity fields (for example a newer `current_fw`)
- **THEN** the existing document reflects the new identity fields and last-seen timestamp,
  status `online`, and no second document exists for that identity

#### Scenario: Rejected registration writes nothing
- **WHEN** a registration exchange is rejected for a device unknown to the fleet
- **THEN** no `devices` document exists for that identity

### Requirement: Last-seen tracking on heartbeats
Every heartbeat accepted for a registered device SHALL refresh that device's last-seen
timestamp in its `devices` document to the time the control plane accepted the heartbeat, SHALL
update the recorded `current_fw` when the heartbeat reports a different firmware version, and
SHALL NOT create or duplicate the device document. Heartbeats from devices without an accepted
registration change no device record.

#### Scenario: Accepted heartbeat refreshes last seen
- **WHEN** a registered device sends a heartbeat and the control plane accepts it
- **THEN** the device's `devices` document carries a last-seen timestamp at or after the
  acceptance time and is still the only document for that identity

#### Scenario: Firmware change is picked up from a heartbeat
- **WHEN** a registered device reports a `current_fw` different from the recorded one
- **THEN** the device document records the reported firmware version

#### Scenario: Heartbeat from an unregistered device changes nothing
- **WHEN** a heartbeat arrives for a device that never completed an accepted registration
- **THEN** no device record is created or updated for it

### Requirement: Last-seen timestamp semantics
The device record's `last_heartbeat` field SHALL hold the time the control plane last heard
from the device: seeded when a registration exchange is accepted and refreshed by every
accepted heartbeat. The field SHALL be present on every device document (a device seen only
through a registration is stored with the registration's acceptance time), so staleness
judgements never depend on a missing field.

#### Scenario: Registration seeds the last-seen timestamp
- **WHEN** a device record is created from an accepted registration
- **THEN** its `last_heartbeat` equals the registration acceptance time

#### Scenario: Last seen survives status changes
- **WHEN** the device is later marked offline for staleness
- **THEN** its `last_heartbeat` still records the last time it was heard from and is not reset
  by the status change
