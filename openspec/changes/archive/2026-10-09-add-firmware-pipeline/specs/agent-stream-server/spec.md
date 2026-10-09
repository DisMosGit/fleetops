# Spec Delta

## ADDED Requirements

### Requirement: Update status intake
The stream server SHALL accept `UpdateStatusRequest` reports for devices enrolled on the stream,
acknowledge each on its correlation id with `UpdateStatusResponse`, and forward every accepted
report to the device workflow so the reported update phase is recorded there. A report missing
its device id, firmware id, or phase — or naming a device not enrolled on that stream — SHALL be
answered with `accepted: false` instead of being forwarded, and MUST NOT fail the stream.

#### Scenario: Accepted report is acknowledged and forwarded
- **WHEN** an enrolled device reports an update phase with its progress
- **THEN** the report is acknowledged as accepted on its correlation id and the device workflow
  receives the reported phase

#### Scenario: Malformed report does not kill the stream
- **WHEN** a report arrives without a phase, or for a device not enrolled on that stream
- **THEN** the acknowledgment carries `accepted: false`, no workflow is signaled, and the stream
  keeps serving
