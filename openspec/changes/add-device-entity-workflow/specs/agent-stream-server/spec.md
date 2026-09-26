# Spec Delta

## ADDED Requirements

### Requirement: Command result intake via Report
The control plane SHALL accept `AgentService.Report` as the terminal result of one command: it
validates that the idempotency key, command id, device id, and outcome are present and usable
(a report missing any of them is rejected with `codes.InvalidArgument` and an operator-safe
message), and forwards the accepted result to the device workflow identified by the device id
so the workflow can conclude its pending command. A report repeating an idempotency key already
seen SHALL be accepted again and MUST NOT produce a second effect on device state. The response
SHALL state whether the result was accepted.

#### Scenario: Complete report is accepted and forwarded
- **WHEN** an agent reports a command result with idempotency key, command id, device id, and
  outcome
- **THEN** the response reports acceptance and the device workflow receives the command result
  for that device

#### Scenario: Incomplete report is rejected
- **WHEN** a report arrives missing the idempotency key, command id, device id, or outcome
- **THEN** the call fails with `codes.InvalidArgument` and an operator-safe message, and no
  command result is forwarded

#### Scenario: Repeated idempotency key is accepted with no second effect
- **WHEN** the same command result is reported twice with the same idempotency key
- **THEN** both calls are accepted and the device's state changes exactly once

### Requirement: Device workflow signaling on accepted heartbeats
Every heartbeat routed to the heartbeat sink SHALL additionally signal the device workflow
identified by the device id exactly once per received message, carrying the heartbeat's event
id, reported firmware version, and timestamps, so the workflow's authoritative state follows
the device. A redelivered heartbeat SHALL be signaled with its original event id (deduplication
is the workflow's contract), and a heartbeat from a device not enrolled on the stream SHALL
signal nothing.

#### Scenario: Routed heartbeats signal the device workflow
- **WHEN** an enrolled device sends a heartbeat that is routed to the sink
- **THEN** the device workflow for that device id receives one heartbeat signal carrying the
  heartbeat's event id

#### Scenario: Redelivered heartbeats keep their identity end to end
- **WHEN** a heartbeat is redelivered after an agent reconnect
- **THEN** the workflow receives the same event id as on the first delivery and the device state
  reflects one application

#### Scenario: Unenrolled heartbeats signal nothing
- **WHEN** a heartbeat arrives for a device not enrolled on that stream
- **THEN** no device workflow is signaled
