# Spec Delta

## Purpose

Defines the control-plane side of the `AgentService.Connect` bidirectional stream: accepting
agent connections, the registration exchange and its validation, routing inbound heartbeats to
the telemetry path, and dispatching commands back to a specific device over the same stream.

## ADDED Requirements

### Requirement: Accepting agent connections
The control plane SHALL accept one `AgentService.Connect` stream per agent and serve concurrent
streams from different agents independently. The devices of a stream SHALL become known only
through that stream's registration exchange, and when a stream ends its devices SHALL stop being
routable: a command for such a device fails visibly instead of being silently dropped or
buffered.

#### Scenario: Concurrent agents are served independently
- **WHEN** two agent processes open streams and register their devices
- **THEN** both conversations proceed independently on their own streams

#### Scenario: A dropped stream makes its devices unroutable
- **WHEN** an agent's stream ends
- **THEN** its devices are no longer routable and a command dispatched for one of them fails
  visibly

### Requirement: Registration exchange
On a registration request the control plane SHALL validate that the correlation id and the
device identity fields (device id, model, region, `current_fw`) are present, answer on the same
correlation id with the acceptance decision and resulting device status, and enroll the device
for heartbeat routing and command delivery only when accepted. A registration missing required
fields SHALL be rejected (not accepted) and SHALL enroll nothing.

#### Scenario: Complete registration is accepted
- **WHEN** an agent registers a device with correlation id and all identity fields
- **THEN** the response on that correlation id reports acceptance and the resulting device status,
  and the device becomes routable

#### Scenario: Incomplete registration enrolls nothing
- **WHEN** a registration request misses the correlation id or any identity field
- **THEN** it is not accepted and no device is enrolled

### Requirement: Heartbeat routing
Every heartbeat received from an enrolled device SHALL be routed, unchanged, to the heartbeat
sink that delivers it to the telemetry path — one routing per received message, with the event
id preserved so downstream ingestion deduplicates redeliveries. A heartbeat from a device that is
not enrolled on the stream SHALL NOT be routed.

#### Scenario: Enrolled heartbeats reach the sink unchanged
- **WHEN** an enrolled device sends a heartbeat
- **THEN** the sink receives it with the event id, device id, firmware version, status,
  timestamp, and metric samples intact

#### Scenario: Unenrolled heartbeats are not routed
- **WHEN** a heartbeat arrives for a device not enrolled on that stream
- **THEN** it is not routed to the sink

#### Scenario: Redelivered heartbeats keep their identity
- **WHEN** a heartbeat is redelivered after an agent reconnect
- **THEN** the sink receives the same event id as on the first delivery

### Requirement: Command dispatch over the stream
The control plane SHALL offer command dispatch to a named enrolled device, delivering the
command on that device's own stream with its command id and device id. Delivery SHALL use a
bounded per-session queue: a dispatch waits for queue space only until its own deadline expires
and then fails with an error — never unbounded buffering, never silent loss. Dispatching to a
device that is unknown or not connected SHALL fail with a not-found error naming no internals.

#### Scenario: A command reaches its target device
- **WHEN** a command is dispatched to an enrolled device
- **THEN** it arrives on that device's agent stream carrying its command id and device id

#### Scenario: Commands never cross agent streams
- **WHEN** a command is dispatched to a device behind agent A while agent B holds another stream
- **THEN** only agent A's stream carries the command

#### Scenario: Dispatch to an unknown device fails visibly
- **WHEN** a command is dispatched to a device that is unknown or disconnected
- **THEN** the dispatch returns a not-found error instead of dropping or buffering the command

#### Scenario: A congested session applies backpressure
- **WHEN** a device's outbound queue is full and the dispatch deadline expires
- **THEN** the dispatch fails with an error and the queue remains bounded

### Requirement: Safe stream error handling
Stream input the control plane cannot act on (an envelope with no payload or no correlation id
where one is required, an unknown payload) SHALL fail the call with a `codes.InvalidArgument`
status whose message carries no internal details. A panic in stream handling SHALL be recovered,
logged with its domain identifiers, and surfaced as a safe internal error rather than crashing
the process or leaking internals to the agent.

#### Scenario: Malformed input maps to InvalidArgument
- **WHEN** an envelope arrives that cannot be acted on
- **THEN** the call fails with `codes.InvalidArgument` and an operator-safe message

#### Scenario: A handler panic is contained
- **WHEN** stream handling panics
- **THEN** the panic is recovered and logged with the device and stream identity, the agent sees
  a safe internal error, and the control-plane process keeps serving other streams
