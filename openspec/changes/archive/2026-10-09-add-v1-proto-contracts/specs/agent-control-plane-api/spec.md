# Spec Delta

## Purpose

Defines the v1 wire contract between device agents and the FleetOps control plane: the
`AgentService` streaming and unary RPCs, the message vocabulary for heartbeats, commands, and the
device-registration, firmware-download, and update-status exchanges, and the compatibility rules
the frozen v1 contract evolves under.

## ADDED Requirements

### Requirement: Bidirectional agent stream
The contract SHALL define `AgentService.Connect` as a bidirectional streaming RPC carrying one
long-lived stream per agent. The agent-to-control-plane direction SHALL carry registration,
heartbeat, firmware-download, and update-status requests; the control-plane-to-agent direction
SHALL carry their responses and commands. Every request message SHALL carry a correlation id that
its response echoes, and every command SHALL carry its own command id, so exchanges on one stream
can be matched independently.

#### Scenario: One stream carries both directions
- **WHEN** an agent opens `AgentService.Connect`
- **THEN** heartbeats and requests travel agent-to-control-plane and responses and commands travel
  control-plane-to-agent over the same stream, each pair matchable by correlation id

#### Scenario: Stream reconnection after loss
- **WHEN** the stream drops and the agent reconnects
- **THEN** the agent re-registers on the new stream and in-flight exchanges not answered on the old
  stream are safe to retry, because requests are correlated rather than ordered assumptions

### Requirement: Heartbeat messages
The contract SHALL define a heartbeat message carrying at least: an event id that uniquely
identifies the heartbeat for idempotent ingestion, the device id, the firmware version the device
currently runs (`current_fw`), its reported status, the sample timestamp, and the per-sample
health metrics `cpu`, `mem`, and `health`.

#### Scenario: Heartbeat carries the telemetry fields
- **WHEN** an agent emits a heartbeat
- **THEN** the message carries event id, device id, current firmware, status, timestamp, and the
  `cpu`, `mem`, and `health` samples needed to produce one telemetry document

#### Scenario: Redelivered heartbeats are identifiable
- **WHEN** the same heartbeat is sent twice (for example after a reconnect)
- **THEN** both messages carry the same event id so downstream ingestion can treat the second as a
  no-op

### Requirement: Command exchange
The contract SHALL define a command message carrying a command id and a typed payload, with a v1
command set that at minimum starts a firmware update (naming the firmware id, version, and
checksum) and aborts a running update (naming a reason). Commands SHALL flow only
control-plane-to-agent on the stream.

#### Scenario: Command delivery is identifiable
- **WHEN** the control plane issues a firmware update command
- **THEN** the agent receives a command carrying a command id it can later reference when reporting
  the result

### Requirement: Device registration exchange
The contract SHALL define `RegisterDeviceRequest` and `RegisterDeviceResponse` messages. The
request SHALL carry the device id, `model`, `region`, and `current_fw` — the same identity fields
the device record stores — and the response SHALL carry whether registration was accepted and the
device's resulting status.

#### Scenario: Enrolling device registers on the stream
- **WHEN** an agent opens its stream and sends a registration request with device id, model,
  region, and current firmware
- **THEN** the control plane answers on the same correlation id with an acceptance decision and
  the resulting device status

#### Scenario: Incomplete registration is rejected
- **WHEN** a registration request misses any of the required identity fields
- **THEN** the response reports rejection and no device record is created

### Requirement: Firmware download exchange
The contract SHALL define `FirmwareDownloadRequest` and `FirmwareDownloadResponse` messages. The
request SHALL name the device id and the firmware id; the response SHALL carry the firmware id,
`version`, `checksum`, and the binary as bounded-size chunks with an offset and end-of-transfer
marker, so a single download never requires unbounded buffering. The checksum MUST be present
independently of the chunk stream so the agent can verify the assembled binary.

#### Scenario: Firmware is fetched in bounded chunks
- **WHEN** an agent requests a firmware id
- **THEN** it receives the firmware version and checksum plus chunked binary data with offsets,
  ending with an explicit end-of-transfer marker

#### Scenario: Unknown firmware is refused safely
- **WHEN** an agent requests a firmware id the control plane does not have
- **THEN** the exchange fails with a `NOT_FOUND` gRPC status whose message carries no internal
  details

### Requirement: Update status exchange
The contract SHALL define `UpdateStatusRequest` and `UpdateStatusResponse` messages. The request
SHALL carry the device id, firmware id, an update phase (at least downloading, applying,
rebooting, completed, failed, and rolled back), a progress percentage, and a free-text detail for
failures; the response SHALL acknowledge receipt.

#### Scenario: Update progress is reported and acknowledged
- **WHEN** an agent moves through firmware update phases
- **THEN** each status request names the phase and progress and receives an acknowledgment on its
  correlation id

#### Scenario: Failure detail travels with the status
- **WHEN** an update reaches the failed phase
- **THEN** the request carries the failure detail so the control plane can record why the device
  did not advance

### Requirement: Unary command-result reporting
The contract SHALL define `AgentService.Report` as a unary RPC whose request carries an idempotency
key, the command id being reported on, the device id, and the command outcome, and whose response
states whether the result was accepted. A repeated report bearing an idempotency key already seen
MUST NOT produce a second effect. A report without an idempotency key MUST be rejected with
`INVALID_ARGUMENT`.

#### Scenario: Duplicate command results are no-ops
- **WHEN** the same command result is reported twice with the same idempotency key
- **THEN** the first report is accepted and the second returns acceptance without re-triggering the
  command's effects

#### Scenario: Missing idempotency key is rejected
- **WHEN** a report arrives with an empty idempotency key
- **THEN** it fails with `INVALID_ARGUMENT` and is not processed

### Requirement: Safe error surface
Every RPC in the contract SHALL signal failures as gRPC status codes with operator-safe messages;
contract-level failures (unknown device or firmware, invalid fields, duplicate registration) MUST
NOT leak internal details such as stack traces, driver errors, or infrastructure topology.

#### Scenario: Errors carry codes, not internals
- **WHEN** any exchange in this contract fails for a contract-level reason
- **THEN** the client observes a documented status code and a message free of implementation
  internals

### Requirement: Frozen v1 evolution
The v1 contract SHALL freeze when this change lands: existing fields MUST keep their numbers and
types, and evolution is by adding fields (and messages, and RPCs) only. A change that renumbers or
rewrites an existing field is not an allowed v1 evolution.

#### Scenario: Adding a field keeps old clients working
- **WHEN** a new field is added to an existing v1 message
- **THEN** previously generated clients keep working against the updated server without
  regeneration failures or wire incompatibility

#### Scenario: Renumbering is rejected
- **WHEN** a proposed contract change reuses or reorders an existing field number
- **THEN** it is rejected as a v1 violation regardless of whether current code compiles
