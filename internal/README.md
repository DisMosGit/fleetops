# internal/ — library code

All FleetOps library code lives here; there is no exported library surface. One package per
capability:

- `agent/` — device-agent emulator: simulated fleet, heartbeat emission, bidi stream client
  with reconnect (landed); firmware apply joins with its stage
- `agentserver/` — control-plane side of the agent stream: session registry, heartbeat routing
  to the `HeartbeatSink` seam, command dispatch (`Hub.Send`) (landed)
- `temporal/` — workflows and activities: `DeviceWorkflow`, `RolloutWorkflow`, `FirmwareWorkflow`
  (stage 2)
- `telemetry/` — RabbitMQ publisher/consumers, idempotent ingestion, DLQ handling (stage 3)
- `config/` — the single YAML configuration file: loading, defaults, validation (landed)
- `firmware/` — firmware registry: GridFS binaries with metadata-only records, upload
  validation, and the `POST /api/firmwares` upload endpoint (landed)
- `health/` — liveness/readiness probes and dependency connectivity checks (landed)

`temporal` and `telemetry` still carry only their doc comments; their code lands with the stage
named beside them. The other packages are implemented. Tests live beside the code as
`*_test.go`.
