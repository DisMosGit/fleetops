# internal/ — library code

All FleetOps library code lives here; there is no exported library surface. One package per
capability:

- `agent/` — device-agent emulator: simulated fleet, heartbeat emission, bidi stream client
  with reconnect (landed); firmware apply joins with its stage
- `agentserver/` — control-plane side of the agent stream: session registry, heartbeat routing
  to the `HeartbeatSink` seam, command dispatch (`Hub.Send`) (landed)
- `temporal/` — workflows and activities: `DeviceWorkflow`, `RolloutWorkflow`, and
  `FirmwareWorkflow` with its upload steps (landed)
- `telemetry/` — RabbitMQ publisher/consumers, idempotent ingestion, DLQ handling (stage 3)
- `config/` — the single YAML configuration file: loading, defaults, validation (landed)
- `firmware/` — firmware registry: GridFS binaries with metadata-only records, upload
  validation, the upload's activity-callable steps, the spool an upload's binary waits in, and
  the `POST /api/firmwares` upload endpoint with its state query (landed)
- `health/` — liveness/readiness probes and dependency connectivity checks (landed)

`telemetry` still carries only its doc comments; its code lands with the stage named beside it.
The other packages are implemented. Tests live beside the code as `*_test.go`.
