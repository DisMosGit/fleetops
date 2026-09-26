# internal/ — library code

All FleetOps library code lives here; there is no exported library surface. One package per
capability:

- `agent/` — device-agent emulator: heartbeat, bidi stream client, firmware apply (stages 1–3)
- `temporal/` — workflows and activities: `DeviceWorkflow`, `RolloutWorkflow`, `FirmwareWorkflow`
  (stage 2)
- `telemetry/` — RabbitMQ publisher/consumers, idempotent ingestion, DLQ handling (stage 3)
- `config/` — the single YAML configuration file: loading, defaults, validation (landed)
- `health/` — liveness/readiness probes and dependency connectivity checks (landed)

The three stage-gated packages above carry only their doc comments; their code lands with the
stage named beside them. `config` and `health` are implemented. Tests live beside the code as
`*_test.go`.
