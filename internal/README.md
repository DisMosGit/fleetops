# internal/ — library code

All FleetOps library code lives here; there is no exported library surface. One package per
capability:

- `agent/` — device-agent emulator: heartbeat, bidi stream client, firmware apply (stages 1–3)
- `temporal/` — workflows and activities: `DeviceWorkflow`, `RolloutWorkflow`, `FirmwareWorkflow`
  (stage 2)
- `telemetry/` — RabbitMQ publisher/consumers, idempotent ingestion, DLQ handling (stage 3)

**Not implemented yet** — each package currently carries only its doc comment; its code lands
with the stage named above. Tests live beside the code as `*_test.go`.
