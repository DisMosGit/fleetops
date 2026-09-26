# cmd/ — binary entrypoints

One thin entrypoint package per FleetOps binary:

- `controlplane/` — control plane: gRPC agent server + HTTP/SSE gateway (stage 1)
- `worker/` — Temporal worker hosting the device entity workflow and its activities
- `agent/` — device-agent emulator running N simulated devices per process (stage 1)

Every entrypoint takes `-config <path>` pointing at the single YAML configuration file
([sample](../deploy/config.yaml)); omitting the flag uses the built-in local-stack defaults, and
an invalid file stops startup with an error naming the offending field. All behavior lives in
`internal/` packages so it is testable without a running binary; entrypoints only parse flags,
construct dependencies, and start the run loop.

**Partially implemented.** What runs today:

- `controlplane` and `worker` serve `GET /healthz` (liveness) and `GET /readyz` (readiness:
  MongoDB, RabbitMQ, and Temporal connectivity, per-dependency in the body) on
  `observability.health_addr`, and shut down gracefully on SIGINT/SIGTERM. The HTTP/SSE
  gateway (`-http-addr`) is still a stage-gated TODO.
- `controlplane` additionally serves `AgentService` on `grpc.listen_addr` for the agent fleet:
  registrations, heartbeat routing to a logging sink (the telemetry pipeline takes over at
  stage 3), and command dispatch through the `agentserver.Hub` seam.
- `worker` hosts the device entity on `temporal.task_queue`: it registers `DeviceWorkflow` and
  the `snapshot-device-state` activity (the projection of device state into
  `device_state_snapshots`) under their explicit names, and registers the namespace's custom
  search attributes (`DeviceRegion`, `DeviceModel`, `DeviceFirmware`, `DeviceOnline`) before
  it starts polling, so device runs are filterable in the Temporal UI. It holds no
  replica-local state: run it as one process or as many replicas on the same task queue —
  any replica executes any workflow or activity task, and replicas may start and stop freely.
  The `dispatch-command` activity is not registered here: it runs in `controlplane`, beside
  the agent hub it dispatches through. On one machine, give each replica its own
  `observability.health_addr` and `observability.metrics_addr`; in the cluster each pod has
  its own.
- `agent` runs `simulation.fleet_size` simulated devices against `grpc.control_plane_addr`:
  periodic heartbeats (event id, firmware, status, cpu/mem/health) over one multiplexed
  `AgentService.Connect` stream that re-registers and reconnects with capped exponential
  backoff, and stops cleanly on SIGINT/SIGTERM.
