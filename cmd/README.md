# cmd/ — binary entrypoints

One thin entrypoint package per FleetOps binary:

- `controlplane/` — control plane: gRPC agent server + HTTP/SSE gateway (stage 1)
- `worker/` — Temporal workers hosting the workflows and activities (stage 2)
- `agent/` — device-agent emulator running N simulated devices per process (stage 1)

Every entrypoint takes `-config <path>` pointing at the single YAML configuration file
([sample](../deploy/config.yaml)); omitting the flag uses the built-in local-stack defaults, and
an invalid file stops startup with an error naming the offending field. All behavior lives in
`internal/` packages so it is testable without a running binary; entrypoints only parse flags,
construct dependencies, and start the run loop.

**Partially implemented.** What runs today:

- `controlplane` and `worker` serve `GET /healthz` (liveness) and `GET /readyz` (readiness:
  MongoDB, RabbitMQ, and Temporal connectivity, per-dependency in the body) on
  `observability.health_addr`, and shut down gracefully on SIGINT/SIGTERM. The gRPC agent
  server, the HTTP/SSE gateway (`-http-addr`), and the Temporal workers are still stage-gated
  TODOs.
- `agent` loads the configuration and hands `simulation.fleet_size` and
  `grpc.control_plane_addr` to its emulator startup path, then exits `not implemented yet`
  until the emulation lands.
