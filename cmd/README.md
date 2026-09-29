# cmd/ — binary entrypoints

One thin entrypoint package per FleetOps binary:

- `controlplane/` — control plane: gRPC agent server + HTTP/SSE gateway (stage 1)
- `worker/` — Temporal worker hosting the device entity workflow, the rollout workflow, and
  their activities
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
- `worker` hosts the device entity and the canary rollout on `temporal.task_queue`. For the
  entity it registers `DeviceWorkflow` and the `snapshot-device-state` activity (the projection of
  device state into `device_state_snapshots`); for a rollout it registers `rollout-workflow` and
  the six activities a rollout's side effects need — `load-firmware`, `resolve-wave-targets`,
  `record-rollout-state`, `record-wave-state`, `dispatch-wave-update`, and
  `evaluate-wave-health` — all under their explicit names, plus the namespace's custom search
  attributes (`DeviceRegion`, `DeviceModel`, `DeviceFirmware`, `DeviceOnline`) before it starts
  polling, so device runs are filterable in the Temporal UI. It holds no replica-local state: run
  it as one process or as many replicas on the same task queue — any replica executes any workflow
  or activity task, and replicas may start and stop freely. The `dispatch-command` activity is not
  registered here: it runs in `controlplane`, beside the agent hub it dispatches through. On one
  machine, give each replica its own `observability.health_addr` and `observability.metrics_addr`;
  in the cluster each pod has its own.

## Starting a rollout

A rollout is started programmatically until its HTTP surface lands (roadmap item 39). The starter
in `internal/temporal` is the entry point:

```go
starter := temporal.NewRolloutStarter(temporalClient, cfg.Temporal.TaskQueue, rolloutSettings)
err := starter.Start(ctx, temporal.RolloutRequest{
    RolloutID:  "ro-2026-01-02",   // names the workflow execution: rollout-<rollout_id>
    FirmwareID: "fw-1a2b3c",       // must target the selector's model, or the rollout fails
    Region:     "eu-west",
    Model:      "oak-s3",
})
```

`cmd/worker`'s `rolloutSettings` is what maps the configured `rollout` section onto the policy a
rollout drives under, so the sequence an operator configures is the sequence the workflow drives.
What those settings mean:

- `rollout.waves` — the canary sequence, in order. Each entry's `percent` is a **cumulative** share
  of the rollout's eligible pool (the devices matching the region and model), so a wave targets the
  devices its share adds beyond the shares before it: with the default 1, 5, 25, 100 over a
  1000-device pool the waves target 10, 40, 200, and the remaining 750 devices, and no device is
  commanded twice. `require_approval` makes a wave wait for an operator's `approve_next_wave`
  signal before it is resolved; the default gates the 25% and 100% waves.
- `rollout.health_window` — how long each dispatched wave is held open before its health is judged.
  The wait is a durable timer anchored at the wave's recorded start, so a worker restart resumes
  the remainder instead of restarting or shortening it.
- `rollout.decision_timeout` — how long a wave may stay undecided before it counts as unhealthy,
  measured from the wave's start. A silent fleet therefore cannot hold a rollout open forever.
- `rollout.min_success_ratio`, `rollout.min_samples`, `rollout.sample_health_threshold` — the
  promotion boundary: a wave is promoted only on a decided verdict at or above the minimum ratio,
  and a verdict needs at least `min_samples` heartbeat samples to be decided at all.

An operator approves a waiting wave by signalling the workflow (Temporal CLI, or the UI):

```sh
temporal workflow signal --workflow-id rollout-ro-2026-01-02 --name approve_next_wave
```

One approval authorizes one gated wave; an approval that arrives before the gate is reached is
held and spent on the next wave that requires one. To watch a rollout, query its state or read the
`rollouts` and `waves` documents (see [deploy/README](../deploy/README.md)):

```sh
temporal workflow query --workflow-id rollout-ro-2026-01-02 --type get-rollout-state
```

- `agent` runs `simulation.fleet_size` simulated devices against `grpc.control_plane_addr`:
  periodic heartbeats (event id, firmware, status, cpu/mem/health) over one multiplexed
  `AgentService.Connect` stream that re-registers and reconnects with capped exponential
  backoff, and stops cleanly on SIGINT/SIGTERM.
