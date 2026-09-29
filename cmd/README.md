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
  `observability.health_addr`, and shut down gracefully on SIGINT/SIGTERM.
- `controlplane` additionally serves `AgentService` on `grpc.listen_addr` for the agent fleet:
  registrations, heartbeat routing to a logging sink (the telemetry pipeline takes over at
  stage 3), and command dispatch through the `agentserver.Hub` seam. On `-http-addr` it serves
  the operator's HTTP gateway: the firmware upload API and the rollout API, each under its own
  path prefix (see [The operator HTTP API](#the-operator-http-api)). The gateway's SSE routes
  join the same listener at stage 5.
- `worker` hosts the device entity and the canary rollout on `temporal.task_queue`. For the
  entity it registers `DeviceWorkflow` and the `snapshot-device-state` activity (the projection of
  device state into `device_state_snapshots`); for a rollout it registers `rollout-workflow` and
  the six activities a rollout's side effects need — `load-firmware`, `resolve-wave-targets`,
  `record-rollout-state`, `record-wave-state`, `update-device`, and `evaluate-wave-health` — all
  under their explicit names, plus the namespace's custom search attributes (`DeviceRegion`,
  `DeviceModel`, `DeviceFirmware`, `DeviceOnline`, `RolloutFirmware`, `RolloutRegion`,
  `RolloutStatus`) before it starts polling, so device and rollout runs are filterable in the
  Temporal UI. It holds no replica-local state: run it as one process or as many replicas on the
  same task queue — any replica executes any workflow or activity task, and replicas may start and
  stop freely. `update-device` runs one activity per target device: it signals that device's
  workflow, then waits for the device's reported result by reading its state query, so the
  worker needs no agent connection. The `dispatch-command` activity is not registered here: it
  runs in `controlplane`, beside the agent hub it dispatches through. On one machine, give each
  replica its own `observability.health_addr` and `observability.metrics_addr`; in the cluster each
  pod has its own.

## Starting a rollout

A rollout is started over the operator HTTP API ([below](#starting-a-rollout-1)) or, for a scripted
start, through the starter in `internal/temporal`, which is the entry point both use:

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
- `rollout.result_timeout` — how long a wave waits for one target device's **reported** update
  result before that device counts as unreported and the wave stops waiting on it, measured from
  the wave's dispatch. It is what makes a wave's completion rest on results rather than on
  delivery: the wave is not gated until every device has settled — reported success, reported
  failure, or run out of time. It must not exceed `rollout.decision_timeout`, the deadline at
  which the wave's verdict is already due. The default five minutes matches the health window, so
  a healthy wave's length is unchanged in the normal case.
- `rollout.decision_timeout` — how long a wave may stay undecided before it counts as unhealthy,
  measured from the wave's start. A silent fleet therefore cannot hold a rollout open forever.
- `rollout.min_success_ratio`, `rollout.min_samples`, `rollout.sample_health_threshold` — the
  promotion boundary: a wave is promoted only on a decided verdict at or above the minimum ratio,
  and a verdict needs at least `min_samples` heartbeat samples to be decided at all.

A device that fails or never reports its update is recorded on the wave document
(`failed_device_ids`, `unreported_device_ids`) and counted in the state query, but the promotion
decision stays the gate's: the configured ratio decides a wave, not a device count. A command that
cannot be **delivered** to a device at all is different — it fails the wave and rolls the rollout
back, because a wave the rollout could not command is not evidence of health.

An operator approves a waiting wave by signalling the workflow (Temporal CLI, or the UI):

```sh
temporal workflow signal --workflow-id rollout-ro-2026-01-02 --name approve_next_wave
```

One approval authorizes one gated wave; an approval that arrives before the gate is reached is
held and spent on the next wave that requires one.

An operator holds a running rollout with `pause_rollout` and continues it with `resume_rollout`:

```sh
temporal workflow signal --workflow-id rollout-ro-2026-01-02 --name pause_rollout
temporal workflow signal --workflow-id rollout-ro-2026-01-02 --name resume_rollout
```

A pause takes effect immediately: the rollout starts no further wave, resolves no membership, and
dispatches nothing until it is resumed, and its status reports `paused`. A wave already in flight
is still driven to its recorded decision and a regression still rolls the rollout back, because a
pause holds promotion and never safety. Recorded wave outcomes, promoted waves, and a banked
approval all survive the pause. Repeated signals change nothing, and both are ignored once the
rollout has concluded.

To watch a rollout, query its state or read the `rollouts` and `waves` documents (see
[deploy/README](../deploy/README.md)):

```sh
temporal workflow query --workflow-id rollout-ro-2026-01-02 --type get-rollout-state
```
