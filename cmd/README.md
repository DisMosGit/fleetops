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
  the nine activities a rollout's side effects need — `load-firmware`, `resolve-wave-targets`,
  `record-rollout-state`, `record-wave-state`, `update-device`, `downgrade-device`,
  `reconcile-device-inventory`, `announce-rollback`, and `evaluate-wave-health` — all under their
  explicit names, plus the namespace's custom search attributes (`DeviceRegion`, `DeviceModel`,
  `DeviceFirmware`, `DeviceOnline`, `RolloutFirmware`, `RolloutRegion`, `RolloutStatus`) before it
  starts polling, so device and rollout runs are filterable in the Temporal UI. It holds no
  replica-local state: run it as one process or as many replicas on the same task queue — any
  replica executes any workflow or activity task, and replicas may start and stop freely.
  `update-device` and `downgrade-device` run one activity per target device: each signals that
  device's workflow, then waits for the device's reported result by reading its state query, so
  the worker needs no agent connection. `reconcile-device-inventory` reconciles one device's
  recorded `current_fw` against the version that device's workflow holds. The `dispatch-command`
  activity is not registered here: it runs in `controlplane`, beside the agent hub it dispatches
  through. On one machine, give each replica its own `observability.health_addr` and
  `observability.metrics_addr`; in the cluster each pod has its own.
- `worker` also hosts the broker publisher a rollback's announcements are published through:
  `announce-rollback` runs here, so the publisher is started with the worker (before it polls) and
  keeps a RabbitMQ session for the worker's lifetime, reconnecting after an outage without a
  worker restart. A broker that is unreachable at startup does not stop the worker polling — an
  announcement fails after its short retry budget and the rollback continues. The worker declares
  the same broker layout `controlplane` does, including the `fleetops.rollout.notifications`
  queue a rollback's events are enqueued on (see [docs/telemetry.md](../docs/telemetry.md)).

## Rolling back

A rollout rolls back on its own when a wave fails: an unhealthy verdict, an undecided wave past its
decision timeout, or a wave whose update commands could not be delivered. Entering rollback derives
the plan of compensations from the rollout's own recorded progress and runs it in order — the
`rollout.notification.rollback.started` announcement, one `downgrade-device` per device of each
dispatched wave with the most recently dispatched first, `reconcile-device-inventory` per device the
plan compensates, and the `rollout.notification.rollback.completed` announcement — after which the
rollout records its terminal `rolled_back` status. While the plan runs the rollout reports
`rolling_back`, and a pause, a resume, or an approval delivered then changes nothing: a
compensation is not interruptible.

Reading the rollout's state query while it compensates or after it concluded:

```json
{
  "rollout_id": "ro-2026-01-02",
  "status": "rolling_back",
  "firmware_id": "fw-1a2b3c",
  "firmware_version": "2.0.0",
  "region": "eu-west",
  "model": "oak-s3",
  "waves": [{"percent": 1, "status": "healthy", "target_count": 10, "failed_count": 0, "unreported_count": 0}],
  "current": -1,
  "outcome": "unhealthy_wave",
  "ended_by": "ro-2026-01-02-w1-5",
  "rollback": {
    "plan": [
      {"kind": "notify_started", "status": "completed", "devices": 0},
      {"kind": "downgrade", "wave_id": "ro-2026-01-02-w1-5", "status": "running", "devices": 40,
       "restored": 0, "failed": 0, "unreported": 0, "skipped": 0, "unavailable": 0},
      {"kind": "reconcile_inventory", "status": "pending", "devices": 50}
    ],
    "unrestored_device_ids": []
  }
}
```

`rollback.inventory` fills in once the reconciliation has run — the number of devices the rollback
touched that were found on each firmware version — and `rollback.unrestored_device_ids` names the
devices left on the rolled-back firmware: the ones that reported a failed restore, never reported,
or could not be restored at all. A device that never reported is not lost work: its device workflow
keeps the restore command pending and delivers it when the agent returns. The same record is written
to the rollout document as it compensates.

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

## The operator HTTP API

`controlplane -http-addr :8080` serves two APIs on one listener: the firmware upload API under
`/api/firmwares` and the rollout API under `/api/rollouts`. Each route is a method-scoped pattern,
so a method that does not match its route is answered `405 Method Not Allowed` and a path that
matches no route is answered `404 Not Found`; every JSON rejection carries an operator-safe
`{"error": "..."}` body, and a backend failure is logged at the boundary and answered
`{"error": "internal error"}` rather than echoing driver or server internals.

Every rollout call is bounded: the API gives the workflow backend a few seconds and answers a
failure rather than holding an operator's request open behind a query nothing is running.

| Route | Purpose |
|---|---|
| `POST /api/rollouts` | start a rollout |
| `GET /api/rollouts/{id}` | read a rollout's current state |
| `POST /api/rollouts/{id}/approve` | authorize the next approval-gated wave |
| `POST /api/rollouts/{id}/pause` | hold the rollout |
| `POST /api/rollouts/{id}/resume` | continue a held rollout |

A rollout that has entered rollback ignores `approve`, `pause`, and `resume`: its compensations are
not interruptible, and the call is still answered `202` because the rollout is not concluded (see
[Rolling back](#rolling-back)).

### Starting a rollout

```sh
curl -sS -X POST localhost:8080/api/rollouts -H 'Content-Type: application/json' -d '{
  "rollout_id":  "ro-2026-01-02",
  "firmware_id": "fw-1a2b3c",
  "region":      "eu-west",
  "model":       "oak-s3"
}'
```

```json
{"rollout_id":"ro-2026-01-02","workflow_id":"rollout-ro-2026-01-02","status":"running"}
```

The **rollout id is the request's idempotency key**: it determines the workflow identity, so a
retried start is refused rather than beginning a second run chain beside the first one's records.
A client that wants to know whether its first attempt landed reads `409` as "already started" and
then reads the state.

The endpoint deliberately does not verify the firmware. A firmware that is unknown, or one that
does not target the selector's model, is the rollout's own recorded failure — a `failed` rollout
with `firmware_unknown` or `firmware_mismatch` — not a start refusal, because the load-firmware
activity is the single decider and its decision is what an operator reads.

| Status | Meaning |
|---|---|
| `202 Accepted` | the rollout was started; the body names it, its workflow id, and its status |
| `400 Bad Request` | the body is not valid JSON, carries an unknown field, or leaves `rollout_id`, `firmware_id`, `region`, or `model` empty |
| `409 Conflict` | that rollout id already has a workflow execution |
| `500 Internal Server Error` | the backend could not be reached |
| `405 Method Not Allowed` | the route exists under another method |

### Reading a rollout's state

```sh
curl -sS localhost:8080/api/rollouts/ro-2026-01-02
```

The response is the rollout workflow's own state view, verbatim — the same answer the
`get-rollout-state` query gives, so the API cannot drift from what the rollout reports:

```json
{
  "rollout_id": "ro-2026-01-02",
  "status": "running",
  "firmware_id": "fw-1a2b3c",
  "firmware_version": "2.0.0",
  "region": "eu-west",
  "model": "oak-s3",
  "waves": [
    {"percent": 25, "status": "healthy", "success_rate": 0.99, "target_count": 4,
     "failed_count": 0, "unreported_count": 0},
    {"percent": 100, "status": "evaluating", "success_rate": 0, "target_count": 12,
     "failed_count": 1, "unreported_count": 2}
  ],
  "current": 1,
  "approval_outstanding": false
}
```

`status` is the [rollout lifecycle status](../deploy/README.md#rollout-lifecycle-statuses), and
`firmware_version` is empty until the rollout has loaded its firmware's metadata. A concluded
rollout adds `outcome`, `ended_by`, and the `decision` its failing wave was measured on. The state
is read from the workflow execution rather than from the `rollouts` document, so a wave in flight
is reported as it stands.

| Status | Meaning |
|---|---|
| `200 OK` | the body is the rollout's current state |
| `404 Not Found` | no workflow execution exists for that rollout id |
| `500 Internal Server Error` | the state could not be read |

### Sending a command

```sh
curl -sS -X POST localhost:8080/api/rollouts/ro-2026-01-02/approve
curl -sS -X POST localhost:8080/api/rollouts/ro-2026-01-02/pause
curl -sS -X POST localhost:8080/api/rollouts/ro-2026-01-02/resume
```

```json
{"rollout_id":"ro-2026-01-02","signal":"pause_rollout"}
```

Each command delivers the workflow's own signal — `approve_next_wave`, `pause_rollout`, or
`resume_rollout` — so the HTTP surface and the Temporal CLI drive a rollout the same way, and the
signal names in the response are the ones the workflow registers. A command on a rollout that has
already concluded is refused: the endpoint reads the state first, so a concluded rollout answers
`409` instead of accepting a signal that could no longer change anything.

| Status | Meaning |
|---|---|
| `202 Accepted` | the signal was delivered; the body names the rollout and the signal |
| `404 Not Found` | no workflow execution exists for that rollout id |
| `409 Conflict` | the rollout has already completed, rolled back, or failed |
| `500 Internal Server Error` | the signal could not be delivered |
| `405 Method Not Allowed` | the route exists under another method |

## Filtering runs in the Temporal UI

The worker's startup bootstrap registers the namespace's custom search attributes before it polls,
so every run is filterable from the first workflow task: an attribute that does not exist in the
namespace is created, an attribute that already exists is left unchanged, and several replicas
starting at once is harmless. Device runs carry:

| Attribute | Type | Mirrors |
|---|---|---|
| `DeviceRegion` | Keyword | the device's registered region |
| `DeviceModel` | Keyword | the device's registered model |
| `DeviceFirmware` | Keyword | the firmware version the device runs |
| `DeviceOnline` | Bool | whether a heartbeat arrived within `liveness.offline_threshold` |

Rollout runs carry:

| Attribute | Type | Mirrors |
|---|---|---|
| `RolloutFirmware` | Keyword | the version of the firmware the rollout deploys; **empty until its metadata has been loaded** |
| `RolloutRegion` | Keyword | the target selector's region |
| `RolloutStatus` | Keyword | the rollout's lifecycle status (`running`, `paused`, `awaiting_approval`, `rolled_back`, `completed`, `failed`) |

Both sets are derived from workflow state by one function per workflow family and upserted as the
state changes, so a filter can never disagree with the run. A UI (or CLI) list query selects runs
by any combination of them, for example every running canary of firmware `2.0.0` in `eu-west`:

```
RolloutFirmware = "2.0.0" AND RolloutRegion = "eu-west" AND RolloutStatus = "running"
```

`RolloutFirmware` holds an empty value for a rollout whose firmware metadata never loaded (an
unknown firmware, or one that does not target the selector's model): the version is not knowable
from the start input, and such a rollout is findable by region and status — as `failed` — rather
than by a version it never had.

- `agent` runs `simulation.fleet_size` simulated devices against `grpc.control_plane_addr`:
  periodic heartbeats (event id, firmware, status, cpu/mem/health) over one multiplexed
  `AgentService.Connect` stream that re-registers and reconnects with capped exponential
  backoff, and stops cleanly on SIGINT/SIGTERM.
