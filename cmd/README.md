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
  join the same listener at stage 5. It polls `temporal.dispatch_task_queue` and registers
  `dispatch-command` there, together with the firmware upload's workflow and activities — see
  [Task queues](#task-queues) and [Uploading a firmware](#uploading-a-firmware).
- `controlplane` also owns firmware uploads. `POST /api/firmwares` spools the request's binary to
  `firmware.spool_dir` and starts (or attaches to) `firmware-workflow` under the version's
  identity `firmware-upload-<version>`; the workflow drives `validate-firmware-models`,
  `store-firmware-binary`, `upsert-firmware-metadata`, and — when an upload stored a binary it
  cannot record — `discard-firmware-upload` as activities, all on `temporal.dispatch_task_queue`,
  and the endpoint answers with the upload's outcome. The spool file is removed once the upload
  concludes, except when the upload outlives `firmware.wait_timeout` and keeps streaming; files a
  crash abandoned are swept at startup once they are older than `firmware.max_spool_age`. The
  worker registers none of it: the binary arrives in this process and the store binding the
  upload's steps write through is here.
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
  through, and no worker is ever registered on `temporal.dispatch_task_queue` (see
  [Task queues](#task-queues)). On one machine, give each replica its own
  `observability.health_addr` and `observability.metrics_addr`; in the cluster each pod has its
  own.
- `worker` also hosts the broker publisher a rollback's announcements are published through:
  `announce-rollback` runs here, so the publisher is started with the worker (before it polls) and
  keeps a RabbitMQ session for the worker's lifetime, reconnecting after an outage without a
  worker restart. A broker that is unreachable at startup does not stop the worker polling — an
  announcement fails after its short retry budget and the rollback continues. The worker declares
  the same broker layout `controlplane` does, including the `fleetops.rollout.notifications`
  queue a rollback's events are enqueued on (see [docs/telemetry.md](../docs/telemetry.md)).
- `worker` keeps the eligible-device view its wave resolutions read, fed by the `devices` change
  stream (see [The eligible-device view](#the-eligible-device-view)). The stream is established
  before the worker polls its first task and lives for the worker's lifetime; a stream that cannot
  be established fails the startup. Change streams need a replica set, so this is the one
  environment requirement the worker adds to MongoDB: a standalone cannot run it.

## Task queues

The two binaries poll two different Temporal task queues, and neither polls the other's:

| Queue | Configured by | Polled by | Task types that run on it |
|---|---|---|---|
| `temporal.task_queue` (`fleetops`) | `-config` | `worker` replicas only | `device-workflow`, `snapshot-device-state`, `rollout-workflow`, and the nine rollout activities |
| `temporal.dispatch_task_queue` (`fleetops-controlplane`) | `-config` | `controlplane` only | `dispatch-command`, `firmware-workflow`, `validate-firmware-models`, `store-firmware-binary`, `upsert-firmware-metadata`, `discard-firmware-upload` |

The split is not cosmetic. Temporal delivers a task to **any** poller of its queue, not to a poller
that registered that task type — a process handed a type it does not host fails it as unknown and
the task is redelivered with retry backoff until a process that hosts it picks it up. So the rule
each binary follows is: poll a queue only if you register every task type that queue can deliver.
`worker` registers the whole work queue and polls only it; `controlplane` registers
`dispatch-command` and the firmware upload's workflow and activities, and polls only the queue that
carries them. A workflow or activity added to the work queue later is therefore delivered to the
workers by construction, with nothing to remember in the control plane.

Where a task runs is decided by where its **side effect** lives, not by tidiness:

- `dispatch-command` sends a command down a device's live agent stream, and those streams are the
  `agentserver.Hub` inside the control-plane process — so the activity is scheduled on the
  control-plane queue and executed there. A device workflow schedules it explicitly, on the queue
  carried in its entity settings, rather than on the queue the workflow itself runs on.
- The firmware upload's workflow and activities run in `controlplane` for the same rule in the
  other direction: the binary arrives there as an HTTP request body, so the storage step's side
  effect — the stream from the spooled file into GridFS — is there, and the store binding its
  metadata write needs is there. The workflow runs on the control-plane queue too, so its
  activities inherit it and no upload task can be delivered to a worker that hosts none of them.
  The worker registers neither, and the upload's document is the only trace of it the worker's
  rollout reads.
- Every rollout activity, including `announce-rollback`, runs in `worker`, because the publisher a
  rollback's announcement goes through is hosted by the process that runs the rollback.

Both binaries read the same configuration file and **must be deployed together**. A control plane on
the new build polls only the control-plane queue, so a worker still on the old build would schedule
`dispatch-command` onto the work queue, where nothing serves it. Rollback is the same pairing in
reverse: revert both binaries together.

Device commands and the device entity meet across the two queues: the workflow runs on the work
queue, its `dispatch-command` task goes to the control-plane queue, and the device's report comes
back to the workflow as a signal. The device's run chain is started with the dispatch queue name in
its settings, so a chain schedules its commands consistently for its whole life. A run chain created
before this split carries a settings payload without the queue and is refused loudly rather than
running half-understood; on the local dev stack the migration is to reset those chains —
`temporal workflow terminate --workflow-id device-<device_id>`, after which the device's next
signal recreates its chain at the current carry version with the dispatch queue in its settings.

## The eligible-device view

A wave's target set is resolved from the eligible pool of its selector — a region and a device
model. The worker keeps that pool in a view keyed by selector and ordered by device identity, and
resolves waves against the view instead of querying `devices` on every resolution:

- **One query per selector, once.** A selector's entry is built lazily at its first resolution,
  with a single ordered read of `devices` filtered by `{region, model}`. The order is device
  identity, so a pool and a set of shares always resolve to the same target sets.
- **Kept current by the fleet's own writes.** An entry is refreshed from the `devices` change
  stream. The stream applies a change in memory and never reads `devices`, so learning what changed
  costs no query. A burst of changes is coalesced: the events the stream already holds are drained
  together, and each affected selector's entry is rewritten once for the batch. A device that
  changes model or region moves between entries, a device that leaves the fleet leaves them, and a
  change applied twice changes nothing beyond the first application.
- **The stream's position is kept.** A lost stream is re-established from the position it reached,
  with capped backoff, so a brief interruption loses nothing. A position the server will not serve
  — the oplog has rolled past it — drops the whole view, which then rebuilds from `devices` rather
  than answering from data older than the collection; the stream continues from a fresh position.
- **Startup fails when the stream cannot be established.** A worker whose view is not being
  maintained has no freshness guarantee and nothing downstream can tell, so the run stops with the
  reason instead of degrading silently. The watcher is stopped by the worker's context and releases
  its cursor.

`devices` stays the only source of truth: the view is derived, holds nothing that cannot be
recomputed from the collection, and losing it costs one query per selector — never information.

Change streams are served by replica set members only, which is an **environment requirement** the
worker adds: a standalone MongoDB cannot run the worker. [deploy/README.md](../deploy/README.md)
documents it for the local stack; `internal/mongotest` starts a single-member replica set with the
same schema bootstrap for the tests that need a change stream.

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
| `POST /api/firmwares` | upload one firmware binary with its metadata |
| `GET /api/firmwares/{version}` | read the state of one version's upload |
| `POST /api/rollouts` | start a rollout |
| `GET /api/rollouts/{id}` | read a rollout's current state |
| `POST /api/rollouts/{id}/approve` | authorize the next approval-gated wave |
| `POST /api/rollouts/{id}/pause` | hold the rollout |
| `POST /api/rollouts/{id}/resume` | continue a held rollout |

A rollout that has entered rollback ignores `approve`, `pause`, and `resume`: its compensations are
not interruptible, and the call is still answered `202` because the rollout is not concluded (see
[Rolling back](#rolling-back)).

### Uploading a firmware

```sh
curl -sS -X POST localhost:8080/api/firmwares \
  -F version=2.0.0 -F models=oak-s3,birch-x1 -F binary=@firmware-2.0.0.bin
```

```json
{"id":"fw-1a2b3c","version":"2.0.0","models":["oak-s3","birch-x1"],"checksum":"9f2c…","size":1048576}
```

**The version is the upload's identity.** `POST /api/firmwares` spools the request's binary under
`firmware.spool_dir` and starts the workflow `firmware-workflow` under the id
`firmware-upload-<version>` on `temporal.dispatch_task_queue` (see [Task queues](#task-queues)).
The workflow owns the upload from acceptance to its outcome — it validates the target models
(`validate-firmware-models`), claims the version's firmware document, streams the spooled binary
into GridFS as its checksum is computed (`store-firmware-binary`), and records each state
transition on the document (`upsert-firmware-metadata`) — and the endpoint waits for that outcome
within `firmware.wait_timeout`. A second upload of the same version reaches the existing execution
instead of starting a second one, so a retried upload never stores a version twice: it is either
answered the conflict (another upload of that version is in flight, or one already concluded) or
the outcome the existing execution recorded.

An upload's state is a `state` field on its firmware document: `accepted`, `storing`, `available`,
or `failed`, with the owning workflow execution and, for a failure, an operator-safe reason. Read
it with the state query; it answers while the upload runs, so a caller whose request timed out —
or one that was refused because another upload holds the version — learns the outcome without
starting anything new:

```sh
curl -sS localhost:8080/api/firmwares/2.0.0
```

```json
{"version":"2.0.0","state":"available","concluded":true,"firmware_id":"fw-1a2b3c",
 "models":["oak-s3","birch-x1"],"checksum":"9f2c…","size":1048576}
```

The state query answers `200` with the state above while the upload runs or after it concluded,
`404` for a version with no upload at all — including one refused before it claimed a record — and
`500` when the state cannot be read.

| Status | Meaning |
|---|---|
| `201 Created` | the upload became available; the body is the stored firmware record |
| `400 Bad Request` | the request carries no `binary` part, no `version`, or no `models` field |
| `409 Conflict` | that version already has an upload: one is in flight, or one already concluded |
| `422 Unprocessable Entity` | a target model no registered device runs; nothing is stored |
| `504 Gateway Timeout` | the upload outlived `firmware.wait_timeout`; it continues durably, and `GET /api/firmwares/{version}` reports it |
| `500 Internal Server Error` | the backend could not be reached, or the upload failed for another reason |
| `405 Method Not Allowed` | the route exists under another method |

The upload's spooled binary lives in `firmware.spool_dir` (default `<os.TempDir()>/fleetops-firmware`)
only until its upload concludes; a control plane that dies mid-upload leaves the file behind, and
the next startup removes the files older than `firmware.max_spool_age` (default `1h`). Age is the
criterion, not presence: a spool file another replica is still uploading through is not this
replica's to delete.

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

## Agent emulator faults and download retry

`agent` downloads each dispatched firmware through the real downloader and retries a transient
download failure — a transfer fault or a stall — up to `simulation.faults.attempts` attempts in
total. The wait between attempts starts at 100 ms and doubles, never past
`simulation.faults.backoff_cap`. A failure a retry cannot clear — a checksum mismatch between the
transfer and the command, or a malformed transfer — fails the attempt at once without spending
the rest of the budget. Retrying wraps the download only: each attempt announces the downloading
phase again, and the command still produces exactly one terminal result.

A positive `simulation.faults.share` also puts the seeded fault injector in front of the
downloader, so the configured share of download attempts stalls or fails before any byte is
fetched. Decisions are drawn per device from the emulator's seed, so the same seed replays the
same faults and one device's faults never depend on another device's draws. The settings:

```yaml
simulation:
  faults:
    # Probability that one download attempt faults, in [0, 1]; the default injects none.
    share: 0.0
    # Fault kinds an injected attempt draws from: a stall, or an immediate transfer failure.
    kinds: ["stall", "transfer"]
    # How long an injected stall waits before its attempt fails (Go duration string), and
    # required only while "stall" is one of the kinds.
    stall_duration: "5s"
    # Total download attempts one update command may make; 0 or 1 makes a single attempt.
    attempts: 3
    # Upper bound on the wait between two download attempts (Go duration string).
    backoff_cap: "5s"
```

With the default `share: 0.0` the injector is not installed at all: every download takes the
real path, unchanged, under the retry budget above. Raise the share to make the configured
portion of the fleet's downloads stall or fail, which is what exercises the retry and the
failed-report path end to end.
