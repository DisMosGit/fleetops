# api/proto/ — Protobuf contracts

v1 `.proto` contracts for FleetOps' bounded contexts (one proto package per bounded context,
messages never shared across services) and the Go code generated from them. v1 freezes at the end
of stage 1; afterwards contracts evolve by adding fields, messages, and RPCs only — never
renumber, never rewrite.

## The v1 agent ↔ control-plane contract

`agent/v1/agent.proto` (package `fleetops.agent.v1`, import
`github.com/DisMosGit/fleetops/api/proto/agent/v1`) defines the whole agent-facing surface:

- **`AgentService.Connect`** — one long-lived bidirectional stream per agent. `AgentEnvelope`
  (agent → control plane) and `ControlEnvelope` (control plane → agent) each carry a `oneof`
  payload plus a correlation id: responses echo their request's id so concurrent exchanges on one
  stream never mismatch, and commands carry their own `command_id`.
- **`AgentService.Report`** — unary command-result reporting. `ReportRequest` carries an
  idempotency key (a redelivered result must be a no-op), the `command_id`, the `device_id`, and
  the terminal `CommandOutcome`.
- **`AgentService.DownloadFirmware`** — server-streaming firmware delivery. The stream opens
  with a metadata message (`version`, `checksum`, `total_size`, no chunk), carries the binary in
  bounded offset-ordered chunks, and closes with the `eof` marker. It reuses the firmware
  download messages of the Connect exchange; that exchange stays frozen and is not served.

Message vocabulary:

| Exchange | Messages | Carries |
|----------|----------|---------|
| Device registration | `RegisterDeviceRequest` / `RegisterDeviceResponse` | `device_id`, `model`, `region`, `current_fw` → acceptance + resulting `status` |
| Firmware download | `FirmwareDownloadRequest` / `FirmwareDownloadResponse` | `firmware_id` → `version`, `checksum`, `total_size`, bounded `chunk` + `offset` + `eof` marker |
| Update status | `UpdateStatusRequest` / `UpdateStatusResponse` | `device_id`, `firmware_id`, `UpdatePhase`, `progress_percent`, failure `detail` → acknowledgment |
| Heartbeat | `Heartbeat` (fire-and-forget) | `event_id`, `device_id`, `current_fw`, `status`, `ts`, `cpu`, `mem`, `health` |
| Commands | `Command` (`StartUpdate`, `AbortUpdate`) | `command_id`, `device_id`, typed payload |

## The v1 rollout health contract

`rollout/v1/rollout.proto` (package `fleetops.rollout.v1`, import
`github.com/DisMosGit/fleetops/api/proto/rollout/v1`) is the operator- and workflow-facing surface
over rollout state. It is a different bounded context from the agent transport and shares no
messages with it:

- **`RolloutService.GetWaveHealth`** — unary. `GetWaveHealthRequest` names a `rollout_id` and a
  `wave_id`; `GetWaveHealthResponse` returns the wave's `success_ratio` and `sample_size`, the
  `WaveHealthVerdict`, the effective `window_start` / `window_end` the samples were read from, and
  the `sample_health_threshold`, `min_success_ratio`, and `min_samples` the verdict was computed
  against. Both ids are echoed so concurrent callers can match results to their requests.

The verdict set is closed and never unset on a served response: `WAVE_HEALTH_VERDICT_UNDECIDED`
when the window holds fewer samples than `min_samples` (thin evidence must not promote a wave),
otherwise `HEALTHY` at or above `min_success_ratio` and `UNHEALTHY` below it. An unknown rollout,
an unknown wave, and a wave belonging to another rollout are all `NOT_FOUND`, and a request
missing either id is `INVALID_ARGUMENT`.

## Regenerating the Go stubs

Generated Go (`*.pb.go`, `*_grpc.pb.go`) is committed beside the contracts under `api/proto/` so
building never requires `protoc` — but it is a build artifact: never edit it by hand. The
contracts are the source of truth, and regeneration restores canonical output over any hand edit.

The workflow (from the repository root):

```sh
make proto-tools   # install the pinned protoc-gen-go, protoc-gen-go-grpc, and goimports
make proto         # regenerate every client and server stub under api/proto/
```

`make proto-tools` installs exactly the generator versions pinned in the Makefile's tool block, so
the same contract files produce byte-identical output on every machine. `make proto` finishes by
formatting its output with the pinned `goimports` — protoc's raw import blocks are not goimports
shape, and without this pass the generation and the Definition of Done's `goimports -w .` would
rewrite each other's output forever. Both targets fail with a message naming the missing tool when
`protoc` or a plugin is unavailable. `protoc` itself is a prerequisite (install it from the
protobuf releases, with its `include/` tree); the Go plugins are installed by the pinned
`proto-tools` target.

After regenerating, run `make proto` a second time and expect no diff — a dirty tree means
generator version skew or a hand edit.
