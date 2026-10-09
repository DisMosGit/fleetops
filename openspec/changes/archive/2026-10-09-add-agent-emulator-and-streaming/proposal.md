# Proposal

## Why

The frozen v1 `AgentService` contract exists, but nothing speaks it yet: `cmd/agent` and the
control-plane gRPC surface are stubs, so no simulated device emits a heartbeat, no stream can be
opened or kept alive, and there is no way to push a command to a device. This change lands the
first end-to-end agent conversation — the emulator fleet, the resilient stream client, and the
server that accepts it — which every later stage (Temporal signals, telemetry fan-out, rollouts)
consumes as its transport.

## What Changes

- **Device-agent emulator** (`internal/agent`): one `cmd/agent` process runs
  `cfg.Simulation.FleetSize` simulated devices as goroutines (errgroup + ctx stop condition, one
  owner per goroutine). Each device emits a periodic heartbeat carrying `event_id`, `device_id`,
  `current_fw`, `status`, sample `ts`, and the `cpu` / `mem` / `health` samples one telemetry
  document needs. Metric values come from an injected simulation source so tests are
  deterministic; a small fixed set of models/regions gives devices rollout-target identity.
- **Agent stream client** (`internal/agent`): one long-lived `AgentService.Connect` stream per
  emulator process, multiplexing every simulated device. The client registers each device on
  every (re)connect, sends heartbeats through a bounded outbound queue (backpressure by blocking
  the producer — never unbounded buffering), matches responses to requests by correlation id, and
  hands inbound commands to a command-handler seam. On stream loss it reconnects with capped
  exponential backoff and jitter, and loses no message: an envelope leaves the queue only after a
  successful stream send, redelivered heartbeats keep their original `event_id`, and unanswered
  requests are retried on the new stream under their original correlation id. gRPC keepalive
  parameters keep the connection detectably alive.
- **Agent stream server** (`internal/agentserver`): the control-plane implementation of
  `AgentService.Connect` — accept one stream per agent, complete the registration exchange
  (required identity fields validated; a rejected or malformed registration enrolls nothing),
  route every inbound heartbeat to a `HeartbeatSink` seam, and expose a command-dispatch API that
  delivers a `Command` to the target device's live session through a bounded per-session queue.
  Failures map to safe gRPC status codes (`codes.InvalidArgument`, `codes.NotFound`, …) that
  never leak internals; recovery and logging interceptors are installed in the documented order,
  with the metrics/tracing slots joining at their stage.
- **Wire-up**: `cmd/agent` replaces its `errNotImplemented` stub with the running fleet;
  `cmd/controlplane` serves `AgentService` on `cfg.GRPC.ListenAddr` next to the probes and wires
  a minimal logging `HeartbeatSink` until the RabbitMQ publisher lands at stage 3.

Explicitly out of scope for this change (they arrive with their own stages): firmware
download/apply exchanges and `AgentService.Report`, Temporal signal delivery, RabbitMQ telemetry
fan-out and persistence, metrics/tracing instrumentation.

## Capabilities

### New Capabilities

- `agent-emulator`: The simulated device fleet — how many devices run per process, what a
  simulated device is (identity, model/region, firmware version, status), the periodic heartbeat
  it emits and the exact fields each heartbeat carries, and the lifecycle/stop-condition rules
  for the fleet's goroutines.
- `agent-stream-client`: The agent side of the `AgentService.Connect` stream — opening the
  stream and registering devices, sending heartbeats under backpressure, keeping the stream
  alive, reconnecting with capped exponential backoff after failure, and the no-message-loss
  guarantees across reconnects (stable event ids, retried correlated requests, bounded queue).
- `agent-stream-server`: The control-plane side of the `AgentService.Connect` stream — accepting
  agent connections, the registration exchange and its validation, routing inbound heartbeats to
  the heartbeat sink, dispatching commands to a specific device's live session over the same
  stream with bounded per-session queues, and the safe error mapping of stream-level failures.

### Modified Capabilities

None. No main specs exist yet (`openspec list --specs` is empty), and this change implements
rather than alters the wire contract defined by `add-v1-proto-contracts`
(`agent-control-plane-api`): the frozen v1 proto is untouched, and every message this change
sends or handles is already defined there.

## Impact

- **Files created:** `internal/agent` grows from a doc stub into the emulator and stream client
  (heartbeat simulation, device goroutines, stream session with reconnect/backoff, plus
  `*_test.go` beside the code); a new `internal/agentserver` package (gRPC `AgentService`
  implementation, session registry, heartbeat routing seam, command dispatch, plus tests).
- **Files touched:** `cmd/agent/main.go` (start the fleet instead of returning
  `errNotImplemented`), `cmd/controlplane/main.go` (serve `AgentService` on
  `cfg.GRPC.ListenAddr` and wire the sink), `internal/README.md` (the new package). The
  `runtime-config` surface is consumed as-is — `simulation.fleet_size`, `grpc.listen_addr`, and
  `grpc.control_plane_addr` already carry everything needed, so `deploy/config.yaml` and the
  config validation contract do not change.
- **Dependencies:** none new — `google.golang.org/grpc`, `google.golang.org/protobuf`,
  `golang.org/x/sync`, and `github.com/google/go-cmp` are already direct requirements.
- **No impact on:** the frozen v1 proto contracts, the MongoDB data model, Temporal workflows,
  the RabbitMQ telemetry pipeline, or the staged delivery plan — this is the stage-1 emulator
  heartbeat path plus the stage-3 stream transport those stages build on.
