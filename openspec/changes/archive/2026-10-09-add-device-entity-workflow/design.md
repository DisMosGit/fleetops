# Design

## Context

`internal/temporal` is a doc stub today: no workflows, no activities, no Temporal SDK
dependency. The producers of device events already exist — `internal/agentserver` routes
heartbeats to a `HeartbeatSink` seam and exposes `Hub.Send` as "the command seam later stages
send through"; `AgentService.Report` is declared in the frozen v1 proto but unimplemented. The
`devices`/`telemetry` persistence pipeline (batched ingest, liveness sweep) is complete and
must keep working unchanged. Process split: `cmd/controlplane` hosts the gRPC server and the
hub; `cmd/worker` is the designated Temporal worker host (both binaries already parse
`cfg.Temporal`). See proposal.md — Why, and the specs deltas for normative behavior.

## Goals / Non-Goals

**Goals:**
- One deterministic, long-lived `DeviceWorkflow` per device with all four state fields owned in
  workflow state and carried intact across continuations.
- Signal idempotency that holds both within a run and across continuations, by construction.
- Bounded event history via rolling continuation, with no signal loss around the rollover.
- A working end-to-end signal path: stream heartbeat → workflow signal; `Report` → workflow
  signal; workflow → dispatch activity → `Hub.Send` → agent stream.

**Non-Goals:**
- `RolloutWorkflow`, `FirmwareWorkflow`, and the producers of `command_issued` /
  `config_changed` (defined here, called from the rollout change).
- Reflecting workflow state back into MongoDB (the `devices` document stays the read model,
  written exactly as today; no reconciliation loop in this change).
- Search Attributes, workflow versioning beyond the initial `GetVersion` discipline, UI work.
- Exactly-once dispatch to the agent wire (at-least-once dispatch + idempotent conclusion).

## Decisions

### D1. Workflow identity, lifecycle, and entry points
One workflow per device, workflow id `device-<device_id>`, created lazily via
`SignalWithStart` from the producers — no separate "start device" step, and never two run
chains for one device id. Entry points: signals `heartbeat`, `command_issued`, `command_result`,
`config_changed`, plus a `GetState` query returning the full state struct. `command_issued`
is a signal (not a Temporal Update): the caller (rollout, later) is asynchronous by nature, and
a signal keeps the entity's "state changes only through signals" story uniform. Alternative
considered: Temporal Update for `command_issued` to get a synchronous ack — deferred until a
caller actually needs one; adding an Update later is additive.

### D2. State layout and carry-over payload
A single `deviceState` struct is the workflow's only mutable data: `current_fw`,
`last_heartbeat_at`, `pending` (`command_id`, kind/target incl. expected version,
`dispatched` flag), `config` (`version`, payload), and dedup bookkeeping (`recent_event_ids`,
`recent_command_ids`, `signals_applied`). The exact same struct is the ContinueAsNew argument,
prefixed with a `carry_version` field so a future state-shape change can migrate old payloads on
read instead of stranding run chains. Timestamps travel inside signal payloads (computed by the
producer, outside the workflow) — the workflow never calls `time.Now`, and `workflow.Now` is
not needed at all (the continuation trigger is count-based).

### D3. Idempotency: fast-path dedup windows plus structural guards
Two layers, in order:
1. **Dedup windows** in state: a ring of recent heartbeat `event_id`s (512) and a ring of
   recent `command_id`s (issued/concluded, 64). A signal whose key is in the ring is dropped
   before touching state.
2. **Structural guards** make even out-of-window duplicates no-ops: a heartbeat applies only
   when its timestamp is strictly newer than `last_heartbeat_at` (so its own redelivery, and
   any older one, can never change anything); a command result applies only when its
   `command_id` matches the pending command (a concluded or unknown command's result is a
   no-op); a config snapshot applies only at a strictly newer version; a `command_issued` whose
   id is the pending command's id is a no-op.

Layer 2 is what makes the spec's "idempotent even after the dedup memory has rolled over" true
for every signal except `command_issued` for a long-concluded command (see Risks). The rings
are therefore an optimization and a guard for that one corner, not the sole mechanism.

### D4. Rolling continuation: count-triggered, drain-before-continue
The main loop is a `workflow.Selector` over the four signal channels and the in-flight dispatch
future. After each applied batch, when `signals_applied` crosses `maxSignalsPerRun` (128, a
package constant — a knob nobody tunes in a demo is YAGNI, revisit with the rollout stage) the
workflow rolls over: it first **drains** every buffered signal (repeated `ReceiveAsync` until
empty, applying each into the carry-over state), then `workflow.ContinueAsNew` with the full
`deviceState`. Draining is mandatory: Temporal discards a run's unhandled buffered signals at
ContinueAsNew, but signals arriving after the drain are buffered server-side against the
workflow id and delivered to the next run — together that gives exactly-once around the
rollover, which the spec requires. The rollover is a safe point by construction: an unresolved
dispatch future is not awaited — the carry-over `dispatched` flag makes the next run finish the
job (see D5). Alternative considered: time-based rollover (e.g., daily) — rejected as the
trigger; count-based bounds history deterministically regardless of signal rate.

### D5. Command lifecycle: at-least-once dispatch, exactly-once conclusion
`command_issued` sets the pending command (a newer command supersedes the pending one) and
starts the `dispatch-command` activity, which sends `*agentv1.Command` through a narrow
`CommandDispatcher` interface defined in `internal/temporal` and satisfied by
`*agentserver.Hub` (interface at the consumer, per house style). The activity is idempotent
from the workflow's perspective: retries re-send the same command id, and a superseded or
concluded command's later delivery is harmless because the agent's only observable effect is
the Report of a command id the workflow dedups. `command_result` clears the pending command
only on an id match and adopts the commanded firmware version on success. `Report`'s
"idempotency key already seen" contract is satisfied by this workflow-level `command_id`
dedup — no second idempotency store: the response is `accepted` for both the first and repeated
reports, and only the first produces a state change.

### D6. Process hosting: activities live beside their side effects
`cmd/worker` gains the Temporal client and a worker registering `DeviceWorkflow` on
`cfg.Temporal.TaskQueue`. The `dispatch-command` activity is registered by a second worker on
the **same task queue** inside `cmd/controlplane`, because its only dependency is the in-process
hub ("shared by the gRPC service and ... Temporal activities" per the hub's own doc comment).
Producers (`heartbeat` and `command_result` signalers) live in `cmd/controlplane`'s gRPC path,
wired through small interfaces defined at their consumers in `internal/agentserver`
(`WorkflowSignaler`), implemented in `internal/temporal` over the Temporal client
(`SignalWithStart`). Alternative considered: a durable handoff (activity writes the command to
Mongo, stream server pushes it) — rejected: a second queue of truth, delivery lag, and more
moving parts than a same-queue activity. Alternative: run everything in one process — rejected:
the repo map fixes `cmd/worker` as the worker host and process boundaries are worth keeping.

### D7. Determinism and naming discipline
No `time.Now`, no `rand`, no I/O in the workflow; goroutines only via `workflow.Go`; activities
take `context.Context` first and are retry-safe; registration uses explicit names
(`"device-workflow"`, `"dispatch-command"`). Any future logic change to the workflow body goes
behind `workflow.GetVersion`. Files: `internal/temporal/device_workflow.go` (loop + signals),
`device_state.go` (state + carry-over + dedup rings), `dispatch.go` (activity +
`CommandDispatcher`), `signaler.go` (`SignalWithStart` producers) — each well under the 400 LOC
split threshold, tests beside them.

## Risks / Trade-offs

- [ContinueAsNew silently drops a run's unhandled buffered signals] → drain-before-continue
  (D4) is part of the continuation procedure itself, and the spec scenario "signals in flight
  around a rollover are applied exactly once" pins it with a workflow test.
- [Duplicate `command_issued` after its dedup ring rolled over could re-dispatch a concluded
  command] → the ring holds 64 commands per device (a device takes one command per rollout
  wave — hours of cadence to wrap), the re-delivered command is idempotent on the device
  (re-apply same firmware), and its result still concludes at most once. Accepting this beats
  unbounded dedup state.
- [One Temporal signal per heartbeat at fleet scale] → payloads are tiny and the emulator fleet
  is 10²–10³ devices; if signal rate ever dominates, producer-side coalescing (at most one
  heartbeat signal per device per interval) is an additive change behind the `WorkflowSignaler`
  seam and does not weaken the idempotency guarantees.
- [Two binaries must agree on task queue and payload shapes] → payloads are plain structs
  versioned by `carry_version` and proto messages; explicit registration names; deploy both
  binaries together (single-node k3d, no skew in practice).
- [Workflow authority vs. the Mongo read model can diverge] → precedence is specified (workflow
  wins on the four owned fields); the read model keeps being written on the ingest path so fleet
  list views and the liveness sweep are unaffected; reconciliation, if ever needed, is a later
  change.
- [Report for a command the workflow never issued is a silent no-op] → correct per spec (result
  for non-pending command changes nothing); the rollout change must correlate command ids, and
  the Report response still says `accepted` so the agent does not retry-loop.

## Migration Plan

No data migration. Deployment order inside the stack: land `cmd/controlplane` (activity worker +
producers) and `cmd/worker` (workflow worker) in the same release; existing device records and
telemetry keep their meaning. Rollback is binary rollback — workflow run chains survive idle in
Temporal and resume when the code returns; nothing external is rewritten.

## Open Questions

- If signal rate ever becomes a bottleneck, coalesce heartbeats at the producer (per-device
  interval) or move liveness onto the stage-3 RabbitMQ fan-out — decided when telemetry
  fan-out lands, without spec changes.
- Whether a superseded pending command should notify its issuer (rollout) — the rollout change
  defines its own command correlation; nothing here depends on the answer.
