# Proposal

## Why

A device's state today is scattered: `devices` documents in MongoDB hold firmware and last-seen
fields written by the ingest path, dispatch state lives only in the hub's in-memory session
registry, and nothing survives as one coherent picture of one device across restarts, racing
writers, or a future rollout addressing it. Stage 2 needs the long-lived per-device entity the
project is built around: a `DeviceWorkflow` that owns the device's authoritative state — current
firmware, last heartbeat timestamp, pending command, configuration snapshot — receives signals
instead of polling, and stays visible in the Temporal UI as one run chain per device. Without it
the coming `RolloutWorkflow` has no durable counterpart to command or query, and duplicate
deliveries or a months-long workflow history would corrupt or bloat whatever state we keep.

## What Changes

- One long-lived `DeviceWorkflow` per device (stable workflow id `device-<device_id>`) owns the
  authoritative state: current firmware, last heartbeat timestamp, pending command, and a
  versioned configuration snapshot. A `GetState` query exposes it; MongoDB `devices`/`telemetry`
  stay the fleet-wide read model and keep being written exactly as today.
- The workflow accepts signals: `heartbeat` (liveness + reported firmware), `command_result`
  (terminal outcome of a dispatched command), and `config_changed` (a new configuration
  snapshot with its version). A fourth entry point, `command_issued`, sets the pending command;
  the workflow then dispatches it to the agent through an idempotent dispatch activity on the
  existing command seam (`agentserver.Hub.Send`), and the matching `command_result` clears it.
- Signals are idempotent when delivered twice: each carries its dedup key (heartbeat
  `event_id`, command result `command_id`, config change version), duplicates inside a bounded
  seen-key window are dropped, and every state transition is monotone so even a stale duplicate
  outside the window has no observable second effect.
- The workflow never accumulates unbounded history: it continues-as-new (rolling continuation)
  once it has processed a bounded number of signal events, carrying the complete state —
  including the dedup windows — into the next run so the entity is seamless across
  continuations. Signals arriving during the transition are buffered by Temporal and processed
  by the next run; nothing is lost.
- `AgentService.Report` (declared in the frozen v1 proto, unimplemented until now) is
  implemented: it validates the report, forwards the command result to the device workflow, and
  a report repeating a seen idempotency key is accepted again with no second effect.
- Accepted heartbeats fan out to the device workflow signal alongside the existing telemetry
  ingestion; the ingestion pipeline itself is unchanged.

## Capabilities

### New Capabilities

- `device-workflow`: the per-device entity workflow — authoritative device state and its query
  surface, the signals it accepts, signal idempotency semantics, the pending-command dispatch
  lifecycle, and the rolling continuation that bounds event history while preserving state.

### Modified Capabilities

- `agent-stream-server`: two new requirements on the control-plane agent surface —
  command-result intake via `AgentService.Report` (validation, workflow signaling, idempotent
  re-acceptance), and device-workflow signaling on accepted heartbeats alongside the existing
  sink routing.

## Impact

- **Code**: `internal/temporal` (DeviceWorkflow state machine, signal handlers, dispatch
  activity, continuation), `internal/agentserver` (Report handler, heartbeat fan-out to the
  workflow signal seam), `cmd/worker` (Temporal client and worker hosting `DeviceWorkflow` and
  its activities), `cmd/controlplane` (Temporal client for signal producers and a worker
  hosting the hub-bound dispatch activity on the same task queue). Tests via Temporal's
  `TestWorkflowEnvironment`.
- **Dependencies**: `go.temporal.io/sdk` — the orchestration backend named in the project's
  stack; nothing else new.
- **Contracts**: none. The v1 proto is untouched — `Report`, `Command`, and `Heartbeat.event_id`
  already exist and carry everything the signals need.
- **Existing behavior**: telemetry ingestion, device-registry persistence, and the liveness
  sweep are unchanged. Where the workflow's authority and a stored document could disagree on
  the four owned fields, the workflow wins — stored documents are a read model (see design).
- **Out of scope**: RolloutWorkflow and its producers for `command_issued`/`config_changed`
  (those signals are defined and tested here; their callers land with the rollout change),
  Search Attributes, and UI consumption of the `GetState` query.
