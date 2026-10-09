# Design

## Context

See `proposal.md` — Why. What shapes the approach here:

- **The v1 proto is frozen** (`api/proto/agent/v1/agent.proto`): `AgentEnvelope` /
  `ControlEnvelope` are correlated (`correlation_id`), heartbeats are fire-and-forget but carry a
  stable `event_id` that "a redelivery reuses", and one `Connect` stream fronts possibly several
  devices. Everything below fits inside that contract; nothing here may require a proto change.
- **Layout rules** (AGENTS.md, `internal/README.md`): one package per capability, everything in
  `internal/`, thin `cmd/` entrypoints, errgroup + ctx for goroutine ownership, bounded queues
  ("block or drop with a metric — never unbounded buffering"), sentinel errors, `slog` at
  boundaries, table-driven hermetic tests without fixed ports.
- **Available seams**: `config.Simulation.FleetSize`, `config.GRPC.{ListenAddr,ControlPlaneAddr}`
  already carry all needed knobs; MongoDB, RabbitMQ, and Temporal clients do not exist yet, so
  the server can only *route* heartbeats and *dispatch* commands behind interfaces those stages
  implement later.

## Goals / Non-Goals

**Goals:**

- One transport implementation that later stages plug into without touching stream code: the
  server's heartbeat sink and command dispatch are the seams Temporal activities and the
  RabbitMQ publisher will consume/produce through.
- Deterministic, race-free emulation: tests observe exact heartbeats and exact backoff sequences
  without wall-clock sleeps or randomness.

**Non-Goals:**

- Firmware download/apply exchanges and `AgentService.Report` (their stage); the generated
  server interface is embedded with these methods unimplemented until then.
- Metrics/tracing interceptors and drop-with-metric queue policies (stage 4 adds them at the
  documented interceptor positions, behind the same code paths).
- Persisting registrations or device records (Mongo stage); the session registry is in-memory
  and per control-plane process — HA is out of scope for the whole project.

## Decisions

1. **One stream per emulator process, devices multiplexed.** The proto defines one stream per
   agent and `Command.device_id` exists precisely because "one agent may front several devices".
   One stream for N goroutine devices also bounds the server's connection count at fleet scale
   (1000 devices = 1 stream). *Alternative:* one stream per device — rejected: contradicts the
   frozen contract and multiplies reconnect storms.

2. **Packages: `internal/agent` (emulator + stream client) and `internal/agentserver`
   (control-plane side).** Matches the documented map (`agent/` = "heartbeat, bidi stream
   client"), and names each package after what it provides. *Alternative:* one package for both
   sides — rejected: client and server are separate capabilities with separate consumers
   (`cmd/agent` vs `cmd/controlplane`).

3. **Fleet concurrency: one errgroup, one goroutine per device.** Each device goroutine owns its
   ticker and its device state and hands finished heartbeats to the client over one bounded
   channel; nothing mutable is shared between devices. Shutdown is ctx cancellation: the
   errgroup returns when every device goroutine has returned, which is also the leak test.
   *Alternative:* one ticker for the whole fleet — rejected: per-device periods and degradations
   become shared state.

4. **Client = one session loop with a writer, a reader, and a reconnect policy.**
   - *Writer* drains a bounded outbound queue onto the stream; an envelope is dequeued only after
     `Send` returns nil; on `Send` error it goes back to the queue head and the session is torn
     down.
   - *Reader* demuxes `ControlEnvelope`: responses join a correlation-id → pending-request map
     (mutex-guarded), commands go to a command-handler seam (firmware apply lands later).
   - *Reconnect* wraps open + re-register: exponential backoff with jitter (1s initial, ×2,
     30s cap), reset on success, stopping on ctx cancellation. Backoff sleeps through an
     injected sleeper and jitter through an injected `rand.Source`, so tests assert the exact
     delay sequence instantly.
   - *Keepalive* via `keepalive.ClientParameters` (Time 20s, Timeout 5s, PermitWithoutStream)
     so a half-open connection surfaces as a stream failure and triggers the same cycle.
   - *Registration before resume:* the queue keeps draining only after every device has been
     re-registered on the new stream, so the server never routes a heartbeat for a device it has
     not seen on this stream.

5. **No-loss guarantee = stable identity + surviving queue.** An unsent envelope survives the
   reconnect; a request sent but unanswered is retried under its original `correlation_id`;
   heartbeats keep the `event_id` assigned at emission, so any duplicate is a downstream no-op
   (ingestion keys on `event_id`). *Alternative:* application-level acks for heartbeats —
   rejected: the proto has no ack and is frozen. *Residual:* a heartbeat that `Send` accepted but
   the network swallowed mid-flight is undetectable without an ack; the window is the transport's
   send buffer. This is recorded as a trade-off below rather than papered over.

6. **Server = `agentserver.Server` (gRPC surface) + session registry + two seams.**
   - `Connect` spawns per-stream session loops (reader + bounded per-session send queue);
     registration validates the correlation id and the four identity fields and enrolls the
     device into a mutex-guarded `map[deviceID]` → session registry; stream end (deferred)
     unenrolls every device of that stream.
   - Seam 1, `HeartbeatSink` — defined where consumed (`agentserver`), taking the wire
     `agentv1.Heartbeat` unchanged so no translation layer exists yet; `cmd/controlplane` wires a
     minimal logging sink until the RabbitMQ publisher becomes the real implementation. The
     proto message is passed through rather than re-mapped into a local struct: it *is* the
     contract, and the telemetry mapping belongs to ingestion.
   - Seam 2, command dispatch (`Send(ctx, *agentv1.Command) error` on the hub): looks up the
     device's session, enqueues onto its bounded queue, and returns `ErrNotFound` for unknown or
     disconnected devices. Blocking until the caller's ctx expires gives future activities their
     timeout semantics for free.
   - Error mapping: unusable envelope → `codes.InvalidArgument` with an operator-safe message;
     panics in stream handling are recovered by the interceptor and mapped to `codes.Internal`.
     Recovery + logging interceptors are installed in the AGENTS.md order, leaving the metrics
     and tracing positions to stage 4.
   - *Alternative:* route heartbeats straight to Mongo — rejected: ingestion is stage 3's
     idempotent consumer and stage 2's signal path; a sink seam keeps this change transport-only.

7. **Simulation is injected.** A small source interface owned by `internal/agent` produces each
   device's next sample (`cpu`, `mem`, `health`, status) plus degradation episodes; the default
   implementation uses an explicit `rand.Source` (seeded, passed in — no package-level `rand`),
   tests inject a scripted source. `ts` comes from an injected `now func() time.Time` defaulting
   to `time.Now` (the emulator is not a workflow; wall clock is correct here).
   - Status vocabulary this change introduces: `online` (normal) and `degraded` (during an
     episode, with lower `health`); firmware stages add `updating` / `rolled_back` later.
   - `event_id = <device_id>-<run_nonce>-<counter>` with a crypto/rand run nonce per process and
     a per-device counter: unique across restarts (matters once ingestion keys on `event_id`)
     and stable across redelivery.

8. **Config is consumed, not extended.** Heartbeat period, queue bounds, and backoff parameters
   are constructor parameters with documented defaults (5s period, queue bound derived from
   fleet size with a floor, backoff 1s→30s), so `internal/config` and `deploy/config.yaml` stay
   untouched. If operators later need them, that is a `runtime-config` change.

## Risks / Trade-offs

- [A heartbeat accepted by `Send` can still be lost in a network cut — v1 has no heartbeat ack]
  → the loss window is bounded by the transport send buffer; redeliveries always reuse
  `event_id`, so the eventual telemetry consumer stays idempotent. Revisit only if stage-3
  telemetry shows real gaps.
- [Blocking producers on a full queue slow device tickers] → that is the intended backpressure;
  the queue bound is derived from fleet size. Drop-with-metric is deferred to stage 4 where the
  metric exists (AGENTS.md forbids unbounded buffering, not blocking).
- [Reconnect storm after a control-plane restart] → one stream per emulator process (not per
  device) plus jittered backoff bounds the attempt rate at one per process.
- [In-memory session registry vanishes on control-plane restart] → acceptable for a single-node
  demo (HA is out of scope); agents re-register on reconnect, so the registry self-heals.
- [Invented `online`/`degraded` status vocabulary] → recorded here and in the emulator spec as an
  assumption; later stages extend the set rather than replace it.

## Migration Plan

None — greenfield transport with no persisted state to migrate and no deploy-time coupling. A
rollback is a plain revert: nothing this change writes outlives the process.

## Open Questions

- Whether a failing `HeartbeatSink` should block the stream (backpressure toward agents) or drop
  with a log: deferred until the sink becomes a real publisher with publish confirms at stage 3.
  Today's logging sink cannot meaningfully fail, so no code depends on the answer.
