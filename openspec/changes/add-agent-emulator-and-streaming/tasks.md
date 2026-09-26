# Tasks

## 1. Emulator core (`internal/agent`)

- [ ] 1.1 Implement the injectable simulation source (seeded `rand.Source` default with
      degradation episodes, scripted fake for tests), the `online`/`degraded` status derivation,
      and `event_id` generation (crypto/rand run nonce + per-device counter), and verify with
      table-driven tests that scripted sources produce exact samples and that event ids are
      unique per emission and stable per redelivery (`go test ./internal/agent/...`)
- [ ] 1.2 Implement the simulated device: one ctx-bound goroutine owning its ticker and state,
      emitting heartbeats at the configured period with event id, device id, `current_fw`,
      `status`, `ts`, and `cpu`/`mem`/`health` in [0.0, 1.0], and verify with a fake clock that
      one heartbeat per period is emitted with the full field set and that ctx cancellation
      stops emission without leaking the goroutine
- [ ] 1.3 Implement the fleet runner (errgroup over N devices from `Simulation.FleetSize`, model
      and region drawn from the emulator's fixed sets) and verify a table-driven test runs a
      small fleet and observes N distinct identities spanning multiple models and regions plus a
      clean shutdown
- [ ] 1.4 Land the emulator's package documentation (doc comment stating the implemented
      behavior) and verify `go vet ./internal/agent/...` passes with godoc on every exported
      symbol

## 2. Agent stream client (`internal/agent`)

- [ ] 2.1 Implement the bounded outbound queue and writer loop (enqueue blocks at the bound,
      an envelope is dequeued only after a successful `Send`, a failed envelope returns to the
      queue) and verify unit tests with a fake stream observe the bound being enforced and the
      failed envelope preserved for redelivery
- [ ] 2.2 Implement the reader demux and pending-request table (responses matched by
      correlation id, unanswered requests retried on a new stream under their original
      correlation id, commands handed to the command-handler seam) and verify tests cover
      matched responses, retried requests, and command delivery
- [ ] 2.3 Implement the session lifecycle: open `Connect`, register every device (correlation id
      per registration) before any of its heartbeats flow, and re-register all devices before
      resuming delivery after a reconnect; verify tests with a fake service assert
      registration-before-heartbeat on first connect and on reconnect
- [ ] 2.4 Implement reconnect with capped exponential backoff and jitter through an injected
      sleeper and `rand.Source`, backoff reset on success, no attempt after ctx cancellation, and
      gRPC keepalive parameters on the connection; verify a test asserts the exact delay
      sequence (initial, doubling, capped) and the reset on success
- [ ] 2.5 Verify the no-message-loss guarantee end to end in-package: a test gRPC server kills
      the stream mid-traffic and the test asserts every queued heartbeat is delivered after
      reconnect with its original event id and duplicates remain identifiable
      (`go test -race ./internal/agent/...`)

## 3. Control-plane stream server (`internal/agentserver`)

- [ ] 3.1 Implement `AgentService.Connect` with recovery and logging interceptors in the
      documented order and safe error mapping, and verify tests observe an unusable envelope
      failing with `codes.InvalidArgument` and an operator-safe message, a handler panic
      recovered as `codes.Internal` without taking the process down, and other streams still
      served
- [ ] 3.2 Implement the registration exchange and the in-memory session registry (validate
      correlation id and the four identity fields, enroll on acceptance, reject incomplete
      registrations without enrolling, unenroll a stream's devices on stream end) and verify
      tests cover acceptance, rejection with nothing enrolled, and devices becoming unroutable
      after their stream ends
- [ ] 3.3 Implement heartbeat routing to the `HeartbeatSink` seam (enrolled heartbeats routed
      unchanged with their event id, unenrolled heartbeats not routed, redeliveries keep their
      identity) and verify tests with a fake sink assert the exact routed messages
- [ ] 3.4 Implement command dispatch (`Send(ctx, command)` over a bounded per-session queue:
      delivery on the target device's own stream with command id and device id, `ErrNotFound`
      for unknown or disconnected devices, ctx-deadline failure when the queue is congested) and
      verify tests cover target-only delivery, not-found, and the bounded-queue deadline
- [ ] 3.5 Land the `agentserver` package documentation and update `internal/README.md`'s
      package list, and verify `go vet ./internal/agentserver/...` passes and the README names
      the implemented package and its seams

## 4. Entrypoint wire-up and integration check

- [ ] 4.1 Replace `cmd/agent`'s `errNotImplemented` stub with the running fleet and stream
      client (fleet size and control-plane address from the loaded config, ctx-bound shutdown)
      and verify `go build ./cmd/agent` succeeds and the emulator starts and stops cleanly
      against a local test server
- [ ] 4.2 Serve `AgentService` in `cmd/controlplane` on `cfg.GRPC.ListenAddr` with the
      interceptors and a minimal logging `HeartbeatSink`, sharing the probe server's errgroup
      lifecycle and graceful shutdown, and verify `go build ./cmd/controlplane` succeeds and the
      existing health-probe tests still pass
- [ ] 4.3 Run the hermetic end-to-end check (no fixed ports): an emulator fleet against a real
      in-process gRPC server delivers heartbeats to the sink, a dispatched command reaches its
      device over the same stream, and a server-side stream kill is survived by reconnect —
      verify `goimports -l .` is empty, `go vet ./...`, `golangci-lint run`, and
      `go test -race -count=1 ./...` all pass
