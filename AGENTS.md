# AGENTS.md

Engineering rules for AI coding agents (and humans) working on FleetOps — a control plane that
rolls firmware to an emulated IoT fleet in canary waves with health-gated promotion and automatic
rollback. Correctness, readability, and testability come first. Where a rule conflicts with
existing code, apply the rule to new code — and never refactor unrelated code in the same change.
Process questions (PR workflow, review) live in [CONTRIBUTING.md](CONTRIBUTING.md); this file is
rules only.

## Project context

- **What FleetOps is:** device-agent emulators + control plane. Long-lived Temporal entity
  workflows per device, a saga rollout workflow (waves 1% → 5% → 25% → 100%, durable health
  windows, compensating rollback), gRPC bidirectional streams for agents, RabbitMQ telemetry
  fan-out, MongoDB time-series telemetry + GridFS firmware binaries.
- **Stack:** Go (module `github.com/DisMosGit/fleetops`), Temporal, gRPC/Protobuf, MongoDB,
  RabbitMQ, React + Vite UI, Docker + k3d, OpenTelemetry / Prometheus / Grafana.
- **Explicitly out of scope:** auth and multi-tenancy (single operator), firmware signing (stub),
  CDN distribution, HA/production config. Do not build toward these "for later".
- **Delivery is staged** (proto freeze → Temporal → streaming/telemetry → observability → UI).
  See [README.md](README.md) for the stage table. Code outside the current stage's scope waits
  for that stage.

## Planned repository map

Target layout from the delivery plan. Entries exist only when their stage lands; never present a
planned path as already working.

```
cmd/<binary>/               — thin entrypoints (control plane, workers, agent emulator): flags, wire-up, run
internal/agent/             — device-agent emulator: heartbeat, bidi stream client, firmware apply
internal/temporal/          — workflows + activities: DeviceWorkflow, RolloutWorkflow, FirmwareWorkflow
internal/telemetry/         — RabbitMQ publisher/consumers, idempotent ingestion, DLQ handling
api/proto/                  — .proto contracts + generated Go
deploy/                     — k3d/compose manifests for the local stack
web/                        — React + Vite frontend
```

- Everything lives in `internal/` except thin `cmd/` entrypoints. There is no exported library
  surface — do not create one.
- No `utils`, `helpers`, `common` — name packages by what they provide.
- Tests sit beside the code as `*_test.go` (same package for units; external `_test` package when
  testing the exported surface).

## Golden rules (non-negotiable)

- **Idiomatic Go.** Follow [Effective Go](https://go.dev/doc/effective_go) and
  [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
  If a solution feels clever, it's probably wrong.
- **Return errors, don't panic.** `panic` only for unrecoverable programmer errors at init time.
  Never in libraries.
- **Wrap, never swallow.** `fmt.Errorf("load device %s: %w", id, err)`; check with
  `errors.Is`/`errors.As`. Never `_ = err`.
- **Accept interfaces, return structs.** Define interfaces where they're consumed, not implemented.
- **No package-level mutable vars.** Pass dependencies explicitly; wire them in `cmd/`.
- **Small functions, small packages.** If a file exceeds ~400 LOC, split it.
- **Zero value is valid** — or fails loudly.

## Concurrency

- Every goroutine has an owner and a stop condition. Prefer `errgroup.Group` with `ctx`.
  The agent emulator runs N simulated devices as goroutines in one process — each goroutine needs
  a stop condition tied to `ctx` shutdown, or fleet scale becomes a leak.
- `context.Context` is the first argument of every blocking call; never store it in a struct.
- Protect shared state with `sync.Mutex` — or better, don't share it; communicate via channels.
- Buffered channels where the sender is controlled; unbuffered only for synchronization.
- The race detector is mandatory — see Definition of done.

## Errors & logging

- Sentinel errors for expected cases: `var ErrNotFound = errors.New("not found")`.
- `slog` for structured logs. Include `trace_id`, `span_id`, and domain IDs
  (`device_id`, `rollout_id`, `firmware_id`).
- Log at boundaries (gRPC handlers, workflow activities, MQ consumers) — not deep in helpers.
- Never log secrets, tokens, or full payloads.

## Temporal

Workflows are the core of FleetOps. Determinism rules are absolute.

- Workflows must be **deterministic**: no `time.Now()`, no `rand`, no direct I/O.
  Use `workflow.Now`, `workflow.SideEffect`, activities.
- Goroutines inside workflows only via `workflow.Go` — never a bare `go` statement.
- Activities take `context.Context` first, are **idempotent** (they will be retried), and get
  their retry policy from the caller.
- Register workflows and activities with explicit names (`RegisterOptions{Name: ...}`) —
  never rely on function-name reflection.
- Change workflow logic in place only behind `workflow.GetVersion`.
- `DeviceWorkflow` is a long-lived entity (hours/days): state = version, config, last heartbeat;
  updates arrive as signals (`heartbeat`, `command_result`). When history grows, use
  `ContinueAsNew` rather than unbounded event history. Health windows and wave gates in
  `RolloutWorkflow` use **durable timers**, never `time.Sleep` or wall-clock checks.
- Rollback is a saga: compensations run in reverse order (downgrade + inventory + notification).
  Every compensation must itself be idempotent.

## gRPC

- One proto package per bounded context; never reuse messages across services.
- **v1 proto is frozen after stage 1.** Evolve by adding fields only — never renumber, never
  rewrite contracts in place.
- `AgentService.Connect` (bidi stream) is long-lived: one stream per agent carrying heartbeats
  inbound and commands outbound. Respect backpressure (bounded send queues, block or drop with a
  metric — never unbounded buffering), and reconnect gracefully with backoff on stream loss.
- All mutating RPCs accept and validate an idempotency key (`AgentService.Report` included).
- Interceptors, in order: recovery, logging, metrics, tracing.
- Never leak internals to clients — map errors to `status.Error(codes.X, safeMsg)`.

## MongoDB

- `devices` is heterogeneous (model/region/firmware/status per doc); `telemetry` is a
  time-series collection; firmware binaries live in GridFS.
- Telemetry ingestion is **idempotent**: `_id = event_id`, so redelivered messages are no-ops.
- Change streams (not polling) trigger eligible-device-pool recalculation on `devices` changes.
- Context first on every call; never let a driver timeout leak into a Temporal activity without
  wrapping.

## RabbitMQ

- Topic exchange fans heartbeats out to analytics / alerting / rollout-health consumers; a work
  queue carries rollout tasks; a DLQ catches poison messages.
- Consumers must be idempotent and acknowledge only after the side effect is durable; publish
  confirms for anything a workflow depends on. Poison messages go to the DLQ — never drop
  silently and never requeue forever.

## Observability

- OTel traces must cover the full path: `RolloutWorkflow` → activity → gRPC → agent → telemetry →
  health check. Propagate `trace_id`/`span_id` through gRPC metadata and message headers.
- Prometheus metrics at least: wave progress, per-region health, heartbeat ingest rate, activity
  latency. Grafana dashboards read these; do not invent dashboard-only metrics without a source.
- Logging rules above still apply inside activities and consumers.

## Testing

- **Table-driven** by default; name subtests with `t.Run(tc.name, ...)`.
- **`t.Parallel()`** where safe. Keep tests hermetic — no shared files, no fixed ports.
- **`t.Cleanup`** for teardown, not bare `defer` after opening resources.
- **Assertions:** stdlib + `cmp.Diff`. No Testify.
- **Mocks:** hand-write small fakes, or generate with `mockery` for large generated mocks. No mock
  frameworks in hot paths.
- **Coverage:** ≥80% on domain logic, ≥90% on workflow/state-machine code. Don't chase 100% on
  glue.
- **Integration tests** behind `//go:build integration`, using `testcontainers-go` for
  Mongo/RabbitMQ.
- **Workflow tests** via Temporal's `TestWorkflowEnvironment` with mocked activities.
  Assert determinism: a workflow that passes once and flaps on replay is a bug.
- **Golden files** in `testdata/` for serialized output. Regenerate with `-update`, never edit by
  hand.

## Style & linting

- `goimports -w .` covers gofmt formatting plus import management.
- Lint with `golangci-lint run` and the repo's `.golangci.yml` (once it exists). Fix findings;
  never disable a linter to make code pass.
- Line length ~100 chars. Comments explain *why*, not *what*.
- Godoc on every exported symbol, starting with the symbol name.
- Error strings: lowercase, no trailing punctuation, no "failed to" prefix.

## Dependencies

- Prefer stdlib. Every new dependency needs a one-line justification in the PR description.
- Pin versions in `go.mod`; run `go mod tidy` when dependencies change.
- Avoid dependencies that pull in cgo unless required — it breaks static builds.

## Definition of done

```sh
goimports -w .
go vet ./...
golangci-lint run
go test -race -count=1 ./...
```

All four must pass. Additionally:

- New or changed logic is covered by tests. For quick iteration, run package-scoped tests
  (`go test ./internal/<pkg>/...`); the full suite above is the bar.
- Dependencies changed → `go mod tidy`.
- Golden files changed → regenerated via `-update`, diff reviewed.
- A flaky test is fixed, not skipped.

## What NOT to do

- Don't add abstractions "for later" (YAGNI) — including auth, multi-tenancy, and CDN concerns
  that are explicitly out of scope.
- Don't write comments that restate the code.
- Don't use `interface{}`/`any` unless truly generic — use generics or concrete types.
- Don't introduce a new pattern when the codebase already has one.
- Don't touch generated files (`*.pb.go`, `mock_*.go`) — regenerate instead.
- Don't weaken workflow determinism for convenience (a `time.Now()` "just for a log line" in a
  workflow is still a bug).
