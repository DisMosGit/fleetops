# Change notes — add-project-docs

## Verification note (task 5.2)

Task 5.2's literal check — `git status` shows only the four doc files as modified/added — cannot
pass in this repository: `main` has zero commits, so every pre-existing file (`.gitignore`,
`LICENSE.md`, `go.mod`, `web/`, `.agents/`, `openspec/`) also reports as untracked. The task's
intent (no stray files changed during this work) was verified by modification time instead:
`find . -newermt <session start>` lists only `README.md`, `CONTRIBUTING.md`, `AGENTS.md`,
`docs/README.md`, and this change's planning artifacts. The literal `git status` check becomes
meaningful once an initial commit lands.

## AGENTS.md rewrite: carry-over / drop checklist (task 1.1)

Evaluation of every rule in the previous generic `AGENTS.md` against the planned FleetOps stack
(control plane + workers in Go, Temporal workflows, gRPC bidi agent streams, MongoDB, RabbitMQ,
React UI; single operator, no auth).

### Carried over (verbatim or FleetOps-adapted wording)

| # | Rule | Notes |
|---|------|-------|
| 1 | Idiomatic Go (Effective Go / Code Review Comments) | kept |
| 2 | Return errors, don't panic | kept |
| 3 | Wrap, never swallow | kept |
| 4 | Accept interfaces, return structs | kept |
| 5 | No package-level mutable vars | kept |
| 6 | Small functions, small packages (~400 LOC) | kept |
| 7 | Zero value is valid or fails loudly | kept |
| 8 | Concurrency: goroutine owner + stop condition, `errgroup` + `ctx` | kept |
| 9 | `context.Context` first arg, never stored in struct | kept |
| 10 | Protect shared state (`sync.Mutex` / channels) | kept |
| 11 | Buffered vs unbuffered channels rule | kept |
| 12 | Race detector mandatory | kept |
| 13 | Sentinel errors for expected cases | kept |
| 14 | `slog` structured logs with `trace_id`/`span_id` | adapted: also domain IDs `device_id`, `rollout_id`, `firmware_id` |
| 15 | Log at boundaries | adapted: names FleetOps boundaries (gRPC handlers, workflow activities, MQ consumers) |
| 16 | Never log secrets/tokens/payloads | kept |
| 17 | Table-driven tests, `t.Run` naming | kept |
| 18 | `t.Parallel()`, hermetic tests | kept |
| 19 | `t.Cleanup` for teardown | kept |
| 20 | Mocks: `mockery` or small fakes, no mock frameworks in hot paths | kept |
| 21 | Coverage ≥80% domain, ≥90% workflow/state-machine | kept |
| 22 | Integration tests behind `//go:build integration`, `testcontainers-go` | kept (Mongo/RabbitMQ are exactly the deps) |
| 23 | Workflow tests via `TestWorkflowEnvironment`, assert determinism | kept |
| 24 | Golden files in `testdata/`, regenerate with `-update` | kept |
| 25 | `internal/` is the default home for code | kept (adapted, see drop D1) |
| 26 | No `utils`/`helpers`/`common` packages | kept |
| 27 | `goimports -w .` formatting | kept (see drop D4) |
| 28 | `golangci-lint run`, fix findings, never disable a linter | kept |
| 29 | ~100 char lines, comments explain *why* | kept |
| 30 | Godoc on exported symbols | kept |
| 31 | Error string style (lowercase, no trailing punctuation) | kept |
| 32 | Prefer stdlib; one-line justification per new dependency | kept |
| 33 | Pin versions, `go mod tidy` | kept |
| 34 | Avoid cgo dependencies | kept |
| 35 | Temporal: workflow determinism (no `time.Now`/`rand`/I/O) | kept |
| 36 | Temporal: `workflow.Go` only | kept |
| 37 | Temporal: activities idempotent, `ctx` first, retry policy from caller | kept |
| 38 | Temporal: explicit `RegisterOptions{Name: ...}` | kept |
| 39 | Temporal: `workflow.GetVersion` for in-place logic changes | kept |
| 40 | gRPC: one proto package per bounded context | kept |
| 41 | gRPC: mutating RPCs accept + validate idempotency key | kept (`AgentService.Report`) |
| 42 | gRPC: interceptor chain order | adapted: `auth` removed (see drop D3) |
| 43 | gRPC: map errors to `status.Error`, never leak internals | kept |
| 44 | Definition of done: four commands (`goimports`, `go vet`, `golangci-lint`, `go test -race`) | kept exactly |
| 45 | DoD extras (tests for new logic, `go mod tidy`, golden `-update`, fix flaky tests) | kept |
| 46 | What NOT to do (YAGNI, no restating comments, no `any`, no new patterns, don't touch generated files) | kept |

### Dropped (every dropped rule listed)

| # | Dropped rule | Rationale |
|---|--------------|-----------|
| D1 | `pkg/` — "only truly reusable, exported code" and "escape to `pkg/` deliberately" | FleetOps has no reusable library surface: control plane, workers, and agent emulator are all `internal/`. A `pkg/` escape hatch with no consumer would be speculative structure (YAGNI). |
| D2 | "Testify only if the package already uses it" | Moot in a greenfield repo. Replaced by the stronger, unconditional rule: stdlib + `cmp.Diff` only — no Testify anywhere. |
| D3 | `auth` in the gRPC interceptor chain ("recovery, logging, metrics, tracing, **auth**") | Auth and multi-tenancy are explicitly out of scope (single operator, local demo). Carrying the rule would pressure future code toward an auth layer the project does not want. |
| D4 | "Enforced in pre-commit" (claim attached to `goimports`) | No pre-commit hook exists in this repo yet. The formatting rule itself is kept; the false enforcement claim is dropped until a hook actually lands. |

Nothing else was dropped: rules 1–46 above all carry into the rewritten `AGENTS.md` (some in
FleetOps-adapted wording). The rewrite additionally *adds* project-specific material the old file
had no reason to know: the target repository map, long-lived entity-workflow rules
(`ContinueAsNew`), gRPC bidi stream rules (backpressure, graceful reconnect), MongoDB/RabbitMQ
conventions (idempotent consumer via `_id = event_id`, topic fan-out, DLQ), and the explicit
out-of-scope list (no auth/multi-tenancy/CDN).