# Contributing to FleetOps

Thanks for your interest in FleetOps. This file covers the *process* — how to set up, how we
work, and what a good change looks like. Engineering rules (Go style, Temporal determinism,
testing standards, the Definition of Done) live in [AGENTS.md](AGENTS.md) and are not repeated
here.

## Where things go

| You want to... | Do this |
|----------------|---------|
| Report a bug or propose a feature | Open a GitHub issue with a clear repro or motivation |
| Change behavior or add a capability | Propose it as a spec-driven change under [`openspec/`](openspec/) — that directory is the home of change proposals (proposal, design, tasks) |
| Fix a typo or improve a doc | Just open a PR; see the docs plan in [docs/README.md](docs/README.md) for where content belongs |
| Ask a question | Open a discussion or an issue |

## Prerequisites

Required today:

- **Go toolchain** matching the version in `go.mod`
- `goimports` and `golangci-lint` (used by the [Definition of Done](AGENTS.md#definition-of-done))
- **Docker** — for the integration tests, which boot real MongoDB, RabbitMQ, PostgreSQL, and
  Temporal containers rather than fakes

Not needed yet — required when the stage that needs them lands:

- **k3d / kubectl** — **planned**, stage 1 (the local stack)
- **Node.js + npm** — **planned**, stage 5 (only for `web/`)

## Development setup

1. Clone the repository (including the `web/` submodule).
2. Verify the toolchain: the four commands in the
   [Definition of Done](AGENTS.md#definition-of-done) must run clean.
3. Before writing code for anything beyond a small fix, check the delivery stage table in
   [README.md](README.md) and, for larger work, the change proposals under [`openspec/`](openspec/).

## Branch and PR workflow

1. Branch from `main`; keep the branch focused on one concern.
2. Spec-driven changes: the plan lives in `openspec/changes/<change-name>/` (proposal → design →
   tasks). Implement against that plan and tick tasks off as they complete.
3. Open a PR with: *what* changed, *why*, and — for any new dependency — a one-line
   justification.
4. Keep PRs reviewable: one concern per PR, no drive-by refactors of unrelated code.
5. All [Definition of Done](AGENTS.md#definition-of-done) checks pass before review.

## What reviewers look for

- The engineering rules in [AGENTS.md](AGENTS.md) applied — especially error handling,
  determinism in workflows, and idempotent activities/consumers (see the respective sections).
- Tests that prove the change works, including failure paths for anything retryable.
- No generated-file edits (`*.pb.go`, `mock_*.go`) — regenerate instead.
- Docs updated when behavior changes; new docs placed per [docs/README.md](docs/README.md).

## Definition of Done

The full checklist lives in [AGENTS.md § Definition of done](AGENTS.md#definition-of-done). In
short, before a PR is ready:

- [ ] The four checks in [AGENTS.md § Definition of done](AGENTS.md#definition-of-done) pass
- [ ] New or changed logic is covered by tests ([AGENTS.md § Testing](AGENTS.md#testing))
- [ ] Dependencies changed → `go mod tidy` ([AGENTS.md § Dependencies](AGENTS.md#dependencies))
- [ ] Generated output regenerated, never hand-edited ([AGENTS.md § What NOT to do](AGENTS.md#what-not-to-do))

## Running the tests

Unit tests need nothing but the Go toolchain:

```sh
go test -race -count=1 ./...
```

Integration tests are behind the `integration` build tag and need a Docker daemon, because they
boot real MongoDB, RabbitMQ, PostgreSQL, and Temporal containers:

```sh
make test-integration   # the suite as a capable machine runs it
make test-weak          # the same suite configured for a 4 GB / 4 CPU machine
```

`make test-weak` also runs one package at a time (`-p 1`). Each package boots its own container
stack, so Go's default of one package per CPU runs several stacks at once; the weak run trades wall
clock for the concurrent-container count its budget is about.

A package's integration tests share one container stack — one MongoDB, one RabbitMQ, and one
Temporal server per package — and isolate themselves from each other by database name, by purging
the broker queues they use, and by namespace. That is what keeps a package's peak memory from
growing with its test count, and it is why a test that stops or restarts a shared service must not
call `t.Parallel()`: it would be pulling the service out from under a test running beside it.

### Configuring a test run

A run's configuration comes from these variables and from nothing else. The suite never inspects
the host's memory, CPU count, or load, so the same environment runs the same assertions on every
machine and a failure stays reproducible somewhere else.

| Variable | Values | Default | What it sets |
|---|---|---|---|
| `FLEETOPS_TEST_PROFILE` | `default`, `weak` | `default` | the profile; a variable below overrides it |
| `FLEETOPS_TEST_FLEET_SIZE` | positive integer | `1000` | the largest simulated fleet a test may run |
| `FLEETOPS_TEST_WAIT_MULTIPLIER` | number greater than zero | `1` | scales the wait budgets a test documents for itself |
| `FLEETOPS_TEST_CONTAINER_STARTUP` | positive duration | each harness's own (`60s` MongoDB, `90s` RabbitMQ, `3m` Temporal) | overrides every container-startup budget |
| `FLEETOPS_TEST_CONTAINER_USE` | `shared`, `per-test` | `shared` | one stack per package, or one per test |

The weak-host profile targets a machine with at most 4 GB of RAM and 4 CPUs: it narrows the
simulated fleet to 100 devices, halves the wait budgets, leaves a container 45s to start, and
shares one stack per package. Narrowing is by workload and budget only — a test under the weak
profile asserts exactly what it asserts under the default — and a test the profile cannot afford
is skipped with a message naming the variable that would allow it, never run with weaker
assertions.

A value that cannot be used fails the run before any test body, naming the variable and the value
it rejected:

```sh
FLEETOPS_TEST_PROFILE=tiny go test -tags integration ./internal/mongotest/
# test configuration: FLEETOPS_TEST_PROFILE: undefined test profile "tiny" (defined: default, weak)
```

## License

By contributing, you agree that your contributions are licensed under the MIT License (see
[LICENSE.md](LICENSE.md)).
