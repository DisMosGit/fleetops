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

Not needed yet — required when the stage that needs them lands:

- **Docker** — **planned**, stage 1
- **k3d / kubectl** — **planned**, stage 1
- **Temporal CLI / UI** — **planned**, stage 2
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

## License

By contributing, you agree that your contributions are licensed under the MIT License (see
[LICENSE.md](LICENSE.md)).
