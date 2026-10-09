# Proposal

## Why

FleetOps is a greenfield project (Go control plane, Temporal workflows, IoT fleet OTA rollouts)
that currently has no README, no contribution guide, and a generic Go `AGENTS.md` that does not
mention the project's stack, layout, or conventions. The product idea lives only in
`.nda/idea_description.md`, which is gitignored and cannot be shared with contributors. Without an
entry-point document set, neither humans nor AI agents know what FleetOps is, how it is organized,
or what "done" means — and every future change re-derives that context from scratch.

## What Changes

- **Rewrite `AGENTS.md` from scratch** as a project-specific engineering guide for FleetOps:
  stack-aware rules (Go, Temporal workflow determinism, gRPC bidi, MongoDB, RabbitMQ), the actual
  planned repository layout, testing and Definition-of-Done commands — replacing the current
  generic Go guideline document. (Per user decision: full rewrite, not an incremental edit.)
- **Add `README.md`** — the project's front door: what FleetOps is, architecture overview and tech
  stack (public-safe summary of the idea source), repository map, and a Getting Started section
  that grows with the implementation (content is gated on what actually exists, so no run
  instructions are documented before the code can run).
- **Add `CONTRIBUTING.md`** — how to contribute: prerequisites, development setup, branch/PR
  workflow, review expectations, and the Definition-of-Done checklist, cross-referencing
  `AGENTS.md` for engineering rules instead of duplicating them.
- **Add a documented plan for future `.md` docs** (`docs/README.md` as the docs index plus a
  backlog of planned documents — architecture, API contracts, Temporal workflows, telemetry
  pipeline, observability, demo/portfolio guide — each with purpose, audience, and the milestone
  after which it becomes writable). This keeps docs in sync with the 5-stage delivery plan instead
  of being written speculatively.

All changes are English-language documentation at repository root and under `docs/`; no code,
configuration, or runtime behavior changes.

## Capabilities

### New Capabilities

None. This change is documentation-only and introduces no spec-level system behavior, so it sets
`skip_specs: true` per the schema's guidance instead of inventing requirements.

### Modified Capabilities

None (no existing specs).

## Impact

- **Files created/replaced:** `AGENTS.md` (rewritten), `README.md` (new), `CONTRIBUTING.md` (new),
  `docs/README.md` (new, docs index + backlog).
- **Audience:** human contributors and AI coding agents working in this repository.
- **Source material:** `.nda/idea_description.md` remains private (gitignored); its public-safe
  content (purpose, architecture, stack, roadmap stages) is distilled into `README.md` and the docs
  backlog. No NDA-covered details are copied verbatim.
- **No impact on:** Go code, `go.mod`, deploy manifests, CI, or runtime behavior. Nothing is
  broken or migrated.
