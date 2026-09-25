# Design

## Context

The repository is greenfield: `go.mod` (module `github.com/DisMosGit/fleetops`), `LICENSE.md` (MIT),
`.gitignore`, a `web/` git submodule placeholder, `openspec/`, and `.agents/skills/`. There are no
commits yet and no Go code. The product intent lives in `.nda/idea_description.md` (gitignored,
private): an IoT-fleet OTA-update platform — Go control plane, Temporal entity/saga workflows,
gRPC bidi agent streams, MongoDB time-series telemetry, RabbitMQ fan-out, React UI — delivered in
5 stages. The current `AGENTS.md` is a generic Go guideline document that predates the project.
See proposal.md for motivation.

Constraints that shape the approach: documentation must not promise behavior that does not exist
yet; the `.nda` source must not leak into public files; the four doc surfaces must not drift into
contradiction; everything is written in English (user decision) and `AGENTS.md` is rewritten from
scratch (user decision), not incrementally edited.

## Goals / Non-Goals

**Goals:**

- One authoritative home per kind of knowledge, with cross-links instead of copy-paste.
- Docs stay honest at every point in the 5-stage delivery plan: each planned document has a
  trigger milestone, so it is written when its subject is verifiable.
- `AGENTS.md` becomes actionable for AI agents working in *this* repo: stack-specific rules they
  would otherwise get wrong (workflow determinism, idempotent activities, DLQ, GridFS, etc.),
  grounded in the planned layout from the idea source.

**Non-Goals:**

- Writing the future docs themselves (architecture, API, telemetry, observability) — this change
  only decides and records where they will live and when they are written.
- Translating or copying `.nda/idea_description.md` into the repo.
- Any code, manifest, CI, or `openspec/` spec changes.

## Decisions

1. **Strict content ownership per file (anti-duplication).**
   - `README.md` — *what and why*: FleetOps purpose, architecture summary, tech stack, repo map,
     status of each delivery stage, Getting Started (only what runs today), navigation links.
   - `CONTRIBUTING.md` — *how to contribute*: prerequisites, setup, branch/PR workflow, review
     expectations, Definition-of-Done checklist. Links to `AGENTS.md` for engineering rules
     instead of restating them.
   - `AGENTS.md` — *engineering rules for AI agents*: rewritten per-project (stack, layout,
     correctness/testing rules, Temporal/gRPC specifics, DoD commands). Human-oriented process
     (PR etiquette, review) lives in `CONTRIBUTING.md` only.
   - `docs/README.md` — docs index + the planned-docs backlog.
   Alternative considered: mirror the DoD in all three root files — rejected: guaranteed drift.

2. **Planned docs live in `docs/` as a backlog table inside `docs/README.md`** — each entry has
   purpose, audience, and a trigger milestone from the delivery plan (e.g. "API reference → after
   stage 1 proto freeze"). Root keeps exactly four top-level docs (`README.md`, `CONTRIBUTING.md`,
   `AGENTS.md`, `LICENSE.md`). Alternative considered: a separate `docs/ROADMAP.md` — rejected:
   extra file churn for a small backlog; the backlog is 6–8 rows, not a roadmap.

3. **Content gating on implementation stage.** The README's Getting Started documents only what
   exists at the time of writing (initially: prerequisites + layout + "under construction" stage
   status). Run commands are added by the stage that introduces them (compose/k3d, Temporal, UI).
   Alternative considered: write the full aspirational README now — rejected: unverifiable
   instructions rot immediately and mislead contributors.

4. **`AGENTS.md` rewrite keeps transferable rules, drops the rest.** The current generic Go rules
   are evaluated against the planned stack: Go idioms, error handling, testing, layout, gRPC and
   Temporal rules survive (all are in scope); anything not applicable to FleetOps is dropped
   rather than kept "just in case". New project-specific material: repository map matching the
   planned `cmd/` + `internal/` layout, workflow-determinism and activity-idempotency rules,
   telemetry/DLQ conventions, and DoD commands for the Go monorepo.

5. **NDA hygiene.** `.nda/` stays gitignored and is never linked from public docs. Public docs
   distill purpose/architecture/stack in their own words; no verbatim passages, and the open
   questions and risk table from the source stay private.

6. **`skip_specs: true` for this change** — documentation-only, no spec-level behavior. Future
   docs-content requirements, if ever needed, belong to the capabilities they document (e.g. the
   rollout capability spec says what the system does, not what its README must say).

## Risks / Trade-offs

- [Docs drift as stages land and docs are not updated] → Every backlog row carries a trigger
  milestone; the stage-5 task list (README + demo) is the natural refresh point; "status" table in
  README makes staleness visible.
- [AGENTS.md rewrite loses a useful generic rule] → The rewrite task explicitly diff-checks the
  old file's rule set against the new one before the old text is gone (git history also preserves
  it).
- [README over-promises] → Decision 3: only verified commands and behavior are documented;
  everything else is a labelled "planned" item in the status table.
- [Overlap between AGENTS.md and CONTRIBUTING.md reappears later] → Ownership table (Decision 1)
  is written into `docs/README.md` as the living rule for where new doc content goes.

## Migration Plan

Docs-only change: no deployment, no migration, rollback = revert the commit. The previous
`AGENTS.md` remains recoverable from git history once the initial commit lands.

## Open Questions

None — remaining unknowns (exact wording, table shapes) are editorial and do not affect scope or
task breakdown.
