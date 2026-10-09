# Design

## Context

See `proposal.md` — Why. The implementation constraints:

- `openspec/specs/` holds **13 capability specs**; `openspec/changes/` holds **9 active changes**
  and **7 archived** ones dated `2026-10-09-<name>`.
- The 9 active changes carry **24 capability deltas** that no main spec accounts for. Those become
  24 new capability specs on archive.
- `runtime-config` is the only drifting capability. Its base spec has three requirements —
  `Single YAML configuration file` (5 scenarios), `Configuration validation fails fast` (4), and
  `Entrypoints consume the shared configuration` (3) — and the two scenario names in question were
  both introduced by the archived `add-heartbeat-persistence-and-device-liveness` delta, which
  MODIFIES all three requirements.
- `openspec validate --strict` currently fails for 7 of the 9 active changes; the refusal is
  reproduced by `openspec validate --changes --strict`.
- The blocking condition and both candidate orders were verified end to end in sandbox copies of
  this repository before this change was written. The order is dependency-correct and needs no
  correction; the drift is content.

## Goals / Non-Goals

**Goals**

- The 9 active changes validate and archive, leaving 16 archived / 0 active.
- `openspec/specs/` grows from 13 to 37 capability specs and passes `--strict`.
- The delta-composition rule is recorded where the next author of a MODIFIED delta will read it.
- The context block of `openspec/config.yaml` stops asserting repository state, so it cannot go
  stale.

**Non-Goals**

- Restructuring the affected deltas into `## ADDED` + `## MODIFIED` splits. It is the structural
  fix for this class of drift, but it changes what each delta means and what the merge produces,
  so it needs its own change and its own verification.
- Removing the redundant scenarios that result where a change renamed one (see Decisions).
- Any change to product code, to `*.pb.go`, or to the OpenSpec CLI.
- Committing or pushing. This change produces working-tree edits only.

## Decisions

**1. Copy the missing scenario blocks back, verbatim, into the delta.**

For each refusal, extract the named scenario blocks from the requirement block in
`openspec/specs/runtime-config/spec.md`, append them to the matching block under
`## MODIFIED Requirements` in the delta, and retry. The repair adds content only.

*Chosen over* splitting the delta into ADDED + MODIFIED: a split decides which requirement each
scenario belongs to and changes what the merge emits, so it would need its own verification to be
trustworthy. *Chosen over* recomposing each delta from today's base: semantically the cleanest, but
it rewrites text the change always carried, producing a diff where the real edits and the
mechanical reconciliation are indistinguishable.

**2. The reconciliation is a procedure, not a hand edit.**

It must be reproducible, because a reviewer cannot check 26 copied blocks by eye:

1. Run `openspec validate "<change>" --strict`; read the first
   `current spec contains scenario(s) not present in the modified block` refusal. It names the
   requirement header and each dropped scenario.
2. Locate that `### Requirement:` block in `openspec/specs/runtime-config/spec.md` and copy each
   named `#### Scenario:` block, with its body, exactly.
3. Append the copied blocks to the same requirement block under `## MODIFIED Requirements` in the
   delta, preserving their relative order from the main spec.
4. Re-run. One repair can expose the next refusal, so a delta may need several rounds; six deltas
   need two repairs and one needs fourteen.

The property to preserve is the one the CLI asserts: after repair, every scenario name in the base
requirement is present in the delta's replacement block. That is checkable mechanically per
requirement, and it is the acceptance condition for the refresh — not "the archive succeeds",
which is the consequence.

**3. Where a change renamed a scenario, both names survive in the merged spec.**

`add-rollout-control-surface` and `fix-dispatch-task-queue-separation` rename
`Liveness and telemetry sections default` to a snapshots-aware variant. Copy-back makes the merged
spec contain both names describing near-identical behavior.

*Chosen over* dropping the base name in the same edit: it is not this change's call to decide that
a scenario should cease to exist, and removing a scenario is exactly what the guard exists to
prevent. The redundancy is recorded in the proposal's Known residual and is a candidate for a
later change that owns that decision.

**4. The governance rule lives in `openspec/config.yaml`; there is no delta for it.**

The rule — a `## MODIFIED` block replaces the requirement and must therefore be a superset of it,
so a whole-file rewrite silently drops whatever landed in between — is about how deltas are
written. It is a config rule, not observable behavior of FleetOps, so it does not belong under
`openspec/specs/`. Adding a capability such as `spec-delta-hygiene` would make the project's spec
baseline describe its own tooling. This change therefore sets `skip_specs: true`, which the
`specs` artifact's instruction defines for exactly this case.

**5. The context block points at state instead of asserting it.**

The block already declares its own principle: it is "deliberately a pointer rather than a copy" so
it cannot drift from its sources. The state claim violates that principle — `openspec/specs/` is
described as "still EMPTY" when it holds 13 specs, and "all 16 changes are unarchived" when 7 are
archived.

The replacement keeps the durable rule and drops the counts. Nothing is lost by dropping them:
`openspec list --changes` reports active changes, `openspec list --specs` reports the capability
inventory, and a reader who wants the count runs the command. Restoring a count would only
re-introduce the same staleness with a newer number.

The two `operations.archive` guidance entries about dependency order and base-spec existence stay.
They were motivated by the empty baseline, but they remain true of any capability with no base
spec, and they are operations guidance rather than a state claim — so removing them would lose
real instruction. Only the false claim goes.

## Risks / Trade-offs

- **A 26-block mechanical diff across 7 files is hard to review by eye** → the diff is auditable by
  construction: filtering it with `git diff --diff-filter=M` and checking that every hunk is a pure
  insertion with no deletion proves no scenario text was rewritten. That check is a task, not an
  intention.
- **The usual Definition of Done does not apply, so the safety net is thinner** → AGENTS.md's four
  commands gate Go code, and this change touches no Go code. The gates that do apply are named as
  tasks: `openspec validate --strict`, `git diff --stat` checked against the 26-copy ledger, and
  the resulting spec count. Stating this openly is better than running the four commands to look
  rigorous.
- **A config edit that the CLI reads while authoring the change that makes it** → the context block
  is a statement of intent; the artifacts written here already satisfy the revised text, so the
  ordering is safe. The edit is context-block-only, and `rules` and `operations` are untouched, so
  the guidance governing these same artifacts does not change underneath them.
- **Archiving is not trivially reversible: it merges into `openspec/specs/`** → the full sequence
  is run against a copy of the repository first, and the end state checked there (16 archived,
  0 active, 37 specs, strict validation clean, no duplicate scenario names within a requirement)
  before the same repairs are applied here. This was already done once while diagnosing, so the
  expected end state is known rather than hoped for.
- **The archive directory date prefix will read `2026-10-09-`** → that is existing repository
  convention from the earlier baseline archive, applied automatically by the CLI. Naming is not
  being revisited here.
- **The residual duplicate scenarios could be mistaken for intent** → the design records them, and
  the proposal names them as a known residual, so a later reader finds a decision rather than an
  accident.

## Migration Plan

1. Apply the reconciliation to a sandbox copy of `openspec/`, run the archive to convergence, and
   confirm the end state. No real file changes.
2. Apply the same repairs to the 7 deltas in the working tree and confirm each delta's replacement
   block is a superset of its base requirement.
3. Archive the 9 changes in dependency order, validating between groups.
4. Update `openspec/config.yaml` last, so the commit that changes the context block is the one that
   makes its statement true again.

Rollback: the delta refresh is additive, so reverting the 7 files restores the pre-change state.
An archive already applied is undone by moving the directory back out of `changes/archive/` and
reverting the corresponding merge in `openspec/specs/` — which is why step 1 runs first.

## Open Questions

None. Both scope decisions that would have changed the artifacts — whether to declare a capability
delta, and how to make the config flexible — were settled with the user before writing.
