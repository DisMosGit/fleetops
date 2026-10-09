# Proposal

## Why

Nine changes under `openspec/changes/` are complete in code but cannot be archived. Archiving
merges a change's delta into `openspec/specs/`, and the CLI refuses a `## MODIFIED Requirements`
block that drops a scenario the current base spec contains — a guard against silently deleting a
requirement. Seven of the nine fail that guard on `runtime-config`, because their MODIFIED blocks
were hand-written against a base spec that no longer exists.

The drift has a single origin. On 2026-10-09 the baseline was established from seven archived
changes, and the `runtime-config` delta of
`add-heartbeat-persistence-and-device-liveness` is what put the current text there: its MODIFIED
blocks carry all three `runtime-config` requirements, and they are the sole source of
`Liveness and telemetry sections default` and `Invalid liveness or telemetry value is rejected`.
Every later change that reshapes `runtime-config` authored its block from the base as it stood
*before* that change landed, so each one omits those scenarios and renames them instead. One
change's content, seven stale copies.

The consequence is that **26 scenario blocks across 7 delta files are missing from blocks that
would replace the base**, and no ordering fixes it. Reordering was tested exhaustively: the order
is already dependency-correct, and because the defect is content rather than sequence, the best a
reordering achieves is stranding a different subset — moving the heartbeat change to the end
archives 13 changes instead of 9, but permanently strands `device-liveness`, `device-registry`,
and `telemetry-ingest`, which that change and no other creates. The remaining work is to refresh
the stale deltas and archive.

Until this lands, the durable capability baseline understates the system: 24 capability specs
exist only inside unarchived deltas, so `openspec/specs/` describes 13 capabilities while the
repository implements 37.

## What Changes

- **Seven `runtime-config` delta files gain the scenario blocks they drop.** Each MODIFIED block
  is refreshed so it is a superset of the requirement it replaces, copying the missing scenario
  blocks verbatim from `openspec/specs/runtime-config/spec.md`. The count is 26 scenario copies
  across the three requirements of the one capability: 12 across six files (`Single YAML
  configuration file` and `Configuration validation fails fast`, 2 each) and 14 in
  `fix-dispatch-task-queue-separation` (`Single YAML configuration file`,
  `Configuration validation fails fast`, and `Entrypoints consume the shared configuration`).
- **Nine changes are archived** once their deltas pass validation, in the dependency order the
  earlier analysis verified: `add-firmware-pipeline`,
  `add-device-search-attributes-snapshot-and-worker`, `add-telemetry-event-pipeline`,
  `add-wave-health-aggregation`, `add-rollout-workflow`, `add-rollout-control-surface`,
  `add-rollback-saga`, `fix-dispatch-task-queue-separation`, and
  `add-rollout-durability-verification`. The eight missing-target-spec refusals the CLI currently
  reports are second-order: they disappear as the drift above is repaired and the changes that
  create those bases land. Archiving merges **24 new capability specs** into `openspec/specs/`,
  taking it from 13 capabilities to 37.
- **The delta-composition rule is recorded where it is acted on.** The rule that a MODIFIED block
  must be a superset of the requirement it replaces, and that a whole-file rewrite of a
  requirement silently drops every scenario added in between, belongs in the repository rather
  than in this proposal — it is what the next author of a MODIFIED delta needs to know before
  writing one.
- **`openspec/config.yaml` stops restating volatile repository state.** Its context block asserts
  that `openspec/specs/` "is still EMPTY" and that all 16 changes are unarchived. Both claims are
  already false and would be false again after this change; a hand-maintained count in a context
  block is a claim that drifts between the moment it is written and the moment it is read. The
  block keeps its pointer style — the durable `ADDED`/`MODIFIED` rule stays, the state assertion
  is replaced by how to read the current state.

## Capabilities

### New Capabilities

None. This change introduces no capability: it reconciles delta documents with the base spec they
will merge into, and archives them.

### Modified Capabilities

None. The 24 capability specs this change causes to exist are already specified by the deltas it
archives; archiving migrates that specification into `openspec/specs/` unchanged in meaning. No
requirement's behavior is altered by this change, so it declares no delta and sets
`skip_specs: true` in `.openspec.yaml`. The one durable rule it establishes is about how OpenSpec
deltas are written, which is config, not an observable behavior of FleetOps — a capability such as
`spec-delta-hygiene` under `openspec/specs/` would describe the project's tooling rather than the
system the project builds.

## Impact

- **OpenSpec artifacts**: 7 delta files under
  `openspec/changes/<change>/specs/runtime-config/spec.md`; 9 change directories move to
  `openspec/changes/archive/`; `openspec/specs/` gains 24 capability directories (13 to 37).
- **Config**: `openspec/config.yaml`, context block only. The `rules` and `operations` sections are
  unchanged.
- **Code**: none. No Go, proto, deploy, or test file changes. This is the point of the change — it
  lands work already complete and verified, and the Definition of Done in AGENTS.md does not apply
  to it in the same form. The gate that does apply is `openspec validate --strict`.
- **Behavior**: none. Archived deltas describe behavior that has already landed; this change
  changes which document the behavior is recorded in, not the behavior.
- **Reviewability**: the delta refresh is a large mechanical diff. It is reviewable precisely
  because every repair is an addition of an existing block: no scenario text is authored, renamed,
  or reworded by this change, and the design records the reconciliation procedure so the same
  problem is not re-diagnosed from scratch next time.
- **Known residual**: where a change renamed a scenario rather than extending it, the merged spec
  carries both the base name and the new name describing near-identical behavior. This change
  records that rather than resolving it; removing a scenario from a merged spec is a separate
  decision with its own archive-time constraints.
- **Unblocks**: `README.md`'s stage table, which currently says stages 1–5 are "Not started", is
  the next stale claim in line, but correcting it is not in this change's scope.
