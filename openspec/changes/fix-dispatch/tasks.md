# Tasks

## 1. Dry-run the whole sequence in a sandbox

- [x] 1.1 Copy `openspec/` to a scratch directory outside the repository, apply the reconciliation
  of `design.md` Decision 2 against the copy, and run `openspec archive "<change>" --yes --json`
  repeatedly in the order of `design.md`'s Migration Plan step 3 until a pass archives nothing
  new; verify the copy ends with 16 directories under `openspec/changes/archive/`, zero active
  changes, and 37 capability directories under `openspec/specs/`
- [x] 1.2 Confirm the sandbox end state is internally consistent rather than merely moved: `openspec
  list --specs --json` reports 37 specs, `openspec validate --specs --strict` passes, and no
  requirement block in `openspec/specs/runtime-config/spec.md` contains the same
  `#### Scenario:` name twice; record the observed counts as the acceptance baseline for group 3

## 2. Refresh the seven drifted deltas

- [x] 2.1 Fix `add-firmware-pipeline`: copy `Liveness and telemetry sections default` into the
  `Single YAML configuration file` block and `Invalid liveness or telemetry value is rejected` into
  the `Configuration validation fails fast` block of
  `openspec/changes/add-firmware-pipeline/specs/runtime-config/spec.md`, then verify with
  `openspec validate add-firmware-pipeline --strict --type change` reporting no omitted-scenario
  error
- [x] 2.2 Fix `add-device-search-attributes-snapshot-and-worker` with the same two copies in
  `openspec/changes/add-device-search-attributes-snapshot-and-worker/specs/runtime-config/spec.md`,
  then verify with `openspec validate add-device-search-attributes-snapshot-and-worker --strict --type change`
- [x] 2.3 Fix `add-telemetry-event-pipeline` with the same two copies in
  `openspec/changes/add-telemetry-event-pipeline/specs/runtime-config/spec.md`, then verify with
  `openspec validate add-telemetry-event-pipeline --strict --type change`
- [x] 2.4 Fix `add-wave-health-aggregation` with the same two copies in
  `openspec/changes/add-wave-health-aggregation/specs/runtime-config/spec.md`, then verify with
  `openspec validate add-wave-health-aggregation --strict --type change`
- [x] 2.5 Fix `add-rollout-workflow` with the same two copies in
  `openspec/changes/add-rollout-workflow/specs/runtime-config/spec.md`, then verify with
  `openspec validate add-rollout-workflow --strict --type change`
- [x] 2.6 Fix `add-rollout-control-surface` with the same two copies in
  `openspec/changes/add-rollout-control-surface/specs/runtime-config/spec.md`, then verify with
  `openspec validate add-rollout-control-surface --strict --type change`
- [x] 2.7 Fix `fix-dispatch-task-queue-separation`, whose MODIFIED blocks need fourteen copies
  across three requirements — `Single YAML configuration file`, `Configuration validation fails
  fast`, and `Entrypoints consume the shared configuration` in
  `openspec/changes/fix-dispatch-task-queue-separation/specs/runtime-config/spec.md` — retrying
  after each round because one repair exposes the next; verify with `openspec validate
  fix-dispatch-task-queue-separation --strict --type change` reporting no omitted-scenario error
- [x] 2.8 Prove the refresh is additive: verify `git diff --diff-filter=M --numstat` over the seven
  delta files reports insertions with zero deletions, so no scenario text was reworded or removed,
  and that `git diff` contains no change outside `specs/runtime-config/spec.md` in those seven
  directories
- [x] 2.9 Assert the acceptance condition directly rather than inferring it from a passing archive:
  verify that for every requirement a delta places under `## MODIFIED Requirements` that the base
  spec also has, each base scenario name appears in that block — scoped to declared requirements,
  because a delta may legitimately leave a requirement alone (`add-firmware-pipeline` declares two
  of the three) and the CLI demands no superset for one it never mentions — and that
  `openspec validate --changes --strict` reports 0 failed

## 3. Archive the nine changes and make the context true

- [x] 3.1 Record the pre-archive state for comparison: verify `openspec list --specs --json` reports
  13 specs and `openspec list --json` reports the 9 archivable changes as active — 10 in total,
  because this change is itself still active and archives separately — before any archive runs
- [x] 3.2 Archive the six drift-repaired changes in dependency order — `add-firmware-pipeline`,
  `add-device-search-attributes-snapshot-and-worker`, `add-telemetry-event-pipeline`,
  `add-wave-health-aggregation`, `add-rollout-workflow`, `add-rollout-control-surface` — then
  verify `openspec validate --changes --strict` reports no omitted-scenario error remaining, confirming each
  archived change's deltas actually merged by checking `openspec list --json` no longer reports it
  active and that each capability it declared now exists under `openspec/specs/`
- [x] 3.3 Archive the three remaining changes — `add-rollback-saga`,
  `fix-dispatch-task-queue-separation`, `add-rollout-durability-verification` — and verify
  `openspec list --json` reports 0 active changes and `openspec list --specs --json` reports 37
  specs, both matching the group 1 sandbox baseline
- [x] 3.4 Confirm the merge preserved the scenarios this change existed to protect: verify
  `openspec/specs/runtime-config/spec.md` still contains `Liveness and telemetry sections default`
  and `Invalid liveness or telemetry value is rejected`, and that `openspec validate --specs
  --strict` passes
- [x] 3.5 Update the context block of `openspec/config.yaml` per `design.md` Decision 5: keep the
  `ADDED`/`MODIFIED` rule and the `openspec/specs/` pointer, and remove the claims that specs "is
  still EMPTY" and that all 16 changes are unarchived; verify the file still parses as YAML, that
  `git diff` on the file touches only the `context:` block so its `rules` and `operations` sections
  are unchanged, and that the CLI still serves the text — `openspec instructions apply --change
  fix-dispatch --json` returns a non-empty `context` carrying the new text. (`openspec context
  --json` reports root and membership only; it does not echo the context, so it cannot show this.)

## 4. Record the outcome

- [x] 4.1 Compare the working tree against the group 1 sandbox and record any divergence in the
  `## Recorded verification` section below, including the final capability count, the number of
  scenario blocks copied, and the per-change repair counts; verify the recorded numbers match
  `openspec list --specs --json` and `git diff --numstat` on the day they are written rather than
  being carried over from the sandbox

## Recorded verification

Recorded 2026-10-09 against the working tree, not carried over from the sandbox. Where a number
differs from the prediction in `proposal.md`, the difference is stated rather than smoothed.

**Outcome**

| measure | before | after | source |
| --- | --- | --- | --- |
| capability specs | 13 | 37 | `openspec list --specs --json` |
| archived changes | 7 | 16 | `openspec/changes/archive/` |
| active changes | 10 | 1 | `openspec list --json` — the 1 is this change |
| `validate --specs --strict` | — | 37 passed, 0 failed | |
| `validate --changes --strict` | 3 passed, 7 failed | 1 passed, 0 failed | |
| new capability dirs merged | — | 24 | 19 from the first six, 5 from the last three |

**Repairs: 26 scenario copies across 7 deltas, 15 repair operations.** Measured by diffing each
delta's content at `HEAD` against its archived content, so the number is derived from the working
tree rather than restated from the sandbox.

| change | copies |
| --- | --- |
| `add-firmware-pipeline` | 2 |
| `add-device-search-attributes-snapshot-and-worker` | 2 |
| `add-telemetry-event-pipeline` | 2 |
| `add-wave-health-aggregation` | 2 |
| `add-rollout-workflow` | 2 |
| `add-rollout-control-surface` | 2 |
| `fix-dispatch-task-queue-separation` | 14 (5 + 5 + 4 across its three requirements) |

The refresh is additive: **zero scenarios removed** from any delta, and the 7 deltas grew by `+174`
lines with `-0` deletions. Merged `runtime-config` ends at 11 / 10 / 9 scenarios across its three
requirements, with no duplicate scenario name inside any requirement.

**Divergences from the sandbox, both deliberate**

1. **Archive directory dates.** The CLI named the nine new directories `2026-10-09-*` (the day they
   ran).
2. **Group 2 was applied as one batched edit, not seven staged ones.** The sandbox had already
   computed the reconciled content for all seven deltas, so that same verbatim content was applied
   at once instead of reproducing the intermediate refusals between 2.1 and 2.7. The end state is
   identical — all 7 deltas are byte-identical to the sandbox's verified versions — and the copy
   counts above are unchanged. Task 2.7's "one repair exposes the next" describes how the sandbox
   derived the content, not the order it was written into the working tree.

**Three task verifications were corrected during implementation, because they named a check the
CLI does not make**

- **2.9** required every base scenario of all three requirements to appear in all seven deltas.
  `add-firmware-pipeline` declares only two of the three, and the CLI demands no superset for a
  requirement a delta never mentions — it passes `--strict` legitimately. The check is now scoped
  to declared requirements; under that scoping all seven are supersets.
- **3.1** said `openspec list --json` would report 9 active changes; it reports 10, because this
  change stays active until it is archived. The task now names the 9 archivable changes and
  accounts for the tenth.
- **3.5** verified the config through `openspec context --json`, which reports root and membership
  only and never echoes the context text — it would have passed whether or not the edit was ever
  served. The check now uses `openspec instructions apply --change fix-dispatch --json`, which does
  return the text; `rules` and `operations` are byte-identical to `HEAD`.

**Known residual, unchanged by this work.** Where a change renamed a scenario rather than extending
it, the merged spec carries both names describing near-identical behavior — for example
`Liveness and telemetry sections default` alongside `Liveness, telemetry, and snapshots sections
default`. This change records that rather than resolving it: removing a scenario from a merged spec
is the operation the archive guard exists to prevent, so it needs its own change. `design.md`
Decision 3 owns that decision.

**Still outstanding, outside this change's scope.** `README.md`'s stage table still says stages 1–5
are "Not started"; `deploy/` still has no compose or k3d manifests, so `make up`/`make down` do
nothing; `add-device-search-attributes-snapshot-and-worker` tasks 5.3 and 6.2 remain deferred; and
the recorded `TestRolloutSmoke` skip plus two defects in that test are unfixed.
