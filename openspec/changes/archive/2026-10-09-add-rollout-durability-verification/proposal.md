# Proposal

## Why

FleetOps' canary rollout is only as safe as its durable execution. A wave's membership, its health
window, the operator's approval, the pause that holds promotion, and the rollback saga are all
supposed to survive the worker process dying underneath them — and nothing proves that they do. The
one end-to-end test drives a real rollout, a gated approval, a pause/resume, and a rollback against
a real Temporal server, but it starts a single worker and stops it only when the test ends: not one
scenario observes what happens when the worker executing the rollout disappears mid-flight. The
restart guarantee lives in a doc comment (`RolloutWorkflow`: "a worker restart is a non-event") and
in two per-feature scenarios; the fake-environment unit tests replay a history they never really
lost. The smoke also shells out to the `temporal` CLI, so wherever that binary is not on `PATH` the
entire end-to-end surface skips silently.

The gap matters because the effects the rollout applies are not free: a firmware update command, a
downgrade restore, a `current_fw` correction, a rollback announcement. Temporal guarantees
at-least-once activity execution across a restart, not exactly-once effects — what keeps a second
delivery from becoming a second effect is the rollout's own deterministic identities and idempotent
writes. That property is argued in comments and unit tests today, never demonstrated against a
worker that actually stops between, and inside, the activities that carry it.

## What Changes

- **A restart matrix over the real stack.** A new integration suite drives the rollout through the
  real worker, real Temporal server, real fleet database, and real broker while restarting the
  worker between every pair of the rollout's activities — and, at every activity, also after that
  activity's side effect has landed but before its result was recorded, which is the case
  at-least-once execution actually makes dangerous.
- **Restarts are injected deterministically, not raced.** A test-only worker interceptor gates each
  activity attempt: it announces the attempt to the harness and applies the harness's decision —
  hold the attempt while the old worker stops and a fresh one starts, run it normally, or run it
  and then fail the attempt the way a worker that died after the effect would. A restart is
  therefore a fact the suite asserts on rather than a timing accident it hopes for.
- **The end-to-end scenarios the smoke only samples are covered on a harness that runs in CI**: a
  full multi-wave rollout that completes with every wave promoted and an approval consumed between
  two of them; a wave whose fleet regresses and rolls the rollout back with the compensations
  running in reverse wave order; and an inventory reconciliation check that every compensated
  device's fleet record matches the version that device actually runs — including the devices the
  rollback could not restore, which stay on the deployed firmware.
- **Durability, not just completion, is asserted.** Each scenario asserts the resumption point (the
  state query and the recorded wave documents after a restart equal what they were before it), that
  no work is re-decided (the history carries exactly one scheduled activity per logical unit), and
  that the effects stay exactly-once (one update command id per device per wave, one restore per
  compensated device, one reconciliation per device, one announcement per rollback phase), with the
  fleet's records converging instead of accumulating duplicates.
- **The completed histories are replayed.** A replayed run that flaps is a bug, so each scenario's
  recorded history is replayed against a fresh worker: a determinism break introduced by anything
  the suite drives fails the test instead of hiding in the Temporal store.
- **A containerized Temporal server harness** (`internal/temporaltest`, integration-only) boots the
  server the way `internal/mongotest` and `internal/rabbittest` boot MongoDB and RabbitMQ, so the
  new suite needs Docker rather than a `temporal` CLI on `PATH`. The existing CLI-based smoke is
  left as it is.
- **No product behavior change is planned.** The suite verifies behavior the rollout already
  claims. If it exposes a real defect, the fix and its own test belong to this change and are named
  in its tasks.

Explicitly not in this change: new rollout features (a checkpoint/resume API, per-wave retries, a
second saga), a rewrite of the existing smoke, any change to an activity's retry policy or to the
workflow's determinism rules, and any production refactor that a failure of this suite does not
demand.

## Capabilities

### New Capabilities

- `rollout-durability`: what a rollout guarantees when the worker executing it stops and another
  starts — where it resumes, that no wave, window, approval, or pause is lost or moved, that an
  activity whose result was recorded is never executed again, and that a retried activity converges
  on the same deterministic identity instead of a second effect.

### Modified Capabilities

- `firmware-inventory`: a new requirement that a rollback's reconciliation leaves the fleet's
  records agreeing with what the compensated devices actually run, including the devices a failed
  or unreported restore left on the deployed firmware.

## Impact

- **Code**: `cmd/worker` (a new integration test file carrying the restart harness, the activity
  gate, and the scenario matrix) and `internal/temporaltest` (a new integration-only harness
  package). Product code is untouched unless the suite fails; any such fix stays inside
  `internal/temporal`, `internal/devices`, or `internal/telemetry` and ships with its own test.
- **Dependencies**: none. `testcontainers-go` is already a direct dependency and no module is
  added. The Temporal server container image becomes a test-time requirement alongside the MongoDB
  and RabbitMQ images the integration suites already need.
- **Contracts**: none. No proto, broker routing key, collection, or configuration key changes; the
  suite reads the same registered activity names, statuses, and event identities the production
  code defines.
- **Runtime**: none. Everything new is behind `//go:build integration`, so the unit suite, the
  Definition of Done, and the worker binary are unaffected.
