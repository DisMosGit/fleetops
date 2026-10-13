# Proposal

## Why

The functional intent for FleetOps — the agent emulator, the Temporal workflows, canary rollout with
health-gated promotion and saga rollback, gRPC bidi streaming, and the RabbitMQ telemetry fan-out —
was written before any code existed, and the repository has never checked its delivery against it
term by term. That check has now been done against the 37 capability specs and the 107 test files.
Three consequences are measurable.

**First, three requirements have no implementation at all.**

- The device pool a rollout resolves its waves from is recomputed by a direct query every time
  (`internal/rollout/store.go` → `eligiblePool`), and the repository contains no MongoDB change
  stream anywhere — `.Watch()` does not appear in the module. Eligibility is therefore derived by
  querying on demand, not kept fresh by the change stream the requirement calls for.
- `FirmwareWorkflow` was never built: the package documentation states plainly that it "is the stage
  still deferred" (`internal/temporal/doc.go`), and firmware upload is a synchronous HTTP handler
  over GridFS with no workflow owning it.
- **Fault injection in the emulator does not exist.** There is no stick, stall, or fault knob in
  `internal/agent` or `internal/config`; `UpdateHandler.handleUpdate` reports the first download
  error and never retries (`internal/agent/update.go`). The feature whose whole purpose is to
  demonstrate retry, backoff, and the dead-letter path is the one that cannot demonstrate them.

**Second, the suite has never been asked whether it tests the thing that was asked for.** Some
requirements are covered thoroughly — idempotent ingestion and the consumer retry ladder against a
real broker are the strongest area of the suite. Others are covered only by proxy. The clearest
example is structural: **no test drives a rollout to a real agent.** Both rollout integration suites
bind the device command seam to an in-process recorder, and every streaming test ignores rollouts,
so a regression in the shape of the command the rollout builds, or in the hub that routes it, leaves
both suites green. `cmd/agent` — the binary that runs the fleet — has no test file at all.

The coverage question is currently unanswerable from the repository. There are 534 scenarios across
37 capability specs and no mapping from a scenario to a test, so nobody can tell what is verified
without reading everything.

**Third, the suite is not runnable on the machines most likely to run it.** Nothing in the suite
reads an environment variable, and there is no way to ask for a cheaper or more patient run. The
costliest package is `cmd/worker`: eight tests each boot MongoDB + RabbitMQ + PostgreSQL + Temporal
containers, plus a smoke test that starts a Temporal dev server subprocess, and none of them call
`t.Parallel()` — 32 container starts in one binary, with the smoke alone measured at 8–10 minutes
and the whole integration run exceeding the default per-package timeout. Measured on this host,
MongoDB and RabbitMQ idle at ~215 MB for the pair before the test process, the schema bootstrap, or
`-race` overhead; with Go's default package parallelism (`-p` = `GOMAXPROCS`) several such stacks
overlap, which is how a 4 GB / 4 CPU machine is exhausted and how the suite produces
`context deadline exceeded` failures that read like product bugs.

## What Changes

- **The eligible device pool is kept fresh by a MongoDB change stream.** A watcher on the `devices`
  collection maintains the derived per-selector eligibility view as devices register, re-register,
  change model, or change status, and wave resolution reads that view instead of recomputing it by
  query. The view is derived and rebuildable: a lost stream resumes from its token, and a token the
  oplog no longer holds flushes the view rather than serving stale eligibility. **This does not
  change when a wave's membership is frozen** — the base spec's rule that a wave resolves once and is
  never re-derived stands untouched; what changes is where the pool it resolves against comes from.
- **Firmware upload is owned by a `FirmwareWorkflow`.** The upload's steps become activities under
  one workflow execution addressed by a stable identity derived from the firmware version, so a
  retried upload converges on one firmware record, and the record carries the upload's state with a
  queryable outcome. The existing guarantees are preserved: a checksum computed while streaming, a
  GridFS binary with a metadata-only document, target-model validation, and version-conflict
  rejection with no orphaned binary.
- **The agent emulator gains fault injection.** A configured share of the fleet can be made to stall
  or fail a firmware download, and the update path retries with bounded backoff before reporting the
  failure as the command's outcome — which is what drives a poison report through the consumer's
  retry ladder into the dead-letter queue. The emulator's simulated faults stay seeded, so the same
  seed replays the same faults.
- **The functional requirements the tests do not cover gain tests**, including the structural gap
  above: a test that joins a rollout to a real agent stream instead of an in-process stub, and tests
  for the `cmd/agent` binary's wiring. The mapping from scenario to backing test is recorded in the
  repository and checked by a command, so a scenario added without a test fails the check.
- **The test suite becomes configurable from the environment.** A test-profile surface reads
  explicit `FLEETOPS_TEST_*` variables and applies them to fleet size, wait budgets, container
  startup budgets, and container use. Defaults preserve current behavior on a capable host; a
  documented weak-host profile targets 4 GB RAM / 4 CPU. Test behavior never auto-detects host
  resources, so the same commit runs the same assertions everywhere.
- **`cmd/worker`'s heavy suites share one container stack per test package** instead of booting four
  containers per test, with per-test isolation by database, namespace, and queue purge. This is the
  single largest reduction in peak memory, and it is what makes the weak-host profile affordable
  without deleting assertions.

### Recorded but not implemented here

Five things the check found are **not** implemented by this change. They are named so they are
tracked rather than silently absent:

- **`GET /api/rollouts/{id}/stream` (SSE) and `GET /api/devices` do not exist.** Both are
  operator-surface endpoints the original intent lists, and a control-plane test currently pins
  `/api/devices` to `404`. The SSE stream is the frontend's live-status mechanism, and the device
  list is its other read; both are excluded by this change's frontend boundary and are recorded as
  gaps for the stage that builds that surface.
- **A running update cannot be stopped.** `CommandKindAbort`, the dispatch case that builds an
  `AbortUpdate` command, and the contract's command set are all in place, but nothing creates an
  abort command and the agent logs one and ignores it. The contract requirement is met — it requires
  the command set to be *defined*, which it is — and no capability spec requires the cancellation
  semantics, so this is a gap between the contract and the emulator rather than a violated
  requirement. It is recorded, not fixed: adding stop-a-running-update semantics is its own change.
- **`telemetry` is not a MongoDB time-series collection**, although
  `openspec/specs/mongo-data-model/spec.md` requires one: `deploy/mongo/init.js` creates a plain
  collection with a TTL index. The reason is sound — a time-series collection cannot enforce the
  unique `_id` the idempotent-ingestion requirement depends on — so the deviation is deliberate and
  the spec is what is wrong. Reconciling the spec with the implementation is a spec edit, not a code
  change, and is left to the change that owns that capability.
- **Telemetry retention is not one named value shared as the spec describes.** The TTL index is
  built from `FLEETOPS_TELEMETRY_RETENTION_DAYS` in the bootstrap script, and `internal/config` has
  no retention field, so the ingestion path does not read the same value.
- **The rollout work queue has no consumer.** It is declared and bound with a dead-letter queue, but
  nothing consumes it and only the heartbeat queue's dead-letter path is exercised.

### Explicitly not in this change

- **Unbacked requirements are not deleted to make coverage look complete.** A scenario is closed
  with a test, or it is recorded as exempt with the reason and what would verify it.
- **Observability, deployment, and the React frontend are out of scope**: no OTel, Prometheus, or
  Grafana work; no compose/k3d manifests or stack bring-up; no UI. The four scenarios that need a
  running local stack to verify (the worker replica and lifecycle scenarios, and the Temporal-UI
  device filter check) stay deferred to the delivery work that gives them a stack, and this change
  records that rather than faking them.
- **Production MongoDB topology is not changed.** The local stack runs a standalone MongoDB, which
  does not serve change streams, so the watcher needs a replica set in the environments that run it.
  The test harness must be able to provide one; what a production deployment does is deployment work.

### Why this lands now, against the staged plan

Every feature in this change belongs to a stage `README.md` already lists as delivered: the
change-stream-maintained eligible pool and the dead-letter and queue work are stage 3 (streaming and
telemetry), `FirmwareWorkflow` was deferred from stage 2, and fault injection is the emulator's
stage-1 showcase. Nothing here waits on a stage that has not landed, and nothing here is
observability, deployment, or frontend work. The one genuine prerequisite is environmental rather
than a delivery stage: the environments that run the worker must serve MongoDB change streams.

## Capabilities

### New Capabilities

- `device-eligibility-pool`: keeping the fleet's eligible-device view fresh from MongoDB change
  streams — what a device change updates, how the view is derived and rebuilt, how a lost stream
  resumes, and what a wave resolution reads.
- `firmware-workflow`: one workflow owning a firmware upload end to end — the stable workflow
  identity per version, the upload steps as activities, the record's state transitions, and the
  queryable state that replaces a request held open.
- `agent-fault-injection`: the emulator's simulated download faults and bounded retry — how faults
  are configured and seeded, how the update path retries them, and how a fault that outlasts its
  retries becomes the command's reported outcome and reaches the dead-letter path.
- `test-execution-profiles`: the environment variables, defaults, and per-run behavior that decide
  how much of the suite runs and with what budgets — including the weak-host profile and the rule
  that behavior is never chosen by probing the host.
- `functional-requirement-coverage`: the contract that every capability scenario is backed by a test
  or exempt with a named reason, including the structural case of a rollout driven through a real
  agent stream rather than an in-process stub.

### Modified Capabilities

- `temporal-worker`: the worker's registration set gains the firmware workflow and the eligible-pool
  watcher's lifecycle, and the weak-host configuration affects how the worker's suites run.

## Impact

- **Code, new**: an eligibility watcher and its derived view (in the package owning the rollout
  store, `internal/rollout/`), a `FirmwareWorkflow` and its activities in `internal/temporal/`, the
  emulator's fault knobs in `internal/agent/`, and a test-profile helper package under `internal/`.
- **Code, changed**: `internal/rollout/store.go` (wave resolution reads the maintained view),
  `internal/agent/update.go` and `internal/agent/download.go` (bounded retry), `internal/firmware/`
  (upload steps become activity-callable), `internal/config` (the fault-injection and fleet settings
  the emulator needs), `cmd/worker` (registers the firmware workflow and owns the watcher),
  `cmd/controlplane` (starts an upload through the workflow), and `deploy/mongo/init.js` if the
  derived view needs a collection and index.
- **Also changed**: `cmd/agent/main.go` gains tests and whatever seams they require — it currently
  has none.
- **Data**: a state field on the firmware document. `$jsonSchema` validators in `deploy/mongo/init.js`
  declare `properties` without `additionalProperties: false`, so an added field is accepted without
  a migration and a record written earlier simply carries no state.
- **Tests**: `cmd/worker`'s integration suites are restructured around a shared per-package stack;
  the other integration packages gain the shared harness and the profile knobs; new tests land for
  the gaps the coverage ledger records, including the rollout-to-real-agent join. A known flake
  (`internal/temporal.TestWorkflowSupersededDispatchIsHarmless`, recorded in the repository's debt
  register as passing standalone but having failed once in a full run) is investigated rather than
  inherited, because this change touches shared harnesses.
- **Config**: new `FLEETOPS_TEST_*` variables, test-only, with defaults preserving today's behavior.
  Runtime configuration gains the emulator's fault settings; nothing existing changes meaning.
- **Dependencies**: a MongoDB replica set in the environments that run the watcher and its tests.
  No new Go dependency is expected; the change stream uses the driver already in `go.mod`.
- **Reviewability**: this is a wide change and deliberately so — three features plus test
  infrastructure. It is sequenced so the test profile and shared harness land first, the coverage
  check lands second, and the features are built and verified through them.
