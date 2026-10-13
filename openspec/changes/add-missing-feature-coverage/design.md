# Design

## Context

See `proposal.md` — Why. The constraints that shape this design, verified against the repository:

- **Wave membership freezes by design.** `openspec/specs/rollout-workflow/spec.md` requires a wave's
  target set to be resolved once, recorded before dispatch, and never re-derived; `Store.ResolveWave`
  (`internal/rollout/store.go:127`) implements exactly that, returning a recorded wave when one
  exists. Whatever keeps the pool fresh must not touch that rule.
- **The pool is a query today.** `Store.eligiblePool` (`internal/rollout/store.go:230`) reads
  `devices` with `{region, model}` and sorts by `_id` — the ordering is what makes membership
  reproducible, so any replacement must preserve order.
- **MongoDB serves change streams only on a replica set.** `deploy/mongo/init.js` and
  `internal/mongotest/mongotest.go:86` both start a standalone; `deploy/README.md` documents the
  local stack as it is. The feature and its tests therefore need an environment change, not only a
  code change.
- **Activities run beside their side effect.** `temporal-worker` requires the command-dispatch
  activity to be registered by the process holding the agent connections, and the worker to
  register everything else it hosts; there is no shared library of activities, which decides where
  the firmware workflow's steps live.
- **The suite has no configuration surface.** `grep -rn "os.Getenv" --include='*_test.go'` returns
  nothing across 107 test files. Every budget is a constant and every container stack is per test.
- **The baseline is 37 capabilities, 163 requirements, 534 scenarios.** Capability scenarios are
  the unit of the coverage contract, because they are the project's own behavioural commitments.
- **`fix-dispatch` is still active.** It archives documents without touching code, so it does not
  conflict with this change; `openspec validate --changes --strict` currently reports 1 passed.
- **The rollout path is tested through a stub.** Both rollout integration suites bind the device
  command seam to an in-process recorder, and every streaming suite ignores rollouts, so nothing
  joins the two: a regression in the command the rollout builds (`internal/temporal/dispatch.go`) or
  in the hub that routes it (`internal/agentserver/hub.go`) leaves both green. This is the largest
  structural gap the audit found, and it is a coverage problem rather than a missing feature — the
  code on both sides exists.
- **The suite's container cost is concentrated and sequential.** Eleven packages start containers;
  within `internal/telemetry` up to nine Mongo and six RabbitMQ containers can be live at once
  because every test calls `t.Parallel()`, while `cmd/worker` runs eight restart worlds strictly
  sequentially — 32 container starts in one binary before the smoke test's own stack and Temporal
  dev-server subprocess. Both shapes matter to the weak-host profile, and they need opposite
  remedies.
- **The emulator's scale test is cheap.** `TestFleetScalesAndLeavesNoGoroutines`
  (`internal/agent/fleet_test.go`) runs a fleet of 100 devices in 0.01 s and of 1000 devices in
  0.07 s under `-race`, and the goroutine count returns to its baseline after cancellation; the
  whole package run is 1.1 s. The two fleet sizes are the emulator's scale story and they cost
  almost nothing, which is why both stay in the suite under the weak-host profile.

The scope of this change was widened, deliberately and by explicit decision, to implement the two
requirements found entirely absent (the change-stream-maintained pool and `FirmwareWorkflow`) rather
than only recording them. The design below treats that as a hard constraint and sequences the test
infrastructure first, so the new features are built under the new configuration rather than
retrofitted into it.

## Goals / Non-Goals

**Goals**

- Keep the eligible pool fresh from the `devices` change stream, on every worker replica, without
  changing when a wave's membership freezes or in what order a pool is ordered.
- Make one firmware upload one workflow execution with a stable identity per version, idempotent
  steps, a recorded state, and a queryable outcome, while keeping the existing upload guarantees.
- Give the suite an explicit environment configuration with defaults equal to today's behavior and
  a documented profile that fits 4 GB / 4 CPU, and cut peak container cost by sharing one stack per
  test package.
- Make every capability scenario either backed by a test that fails when its behavior is removed,
  or exempt with a named reason and a named blocker.

**Non-Goals**

- Changing when a wave resolves its membership, or making a running rollout pick up new devices.
- Production MongoDB topology, deployment manifests, or stack bring-up — beyond the environment
  prerequisite the change stream genuinely requires.
- Observability: no metrics, traces, or dashboards for the watcher, the pool, or the firmware
  workflow. The worker's existing health endpoints are untouched.
- Any automatic narrowing of a run based on host resources.
- Removing or rewriting any existing requirement to make coverage numbers look complete.

## Decisions

### D1 — A change-stream-fed, selector-keyed view held in process, not a new collection

A `PoolView` holds `selector -> ordered device ids`, built lazily per selector and refreshed when
the `devices` change stream reports a change that can affect it. `Store.ResolveWave` reads a
selector from the view instead of querying `devices`.

The view lives in the worker's process rather than in a derived MongoDB collection. Reasons:

- `devices` stays the only source of truth, so there is no second store to keep consistent, no
  write amplification on every device registration, and no new validator or index to get wrong.
- Memory is bounded by the selectors a worker actually resolves. A fleet's selectors are a
  handful; the per-selector ids are the same data the current query already materializes in memory
  on every resolution.
- Losing the view costs one query per selector, which is what the code does today.

Lazy building is what makes first use cheap and the change stream's role precise: the stream is the
*invalidation* signal that keeps an already-built entry current, not the thing that constructs it.
A burst of device changes coalesces into one refresh per affected selector rather than one query
per event, which is the property the "without polling" requirement is about — freshness comes from
the fleet's writes, and nothing scans `devices` to discover what changed.

*Alternatives considered.* A materialized `eligible_devices` collection updated by the stream: it
survives process restarts and could be read by other processes, but adds a derived collection, an
index, a bootstrap change, write amplification on the hot registration path, and a new way for two
stores to disagree — with no consumer that needs it, since only the worker resolves waves. A TTL
cache over the existing query: simplest, but it is polling with a delay, which is the specific
thing the requirement rejects.

### D2 — Stream loss resumes from its token; an unusable token flushes the view

The watcher keeps the change stream's resume token and reconnects from it with capped backoff, so a
brief interruption loses nothing. If the server no longer holds that position, or the stream cannot
be re-established, the watcher discards the entire view and lets it rebuild lazily, so the view can
never serve a pool that predates an interruption the watcher could not recover from.

The watcher is established **before the worker resolves its first wave** and its failure to start is
a startup failure rather than a silently absent feature. That is the honest choice: a worker whose
view is not being maintained answers wave resolutions from data with no freshness guarantee, and
nothing downstream can tell. Failing loudly at startup is the same posture the worker already takes
for search-attribute bootstrap and for the broker.

This is also why the environment prerequisite is real rather than incidental: a standalone MongoDB
cannot serve the stream, so an environment that intends to run the worker must run a replica set.
The prerequisite is recorded here and in the migration plan; providing that environment is out of
scope, and the test harness — which is in scope — must provide one.

*Alternatives considered.* A best-effort watcher that logs and continues: a stale pool would then
silently change which devices a wave targets, which is a correctness failure with no symptom. A
periodic full rebuild as a safety net: it reintroduces polling and hides a broken stream instead of
surfacing it.

### D3 — `FirmwareWorkflow` runs in the control plane, waits for its outcome, and is keyed by version

The workflow identity is the firmware version (`firmware-upload-<version>`), so a retried upload
reaches the existing execution rather than starting a second one, and the upload path's steps —
target-model validation, streaming checksum plus GridFS storage, metadata write — become activities
of that workflow.

**Where it runs:** the control-plane process, on the control-plane task queue — the workflow as well
as its activities. The binary arrives as an HTTP request body in that process, and the rule that
activities run beside their side effect puts the storage step where the stream is; the control plane
therefore gets the store binding the upload activities need, in the same shape it already has for
command dispatch. The worker registers neither the workflow nor its activities, which keeps
`temporal-worker`'s "a process never polls a queue that can deliver a task type it does not host"
rule intact in both directions.

**How the binary reaches the storage activity:** an activity cannot be handed an `io.Reader` — its
input is serialized — so the handler spools the request body to a file under a configured spool
directory and passes that path in the activity's input. The storage activity streams the file into
GridFS, so the binary is still never held in memory whole, and a retried attempt re-reads the same
file rather than a stream the failed attempt already consumed, which is what makes the storage step
genuinely idempotent under retry. The spool file is removed once the upload concludes. A control
plane that dies mid-upload leaves its spool file behind, so startup sweeps the spool directory for
files older than a configured maximum upload age — by age rather than by presence, because a second
replica's in-flight upload is not this replica's to delete.

**How a caller learns the outcome:** the HTTP handler starts the workflow and waits for its result
within a bounded timeout, so the endpoint keeps its current external shape — `201` with the record,
`422` for a refused model, `409` for a version conflict — while the work itself is durable and
retryable. The state query exists for the case the wait cannot cover: a caller that timed out, or an
operator looking at an upload after the fact, reads the workflow's state instead of the request.

**Idempotency:** two concurrent uploads of one version must collapse to one record. The start uses
`UseExisting` as the workflow-id conflict policy so the second caller attaches to the running
execution, a `WorkflowExecutionAlreadyStarted`-style conflict is mapped to the existing conflict
response, and the storage and metadata activities are individually idempotent — storage addresses
its GridFS object by version, the metadata write is an upsert on the version's unique index. If the
upload concludes as failed, the losing caller is answered the conflict the existing record implies.

**Storage of the state:** a `state` field on the firmware document. The `$jsonSchema` validators in
`deploy/mongo/init.js` use `properties` without `additionalProperties: false`, so an added field is
accepted by the existing validator and no migration is required; a record written before this change
simply has no state until its next write, which is why the reader treats a missing state as
"unknown" rather than as a failure.

*Alternatives considered.* A `FirmwareWorkflow` that owns only GridFS while the HTTP handler writes
metadata: splits one upload across two owners and reintroduces the orphan window the spec forbids.
Activities on the worker queue: requires streaming the binary through Temporal or staging it
somewhere first, which is more machinery for no benefit at this scale. An asynchronous 202 +
poll-only API: cleaner temporally, but changes the operator contract for no gain the wait does not
already provide.

### D4 — One test profile surface, explicit, with defaults equal to today

A helper package resolves the run's configuration from a documented variable set at first use:
the profile name, workload sizes (simulated fleet size, sample counts), probe budgets (container
startup, wait and poll budgets, per-test deadlines), and container use (shared stack or per-test).
Defaults reproduce today's constants exactly; the weak-host profile narrows workload and budgets to
fit 4 GB / 4 CPU.

The helper never reads host memory, CPU count, or load. A run's behavior is therefore a function of
its environment alone, which keeps a failure reproducible on another machine and keeps a
misconfiguration visible: an unknown profile, a non-positive size, or a non-numeric budget fails
when the configuration is resolved, naming the variable and the value, instead of surfacing later as
an unexplained timeout.

Narrowing is by workload and budget only. Assertions are not parameterized by the profile, so a test
under the weak-host profile asserts exactly what it asserts under the default; when a test genuinely
cannot fit the budget, it skips with a message naming the budget and the variable that would raise
it, rather than running a weakened version of itself.

*Alternatives considered.* Probing `runtime.NumCPU()` and `/proc/meminfo`: convenient, but the same
commit then executes different assertions on different machines, and a developer's failing test
becomes unreproducible elsewhere. A config file: more structure than a handful of knobs needs, and
one more thing to locate when a run behaves oddly. Build tags per profile: they change which files
compile, so the suite would no longer be one suite.

### D5 — One container stack per test package, isolation by database, namespace, and purge

The three harnesses gain a shared mode: a package's integration tests start MongoDB, RabbitMQ, and
Temporal once — through `TestMain` for the package, or a cached stack reached by a `Shared(t)`
accessor — and the per-test `Start(t)` entry points remain for tests that genuinely need a private
service.

Isolation per test, using the mechanism each service actually offers:

| Service | Isolation | Why this one |
| --- | --- | --- |
| MongoDB | one database name per test, schema bootstrap applied to it | the bootstrap is per-database, so a test gets the real validators and indexes; the shared server keeps its own cache budget |
| RabbitMQ | purge the queues a test uses before it runs | queues are the only shared state, and `Harness.Purge` already exists for this |
| Temporal | one namespace per test, created from the shared server | namespaces isolate workflow ids, histories, and search attributes; `Client.WorkflowService().Register` (`go.temporal.io/sdk@v1.49.0/client/client.go:1709`) creates one, and the harness's readiness probe already asserts a namespace exists |

A package therefore starts one stack instead of one per test, which is the peak-memory reduction the
weak-host profile depends on: today five tests in `cmd/worker` alone each bring up four containers.

*Alternatives considered.* Reusing one database and wiping collections between tests: cheaper, but
wiping a time-series collection and its GridFS bucket is more code than naming a new database, and a
failed wipe leaves a test reading another's documents. Sharing one stack across packages: saves more
memory, but requires cross-package synchronisation (a lock file or a supervisor process) and gives
one package's crash the power to fail another's run.

### D6 — The coverage contract is a checked ledger over capability scenarios

`openspec/specs/*/spec.md` defines 534 scenarios; that is the checklist, so nothing has to be
invented. A ledger file in the repository records one entry per scenario: the scenario name, the
capability, the test function that backs it, and the file that test lives in; plus a short list of
exemptions, each with its reason and what would verify it.

A unit test under `internal/` checks the ledger against the specs by parsing both: every scenario
has an entry or an exemption, no entry names a scenario that no longer exists, and every referenced
test function exists in the named file. It runs as part of `go test ./...`, so the check is on the
normal path and costs no containers.

The ledger is hand-maintained, and a hand-maintained record can be wrong in one direction a parser
cannot catch: claiming a test backs a scenario it does not. The mitigation is stated in the spec —
the test must fail when the behavior is removed — and enforced by review and by the mutation check
the tasks perform on a sample of newly backed scenarios. Given 534 scenarios over roughly 163
requirements, an exhaustive mutation harness is not worth its cost here; a reviewed ledger plus a
per-scenario spot check is.

*Alternatives considered.* Markers in test code (`// covers: <scenario>`) parsed by the checker: the
mapping would live next to the test, but it would rewrite a large number of existing test files in
one mechanical diff and every new test would have to carry a comment. Executing tests with a
runtime registry and comparing afterwards: catches only tests that ran, so an integration-only
scenario would look unbacked in a unit-only run, making the check's result depend on which profile
ran it.

### D7 — Faults are injected at the fetch seam, and retry wraps the fetch, not the report

`UpdateHandler` already depends on a `FirmwareFetcher` interface, so fault injection belongs behind
that seam: a decorating fetcher that consults a seeded decision per attempt either stalls, returns a
transfer error, or delegates to the real downloader. `handleUpdate` gains a bounded retry loop
around the fetch call, with delays from the same capped-backoff shape the stream client already
uses (`internal/agent/backoff.go`), so there is one backoff idiom in the package rather than two.

Two decisions inside that:

- **Retry wraps the fetch only.** Phase reports and the terminal result stay outside the loop, so a
  retried download reports the downloading phase again as the attempt begins and still produces
  exactly one terminal result — the existing "one command has one terminal result" contract is
  untouched.
- **Not every error is retryable.** A checksum mismatch between the transfer and the command, or a
  malformed transfer, is deterministic: retrying it burns the budget to produce the same failure.
  The loop retries transfer and stall faults and returns immediately on checksum and protocol
  errors, which is why the retry classification lives beside the fetcher rather than in the handler.

Fault decisions are drawn from the emulator's existing seeded source, keyed per device, so the same
seed replays the same faults and a test pins a device's fault by seed rather than by timing. The
configuration is optional with a zero value meaning no faults, so an emulator without fault settings
behaves exactly as today — which is what makes this additive to the existing emulator tests.

*Alternatives considered.* Faults injected inside the real downloader: it would put test-only
behavior on the production path and make the downloader's error contract conditional. Retrying the
whole `handleUpdate` cycle: it would re-report phases and re-apply firmware, breaking the
one-terminal-result contract. An unseeded `math/rand`: unreproducible failures in a suite that is
supposed to be deterministic, and no way to pin a device's fault from a test.

### D8 — The rollout-to-agent join is a test at the seam, not a new production path

The missing join is a coverage gap, so the remedy is a test, not new code. The test drives a rollout
whose device command seam is the real dispatch activity bound to a real `agentserver.Hub`, with a
real agent client on the other end of the stream, and asserts the chain end to end: the rollout's
command reaches the agent carrying the firmware id, version, and checksum the wave resolved; the
agent's report reaches the device workflow; the wave records that device's outcome.

This is the one place where a test needs a control-plane-shaped process and a worker-shaped process
in one binary. The existing `cmd/controlplane` integration tests already stand up a pipeline over
real dependencies, and the worker's restart world already stands up a worker over real
dependencies, so the test belongs where both can be wired: a new integration file that starts the
container stack once (D5) and binds the real pieces rather than stubbing the seam.

*Alternatives considered.* A unit test with a fake hub: that is exactly what exists today and what
fails to catch the regression. Asserting the command shape in the dispatch activity and the
routing in the hub separately: each side is already tested in isolation, which is how the gap
survived. Making the rollout suite use the real agent everywhere: it would multiply that suite's
cost for a property one test can establish once.

## Risks / Trade-offs

- **A change stream needs a replica set, and today's environments are standalone** → the worker
  fails at startup without one rather than degrading silently; the migration plan names the
  environment prerequisite, and the test harness must initiate one, so the tests prove the
  requirement is met rather than assuming an environment that does not exist.
- **A stream that is down for longer than the oplog retains means a view built on stale data** →
  the token is kept and reconnection is from the token; an unusable token flushes the whole view,
  so staleness cannot outlive the interruption. The window between interruption and detection is
  the residual exposure, and it is bounded by the reconnect backoff.
- **A worker that requires the watcher cannot start against a database that cannot serve one** →
  accepted deliberately (D2): the alternative is a silent correctness gap. Rollback is simply not
  registering the watcher once the feature is stable, but the code path that reads the view must go
  with it, which is why the migration plan orders the change that way.
- **`FirmwareWorkflow` adds a Temporal round trip to a path that is currently local** → the upload
  endpoint waits on the workflow, so the added latency is the workflow's scheduling and activity
  round trips; the timeout that bounds the wait is configurable, and a timeout does not lose the
  upload, which the state query still reports.
- **Spooling an upload puts the binary on the control plane's disk before it reaches GridFS** → the
  spool directory is configuration with a documented default under the OS temporary directory, a
  spool file lives only for the upload's bounded wait plus its retry attempts, and files abandoned
  by a crash are swept by age at startup, so disk use is bounded by the uploads in flight rather
  than by the uploads ever performed.
- **A shared stack couples a package's tests** → isolation is per-service and asserted (a test that
  reads another's documents fails), queue purging is explicit rather than assumed, and a stack that
  cannot start fails the package with one clear error instead of a cascade of unrelated failures.
- **A weakened workload can hide a regression a full-size run would find** → the profile narrows
  sizes and budgets, never assertions; the default remains what CI and a capable machine run, and
  the weak-host profile is documented as the low-resource option rather than the reference run.
- **The coverage ledger can drift from the code** → the checker fails on both directions of drift
  (a scenario with no entry, an entry naming a test that no longer exists), and it runs on the unit
  path, so drift is caught by the same command as any other broken test.
- **Retrying a download extends how long a device holds a command** → the attempt budget and the
  backoff cap are configuration, the loop stops when the command's context ends, and a device whose
  budget is spent reports the failure rather than holding the wave open: the wave already has a
  result timeout, and a reported failure is strictly more informative than an unreported one.
- **Injected faults could mask a real download regression in the emulator's own tests** → the zero
  value means no faults, so the existing emulator tests keep exercising the real path unchanged, and
  fault tests configure faults explicitly rather than inheriting them.
- **The rollout-to-agent join test is the most expensive single test in the change** → it shares the
  package's container stack (D5) instead of starting its own, and it asserts a chain rather than a
  matrix, so it earns its cost once rather than per scenario.
- **A known flake is being inherited** → `internal/temporal.TestWorkflowSupersededDispatchIsHarmless`
  passes standalone but has failed once in a full integration run (recorded in the repository's debt
  register). This change touches the shared harnesses that run beside it, so the flake is
  investigated rather than assumed unrelated; if it cannot be reproduced, that is recorded with what
  was tried rather than left silent. **Outcome: investigated, not reproduced, not fixed.**
  `go test -race -count=20 -run TestWorkflowSupersededDispatchIsHarmless ./internal/temporal/` →
  `ok` in 32.7 s on an idle host, and the same command re-run while two container-backed suites ran
  beside it (load average 20.3) → `ok` in 57.7 s, 0 failures in 20 runs. `internal/temporal` was not
  changed, so the cause stays unidentified and the debt item stays open rather than being declared
  fixed; the load run is a bounds check, not a reproduction, and the record says so instead of
  claiming the flake is unrelated.
- **`AbortUpdate` remains a stub with no producer, and is not fixed here** → the type, the dispatch
  case, and the contract are all in place, but nothing creates an abort command and the agent logs
  and ignores one, so a running update cannot be stopped. It is recorded as a gap in the proposal
  rather than implemented in this change: the contract requirement is met (the command set is
  defined), no spec requires the cancellation semantics, and adding them would widen a change that
  already carries three features.

## Migration Plan

Work lands in six steps, each independently revertible:

1. **Test configuration and shared stacks first** (D4, D5). Foundational and behavior-preserving:
   with no variables set, the suite behaves as it does today. Reverting restores per-test stacks;
   nothing in the production binaries changes.
2. **The known defects the shared work sits next to.** `TestRolloutSmoke`'s three defects are
   repaired before the harness it uses is restructured, so the suite that verifies everything else
   is green rather than red for reasons unrelated to the new features. Test-only.
3. **Coverage ledger, the rollout-to-agent join, and the remaining new tests** (D6, D8).
   Test-only; reverting removes tests and the checker.
4. **Fault injection and bounded download retry** (D7). Changes emulator behavior only when
   configured, so its rollback is removing the configuration; the retry loop's rollback is the
   single-attempt path it replaces.
5. **`FirmwareWorkflow`** (D3). The upload endpoint keeps its contract, so the change is invisible
   to a caller; rolling back means restoring the synchronous handler, and the firmware documents'
   `state` field is additive and ignored by the old path.
6. **The eligible-pool view** (D1, D2). The only step with an environment prerequisite: the
   environments that run the worker must serve change streams (a replica set). Rolling back means
   reading the pool with the existing query again; the watcher has no persisted state to undo.

The order matters three times: the test infrastructure exists before the features that must be
tested under it, the suite that verifies those features is made trustworthy (step 2) before it is
asked to verify new work, and the change-stream feature lands after the suite can afford to run its
tests.

## Open Questions

- The exact variable names and numeric defaults of the weak-host profile. They are documentation and
  constants, not architecture: any choice that keeps the defaults equal to today's behavior and
  stays inside the documented set satisfies `test-execution-profiles`, so this is settled while
  implementing rather than here.
- Whether the fleet's largest realistic selector count makes lazy per-selector building worth
  bounding. The view is lazy and per-selector precisely so this stays an implementation detail; if a
  bound is ever needed it does not change the specs.
