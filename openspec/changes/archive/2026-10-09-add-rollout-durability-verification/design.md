# Design

## Context

The rollout already relies on durable execution mechanisms, and this change adds the harness that
puts them under a worker that actually dies. What matters for the design:

- **The workflow keeps everything in Temporal's record.** Every wait is a durable timer or a signal
  receive on the workflow clock, every signal folds into the same state at every wait, activity
  deadlines are computed from `workflow.Now`, and every write is idempotent
  (`internal/temporal/rollout_workflow.go`). The restart guarantee is therefore not something the
  suite has to build — it is something it has to falsify.
- **Effects are made single by identity, not by delivery.** A wave's update command id derives from
  the wave and the device, a restore command id from the rollback and the device, a rollback
  announcement's event id from the rollout and the phase, and `devices.current_fw` is only written
  where it disagrees. Temporal guarantees at-least-once activity execution, so a restart inside an
  activity can redeliver; these identities are what keep the redelivery from being a second effect.
- **The only end-to-end harness today is CLI-bound and restart-free.**
  `cmd/worker/rollout_integration_test.go` drives the real worker, MongoDB (container), RabbitMQ
  (container), and a Temporal dev server started by shelling out to `temporal server start-dev`. It
  starts one worker and stops it in `t.Cleanup`, so durability is never exercised, and it skips
  wherever the `temporal` binary is not on `PATH` (it is not on this machine's).
- **The repo's container harnesses are the pattern to follow.** `internal/mongotest` and
  `internal/rabbittest` are integration-tagged packages whose `Start(t)` boots a container, returns
  a harness, and registers teardown with `t.Cleanup`.
- **A containerized Temporal server works here.** Probed with `temporalio/auto-setup:1.25.2` plus
  `postgres:18-alpine`: the frontend serves, the bootstrap creates the `default` namespace, and the
  container needs `DB=postgres12`, `DB_PORT`, `POSTGRES_USER`, `POSTGRES_PWD`, `POSTGRES_SEEDS` —
  and must *not* set `DYNAMIC_CONFIG_FILE_PATH`, which this image resolves against a path it does
  not ship. The published port accepts TCP well before the frontend answers, so readiness has to be
  a service call, not a port dial — and the namespace appears later still: the bootstrap registers
  it while the frontend is already serving, and the workflow service describes it before the
  operator service will list its search attributes. Readiness therefore polls what a worker's own
  startup needs — `GetSystemInfo`, `DescribeNamespace`, and the operator service's
  `ListSearchAttributes` — rather than the first call that happens to answer.
- **The retry policies are the product's.** Activity retries start at one second and cap at five
  attempts, so injected failures must stay inside those budgets and cost roughly a second each.

## Goals / Non-Goals

**Goals:**

- Make a worker restart an event the suite asserts on: injected at a known point relative to an
  activity, counted, and reported in the failure output — never a race it hopes to win.
- Prove the four claims the specs make: the resumption point survives, no work is re-decided, no
  effect is applied twice, and the records converge.
- Run the end-to-end scenario matrix (full sequence with an approval between waves, rollback with
  compensations in reverse order, inventory reconciliation against the devices) on a harness that
  works wherever Docker works, without a `temporal` CLI.
- Keep the production code path under test exactly what the worker runs: the same registrations,
  the same activity implementations, the same retry policies, the same configuration mapping.

**Non-Goals:**

- Any change to rollout behavior. This change's product-code delta is expected to be zero; a defect
  the suite finds is fixed as its own task, with its own unit test.
- Rewriting or replacing the CLI-based smoke, and re-testing the operator HTTP surface (the smoke's
  job).
- Restarting anything but the worker: the device world stays the scripted command seam, the fleet
  database and broker stay up, and the Temporal server stays up.
- Testing Temporal's own delivery guarantees, multi-replica handoff under live overlap, or scale.
  The suite verifies FleetOps' idempotency, and a restart is one worker stopping before another
  starts.

## Decisions

### D1 — A containerized Temporal harness in `internal/temporaltest`

A new integration-tagged package mirrors `mongotest`/`rabbittest`: `Start(t)` boots
`postgres:18-alpine` and `temporalio/auto-setup` on a private network, exposes the frontend address
and a connected SDK client, waits for readiness on the three service calls a worker's startup needs
(the port opens first and the namespace's search attributes answer last), and terminates both
containers in `t.Cleanup`. Namespace `default` comes from the image's bootstrap. The client is a
lazy one on purpose: readiness is asserted by the harness's own bounded poll, where an eager dial
would fail with the frontend's transport error and no bound on how long it waits.

*Alternatives:* extend the CLI smoke (keeps a machine-level dependency and an entire suite that
skips silently — the reason the restart work would be invisible in CI); run the dev server in-process
(there is no supported embedded server); rely on `TestWorkflowEnvironment` (no real worker exists to
restart, and the harness under test is the worker).

### D2 — Restarts are injected through an activity interceptor gate

The harness registers the worker's real activities with a `worker.Options.Interceptors` entry whose
`ActivityInboundInterceptor` wraps every attempt. Each attempt reports itself — activity id, type,
attempt number, workflow execution — to a ledger and then asks the gate for a decision:

- **run** — the default: the attempt executes normally.
- **hold** — the attempt blocks until the harness has stopped the current worker and started a fresh
  one; the stop is what ends the held attempt (the worker is started with `WorkerStopTimeout` so a
  held attempt is cancelled rather than waited on forever), and Temporal's retry delivers the
  activity to the new worker.
- **crash-after** — the attempt executes the real activity, then returns a retryable error, which is
  what a worker that died after the effect landed but before its result was recorded looks like from
  Temporal's side. The harness restarts the worker before the retry, so the attempt that repeats the
  effect runs on a worker that never saw the first one.

The gate is deterministic: an attempt cannot be picked up by a worker that is not polling, the
harness waits for the stop to complete before starting the next worker, and the ledger records every
attempt so the test can state exactly how many restarts it injected.

*Alternatives:* poll `GetWorkflowHistory` and restart whenever an activity completes (racy — the
restart may land anywhere, and a green run could mean nothing happened); kill a separate worker
process (`cmd/worker` cannot be handed the scripted device world, so it would need a real agent and
device workflows); a test-only activity wrapper registered instead of the real one (it would test a
wrapper, not the worker's registrations).

### D3 — Two restart flavours, because "between" is the easy case

A restart *between* two activities is nearly free: Temporal does not re-execute an activity whose
result was recorded. The case that can duplicate an effect is a restart *inside* an activity, after
the effect landed and before the result was recorded. So the matrix runs both: the boundary flavour
(hold, restart, let the retry run) for every activity, and the post-effect flavour (crash-after) for
every activity of a second scenario. Both share the same ledger, and both must end with the same
exactly-once effect ledger.

### D4 — Assertions are split by level: attempts, decided work, effects, records

- **Attempts**: counted from the interceptor ledger, and expected to exceed effects exactly by the
  injected failures. A cancelled attempt is asserted to have been cancelled by the stop, so a
  changed SDK stop semantic fails loudly instead of quietly weakening the test.
- **Decided work**: `GetWorkflowHistory` is read after the run and asserted to carry exactly one
  `ActivityTaskScheduled` per logical unit (one load-firmware, one resolution and one update per
  device per wave, one evaluation per gate pass, one step per compensation, one reconciliation per
  compensated device, one announcement per phase) — the workflow never re-decided work after a
  restart.
- **Effects**: asserted by identity over the command seam and the broker — exactly one update
  command id per device per wave, exactly one restore command id per compensated device, one
  announcement event id per rollback phase (a redelivery repeats the id), each device's record
  reconciled to the version the device-authority seam holds.
- **Records**: the rollout document, its wave documents, and the devices' recorded firmware
  versions are read after the run and asserted to describe one run — no duplicated wave, no counter
  that grew with the number of restarts.
- **Resumption point**: after every injected restart the state query is sampled and asserted not to
  regress against the previous sample (a promoted wave stays promoted, a status never moves
  backwards, the current wave never moves back), which is what "resumes from the correct point"
  means for a run that is still in flight.
- **Determinism**: the completed history of every scenario is replayed against
  `worker.NewWorkflowReplayer()` with `RolloutWorkflow` registered under its explicit name, so a run
  that passes once and flaps on replay fails the suite. Events are collected from
  `GetWorkflowHistory` into a `*historypb.History`; no golden file is checked in (histories carry
  wall-clock timestamps, and a golden file would be regenerated rather than understood).

### D5 — The suite lives in `cmd/worker`, and reuses the smoke's world-building

New files in `cmd/worker` (same `package main`, `//go:build integration`) hold the restart harness,
the gate, the ledger, the assertions, and the scenario matrix, split by concern so each stays inside
the repo's file-size guidance: `rollout_restart_integration_test.go` carries the suite's doc comment,
the plan, and the gate; the ledger, the gate's tests, the restartable worker, the world, the
resumption sampler, the effect and history assertions, the shared checks, and the forward and
rollback scenarios each have their own file. The suite reuses
the smoke's helpers for what is not under test — `seedFleet`, `seedFirmware`, `seedPreviousFirmware`,
`startHeartbeats`, `smokeRolloutSettings`, `waitForRolloutStatus`, `waitForWave`,
`awaitAnnouncements`, `newRolloutDeps`, `registerRollout`, `bootstrapNamespace`, and the
`commandRecorder` device world — and adds only the restarted worker lifecycle and the gate, so the
smoke is untouched.

*Alternative considered:* refactor `startSmokeWorker` into a shared restartable harness used by both
(touches a working 1147-line test file for no behavioral gain, and couples the CLI smoke's lifecycle
to the new suite's).

Two of the smoke's helpers are reused in shape rather than in call, because their test-local
semantics do not hold under restarts. The sample stream is the suite's own: the smoke's sample ids
restart at one per stream, so a fleet whose health was deliberately flipped would keep upserting the
healthy stream's documents and go on reporting its old health. And every scenario that reads the
effects of a concluded run waits for the workflow execution to close first: a rollout's terminal
document is written by its last activity, whose attempt may still be retried after that write
landed.

### D6 — Scenario matrix

| Scenario | Device world | Restart injection | What it proves |
| --- | --- | --- | --- |
| Full sequence with an approval between waves | 4 devices, healthy heartbeats; waves 25% then 100%-with-approval | boundary restart on every activity; approval sent while no worker polls | the sequence completes, one approval passes one gated wave, waves and commands are recorded once, resumption point never regresses |
| Pause mid-wave, restart, resume | small fleet, healthy | pause during the in-flight wave's window, restart while paused, resume | still paused after the restart, the in-flight wave is judged at its recorded deadline with its recorded start time, the next wave starts only on resume, no wave re-dispatched |
| Rollback with the worker restarted between every compensation | 2 waves: first healthy, then the fleet regresses; one device fails or never reports its restore | boundary restart on every activity of the plan | the plan runs in reverse wave order, each step is recorded once, restores use one id per device, both announcements carry one event id per phase, the terminal status lands after the plan |
| Bookkeeping against the devices | same rollback world: one restored device, one failed restore, one unreported restore | none (the reconciliation's own correctness is the subject) | every compensated device's recorded version equals the version the device authority holds, unrestored devices are recorded on what they still run, the inventory accounts for every device exactly once |
| Post-effect crash across the whole rollout | one device whose wave fails its gate, so the run compensates it and every compensation runs too | crash-after on every activity, with a restart before each retry | the retry repeats the same identity; the effect ledger is exactly-once; the records describe one run |

Scenarios are separate test functions, each booting its own stack (MongoDB, RabbitMQ, Temporal,
Postgres) and each using small fleets and the smoke's short windows. They are deliberately not run in
parallel: four containers per scenario make the suite's cost dominated by container startup, and the
integration tag keeps it out of the Definition of Done.

### D7 — A failure is a defect, not an assertion to relax

Every scenario asserts behavior the specs already require. If the suite fails, the fix belongs in the
product (`internal/temporal`, `internal/devices`, or `internal/telemetry`) with a focused unit test
alongside it; the scenario's assertion and the fleet's retry policies are not weakened to make it
pass. Applying a test-only retry policy or a shortened window would test a rollout nobody runs, so
the suite keeps the product's policies and only uses the existing `updateOptions` seam the smoke
already uses to shrink the update activity's observation interval.

## Risks / Trade-offs

- **Container startup flakiness** → readiness is a real service call with a generous budget, both
  containers are torn down with `t.Cleanup`, and the harness logs the containers' condition so a
  failure names the service that did not come up.
- **Runtime cost of restart churn** → one injected failure per activity per scenario keeps attempts
  inside the product's retry budget, small fleets keep logical units few, and short windows (the
  existing smoke constants) keep the clock from dominating; each scenario reports the restarts it
  injected.
- **Attempt versus effect conflation** → the two are asserted separately (D4), so a test cannot pass
  by counting attempts that did nothing, or fail because a legitimate retry ran.
- **Stop semantics are SDK behavior** → the harness asserts the held attempt was cancelled; if a
  future SDK stops waiting differently, the assertion fails rather than the suite silently losing
  its restart.
- **The device world is scripted** → the reconciliation's "reality" is the device-authority seam the
  reconciliation itself reads; the suite proves bookkeeping matches that authority and that the
  commands imply the same version, not that a real agent applied a real binary (the emulator's job
  in the local stack).
- **Two end-to-end harnesses exist** → the CLI smoke and this suite overlap in intent and will
  drift; accepted for this change because replacing the smoke would put the container dependency
  into scenarios that do not need restarts (see Open Questions).

## Migration Plan

Test-only: nothing is deployed and no production artifact changes. The suite is behind
`//go:build integration` and adds a test-time dependency on Docker plus the MongoDB, RabbitMQ,
Postgres, and Temporal images; the package documents them, and a missing daemon fails the suite the
way the existing integration suites do. If the matrix exposes a defect, the fix is a normal worker
rollout — the rollout workflow's change rules (drain in-flight runs or terminate them before a new
build polls) apply as they do to any workflow change.

## Open Questions

- Whether the CLI-based smoke should later migrate onto `internal/temporaltest` and drop its
  `temporal` CLI dependency. Deferred: this change keeps the smoke as it is, and the migration
  changes no spec or approach here.
- Whether the restart matrix runs on every pull request or on a schedule. Deferred: it is a CI
  configuration choice, and the suite reports its injected restarts so a scheduled run is
  interpretable either way.
