# Tasks

## 1. The containerized Temporal harness

- [x] 1.1 Create `internal/temporaltest` as an integration-tagged harness package mirroring `internal/mongotest` and `internal/rabbittest`: a `Harness` carrying the frontend address, the namespace, and a connected SDK client; a `Start(t)` that boots `postgres:18-alpine` and `temporalio/auto-setup` on a private container network with `DB=postgres12`, `DB_PORT`, `POSTGRES_USER`, `POSTGRES_PWD`, and `POSTGRES_SEEDS` (and deliberately without `DYNAMIC_CONFIG_FILE_PATH`, which this image resolves against a path it does not ship), waits for readiness by calling `GetSystemInfo` rather than dialing the port — the mapped port accepts TCP before the frontend answers — and tears the client, both containers, and the network down through `t.Cleanup`; and a package doc naming the images and what readiness means — verify `go test -tags integration ./internal/temporaltest/ -count=1` passes with a test that boots the harness, asserts `GetSystemInfo` answers in the `default` namespace, and boots a second harness after the first was torn down
- [x] 1.2 Make the harness's failure surface explicit rather than a hang: the readiness wait is bounded by a startup budget and a service that never answers fails with the address it was waiting on — verify a table-driven test (no containers) drives the readiness helper against a closed port with a short budget and asserts the error names the address, and that a budget already elapsed returns immediately

## 2. The restarted worker and its activity gate

- [x] 2.1 Add the restart plan and attempt ledger to `cmd/worker`'s new integration test file — with a file-level doc comment naming what the suite proves, the containers it needs, and how to run it — a `restartPlan` deciding per activity attempt between `run`, `hold` (the attempt blocks until the harness has stopped this worker and started the next one), and `crash-after` (the attempt executes the real activity and then fails it, which is what a worker that died after the effect landed looks like to Temporal), plus an attempt ledger recording each attempt's activity id, registered name, attempt number, and workflow execution — verify a table-driven unit test (integration-tagged, no containers needed) covers a boundary plan holding only the first attempt, a crash-after plan running then failing only the first attempt, an unplanned activity running every attempt, and the ledger's per-activity attempt counts
- [x] 2.2 Implement the `activityGate` as a `worker.Options.Interceptors` entry (`ActivityInboundInterceptor.ExecuteActivity`): it reads `activity.GetInfo`, records the attempt, consults the plan, waits on the release channel together with `ctx.Done()` so a worker stop cancels a held attempt, and returns a retryable failure for `crash-after` — verify the gate's decisions are covered by 2.1's test, and its cancellation path is exercised by 3.1, whose assertions fail if a held attempt executed instead of being cancelled
- [x] 2.3 Implement the restartable worker harness over the worker's real registrations: dial and wait for the frontend, bootstrap the namespace with the worker's own `bootstrapNamespace`, build the worker with the rollout's registrations (`newRolloutDeps`, `registerRollout`), the gate interceptor, an identity per generation, and a `WorkerStopTimeout` that bounds a stop while an attempt is held; expose `Start`, `Restart` (stop the current worker and wait for it to finish, then start a fresh one), `Stop`, the generation identity, and the number of restarts injected — verify `TestRolloutRestartsAcrossEveryActivity` (3.1) is the first scenario to exercise start, several restarts, and a clean stop, and completes with the restart count 2.3 reports rather than one inferred from timing

## 3. The restart scenario matrix

- [x] 3.1 Cover the full sequence with an approval between waves under boundary restarts: a multi-wave rollout whose later wave requires approval, driven with a restart injected at every activity (`hold`), the approval sent while no worker is polling; assert the rollout completes with every wave healthy, exactly one approval passes exactly one gated wave, the effect ledger carries exactly one update command id per device per wave, and the history carries exactly one scheduled activity per logical unit; restart the worker once more after the rollout concluded and assert its terminal record is unchanged and no further effect appears — verify the scenario passes reporting at least one restart per activity, the approval that arrived during the outage was consumed by the gated wave, and the history assertion finds one scheduled activity per logical unit
- [x] 3.2 Add the resumption-point sampler used by 3.1's run: after every injected restart it reads the rollout state query (`temporal.NewRolloutStates`) and compares the sample with the previous one, refusing a sample in which a promoted wave became unpromoted, a status moved backwards, or the current wave moved back — verify a table-driven unit test over synthetic samples (advancing, holding, regressing in each of the three ways) asserts the detector flags exactly the regressions, and 3.1's run passes it for every restart it injects
- [x] 3.3 Cover pausing mid-wave, restarting, and resuming: pause a rollout while a wave is inside its health window, restart the worker while it is paused, and assert it is still paused, the wave's recorded start time and window deadline are unchanged, and the in-flight wave is still judged at its recorded deadline; then resume and assert the next wave starts, the rollout completes, and no wave's commands were dispatched twice — verify the scenario passes, and its assertions name the pause, the restart, and the recorded deadline it read
- [x] 3.4 Cover the rollback under restarts: two waves whose second one regresses, with a boundary restart at every activity of the compensation plan, one device whose restore fails and one that never reports; assert the plan compensates the later wave before the earlier one, every step is recorded with its outcome, each compensated device was restored under exactly one restore command id (a redelivery may repeat that id but never mints a second one), each announcement phase carries one event id, and the terminal `rolled_back` status is recorded only after the last step — verify the scenario passes with the restarts injected and its recorded plan matches the plan the workflow derives (`rollout.RollbackStepRecord` comparison with `cmp.Diff`)
- [x] 3.5 Cover bookkeeping after a rollback: over the same scripted rollback world (one restored device, one whose restore failed, one that never reported), read the fleet's `devices.current_fw` and compare it with the version the device-authority seam holds for every compensated device; assert every unrestored device is recorded on the firmware it still runs and counted among the rollout's unrestored devices, that the reconciliation's inventory accounts for every compensated device exactly once (`agreed + corrected + unverified` equals the devices touched), and that no device is recorded on a version it does not run — verify the scenario passes against the real MongoDB container and fails when a seeded record is left deliberately disagreeing with the device authority (checked during development, then removed)
- [x] 3.6 Cover a restart after each activity's effect: run the whole rollout with `crash-after` on every activity's first attempt, so each activity is redelivered to a fresh worker after its effect landed; assert the retry repeats the same identity, the effect ledger is exactly-once (one update command id per device per wave, one restore id per compensated device, one announcement event id per phase, one reconciliation per device), and the recorded rollout, waves, and device versions describe one run — verify the scenario passes with every activity's attempt count exactly two in the ledger, and the documents it records match the expected statuses and membership for the fleet it seeded
- [x] 3.7 Assert determinism after the churn: add a helper that collects a completed rollout's history from `GetWorkflowHistory` into a `*historypb.History` and replays it with `worker.NewWorkflowReplayer()` under `RolloutWorkflow`'s explicit registered name, and apply it to every scenario in this group — verify each scenario replays without a nondeterminism error, and the helper reports failure when handed a history the registered workflow cannot produce, asserted by replaying one recorded history against a different workflow registration

## 4. Verification and defect triage

- [x] 4.1 Run the change-level verification: `goimports -w .`, `go vet ./...` and `go vet -tags integration ./...`, `golangci-lint run`, `go test -race -count=1 ./...`, and the integration suites including the new one (`go test -tags integration ./internal/temporaltest/ ./cmd/worker/ -run 'TestRollout' -count=1`) — verify every command passes and the new suite reports the restarts it injected
- [x] 4.2 Confirm the CLI-based smoke is untouched and still behaves as before: `git diff --stat cmd/worker/rollout_integration_test.go` shows no change, `go test -tags integration ./cmd/worker/ -run TestRolloutSmoke -count=1` skips (no `temporal` CLI on this machine) rather than failing, and `go vet -tags integration ./cmd/worker/` compiles both integration files together
- [x] 4.3 Triage what the matrix found: if every scenario passed against the current code, record that the change is verification-only and carries no product diff; if a scenario failed, fix the defect in the package that owns it (`internal/temporal`, `internal/devices`, or `internal/telemetry`) with a focused unit test in the same task — verify the fix's own test fails before it and passes after, the scenario passes without any assertion or retry policy being weakened, and the affected package's tests pass with `-race -count=2`

## Outcome

Every scenario in the matrix passed against the code as it stands, so this change is
verification-only: it carries no product diff, and nothing in `internal/temporal`,
`internal/devices`, or `internal/telemetry` was changed. What the matrix verified, in the order the
tasks above built it: the containerized harness boots and answers (`internal/temporaltest`); a full
approval-gated sequence completes with a restart injected at every activity's first attempt, with
the approval delivered while no worker was polling; a pause survives a restart with the wave judged
at its recorded deadline; a rollback compensates in reverse wave order under a restart at every
compensating activity; the fleet's records match what the compensated devices run; and a
post-effect crash on every activity is retried on a fresh worker into exactly one effect per
identity. Every scenario also replayed its completed history against the rollout workflow, and the
replay assertion was itself shown to fail when handed a registration that cannot produce the
history.

The change-level commands: `goimports -l .` (clean), `go vet ./...` and
`go vet -tags integration ./...` (clean), `golangci-lint run` (0 issues), and
`go test -race -count=1 ./...` (all packages pass). The integration suites pass in full:
`go test -tags integration ./internal/temporaltest/ ./cmd/worker/ -count=1` — five restart
scenarios, the harness suite, and the suite's own unit tests — with the CLI-based smoke still
skipping for want of a `temporal` binary, unchanged.

Two pre-existing test-side defects surfaced while building the world these scenarios drive. Both are
in the smoke that this change deliberately leaves untouched, so neither was fixed here, and both are
reported rather than absorbed:

- The smoke's restore-order assertion passes a wave id to a helper that matches device ids, so it
  always reports a mismatch once it runs. The restart suite asserts the same property against the
  first wave's device ids instead.
- The smoke's sample stream numbers its samples from one per stream, so a fleet whose health is
  deliberately flipped keeps upserting the healthy stream's documents and goes on reporting its old
  health until its counter passes what the healthy stream had written. The restart suite drives its
  own stream, whose sample ids carry the stream's generation.
