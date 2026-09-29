# Tasks

## 1. Rollout sequence and gate configuration

- [x] 1.1 Add the wave sequence and decision timeout to `internal/config`: a `Wave` entry type
      (`Percent`, `RequireApproval`), `Rollout.Waves []Wave`, and `Rollout.DecisionTimeout`, wired
      into `Defaults()` with the demo canary — 1% and 5% without approval, 25% and 100% with
      approval, decision timeout `"30m"`; verify `go build ./...` succeeds and
      `internal/config/config_test.go` pins each default by table case
- [x] 1.2 Validate the sequence in `internal/config/validate.go`: non-empty, every percentage in
      `(0, 100]`, strictly increasing, the last entry at 100, and `decision_timeout` a positive
      duration no shorter than `health_window`, each error naming `rollout.waves` or
      `rollout.decision_timeout`; verify table cases in `internal/config/validate_test.go` cover an
      empty sequence, a `0` and a `101` percentage, a repeated and a descending percentage, a
      sequence ending below 100, and a decision timeout below the window, run by
      `go test ./internal/config/...`
- [x] 1.3 Add load cases proving a configured sequence (a 10% then a 100% wave, both un-gated)
      replaces the default while every other field keeps its value; verify
      `go test ./internal/config/...` passes
- [x] 1.4 Document `rollout.waves` and `rollout.decision_timeout` with the default sequence in
      `deploy/config.yaml`; verify the commented values match `Defaults()` and a file with those
      values loads unchanged

## 2. Rollout records, target resolution, and the fleet-database store

- [x] 2.1 Create `internal/rollout` with its package doc (what the package owns: the rollout and
      wave records, the eligible-pool query, and wave membership resolution) and the stored shapes —
      a rollout record (firmware id, status, workflow id, region, model) and a wave record (rollout
      id, percent, status, success rate, device ids, started at); verify `go build
      ./internal/rollout/` succeeds and the package's exported symbols carry godoc
- [x] 2.2 Add the pure resolution logic with the status vocabulary: the wave id derived from the
      rollout, the wave's sequence position, and its percentage; the cumulative-share computation
      that turns an ordered pool plus the device ids already targeted into a wave's target set; and
      the rollout/wave status constants; verify table-driven tests in `internal/rollout` cover
      shares of 1/5/25/100 over a 1000-device pool (10, 40, 200, 750, disjoint and covering), a
      share smaller than one device (empty set), a share already fully covered by earlier waves,
      and wave ids that are stable for the same rollout and position
- [x] 2.3 Add the Mongo-backed store: query the eligible pool for a selector ordered
      deterministically, read the device ids recorded on a rollout's earlier waves, and write the
      wave document; a wave document that already exists is returned as recorded instead of
      re-resolved, so a retried resolution converges; verify `go vet ./internal/rollout/...` passes
- [x] 2.4 Add the rollout record writes: ensure the document when the rollout starts (firmware id,
      status, workflow id, selector) and move its status afterwards, refusing to move a document out
      of a terminal status; verify `go vet ./internal/rollout/...` passes
- [x] 2.5 Add `internal/rollout/store_integration_test.go` behind `//go:build integration` using the
      `internal/mongotest` harness, proving against real validators and indexes that the pool query
      selects exactly the selector's devices in a stable order, that a wave write is rejected when
      it omits a required field, that a second resolution of the same wave returns the recorded
      membership and start time, and that a terminal rollout status is not overwritten; verify
      `go test -tags integration ./internal/rollout/...` passes with Docker available

## 3. Rollout workflow state, activities, and starter

- [x] 3.1 Add the rollout workflow's contract to `internal/temporal`: the workflow id
      (`rollout-<rollout_id>`), the registered workflow name, the start input (rollout id, firmware
      id, region, model), the approval signal name and payload, the state query name, the status
      vocabulary, and the state and query-view types (status, firmware, selector, the sequence with
      each wave's percentage, recorded outcome, success rate, and target count, the current wave,
      whether an approval is outstanding, and the terminal outcome with the wave that ended the
      rollout); verify table-driven unit tests cover
      the view's derivation from the state — a running rollout, a rollout awaiting approval, and
      each terminal outcome
- [x] 3.2 Add the workflow's activities in `internal/temporal` behind consumed interfaces, each
      taking `context.Context` first: load the firmware's metadata (version, checksum, target
      models), resolve a wave's target membership at a workflow-decided start time, record the
      rollout's state, record a wave's state and success rate, dispatch a wave's update commands to
      the target devices' workflows through the device command seam, and evaluate a wave's health
      through the `wavehealth` aggregator at a workflow-decided moment; verify with fake-backed unit
      tests covering a partial dispatch retry (same command ids, no duplicates), an empty target set
      (no signals), the firmware-mismatch error, and the health activity's result and error mapping
- [x] 3.3 Add the command id derivation (`<wave_id>-<device_id>`) as a pure function with table
      tests proving it is stable across calls and distinct per device and wave; verify `go test
      ./internal/temporal/...` passes
- [x] 3.4 Add the rollout starter beside the device signaler: it starts one workflow execution per
      rollout id under the derived workflow id with the input, refusing an empty firmware id,
      region, or model, and a second start for the same rollout id does not create a second
      execution; verify fake-client unit tests cover the workflow id, the task queue, the input, and
      the rejection path

## 4. The rollout workflow: progression, dispatch, and completion

- [x] 4.1 Implement the workflow's spine in `internal/temporal`: reject an input missing its
      rollout id, firmware id, region, or model; record the rollout as running; then drive the
      configured sequence in order — resolve each wave's membership at a workflow-clock start time,
      record the wave, and advance only after the previous wave was promoted — until the last wave
      is promoted and the rollout is recorded as completed; verify `TestWorkflowEnvironment` tests
      with mocked activities prove waves start in sequence order, only one wave is in flight, the
      rollout completes after the last wave, and an unknown firmware or a firmware that does not
      target the selector's model records the rollout as failed and dispatches nothing
- [x] 4.2 Implement wave dispatch and the skipped-wave path: dispatch each resolved wave's update
      commands through the device command seam, wait for the dispatch to finish, and record a wave
      that resolved no devices as skipped and advance without dispatching or gating; verify workflow
      tests prove every target device receives one command carrying the firmware's id, version, and
      checksum, an empty wave dispatches nothing and is recorded skipped, and a dispatch that fails
      for good rolls the rollout back instead of advancing
- [x] 4.3 Register the state query handler in the workflow and record each transition through the
      recording activities; verify workflow tests prove the query reports the rollout's status, the
      current wave, and each recorded wave's outcome, and that a retried recording activity leaves
      the recorded documents unchanged

## 5. The rollout workflow: durable window, decision, and approval

- [x] 5.1 Implement the durable health window: after a wave is resolved, start a Temporal timer for
      the configured window anchored at the wave's recorded start, dispatch concurrently, and judge
      the wave only once both the dispatch and the timer have completed — never a wall-clock read
      and never a sleep; verify `TestWorkflowEnvironment` tests with time skipping prove a wave is
      judged exactly one window after its start, that a window already elapsed is not re-waited,
      and that a resumed workflow (a replayed run whose worker stopped mid-window) waits only the
      remainder and does not restart the window
- [x] 5.2 Implement the promotion decision: evaluate the wave through the health activity at the
      workflow's decided moment, record a healthy wave and advance, record an unhealthy wave and
      transition the rollout into rollback, and for an undecided verdict re-measure after another
      window until the configured decision timeout has passed since the wave's start, then treat
      the wave as unhealthy; verify workflow tests prove the boundary ratio promotes, a below-
      boundary ratio rolls back, an undecided wave is never promoted, an undecided wave is
      re-measured with its window slid forward, and an undecided wave past the decision timeout
      rolls back
- [x] 5.3 Implement the rollback transition: record the rollout as rolled back with the wave that
      ended it and that wave's decision (verdict, success ratio, sample size, window), start no
      further wave, dispatch no further command, and leave already-promoted waves' records
      unchanged; verify workflow tests prove the failing wave is the one reported, no later wave
      activity runs after the transition, and the query reports the terminal outcome
- [x] 5.4 Implement the approval gate: when a wave's sequence entry requires approval and no
      approval is outstanding, record the rollout as awaiting approval and wait for the approval
      signal before resolving that wave; an approval received while the rollout is running is held
      and consumed by the next wave that requires one; verify workflow tests prove a gated wave is
      not resolved before approval, one approval passes exactly one gated wave, an ungated wave does
      not consume an outstanding approval, an early approval is reported outstanding and used by the
      next gated wave, an approval delivered while no worker is running is applied when the workflow
      resumes, and an approval after a terminal outcome changes nothing

## 6. Worker wiring and documentation

- [x] 6.1 Register the rollout workflow and its activities in `cmd/worker` under their explicit
      names, wire the store, the wave-health aggregator, and the device-command signaler, and
      consume the `rollout` configuration section; verify `go build ./...` succeeds and
      `cmd/worker` tests over a fake registry assert every registered name, that the agent-stream
      dispatch activity is not registered there, and that the configured sequence reaches the
      workflow's start path
- [x] 6.2 Update `cmd/README.md` (what the worker registers now, how a rollout is started
      programmatically, what the rollout settings mean) and the `internal/temporal` package doc
      (the rollout workflow joins the device entity, with the gate and rollback being the saga's
      later steps); verify the documented names and flags match the binary's registration and the
      configuration surface
- [x] 6.3 Document the rollout and wave status vocabularies and the write path in
      `deploy/README.md` — which statuses the workflow records on each collection, that a wave's
      `success_rate` is filled in when its health is decided, and that the wave id is derived from
      the rollout and the wave's position; verify every documented field matches the validator in
      `deploy/mongo/init.js` and the store's writes

## 7. Definition of done and integration verification

- [x] 7.1 Run the full bar from AGENTS.md — `goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...` — and confirm all four pass, with workflow and state-machine
      coverage at or above 90% for `internal/temporal`'s rollout code and domain coverage at or
      above 80% in `internal/rollout`
- [x] 7.2 Run the integration suites this change touches (`go test -tags integration
      ./internal/rollout/... ./internal/mongotest/... ./internal/wavehealth/...`) with Docker
      available and confirm they pass
- [x] 7.3 Smoke the rollout end to end on the local stack: control plane, worker, and agent
      emulator running with a short health window, then start a rollout programmatically and verify
      the waves advance in order, the wave documents fill in with membership, start time, status,
      and success rate, an approval-requiring wave waits until the signal, and a wave driven below
      the success ratio (through the emulator's apply failure rate) ends the rollout as rolled back
- [x] 7.4 Validate the change artifacts with `openspec validate add-rollout-workflow --strict` and
      confirm every requirement in the delta specs is covered by a task or a test
