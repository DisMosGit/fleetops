# Tasks

## 1. Configuration value for the control-plane queue

- [x] 1.1 Add `temporal.dispatch_task_queue` to `internal/config`: the field on the `Temporal`
  section, its default `"fleetops-controlplane"` in the defaults table, and validation rejecting
  an empty value with the field named; verify table-driven tests cover the default, an explicit
  override, and the empty-value rejection, and `go test ./internal/config/` passes
- [x] 1.2 Update the `deploy/config.yaml` sample with `temporal.dispatch_task_queue` and its
  comment, stating that it is the queue the control plane polls for device command dispatch and
  that it must differ from `temporal.task_queue`; verify `TestSampleConfigLoads` (the sample must
  keep decoding to the built-in defaults) passes

## 2. The dispatch queue rides the carried device state

- [x] 2.1 Add `DispatchTaskQueue` to `DeviceSettings` in `internal/temporal/device_state.go`,
  bump `carry_version` from 5 to 6 with its version comment extended, and extend `validate` to
  refuse a carried state whose dispatch queue is empty alongside the existing positive-interval and
  positive-threshold guards; verify table-driven tests cover a lossless round trip carrying the
  queue, the loud refusal of a carry-version-5 payload via `ErrUnsupportedCarryVersion`, and the
  empty-queue refusal
- [x] 2.2 Set `DispatchTaskQueue` at both `DeviceSettings` construction sites — `cmd/controlplane`
  and `cmd/worker` — from `cfg.Temporal.DispatchTaskQueue`; verify `go test ./cmd/controlplane/
  ./cmd/worker/` passes and the signaler tests assert the start args carry the configured queue

## 3. The device workflow schedules dispatch on the control-plane queue

- [x] 3.1 Give the device workflow's dispatch activity options an explicit `TaskQueue` taken from
  `state.Settings.DispatchTaskQueue`, leaving the snapshot activity on the workflow's own queue;
  verify a `TestWorkflowEnvironment` test pins the activity's task queue using
  `SetActivityTaskQueue(settings.DispatchTaskQueue, DispatchActivityName)` and fails when the
  activity is scheduled on any other queue, so the queue is asserted rather than assumed

## 4. The control plane polls only its own queue

- [x] 4.1 Change `cmd/controlplane` to poll `cfg.Temporal.DispatchTaskQueue` instead of
  `cfg.Temporal.TaskQueue`, keeping `dispatch-command` its only registration and keeping
  `NewSignaler`/`NewRolloutStarter` targeting `cfg.Temporal.TaskQueue` so workflows still start on
  the work queue; verify a unit test over the registry seam asserts the polled queue and the
  registration set, and `go build ./cmd/controlplane` succeeds
- [x] 4.2 Give the control plane's worker a bounded `WorkerStopTimeout` matching the worker
  binary's, and introduce the same small `registry` seam `cmd/worker` uses so registration is
  asserted directly instead of only through a running server; verify `go test
  ./cmd/controlplane/` covers the seam and `go vet ./...` passes
- [x] 4.3 Confirm the two binaries agree on the topology in code: `cmd/worker` polls only
  `cfg.Temporal.TaskQueue` and registers no dispatch activity, `cmd/controlplane` polls only
  `cfg.Temporal.DispatchTaskQueue` and registers nothing else; verify a test in each package
  asserts its binary never registers the other's task types, so a future workflow added to the
  work queue cannot be silently picked up by the control plane

## 5. Documentation

- [x] 5.1 Update `cmd/README.md` to state the queue topology: which binary polls which queue, that
  `dispatch-command` runs on `temporal.dispatch_task_queue` beside the agent hub, that workers are
  never registered on the control-plane queue, and that both binaries must be deployed together;
  verify the documented queue names and flags match the binaries' configuration surface
- [x] 5.2 Update `deploy/README.md`'s configuration section to list the new
  `temporal.dispatch_task_queue` value and why the dispatch activity has its own queue; verify the
  documented default matches `internal/config`'s defaults table

## 6. Definition of done

- [x] 6.1 Run the full definition of done — `goimports -w .`, `go vet ./...`, `golangci-lint run`,
  `go test -race -count=1 ./...` (plus the `integration`-tagged suite) — and fix every finding
  without disabling linters; verify `goimports -l .` reports nothing, `golangci-lint run` reports
  0 issues, the race-enabled suite passes, and `go test -race -count=2 ./internal/temporal/`
  passes with stable results
- [x] 6.2 Confirm the fix's observable claim at the level this environment allows: with the
  control plane polling only `temporal.dispatch_task_queue`, no `device-workflow`,
  `snapshot-device-state`, or rollout task type is ever delivered to it; verify with a test that
  drives the control plane's registry seam and asserts the registered set contains
  `dispatch-command` and none of the work queue's task types, and record that the live two-replica
  and end-to-end smoke recheck remains the deferred tasks 5.3 and 6.2 of
  `add-device-search-attributes-snapshot-and-worker`

## Recorded verification

Definition of done, run against this working tree:

- `goimports -l .` reports nothing; `go vet ./...` is clean; `golangci-lint run` reports 0 issues.
- `go test -race -count=1 ./...` passes for every package, and `go test -race -count=2
  ./internal/temporal/` passes with stable results.
- The `integration`-tagged suite passes for every package except `cmd/worker`, where the
  pre-existing `TestRolloutSmoke` fails on its own two documented defects (rollback restore order
  across waves, and `fleetops.rollout.notifications` depth). It is not this change's: that test
  registers only `registerRollout`, creates no device workflow, and so never reaches the device
  settings, the carry version, or `dispatch-command`. Measured on a clean worktree of HEAD
  `b0e2afa` with none of this change applied, it fails the same way on the same two assertions
  (`rollout_integration_test.go:586` and `:614`) — the same pair the project roadmap already tracks
  under P2. `TestRolloutSmoke` needs 8–10 minutes by itself, so a bare
  `go test -tags integration ./...` also exceeds the default per-package timeout when the rest of
  the tree compiles and runs alongside it.
- Topology, asserted rather than assumed: `TestStartWorker` (cmd/controlplane) captures the queue
  the process hands to `worker.New` and asserts it is `temporal.dispatch_task_queue` and not the
  work queue, and asserts the registered set is exactly `dispatch-command` with none of the work
  queue's twelve task types. `TestWorkerRegistersTheWholeWorkQueue` (cmd/worker) asserts the
  mirror image: all twelve registered, `dispatch-command` never.
- Scheduling, asserted at the workflow: `TestDeviceWorkflowDispatchTaskQueue` binds
  `dispatch-command` to the control-plane queue alone, so the activity executes only if the
  workflow scheduled its task there. Removing the activity's `TaskQueue` option makes it fail,
  leaving the pending command undispatched — the same silent-strand the defect produced.
- The negative controls above were run and observed to fail before being restored, so the
  assertions are not vacuous.

The live check is still deferred: whether the bounce is gone under real load and whether snapshot
staleness returns inside its bound remains tasks 5.3 and 6.2 of
`add-device-search-attributes-snapshot-and-worker`, where the deferral and what unblocks it are
recorded. A Temporal server can be booted here — `TestRolloutSmoke` does — but there are no deploy
manifests, so the split topology (control plane, worker replicas, and agents together, with the
Temporal UI to observe delivery) cannot be run end to end; that is the standing deferral recorded
in the proposal's Out of scope.
