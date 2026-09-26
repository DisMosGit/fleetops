# Tasks

## 1. State model and pure transitions

- [ ] 1.1 Add the `go.temporal.io/sdk` dependency (`go get go.temporal.io/sdk`, `go mod tidy`) and
  verify `go build ./...` passes with the version pinned in `go.mod`
- [ ] 1.2 Implement `deviceState` in `internal/temporal/device_state.go` — the four owned fields,
  pending-command record with its `dispatched` flag, dedup rings (512 heartbeat event ids, 64
  command ids) and `signals_applied` counter — with table-driven unit tests for ring eviction and
  zero-value validity; verify `go test ./internal/temporal/` passes
- [ ] 1.3 Implement the pure apply transitions (heartbeat strictly-newer rule + firmware update,
  config strictly-newer version, command-result id match + success firmware adoption,
  command-issued set/supersede/no-op) as methods on `deviceState`, with table-driven tests
  covering every scenario in `specs/device-workflow/spec.md` at the state level
- [ ] 1.4 Implement the versioned carry-over encoding (`carry_version`) round trip — encode state
  to the ContinueAsNew payload and decode it back — with tests asserting a full round trip is
  lossless and an unknown future version fails loudly

## 2. DeviceWorkflow entity

- [ ] 2.1 Implement `DeviceWorkflow` in `internal/temporal/device_workflow.go`: selector loop over
  the `heartbeat`, `command_issued`, `command_result`, `config_changed` signal channels and the
  dispatch future, `GetState` query handler, registered under the explicit name
  `device-workflow`; verify `TestWorkflowEnvironment` tests cover lazy creation via
  signal-with-start and a state query reflecting delivered signals
- [ ] 2.2 Add workflow-level idempotency tests: duplicate heartbeat by event id, stale heartbeat
  by timestamp, duplicate command result, config version at-or-below current, duplicate
  `command_issued` for the pending command — verify each leaves the queried state exactly as
  after the first delivery (spec "Signals are idempotent when delivered twice" scenarios)
- [ ] 2.3 Add command-lifecycle workflow tests with a mocked dispatch activity: issued command
  becomes pending and dispatches, a newer command supersedes the pending one, a matching result
  clears pending and adopts the commanded firmware on success, failure leaves firmware
  unchanged, a result for a non-pending command changes nothing
- [ ] 2.4 Implement rolling continuation: count trigger at `maxSignalsPerRun` (128),
  drain-before-continue, `workflow.ContinueAsNew` with the full carry-over payload; verify with
  tests that state is identical before/after rollover, signals delivered around the rollover are
  applied exactly once, and the run ends as a continuation carrying the complete state
  (`ContinueAsNewError` payload assertions), including the dedup rings
- [ ] 2.5 Update the `internal/temporal` package doc (drop the "not implemented" wording) and
  verify godoc covers every exported symbol; run `go test -race -count=2 ./internal/temporal/`
  to confirm the workflow tests are deterministic across runs

## 3. Dispatch activity and signal producers

- [ ] 3.1 Implement `CommandDispatcher` and the `dispatch-command` activity in
  `internal/temporal/dispatch.go` (context first, idempotent on command id, explicit
  registration name, caller-supplied retry policy); verify unit tests with a hand-written fake
  dispatcher cover success, not-connected retry, and superseded-command harmlessness
- [ ] 3.2 Implement the `SignalWithStart`-backed signaler in `internal/temporal/signaler.go` for
  `heartbeat`, `command_result`, `command_issued`, and `config_changed`; verify unit tests with a
  fake Temporal client assert one signal per call, the correct workflow id
  (`device-<device_id>`), and payload fidelity
- [ ] 3.3 Define the consumer-side `DeviceSignaler` seam in `internal/agentserver` and fan every
  routed heartbeat out to it alongside the existing sink; verify `internal/agentserver` tests
  show one signal per routed heartbeat with the original event id, no signal for unenrolled
  devices, and unchanged sink behavior
- [ ] 3.4 Implement `AgentService.Report` in `internal/agentserver`: validate idempotency key,
  command id, device id, and outcome (`codes.InvalidArgument`, operator-safe messages when
  missing), forward the command result to the device workflow, answer `accepted` for first and
  repeated reports alike; verify table-driven tests cover the validation matrix, forwarding,
  and repeated-idempotency-key acceptance with no second effect (fakes for registry and
  signaler)

## 4. Binary wiring

- [ ] 4.1 Wire `cmd/worker`: build the Temporal client from `cfg.Temporal`, register
  `DeviceWorkflow` on `cfg.Temporal.TaskQueue`, join the worker to the existing errgroup
  lifecycle with a clean stop condition; verify `go build ./cmd/...` passes and the probe server
  still serves during a worker run
- [ ] 4.2 Wire `cmd/controlplane`: Temporal client for the signalers, a same-task-queue worker
  registering `dispatch-command` with the `*agentserver.Hub` as `CommandDispatcher`, and the
  heartbeat fan-out (ingest sink + signaler); verify the `internal/agentserver` e2e tests pass
  against the composed wiring and existing tests are unbroken

## 5. Integration verification

- [ ] 5.1 Exercise the full signal path end to end with `TestWorkflowEnvironment` plus fakes:
  heartbeat → state, command issued → dispatch → result → cleared pending, duplicate deliveries
  at both the RPC and signal layers; verify the combined scenarios from the two spec deltas pass
- [ ] 5.2 Run the definition of done — `goimports -w .`, `go vet ./...`, `golangci-lint run`,
  `go test -race -count=1 ./...` — and verify all four commands pass clean
