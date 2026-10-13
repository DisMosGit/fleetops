# Tasks

## 1. A configured test run every package shares

- [x] 1.1 Add `internal/testexec/profile.go`: the documented `FLEETOPS_TEST_*` variable set (profile,
      simulated fleet size, wait-budget multiplier, container-startup budget, container use), defaults
      equal to today's constants, a weak-host profile sized for 4 GB / 4 CPU, and resolution that
      fails naming the variable and the rejected value; verify with
      `go test -race -count=1 ./internal/testexec/` over a table covering defaults, every variable,
      the weak profile, and each invalid case (unknown profile, zero, negative, non-numeric), and
      verify the same environment plus a different CPU count resolves identically, since nothing may
      probe the host
- [x] 1.2 Add the shared-stack entry point to `internal/mongotest`: a package-level stack started once
      and reached by tests, per-test database names carrying the `deploy/mongo/init.js` bootstrap, and
      the existing per-test `Start` left intact; verify with
      `go test -tags integration -count=1 ./internal/mongotest/` and a test asserting two tests' data
      land in different databases and that the second never sees the first's documents
- [x] 1.3 Add the shared-stack entry point to `internal/rabbittest`: one container per package, reuse of
      the existing `Purge` for per-test isolation, and the existing stop/start/restart support
      preserved; verify with `go test -tags integration -count=1 ./internal/rabbittest/` and a test
      publishing before and after a purge
- [x] 1.4 Add the shared-stack entry point to `internal/temporaltest`: one server per package with a
      namespace created per test through `Client.WorkflowService().Register`, each test's client bound
      to its own namespace, and the readiness probe unchanged; verify with
      `go test -tags integration -count=1 ./internal/temporaltest/` and a test asserting two tests'
      workflows are invisible to each other
- [ ] 1.5 Migrate every integration-test file that calls `mongotest.Start`, `rabbittest.Start`, or
      `temporaltest.Start` to the shared stack and `internal/testexec` sizes and budgets — the shared
      stack is started once per package, not once per test — verify with
      `go vet -tags integration ./...`, then `go test -tags integration -count=1 ./internal/telemetry/`
      and `go test -tags integration -count=1 ./internal/devices/` passing with the same assertions as
      before
- [x] 1.6 Migrate `cmd/controlplane`'s integration tests to the shared stack, including the
      end-to-end pipeline test, and verify with
      `go test -tags integration -count=1 ./cmd/controlplane/` passing with its assertions unchanged
- [x] 1.7 Document the variable set and the weak-host profile in `CONTRIBUTING.md` (a "Running the
      tests" section beside Definition of Done) and add a Makefile target that runs the integration
      suite with the weak-host profile; verify `make help` lists the target and that the documented
      command runs as written

## 2. Make the existing suite trustworthy before building on it

- [ ] 2.1 Fix `TestRolloutSmoke`'s restore-order assertion in `cmd/worker/rollout_integration_test.go`:
      hoist the first wave's record out of its `if` scope and pass its device ids to
      `commandPosition` instead of a wave id, so the reverse-wave-order property is actually asserted;
      verify `go test -tags integration -count=1 -run TestRolloutSmoke ./cmd/worker/` no longer fails
      at the restore-order assertion, and that reverting the helper call makes it fail again
- [x] 2.2 Fix `TestRolloutSmoke`'s announcement assertion: drain what the earlier rollback left on
      `fleetops.rollout.notifications` before asserting the depth this case expects, so the assertion
      measures its own case; verify `go test -tags integration -count=1 -run TestRolloutSmoke
      ./cmd/worker/` no longer fails the `AwaitDepth` assertion
- [ ] 2.3 Fix `TestRolloutSmoke`'s heartbeat sample identity: give each heartbeat stream a generation of
      its own so a flipped fleet's samples are distinct documents, and verify the flipped fleet's
      health actually changes the evaluated ratio — assert in the smoke that the failing wave's
      success rate is below `MinSuccessRatio` (the assertion at `rollout_integration_test.go:365`
      currently cannot detect the stale-health case)
- [x] 2.4 Make `TestRolloutSmoke` run rather than skip: replace the `temporal` CLI dependency with the
      containerized harness, or make the CLI lookup a hard requirement of the integration profile
      rather than a silent skip; verify `go test -tags integration -count=1 -run TestRolloutSmoke
      ./cmd/worker/` runs the smoke and does not report SKIP
- [ ] 2.5 Record the shared-stack move's effect: re-run the two heaviest packages and record each one's
      wall-clock time and observed peak container memory beside the numbers in `design.md` §Context,
      so the weak-host claim rests on measurement; verify the recorded numbers against a fresh
      `docker stats` sample taken during `go test -tags integration -count=1 ./cmd/worker/`

## 3. The coverage ledger and the tests it finds missing

- [ ] 3.1 Add `openspec/coverage.md`: one entry per capability scenario with its capability, the test
      function backing it, and the file that test lives in, plus an exemptions section where each
      entry names the reason and what would verify it; verify the file parses and every listed test
      function exists by running the checker from 3.2
- [ ] 3.2 Add the checker as a unit test: parse `openspec/specs/*/spec.md` for scenarios and
      `openspec/coverage.md` for entries, fail when a scenario has neither an entry nor an exemption,
      fail when an entry names a scenario that no longer exists, and fail when an entry's test
      function is not found in the named file; verify `go test -race -count=1 ./internal/speccoverage/`
      passes and that deleting one entry, or renaming one referenced test, makes it fail
- [x] 3.3 Add the missing tests the ledger records for the agent emulator: real-ticker cadence over a
      configured period and a fleet of 100 and 1000 devices run and cancelled within a bounded time
      with no goroutine left behind, in `internal/agent/fleet_test.go`; verify
      `go test -race -count=1 ./internal/agent/` and record the fleet sizes' runtime
- [x] 3.4 Add tests for the `cmd/agent` binary's wiring — fleet size, heartbeat period, download
      directory, the relay that answers dispatched commands, and shutdown on context cancellation —
      which today has no test file at all; verify `go test -race -count=1 ./cmd/agent/`
- [x] 3.5 Add a `DeviceWorkflow` history replay test in `internal/temporal/`, replaying a recorded
      device history the way the rollout history is replayed today, with a negative control that
      rejects a foreign workflow's history; verify `go test -race -count=2 ./internal/temporal/`
- [ ] 3.6 Add the remaining tests the ledger records as unbacked and not exempt — the agent-server
      interceptor chain installing both interceptors and mapping a panic to `Internal`, `Report`
      idempotency observed at the workflow rather than the seam, and the rollout work queue's
      dead-letter path — and update `openspec/coverage.md` for each; verify
      `go test -race -count=1 ./internal/agentserver/ ./internal/telemetry/` and the checker from 3.2
- [x] 3.7 Pin the telemetry collection's actual contract in `internal/mongotest/schema_test.go`: that
      it is a plain collection with the TTL index and the `_id` uniqueness the ingestion no-op
      depends on, so the divergence from `mongo-data-model`'s time-series wording is asserted rather
      than incidental; verify `go test -tags integration -count=1 ./internal/mongotest/`

## 4. The rollout actually reaching an agent

- [ ] 4.1 Add the join test: a rollout whose device command seam is the real dispatch activity bound to
      a real `agentserver.Hub`, with a real agent client on the stream, asserting the command reaches
      the agent carrying the wave's firmware id, version, and checksum, that the agent's report
      reaches the device workflow, and that the wave records that device's outcome; verify
      `go test -tags integration -count=1 -run TestRolloutReachesARealAgent ./cmd/controlplane/` and
      that changing the command's checksum field makes it fail
- [ ] 4.2 Add it to the ledger as the backing test for the scenarios it covers (the per-device update
      dispatch and the report hop) and verify the checker from 3.2 passes

## 5. Emulator fault injection and bounded download retry

- [x] 5.1 Add the emulator's fault configuration to `internal/config`: the fault share, the fault
      kinds, the stall duration, and the retry attempt budget and backoff cap, each validated, with an
      absent or zero value meaning no faults; verify `go test -race -count=1 ./internal/config/` over
      a table of valid and rejected values
- [x] 5.2 Add the seeded fault injector and the retry classification to `internal/agent`: a fetcher
      decorator that draws per-device decisions from the emulator's seeded source and injects a
      stall or a transfer failure, and an error classification marking transfer and stall faults
      retryable while checksum and protocol faults are not; verify
      `go test -race -count=1 ./internal/agent/` covering the same seed replaying the same faults, a
      stall ending with the context, and each error's classification
- [x] 5.3 Add bounded retry to `handleUpdate` in `internal/agent/update.go`: retry only the fetch,
      with growing capped delays and the attempt budget, keep phase reports and the single terminal
      result outside the loop, and stop promptly when the command's context ends; verify
      `go test -race -count=1 -count=2 ./internal/agent/` over a table covering success on a later
      attempt, an exhausted budget producing exactly one failed result, a non-retryable checksum
      mismatch failing on the first attempt, and cancellation during a retry wait
- [ ] 5.4 Wire the fault configuration and the decorating fetcher into `cmd/agent` and document the
      settings in `cmd/README.md`; verify `go test -race -count=1 ./cmd/agent/` asserting the fetcher
      is installed when faults are configured and absent when they are not, and that the documented
      settings match the configuration fields
- [x] 5.5 Add the dead-letter test the fault path exists to demonstrate: a report a faulted device
      produces that the consumer cannot process travels its retry ladder and lands in the dead-letter
      queue with its reason and attempt count; verify
      `go test -tags integration -count=1 -run TestFaultedReportReachesTheDeadLetterQueue ./internal/telemetry/`
- [ ] 5.6 Add the scenario-to-test entries for fault injection to `openspec/coverage.md` and verify the
      checker from 3.2 passes

## 6. Firmware upload as a workflow

- [x] 6.1 Add the workflow identity, input, state, and the `state` field's transitions in
      `internal/temporal/firmware_state.go` and `firmware_workflow.go`, registered under an explicit
      name, with the storage activity's input carrying the path of the spooled binary rather than
      its bytes; verify `go test -race -count=1 ./internal/temporal/` over the state transitions and
      the identity derivation, including that a second upload of one version reaches the existing
      execution
- [x] 6.2 Make the upload steps activity-callable in `internal/firmware`: target-model validation,
      streaming checksum plus GridFS storage addressed by version, and the metadata upsert, each
      idempotent under retry; verify `go test -race -count=1 ./internal/firmware/` asserting a
      retried storage step leaves one GridFS object and a retried metadata write one record
- [x] 6.3 Wire the workflow and its activities into `cmd/controlplane` on the control-plane task
      queue, with the store binding, the spool directory, and the workflow registration they need,
      and register nothing for them in `cmd/worker`; verify
      `go test -race -count=1 ./cmd/controlplane/ ./cmd/worker/` asserting the control plane's
      registered set gains exactly the upload workflow and activities and the worker's set is
      unchanged
- [x] 6.4 Serve the state query and start-or-attach from the upload endpoint, keeping the endpoint's
      existing contract (`201` with the record, `422` for a refused model, `409` for a version
      conflict) and answering the conflict when a concurrent upload loses the race: spool the
      request body to the configured directory before starting the workflow, remove the spool file
      when the upload concludes, and sweep files abandoned by a crash by age at startup; verify
      `go test -race -count=1 ./internal/firmware/` over a table of the endpoint's status codes plus
      a concurrency case asserting exactly one record
- [ ] 6.5 Update the firmware workflow's scenario-to-test entries in `openspec/coverage.md` and verify
      the checker from 3.2 passes
- [x] 6.6 Update `cmd/README.md`'s control-plane section for the upload workflow and its state query,
      including the spool directory setting and the task queue the upload runs on, and `docs/` where
      the upload path is described, so the documented behavior matches the new ownership; verify the
      documented task queue, spool setting, and registered names against the binary's flags by
      running `go test -race -count=1 ./cmd/controlplane/` and reading the registration assertions

## 7. The eligible-device view from the change stream

- [ ] 7.1 Add replica-set support to `internal/mongotest`: a shared stack the tests that need change
      streams can request, initiated as a single-node replica set with the bootstrap applied, while
      the default shared stack stays as it is; verify
      `go test -tags integration -count=1 -run TestReplicaSetStack ./internal/mongotest/` shows the
      stack accepting a change stream, and record the startup budget it needs
- [ ] 7.2 Add the watcher in `internal/rollout/`: a change stream over `devices` that updates a
      selector-keyed view of ordered device ids, coalescing a burst of changes into one refresh per
      affected selector, with the resume token kept, a full flush when the token is unusable, and a
      cursor released when the context ends; verify
      `go test -tags integration -count=1 ./internal/rollout/` covering a new device appearing, a
      model change moving a device between selectors, a removal, a redelivered change being a no-op,
      an interrupted stream losing nothing, and a flushed view rebuilding to exactly what a full read
      returns
- [ ] 7.3 Make wave resolution read the view in `internal/rollout/store.go`, keeping the pool ordered
      by device identity, the resolve-once-and-record rule, the converge-on-retry behavior, and an
      empty selector resolving to an empty pool; verify
      `go test -tags integration -count=1 ./internal/rollout/` plus
      `go test -race -count=1 ./internal/rollout/` with the existing resolution assertions unchanged
- [ ] 7.4 Add the test that proves no query is paid per resolution: after a device change, a wave
      resolution reflects the change while the Mongo command monitor records no `find` over
      `devices`; verify `go test -tags integration -count=1 -run TestResolutionReadsTheView
      ./internal/rollout/`, and that the same test fails when the view is replaced with the old query
- [ ] 7.5 Start the watcher with `cmd/worker` before the first wave resolution, stop it with the
      worker's context, and fail worker startup when the stream cannot be established; verify
      `go test -race -count=1 ./cmd/worker/` asserting the watcher's lifecycle and the startup
      failure path, and `go test -tags integration -count=1 ./cmd/worker/` still passing
- [ ] 7.6 Update `cmd/README.md` and `deploy/README.md` for the watcher's lifecycle and the
      replica-set requirement the change stream adds to the environments that run the worker; verify
      the documented requirement matches `deploy/mongo/init.js` and the harness by running
      `go test -tags integration -count=1 ./internal/mongotest/`
- [ ] 7.7 Add the eligible-pool scenarios' entries to `openspec/coverage.md` and verify the checker
      from 3.2 passes

## 8. The whole thing, verified

- [ ] 8.1 Run the Definition of Done on the whole change: `goimports -w .`, `go vet ./...`,
      `golangci-lint run`, `go test -race -count=1 ./...`, and fix every finding without disabling a
      linter
- [ ] 8.2 Run `go vet -tags integration ./...` and the integration suite with the weak-host profile
      (`make <target from 1.7>`), record the wall-clock time and the observed peak memory, and state
      plainly whether the recorded peak is within a 4 GB machine's budget and that no run on such a
      machine was possible here
- [x] 8.3 Investigate `internal/temporal.TestWorkflowSupersededDispatchIsHarmless`, recorded in the
      repository's debt register as having failed once in a full integration run while passing
      standalone: attempt to reproduce it under `-race -count=20` and under load, and either fix the
      cause with a regression test or record what was tried and what was observed — verify the record
      names the commands run and their outcomes rather than asserting the flake is unrelated
- [ ] 8.4 Confirm the coverage gate is green and honest: verify the checker from 3.2 passes, and that
      every exemption in `openspec/coverage.md` names a reason and names what would verify it
- [ ] 8.5 Verify the change artifacts are internally consistent before hand-off:
      `openspec validate "add-missing-feature-coverage" --strict` passes, every scenario in this
      change's own deltas appears in `openspec/coverage.md`, and the five conditions this change
      recorded as out of scope — the SSE and device-list endpoints, abort semantics, the time-series
      wording, retention as one value, and the unconsumed rollout queue — are still named in
      `proposal.md` rather than quietly dropped
- [ ] 8.6 Record the outcome in a `## Recorded verification` section of this file: the capability and
      scenario counts before and after, the number of scenarios newly backed, the exemptions that
      remain and why, the measured suite cost, and any divergence from what `design.md` predicted —
      stating differences rather than smoothing them
