# Tasks

## 1. Configuration and workflow settings

- [ ] 1.1 Add the `snapshots` section (`interval`, Go duration, default `"1m"`) to
  `internal/config` — Config field, Defaults entry, and validation rejecting a non-positive or
  unparseable `snapshots.interval` with the field named — with table-driven tests covering
  default, override, and invalid values; verify `go test ./internal/config/` passes
- [ ] 1.2 Update the `deploy/config.yaml` sample with the `snapshots` section and its comment;
  verify `TestSampleConfigLoads` (the sample must keep decoding to the built-in defaults) passes
- [ ] 1.3 Thread workflow settings into the entity's start args: a `DeviceSettings` value
  (snapshot interval + offline threshold) carried in `deviceState`, built in `cmd/controlplane`
  from `snapshots.interval` and `liveness.offline_threshold` and passed through `NewSignaler`
  into `newDeviceState` on every `SignalWithStart`; verify signaler unit tests assert the start
  args carry the settings and `go test ./internal/temporal/ ./cmd/controlplane/` passes

## 2. State model and pure transitions

- [ ] 2.1 Extend `deviceState` and `State` in `internal/temporal/device_state.go` with the
  identity attributes (`region`, `model`), the liveness status (`online`), and the settings, and
  bump `carry_version` to 2 with `validate` refusing older payloads via
  `ErrUnsupportedCarryVersion`; verify table-driven tests cover a lossless round trip, the new
  fields in the state query view, and a loud refusal of a carry-version-1 payload
- [ ] 2.2 Extend `HeartbeatSignal` with the registration's region and model and the apply
  transitions with their observable outcomes: identity adoption (non-empty values win), the
  liveness judgement (`workflow.Now`-based recency against the settings' offline threshold) with
  its online/offline flips, and a report of which meaningful transitions each apply performed
  (firmware adoption, pending-command set/superseded/concluded, config apply, identity adoption,
  liveness flip); verify table-driven tests cover every scenario in
  `specs/device-workflow/spec.md` at the state level
- [ ] 2.3 Implement the search-attribute derivation as one pure function of state
  (`DeviceRegion`, `DeviceModel`, `DeviceFirmware`, `DeviceOnline`) plus change detection against
  the previously derived map; verify table-driven tests assert the derived values, that a
  heartbeat-only timestamp refresh derives an unchanged map, and that each mirrored transition
  derives a changed one

## 3. Snapshot activity and store

- [ ] 3.1 Implement the `StateSnapshotter` seam and the `snapshot-device-state` activity in
  `internal/temporal/snapshot.go` (context first, explicit registration name, no retry policy of
  its own, snapshot payload = the state view plus `snapshot_at`) with unit tests over a
  hand-written fake snapshotter covering payload fidelity and wrapped write errors
- [ ] 3.2 Implement `SnapshotStore` in `internal/devices` on `device_state_snapshots`: one
  document per `_id`, updated in place, with the `$max`-guarded `snapshot_at` write so retries
  are no-ops and out-of-order writes converge to the newest state; verify integration tests
  (`//go:build integration`, via `internal/mongotest`) cover upsert-in-place, retry idempotency,
  reversed-write-order convergence, and rejection of documents missing required fields
- [ ] 3.3 Extend `deploy/mongo/init.js` and `deploy/mongo/verify.sh` with the
  `device_state_snapshots` collection — schema validator on the required fields, unique `_id`,
  `idx_region_model`, `idx_online` — and document the collection in the data-model section of
  `deploy/README.md`; verify `deploy/mongo/verify.sh` passes against a MongoDB the bootstrap was
  applied to and the mongotest-backed integration tests still start

## 4. Workflow scheduling and search-attribute maintenance

- [ ] 4.1 Add snapshot scheduling to `DeviceWorkflow`: a durable timer rearmed every
  `settings.snapshot_interval`, an immediate snapshot on each meaningful transition reported by
  the apply methods, and failure isolation (the activity future's error is logged at the
  boundary and never fails or blocks the entity, with the caller-supplied retry policy on the
  activity); verify `TestWorkflowEnvironment` tests cover the periodic cadence, immediate
  snapshots on firmware/command/config/liveness transitions, no immediate snapshot on a routine
  heartbeat, and entity survival when the snapshot activity fails permanently
- [ ] 4.2 Maintain search attributes in `DeviceWorkflow`: upsert the full derived map at run
  start and re-upsert only when the derived map changes, keeping the workflow free of
  `time.Now`, `rand`, and direct I/O and keeping `device_workflow.go` under the ~400 LOC split
  threshold (helpers live in `searchattrs.go`/`snapshot.go`); verify `TestWorkflowEnvironment`
  tests assert attribute values at run start, after a firmware change, after each liveness flip,
  no upsert on a routine heartbeat, and unchanged values across a rolling continuation
- [ ] 4.3 Verify determinism of the changed workflow: `go test -race -count=2
  ./internal/temporal/` passes with stable results, and the rollover tests confirm the carried
  state (identity, liveness, settings) survives `ContinueAsNew` exactly

## 5. Worker binary and replica safety

- [ ] 5.1 Extend `cmd/worker`: open the Mongo client, bind `SnapshotStore` into the
  `snapshot-device-state` activity, and register the device workflow and the activity on
  `cfg.Temporal.TaskQueue` under their explicit names; verify `go test ./cmd/worker/` covers the
  wire-up (registration names and the store binding) and `go build ./cmd/worker` succeeds
- [ ] 5.2 Add search-attribute bootstrap to `cmd/worker` startup: ensure the four custom
  attributes exist in the configured namespace before polling, treating an existing attribute as
  success and any other error as startup failure; verify tests over a fake operator client cover
  fresh registration, re-registration, and the failure path, and `go test ./internal/temporal/
  ./cmd/worker/` passes
- [ ] 5.3 Confirm the worker's lifecycle and replica story: bounded graceful shutdown on
  SIGTERM (stop polling, drain in-flight work, exit cleanly) and per-replica probes on
  `observability.health_addr`; verify by running two `cmd/worker` replicas against the dev stack
  and observing that signals advance workflows and snapshots keep landing (one document per
  device in `device_state_snapshots`) with either replica stopped
- [ ] 5.4 Update the worker entrypoint documentation in `cmd/README.md` (what it registers, the
  search-attribute bootstrap, how to run replicas) and the `internal/temporal` package doc for
  the entity's extended state and snapshot behavior; verify the docs match the binary's flags
  and registered names

## 6. Integration verification

- [ ] 6.1 Run the full definition of done — `goimports -w .`, `go vet ./...`,
  `golangci-lint run`, `go test -race -count=1 ./...` (plus the `integration`-tagged suite) —
  and fix every finding without disabling linters
- [ ] 6.2 Smoke the whole path on the local stack: control plane + two worker replicas + agent
  emulator; verify in the Temporal UI that device runs filter by region, model, firmware version,
  and online status (including a device flipping offline after the threshold), and in MongoDB
  that `device_state_snapshots` shows fresh snapshots at the configured cadence and immediate
  ones after a firmware change
