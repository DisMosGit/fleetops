# Tasks

## 1. Configuration knobs

- [ ] 1.1 Add `Liveness` (`offline_threshold` `"30s"`, `sweep_interval` `"10s"`) and `Telemetry`
      (`batch_size` `500`, `flush_interval` `"1s"`) sections to `internal/config` with defaults
      and strict-key decoding, and verify `go test ./internal/config/...` passes with new
      table-driven cases for absent fields taking the documented defaults
- [ ] 1.2 Extend `Config.Validate` with positive-duration checks for
      `liveness.offline_threshold`, `liveness.sweep_interval`, `telemetry.flush_interval` and a
      positive-int check for `telemetry.batch_size`, each error naming the field; verify new
      subtests (`"0s"`, `"soon"`, `-1`) pass and `go test ./internal/config/...` stays green
- [ ] 1.3 Document the four keys in `deploy/config.yaml` beside their sections and verify a
      `config.Load("deploy/config.yaml")` assertion (subtest in `internal/config`) shows the
      documented values parse and validate

## 2. Persistence stores

- [ ] 2.1 Add `go.mongodb.org/mongo-driver` and `github.com/prometheus/client_golang` to
      `go.mod` (one-line justification each for the PR description) and verify `go mod tidy`
      plus `go build ./...` succeed
- [ ] 2.2 Build the `//go:build integration` MongoDB harness with `testcontainers-go`, running
      `deploy/mongo/init.js` against the container so tests exercise the real schema, and
      verify `go test -tags integration ./internal/devices/...` boots the container and the
      schema checks pass
- [ ] 2.3 Implement `internal/devices.Store` against MongoDB — `Upsert` (register-or-update:
      identity fields, `status: "online"`, `last_heartbeat` = acceptance time, one document per
      identity), a batched per-device state update (`$max` on `last_heartbeat`, `$set` of
      `status`/`current_fw`), and `MarkStale(cutoff)` returning the number of online→offline
      transitions — with integration tests for first registration, in-place reconnect update,
      last-seen refresh, and once-only transition counting
- [ ] 2.4 Implement `internal/telemetry`'s batched ingest writer (bounded queue, flush on
      `telemetry.batch_size` or `telemetry.flush_interval`, unordered `InsertMany` keyed by
      `_id` = event id, duplicate-key errors as no-ops, drain-and-flush on shutdown) and verify
      integration tests cover batched steady ingest, prompt flush on a quiet fleet, redelivery
      no-op, mixed duplicate/new batches, and backpressure instead of unbounded buffering

## 3. Stream integration

- [ ] 3.1 Extend the `agentserver` seams: `HeartbeatSink.Handle` receives the device's
      registered identity (region/model), the session records identity fields at enrollment,
      and an accepted registration calls the registry `Upsert` while a rejected one writes
      nothing; verify `go test ./internal/agentserver/...` with hand-written fakes covers
      upsert-on-accept, no-write-on-reject, and meta propagation
- [ ] 3.2 Reject heartbeats without a measurement time (`ts`) before any persistence, with an
      `InvalidArgument` status like the existing missing-field checks, and verify the fake-sink
      tests show neither a telemetry event nor a device-state update results from such a
      heartbeat
- [ ] 3.3 Replace `loggingSink` in `cmd/controlplane` with the wired ingest pipeline (registry
      `Upsert` + telemetry sink) and verify `go test ./internal/agentserver/...` and
      `go build ./cmd/controlplane` pass

## 4. Liveness sweep and metric

- [ ] 4.1 Serve Prometheus `/metrics` on `observability.metrics_addr` from
      `cmd/controlplane` using a private `prometheus.Registry`, and verify a test scrapes the
      handler and finds the exposition format
- [ ] 4.2 Implement the staleness sweep (`internal/devices`, ticker at `liveness.sweep_interval`,
      cutoff `now - liveness.offline_threshold`, `MarkStale`) incrementing
      `fleetops_device_offline_transitions_total` by exactly the transition count, and verify
      unit tests with a fake store assert once-per-transition counting, no counting for already
      offline devices, and no marking of devices still reporting
- [ ] 4.3 Wire the sweep into `cmd/controlplane`'s errgroup with a stop condition tied to
      `ctx` and verify the wired process marks a silent device offline and increments the
      counter once (integration test against the harness)

## 5. End-to-end verification

- [ ] 5.1 Add an integration test spanning the path — in-process gRPC stream: registration
      upserts the device record, heartbeats land as batched `telemetry` documents with `ts` and
      `meta`, silence past the threshold flips `offline` and counts one transition — and verify
      it passes with `-tags integration`
- [ ] 5.2 Run the definition of done (`goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...`) and verify all four pass; run
      `go test -race -tags integration -count=1 ./...` as the integration bar
