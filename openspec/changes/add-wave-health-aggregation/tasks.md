# Tasks

## 1. Rollout v1 contract

- [x] 1.1 Add `api/proto/rollout/v1/rollout.proto` defining package `fleetops.rollout.v1`, the
      `RolloutService.GetWaveHealth` unary RPC, `GetWaveHealthRequest` (`rollout_id`, `wave_id`),
      `GetWaveHealthResponse` (echoed ids, `success_ratio`, `sample_size`, `verdict`, window start
      and end, per-sample health threshold, minimum success ratio, minimum sample count), and the
      `WaveHealthVerdict` enum with unspecified, undecided, healthy, and unhealthy values; verify
      `make proto-tools && make proto` regenerates `rollout.pb.go` and `rollout_grpc.pb.go` and
      `go build ./...` succeeds
- [x] 1.2 Add `api/proto/rollout/v1/rollout_test.go` mirroring the agent contract test: one
      populated request and response survive a proto round trip unchanged, and a guard asserts the
      frozen field numbers/types of this new v1 package; verify `go test ./api/proto/rollout/...`
      passes

## 2. Wave record membership and start time

- [x] 2.1 Extend the `waves` validator in `deploy/mongo/init.js` so `device_ids` (array of device
      identities, empty array allowed) and `started_at` (date) are required alongside the existing
      fields; verify a fresh bootstrap applies (the `internal/mongotest` harness runs `init.js` on
      `Start`) and `go test -tags integration ./internal/mongotest/...` still passes
- [x] 2.2 Add both fields to the expected required set for `waves` in `deploy/mongo/verify.sh`;
      verify the script's list matches the validator in `init.js` and `bash -n deploy/mongo/verify.sh`
      reports no syntax error
- [x] 2.3 Document `device_ids` and `started_at` in the `waves` collection table in
      `deploy/README.md`, including that a wave's health evaluation never reads heartbeats older
      than `started_at`; verify the table lists every required field the validator enforces
- [x] 2.4 Extend `internal/mongotest/schema_test.go` to prove the `waves` validator rejects a
      document missing `device_ids` or `started_at` and accepts one whose `device_ids` is an empty
      array; verify `go test -tags integration -run TestSchema ./internal/mongotest/...` passes

## 3. Rollout gating configuration

- [x] 3.1 Add a `Rollout` section to `internal/config/config.go` (`health_window`,
      `sample_health_threshold`, `min_success_ratio`, `min_samples`) with defaults `"5m"`, `0.6`,
      `0.95`, and `10`, wired into `Config` and `Defaults()`; verify `go build ./...` succeeds
- [x] 3.2 Add the section's validation to `internal/config/validate.go`: `health_window` a positive
      Go duration, `min_samples` a positive integer, and both ratios numbers in `[0, 1]`, each
      error naming the offending field; verify with table cases in `internal/config/validate_test.go`
      covering `"soon"`/`"0s"` windows, a zero and a negative sample count, and out-of-range ratios,
      run by `go test ./internal/config/...`
- [x] 3.3 Cover the defaults in `internal/config/config_test.go` (absent `rollout` section yields
      `"5m"`, `0.6`, `0.95`, `10`; present values override them) and document the section with its
      defaults in `deploy/config.yaml`; verify `go test ./internal/config/...` passes and the sample
      file's commented section matches the defaults table

## 4. Wave health aggregation

- [x] 4.1 Create `internal/wavehealth` with the evaluation types (query, result carrying ratio,
      sample size, verdict, window, and thresholds), the sentinel not-found error, the consumed
      `WaveSource` and `SampleStore` interfaces, and `Evaluate` implementing the effective window
      `[max(at − health_window, wave.started_at), at]`, the per-sample success test, and the
      three-state verdict with its inclusive boundary; verify `go build ./internal/wavehealth/`
      succeeds
- [x] 4.2 Add `internal/wavehealth/wavehealth_test.go` as table-driven tests over hand-written
      fakes, covering empty windows (no samples, no target devices, wave not started), partial
      windows (clipped at the wave start; fewer samples than the minimum), transient failures (a
      burst that stays above the gate, a sustained dip that crosses it, recovery once the dip ages
      out), the boundary (ratio exactly at the minimum is healthy; one more failing sample flips
      it), and that an empty target set performs no sample query; verify `go test -race
      ./internal/wavehealth/...` passes with `-cover` at or above 80%
- [x] 4.3 Add the Mongo implementations beside the aggregation: a wave source reading `rollouts`
      and `waves` that maps an unknown rollout, an unknown wave, and a wave belonging to another
      rollout to the not-found sentinel, and a sample store computing total and successful samples
      in one `$match` + `$group` pipeline served by the telemetry device/time index, short-
      circuiting an empty device set; verify `go vet ./internal/wavehealth/...` passes and `go test
      ./internal/wavehealth/...` still passes
- [x] 4.4 Add `internal/wavehealth/store_integration_test.go` behind `//go:build integration`
      using `mongotest.Start`, proving against real validators and indexes that only in-window
      samples of the wave's own devices are counted, that a wave of another rollout is not found,
      and that a wave with an empty `device_ids` reports an empty window; verify `go test -tags
      integration ./internal/wavehealth/...` passes with Docker available

## 5. Wave health gRPC service

- [x] 5.1 Add the gRPC adapter in `internal/wavehealth` implementing
      `rolloutv1.RolloutServiceServer`: reject an empty `rollout_id` or `wave_id` with
      `INVALID_ARGUMENT` before evaluating, map the not-found sentinel to `NOT_FOUND`, map every
      other failure to `INTERNAL` with a message carrying no driver or collection detail, and
      convert the window and thresholds into the response; verify `go build ./...` succeeds
- [x] 5.2 Add table-driven service tests over fakes covering each mapped status code (empty ids,
      unknown rollout, wave of another rollout, a storage failure whose response message must not
      leak internals), a served empty window that reports undecided with sample size zero, and that
      no query runs for an invalid request; verify `go test -race ./internal/wavehealth/...` passes
- [x] 5.3 Register the service in `cmd/controlplane`: build the aggregator from the loaded
      `rollout` configuration and the fleet database, register `RolloutService` on the existing
      gRPC server, and log the served window and thresholds at startup; verify `go build ./...` and
      `go test ./cmd/controlplane/...` pass, and a local run answers `GetWaveHealth` for a
      hand-inserted wave

## 6. Definition of done

- [x] 6.1 Run the full bar from AGENTS.md — `goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...` — and confirm all four pass
- [x] 6.2 Run the integration suites affected by the schema change (`go test -tags integration
      ./internal/wavehealth/... ./internal/mongotest/...`) with Docker available and confirm they
      pass
- [x] 6.3 Validate the change artifacts with `openspec validate add-wave-health-aggregation
      --strict` and confirm every requirement in the delta specs is covered by a task or a test
