# Tasks

## 1. Configuration core (`internal/config`)

- [x] 1.1 Define the typed `Config` struct (sections `simulation`, `grpc`, `mongodb`, `rabbitmq`,
      `temporal`, `observability`), a fully-populated defaults constructor matching the
      `specs/runtime-config/spec.md` defaults table, and strict YAML decoding with
      `go.yaml.in/yaml/v3` (`KnownFields(true)`); add the dependency and verify `go mod tidy`
      plus table-driven `Load` tests pass for: absent fields take defaults, present fields
      override, `-config` path missing/unreadable errors with the path named, unknown key and
      malformed YAML are rejected
- [x] 1.2 Implement the validation pass over the decoded struct (positive `fleet_size`,
      `host:port` addresses with numeric ports, `mongodb://`/`mongodb+srv://` and
      `amqp://`/`amqps://` schemes, non-empty database/namespace/task_queue, empty
      `otel_endpoint` allowed as "tracing disabled") returning all violations joined with the
      offending field named per error; verify with table-driven tests covering every rule and a
      valid partial file
- [x] 1.3 Add `deploy/config.yaml` — a commented sample documenting every field and its default —
      and verify a test loads it without validation errors

## 2. Health/readiness surface (`internal/health`)

- [x] 2.1 Define the `Check` interface (`Name()`, `Check(ctx) error`) and the dial-based checks
      for `mongodb`, `rabbitmq`, and `temporal` (bounded `net.DialTimeout`, errors wrapped with
      the dependency name and stripped of credentials); verify with tests over ephemeral
      `net.Listen(":0")` endpoints covering reachable, refused, and timed-out cases
- [x] 2.2 Implement the HTTP handler serving `GET /healthz` (always `200`, `{"status":"ok"}`) and
      `GET /readyz` (all checks concurrent with per-check timeout, `200` only when all pass
      else `503`, JSON body with per-dependency `ok`/`error` and safe summaries) and verify with
      `httptest` cases: all-green, one-down, all-down bounded latency, recovery without restart,
      and no credentials in error bodies (fake `Check` implementations)

## 3. Entrypoint wiring

- [x] 3.1 Wire `cmd/controlplane`: `-config` flag (omitted → defaults), full-file validation
      before any listener, health server on `observability.health_addr` with signal-driven
      graceful shutdown (`errgroup` ownership); keep the stage-gated gRPC/Temporal TODOs in
      place and verify by building the binary, running it with a test config whose
      `health_addr` is an ephemeral port, and curling `/healthz` and `/readyz`
- [x] 3.2 Wire `cmd/worker` the same way and verify identically (probe answers on the configured
      `health_addr`, invalid config exits non-zero naming the field before any listener starts)
- [x] 3.3 Wire `cmd/agent`: `-config` flag replaces the `-control-plane`/`-fleet-size` flags, the
      loaded `simulation.fleet_size` and `grpc.control_plane_addr` are passed to the emulator
      startup path; verify with a config file whose values differ from the defaults and an
      assertion at the `run` seam (or a startup log line) that the configured values, not
      constants, are used

## 4. Documentation

- [x] 4.1 Update `cmd/README.md` and `deploy/README.md` for the `-config` flag, the sample file,
      and the `/healthz` + `/readyz` endpoints on `health_addr`; verify the documented commands
      run as written against the sample `deploy/config.yaml`

## 5. Integration checks

- [x] 5.1 Run the Definition of Done (`goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...`) and verify all four pass
- [x] 5.2 End-to-end smoke: start `controlplane` and `worker` side by side with a config pointing
      at unreachable dependencies, verify `/healthz` is `200` on both and `/readyz` is `503`
      with per-dependency errors, then start a TCP listener standing in for one dependency and
      verify that check flips to `ok` without a restart
