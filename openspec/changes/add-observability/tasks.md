# Tasks

## 1. Dependencies and the sampling field

- [ ] 1.1 Promote the tracing and metrics dependencies from indirect to direct in `go.mod`: the OTel
  API and SDK, the OTLP trace exporter, the gRPC instrumentation package, and the MongoDB driver
  instrumentation package; pin each version and run `go mod tidy`; verify `go build ./...` succeeds
  and `git diff go.mod` shows the pins, with each dependency carrying its one-line justification in
  the PR description per AGENTS.md
- [ ] 1.2 Add `observability.otel_sample_ratio` to `internal/config/config.go` with the `0.1`
  default and a range check in `internal/config/validate.go` rejecting values below `0.0` or above
  `1.0` with the field named, and extend `internal/config` tests for the default, both valid bounds,
  and both rejection directions; verify `go test -race -count=1 ./internal/config/`
- [ ] 1.3 Document the new field in `deploy/config.yaml` next to `otel_endpoint`, stating what the
  ratio applies to and that the decision is parent-based; verify `go test -race -count=1 ./internal/config/`
  passes with the sample file loading as written, and that `make check` reports no vet or lint finding
- [ ] 1.4 Record the resolved dependency versions and the chosen sampling default in the change's
  `design.md` and in the metric catalogue stub of `docs/observability.md` (created here, filled in by
  group 11); verify `docs/observability.md` exists and its dependency section names versions matching
  `go.mod`

## 2. The tracing pipeline

- [ ] 2.1 Create the observability package holding the trace pipeline: a provider built from the
  configured endpoint and sample ratio, with a parent-based sampler, a batched OTLP exporter over
  gRPC, and a bounded shutdown flush; an empty endpoint returns a working provider that records
  nothing rather than an error; verify table-driven unit tests over a fake exporter cover a
  configured endpoint, an empty endpoint, an unreachable endpoint, and bounds rejection at the config
  layer, run with `go test -race -count=1 ./internal/observability/`
- [ ] 2.2 Add the span-derived histogram to that package: a span processor that observes each ended
  span's duration into a Prometheus histogram labeled by operation and span kind only, registered
  into a caller-supplied registry; verify unit tests assert the label set is exactly operation and
  kind, that a domain identifier attribute does not create a new series, and that a span with no
  operation name still records, run with `go test -race -count=1 ./internal/observability/`
- [ ] 2.3 Prove the export path is bounded and non-blocking: a stalled receiver does not grow the
  exporter's queue without limit and does not block the caller that ended the span, and a shutdown
  flushes what was produced within its bound; verify `go test -race -count=1 ./internal/observability/`
  with a receiver that accepts nothing passes the drop-and-shutdown tests
- [ ] 2.4 Write the package documentation for `internal/observability`: what it owns, the sampling
  decision and why it is parent-based, and the rule that no instrumentation failure reaches a caller;
  verify `go doc ./internal/observability` renders the package comment and `make check` passes

## 3. Storage tracing

- [ ] 3.1 Add the driver command monitor to the observability package: a helper that returns the
  client options applying it, so each binary instruments its MongoDB client at construction; verify
  a unit test asserts the monitor is applied to the returned options and that an uninstrumented
  client still builds
- [ ] 3.2 Wire the monitor into every `mongo.Connect` in `cmd/controlplane`, `cmd/worker`, and
  `cmd/agent`; verify `go build ./...` succeeds and a unit test per binary asserts the options it
  builds carry the monitor
- [ ] 3.3 Verify against a real driver that operations become spans: reads, writes, aggregates, and
  GridFS reads and writes each produce a span carrying the operation and the collection or bucket,
  a failing operation's span records the failure, and each span's parent is the caller's span rather
  than a new root; run `go test -tags integration -count=1 ./internal/mongotest/` for the new
  instrumentation suite and `make test-integration`
- [ ] 3.4 Measure what driver-level tracing costs on the GridFS path rather than assuming it is
  small: count the spans one firmware upload and one download produce, record the number in
  `design.md` under D3, and verify the recorded number matches the count the test prints
- [ ] 3.5 Document storage tracing in `docs/observability.md`: which operations are traced, the
  attributes they carry, and where the GridFS volume comes from; verify the documented attributes
  match those the integration test asserts

## 4. gRPC tracing

- [ ] 4.1 Add the tracing interceptor to the server chain in `internal/agentserver/interceptor.go`,
  keeping the documented order recovery → logging → metrics → tracing so recovery stays outermost;
  verify a unit test asserts the chain order and that a handler panic under tracing still becomes an
  internal status error
- [ ] 4.2 Add the client interceptors in `internal/agent/dial.go` so the emulator's outbound calls
  and its stream are traced, including a counter for stream loss and re-establishment; verify unit
  tests over a fake server assert a client span covers the stream, that a reconnect is recorded, and
  that a stream served with tracing disabled behaves identically, run with
  `go test -race -count=1 ./internal/agent/ ./internal/agentserver/`
- [ ] 4.3 Prove context crosses the boundary in both directions: an inbound heartbeat carrying trace
  context produces server spans in that trace with the agent's span as parent, an outbound command
  carries the deciding span's context, and a message with no context starts a new trace instead of
  failing; verify `go test -race -count=1 ./internal/agentserver/` and the stream join suite with
  `go test -tags integration -count=1 ./cmd/controlplane/`
- [ ] 4.4 Put `trace_id` and `span_id` into the stream logs the server and client write, absent
  rather than placeholder-filled when no span is active; verify a unit test captures the log output
  and asserts both fields for a traced stream and their absence outside one
- [ ] 4.5 Document the gRPC trace paths in `docs/observability.md`: the span hierarchy for a stream,
  what a reconnect looks like, and which metadata carries context; verify the documented hierarchy
  matches the spans the integration tests assert

## 5. Workflow and activity tracing

- [ ] 5.1 Create the workflow's own span at the start of `RolloutWorkflow` and carry its trace
  context in workflow state, including across `ContinueAsNew`, so a rollout that continues is still
  one trace; verify `TestWorkflowEnvironment` tests assert one trace root per rollout and that a
  continuing rollout's successor belongs to the same trace, run with
  `go test -race -count=1 ./internal/temporal/`
- [ ] 5.2 Pass the carried context into every activity input and start each activity's span as its
  child, attaching `rollout_id`, `wave_id`, `device_id`, and `firmware_id` where the activity
  concerns them; verify workflow tests assert the parent-child relationship and the identifier
  attributes for wave dispatch, health evaluation, per-device update, and the snapshot activity
- [ ] 5.3 Make the saga's compensations visible in the rollout's trace, each compensation
  identifiable by the step it compensates and the failing wave identifiable from the trace; verify
  the rollback scenario tests assert the compensating spans share the failing wave's trace
- [ ] 5.4 Prove tracing did not break determinism or change outcomes: replay every rollout
  workflow's completed history with tracing active and assert no determinism error, and run the
  rollback and promotion scenarios with tracing disabled asserting the same terminal status, waves,
  success rates, and rollback steps; verify `go test -race -count=2 ./internal/temporal/` and
  `go test -tags integration -count=1 ./cmd/worker/`
- [ ] 5.5 Document workflow tracing in `docs/observability.md`: the trace a rollout produces, how a
  continuing rollout stays one trace, and the determinism rule a future contributor must keep;
  verify the documented span structure matches what the workflow tests assert

## 6. Broker propagation

- [ ] 6.1 Inject trace context into the message headers at publish and extract it at consume in
  `internal/telemetry`, covering the heartbeat, rollout, and rollback-notification families without
  changing any event payload or schema version; verify unit tests assert the headers round-trip
  through the envelope and that a message with no trace headers is consumed normally
- [ ] 6.2 Verify propagation against a real broker: a consumed event's spans belong to the
  publishing trace, and an event published without trace headers starts a new trace rather than
  failing consumption; run `go test -tags integration -count=1 ./internal/telemetry/` and
  `make test-integration`
- [ ] 6.3 Document broker propagation in `docs/observability.md`: which headers carry context, that
  payloads and schema versions are untouched, and what an untraced event looks like; verify the
  documented header names match those the integration test asserts

## 7. Shared metrics exposition

- [ ] 7.1 Create the metrics exposition helper in the observability package: build a listener from
  a configured address, serve a caller-supplied registry in Prometheus exposition format, and shut
  down within the process's shutdown bound; verify table-driven tests cover serving a registry,
  honoring a non-default address, and a failed bind reported at startup rather than swallowed, run
  with `go test -race -count=1 ./internal/observability/`
- [ ] 7.2 Move the control plane's inline registry and handler out of `cmd/controlplane/main.go`
  into the shared helper without changing any existing metric name; verify its existing metrics tests
  pass unchanged with `go test -race -count=1 ./cmd/controlplane/`, and a scrape of a started control
  plane still returns every metric it returned before
- [ ] 7.3 Register the gRPC server instrumentation in the control plane's registry so request
  counts, durations, and open connections are exposed, with outcomes from bounded status labels
  rather than raw error text; verify unit tests assert the presence of each family and that a
  distinct error message does not add a series, and `go test -race -count=1 ./cmd/controlplane/`
- [ ] 7.4 Assert the new families are registered before the endpoint serves: a scrape immediately
  after startup returns them; verify an integration test that starts the binary and scrapes it
  without a retry loop passes with `go test -tags integration -count=1 ./cmd/controlplane/`

## 8. The worker's and agent's metrics surfaces

- [ ] 8.1 Serve metrics from `cmd/worker` on `observability.metrics_addr` using the shared helper,
  registered before its probe listener starts, and stop it within the worker's shutdown bound;
  verify a unit test asserts the metrics address is honored and that a non-default address serves
  exposition while nothing listens on the default, run with `go test -race -count=1 ./cmd/worker/`
- [ ] 8.2 Expose activity execution metrics from the worker — a counter by activity type and
  outcome and a duration histogram by activity type — fed where activities are registered so a
  failed execution is counted as a failure; verify `go test -race -count=1 ./cmd/worker/` and the
  integration suite `go test -tags integration -count=1 ./cmd/worker/`
- [ ] 8.3 Add the agent's metrics surface: a listener on `observability.metrics_addr` in
  `cmd/agent` with the agent's gRPC client metrics, open-connection gauge, and reconnect counter
  registered before it serves; verify `go test -race -count=1 ./cmd/agent/` covers the wire-up and
  that the binary starts and serves exposition
- [ ] 8.4 Assert across all three binaries that every metric added is in the `fleetops_` namespace,
  that no name collides with the existing pipeline metrics, and that the pipeline names and meanings
  are unchanged; verify a test comparing the exposition against the recorded name set passes with
  `go test -race -count=1 ./internal/observability/ ./internal/telemetry/`

## 9. Rollout and fleet metrics

- [ ] 9.1 Expose rollouts by status together with per-rollout progress through the wave sequence,
  derived from the recorded rollout and wave state rather than an independent counter, and
  re-established on process start so a restart does not reset progress; verify a unit test asserts
  the count per status and that progress advances with a concluded wave, and an integration test
  asserts a restarted worker's exposition still reflects an in-flight rollout, run with
  `go test -tags integration -count=1 ./internal/rollout/`
- [ ] 9.2 Expose wave health: the measured success ratio, the sample size behind it, and the wave's
  status, with an undecided wave distinguishable from a failing ratio, and with a wave's series
  withdrawn once its rollout reaches a terminal status; verify unit tests cover a measured wave, an
  undecided wave, and a concluded rollout, run with `go test -race -count=1 ./internal/wavehealth/`
- [ ] 9.3 Expose the device online ratio from the liveness state the sweep already maintains,
  defined when no device is known, and expose heartbeat throughput where heartbeats are accepted
  rather than where they are published, so a broker failure does not read as a silent fleet; verify
  `go test -race -count=1 ./internal/devices/ ./internal/telemetry/` covers the ratio at both
  liveness states, the empty case, and throughput rising with accepted heartbeats while publication
  fails
- [ ] 9.4 Expose rollback metrics: a counter by the outcome that caused the rollback, and per
  rollback the completed and planned compensation steps together with the count of devices left
  unrestored; verify tests assert each cause increments only its own counter and that a partially
  compensated rollback reports both step counts, run with `go test -race -count=1 ./internal/rollout/`
- [ ] 9.5 Assert no metric in this group fabricates a value when its source cannot be read: a failed
  read leaves the series absent or unchanged and logs, rather than publishing a zero; verify the
  tests exercise a failing source and assert no fabricated value reaches the registry, run with
  `go test -race -count=1 ./internal/rollout/ ./internal/wavehealth/ ./internal/devices/`
- [ ] 9.6 Assert the rollout path never gates on metrics: a rollout completes to the same terminal
  status and records the same waves and rollback steps with metric publication failing throughout;
  verify `go test -tags integration -count=1 ./cmd/worker/` with publication failures injected

## 10. The observability stack

- [ ] 10.1 Add the compose file under `deploy/` starting the OpenTelemetry collector, Prometheus,
  and Grafana, with every service's configuration mounted from a tracked file, and add
  `observability-up` and `observability-down` targets to the `Makefile` documented with the others;
  verify `make help` lists both targets and `make observability-up` starts all three services
- [ ] 10.2 Add the collector configuration: an OTLP receiver on the port the binaries are configured
  with, a configurable forwarding exporter whose default needs no external account, and a health
  endpoint; verify `make observability-up` followed by a query of the collector's health endpoint
  reports its status, and that adding an unreachable first exporter and restarting leaves the
  receiver answering
- [ ] 10.3 Add the Prometheus scrape configuration covering the control plane, the worker, and the
  agent on their configured metrics addresses; verify with the three binaries running that
  Prometheus reports all three targets up, and with one stopped that its target reports down rather
  than holding a stale value
- [ ] 10.4 Add Grafana's file-based provisioning for the Prometheus datasource and the dashboards
  directory, and confirm the stack runs with no application services up while Prometheus reports the
  application targets down; verify `make observability-up` with nothing else running starts all
  three services, and `make observability-down` stops and removes them
- [ ] 10.5 Record the stack in `deploy/README.md` — the up and down targets, each service's port, the
  backend setting, and the explicit note that this is not the application stack — and remove the
  "not implemented yet" claim it makes about the observability services; verify every command
  documented in `deploy/README.md` runs as written

## 11. The dashboards

- [ ] 11.1 Build the rollout dashboard in `dashboards/`: wave progress across active rollouts with a
  stalled rollout distinguishable, health trends with the sample size beside the ratio and the
  decision boundary marked, and rollback events including a concluded rollback that outlives its
  rollout's active period; verify each panel's query returns data against the provisioned stack with
  a rollout run, and a panel whose work has not happened reports no data rather than zero
- [ ] 11.2 Build the pipeline dashboard in `dashboards/`: heartbeat throughput as a rate with
  acceptance distinguishable from publication, consumer lag per consumer, dead-letter depth per
  dead-letter queue distinguished from ordinary backlog, and trace latency by hop from the
  span-derived histograms; verify each panel resolves reading only the provisioned Prometheus
  datasource, with no panel requiring a tracing-backend datasource
- [ ] 11.3 Give both dashboards stable titles and identifiers and confirm provisioning is
  file-driven: verify a clean start lists both dashboards, a restart after editing a file serves the
  edited version, and neither dashboard overwrites the other
- [ ] 11.4 Confirm the no-dashboard-only-metric rule holds mechanically: compare every metric name
  referenced by the two dashboards against the names the binaries expose, and record the comparison
  outcome; verify a check that extracts the dashboard queries and matches them against a scrape of
  the running stack reports no reference to a metric nothing publishes
- [ ] 11.5 Rewrite `dashboards/README.md` to describe the two dashboards, the panels each carries,
  the metrics each panel reads, and how provisioning delivers them; verify each metric named in the
  rewritten README appears in a scrape of the running stack

## 12. Integration, documentation, and the record

- [ ] 12.1 Write `docs/observability.md` as the stage-4 document the backlog specifies: the trace
  paths through the system, the sampling decision, the full metric catalogue with each metric's
  source, and how to run the stack and read the dashboards; verify every metric named in the
  catalogue appears in a running binary's exposition and every command it documents runs as written
- [ ] 12.2 Run the full gates on the finished change and fix every finding without disabling a
  linter or relaxing a test: verify `make check` reports zero issues and a passing race-enabled
  suite, `make test-integration` passes, and `go vet -tags integration ./...` is clean
- [ ] 12.3 Prove instrumentation does not change behavior by running the existing suites three ways
  — tracing disabled, tracing pointed at a reachable collector, and tracing pointed at a dead
  endpoint — and record that the observable outcomes match; verify the three runs of
  `go test -tags integration -count=1 ./cmd/worker/ ./cmd/controlplane/` reach the same results and
  record the comparison
- [ ] 12.4 Record the outcome in this file: the resolved dependency versions, the measured GridFS
  span volume, the metric count added per binary, and any divergence between what the specs require
  and what was built, stated rather than smoothed; verify each recorded number matches the working
  tree on the day it is written rather than being carried over from an earlier run
- [ ] 12.5 Update the delivery stage table in `README.md` only if the stage's scope is now fully
  delivered, and otherwise leave it stating what remains; verify the table's claims match the
  repository — the observability services exist and the application stack manifests do not — and
  record the decision either way
