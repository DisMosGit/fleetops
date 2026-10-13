# Proposal

## Why

FleetOps runs work that is defined by its *transit*, not its endpoints: a rollout is a workflow that
signals a device workflow, which writes a command to a long-lived gRPC stream, which an emulated
agent applies, which reports a heartbeat that fans out through RabbitMQ into MongoDB, which a health
window then reads to decide whether the next wave may start. Every one of those hops already exists
and is tested. None of them is connected to the next in an operator's view.

Three consequences are measurable in the repository today.

**First, the trace does not exist, and the seam that would carry it is dead config.**
`observability.otel_endpoint` is declared with a default of `localhost:4317`
(`internal/config/config.go`), documented in `deploy/config.yaml` as "OTLP collector endpoint; empty
disables trace export" — and read by nothing. `go.mod` carries `go.opentelemetry.io/otel` v1.44 and
`otelhttp` as **indirect** dependencies with zero importing files. The gRPC interceptor chain is
`grpc.ChainStreamInterceptor(Recovery(log), Logging(log))` with a comment naming what is missing:
"metrics and tracing join at their stage" (`internal/agentserver/interceptor.go`). Nothing on the
client side exists at all — `internal/agent/dial.go` calls `grpc.NewClient` bare. A rollout's
timeline is therefore assembled, when it is assembled at all, by correlating log lines by hand
across three processes, and the `trace_id`/`span_id` fields that AGENTS.md requires in structured
logs have no producer.

**Second, two of the three binaries have no metrics surface.** The control plane serves `/metrics`
from a private registry holding exactly two metric families — `fleetops_queue_depth` and the
pipeline counters plus `fleetops_device_offline_transitions_total` — while `cmd/worker` and
`cmd/agent` serve no metrics endpoint at all. `observability.metrics_addr` is validated for every
binary, and `deploy/README.md` describes `health_addr` as "the liveness/readiness listen address of
the control plane and the worker"; the field exists, the worker's Prometheus surface does not. An
operator running a canary has no way to ask how many waves are in flight, what a wave's measured
health is, how much of the fleet is online, or whether a rollback is running — the numbers are in
MongoDB and in Temporal, and reaching them means querying those systems directly.

**Third, the dashboards the project promises have nowhere to run.** `dashboards/README.md` states
the requirement plainly — "Every panel must read a Prometheus metric that has a real source — no
dashboard-only metrics" — and then records that nothing is implemented. `deploy/` ships the MongoDB
bootstrap and a configuration sample but no compose or k3d manifests, so there is no Prometheus to
scrape, no Grafana to render, no collector for traces to reach, and no backend for them to land in.
The gap has already cost the project verification: the deferred tasks in
`add-device-search-attributes-snapshot-and-worker` ("Confirm the worker's lifecycle and replica
story", "Smoke the whole path on the local stack") are blocked on exactly this, and four spec
scenarios carry no automated test for want of a running stack.

This is stage 4 of the delivery plan in `README.md` — "OTel instrumentation, Prometheus metrics,
Grafana dashboard, Temporal UI in the stack". The instrumentation it names is the subject of this
change. The stage table's "Not started" status is itself stale, but correcting the table is not this
change's work.

## What Changes

- **A rollout becomes one trace, from the workflow to the agent and back.** One `trace_id`
  originates at the rollout workflow and is carried across every hop: into activities, through the
  dispatch activity into the control plane's gRPC write, through gRPC metadata onto the agent's
  stream, into the agent's apply and report, and onward through the RabbitMQ event headers into
  ingestion and the health read. A stream's lifecycle — connect, heartbeat inbound, command
  outbound, reconnect after a drop — is a child span tree of the trace that caused it, not a
  separate trace per RPC.
- **Workflow and activity execution is traced with the workflow owning the trace context.** The
  workflow's span is the root the activities hang from, so the Temporal UI's execution and the
  trace of the same rollout name the same work, and a retried activity is visibly a retry of one
  step rather than an unrelated execution.
- **MongoDB operations are traced, so a slow query is visible inside its caller's trace.** Every
  database round trip the pipeline and the workflows make appears as a child span carrying the
  collection and operation, with the caller's span as parent — the slow read that delayed a health
  window is legible without a profiler.
- **Traces are exported to the collector the configuration already names, with demo-scale
  sampling.** `observability.otel_endpoint` stops being dead config: the binary exports OTLP to it,
  an empty value keeps tracing off without failing startup, and the sampling decision is a
  configured ratio with a documented default suited to a demo — parent-based, so a sampled rollout
  stays sampled across every hop rather than being re-rolled per process. The ratio is a new field
  in the existing `observability` section, which is why `runtime-config` takes a delta.
- **Request metrics exist in all three binaries, not one.** Request rate, latency, error rate, and
  connection counts are exposed for the control plane's gRPC server, the agent's gRPC client, and
  the worker — which gains the metrics listener it is configured for but does not serve — in the
  `fleetops_` namespace alongside the pipeline metrics that already exist, registered before the
  endpoint serves so no scrape misses them.
- **Rollout and fleet metrics exist, sourced from state that already exists.** Rollout progress,
  wave health, the device online ratio, heartbeat throughput, and rollback counts are exposed with
  real producers, so the dashboards have something to read.
- **An observability stack the dashboards can actually run on.** `deploy/` gains compose manifests
  for an OTel collector, Prometheus, and Grafana, with the collector forwarding traces to a
  configured backend, Prometheus scraping all three binaries, and Grafana provisioning its
  datasource and dashboards from files in the repository. This deliberately does **not** bring up
  the application stack — Temporal, MongoDB, RabbitMQ, and k3d remain stage-1 work; the trio runs
  so the instrumentation is verifiable.
- **Two Grafana dashboards, one per audience.** A rollout dashboard (wave progress, health trends,
  rollback events across active rollouts) and a pipeline dashboard (heartbeat throughput, consumer
  lag, dead-letter depth, trace latency). Every panel reads a metric this change or the existing
  pipeline instrumentation produces; trace latency is served by a span-derived Prometheus histogram
  precisely because traces themselves live in the backend, not in Prometheus.
- **The stale stage table is left alone.** `README.md`'s delivery-status table still reports stages
  1–5 as "Not started" while four stages are implemented. That claim is wrong today and this change
  does not fix it; it is named here so the omission is a recorded decision rather than an oversight.

## Capabilities

### New Capabilities

- `tracing-grpc`: a stream's lifecycle and the synchronous calls beside it appear as one trace
  across the server and client gRPC paths, with context propagated through gRPC metadata,
  reconnect treated as a continuation, and trace identifiers reaching the logs.
- `tracing-workflow`: workflow and activity execution is traced with workflow-owned context, so a
  rollout's full timeline — waves, decisions, retries, compensations — is one trace.
- `tracing-storage`: database operations are traced as child spans of their caller, carrying the
  operation and collection, so a slow query is attributable to the caller it delayed.
- `observability-export`: OTLP export to the configured collector with a configured, parent-based
  sampling ratio; an unreachable or unconfigured collector degrades rather than fails.
- `observability-stack`: compose manifests for the OTel collector, Prometheus, and Grafana, with
  the collector's forwarding backend, the scrape configuration, and Grafana's file-provisioned
  datasource and dashboards, plus the `make` targets that drive them.
- `request-metrics`: request rate, latency, error rate, and connection-count metrics on the control
  plane's gRPC server, the worker, and the agent's gRPC client, served in the `fleetops_` namespace
  on the configured metrics address.
- `rollout-metrics`: rollout progress, wave health, device online ratio, heartbeat throughput, and
  rollback-count metrics with real producers in the workflow, the health gate, the liveness sweep,
  and the ingestion path.
- `rollout-dashboard`: the Grafana dashboard for wave progress, health trends, and rollback events
  across active rollouts, provisioned from the repository.
- `pipeline-dashboard`: the Grafana dashboard for heartbeat throughput, consumer lag, dead-letter
  depth, and trace latency, provisioned from the repository.

### Modified Capabilities

- `runtime-config`: two of its three requirements change, because the sampling ratio is a new
  field and that capability's field table and validation contract are normative — a new field
  without a delta there would be a configuration surface the specifications do not describe. The
  `observability` section itself is unchanged in shape: the field is additive to a section that
  already carries `otel_endpoint`, `metrics_addr`, and `health_addr`, and the third requirement
  (`Entrypoints consume the shared configuration`) is left alone because this change does not alter
  which sections a binary consumes. Both modified requirements are replaced as supersets of the
  blocks they replace, per the delta-composition rule in `openspec/config.yaml`.

`broker-observability` is deliberately **not** modified. It already owns queue depth, consumer lag,
and event outcome counters; the pipeline dashboard reads those metrics, and this change adds none of
them twice.

## Impact

- **Dependencies**: `go.opentelemetry.io/otel`, `.../otel/sdk`, `.../otel/exporters/otlp/otlptrace/otlptracegrpc`,
  `.../otel/sdk/metric` (or the Prometheus exporter), and `.../contrib/instrumentation/google.golang.org/grpc/otelgrpc`
  move from indirect to direct. Each needs its one-line justification in the PR, per AGENTS.md.
  `otelhttp` is already present as an indirect dependency and may remain one: this change traces
  gRPC, workflows, activities, and Mongo, not the HTTP surface.
- **Code**: `internal/agentserver` (interceptor chain gains metrics and tracing in the documented
  order), `internal/agent` (client interceptors, apply and report spans), `internal/temporal`
  (workflow and activity spans, trace propagation through signals and activities),
  `internal/telemetry` (message-header propagation; metrics registration shared with the existing
  pipeline source), `internal/rollout`, `internal/wavehealth`, `internal/devices`, and a new
  package for the exporter and the shared registry wiring; `cmd/controlplane`, `cmd/worker`, and
  `cmd/agent` for startup, shutdown, and the new listeners.
- **Config**: additive — a sampling ratio in the existing `observability` section. No existing field
  changes meaning; `otel_endpoint` acquires the behavior its comment already claims.
- **Deploy**: new compose manifests, collector/Prometheus/Grafana configuration, and Grafana
  provisioning under `deploy/`; `make up` / `make down` retargeted or joined by observability-stack
  targets. Dashboard JSON lands in `dashboards/`, which is where `dashboards/README.md` says it
  belongs.
- **Docs**: `docs/observability.md` is the document the backlog already specifies for this stage
  ("OTel trace paths, Prometheus metrics catalogue, Grafana dashboards"); it is written as part of
  this change, and the metric catalogue it carries is the contract the dashboards read.
  `dashboards/README.md` and `deploy/README.md` stop saying "not implemented yet".
- **Sequencing**: `add-missing-feature-coverage` is in flight and uncommitted, and it rewrites
  `cmd/worker`, `cmd/controlplane`, and `cmd/agent` — the same entrypoints this change instruments.
  It lands first; this change builds on it. Recorded as a dependency in `design.md`, not assumed.
- **Behavior**: no change to rollout, rollback, ingestion, or streaming semantics. Instrumentation
  observes the paths it is added to and does not gate them: a collector that is down, a metric that
  cannot be registered, or a span that cannot be exported never fails a heartbeat, a wave, or a
  rollout.
- **Not in scope**: the application stack (compose/k3d for Temporal, MongoDB, RabbitMQ), the stale
  stage table in `README.md`, the Temporal UI in the stack, and the two deferred verification tasks
  in `add-device-search-attributes-snapshot-and-worker` — those need the full stack, which this
  change does not build.
