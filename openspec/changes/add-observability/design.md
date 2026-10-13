# Design

## Context

See [proposal.md](proposal.md) — Why for the motivating gap. What shapes the approach is the state
the instrumentation has to attach to:

- **The trace boundaries already exist as seams.** `internal/agentserver` builds its interceptor
  chain in one place (`ServerOptions`) and its package doc already names the missing links;
  `internal/agent/dial.go` creates the agent's connection in one place; `internal/temporal` owns
  every workflow and activity registration; `internal/telemetry` owns the broker session, the event
  envelope, and the existing Prometheus source. Nothing requires a new layer to hook into.
- **The configuration surface already exists and is validated.** `observability.otel_endpoint`,
  `metrics_addr`, and `health_addr` are declared, defaulted, and validated today; `otel_endpoint`
  is the only one with no consumer. This change fills a declared seam rather than adding a section.
- **One metrics source exists, with a shape worth generalizing.** `internal/telemetry.NewMetrics`
  registers into a caller-supplied `prometheus.Registerer` and the control plane serves a private
  registry at `/metrics` (`cmd/controlplane/main.go` → `metricsHandler`). The worker serves probes
  on `health_addr` but no metrics; the agent serves no HTTP surface at all.
- **`broker-observability` already owns the pipeline's metrics.** Queue depth, consumer lag, and
  event outcome counters exist and are specified. The pipeline dashboard reads them; nothing here
  re-derives them.
- **`add-missing-feature-coverage` is in flight and uncommitted**, rewriting `cmd/worker`,
  `cmd/controlplane`, and `cmd/agent` — the entrypoints this change also edits. It lands first.

## Goals / Non-Goals

**Goals:**

- One trace per rollout, spanning workflow → activity → gRPC → agent → broker → storage, and one
  trace per agent stream covering its whole lifecycle including reconnects.
- A Prometheus surface in all three binaries, in the existing `fleetops_` namespace, serving both
  request-level metrics and the rollout/fleet metrics the dashboards need.
- A local stack that makes the tracing and the dashboards genuinely runnable and checkable, without
  requiring the application stack to be up.

**Non-Goals:**

- The application stack (Temporal, MongoDB, RabbitMQ, k3d). Compose manifests for those remain
  stage-1 work; this change adds the observability trio only.
- Tracing the HTTP surfaces. `otelhttp` is already an indirect dependency and stays one: the rollout
  control surface and the probe endpoints are not in this change's scope.
- Any dashboard-only metric, any change to rollout, rollback, ingestion, or streaming semantics,
  and any correction of the stale stage table in `README.md`.
- Distributed trace *storage*. The stack forwards to a backend; running a production tracing
  backend is not part of a local demo.

## Decisions

### D1 — The workflow owns the trace, and only activities create spans

A workflow creates one span for its own execution and passes its trace context to every activity it
schedules, through the activity input. Activities start child spans from that context. The workflow
never creates a span for anything else, and no other component creates a *root* span for work a
workflow caused.

**Why:** Temporal's scheduler is not an RPC boundary the way gRPC is, so there is no transport to
carry context automatically. Passing it explicitly through the activity input is the only approach
that keeps the workflow's decisions inspectable while giving one rollout one trace — including a
rollout that spans a `ContinueAsNew`, because the successor inherits the context as carried state
and therefore continues the same trace rather than starting a second one.

**Alternative considered:** letting each activity start its own trace and correlating them by
`rollout_id` afterwards. Rejected: it produces N traces per rollout and makes "the full timeline" a
query-time join rather than a trace — exactly the manual correlation the proposal exists to remove.

**Determinism constraint:** spans are created in activities and in workflow code only through
deterministic operations. No span is created from wall-clock time, randomness, or I/O in workflow
code (AGENTS.md), and the replay test in the tasks exists to prove it.

### D2 — Propagate over the wire with the official instrumentation; propagate over the broker in headers

Server and client gRPC instrumentation comes from the OpenTelemetry gRPC interceptor package, added
to the existing chain in its documented order (recovery, logging, metrics, tracing). Broker
propagation injects the context into the AMQP message headers at publish and extracts it at
consume, using the same envelope that already carries schema and attempt headers.

**Why:** Trace propagation over gRPC metadata is exactly what the instrumentation package does, and
hand-rolling it would be a second implementation of a solved problem (AGENTS.md: don't introduce a
new pattern when one exists). Message headers are metadata, so putting trace context there leaves
the event payload — the part of the envelope with a schema contract — untouched.

**Alternative considered:** adding `trace_id` and `span_id` fields to the event payload. Rejected:
it changes a versioned contract to carry instrumentation data, and a consumer that does not
understand the fields is then handling a payload the schema describes differently.

### D3 — MongoDB is instrumented at the driver, not in the store methods

A driver command monitor wraps the `mongo.Client` each process builds, so every operation the
driver performs — reads, writes, aggregates, GridFS chunk traffic, index administration — becomes a
span parented to the caller's, with attributes for the operation and the collection.

**Why:** it is the canonical instrumentation for this driver, it covers operations this codebase
would not remember to wrap by hand, and it leaves every store type, interface, and call site
unchanged. The `trace_id`/`span_id` logging requirement then falls out of the active span for free
rather than needing a helper called at each store boundary.

**Alternative considered:** wrapping each store method with an explicit span. Rejected: it needs a
decorator per method across `devices`, `firmware`, `rollout`, `wavehealth`, `telemetry`, and the
GridFS paths, and the one method somebody forgets is invisible — the failure mode is silence.

**Cost accepted and recorded:** the monitor also traces GridFS chunk reads, which are numerous. The
sampler gates recording, and the tasks measure the span volume a firmware download produces so the
cost is a recorded number rather than an assumption.

### D4 — Metrics exposition is shared; each process registers into its own registry

A small shared package owns the metrics listener — listen address, `promhttp` handler, shutdown —
and each binary passes it the registry it built. `cmd/controlplane`'s existing inline registry and
handler move to it; the worker and the agent adopt it.

**Why:** the alternative is the same listener written three times with three chances to drift, and
the worker's surface does not exist yet, so there is nothing to preserve. A per-process registry
(rather than a package-level default) is what keeps `MustRegister` collisions loud and keeps the
existing "no package-level mutable vars" rule intact.

**Alternative considered:** a process-wide default registry that metrics packages register into at
init. Rejected: AGENTS.md forbids package-level mutable state, and a default registry hides the
duplicate-registration panics that a private registry surfaces.

### D5 — Metrics listen on their own address, separate from probes

Each binary that exposes metrics serves them on `observability.metrics_addr`, on a listener
distinct from the probe listener on `health_addr`.

**Why:** the two fields already exist and are already distinct; the control plane already runs them
as two servers, and the worker already runs its probe server. Merging them would mean either
ignoring a configured address or changing what an existing field means — a compatibility change
this change has no reason to make. The agent gains an HTTP listener it did not have, which is the
cost of giving it a metrics surface at all.

**Alternative considered:** mounting `/metrics` on the probe mux to avoid a second listener.
Rejected: `metrics_addr` would then be dead configuration in two binaries, repeating exactly the
problem `otel_endpoint` has today.

### D6 — Trace latency reaches Prometheus through a span-derived histogram

A span processor turns finished spans into Prometheus histogram observations labeled by operation
and span kind, registered in the same registry the process already serves.

**Why:** the pipeline dashboard's trace-latency panel has to read Prometheus — a dashboard that
needs a second datasource the stack does not provision is a panel that cannot render, which the
spec forbids. Deriving the histogram from finished spans keeps one source of truth (the spans) and
gives per-hop percentiles without a tracing-backend query.

**Alternative considered:** the OpenTelemetry SDK's own Prometheus exporter for span duration
metrics. Rejected: it serves its own exposition, so a process would expose two metric endpoints and
the dashboards would need to know which metric lives where. One registry per process is simpler to
reason about and to scrape.

**Cardinality bound:** the histogram is labeled by operation and kind only. Domain identifiers
(`device_id`, `rollout_id`, `firmware_id`) are attached to *spans*, where cardinality is bounded by
sampling, and never to this histogram, where it would be unbounded.

### D7 — Sampling defaults to parent-based with a 10% root ratio

`observability.otel_sample_ratio` defaults to `0.1`, applied only to newly rooted traces; a trace
that arrives already sampled is recorded regardless of the local ratio.

**Why:** it is the demo-scale default the request asks for — one rollout in ten is traced in full —
and parent-based propagation is what keeps that decision stable across six processes, so a sampled
rollout is never half-traced. A demo that needs every trace sets the ratio to `1.0` in one place.

**Alternatives considered:** always-on (every trace recorded; unbounded volume for a fleet sending
heartbeats continuously) and a rate-limiting sampler (needs shared state across processes, and a
rate limit re-decided per process is not a property anyone can reason about).

### D8 — The dashboards are provisioned from `dashboards/`, one file per audience

Two dashboard JSON files, each with a stable UID and title, provisioned by Grafana from a
file-based provider wired in `deploy/`, each referring to the datasource by its provisioned
reference.

**Why:** `dashboards/README.md` already declares this directory the home for dashboard JSON and
`deploy/` the place provisioning is wired; two files keep two audiences — a rollout operator and a
pipeline operator — from sharing one document whose panels answer different questions. Stable UIDs
mean a restart updates rather than duplicates.

### D9 — `runtime-config` is modified, and both blocks are supersets

The delta replaces `Single YAML configuration file` and `Configuration validation fails fast`
wholesale, each a superset of the base block, and leaves `Entrypoints consume the shared
configuration` untouched.

**Why:** the field table and the validation contract are normative, so a new field without a delta
there is a configuration surface the specifications do not describe. The replacement is
scenario-preserving by construction — every base scenario is carried over verbatim and the new ones
are added — because the composition rule in `openspec/config.yaml` is exactly about not dropping
scenarios during a whole-block rewrite. The third requirement is untouched because this change does
not alter which sections a binary consumes.

## Risks / Trade-offs

- **A 10% default can leave a demo with no traces to look at.** → The ratio is configuration, the
  stack ships it with the value documented, and `docs/observability.md` names `1.0` as the setting
  for a walkthrough. The risk is discoverability, not capability.
- **Span-derived histograms are cardinality-sensitive.** → Labels are operation and kind only;
  domain identifiers stay on spans. A task asserts the label set does not grow with entities.
- **Driver-level tracing makes GridFS chunk reads visible, which is noisy.** → Sampling bounds it,
  and a task measures the span count of a firmware download and records the number rather than
  asserting it is small.
- **A new direct dependency can move the module's minimum Go version.** → Versions are pinned in
  `go.mod` and `go mod tidy` is a task; if a dependency requires a newer toolchain than the module
  declares, the toolchain requirement is raised deliberately and recorded, not worked around.
- **Workflow tracing can break determinism.** → All span creation in workflow code goes through
  deterministic operations, and the tasks include the replay test that catches a violation.
- **Instrumentation could gate the work it observes.** → Every component is added so that failure
  paths log and continue; the specs carry explicit scenarios for a dead collector, a disabled
  exporter, and a failed metric registration, and the tasks run the existing suites with
  instrumentation off, on, and pointed at a dead endpoint.
- **The stack's three containers add local resource pressure.** → The stack is deliberately not part
  of the test suite, so no test pays for it, and it runs independently of the application stack.
- **Coordination with the in-flight change.** → `add-missing-feature-coverage` lands first; this
  change edits the same entrypoints and is rebased on top rather than merged with it.

## Migration Plan

1. `add-missing-feature-coverage` lands and is archived; this change starts from that tree.
2. Dependencies move from indirect to direct and the config field lands, before any instrumentation
   consumes them.
3. Tracing lands bottom-up: the shared exporter and sampling, then storage, then gRPC, then
   workflow and activity — each verifiable on its own, each leaving the previous suite green.
4. Metrics land in the order the dashboards need them: the shared exposition package and request
   metrics first, then the rollout and fleet metrics.
5. The stack and the dashboards land last, against metrics that already exist, so a panel with no
   source is caught while writing it rather than during a demo.
6. Rollback: every step is additive. Tracing is disabled by an empty `otel_endpoint` and the stack
   is not required by any binary, so reverting the deploy step alone returns the system to
   today's behavior with the instrumentation still working.

## Open Questions

- **The forwarding backend's default.** The stack forwards traces to a configurable backend, and
  `deploy/`'s compose file chooses a local default that needs no account. Which local backend is the
  most useful default is a configuration choice inside the stack; it is settled when the compose
  file is written and does not change the specs, the approach, or the task breakdown.
- **Whether the trace-latency histogram should be enabled when tracing is off.** With an empty
  `otel_endpoint` there are no spans to derive latency from, so the latency panels go quiet while
  request and activity histograms keep working. Whether that is the right coupling — or whether the
  latency panels should read the request histograms instead — is a dashboard detail that can be
  answered when the panel is built.
