# Spec Delta

## Purpose

Defines how traces leave the process: the configured collector endpoint they are exported to, the
sampling decision that keeps a demo's trace volume affordable without losing a rollout's trace
halfway through, and the degraded behaviour that keeps instrumentation from ever failing the work
it observes.

## ADDED Requirements

### Requirement: Traces are exported to the configured collector

Each binary SHALL export the spans it creates to the OTLP endpoint configured by
`observability.otel_endpoint`. An empty value SHALL disable trace export without failing startup —
the behavior the configuration already documents — and a non-empty value that cannot be reached
SHALL NOT prevent the process from starting or serving.

#### Scenario: A configured endpoint receives spans
- **WHEN** a binary starts with `observability.otel_endpoint` naming a reachable collector
- **THEN** the spans it creates arrive at that collector

#### Scenario: An empty endpoint disables export
- **WHEN** a binary starts with `observability.otel_endpoint` empty
- **THEN** it starts and serves normally, and no export is attempted

#### Scenario: An unreachable collector does not stop startup
- **WHEN** a binary starts with an endpoint no collector is listening on
- **THEN** the process starts, serves its surface, and reports the export failure in its logs

#### Scenario: Invalid endpoint configuration fails loudly
- **WHEN** `observability.otel_endpoint` is not a valid `host:port` pair
- **THEN** startup fails naming the offending field, as the configuration contract already requires

### Requirement: Sampling is configured and parent-based

The share of traces recorded SHALL be configured by `observability.otel_sample_ratio`, ranging from
`0.0` (record nothing) to `1.0` (record everything), with a default that keeps a demo's trace volume
affordable without dropping a rollout's trace mid-flight. The decision SHALL be **parent-based**: a
process that receives trace context already sampled SHALL record its spans regardless of its own
ratio, so one rollout's trace is complete across every process and hop it passes through rather
than being re-decided at each boundary.

#### Scenario: A sampled rollout stays sampled across processes
- **WHEN** a rollout's trace is sampled at the workflow and a later process in the chain has a lower ratio
- **THEN** that process records its spans for the same trace, and the rollout's trace is complete

#### Scenario: An unsampled trace stays unsampled downstream
- **WHEN** a trace's sampling decision was to drop it, and a downstream process has a ratio of `1.0`
- **THEN** the downstream process does not resurrect the trace

#### Scenario: The ratio bounds what is recorded
- **WHEN** a process with a ratio below `1.0` starts many traces of its own
- **THEN** the share it records is bounded by its configured ratio

#### Scenario: An out-of-range ratio is rejected
- **WHEN** `observability.otel_sample_ratio` is below `0.0` or above `1.0`
- **THEN** startup fails and the error names the offending field

### Requirement: Export is bounded and flushed on shutdown

Export SHALL be bounded so a slow or unreachable collector cannot grow memory without limit or
block the work being traced, and a shutdown SHALL flush spans already produced within a bounded
time before the process exits.

#### Scenario: A stalled collector does not grow memory without bound
- **WHEN** the collector stops responding while the process keeps producing spans
- **THEN** the process keeps serving, and unexported spans are dropped rather than accumulated without limit

#### Scenario: Shutdown flushes what was produced
- **WHEN** a process receives a shutdown signal after producing spans
- **THEN** it attempts to flush them within a bounded time and exits without hanging

#### Scenario: Export never blocks the traced path
- **WHEN** the collector is slow to accept spans while heartbeats are being ingested
- **THEN** heartbeat ingestion latency is not determined by export latency

### Requirement: Trace context is carried in message headers

Traces SHALL propagate across the message broker in the headers of the events published to it, so
the pipeline a heartbeat enters is part of the same trace as the device report that produced it.

#### Scenario: A published event carries its trace
- **WHEN** the control plane publishes a heartbeat event to RabbitMQ
- **THEN** the event's headers carry the trace context of the work that produced it

#### Scenario: A consumed event joins its producer's trace
- **WHEN** a consumer processes an event carrying trace context
- **THEN** the spans it creates are part of that trace rather than a new one

#### Scenario: An event without headers still produces a trace
- **WHEN** a consumer processes an event whose headers carry no trace context
- **THEN** it starts a new trace and processes the event normally

### Requirement: Instrumentation never becomes a failure mode

No instrumentation path SHALL fail the work it observes. A metric that cannot be registered, a span
that cannot be exported, a collector that is unreachable, or tracing left unconfigured SHALL all
leave the observed behavior — heartbeats accepted, waves decided, commands dispatched, rollouts
concluded — unchanged. Where instrumentation cannot do its job it SHALL log once rather than
reporting an error to a caller.

#### Scenario: Instrumentation failures are not returned to callers
- **WHEN** any instrumentation component fails while a caller's work proceeds
- **THEN** the caller receives its own result, not an instrumentation error

#### Scenario: Behavior is identical with instrumentation off and broken
- **WHEN** the same work runs with tracing configured, unconfigured, and pointed at a dead endpoint
- **THEN** the observable outcome is the same in all three cases
