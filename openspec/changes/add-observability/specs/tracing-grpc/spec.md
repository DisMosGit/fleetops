# Spec Delta

## Purpose

Makes a gRPC stream's lifecycle legible as one trace: the server side that accepts and serves an
agent's stream, and the client side that dials, reconnects, and reports over it, share a trace
context rather than each hop starting a new one.

## ADDED Requirements

### Requirement: Server spans cover every stream

The control plane SHALL create a span for every inbound gRPC stream it serves, covering the whole
life of the stream rather than a single message: the span starts when the stream is accepted and
ends when the stream terminates, whether by the agent closing it, by the server closing it, or by
an error. The span SHALL carry the full method name and the outcome, so a stream that ended in a
safe status error is distinguishable from one that ended cleanly.

#### Scenario: An accepted stream is one span
- **WHEN** an agent opens `AgentService.Connect` and the stream stays open across many heartbeats and commands
- **THEN** exactly one server span covers that stream, with the method name recorded

#### Scenario: A stream that fails is marked as failed
- **WHEN** a stream terminates because its handler returned a status error
- **THEN** the stream's span records that status rather than ending as a success

#### Scenario: Concurrent streams are separate spans
- **WHEN** two agents hold streams open at the same time
- **THEN** each stream has its own span, and neither is reported as a child of the other

### Requirement: Client spans cover the agent's outbound calls

The agent emulator SHALL create a span for every outbound gRPC call it makes and for the stream it
holds open, so a command received, a heartbeat sent, or a reconnect attempted is visible from the
agent's side of the same trace as the server that served it.

#### Scenario: The agent's stream is traced
- **WHEN** the agent emulator establishes its stream to the control plane
- **THEN** a client span covers the stream's lifetime on the agent side

#### Scenario: Stream loss is visible as a continuation
- **WHEN** the agent's stream is lost and the agent reconnects
- **THEN** the reconnect is traced, and the span for the new stream is attributable to the same agent rather than appearing as an unrelated first connection

### Requirement: Trace context crosses the gRPC boundary

Trace context SHALL be propagated in gRPC metadata in both directions. A span the server creates
for work that an inbound message caused SHALL be parented to the trace that message carries, and a
command the server writes outbound SHALL carry the context of the span that decided it, so the
dispatch decision and the agent's application of the command belong to one trace.

#### Scenario: An inbound heartbeat joins the caller's trace
- **WHEN** an agent sends a heartbeat carrying trace context and the control plane ingests it
- **THEN** the spans the control plane creates for that heartbeat name the agent's trace as their trace, and the agent's span as their parent

#### Scenario: An outbound command carries the deciding trace
- **WHEN** the control plane writes a firmware update command to an agent's stream
- **THEN** the command's metadata carries the context of the span that decided to dispatch it

#### Scenario: A message without context still produces a trace
- **WHEN** a message arrives carrying no trace context
- **THEN** the server starts a new trace for it rather than failing the call or emitting a span without identifiers

### Requirement: Trace identifiers reach the logs

The server and client SHALL include `trace_id` and `span_id` in the structured logs they write
while handling a stream, so a log line can be tied to its trace without searching by timestamp.

#### Scenario: Stream logs carry trace identifiers
- **WHEN** the server logs an event while serving a traced stream
- **THEN** that log line carries the stream's `trace_id` and `span_id`

#### Scenario: Logging without an active span stays valid
- **WHEN** the server logs an event outside any span
- **THEN** the line is still written, with the trace fields absent rather than filled with placeholder values

### Requirement: The interceptor chain keeps its documented order

The server's interceptor chain SHALL remain recovery, logging, metrics, tracing in that order, as
the package documentation already declares. Recovery SHALL remain outermost, so a panic in any
later interceptor is still converted into a safe status error rather than terminating the process.

#### Scenario: The chain is ordered as documented
- **WHEN** the server's options are built
- **THEN** the stream interceptors are chained recovery first and tracing last

#### Scenario: A panic in a traced stream still recovers
- **WHEN** a handler panics while tracing is active
- **THEN** the panic becomes an internal status error for the client, is logged, and the process keeps serving other streams

### Requirement: Instrumentation never gates the stream

Tracing and metrics SHALL NOT be able to fail a stream. If no exporter is configured, if the
collector is unreachable, or if a span cannot be recorded, the stream SHALL be served exactly as it
would be without instrumentation.

#### Scenario: A stream is served with tracing disabled
- **WHEN** an agent connects while no tracing endpoint is configured
- **THEN** the stream is served normally and heartbeats are ingested

#### Scenario: An unreachable collector does not fail a stream
- **WHEN** the configured collector cannot be reached and an agent connects
- **THEN** the stream is served normally, and the export failure is logged rather than returned to the agent
