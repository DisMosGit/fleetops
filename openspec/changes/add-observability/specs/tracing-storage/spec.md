# Spec Delta

## Purpose

Makes every database round trip a child of the work that made it, so a slow query is attributable
to the caller it delayed instead of being invisible until someone profiles the process.

## ADDED Requirements

### Requirement: Every database operation is traced

Each MongoDB operation a process performs — reads, writes, aggregates, and index or collection
administration — SHALL be traced as a span carrying the operation name and the collection or
GridFS bucket it acted on, and its duration. The span SHALL end when the driver call returns,
whether it succeeded or failed.

#### Scenario: A read is traced with its collection
- **WHEN** the health gate reads heartbeat samples from the `telemetry` collection
- **THEN** a span records the read operation against that collection

#### Scenario: A write is traced with its collection
- **WHEN** a device workflow's snapshot activity upserts a `device_state_snapshots` document
- **THEN** a span records the write operation against that collection

#### Scenario: A failed operation is traced as failed
- **WHEN** a database operation returns an error
- **THEN** its span records the failure rather than ending as a success

#### Scenario: GridFS operations are traced
- **WHEN** a firmware binary is streamed into or out of GridFS
- **THEN** those operations are traced and name the GridFS bucket they used

### Requirement: Database spans belong to their caller's trace

A database span SHALL be a child of the span for the work that issued the call, so a slow query
appears inside the trace of the request, activity, or consumption that waited for it. The database
span SHALL NOT start an independent trace when its caller has one.

#### Scenario: A query appears inside its activity's trace
- **WHEN** a rollout activity reads the eligible device pool and a span is active for that activity
- **THEN** the read's span names the activity's span as its parent

#### Scenario: A query appears inside its request's trace
- **WHEN** an HTTP request to the rollout control surface reads a rollout document
- **THEN** the read's span is in the same trace as the request that caused it

#### Scenario: Slow work is attributable from the trace alone
- **WHEN** a database operation is the slowest span in a trace
- **THEN** the trace identifies the caller that waited for it without consulting logs

### Requirement: Database spans carry enough to identify the work

A database span SHALL carry the logical operation, the collection, and where the schema defines it
the document or entity identity the operation concerned — using domain identifiers such as
`device_id`, `rollout_id`, or `firmware_id` — so a trace can be filtered by the entity an operator
already knows.

#### Scenario: Operations are filterable by device
- **WHEN** the ingestion path writes telemetry for one device
- **THEN** its spans carry that device's id

#### Scenario: Operations are filterable by rollout
- **WHEN** the rollout workflow reads or writes a rollout's documents
- **THEN** its spans carry the rollout id

### Requirement: Tracing does not alter database behaviour

Instrumentation SHALL NOT change which operations are issued, their ordering, their timeouts, or
their error handling. An operation SHALL report the same result with tracing enabled and disabled,
and a tracing failure SHALL NOT fail a database call.

#### Scenario: Results are unchanged with tracing off
- **WHEN** the same operation runs with tracing disabled
- **THEN** it returns the same result or the same error

#### Scenario: A tracing failure does not fail a query
- **WHEN** span creation or export fails while a database call runs
- **THEN** the call still returns its own result

### Requirement: Client-side operations are counted for the caller's own metrics

Database spans SHALL be produced by the process that performs the operation, for every process that
talks to MongoDB — the control plane, the worker, and the agent where it reads its configuration or
state — so no process's database work is missing from the trace of a rollout it took part in.

#### Scenario: Worker database work is traced
- **WHEN** the worker's activities read or write MongoDB
- **THEN** those operations are traced and join the trace of the rollout they serve

#### Scenario: Control plane database work is traced
- **WHEN** the control plane ingests a heartbeat into MongoDB
- **THEN** that write is traced and joins the trace of the heartbeat that caused it
