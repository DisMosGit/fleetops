# Spec Delta

## Purpose

Gives an operator watching the telemetry pipeline one screen that answers whether the fleet's data
is flowing: how many heartbeats are arriving, how far behind the consumers are, whether anything is
piling up undelivered, and how long traced work is taking end to end.

## ADDED Requirements

### Requirement: The dashboard shows heartbeat throughput

The dashboard SHALL show heartbeats accepted over time, so a fleet going quiet is distinguishable
from an ingestion path that has stalled — the two look identical in a static total and different in
a rate.

#### Scenario: Throughput is shown as a rate over time
- **WHEN** heartbeats arrive at a steady rate
- **THEN** the dashboard shows that rate over time rather than an ever-increasing total

#### Scenario: A silent fleet is visible
- **WHEN** the devices stop reporting
- **THEN** the throughput panel falls toward zero within the dashboard's resolution

#### Scenario: Publication and acceptance are distinguishable
- **WHEN** ingestion accepts heartbeats while broker publication fails
- **THEN** the dashboard shows accepted heartbeats continuing, so a broker problem is not read as a silent fleet

### Requirement: The dashboard shows consumer lag

The dashboard SHALL show each consumer's lag against the events it consumes, so a consumer falling
behind is visible while it is falling behind rather than only once its queue is deep.

#### Scenario: Lag is shown per consumer
- **WHEN** two consumers are at different positions in their streams
- **THEN** the dashboard shows each consumer's lag separately

#### Scenario: A consumer falling behind is visible as a trend
- **WHEN** production outpaces one consumer's processing
- **THEN** the dashboard shows that consumer's lag rising over time

#### Scenario: Recovery is visible
- **WHEN** a lagging consumer catches up
- **THEN** its lag returns to zero on the dashboard

### Requirement: The dashboard shows dead-letter depth

The dashboard SHALL show the depth of every dead-letter queue, so a poison message that has been
set aside is visible rather than being discovered by inspecting the broker by hand. Work and retry
queue depths SHALL be distinguishable from dead-letter depth, so a busy pipeline is not mistaken
for a failing one.

#### Scenario: A dead letter is visible as depth
- **WHEN** a message is dead-lettered
- **THEN** the dashboard shows that queue's depth rising above zero

#### Scenario: Dead-letter depth is distinguishable from ordinary backlog
- **WHEN** a work queue and a dead-letter queue both hold messages
- **THEN** the dashboard shows them as different series rather than one combined depth

#### Scenario: An empty dead-letter queue reads as empty
- **WHEN** no messages have been dead-lettered
- **THEN** the panel shows zero rather than no data, so the absence of failures is itself readable

### Requirement: The dashboard shows trace latency

The dashboard SHALL show latency as observed at the traced hops — the control plane's request
handling, the worker's activity execution, and the database calls beneath them — with percentiles
over a window, so slowness is attributable to a hop rather than reported as one end-to-end number.
Because traces live in the tracing backend and not in Prometheus, these panels SHALL read
span-derived Prometheus histograms that this change exposes; the dashboard SHALL NOT query a
tracing backend.

#### Scenario: Latency is shown by hop
- **WHEN** activity execution is slower than request handling
- **THEN** the dashboard shows the two separately rather than as one combined latency

#### Scenario: Percentiles are available over a window
- **WHEN** an operator inspects a latency panel over a time range
- **THEN** the panel shows percentiles computed over that range

#### Scenario: The dashboard reads Prometheus, not the tracing backend
- **WHEN** the dashboard is provisioned with only the Prometheus datasource available
- **THEN** every panel resolves, and no panel requires a tracing-backend datasource

#### Scenario: A latency regression is visible against its baseline
- **WHEN** a hop's latency rises above its recent baseline
- **THEN** the change is visible on the panel without comparing two separate queries by hand

### Requirement: Every panel reads a metric with a real source

Every panel on the dashboard SHALL read a Prometheus metric that this change, the existing pipeline
instrumentation, or an existing capability produces. No panel SHALL read a metric that no component
publishes, and no metric SHALL be invented for a panel's convenience.

#### Scenario: Panels resolve against the stack
- **WHEN** the dashboard is opened against the provisioned stack with the application running
- **THEN** every panel returns data rather than a no-data state caused by a metric nothing publishes

#### Scenario: Pipeline panels read the existing broker metrics
- **WHEN** the consumer lag and dead-letter panels are inspected
- **THEN** they read the queue and lag metrics the pipeline already exposes rather than a duplicate series added for the dashboard

#### Scenario: A panel with no data says so
- **WHEN** a panel's query has no data because the work it describes has not happened yet
- **THEN** the panel reports no data rather than rendering zero as if it were a measurement

### Requirement: The dashboard is provisioned from the repository

The dashboard SHALL be a file tracked in `dashboards/`, provisioned into Grafana by the stack, and
SHALL name its datasource by the provisioned reference rather than by an environment-specific
identifier, so it works on a clean start with no import or edit step.

#### Scenario: The dashboard loads on a clean start
- **WHEN** the stack starts from a clean checkout
- **THEN** the dashboard is present and its panels query the provisioned datasource

#### Scenario: The dashboard carries its own identity
- **WHEN** the dashboard is provisioned
- **THEN** it has a stable title and identifier, so a restart updates the existing dashboard instead of creating a duplicate

#### Scenario: The two dashboards do not collide
- **WHEN** both the rollout and pipeline dashboards are provisioned together
- **THEN** each appears once with its own identity, and neither overwrites the other
