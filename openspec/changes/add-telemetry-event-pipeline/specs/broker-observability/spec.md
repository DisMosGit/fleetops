# Spec Delta

## Purpose

Makes the event pipeline's backlog observable: how deep every queue is (dead-letter queues
included), how far behind each consumer is relative to the latest produced event, and what happened
to every event that was published or consumed.

## ADDED Requirements

### Requirement: Queue depth metric
The control plane SHALL expose a gauge of the number of messages waiting on every queue in the
declared topology, labeled by queue name and queue kind (`work`, `retry`, `dead_letter`), sampled at
`rabbitmq.queue_depth_interval`. The dead-letter queue of each consumer SHALL be covered, so a
growing dead-letter backlog is visible as a metric without inspecting the broker by hand. A sampling
failure SHALL NOT terminate the process and SHALL NOT report a fabricated depth.

#### Scenario: Dead-letter depth reflects dead letters
- **WHEN** a message lands on a dead-letter queue
- **THEN** that queue's depth metric reports the number of messages waiting on it, with kind
  `dead_letter`

#### Scenario: Work queue backlog is visible
- **WHEN** events accumulate on a work queue faster than the consumer processes them
- **THEN** that queue's depth metric rises above zero

#### Scenario: Retry backlog is visible
- **WHEN** messages wait in a retry queue for their backoff delay
- **THEN** that retry queue's depth metric reports them, with kind `retry`

#### Scenario: Unreachable broker does not fake a depth
- **WHEN** the broker cannot be queried at sampling time
- **THEN** the process keeps running, the failure is logged, and no zero depth is published for the
  queues that could not be sampled

### Requirement: Consumer lag metric
The control plane SHALL expose a gauge of how far behind each consumer is relative to the latest
produced event of the type that consumer consumes, labeled by consumer name. Lag SHALL be the number
of events of that type published after the last event the consumer processed, so it is zero when the
consumer has caught up, rises while production outpaces consumption, and never falls below zero.

#### Scenario: Lag rises while production outpaces consumption
- **WHEN** events of a consumer's type are published faster than that consumer processes them
- **THEN** its lag reflects the number of events published after the last one it processed

#### Scenario: Lag returns to zero on catch-up
- **WHEN** the consumer processes the newest published event
- **THEN** its lag is zero

#### Scenario: Lag never counts backwards
- **WHEN** a consumer processes a duplicate or an out-of-order older event
- **THEN** its lag does not become negative and does not report progress it did not make

#### Scenario: Lag is reported per consumer
- **WHEN** two consumers with different progress both exist
- **THEN** each is reported under its own label, and a lagging consumer does not hide behind a
  caught-up one

### Requirement: Event outcome counters
The control plane SHALL expose counters for published events by event type, for events that could
not be published (broker unreachable, buffer full, broker rejection, or unroutable), and for
consumption outcomes by consumer and outcome, where the outcomes distinguish a newly processed
event, a duplicate suppressed by deduplication, a delivery sent to the retry path, and a delivery
dead-lettered. Every event that enters the pipeline SHALL be accountable in exactly one terminal
outcome counter.

#### Scenario: Published events are counted by type
- **WHEN** heartbeat events are published
- **THEN** the published counter for the heartbeat event type increases once per published event

#### Scenario: Dropped events are counted with a reason
- **WHEN** an event cannot be published
- **THEN** the failed-publication counter increases and its labels distinguish an unreachable
  broker, a full buffer, a broker rejection, and an unroutable event

#### Scenario: Duplicates are visible as duplicates
- **WHEN** a consumer suppresses a redelivery through deduplication
- **THEN** its duplicate outcome counter increases and its processed outcome counter does not

#### Scenario: Retries and dead letters are counted
- **WHEN** a delivery enters the retry path, and when a delivery is dead-lettered
- **THEN** the retry and dead-letter outcome counters for that consumer increase respectively

### Requirement: Pipeline metrics are scrapeable with the existing metrics
These metrics SHALL be served in Prometheus exposition format on the configured
`observability.metrics_addr`, in the `fleetops_` namespace alongside the metrics that already exist,
and SHALL be registered before the metrics endpoint starts serving so no scrape can miss them.

#### Scenario: Metrics endpoint exposes the pipeline
- **WHEN** the metrics endpoint is scraped
- **THEN** the queue depth, consumer lag, published, failed-publication, and outcome metrics are all
  present in the exposition

#### Scenario: Metric names follow the project namespace
- **WHEN** the exposition is inspected
- **THEN** every metric added by this capability is prefixed `fleetops_`
