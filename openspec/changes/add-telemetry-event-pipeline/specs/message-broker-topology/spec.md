# Spec Delta

## Purpose

Declares the broker layout every FleetOps event travels through — one topic exchange for heartbeat
and rollout events, the queues that consume them, and the retry and dead-letter paths that keep a
failing message from being requeued forever or dropped silently.

## ADDED Requirements

### Requirement: Exchange layout and routing-key grammar
The system SHALL carry all event families through one durable topic exchange, `fleetops.events`,
and SHALL route them by a fixed key grammar: heartbeat events under `heartbeat.<region>.<model>`
using the device's registered region and model, and rollout work under `rollout.task.<kind>`.
Retry deliveries SHALL travel through the durable direct exchange `fleetops.retry` and terminal
failures through the durable direct exchange `fleetops.dead-letter`. A consumer SHALL be able to
subscribe to a whole family (`heartbeat.#`) or to a narrower slice (`heartbeat.<region>.#`) with a
binding alone, without a change to the publisher.

#### Scenario: Heartbeat event reaches the heartbeat consumer queue
- **WHEN** a heartbeat event is published to `fleetops.events` with routing key `heartbeat.eu-west.v3`
- **THEN** it is enqueued on the work queue bound to the heartbeat family, and no copy is enqueued
  on the rollout work queue

#### Scenario: Rollout work reaches the rollout work queue
- **WHEN** a rollout task event is published to `fleetops.events` with routing key
  `rollout.task.start`
- **THEN** it is enqueued on `fleetops.rollout.tasks`

#### Scenario: Narrow binding selects one region
- **WHEN** a queue is bound to `fleetops.events` with `heartbeat.eu-west.#` and another with
  `heartbeat.#`
- **THEN** an event for region `eu-west` is enqueued on both, and an event for region `us-east` only
  on the family-wide queue

#### Scenario: Every event family has its own key space
- **WHEN** an event is published with routing key `heartbeat.eu-west.v3`
- **THEN** it cannot be delivered to a queue that is bound only to `rollout.task.#`

### Requirement: Queue layout with durable work, retry, and dead-letter queues
Each event family SHALL own a durable work queue — `fleetops.heartbeat.alerting` for heartbeat
events and `fleetops.rollout.tasks` for rollout work — plus a durable retry queue per retry attempt
named `<work queue>.retry.<attempt>` and a durable dead-letter queue named `<work queue>.dlq`. Every
queue and exchange SHALL be durable and every published event SHALL be persistent, so the layout and
its undelivered messages survive a broker restart.

#### Scenario: Work queue names are the consumer contract
- **WHEN** the topology is declared
- **THEN** `fleetops.heartbeat.alerting`, `fleetops.rollout.tasks`, their retry queues, and their
  `.dlq` queues all exist

#### Scenario: Layout survives a broker restart
- **WHEN** the broker restarts with a message waiting in a work queue and a message waiting in a
  retry queue
- **THEN** both queues still exist and both messages are still there

### Requirement: Retry path with exponential backoff
A failed delivery SHALL take the retry path instead of being requeued on the work queue: it is
republished to `fleetops.retry` under the routing key `<work queue>.retry.<attempt>` — the retry
queue of the attempt that just failed — where it waits for that queue's backoff delay and then
returns to the work queue with its attempt count increased and its event identity unchanged. The delay of retry attempt `n` SHALL be
`rabbitmq.retry_base × 2^(n-1)` capped at `rabbitmq.retry_max`, retry queues SHALL exist for
attempts `1` through `rabbitmq.max_attempts - 1`, and a message SHALL NOT be delivered to the work
queue more often than `rabbitmq.max_attempts` times.

#### Scenario: First failure waits the base delay
- **WHEN** the first processing attempt of a delivery fails
- **THEN** the event appears on the work queue again only after `rabbitmq.retry_base` has elapsed,
  with an attempt count of 2

#### Scenario: Each further failure waits twice as long
- **WHEN** the second processing attempt of that delivery fails
- **THEN** it waits `rabbitmq.retry_base × 2` before returning to the work queue, and the delay
  never exceeds `rabbitmq.retry_max`

#### Scenario: Retry preserves the event
- **WHEN** an event returns from the retry path
- **THEN** its payload and its event identifier are exactly what the publisher sent

#### Scenario: Retry attempts are bounded
- **WHEN** a delivery keeps failing
- **THEN** it is delivered to the work queue at most `rabbitmq.max_attempts` times before it is
  dead-lettered

### Requirement: Dead-letter path
A message that has exhausted its attempts, and any delivery the consumer cannot return to the retry
path, SHALL land in that work queue's dead-letter queue rather than being requeued indefinitely or
discarded. A dead-lettered message SHALL carry its original routing key, its event identifier, the
number of attempts made, and the failure reason, and SHALL NOT be redelivered automatically.

#### Scenario: Exhausted attempts end in the dead-letter queue
- **WHEN** a delivery fails on its final permitted attempt
- **THEN** it is present on `<work queue>.dlq` with its attempt count, original routing key, and
  failure reason, and no further delivery of it is made

#### Scenario: Undeliverable failure does not vanish
- **WHEN** a delivery is rejected because it cannot be republished to the retry path
- **THEN** the broker's dead-letter path places it on `<work queue>.dlq` instead of dropping it

#### Scenario: Dead letters wait for an operator
- **WHEN** a message sits in a dead-letter queue
- **THEN** it stays there until it is inspected or removed — no automatic redelivery happens

### Requirement: Topology declaration is idempotent and explicit
The process that publishes or consumes events SHALL declare the whole topology it uses before its
first publish or delivery, and the declaration SHALL be idempotent: declaring an existing,
identically configured element is a no-op that neither purges messages nor fails. A declaration
that conflicts with an existing element's type or arguments SHALL fail loudly instead of silently
adapting to it.

#### Scenario: Declaration is repeatable
- **WHEN** the topology is declared twice, with messages already waiting in a queue
- **THEN** the second declaration succeeds and the waiting messages are still there

#### Scenario: Declaration precedes use
- **WHEN** a process starts against a broker that carries no FleetOps topology
- **THEN** every exchange, work queue, retry queue, and dead-letter queue it uses exists before its
  first publish or delivery

#### Scenario: Conflicting declaration fails loudly
- **WHEN** an element with a FleetOps name already exists with different arguments
- **THEN** startup fails with an error naming the conflicting element instead of running against it
