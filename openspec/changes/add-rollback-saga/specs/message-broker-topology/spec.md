# Spec Delta

## MODIFIED Requirements

### Requirement: Exchange layout and routing-key grammar

The system SHALL carry all event families through one durable topic exchange, `fleetops.events`,
and SHALL route them by a fixed key grammar: heartbeat events under `heartbeat.<region>.<model>`
using the device's registered region and model, rollout work under `rollout.task.<kind>`, and
rollout notifications under `rollout.notification.<kind>.<phase>`. Retry deliveries SHALL travel
through the durable direct exchange `fleetops.retry` and terminal failures through the durable direct
exchange `fleetops.dead-letter`. A consumer SHALL be able to subscribe to a whole family
(`heartbeat.#`) or to a narrower slice (`heartbeat.<region>.#`, `rollout.notification.rollback.#`)
with a binding alone, without a change to the publisher. Because the key spaces are disjoint, a
notification SHALL NOT be delivered to a queue that consumes rollout work, and a consumer of
notifications SHALL receive none of the work a rollout dispatches.

#### Scenario: Heartbeat event reaches the heartbeat consumer queue

- **WHEN** a heartbeat event is published to `fleetops.events` with routing key `heartbeat.eu-west.v3`
- **THEN** it is enqueued on the work queue bound to the heartbeat family, and no copy is enqueued
  on the rollout work queue

#### Scenario: Rollout work reaches the rollout work queue

- **WHEN** a rollout task event is published to `fleetops.events` with routing key
  `rollout.task.start`
- **THEN** it is enqueued on `fleetops.rollout.tasks`

#### Scenario: A notification reaches the notification queue

- **WHEN** a rollback notification is published to `fleetops.events` with routing key
  `rollout.notification.rollback.started`
- **THEN** it is enqueued on the queue bound to the notification family, and no copy is enqueued on
  `fleetops.rollout.tasks`

#### Scenario: A consumer narrows to one kind of notification

- **WHEN** a queue is bound to `fleetops.events` with `rollout.notification.rollback.#` and another
  with `rollout.notification.#`
- **THEN** a rollback notification is enqueued on both, and a notification of another kind only on
  the family-wide queue

#### Scenario: Narrow binding selects one region

- **WHEN** a queue is bound to `fleetops.events` with `heartbeat.eu-west.#` and another with
  `heartbeat.#`
- **THEN** an event for region `eu-west` is enqueued on both, and an event for region `us-east` only
  on the family-wide queue

#### Scenario: Every event family has its own key space

- **WHEN** an event is published with routing key `heartbeat.eu-west.v3`
- **THEN** it cannot be delivered to a queue that is bound only to `rollout.task.#` or only to
  `rollout.notification.#`

### Requirement: Queue layout with durable work, retry, and dead-letter queues

Each event family SHALL own a durable work queue — `fleetops.heartbeat.alerting` for heartbeat
events, `fleetops.rollout.tasks` for rollout work, and `fleetops.rollout.notifications` for rollout
notifications — plus a durable retry queue per retry attempt named `<work queue>.retry.<attempt>` and
a durable dead-letter queue named `<work queue>.dlq`. The notification queue SHALL be declared and
bound whether or not a consumer is attached to it, so that a published notification is routable from
the moment the topology is declared and a consumer attaches by binding to the queue that already
exists. Every queue and exchange SHALL be durable and every published event SHALL be persistent, so
the layout and its undelivered messages survive a broker restart.

#### Scenario: Work queue names are the consumer contract

- **WHEN** the topology is declared
- **THEN** `fleetops.heartbeat.alerting`, `fleetops.rollout.tasks`, `fleetops.rollout.notifications`,
  their retry queues, and their `.dlq` queues all exist

#### Scenario: A notification is routable before a consumer exists

- **WHEN** a notification is published while no consumer is attached to the notification queue
- **THEN** the broker accepts and enqueues it rather than returning it as unroutable, and it waits on
  the queue until a consumer takes it

#### Scenario: Layout survives a broker restart

- **WHEN** the broker restarts with a message waiting in a work queue and a message waiting in a
  retry queue
- **THEN** both queues still exist and both messages are still there
