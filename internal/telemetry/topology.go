package telemetry

import (
	"fmt"
	"math"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker element names — the contract every FleetOps process declares and binds against, and
// the names an operator sees in the RabbitMQ UI.
const (
	// EventsExchange carries every event family, routed by the key grammar below.
	EventsExchange = "fleetops.events"
	// RetryExchange carries a failed delivery into the retry queue of the attempt that failed.
	RetryExchange = "fleetops.retry"
	// DeadLetterExchange carries deliveries that exhausted their attempts.
	DeadLetterExchange = "fleetops.dead-letter"

	// HeartbeatEventType is the event type of heartbeat events.
	HeartbeatEventType = "heartbeat"
	// RolloutEventType is the event type of rollout work events.
	RolloutEventType = "rollout"

	// HeartbeatQueue is the work queue of the alerting heartbeat consumer.
	HeartbeatQueue = "fleetops.heartbeat.alerting"
	// RolloutQueue is the work queue rollout work will be consumed from; the rollout workflow
	// is its producer and consumer, so nothing binds to it yet.
	RolloutQueue = "fleetops.rollout.tasks"
)

// Routing-key grammar. An event family is addressed by its prefix, and a consumer narrows its
// slice with a wildcard binding alone — no publisher change.
const (
	heartbeatKeyPrefix = "heartbeat."
	rolloutKeyPrefix   = "rollout.task."
	// heartbeatFamilyKey and rolloutFamilyKey subscribe a work queue to a whole family.
	heartbeatFamilyKey = "heartbeat.#"
	rolloutFamilyKey   = "rollout.task.#"
	// retryKeyPrefix names the path an expired retry takes back to its work queue.
	retryKeyPrefix = "retry."
	// retryQueueInfix and deadLetterQueueSuffix build a work queue's retry and dead-letter
	// queue names from the work queue name.
	retryQueueInfix         = ".retry."
	deadLetterQueueSuffix   = ".dlq"
	argMessageTTL           = "x-message-ttl"
	argDeadLetterExchange   = "x-dead-letter-exchange"
	argDeadLetterRoutingKey = "x-dead-letter-routing-key"
)

// QueueKind labels a queue in the depth metric with what the queue is for.
type QueueKind string

const (
	// QueueKindWork is a consumer's work queue.
	QueueKindWork QueueKind = "work"
	// QueueKindRetry is one of a work queue's per-attempt backoff queues.
	QueueKindRetry QueueKind = "retry"
	// QueueKindDeadLetter is a work queue's terminal queue.
	QueueKindDeadLetter QueueKind = "dead_letter"
)

// Queue is one queue of the declared layout: the name it is declared under and the kind it is
// reported with.
type Queue struct {
	// Name is the queue name.
	Name string
	// Kind is what the queue is for.
	Kind QueueKind
}

// WorkQueue is one consumer's work queue together with the routing keys it consumes from the
// events exchange.
type WorkQueue struct {
	// Name is the work queue name and the consumer's identity: it labels the consumer's
	// processed-events ledger entries and its metrics.
	Name string
	// EventType is the event type the queue carries, which is what its consumers' lag is
	// measured against.
	EventType string
	// Keys are the routing keys bound from EventsExchange, including the key its retries
	// return on.
	Keys []string
}

// Topology is the declared FleetOps broker layout: one topic exchange for every event family,
// the retry and dead-letter exchanges, and for each work queue its per-attempt retry queues and
// its dead-letter queue. The layout is declared by the processes that use it, so it lives in
// code and a fresh broker needs no manual setup.
type Topology struct {
	maxAttempts int
	retryBase   time.Duration
	retryMax    time.Duration
}

// NewTopology returns the layout for maxAttempts processing attempts whose retry delays start
// at retryBase and double per attempt, capped at retryMax.
func NewTopology(maxAttempts int, retryBase, retryMax time.Duration) Topology {
	return Topology{maxAttempts: maxAttempts, retryBase: retryBase, retryMax: retryMax}
}

// MaxAttempts returns how often a delivery may reach a work queue before it is dead-lettered.
func (t Topology) MaxAttempts() int { return t.maxAttempts }

// WorkQueues returns the work queues of the layout with their bindings, in declaration order.
func (t Topology) WorkQueues() []WorkQueue {
	return []WorkQueue{
		{
			Name:      HeartbeatQueue,
			EventType: HeartbeatEventType,
			Keys:      []string{heartbeatFamilyKey, t.RetryKey(HeartbeatQueue)},
		},
		{
			Name:      RolloutQueue,
			EventType: RolloutEventType,
			Keys:      []string{rolloutFamilyKey, t.RetryKey(RolloutQueue)},
		},
	}
}

// WorkQueue returns the layout entry of a named work queue. An unknown name is an error rather
// than an empty entry: a consumer wired to a queue the layout does not declare would declare
// and bind nothing.
func (t Topology) WorkQueue(name string) (WorkQueue, error) {
	for _, q := range t.WorkQueues() {
		if q.Name == name {
			return q, nil
		}
	}
	return WorkQueue{}, fmt.Errorf("work queue %s is not part of the topology", name)
}

// Queues returns every queue of the layout — work, retry, and dead-letter queues — in
// declaration order, each with the kind it is reported under.
func (t Topology) Queues() []Queue {
	var queues []Queue
	for _, q := range t.WorkQueues() {
		queues = append(queues, Queue{Name: q.Name, Kind: QueueKindWork})
		for attempt := 1; attempt < t.maxAttempts; attempt++ {
			queues = append(queues, Queue{Name: t.RetryQueue(q.Name, attempt), Kind: QueueKindRetry})
		}
		queues = append(queues, Queue{Name: t.DeadLetterQueue(q.Name), Kind: QueueKindDeadLetter})
	}
	return queues
}

// RetryQueue returns the retry queue holding failed attempt n of a work queue until its
// backoff delay has elapsed.
func (t Topology) RetryQueue(work string, attempt int) string {
	return work + retryQueueInfix + strconv.Itoa(attempt)
}

// DeadLetterQueue returns the terminal queue of a work queue.
func (t Topology) DeadLetterQueue(work string) string {
	return work + deadLetterQueueSuffix
}

// RetryKey returns the routing key a retry queue returns an expired message on, delivering it
// to the work queue that failed it.
func (t Topology) RetryKey(work string) string {
	return retryKeyPrefix + work
}

// DeadLetterKey returns the routing key a terminal failure is published under, delivering it
// to the work queue's dead-letter queue.
func (t Topology) DeadLetterKey(work string) string {
	return t.DeadLetterQueue(work)
}

// RetryDelay returns the backoff delay of retry attempt n: the base delay doubled once per
// attempt and capped at the configured maximum. Attempts below one take the base delay.
func (t Topology) RetryDelay(attempt int) time.Duration {
	delay := t.retryBase
	for i := 1; i < attempt; i++ {
		if delay >= t.retryMax {
			return t.retryMax
		}
		delay *= 2
	}
	if delay > t.retryMax {
		return t.retryMax
	}
	return delay
}

// HeartbeatRoutingKey returns the routing key of a heartbeat event for a device's registered
// region and model.
func HeartbeatRoutingKey(region, model string) string {
	return heartbeatKeyPrefix + region + "." + model
}

// RolloutRoutingKey returns the routing key of rollout work of one kind.
func RolloutRoutingKey(kind string) string {
	return rolloutKeyPrefix + kind
}

// declarer is the AMQP surface a topology declaration needs. *amqp.Channel and every Session
// satisfy it.
type declarer interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

// declare creates every exchange, queue, and binding of the layout. Declaration is idempotent:
// re-declaring an identically configured element changes nothing and leaves waiting messages
// alone, while an element that exists with different arguments is refused by the broker with an
// error naming it.
func (t Topology) declare(ch declarer) error {
	exchanges := []struct {
		name string
		kind string
	}{
		{EventsExchange, amqp.ExchangeTopic},
		{RetryExchange, amqp.ExchangeDirect},
		{DeadLetterExchange, amqp.ExchangeDirect},
	}
	for _, x := range exchanges {
		if err := ch.ExchangeDeclare(x.name, x.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", x.name, err)
		}
	}

	for _, q := range t.WorkQueues() {
		// A rejected delivery takes the broker's dead-letter path to the work queue's DLQ: the
		// consumer uses it when it cannot even republish to the retry path.
		if _, err := ch.QueueDeclare(q.Name, true, false, false, false, amqp.Table{
			argDeadLetterExchange:   DeadLetterExchange,
			argDeadLetterRoutingKey: t.DeadLetterKey(q.Name),
		}); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.Name, err)
		}
		for _, key := range q.Keys {
			if err := ch.QueueBind(q.Name, key, EventsExchange, false, nil); err != nil {
				return fmt.Errorf("bind queue %s to %s under %s: %w", q.Name, EventsExchange, key, err)
			}
		}
		for attempt := 1; attempt < t.maxAttempts; attempt++ {
			retry := t.RetryQueue(q.Name, attempt)
			ttl, err := millis(t.RetryDelay(attempt))
			if err != nil {
				return fmt.Errorf("retry queue %s: %w", retry, err)
			}
			// One queue per attempt: every message in it shares the same TTL, so a short delay
			// can never wait behind a longer one at the queue head.
			if _, err := ch.QueueDeclare(retry, true, false, false, false, amqp.Table{
				argMessageTTL:           ttl,
				argDeadLetterExchange:   EventsExchange,
				argDeadLetterRoutingKey: t.RetryKey(q.Name),
			}); err != nil {
				return fmt.Errorf("declare retry queue %s: %w", retry, err)
			}
			if err := ch.QueueBind(retry, retry, RetryExchange, false, nil); err != nil {
				return fmt.Errorf("bind retry queue %s to %s: %w", retry, RetryExchange, err)
			}
		}
		dlq := t.DeadLetterQueue(q.Name)
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare dead-letter queue %s: %w", dlq, err)
		}
		if err := ch.QueueBind(dlq, dlq, DeadLetterExchange, false, nil); err != nil {
			return fmt.Errorf("bind dead-letter queue %s to %s: %w", dlq, DeadLetterExchange, err)
		}
	}
	return nil
}

// millis converts a delay to the millisecond integer RabbitMQ's x-message-ttl expects, failing
// loudly instead of truncating a delay the broker could not represent.
func millis(delay time.Duration) (int32, error) {
	ms := delay.Milliseconds()
	if ms > math.MaxInt32 {
		return 0, fmt.Errorf("delay %s exceeds the %dms a message TTL can express", delay, int64(math.MaxInt32))
	}
	return int32(ms), nil
}
