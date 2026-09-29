package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler applies the durable side effect of one consumed event. It must be idempotent: an
// unfinished claim is resumed and an event aged past the ledger's retention window is applied
// again, so applying the same event twice has to converge. *Alerting satisfies it.
type Handler interface {
	// Apply applies one event's side effect.
	Apply(ctx context.Context, env Envelope) error
}

// ClaimLedger is the deduplication ledger a consumer needs: it claims an event before applying
// it and records the claim as processed once the side effect is durable. *Ledger satisfies it.
type ClaimLedger interface {
	// Claim records that the consumer is taking an event and reports whether the event is new,
	// unfinished work, or already processed.
	Claim(ctx context.Context, consumer, eventID, deviceID string, claimedAt time.Time) (ClaimState, error)
	// MarkProcessed records that the consumer's side effect for the event is durable.
	MarkProcessed(ctx context.Context, consumer, eventID string, processedAt time.Time) error
}

// SequenceSource reports the sequence of the latest event published for an event type, which is
// what a consumer's lag is measured against. *Publisher satisfies it.
type SequenceSource interface {
	// LatestSequence returns the sequence of the most recently published event of a type, and
	// zero when none has been published.
	LatestSequence(eventType string) uint64
}

// ConsumerOptions configure one work-queue consumer. Metrics and Source are required.
type ConsumerOptions struct {
	// Prefetch bounds the unacknowledged deliveries the consumer holds in flight, so a slow side
	// effect applies backpressure instead of accumulating work.
	Prefetch int
	// Metrics receives the pipeline counters and the consumer's lag gauge. Required.
	Metrics *Metrics
	// Source reports the latest published sequence lag is measured against. Required.
	Source SequenceSource
	// Now is the clock the claim and completion times are stamped with. Defaults to time.Now.
	Now func() time.Time
}

// Consumer consumes one work queue. It deduplicates by (consumer, event id) through the ledger,
// acknowledges a delivery only after its side effect is durable, drives failures through the
// retry ladder, and dead-letters what keeps failing or cannot be understood.
type Consumer struct {
	work       WorkQueue
	topology   Topology
	ledger     ClaimLedger
	handler    Handler
	metrics    *Metrics
	source     SequenceSource
	prefetch   int
	log        *slog.Logger
	supervisor *Supervisor
	now        func() time.Time

	mu sync.Mutex
	// lastProcessed is the sequence of the newest event whose side effect is durable. It only
	// moves forward, so a duplicate or an older event cannot report progress that was not made.
	lastProcessed uint64
}

// NewConsumer returns the consumer of one work queue, deduplicating under the work queue's name
// — the consumer's identity in the ledger and in its metrics — and reporting its lag against
// opts.Source.
func NewConsumer(
	url string,
	work WorkQueue,
	topology Topology,
	ledger ClaimLedger,
	handler Handler,
	opts ConsumerOptions,
	log *slog.Logger,
) *Consumer {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	c := &Consumer{
		work:       work,
		topology:   topology,
		ledger:     ledger,
		handler:    handler,
		metrics:    opts.Metrics,
		source:     opts.Source,
		prefetch:   opts.Prefetch,
		log:        log,
		now:        opts.Now,
		supervisor: NewSupervisor(url, topology, log),
	}
	opts.Metrics.RegisterLag(work.Name, c.Lag)
	return c
}

// Run consumes until ctx is done, reconnecting through the supervisor whenever the connection or
// channel is lost. Deliveries left unacknowledged by a lost session are redelivered, and
// deduplication makes that safe.
func (c *Consumer) Run(ctx context.Context) error {
	return c.supervisor.Run(ctx, c.work.Name, c.serve)
}

// Lag reports how many events of the consumer's event type were published after the last event
// it processed: zero once it has caught up, and never negative.
func (c *Consumer) Lag() float64 {
	latest := c.source.LatestSequence(c.work.EventType)
	c.mu.Lock()
	last := c.lastProcessed
	c.mu.Unlock()
	if latest <= last {
		return 0
	}
	return float64(latest - last)
}

// serve consumes one work queue on a live session until ctx is done or the session drops.
func (c *Consumer) serve(ctx context.Context, sess Session) error {
	if err := sess.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("bound in-flight deliveries of %s to %d: %w", c.work.Name, c.prefetch, err)
	}
	deliveries, err := sess.Consume(c.work.Name, c.work.Name, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", c.work.Name, err)
	}
	pub, err := newSessionPublisher(sess)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sess.Lost():
			return errSessionLost
		case delivery, ok := <-deliveries:
			if !ok {
				return errSessionLost
			}
			if err := c.handle(ctx, pub, delivery); err != nil {
				return err
			}
		}
	}
}

// handle processes one delivery: decode, claim, apply, complete, and settle. The returned error
// means the session itself became unusable and the caller must reconnect.
func (c *Consumer) handle(ctx context.Context, pub *sessionPublisher, delivery amqp.Delivery) error {
	attempt := attemptOf(delivery)
	env, err := DecodeEvent(delivery.Body)
	if err != nil {
		// An event that cannot be understood is not a transient failure: dead-letter it at once
		// instead of burning retry attempts on it.
		return c.deadLetter(ctx, pub, delivery, attempt, fmt.Errorf("unusable event: %w", err))
	}

	state, err := c.ledger.Claim(ctx, c.work.Name, env.EventID, env.DeviceID, c.now())
	if err != nil {
		return c.fail(ctx, pub, delivery, attempt, err)
	}
	if state == ClaimProcessed {
		// Already applied: acknowledge as a duplicate without repeating the side effect.
		c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeDuplicate).Inc()
		return c.settle(delivery)
	}

	if err := c.handler.Apply(ctx, env); err != nil {
		return c.fail(ctx, pub, delivery, attempt, fmt.Errorf("apply event %s: %w", env.EventID, err))
	}
	if err := c.ledger.MarkProcessed(ctx, c.work.Name, env.EventID, c.now()); err != nil {
		return c.fail(ctx, pub, delivery, attempt, err)
	}

	c.observe(env.Sequence)
	c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeProcessed).Inc()
	return c.settle(delivery)
}

// fail moves a failed delivery along: into the retry ladder while attempts remain, and to the
// dead-letter queue once they are exhausted. A delivery that cannot be republished at all is
// rejected so the broker's own dead-letter path holds it instead of losing it.
func (c *Consumer) fail(ctx context.Context, pub *sessionPublisher, delivery amqp.Delivery, attempt int, cause error) error {
	c.log.Warn("delivery failed",
		"consumer", c.work.Name, "event_id", delivery.MessageId, "attempt", attempt, "err", cause)
	if attempt >= c.topology.MaxAttempts() {
		return c.deadLetter(ctx, pub, delivery, attempt, cause)
	}

	// The retry queue of the attempt that just failed holds the event for that attempt's
	// backoff delay and then returns it to the work queue with the next attempt stamped on it.
	next := attempt + 1
	key := c.topology.RetryQueue(c.work.Name, attempt)
	outcome, err := pub.Publish(ctx, RetryExchange, key, forwardMessage(delivery, next, ""))
	if outcome == publishConfirmed {
		c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeRetry).Inc()
		return c.settle(delivery)
	}

	c.log.Error("retry republish failed, dead-lettering the delivery",
		"consumer", c.work.Name, "event_id", delivery.MessageId, "attempt", attempt,
		"outcome", outcome.String(), "err", err)
	c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeDeadLetter).Inc()
	rejectErr := c.reject(delivery)
	if outcome == publishLost {
		return errors.Join(err, rejectErr)
	}
	return rejectErr
}

// deadLetter publishes a terminal failure to the work queue's dead-letter queue with the attempts
// made and the reason it failed, and acknowledges the delivery only once that publish is
// confirmed. If even the dead letter cannot be published, the delivery is rejected so the
// broker's dead-letter path takes it.
func (c *Consumer) deadLetter(ctx context.Context, pub *sessionPublisher, delivery amqp.Delivery, attempts int, cause error) error {
	key := c.topology.DeadLetterKey(c.work.Name)
	outcome, err := pub.Publish(ctx, DeadLetterExchange, key, forwardMessage(delivery, attempts, cause.Error()))
	if outcome == publishConfirmed {
		c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeDeadLetter).Inc()
		c.log.Warn("dead-lettered a delivery",
			"consumer", c.work.Name, "event_id", delivery.MessageId, "queue", key,
			"attempts", attempts, "reason", cause)
		return c.settle(delivery)
	}

	c.log.Error("dead letter not published, rejecting the delivery onto the broker's dead-letter path",
		"consumer", c.work.Name, "event_id", delivery.MessageId,
		"outcome", outcome.String(), "err", err)
	c.metrics.Consumed.WithLabelValues(c.work.Name, outcomeDeadLetter).Inc()
	rejectErr := c.reject(delivery)
	if outcome == publishLost {
		return errors.Join(err, rejectErr)
	}
	return rejectErr
}

// settle acknowledges one processed delivery. It fails only when the session is already gone.
func (c *Consumer) settle(delivery amqp.Delivery) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge delivery of event %s: %w", delivery.MessageId, err)
	}
	return nil
}

// reject returns one delivery to the broker without requeue, so its dead-letter exchange takes
// it. Requeueing would loop it on the work queue forever.
func (c *Consumer) reject(delivery amqp.Delivery) error {
	if err := delivery.Reject(false); err != nil {
		return fmt.Errorf("reject delivery of event %s: %w", delivery.MessageId, err)
	}
	return nil
}

// observe records the sequence of an event whose side effect is durable.
func (c *Consumer) observe(sequence uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sequence > c.lastProcessed {
		c.lastProcessed = sequence
	}
}
