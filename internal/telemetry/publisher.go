package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// Sequences is a publisher's per-event-type monotonic counter. Consumers read the latest issued
// sequence to measure how far behind the newest produced event they are.
type Sequences struct {
	mu   sync.Mutex
	next map[string]uint64
}

// Next returns the next sequence for an event type.
func (s *Sequences) Next(eventType string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next == nil {
		s.next = make(map[string]uint64)
	}
	s.next[eventType]++
	return s.next[eventType]
}

// LatestSequence returns the sequence of the most recently issued event of a type, and zero when
// none has been issued yet, so a consumer can measure its lag against the newest produced event.
func (s *Sequences) LatestSequence(eventType string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next[eventType]
}

// HeartbeatPublisher queues an accepted heartbeat for broker publication. It reports nothing:
// the publication is a derived fan-out beside the durable ingest write, so broker trouble may
// never fail — or block — heartbeat acceptance.
type HeartbeatPublisher interface {
	// Handle queues one accepted heartbeat with the device identity its registration
	// established.
	Handle(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record)
}

// Publisher turns accepted heartbeats into persistent broker events. Handle never blocks: it
// queues into a bounded buffer and sheds load with a counter when the broker cannot keep up. A
// publisher goroutine owns the AMQP connection, stamps each event with the publisher's sequence
// and publish time, awaits the broker's confirmation, and reconnects with backoff when the
// connection drops.
type Publisher struct {
	url        string
	topology   Topology
	log        *slog.Logger
	metrics    *Metrics
	sequences  *Sequences
	supervisor *Supervisor
	buffer     chan Envelope
	// now is the publish clock; a test replaces it to pin published_at.
	now func() time.Time
	// dropping records whether the current burst of drops has already been logged, so a broker
	// outage logs its first dropped event instead of every one.
	dropping atomic.Bool
}

// NewPublisher returns a publisher of accepted heartbeats on the topology's events exchange.
// bufferSize bounds the events awaiting publication; a full buffer sheds load instead of
// blocking the heartbeat path.
func NewPublisher(url string, topology Topology, bufferSize int, metrics *Metrics, log *slog.Logger) *Publisher {
	p := &Publisher{
		url:       url,
		topology:  topology,
		log:       log,
		metrics:   metrics,
		sequences: &Sequences{},
		buffer:    make(chan Envelope, bufferSize),
		now:       time.Now,
	}
	p.supervisor = NewSupervisor(url, topology, log)
	return p
}

// Handle queues one accepted heartbeat for publication. It never blocks and never fails: a full
// buffer sheds the event with a drop counter, because the durable ingest write — not the
// fan-out — is the source of truth for heartbeat acceptance.
func (p *Publisher) Handle(_ context.Context, hb *agentv1.Heartbeat, rec devices.Record) {
	if hb.GetEventId() == "" || hb.GetTs() == nil {
		// The ingest path refuses these, so the fan-out only sees them if it is wired as the
		// sink directly; counting them keeps every event accountable.
		p.drop(HeartbeatEventType, dropReasonUnusable, nil)
		return
	}
	env := NewHeartbeatEvent(
		hb.GetEventId(),
		hb.GetDeviceId(),
		rec.Region,
		rec.Model,
		hb.GetTs().AsTime(),
		time.Time{},
		0,
		Payload{
			CPU:       hb.GetCpu(),
			Mem:       hb.GetMem(),
			Health:    hb.GetHealth(),
			CurrentFW: hb.GetCurrentFw(),
			Status:    hb.GetStatus(),
		},
	)
	select {
	case p.buffer <- env:
	default:
		p.drop(env.EventType, dropReasonBufferFull, nil)
	}
}

// Run publishes until ctx is done, reconnecting through the supervisor whenever the connection
// or channel is lost. Buffered events survive a reconnect; they are published afterwards.
func (p *Publisher) Run(ctx context.Context) error {
	return p.supervisor.Run(ctx, "publisher", p.serve)
}

// LatestSequence returns the sequence of the latest event published for an event type, which is
// what a consumer measures its lag against.
func (p *Publisher) LatestSequence(eventType string) uint64 {
	return p.sequences.LatestSequence(eventType)
}

// serve publishes buffered events on one live session until ctx is done or the session drops.
func (p *Publisher) serve(ctx context.Context, sess Session) error {
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
		case env := <-p.buffer:
			// The sequence and the publish time are stamped here, in the one goroutine that
			// publishes, so the sequence is monotonic in publication order.
			env.Sequence = p.sequences.Next(env.EventType)
			env.PublishedAt = p.now()
			key, msg, err := p.message(env)
			if err != nil {
				p.drop(env.EventType, dropReasonUnusable, err)
				continue
			}

			outcome, err := pub.Publish(ctx, EventsExchange, key, msg)
			switch outcome {
			case publishConfirmed:
				p.metrics.Published.WithLabelValues(env.EventType).Inc()
				p.dropping.Store(false)
			case publishNacked:
				p.drop(env.EventType, dropReasonRejected, fmt.Errorf("broker rejected the event under %s", key))
			case publishUnroutable:
				p.drop(env.EventType, dropReasonUnroutable, fmt.Errorf("no queue is bound to %s", key))
			case publishLost:
				p.drop(env.EventType, dropReasonUnreachable, err)
				// The verdict never arrived, so the session can no longer be trusted to keep
				// confirmations aligned with publishes.
				return err
			}
		}
	}
}

// message renders one event as the routing key and persistent message it is published as.
func (p *Publisher) message(env Envelope) (string, amqp.Publishing, error) {
	key, err := routingKey(env)
	if err != nil {
		return "", amqp.Publishing{}, err
	}
	msg, err := env.Message(1, key)
	if err != nil {
		return "", amqp.Publishing{}, err
	}
	return key, msg, nil
}

// drop counts one event that could not be published and logs the first drop of a burst, so an
// outage is visible without one log line per event.
func (p *Publisher) drop(eventType, reason string, err error) {
	p.metrics.Dropped.WithLabelValues(eventType, reason).Inc()
	if p.dropping.CompareAndSwap(false, true) {
		p.log.Warn("dropping an event", "event_type", eventType, "reason", reason, "err", err)
	}
}

// routingKey returns the key an event is published under, per the topology's key grammar.
func routingKey(env Envelope) (string, error) {
	switch env.EventType {
	case HeartbeatEventType:
		return HeartbeatRoutingKey(env.Region, env.Model), nil
	default:
		return "", fmt.Errorf("no routing key for event type %s", env.EventType)
	}
}
