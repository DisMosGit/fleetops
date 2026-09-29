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

// Reconnect backoff: a lost broker connection is retried with a doubling pause, capped so a
// long outage settles into an occasional retry instead of a busy loop.
const (
	initialReconnectBackoff = 250 * time.Millisecond
	maxReconnectBackoff     = 30 * time.Second
)

// errTopologyConflict marks a declaration the broker refused because an element with a
// FleetOps name exists with different properties. It is a configuration error rather than a
// transient outage, so it stops the process instead of being retried forever.
var errTopologyConflict = errors.New("broker topology conflict")

// errSessionLost reports that a session's connection or channel dropped while a component was
// using it. It is the expected end of every session that did not stop because ctx was done.
var errSessionLost = errors.New("broker session lost")

// Channel is the AMQP channel surface the pipeline uses: topology declaration, publishing with
// confirms, consuming with prefetch, queue inspection, and message settlement. *amqp.Channel
// satisfies it.
type Channel interface {
	declarer
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Reject(tag uint64, requeue bool) error
}

// Session is one live broker channel — what a supervised component runs against — plus the
// notification that the channel and its connection are gone.
type Session interface {
	Channel
	// Lost is closed when the session's connection or channel drops. Every loop over a session
	// must stop when it fires: publishing into a dead channel only produces errors.
	Lost() <-chan struct{}
	// Close releases the session's channel and connection.
	Close() error
}

// Supervisor owns one AMQP connection: it dials, declares the topology, hands a live session to
// a component's loop, and — when the connection or channel drops — reconnects with capped
// exponential backoff and declares the topology again, until ctx is done. A broker outage is
// therefore a reconnect, never a process exit.
type Supervisor struct {
	url      string
	topology Topology
	log      *slog.Logger
	// open is the session source; a test replaces it with a scripted session.
	open func(ctx context.Context) (Session, error)
}

// NewSupervisor returns a supervisor dialing url and declaring topology on every connection it
// opens.
func NewSupervisor(url string, topology Topology, log *slog.Logger) *Supervisor {
	s := &Supervisor{url: url, topology: topology, log: log}
	s.open = s.dial
	return s
}

// Run calls serve with a live session until ctx is done, reconnecting in between. serve must
// return when the session is lost or when ctx is done. A serve error is logged, not returned:
// once the topology is declared, losing the broker is a degraded fan-out, not a failure worth
// stopping the control plane for. Run returns non-nil only for a topology conflict, which no
// amount of reconnecting can resolve.
func (s *Supervisor) Run(ctx context.Context, name string, serve func(ctx context.Context, sess Session) error) error {
	attempt := 0
	for {
		sess, err := s.open(ctx)
		if err == nil {
			attempt = 0
			s.log.Info("broker session established", "component", name)
			err = serve(ctx, sess)
			// The session is dead either way: serve returns when ctx is done or when the
			// session is lost, and a closed channel is not worth reporting against a shutdown.
			if closeErr := sess.Close(); closeErr != nil && ctx.Err() == nil {
				s.log.Debug("close broker session", "component", name, "err", closeErr)
			}
		} else if errors.Is(err, errTopologyConflict) {
			return fmt.Errorf("declare topology for %s: %w", name, err)
		}
		if ctx.Err() != nil {
			return nil
		}

		attempt++
		backoff := ReconnectDelay(attempt)
		s.log.Error("broker session lost, reconnecting",
			"component", name, "attempt", attempt, "backoff", backoff, "err", err)
		if !pause(ctx, backoff) {
			return nil
		}
	}
}

// ReconnectDelay returns the pause before the nth reconnect attempt: doubling from
// initialReconnectBackoff, capped at maxReconnectBackoff.
func ReconnectDelay(attempt int) time.Duration {
	delay := initialReconnectBackoff
	for i := 1; i < attempt; i++ {
		if delay >= maxReconnectBackoff {
			return maxReconnectBackoff
		}
		delay *= 2
	}
	if delay > maxReconnectBackoff {
		return maxReconnectBackoff
	}
	return delay
}

// dial opens a connection, declares the topology on a fresh channel, and returns the live
// session. Any error is returned as-is: Run decides whether it is worth retrying.
func (s *Supervisor) dial(ctx context.Context) (Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := amqp.Dial(s.url)
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			s.log.Debug("close broker connection after channel failure", "err", closeErr)
		}
		return nil, fmt.Errorf("open broker channel: %w", err)
	}
	sess := watchSession(conn, ch)
	if err := s.topology.declare(sess); err != nil {
		if closeErr := sess.Close(); closeErr != nil {
			s.log.Debug("close broker session after declaration failure", "err", closeErr)
		}
		if isDeclarationRefusal(err) {
			return nil, fmt.Errorf("%w: %w", errTopologyConflict, err)
		}
		return nil, err
	}
	return sess, nil
}

// isDeclarationRefusal reports whether err is the broker's refusal of a declaration that
// conflicts with an existing element (AMQP 406 PRECONDITION_FAILED) or of arguments it rejects
// outright (503 COMMAND_INVALID): neither is resolved by reconnecting.
func isDeclarationRefusal(err error) bool {
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return false
	}
	return amqpErr.Code == amqp.PreconditionFailed || amqpErr.Code == amqp.CommandInvalid
}

// session is one live connection and channel. Embedding the channel gives the session the AMQP
// surface a supervised component uses; Lost and Close carry the connection's lifetime.
type session struct {
	conn *amqp.Connection
	*amqp.Channel
	lost chan struct{}
	once sync.Once
}

// watchSession returns a session whose Lost channel closes as soon as conn or ch drops. The
// watcher goroutine has a stop condition of its own: both notifications fire when the session
// is closed deliberately.
func watchSession(conn *amqp.Connection, ch *amqp.Channel) *session {
	s := &session{conn: conn, Channel: ch, lost: make(chan struct{})}
	connLost := conn.NotifyClose(make(chan *amqp.Error, 1))
	chLost := ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case <-connLost:
		case <-chLost:
		}
		s.once.Do(func() { close(s.lost) })
	}()
	return s
}

// Lost reports the session's loss notification.
func (s *session) Lost() <-chan struct{} { return s.lost }

// Close releases the channel and the connection, returning the first failure of either.
func (s *session) Close() error {
	err := s.Channel.Close()
	if connErr := s.conn.Close(); err == nil {
		err = connErr
	}
	return err
}

// pause waits for d and reports whether it elapsed; false means ctx finished first.
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
