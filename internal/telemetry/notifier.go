package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Notifier publishes rollback announcements, waiting for the broker's verdict on every one. It is
// the announcing step's seam, and its contract is the opposite of the heartbeat publisher's: the
// heartbeat fan-out is fire-and-forget beside a durable write, while a publishing step must know
// that the broker took the event before it is recorded as done.
//
// One goroutine owns the session, so publishes are serialized and each confirmation belongs to the
// publish that is waiting for it. A lost session is a reconnect, never a process exit: the next
// announcement is published through the session that replaced it.
type Notifier struct {
	topology   Topology
	log        *slog.Logger
	supervisor *Supervisor
	// requests carries announcements from their callers to the publishing goroutine. It is
	// unbuffered on purpose: a caller waits for the broker's verdict anyway, and queueing
	// announcements ahead of a live session would only turn a broker outage into a backlog.
	requests chan announcement
}

// announcement is one pending publication: the event, the caller's context, and where the verdict
// is reported back.
type announcement struct {
	ctx    context.Context
	event  RollbackEvent
	result chan error
}

// NewNotifier returns a notifier publishing rollback announcements on the topology's events
// exchange. It must be started with Run before anything is announced: an announcement waits for
// the goroutine Run owns, and a caller whose context ends first is told so rather than left
// hanging.
func NewNotifier(url string, topology Topology, log *slog.Logger) *Notifier {
	return &Notifier{
		topology:   topology,
		log:        log,
		supervisor: NewSupervisor(url, topology, log),
		requests:   make(chan announcement),
	}
}

// Announce publishes one rollback announcement and blocks until the broker has taken it. A nack, a
// message no queue is bound to, and a session lost under the publish are all errors: the announcing
// step retries them, and a step is never recorded as complete on the strength of a publication the
// broker did not accept. An event the notifier refuses is reported without a publish at all.
func (n *Notifier) Announce(ctx context.Context, event RollbackEvent) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("rollback announcement: %w", err)
	}
	result := make(chan error, 1)
	select {
	case n.requests <- announcement{ctx: ctx, event: event, result: result}:
	case <-ctx.Done():
		return fmt.Errorf("announce rollback %s: %w", event.EventID, ctx.Err())
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("announce rollback %s: %w", event.EventID, ctx.Err())
	}
}

// Run publishes announcements until ctx is done, reconnecting through the supervisor whenever the
// session is lost.
func (n *Notifier) Run(ctx context.Context) error {
	return n.supervisor.Run(ctx, "rollback-notifier", n.serve)
}

// serve publishes announcements on one live session until ctx is done or the session drops. Every
// announcement is answered exactly once through its own result channel, including the ones that
// arrive while the session is going down: an unanswered caller would wait out its own deadline for
// no reason.
func (n *Notifier) serve(ctx context.Context, sess Session) error {
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
		case req := <-n.requests:
			key := RollbackRoutingKey(req.event.Phase)
			msg, err := req.event.Message(key)
			if err != nil {
				req.respond(err)
				continue
			}
			outcome, err := pub.Publish(req.ctx, EventsExchange, key, msg)
			req.respond(n.verdict(req.event, key, outcome, err))
			if outcome == publishLost && errors.Is(err, errSessionLost) {
				// The verdict never arrived, so the session can no longer be trusted to keep
				// confirmations aligned with publishes: the supervisor reconnects. A caller
				// whose own context ended is not a reason to drop a healthy session.
				return err
			}
		}
	}
}

// verdict maps what the broker reported for one publish onto the error the announcing step sees.
func (n *Notifier) verdict(event RollbackEvent, key string, outcome publishOutcome, err error) error {
	switch outcome {
	case publishConfirmed:
		return nil
	case publishNacked:
		return fmt.Errorf("broker rejected rollback announcement %s", event.EventID)
	case publishUnroutable:
		return fmt.Errorf("no queue is bound to %s", key)
	default:
		return fmt.Errorf("publish rollback announcement %s under %s: %w", event.EventID, key, err)
	}
}

// respond reports one verdict to the caller that is waiting for it. The result channel is buffered,
// so a caller that gave up — its context ended while the publish was in flight — never blocks the
// publishing goroutine.
func (a announcement) respond(err error) {
	a.result <- err
}
