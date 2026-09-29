package telemetry

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Buffer sizes for the broker's asynchronous notifications. A single-component channel keeps
// one publish outstanding at a time, so these only absorb the notification the client library
// has already dispatched.
const (
	confirmBuffer = 16
	returnBuffer  = 16
)

// publishOutcome is what the broker reported for one publish.
type publishOutcome int

const (
	// publishConfirmed is a publish the broker accepted and routed.
	publishConfirmed publishOutcome = iota
	// publishNacked is a publish the broker refused.
	publishNacked
	// publishUnroutable is a publish no queue was bound to, so the broker returned it.
	publishUnroutable
	// publishLost is a publish whose verdict never arrived because the channel or its connection
	// died. The session must be torn down.
	publishLost
)

// String names the outcome for logs.
func (o publishOutcome) String() string {
	switch o {
	case publishConfirmed:
		return "confirmed"
	case publishNacked:
		return "rejected"
	case publishUnroutable:
		return "unroutable"
	case publishLost:
		return "lost"
	default:
		return "unknown"
	}
}

// sessionPublisher publishes on one live session channel and waits for the broker's verdict on
// every message, so nothing is counted as published that the broker did not take. Exactly one
// publish is outstanding at a time, and an owner that sees publishLost tears its session down,
// so the next confirmation always belongs to the publish that is waiting for it.
type sessionPublisher struct {
	session  Session
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
}

// newSessionPublisher puts a session's channel into publisher-confirm mode and subscribes to its
// confirmations and returned messages.
func newSessionPublisher(sess Session) (*sessionPublisher, error) {
	if err := sess.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	p := &sessionPublisher{
		session:  sess,
		confirms: make(chan amqp.Confirmation, confirmBuffer),
		returns:  make(chan amqp.Return, returnBuffer),
	}
	sess.NotifyPublish(p.confirms)
	sess.NotifyReturn(p.returns)
	return p, nil
}

// Publish sends one message with the mandatory flag set — an unroutable message comes back
// instead of vanishing — and waits for the broker's verdict. The returned error is non-nil only
// for publishLost, where the caller must stop using the session.
func (p *sessionPublisher) Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) (publishOutcome, error) {
	if err := p.session.PublishWithContext(ctx, exchange, key, true, false, msg); err != nil {
		return publishLost, fmt.Errorf("publish to %s under %s: %w: %w", exchange, key, errSessionLost, err)
	}
	select {
	case <-ctx.Done():
		return publishLost, fmt.Errorf("publish to %s under %s: %w", exchange, key, ctx.Err())
	case <-p.session.Lost():
		return publishLost, fmt.Errorf("publish to %s under %s: %w", exchange, key, errSessionLost)
	case confirmation, ok := <-p.confirms:
		if !ok {
			return publishLost, fmt.Errorf("publish to %s under %s: %w", exchange, key, errSessionLost)
		}
		if !confirmation.Ack {
			return publishNacked, nil
		}
		// The broker returns an unroutable message before it confirms it, so a returned message
		// is already waiting here when the confirmation arrives.
		select {
		case <-p.returns:
			return publishUnroutable, nil
		default:
			return publishConfirmed, nil
		}
	}
}
