package telemetry

import (
	"context"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// verdict is the broker's scripted answer to one publish.
type verdict struct {
	// ack is whether the broker confirms the publish.
	ack bool
	// returned is whether the broker returns the message as unroutable.
	returned bool
	// err is a transport-level failure instead of a verdict.
	err error
}

// sentMessage is one recorded publish.
type sentMessage struct {
	exchange  string
	key       string
	mandatory bool
	msg       amqp.Publishing
}

// settlement is one recorded acknowledgement, rejection, or negative acknowledgement.
type settlement struct {
	kind string
	tag  uint64
}

// fakeSession is a scripted broker session: publishes are recorded and answered with scripted
// verdicts, settlements are recorded, deliveries are fed by the test, and the loss notification
// is triggered by the test. It stands in for the AMQP channel so deduplication, retry, and
// dead-letter decisions are unit-testable without a broker.
type fakeSession struct {
	mu          sync.Mutex
	verdicts    []verdict
	sent        []sentMessage
	settlements []settlement
	depths      map[string]int
	qos         int
	confirmErr  error
	consumeErr  error
	lost        chan struct{}
	closed      bool
	// deliveries is what Consume hands its caller.
	deliveries chan amqp.Delivery
	// publishNotify and returnNotify are the caller's notification channels, registered through
	// NotifyPublish and NotifyReturn.
	publishNotify chan amqp.Confirmation
	returnNotify  chan amqp.Return
}

// newFakeSession returns a live scripted session.
func newFakeSession() *fakeSession {
	return &fakeSession{
		depths:     make(map[string]int),
		lost:       make(chan struct{}),
		deliveries: make(chan amqp.Delivery, 16),
	}
}

// script sets the broker's answers to the next publishes, in order.
func (f *fakeSession) script(verdicts ...verdict) *fakeSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verdicts = append(f.verdicts, verdicts...)
	return f
}

// lose closes the session's loss notification.
func (f *fakeSession) lose() {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.lost:
	default:
		close(f.lost)
	}
}

// publishes returns the recorded publishes.
func (f *fakeSession) publishes() []sentMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMessage(nil), f.sent...)
}

// Lost reports the session's loss notification.
func (f *fakeSession) Lost() <-chan struct{} { return f.lost }

// isClosed reports whether the session was released, read under the lock so a test can observe it
// while a supervisor goroutine closes it.
func (f *fakeSession) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// Close records that the session was released.
func (f *fakeSession) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// ExchangeDeclare records nothing: the topology declaration has its own unit test.
func (f *fakeSession) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return nil
}

// QueueDeclare echoes the declared name.
func (f *fakeSession) QueueDeclare(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

// QueueBind records nothing.
func (f *fakeSession) QueueBind(string, string, string, bool, amqp.Table) error { return nil }

// QueueDeclarePassive reports the depth the test scripted for a queue.
func (f *fakeSession) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return amqp.Queue{Name: name, Messages: f.depths[name]}, nil
}

// Qos records the prefetch the consumer asked for.
func (f *fakeSession) Qos(prefetchCount, _ int, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.qos = prefetchCount
	return nil
}

// Consume hands the caller the delivery channel the test feeds.
func (f *fakeSession) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.deliveries, nil
}

// PublishWithContext records one publish and answers it with the next scripted verdict.
func (f *fakeSession) PublishWithContext(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) error {
	f.mu.Lock()
	f.sent = append(f.sent, sentMessage{exchange: exchange, key: key, mandatory: mandatory, msg: msg})
	tag := uint64(len(f.sent))
	answer := verdict{ack: true}
	if len(f.verdicts) > 0 {
		answer = f.verdicts[0]
		f.verdicts = f.verdicts[1:]
	}
	f.mu.Unlock()

	if answer.err != nil {
		return answer.err
	}
	f.mu.Lock()
	notify, returned := f.publishNotify, f.returnNotify
	f.mu.Unlock()
	if notify != nil {
		notify <- amqp.Confirmation{DeliveryTag: tag, Ack: answer.ack}
	}
	if answer.returned && returned != nil {
		returned <- amqp.Return{RoutingKey: key}
	}
	return nil
}

// Confirm records that the channel entered confirm mode.
func (f *fakeSession) Confirm(bool) error { return f.confirmErr }

// NotifyPublish registers the caller's confirmation channel, which recorded publishes are
// answered on.
func (f *fakeSession) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishNotify = confirm
	return confirm
}

// NotifyReturn registers the caller's returned-message channel.
func (f *fakeSession) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.returnNotify = c
	return c
}

// Ack records one acknowledgement.
func (f *fakeSession) Ack(tag uint64, _ bool) error { return f.settle("ack", tag) }

// Nack records one negative acknowledgement.
func (f *fakeSession) Nack(tag uint64, _, _ bool) error { return f.settle("nack", tag) }

// Reject records one rejection.
func (f *fakeSession) Reject(tag uint64, _ bool) error { return f.settle("reject", tag) }

// settle records one settlement.
func (f *fakeSession) settle(kind string, tag uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settlements = append(f.settlements, settlement{kind: kind, tag: tag})
	return nil
}

// fakeAcker settles deliveries the fake session handed out.
type fakeAcker struct {
	session *fakeSession
}

// Ack acknowledges one delivery.
func (a fakeAcker) Ack(tag uint64, multiple bool) error { return a.session.Ack(tag, multiple) }

// Nack negatively acknowledges one delivery.
func (a fakeAcker) Nack(tag uint64, multiple, requeue bool) error {
	return a.session.Nack(tag, multiple, requeue)
}

// Reject rejects one delivery.
func (a fakeAcker) Reject(tag uint64, requeue bool) error { return a.session.Reject(tag, requeue) }

// deliveryFor returns a delivery of one event on the fake session's channel.
func deliveryFor(t *testing.T, session *fakeSession, env Envelope, attempt int, routingKey string) amqp.Delivery {
	t.Helper()
	msg, err := env.Message(attempt, routingKey)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	return amqp.Delivery{
		Acknowledger: fakeAcker{session: session},
		Headers:      msg.Headers,
		ContentType:  msg.ContentType,
		DeliveryMode: msg.DeliveryMode,
		MessageId:    msg.MessageId,
		Type:         msg.Type,
		Timestamp:    msg.Timestamp,
		RoutingKey:   routingKey,
		DeliveryTag:  1,
		Body:         msg.Body,
	}
}
