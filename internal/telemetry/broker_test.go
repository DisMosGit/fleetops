package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// stubSession is a scripted Session: the AMQP surface is deliberately absent, so a supervised
// loop that touches the broker instead of stopping on Lost fails loudly with a nil panic.
type stubSession struct {
	Channel
	lost   chan struct{}
	closed bool
}

// newStubSession returns a live scripted session.
func newStubSession() *stubSession { return &stubSession{lost: make(chan struct{})} }

// Lost reports the session's loss notification.
func (s *stubSession) Lost() <-chan struct{} { return s.lost }

// Close records that the supervisor released the session.
func (s *stubSession) Close() error {
	s.closed = true
	return nil
}

// TestReconnectDelay pins the doubling, capped reconnect schedule.
func TestReconnectDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 250 * time.Millisecond},
		{attempt: 2, want: 500 * time.Millisecond},
		{attempt: 3, want: time.Second},
		{attempt: 7, want: 16 * time.Second},
		{attempt: 8, want: 30 * time.Second},
		{attempt: 20, want: 30 * time.Second},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt %d", tc.attempt), func(t *testing.T) {
			t.Parallel()

			if got := ReconnectDelay(tc.attempt); got != tc.want {
				t.Errorf("ReconnectDelay(%d) = %s, want %s", tc.attempt, got, tc.want)
			}
		})
	}
}

// TestSupervisorRunRedials drives the supervisor through one lost session: it must serve the
// first session, dial again after the loss, serve the second, release both, and stop cleanly on
// ctx.
func TestSupervisorRunRedials(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		opened int
	)
	sessions := []*stubSession{newStubSession(), newStubSession()}
	sup := &Supervisor{
		log: slog.New(slog.DiscardHandler),
		open: func(context.Context) (Session, error) {
			mu.Lock()
			defer mu.Unlock()
			if opened >= len(sessions) {
				return nil, errors.New("test opened more sessions than it scripted")
			}
			sess := sessions[opened]
			opened++
			return sess, nil
		},
	}

	served := make(chan struct{}, len(sessions))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx, "publisher", func(ctx context.Context, sess Session) error {
			served <- struct{}{}
			select {
			case <-sess.Lost():
				return errSessionLost
			case <-ctx.Done():
				return nil
			}
		})
	}()

	awaitSignal(t, served, "the first session")
	close(sessions[0].lost)
	awaitSignal(t, served, "the second session after the loss")

	mu.Lock()
	if opened != 2 {
		t.Errorf("supervisor opened %d sessions, want 2 — a lost channel must re-dial", opened)
	}
	mu.Unlock()
	if !sessions[0].closed {
		t.Error("the lost session was never released")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after ctx was cancelled")
	}
	if !sessions[1].closed {
		t.Error("the second session was never released")
	}
}

// TestSupervisorReturnsOnShutdown proves a supervisor with no reachable broker still stops when
// ctx is done instead of retrying forever.
func TestSupervisorReturnsOnShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	sup := &Supervisor{
		log: slog.New(slog.DiscardHandler),
		open: func(context.Context) (Session, error) {
			return nil, errors.New("broker unreachable")
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx, "sampler", func(context.Context, Session) error {
			t.Error("serve called without a session")
			return nil
		})
	}()
	// The first reconnect pauses before dialing again; cancelling then must end the run.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after ctx was cancelled")
	}
}

// TestSupervisorFailsOnTopologyConflict proves a refused declaration stops the process instead
// of being retried: no amount of reconnecting fixes a conflicting broker element.
func TestSupervisorFailsOnTopologyConflict(t *testing.T) {
	t.Parallel()

	conflict := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg"}
	sup := &Supervisor{
		log: slog.New(slog.DiscardHandler),
		open: func(context.Context) (Session, error) {
			return nil, fmt.Errorf("%w: declare queue %s: %w", errTopologyConflict, HeartbeatQueue, conflict)
		},
	}
	err := sup.Run(context.Background(), "consumer", func(context.Context, Session) error {
		t.Error("serve called for a conflicted topology")
		return nil
	})
	if !errors.Is(err, errTopologyConflict) {
		t.Fatalf("Run() error = %v, want a topology conflict", err)
	}
	if !strings.Contains(err.Error(), HeartbeatQueue) {
		t.Errorf("Run() error = %q, want it to name %q", err, HeartbeatQueue)
	}
}

// TestIsDeclarationRefusal separates a refused declaration from a transient outage.
func TestIsDeclarationRefusal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "precondition failed", err: &amqp.Error{Code: amqp.PreconditionFailed}, want: true},
		{name: "command invalid", err: &amqp.Error{Code: amqp.CommandInvalid}, want: true},
		{name: "connection refused", err: errors.New("dial tcp: connect: connection refused")},
		{name: "queue not found", err: &amqp.Error{Code: amqp.NotFound}},
		{name: "wrapped conflict", err: fmt.Errorf("declare queue q: %w", &amqp.Error{Code: amqp.PreconditionFailed}), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isDeclarationRefusal(tc.err); got != tc.want {
				t.Errorf("isDeclarationRefusal(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// awaitSignal waits for one signal on ch.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
