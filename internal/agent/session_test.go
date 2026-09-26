package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// errStreamLost is the scripted stream failure the fakes die with.
var errStreamLost = errors.New("stream lost")

// diffDurations reports the cmp.Diff of two duration slices.
func diffDurations(want, got []time.Duration) string {
	return cmp.Diff(want, got)
}

// assertRegistrationOrder checks that on every recorded stream the registration envelope
// precedes every heartbeat, which is the register-before-resume barrier.
func assertRegistrationOrder(t *testing.T, streams []*fakeStream) {
	t.Helper()
	for i, stream := range streams {
		seenHeartbeat := false
		registrations := 0
		for _, env := range stream.sentEnvelopes() {
			switch {
			case env.GetRegisterDevice() != nil:
				if seenHeartbeat {
					t.Errorf("stream %d registered after a heartbeat was sent", i)
				}
				registrations++
			case env.GetHeartbeat() != nil:
				seenHeartbeat = true
			}
		}
		if registrations != 1 {
			t.Errorf("stream %d carried %d registrations, want 1", i, registrations)
		}
	}
}

func TestSessionRegistersBeforeHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streams := make(chan *fakeStream, 2)
	svc := &fakeService{connect: func(ctx context.Context, _ int) (*fakeStream, error) {
		s := newFakeStream(ctx)
		s.auto = autoRegister()
		streams <- s
		return s, nil
	}}
	c := newTestClient(t, svc, Options{})

	heartbeats := make(chan *agentv1.Heartbeat, 8)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, heartbeats) }()

	// First session: the heartbeat flows only after registration.
	first := <-streams
	heartbeats <- &agentv1.Heartbeat{EventId: "device-0-nonce-1", DeviceId: "device-0"}
	waitSent(t, first, 2)

	// Kill the stream; the reconnect must re-register before resuming.
	first.recvs <- recvResult{err: errStreamLost}
	second := <-streams
	heartbeats <- &agentv1.Heartbeat{EventId: "device-0-nonce-2", DeviceId: "device-0"}
	sent := waitSent(t, second, 2)

	if sent[1].GetHeartbeat().GetEventId() != "device-0-nonce-2" {
		t.Errorf("reconnected stream sent event id %q, want device-0-nonce-2",
			sent[1].GetHeartbeat().GetEventId())
	}
	assertRegistrationOrder(t, []*fakeStream{first, second})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}

func TestUnsentHeartbeatsSurviveReconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streams := make(chan *fakeStream, 2)
	firstStream := true
	svc := &fakeService{connect: func(ctx context.Context, _ int) (*fakeStream, error) {
		s := newFakeStream(ctx)
		s.auto = autoRegister()
		if firstStream {
			// The first stream dies on its third send: the second heartbeat is taken but not
			// written, the third is still queued. Both must survive the reconnect.
			s.sendFailAt = 3
			firstStream = false
		}
		streams <- s
		return s, nil
	}}
	c := newTestClient(t, svc, Options{HeartbeatQueueBound: 8})

	heartbeats := make(chan *agentv1.Heartbeat, 8)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, heartbeats) }()

	first := <-streams
	heartbeats <- &agentv1.Heartbeat{EventId: "device-0-nonce-1", DeviceId: "device-0"}
	heartbeats <- &agentv1.Heartbeat{EventId: "device-0-nonce-2", DeviceId: "device-0"}
	heartbeats <- &agentv1.Heartbeat{EventId: "device-0-nonce-3", DeviceId: "device-0"}

	second := <-streams
	sent := waitSent(t, second, 3) // registration + the two surviving heartbeats

	// The dead stream took one heartbeat onto the wire and lost the second at send.
	if got := len(first.sentEnvelopes()); got != 3 {
		t.Errorf("first stream recorded %d send attempts, want 3 (registration + two heartbeats)", got)
	}

	var ids []string
	for _, env := range sent {
		if hb := env.GetHeartbeat(); hb != nil {
			ids = append(ids, hb.GetEventId())
		}
	}
	want := []string{"device-0-nonce-2", "device-0-nonce-3"}
	if diff := diffStrings(want, ids); diff != "" {
		t.Errorf("delivered event ids mismatch (-want +got):\n%s", diff)
	}
	assertRegistrationOrder(t, []*fakeStream{first, second})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}

// recordingSleep is a hand-written Sleep double: it records the requested delays and cancels
// the run once the expected number has been observed.
type recordingSleep struct {
	mu     sync.Mutex
	delays []time.Duration
	cancel context.CancelFunc
	stopAt int
}

func (s *recordingSleep) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delays = append(s.delays, d)
	if len(s.delays) >= s.stopAt {
		s.cancel()
	}
	return nil
}

func (s *recordingSleep) recorded() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.delays...)
}

func TestReconnectDelaysGrowAndAreCapped(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &recordingSleep{cancel: cancel, stopAt: 5}

	svc := &fakeService{connect: func(ctx context.Context, _ int) (*fakeStream, error) {
		return nil, errStreamLost
	}}
	c := newTestClient(t, svc, Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Sleep:          rec.sleep,
	})

	if err := c.Run(ctx, make(chan *agentv1.Heartbeat)); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	// Jitter draws from unitSource(0): every delay is 80% of the doubling base.
	want := []time.Duration{
		800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond,
		4 * time.Second, 4 * time.Second,
	}
	if diff := diffDurations(want, rec.recorded()); diff != "" {
		t.Errorf("reconnect delay sequence mismatch (-want +got):\n%s", diff)
	}
}

func TestSuccessfulConnectionResetsBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &recordingSleep{cancel: cancel, stopAt: 3}

	var mu sync.Mutex
	calls := 0
	svc := &fakeService{connect: func(ctx context.Context, _ int) (*fakeStream, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 2 {
			// A stream that opens and registers, then dies: success must reset the backoff.
			s := newFakeStream(ctx)
			s.auto = autoRegister()
			s.recvs <- recvResult{err: errStreamLost}
			return s, nil
		}
		return nil, errStreamLost
	}}
	c := newTestClient(t, svc, Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Sleep:          rec.sleep,
	})

	if err := c.Run(ctx, make(chan *agentv1.Heartbeat)); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	want := []time.Duration{
		800 * time.Millisecond, // failed connect
		800 * time.Millisecond, // reset after the successful connect
		1600 * time.Millisecond,
	}
	if diff := diffDurations(want, rec.recorded()); diff != "" {
		t.Errorf("reconnect delay sequence mismatch (-want +got):\n%s", diff)
	}
}
