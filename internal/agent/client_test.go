package agent

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// fakeStream is a hand-written Connect stream double: it records every envelope handed to
// Send, replays scripted receives, and can auto-answer registration requests like the server
// does. Failures are scriptable per call position.
type fakeStream struct {
	mu   sync.Mutex
	sent []*agentv1.AgentEnvelope

	sendFailAt int // 1-based Send call that fails; 0 never fails
	sendCalls  int
	sendErr    error

	ctx   context.Context
	recvs chan recvResult
	// auto, when set, answers each sent envelope; a nil result means no response.
	auto func(*agentv1.AgentEnvelope) *agentv1.ControlEnvelope
}

type recvResult struct {
	env *agentv1.ControlEnvelope
	err error
}

// newFakeStream returns a stream double whose Recv ends with ctx, like a real stream does.
func newFakeStream(ctx context.Context) *fakeStream {
	return &fakeStream{
		recvs:   make(chan recvResult, 16),
		sendErr: errors.New("stream broken"),
		ctx:     ctx,
	}
}

func (f *fakeStream) Send(env *agentv1.AgentEnvelope) error {
	f.mu.Lock()
	f.sent = append(f.sent, env)
	f.sendCalls++
	fail := f.sendFailAt != 0 && f.sendCalls >= f.sendFailAt
	f.mu.Unlock()
	if fail {
		return f.sendErr
	}
	if f.auto != nil {
		if resp := f.auto(env); resp != nil {
			f.recvs <- recvResult{env: resp}
		}
	}
	return nil
}

func (f *fakeStream) Recv() (*agentv1.ControlEnvelope, error) {
	select {
	case r, ok := <-f.recvs:
		if !ok {
			return nil, errors.New("stream closed")
		}
		return r.env, r.err
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

// sentEnvelopes returns a snapshot of the recorded sends.
func (f *fakeStream) sentEnvelopes() []*agentv1.AgentEnvelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*agentv1.AgentEnvelope(nil), f.sent...)
}

// grpc.ClientStream plumbing the generated stream interface embeds; unused by the client.
func (f *fakeStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeStream) Trailer() metadata.MD         { return nil }
func (f *fakeStream) CloseSend() error             { return nil }
func (f *fakeStream) Context() context.Context     { return context.Background() }
func (f *fakeStream) SendMsg(any) error            { return nil }
func (f *fakeStream) RecvMsg(any) error            { return nil }

// autoRegister returns a Send responder that accepts every registration like the control
// plane does, so sessions can pass their barrier without scripted receives.
func autoRegister() func(*agentv1.AgentEnvelope) *agentv1.ControlEnvelope {
	return func(env *agentv1.AgentEnvelope) *agentv1.ControlEnvelope {
		reg := env.GetRegisterDevice()
		if reg == nil {
			return nil
		}
		return &agentv1.ControlEnvelope{
			CorrelationId: env.CorrelationId,
			Payload: &agentv1.ControlEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceResponse{
				Accepted: true,
				DeviceId: reg.DeviceId,
				Status:   "online",
			}},
		}
	}
}

// fakeService is a hand-written AgentServiceClient double handing out scripted streams, one
// per Connect call. Each stream is bound to the ctx Connect receives — the session's RPC
// context — exactly like a real stream.
type fakeService struct {
	mu      sync.Mutex
	streams []*fakeStream
	// connect, when set, builds the stream for each call; a nil stream with nil error is a
	// connection failure.
	connect func(ctx context.Context, call int) (*fakeStream, error)
	calls   int
}

func (f *fakeService) Connect(ctx context.Context, _ ...grpc.CallOption) (
	grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope], error,
) {
	f.mu.Lock()
	call := f.calls
	f.calls++
	f.mu.Unlock()

	if f.connect == nil {
		return nil, errors.New("connection refused")
	}
	stream, err := f.connect(ctx, call)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, errors.New("connection refused")
	}
	f.mu.Lock()
	f.streams = append(f.streams, stream)
	f.mu.Unlock()
	return stream, nil
}

// Report is unreachable in this change: command results arrive with the firmware stage.
func (f *fakeService) Report(context.Context, *agentv1.ReportRequest, ...grpc.CallOption) (
	*agentv1.ReportResponse, error,
) {
	return &agentv1.ReportResponse{}, nil
}

// fakeHandler is a hand-written CommandHandler double recording what it receives.
type fakeHandler struct {
	mu       sync.Mutex
	commands []*agentv1.Command
}

func (h *fakeHandler) Handle(_ context.Context, cmd *agentv1.Command) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commands = append(h.commands, cmd)
	return nil
}

func (h *fakeHandler) received() []*agentv1.Command {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*agentv1.Command(nil), h.commands...)
}

// newTestClient wires a client around the given service and options with test defaults.
func newTestClient(t *testing.T, svc agentv1.AgentServiceClient, opts Options) *Client {
	t.Helper()
	ids, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	if opts.Devices == nil {
		opts.Devices = []Identity{{ID: "device-0", Model: "oak-s3", Region: "eu-west", Firmware: "1.0.0"}}
	}
	if opts.Handler == nil {
		opts.Handler = &fakeHandler{}
	}
	opts.IDs = ids
	opts.Log = slog.New(slog.DiscardHandler)
	opts.Rand = rand.New(unitSource{raw: 0})
	opts.Jitter = 0.2
	client, err := NewClient(svc, opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// waitSent waits until stream has recorded at least n envelopes.
func waitSent(t *testing.T, stream *fakeStream, n int) []*agentv1.AgentEnvelope {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sent := stream.sentEnvelopes()
		if len(sent) >= n {
			return sent
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("stream recorded %d envelopes, want at least %d", len(stream.sentEnvelopes()), n)
	return nil
}

func TestClientEnqueueBlocksAtBound(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{HeartbeatQueueBound: 2})
	ctx, cancel := context.WithCancel(context.Background())

	env := &agentv1.AgentEnvelope{Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{}}}
	for range 2 {
		if err := c.enqueue(ctx, c.heartbeats, env); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- c.enqueue(ctx, c.heartbeats, env) }()
	select {
	case err := <-done:
		t.Fatalf("enqueue at the bound returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("enqueue error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue did not stop after ctx cancellation")
	}
}

func TestWriteLoopKeepsFailedEnvelope(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{})
	first := &agentv1.AgentEnvelope{CorrelationId: "corr-1"}
	second := &agentv1.AgentEnvelope{CorrelationId: "corr-2"}
	c.reqs <- first
	c.reqs <- second

	broken := newFakeStream(context.Background())
	broken.sendFailAt = 2 // the first envelope goes out, the second fails
	ready := make(chan struct{})
	close(ready)
	if err := c.writeLoop(context.Background(), broken, ready); err == nil {
		t.Fatal("writeLoop returned nil on a failed send")
	}

	sent := broken.sentEnvelopes()
	if len(sent) != 2 || sent[0].CorrelationId != "corr-1" || sent[1].CorrelationId != "corr-2" {
		t.Errorf("sent envelopes = %d, want corr-1 then corr-2", len(sent))
	}
	if c.held == nil || c.held.CorrelationId != "corr-2" {
		t.Errorf("held envelope = %+v, want the failed corr-2", c.held)
	}

	// The next session's writer sends the held envelope before draining the queue.
	c.reqs <- &agentv1.AgentEnvelope{CorrelationId: "corr-3"}
	recovered := newFakeStream(context.Background())
	go func() { _ = c.writeLoop(context.Background(), recovered, ready) }()
	sent = waitSent(t, recovered, 2)
	if sent[0].CorrelationId != "corr-2" || sent[1].CorrelationId != "corr-3" {
		t.Errorf("recovered sends = [%s %s], want [corr-2 corr-3]",
			sent[0].CorrelationId, sent[1].CorrelationId)
	}
}

func TestRequestMatchesResponseByCorrelationID(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := make(chan *agentv1.ControlEnvelope, 1)
	go func() {
		resp, err := c.request(ctx, &agentv1.AgentEnvelope{CorrelationId: "corr-7"})
		if err != nil {
			t.Errorf("request: %v", err)
			close(got)
			return
		}
		got <- resp
	}()

	sent := <-c.reqs
	if sent.CorrelationId != "corr-7" {
		t.Fatalf("queued envelope correlation id = %q, want corr-7", sent.CorrelationId)
	}
	want := &agentv1.ControlEnvelope{CorrelationId: "corr-7"}
	c.deliver("corr-7", want)

	select {
	case resp := <-got:
		if resp != want {
			t.Errorf("response = %+v, want the delivered one", resp)
		}
	case <-ctx.Done():
		t.Fatal("request was not matched to its response")
	}
}

func TestResendUnansweredKeepsCorrelationID(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_, _ = c.request(ctx, &agentv1.AgentEnvelope{CorrelationId: "corr-9"})
	}()
	sent := <-c.reqs
	c.markSending(sent.CorrelationId, true) // the dead stream took it but never answered

	if err := c.resendUnanswered(ctx); err != nil {
		t.Fatalf("resendUnanswered: %v", err)
	}
	select {
	case resent := <-c.reqs:
		if resent.CorrelationId != "corr-9" {
			t.Errorf("resent correlation id = %q, want corr-9", resent.CorrelationId)
		}
	case <-ctx.Done():
		t.Fatal("unanswered request was not resent")
	}
}

func TestResendUnansweredSkipsRegistrations(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_, _ = c.request(ctx, &agentv1.AgentEnvelope{
			CorrelationId: "corr-1",
			Payload: &agentv1.AgentEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceRequest{
				DeviceId: "device-0",
			}},
		})
	}()
	sent := <-c.reqs
	c.markSending(sent.CorrelationId, true)

	if err := c.resendUnanswered(ctx); err != nil {
		t.Fatalf("resendUnanswered: %v", err)
	}
	select {
	case resent := <-c.reqs:
		t.Errorf("registration %q must not be resent: the barrier re-registers", resent.CorrelationId)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReadLoopDeliversCommands(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	c := newTestClient(t, &fakeService{}, Options{Handler: handler})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newFakeStream(ctx)
	stream.recvs <- recvResult{env: &agentv1.ControlEnvelope{
		Payload: &agentv1.ControlEnvelope_Command{Command: &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"}},
	}}

	done := make(chan error, 1)
	go func() { done <- c.readLoop(ctx, stream) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(handler.received()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := handler.received(); len(got) != 1 || got[0].GetCommandId() != "cmd-1" {
		t.Errorf("handler received %+v, want one command cmd-1", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("readLoop returned %v, want nil on cancellation", err)
	}
}
