package agentserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// fakeSink is a hand-written HeartbeatSink double recording what it receives.
type fakeSink struct {
	mu         sync.Mutex
	heartbeats []*agentv1.Heartbeat
	err        error
}

func (s *fakeSink) Handle(_ context.Context, hb *agentv1.Heartbeat) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats = append(s.heartbeats, hb)
	return s.err
}

func (s *fakeSink) recorded() []*agentv1.Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.Heartbeat(nil), s.heartbeats...)
}

// startServer serves hub over an in-memory listener with the production server options and
// returns a connection to it.
func startServer(t *testing.T, hub *Hub) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(ServerOptions(slog.New(slog.DiscardHandler))...)
	agentv1.RegisterAgentServiceServer(server, NewServer(hub, slog.New(slog.DiscardHandler)))
	go func() {
		_ = server.Serve(lis) // ends at Stop
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
	})
	return conn
}

// registerDevice runs one registration exchange and returns the response.
func registerDevice(
	t *testing.T,
	stream grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
	correlationID, deviceID, model, region, firmware string,
) *agentv1.RegisterDeviceResponse {
	t.Helper()
	req := &agentv1.AgentEnvelope{
		CorrelationId: correlationID,
		Payload: &agentv1.AgentEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceRequest{
			DeviceId:  deviceID,
			Model:     model,
			Region:    region,
			CurrentFw: firmware,
		}},
	}
	if err := stream.Send(req); err != nil {
		t.Fatalf("send registration: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive registration response: %v", err)
	}
	if resp.CorrelationId != correlationID {
		t.Fatalf("response correlation id = %q, want %q", resp.CorrelationId, correlationID)
	}
	return resp.GetRegisterDevice()
}

// openStream opens a Connect stream on conn.
func openStream(t *testing.T, conn *grpc.ClientConn) grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope] {
	t.Helper()
	stream, err := agentv1.NewAgentServiceClient(conn).Connect(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return stream
}

func TestRegistrationAccepted(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub))

	reg := registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if !reg.GetAccepted() {
		t.Fatalf("registration rejected: %s", reg.GetReason())
	}
	if reg.GetDeviceId() != "device-0" || reg.GetStatus() != "online" {
		t.Errorf("response = %+v, want device-0 online", reg)
	}
}

func TestRegistrationRejectedEnrollsNothing(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub))

	reg := registerDevice(t, stream, "corr-1", "device-0", "", "eu-west", "1.0.0")
	if reg.GetAccepted() {
		t.Fatal("registration without model must be rejected")
	}
	if reg.GetReason() == "" {
		t.Error("rejected registration must carry a reason")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("dispatch to a rejected device = %v, want ErrNotFound", err)
	}
}

func TestHeartbeatRoutedUnchanged(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")

	want := &agentv1.Heartbeat{
		EventId: "evt-1", DeviceId: "device-0", CurrentFw: "1.0.0",
		Status: "online", Cpu: 0.3, Mem: 0.4, Health: 0.9,
	}
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: want},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(sink.recorded()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := sink.recorded()
	if len(got) != 1 {
		t.Fatalf("sink received %d heartbeats, want 1", len(got))
	}
	if got[0].GetEventId() != want.GetEventId() || got[0].GetHealth() != want.GetHealth() ||
		got[0].GetCurrentFw() != want.GetCurrentFw() {
		t.Errorf("routed heartbeat = %+v, want %+v", got[0], want)
	}
}

func TestUnenrolledHeartbeatNotRouted(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub))

	// First a heartbeat from a device this stream never enrolled: dropped. Then one from an
	// enrolled device: routed. Only the latter may reach the sink.
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-x", DeviceId: "device-x",
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-1", DeviceId: "device-0",
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(sink.recorded()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := sink.recorded()
	if len(got) != 1 || got[0].GetEventId() != "evt-1" {
		t.Errorf("sink received %+v, want only evt-1", got)
	}
}

func TestCommandDispatchTargetsOneStream(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	conn := startServer(t, hub)
	streamA := openStream(t, conn)
	streamB := openStream(t, conn)
	registerDevice(t, streamA, "corr-1", "device-a", "oak-s3", "eu-west", "1.0.0")
	registerDevice(t, streamB, "corr-1", "device-b", "oak-s5", "us-east", "1.0.0")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := &agentv1.Command{
		CommandId: "cmd-1",
		DeviceId:  "device-a",
		Kind:      &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{FirmwareId: "fw-1"}},
	}
	if err := hub.Send(ctx, want); err != nil {
		t.Fatalf("Send: %v", err)
	}

	got, err := streamA.Recv()
	if err != nil {
		t.Fatalf("recv on target stream: %v", err)
	}
	cmd := got.GetCommand()
	if cmd == nil || cmd.GetCommandId() != "cmd-1" || cmd.GetDeviceId() != "device-a" {
		t.Errorf("target stream received %+v, want command cmd-1 for device-a", got)
	}

	// The other agent's stream must stay silent.
	other := make(chan *agentv1.ControlEnvelope, 1)
	go func() {
		env, err := streamB.Recv()
		if err == nil {
			other <- env
		}
	}()
	select {
	case env := <-other:
		t.Errorf("command leaked to another agent's stream: %+v", env)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCommandDispatchUnknownDevice(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "ghost"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Send error = %v, want ErrNotFound", err)
	}
	err = hub.Send(ctx, &agentv1.Command{DeviceId: "device-0"})
	if err == nil {
		t.Error("dispatch without command id must fail")
	}
}

func TestDispatchCongestedSessionTimesOut(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	sess := &session{
		send:    make(chan *agentv1.ControlEnvelope, 1),
		devices: make(map[string]struct{}),
	}
	sess.send <- &agentv1.ControlEnvelope{} // fill the bounded queue: no writer drains it
	hub.enroll("device-0", sess)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Send error = %v, want context.DeadlineExceeded", err)
	}
}

func TestStreamEndMakesDevicesUnroutable(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	// Drain the stream so the server observes the end and unenrolls.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected stream end after CloseSend")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"})
		if errors.Is(err, ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatch after disconnect = %v, want ErrNotFound", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMalformedEnvelopeMapsToInvalidArgument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  *agentv1.AgentEnvelope
	}{
		{
			name: "no payload",
			env:  &agentv1.AgentEnvelope{CorrelationId: "corr-1"},
		},
		{
			name: "registration without correlation id",
			env: &agentv1.AgentEnvelope{
				Payload: &agentv1.AgentEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceRequest{
					DeviceId: "device-0", Model: "oak-s3", Region: "eu-west", CurrentFw: "1.0.0",
				}},
			},
		},
		{
			name: "heartbeat without event id",
			env: &agentv1.AgentEnvelope{
				Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{DeviceId: "device-0"}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
			stream := openStream(t, startServer(t, hub))

			if err := stream.Send(tc.env); err != nil {
				t.Fatalf("send envelope: %v", err)
			}
			_, err := stream.Recv()
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("stream error = %v, want InvalidArgument", err)
			}
			if err != nil && status.Convert(err).Message() == "" {
				t.Error("status must carry an operator-safe message")
			}
		})
	}
}
