package agent

import (
	"context"
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

// scriptedAgentServer is a hand-written AgentService server behind a real gRPC transport: it
// accepts registrations, records heartbeats, and can kill a stream mid-traffic on demand.
type scriptedAgentServer struct {
	agentv1.UnimplementedAgentServiceServer

	mu            sync.Mutex
	heartbeats    []*agentv1.Heartbeat
	registrations int
	killAfter     int // heartbeats after which the first stream dies; 0 never kills
	received      chan struct{}
}

func (s *scriptedAgentServer) Connect(
	stream grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
) error {
	s.mu.Lock()
	kill := s.killAfter
	s.killAfter = 0
	s.mu.Unlock()

	seen := 0
	for {
		env, err := stream.Recv()
		if err != nil {
			return err
		}
		if reg := env.GetRegisterDevice(); reg != nil {
			s.mu.Lock()
			s.registrations++
			s.mu.Unlock()
			resp := &agentv1.ControlEnvelope{
				CorrelationId: env.CorrelationId,
				Payload: &agentv1.ControlEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceResponse{
					Accepted: true,
					DeviceId: reg.DeviceId,
					Status:   "online",
				}},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}
		if hb := env.GetHeartbeat(); hb != nil {
			s.mu.Lock()
			s.heartbeats = append(s.heartbeats, hb)
			s.mu.Unlock()
			select {
			case s.received <- struct{}{}:
			default:
			}
			seen++
			if kill > 0 && seen >= kill {
				return status.Error(codes.Internal, "stream killed by test")
			}
		}
	}
}

// recorded returns a snapshot of every heartbeat the server received.
func (s *scriptedAgentServer) recorded() []*agentv1.Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.Heartbeat(nil), s.heartbeats...)
}

func (s *scriptedAgentServer) registrationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registrations
}

// startTestServer serves scripted on an in-memory listener and returns the client conn.
func startTestServer(t *testing.T, scripted *scriptedAgentServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(server, scripted)
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

func TestNoMessageLossAcrossServerKilledStreams(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scripted := &scriptedAgentServer{killAfter: 3, received: make(chan struct{}, 16)}
	conn := startTestServer(t, scripted)
	c := newTestClient(t, agentv1.NewAgentServiceClient(conn), Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	heartbeats := make(chan *agentv1.Heartbeat, 8)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, heartbeats) }()

	// The first three heartbeats reach the server, which then kills its stream.
	for _, id := range []string{"evt-1", "evt-2", "evt-3"} {
		heartbeats <- &agentv1.Heartbeat{EventId: id, DeviceId: "device-0", Status: StatusOnline}
	}
	deadline := time.After(10 * time.Second)
	for len(scripted.recorded()) < 3 {
		select {
		case <-scripted.received:
		case <-deadline:
			t.Fatalf("server received %d heartbeats before the kill, want 3", len(scripted.recorded()))
		}
	}

	// Wait for the re-registration the reconnect performs, then produce more heartbeats:
	// all of them must land, with their original event ids, across the stream loss.
	for scripted.registrationCount() < 2 {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatal("agent never re-registered after the stream loss")
		}
	}
	for _, id := range []string{"evt-4", "evt-5", "evt-6"} {
		heartbeats <- &agentv1.Heartbeat{EventId: id, DeviceId: "device-0", Status: StatusOnline}
	}

	want := []string{"evt-1", "evt-2", "evt-3", "evt-4", "evt-5", "evt-6"}
	for len(scripted.recorded()) < len(want) {
		select {
		case <-scripted.received:
		case <-deadline:
			t.Fatalf("server received %d heartbeats, want %d", len(scripted.recorded()), len(want))
		}
	}

	got := map[string]int{}
	for _, hb := range scripted.recorded() {
		got[hb.GetEventId()]++
		if hb.GetDeviceId() != "device-0" {
			t.Errorf("heartbeat for device %q, want device-0", hb.GetDeviceId())
		}
	}
	for _, id := range want {
		if got[id] == 0 {
			t.Errorf("event %q never reached the server", id)
		}
	}
	if scripted.registrationCount() < 2 {
		t.Errorf("server saw %d registrations, want one per stream (at least 2)",
			scripted.registrationCount())
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}
