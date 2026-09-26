package agentserver

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
)

// fakeSink is a hand-written HeartbeatSink double recording what it receives.
type fakeSink struct {
	mu         sync.Mutex
	heartbeats []handled
	err        error
}

// handled is one routed heartbeat as the sink saw it: the message and the identity the device
// registered with.
type handled struct {
	hb  *agentv1.Heartbeat
	rec devices.Record
}

func (s *fakeSink) Handle(_ context.Context, hb *agentv1.Heartbeat, rec devices.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats = append(s.heartbeats, handled{hb: hb, rec: rec})
	return s.err
}

func (s *fakeSink) recorded() []handled {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]handled(nil), s.heartbeats...)
}

// fakeRegistry is a hand-written DeviceRegistry double recording upserts.
type fakeRegistry struct {
	mu      sync.Mutex
	records []devices.Record
	err     error
}

func (r *fakeRegistry) Upsert(_ context.Context, rec devices.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.records = append(r.records, rec)
	return nil
}

func (r *fakeRegistry) recorded() []devices.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]devices.Record(nil), r.records...)
}

// fakeSignaler is a hand-written DeviceSignaler double recording what it receives.
type fakeSignaler struct {
	mu         sync.Mutex
	heartbeats []signaled
	reports    []*agentv1.ReportRequest
	updates    []*agentv1.UpdateStatusRequest
	err        error
}

// signaled is one heartbeat signal as the device-workflow seam saw it: the message and the
// identity the device registered with.
type signaled struct {
	hb  *agentv1.Heartbeat
	rec devices.Record
}

func (s *fakeSignaler) SignalHeartbeat(_ context.Context, rec devices.Record, hb *agentv1.Heartbeat) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.heartbeats = append(s.heartbeats, signaled{hb: hb, rec: rec})
	return nil
}

func (s *fakeSignaler) SignalCommandResult(_ context.Context, res *agentv1.ReportRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.reports = append(s.reports, res)
	return nil
}

func (s *fakeSignaler) SignalUpdateStatus(_ context.Context, req *agentv1.UpdateStatusRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.updates = append(s.updates, req)
	return nil
}

func (s *fakeSignaler) signaledHeartbeats() []signaled {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signaled(nil), s.heartbeats...)
}

func (s *fakeSignaler) signaledResults() []*agentv1.ReportRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.ReportRequest(nil), s.reports...)
}

func (s *fakeSignaler) signaledUpdates() []*agentv1.UpdateStatusRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.UpdateStatusRequest(nil), s.updates...)
}

// unavailableFirmware is a hand-written FirmwareReader double with nothing stored: tests that
// do not exercise downloads pass it as the server's firmware seam.
type unavailableFirmware struct{}

// Open reports every firmware as not found.
func (unavailableFirmware) Open(context.Context, string) (firmware.Record, io.ReadCloser, error) {
	return firmware.Record{}, nil, fmt.Errorf("open firmware: %w", firmware.ErrNotFound)
}

// startServer serves hub over an in-memory listener with the production server options and
// returns a connection to it. Downloads have nothing to serve from; startServerWith wires a
// firmware seam.
func startServer(t *testing.T, hub *Hub, registry DeviceRegistry, signals DeviceSignaler) *grpc.ClientConn {
	t.Helper()
	return startServerWith(t, hub, registry, signals, unavailableFirmware{})
}

// startServerWith serves hub over an in-memory listener with the production server options and
// the given firmware seam, and returns a connection to it.
func startServerWith(
	t *testing.T,
	hub *Hub,
	registry DeviceRegistry,
	signals DeviceSignaler,
	firmware FirmwareReader,
) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(ServerOptions(slog.New(slog.DiscardHandler))...)
	agentv1.RegisterAgentServiceServer(server, NewServer(hub, registry, signals, firmware, slog.New(slog.DiscardHandler)))
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
	registry := &fakeRegistry{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, registry, &fakeSignaler{}))

	before := time.Now()
	reg := registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	after := time.Now()
	if !reg.GetAccepted() {
		t.Fatalf("registration rejected: %s", reg.GetReason())
	}
	if reg.GetDeviceId() != "device-0" || reg.GetStatus() != "online" {
		t.Errorf("response = %+v, want device-0 online", reg)
	}

	got := registry.recorded()
	if len(got) != 1 {
		t.Fatalf("registry recorded %d upserts, want 1", len(got))
	}
	rec := got[0]
	if rec.ID != "device-0" || rec.Model != "oak-s3" || rec.Region != "eu-west" || rec.CurrentFw != "1.0.0" {
		t.Errorf("registered record = %+v, want the submitted identity", rec)
	}
	if rec.Status != devices.StatusOnline {
		t.Errorf("registered status = %q, want %q", rec.Status, devices.StatusOnline)
	}
	if rec.LastSeen.Before(before) || rec.LastSeen.After(after) {
		t.Errorf("registered last seen = %v, want the acceptance time within [%v, %v]",
			rec.LastSeen, before, after)
	}
}

func TestRegistrationRejectedWritesNothing(t *testing.T) {
	t.Parallel()

	registry := &fakeRegistry{}
	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, registry, &fakeSignaler{}))

	reg := registerDevice(t, stream, "corr-1", "device-0", "", "eu-west", "1.0.0")
	if reg.GetAccepted() {
		t.Fatal("registration without model must be rejected")
	}
	if reg.GetReason() == "" {
		t.Error("rejected registration must carry a reason")
	}
	if got := registry.recorded(); len(got) != 0 {
		t.Errorf("registry recorded %+v, want no writes for a rejected registration", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("dispatch to a rejected device = %v, want ErrNotFound", err)
	}
}

func TestRegistrationRejectedWhenRegistryUnavailable(t *testing.T) {
	t.Parallel()

	registry := &fakeRegistry{err: errors.New("mongo is down")}
	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, registry, &fakeSignaler{}))

	reg := registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if reg.GetAccepted() {
		t.Fatal("registration must be rejected when the device record cannot be stored")
	}
	if reg.GetReason() == "" {
		t.Error("rejected registration must carry a reason")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hub.Send(ctx, &agentv1.Command{CommandId: "cmd-1", DeviceId: "device-0"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("dispatch to an unstored device = %v, want ErrNotFound", err)
	}
}

func TestHeartbeatRoutedUnchanged(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, &fakeSignaler{}))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")

	want := &agentv1.Heartbeat{
		EventId: "evt-1", DeviceId: "device-0", CurrentFw: "1.0.0",
		Status: "online", Cpu: 0.3, Mem: 0.4, Health: 0.9,
		Ts: timestamppb.New(time.Now()),
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
	if got[0].hb.GetEventId() != want.GetEventId() || got[0].hb.GetHealth() != want.GetHealth() ||
		got[0].hb.GetCurrentFw() != want.GetCurrentFw() {
		t.Errorf("routed heartbeat = %+v, want %+v", got[0].hb, want)
	}
	if got[0].rec.ID != "device-0" || got[0].rec.Model != "oak-s3" || got[0].rec.Region != "eu-west" {
		t.Errorf("routed identity = %+v, want the registered identity of device-0", got[0].rec)
	}
}

func TestHeartbeatWithoutMeasurementTimeNotPersisted(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	registry := &fakeRegistry{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, registry, &fakeSignaler{}))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")

	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-1", DeviceId: "device-0",
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("stream error = %v, want InvalidArgument", err)
	}
	if got := sink.recorded(); len(got) != 0 {
		t.Errorf("sink received %+v, want no heartbeat without a measurement time", got)
	}
	if got := registry.recorded(); len(got) != 1 {
		t.Errorf("registry recorded %d upserts, want only the registration write", len(got))
	}
}

func TestUnenrolledHeartbeatNotRouted(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, &fakeSignaler{}))

	// First a heartbeat from a device this stream never enrolled: dropped. Then one from an
	// enrolled device: routed. Only the latter may reach the sink.
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-x", DeviceId: "device-x", Ts: timestamppb.New(time.Now()),
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-1", DeviceId: "device-0", Ts: timestamppb.New(time.Now()),
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(sink.recorded()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := sink.recorded()
	if len(got) != 1 || got[0].hb.GetEventId() != "evt-1" {
		t.Errorf("sink received %+v, want only evt-1", got)
	}
}

func TestCommandDispatchTargetsOneStream(t *testing.T) {
	t.Parallel()

	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	conn := startServer(t, hub, &fakeRegistry{}, &fakeSignaler{})
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
		devices: make(map[string]devices.Record),
	}
	sess.send <- &agentv1.ControlEnvelope{} // fill the bounded queue: no writer drains it
	hub.enroll(devices.Record{ID: "device-0"}, sess)

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
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, &fakeSignaler{}))
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
		{
			name: "heartbeat without measurement time",
			env: &agentv1.AgentEnvelope{
				Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
					EventId: "evt-1", DeviceId: "device-0",
				}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
			stream := openStream(t, startServer(t, hub, &fakeRegistry{}, &fakeSignaler{}))

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

func TestHeartbeatsSignalDeviceWorkflow(t *testing.T) {
	t.Parallel()

	sink := &fakeSink{}
	signals := &fakeSignaler{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, signals))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")

	// One message, then its redelivery after a reconnect-style retry: the seam must see one
	// signal per received message, both carrying the same event id, and the sink must still
	// see both messages unchanged.
	hb := &agentv1.Heartbeat{
		EventId: "evt-1", DeviceId: "device-0", CurrentFw: "1.0.0",
		Ts: timestamppb.New(time.Now()),
	}
	for range 2 {
		if err := stream.Send(&agentv1.AgentEnvelope{
			Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: hb},
		}); err != nil {
			t.Fatalf("send heartbeat: %v", err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(signals.signaledHeartbeats()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := signals.signaledHeartbeats()
	if len(got) != 2 {
		t.Fatalf("workflow seam received %d heartbeat signals, want 2", len(got))
	}
	for i, g := range got {
		if g.hb.GetEventId() != "evt-1" {
			t.Errorf("signal %d event id = %q, want evt-1", i, g.hb.GetEventId())
		}
		if g.rec.ID != "device-0" || g.rec.Model != "oak-s3" {
			t.Errorf("signal %d identity = %+v, want the registered identity of device-0", i, g.rec)
		}
	}
	if len(sink.recorded()) != 2 {
		t.Errorf("sink received %d heartbeats, want 2 — the fan-out changed sink behavior",
			len(sink.recorded()))
	}
}

func TestUnenrolledHeartbeatSignalsNothing(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{}
	hub := NewHub(&fakeSink{}, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, signals))

	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-x", DeviceId: "device-x", Ts: timestamppb.New(time.Now()),
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")
	if err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
			EventId: "evt-1", DeviceId: "device-0", Ts: timestamppb.New(time.Now()),
		}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(signals.signaledHeartbeats()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := signals.signaledHeartbeats()
	if len(got) != 1 || got[0].hb.GetEventId() != "evt-1" {
		t.Errorf("workflow seam received %+v, want only evt-1", got)
	}
}

func TestSignalFailureDoesNotFailStream(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{err: errors.New("temporal is down")}
	sink := &fakeSink{}
	hub := NewHub(sink, slog.New(slog.DiscardHandler))
	stream := openStream(t, startServer(t, hub, &fakeRegistry{}, signals))
	registerDevice(t, stream, "corr-1", "device-0", "oak-s3", "eu-west", "1.0.0")

	// Heartbeats are fire-and-forget: a workflow seam failure is logged at the boundary and
	// the stream keeps serving, exactly like a sink failure.
	for _, id := range []string{"evt-1", "evt-2"} {
		if err := stream.Send(&agentv1.AgentEnvelope{
			Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: &agentv1.Heartbeat{
				EventId: id, DeviceId: "device-0", Ts: timestamppb.New(time.Now()),
			}},
		}); err != nil {
			t.Fatalf("send heartbeat %s after a signal failure: %v", id, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(sink.recorded()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(sink.recorded()) != 2 {
		t.Errorf("sink received %d heartbeats, want 2 — the stream must survive a signal failure",
			len(sink.recorded()))
	}
}
