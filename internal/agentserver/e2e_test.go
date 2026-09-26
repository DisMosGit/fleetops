package agentserver_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
)

// recordingSink is a hand-written HeartbeatSink double recording routed heartbeats.
type recordingSink struct {
	mu         sync.Mutex
	heartbeats []*agentv1.Heartbeat
}

func (s *recordingSink) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats = append(s.heartbeats, hb)
	return nil
}

// nopRegistry is a hand-written DeviceRegistry double that stores nothing: the e2e path under
// test is stream routing, not persistence.
type nopRegistry struct{}

func (nopRegistry) Upsert(context.Context, devices.Record) error { return nil }

// nopSignaler is a hand-written DeviceSignaler double that swallows signals: the e2e path
// under test is stream routing, not the workflow.
type nopSignaler struct{}

func (nopSignaler) SignalHeartbeat(context.Context, devices.Record, *agentv1.Heartbeat) error {
	return nil
}

func (nopSignaler) SignalCommandResult(context.Context, *agentv1.ReportRequest) error { return nil }

func (nopSignaler) SignalUpdateStatus(context.Context, *agentv1.UpdateStatusRequest) error {
	return nil
}

// nopFirmware is a hand-written FirmwareReader double with nothing stored: the e2e path under
// test is stream routing, not firmware delivery.
type nopFirmware struct{}

// Open reports every firmware as not found.
func (nopFirmware) Open(context.Context, string) (firmware.Record, io.ReadCloser, error) {
	return firmware.Record{}, nil, fmt.Errorf("open firmware: %w", firmware.ErrNotFound)
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.heartbeats)
}

func (s *recordingSink) devices() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	got := map[string]int{}
	for _, hb := range s.heartbeats {
		got[hb.GetDeviceId()]++
	}
	return got
}

// recordingHandler is a hand-written CommandHandler double recording dispatched commands.
type recordingHandler struct {
	mu       sync.Mutex
	commands []*agentv1.Command
}

func (h *recordingHandler) Handle(_ context.Context, cmd *agentv1.Command) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commands = append(h.commands, cmd)
	return nil
}

func (h *recordingHandler) received() []*agentv1.Command {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*agentv1.Command(nil), h.commands...)
}

// sawCommand reports whether the handler has received the named command.
func sawCommand(h *recordingHandler, commandID string) bool {
	for _, cmd := range h.received() {
		if cmd.GetCommandId() == commandID {
			return true
		}
	}
	return false
}

// killAfter kills the first stream from the server side once it has carried n inbound
// messages: a test-only stream interceptor, so the production Server code is exercised whole
// and the failure still originates server-side.
func killAfter(n int) grpc.StreamServerInterceptor {
	var once sync.Once
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		wrapped := &killStream{ServerStream: ss, remaining: n}
		once.Do(func() { wrapped.kill = true })
		return handler(srv, wrapped)
	}
}

type killStream struct {
	grpc.ServerStream
	remaining int
	kill      bool
}

func (s *killStream) RecvMsg(m any) error {
	if s.kill {
		if s.remaining <= 0 {
			return status.Error(codes.Internal, "stream killed by test")
		}
		s.remaining--
	}
	return s.ServerStream.RecvMsg(m)
}

// TestFleetEndToEnd runs a real emulator fleet against a real in-process control plane: the
// fleet's heartbeats reach the sink through gRPC, a dispatched command reaches its device on
// the same stream, and a server-side stream kill is survived by reconnecting and
// re-registering.
func TestFleetEndToEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &recordingSink{}
	handler := &recordingHandler{}
	hub := agentserver.NewHub(sink, slog.New(slog.DiscardHandler))
	server := grpc.NewServer(
		append(agentserver.ServerOptions(slog.New(slog.DiscardHandler)), grpc.ChainStreamInterceptor(killAfter(4)))...,
	)
	agentv1.RegisterAgentServiceServer(server, agentserver.NewServer(hub, nopRegistry{}, nopSignaler{}, nopFirmware{}, slog.New(slog.DiscardHandler)))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = server.Serve(lis) // ends at Stop
	}()
	t.Cleanup(server.Stop)

	conn, err := agent.Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("agent.Dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
	})

	ids, err := agent.NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	fleet, err := agent.NewFleet(2, agent.FleetOptions{
		IDs:    ids,
		Source: agent.NewSimulation(rand.NewSource(1)),
		Period: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	client, err := agent.NewClient(agentv1.NewAgentServiceClient(conn), agent.Options{
		Devices:        fleet.Identities(),
		Handler:        handler,
		IDs:            ids,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	heartbeats := make(chan *agentv1.Heartbeat, 4)
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, heartbeats) }()
	fleetDone := make(chan error, 1)
	go func() { fleetDone <- fleet.Run(ctx, heartbeats) }()

	// Heartbeats from both devices must reach the sink despite the killed first stream.
	deadline := time.Now().Add(10 * time.Second)
	for {
		byDevice := sink.devices()
		if byDevice["device-0"] > 0 && byDevice["device-1"] > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink saw heartbeats %v, want both device-0 and device-1", byDevice)
		}
		time.Sleep(time.Millisecond)
	}

	// A dispatched command must reach its device over the live stream. Dispatch may hit the
	// reconnect window (device briefly unroutable) or lose the command with a dying stream,
	// so the test redelivers until it is observed — the same at-least-once stance the
	// command-result stage takes.
	for !sawCommand(handler, "cmd-e2e") {
		if time.Now().After(deadline) {
			t.Fatalf("command handler received %+v, want cmd-e2e", handler.received())
		}
		sendCtx, sendCancel := context.WithTimeout(ctx, time.Second)
		if err := hub.Send(sendCtx, &agentv1.Command{CommandId: "cmd-e2e", DeviceId: "device-0"}); err != nil {
			if !errors.Is(err, agentserver.ErrNotFound) && !errors.Is(err, context.DeadlineExceeded) {
				sendCancel()
				t.Fatalf("dispatch command: %v", err)
			}
		}
		sendCancel()
		time.Sleep(10 * time.Millisecond)
	}

	// The kill ended the first stream after at most two heartbeats; sustained delivery past
	// that proves the reconnect carried the fleet again.
	for sink.count() < 6 {
		if time.Now().After(deadline) {
			t.Fatalf("sink received %d heartbeats, want at least 6 across the reconnect", sink.count())
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("client Run returned %v, want nil", err)
	}
	if err := <-fleetDone; err != nil {
		t.Errorf("fleet Run returned %v, want nil", err)
	}
}
