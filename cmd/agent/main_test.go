package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The firmware every wiring test's control plane serves and the command that asks for it.
const (
	wiringFirmwareID = "fw-wiring-1.1.0"
	wiringVersion    = "1.1.0"
	wiringBinary     = "fleetops wiring-test firmware binary"
)

// heartbeatRecord is one heartbeat with the moment the control plane read it: the arrival time
// is what makes the emulator's period observable from outside the process.
type heartbeatRecord struct {
	heartbeat *agentv1.Heartbeat
	arrivedAt time.Time
}

// wiringServer is the control plane the emulator binary is wired against: a real AgentService
// on a real loopback listener. It accepts every registration, records the heartbeats, phases,
// and terminal results that reach it, and serves the one firmware binary — the observations the
// wiring tests assert on.
type wiringServer struct {
	agentv1.UnimplementedAgentServiceServer

	version  string
	binary   []byte
	checksum string
	commands chan *agentv1.Command

	// sendMu serializes the two goroutines that write to a stream — the receive loop answering
	// requests and the command forwarder — since a gRPC stream accepts one sender at a time.
	sendMu sync.Mutex

	mu            sync.Mutex
	regs          []*agentv1.RegisterDeviceRequest
	beats         map[string][]heartbeatRecord
	statuses      []*agentv1.UpdateStatusRequest
	results       []*agentv1.ReportRequest
	downloads     int
	streamsClosed int
}

// Connect serves one stream: registrations are accepted, heartbeats and update reports are
// recorded and acknowledged, and commands dispatched by the test are forwarded to the agent.
func (s *wiringServer) Connect(
	stream grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	go s.forwardCommands(ctx, stream)

	err := s.receive(stream)
	s.mu.Lock()
	s.streamsClosed++
	s.mu.Unlock()
	return err
}

// receive consumes one stream's inbound envelopes until it ends.
func (s *wiringServer) receive(
	stream grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
) error {
	for {
		env, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case env.GetRegisterDevice() != nil:
			reg := env.GetRegisterDevice()
			s.mu.Lock()
			s.regs = append(s.regs, reg)
			s.mu.Unlock()
			if err := s.send(stream, &agentv1.ControlEnvelope{
				CorrelationId: env.GetCorrelationId(),
				Payload: &agentv1.ControlEnvelope_RegisterDevice{
					RegisterDevice: &agentv1.RegisterDeviceResponse{
						Accepted: true,
						DeviceId: reg.GetDeviceId(),
						Status:   agent.StatusOnline,
					},
				},
			}); err != nil {
				return err
			}
		case env.GetHeartbeat() != nil:
			hb := env.GetHeartbeat()
			s.mu.Lock()
			s.beats[hb.GetDeviceId()] = append(s.beats[hb.GetDeviceId()], heartbeatRecord{
				heartbeat: hb,
				arrivedAt: time.Now(),
			})
			s.mu.Unlock()
		case env.GetUpdateStatus() != nil:
			status := env.GetUpdateStatus()
			s.mu.Lock()
			s.statuses = append(s.statuses, status)
			s.mu.Unlock()
			if err := s.send(stream, &agentv1.ControlEnvelope{
				CorrelationId: env.GetCorrelationId(),
				Payload: &agentv1.ControlEnvelope_UpdateStatus{
					UpdateStatus: &agentv1.UpdateStatusResponse{Accepted: true},
				},
			}); err != nil {
				return err
			}
		default:
			// These tests assert on the exchanges they drive; an envelope they do not drive
			// is not theirs to fail on.
		}
	}
}

// forwardCommands writes the commands the test dispatches onto the stream.
func (s *wiringServer) forwardCommands(
	ctx context.Context,
	stream grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-s.commands:
			err := s.send(stream, &agentv1.ControlEnvelope{
				Payload: &agentv1.ControlEnvelope_Command{Command: cmd},
			})
			if err != nil {
				return
			}
		}
	}
}

// send writes one envelope to the stream, serialized against the stream's other sender.
func (s *wiringServer) send(
	stream grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
	env *agentv1.ControlEnvelope,
) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return stream.Send(env)
}

// Report records one terminal command result and accepts it.
func (s *wiringServer) Report(
	_ context.Context,
	req *agentv1.ReportRequest,
) (*agentv1.ReportResponse, error) {
	s.mu.Lock()
	s.results = append(s.results, req)
	s.mu.Unlock()
	return &agentv1.ReportResponse{Accepted: true}, nil
}

// DownloadFirmware streams the configured binary: the metadata message first, the bytes as one
// chunk, and the end-of-transfer marker last.
func (s *wiringServer) DownloadFirmware(
	req *agentv1.FirmwareDownloadRequest,
	stream agentv1.AgentService_DownloadFirmwareServer,
) error {
	s.mu.Lock()
	s.downloads++
	s.mu.Unlock()

	firmwareID := req.GetFirmwareId()
	if err := stream.Send(&agentv1.FirmwareDownloadResponse{
		FirmwareId: firmwareID,
		Version:    s.version,
		Checksum:   s.checksum,
		TotalSize:  int64(len(s.binary)),
	}); err != nil {
		return err
	}
	if len(s.binary) > 0 {
		if err := stream.Send(&agentv1.FirmwareDownloadResponse{
			FirmwareId: firmwareID,
			Chunk:      s.binary,
			Offset:     0,
		}); err != nil {
			return err
		}
	}
	return stream.Send(&agentv1.FirmwareDownloadResponse{
		FirmwareId: firmwareID,
		Offset:     int64(len(s.binary)),
		Eof:        true,
	})
}

// registrations returns the registrations that arrived, in arrival order.
func (s *wiringServer) registrations() []*agentv1.RegisterDeviceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.regs)
}

// heartbeatsOf returns the heartbeats one device sent, in arrival order.
func (s *wiringServer) heartbeatsOf(deviceID string) []heartbeatRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.beats[deviceID])
}

// statusPhases returns the update phases that arrived, in arrival order.
func (s *wiringServer) statusPhases() []agentv1.UpdatePhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	phases := make([]agentv1.UpdatePhase, 0, len(s.statuses))
	for _, status := range s.statuses {
		phases = append(phases, status.GetPhase())
	}
	return phases
}

// commandResults returns the terminal command results that arrived, in arrival order.
func (s *wiringServer) commandResults() []*agentv1.ReportRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.results)
}

// closedStreams returns how many Connect streams have ended.
func (s *wiringServer) closedStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamsClosed
}

// dispatch queues one command for the connected agent.
func (s *wiringServer) dispatch(t *testing.T, cmd *agentv1.Command, budget time.Duration) {
	t.Helper()
	select {
	case s.commands <- cmd:
	case <-time.After(budget):
		t.Fatalf("the control plane could not hand %s to the agent within %s", cmd.GetCommandId(), budget)
	}
}

// startWiringServer serves a wiringServer on a loopback listener and returns it with the
// address the emulator dials the way it dials a real control plane.
func startWiringServer(t *testing.T) (*wiringServer, string) {
	t.Helper()

	binary := []byte(wiringBinary)
	srv := &wiringServer{
		version:  wiringVersion,
		binary:   binary,
		checksum: sha256Hex(binary),
		commands: make(chan *agentv1.Command, 8),
		beats:    make(map[string][]heartbeatRecord),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the control plane: %v", err)
	}
	server := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(server, srv)
	go func() {
		_ = server.Serve(lis) // ends at Stop
	}()
	t.Cleanup(server.Stop)

	return srv, lis.Addr().String()
}

// emulator is one run of the binary's entry point and the result of its shutdown.
type emulator struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

// startEmulator runs the emulator against cfg in the background. Its cleanup stops the
// emulator, so a failing test still takes the agent down before the control plane it talks to.
func startEmulator(t *testing.T, cfg config.Config, shutdownBudget time.Duration) *emulator {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	e := &emulator{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(e.done)
		err := run(ctx, cfg)
		e.mu.Lock()
		e.err = err
		e.mu.Unlock()
	}()
	t.Cleanup(func() {
		e.cancel()
		select {
		case <-e.done:
		case <-time.After(shutdownBudget):
			t.Error("the emulator did not stop within its shutdown budget")
		}
	})
	return e
}

// stopped cancels the emulator, waits for run to return, and reports how it ended.
func (e *emulator) stopped(t *testing.T, budget time.Duration) error {
	t.Helper()

	e.cancel()
	select {
	case <-e.done:
	case <-time.After(budget):
		t.Fatalf("run() did not return within %s of cancellation", budget)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// requireCleanStop cancels the emulator and fails unless run returned nil.
func requireCleanStop(t *testing.T, e *emulator, budget time.Duration) {
	t.Helper()
	if err := e.stopped(t, budget); err != nil {
		t.Errorf("run() = %v, want nil after cancellation", err)
	}
}

// newWiringConfig returns the shared configuration wired for one test's control plane, with the
// simulation settings that keep a firmware update's phases observable rather than slow.
func newWiringConfig(t *testing.T, addr string, fleetSize int) config.Config {
	t.Helper()

	cfg := config.Defaults()
	cfg.GRPC.ControlPlaneAddr = addr
	cfg.Simulation.FleetSize = fleetSize
	cfg.Simulation.ApplyDelay = config.Duration{Duration: time.Millisecond}
	cfg.Simulation.ApplySuccessRate = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the wiring test's configuration does not validate: %v", err)
	}
	return cfg
}

// waitFor polls cond until it holds or the budget runs out. It is lenient about ordering — the
// emulator's goroutines reach the control plane in whatever order the scheduler picks — and
// strict about what eventually arrived.
func waitFor(t *testing.T, what string, budget time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", budget, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestRunRegistersTheConfiguredFleetSize covers the binary's fleet-size wiring: the number of
// devices the configuration names is the number of devices the control plane sees, with the
// identities the fleet carries.
func TestRunRegistersTheConfiguredFleetSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
	}{
		{name: "one device", size: 1},
		{name: "a small fleet", size: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runCfg := testexec.Get(t)
			budget := runCfg.Wait(10 * time.Second)
			srv, addr := startWiringServer(t)
			e := startEmulator(t, newWiringConfig(t, addr, tc.size), budget)

			waitFor(t, "every device to register", budget, func() bool {
				return len(srv.registrations()) >= tc.size
			})
			regs := srv.registrations()
			if len(regs) != tc.size {
				t.Fatalf("the emulator registered %d devices, want the configured %d", len(regs), tc.size)
			}

			got := make([]string, 0, len(regs))
			for _, reg := range regs {
				got = append(got, reg.GetDeviceId())
				if reg.GetModel() == "" || reg.GetRegion() == "" || reg.GetCurrentFw() == "" {
					t.Errorf("device %q registered without its identity: %+v", reg.GetDeviceId(), reg)
				}
			}
			want := make([]string, 0, tc.size)
			for i := range tc.size {
				want = append(want, fmt.Sprintf("device-%d", i))
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("registered devices mismatch (-want +got):\n%s", diff)
			}

			requireCleanStop(t, e, budget)
		})
	}
}

// TestRunAnswersADispatchedCommandThroughTheRelay covers the relay and the staging directory:
// a dispatched StartUpdate is answered with the update's phases on the stream the client was
// bound to after construction, its terminal result travels as the unary Report under the
// command's idempotency key, the verified binary lands in the process's own temporary
// directory, and that directory leaves with the process.
func TestRunAnswersADispatchedCommandThroughTheRelay(t *testing.T) {
	t.Parallel()

	runCfg := testexec.Get(t)
	budget := runCfg.Wait(10 * time.Second)
	srv, addr := startWiringServer(t)
	e := startEmulator(t, newWiringConfig(t, addr, 1), budget)

	waitFor(t, "the device to register", budget, func() bool {
		return len(srv.registrations()) == 1
	})
	device := srv.registrations()[0].GetDeviceId()

	const commandID = "cmd-wiring-1"
	srv.dispatch(t, &agentv1.Command{
		CommandId: commandID,
		DeviceId:  device,
		Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
			FirmwareId: wiringFirmwareID,
			Version:    wiringVersion,
			Checksum:   sha256Hex([]byte(wiringBinary)),
		}},
	}, budget)

	// The relay is what carries progress: the handler reports through a client that did not
	// exist when the handler was built, so a phase reaching the control plane proves the bind.
	waitFor(t, "the update to complete", runCfg.Wait(20*time.Second), func() bool {
		return slices.Contains(srv.statusPhases(), agentv1.UpdatePhase_UPDATE_PHASE_COMPLETED)
	})
	if diff := cmp.Diff(
		[]agentv1.UpdatePhase{
			agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
			agentv1.UpdatePhase_UPDATE_PHASE_APPLYING,
			agentv1.UpdatePhase_UPDATE_PHASE_COMPLETED,
		},
		collapsePhases(srv.statusPhases()),
	); diff != "" {
		t.Errorf("update phases mismatch (-want +got):\n%s", diff)
	}

	// The terminal result follows the last phase rather than the phase standing for it: the handler
	// reports the completed phase first and concludes the command after, so the result is waited
	// for, not assumed to have arrived with it.
	waitFor(t, "the command's terminal result", budget, func() bool {
		return len(srv.commandResults()) > 0
	})
	results := srv.commandResults()
	if len(results) != 1 {
		t.Fatalf("the control plane received %d terminal results, want exactly 1", len(results))
	}
	result := results[0]
	if result.GetCommandId() != commandID ||
		result.GetDeviceId() != device ||
		result.GetIdempotencyKey() != commandID ||
		result.GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		t.Errorf("terminal result = %+v, want %s of %s succeeded under its idempotency key",
			result, commandID, device)
	}

	// The staging directory is discovered the way the binary names it, so the test observes the
	// directory the process really used rather than one it handed the emulator.
	dir, staged := awaitStagedFirmware(t, wiringFirmwareID, budget)
	if string(staged) != wiringBinary {
		t.Errorf("staged firmware = %q, want the served binary %q", staged, wiringBinary)
	}

	// The applied version is adopted by the simulated device, so the fleet reports it next.
	waitFor(t, "a heartbeat carrying the applied version", runCfg.Wait(20*time.Second), func() bool {
		for _, beat := range srv.heartbeatsOf(device) {
			if beat.heartbeat.GetCurrentFw() == wiringVersion {
				return true
			}
		}
		return false
	})

	requireCleanStop(t, e, budget)
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the staging directory %s survived shutdown: stat error = %v", dir, err)
	}
}

// TestRunHeartbeatsOnTheConfiguredPeriod covers the heartbeat-period wiring. The fleet's
// period is a deployment constant rather than a setting, and a fresh ticker fires its first
// tick one period in, so one heartbeat's arrival already says which period the binary wired.
func TestRunHeartbeatsOnTheConfiguredPeriod(t *testing.T) {
	t.Parallel()

	runCfg := testexec.Get(t)
	budget := runCfg.Wait(10 * time.Second)
	srv, addr := startWiringServer(t)
	started := time.Now()
	e := startEmulator(t, newWiringConfig(t, addr, 1), budget)

	waitFor(t, "the first heartbeat", runCfg.Wait(2*agent.DefaultPeriod+5*time.Second), func() bool {
		return len(srv.heartbeatsOf("device-0")) > 0
	})
	beat := srv.heartbeatsOf("device-0")[0]
	if beat.heartbeat.GetEventId() == "" || beat.heartbeat.GetStatus() == "" {
		t.Errorf("heartbeat = %+v, want an event id and a status", beat.heartbeat)
	}

	// The bounds are what make this a period rather than an arrival: an immediately emitted or
	// much faster burst misses the lower one, and a period longer than the wired one misses the
	// upper one, which is also what keeps a loaded machine from turning this into a flake.
	arrival := beat.arrivedAt.Sub(started)
	if arrival < agent.DefaultPeriod/2 || arrival > 2*agent.DefaultPeriod {
		t.Errorf("the first heartbeat arrived %s after start, want about %s: the fleet's period is not the wired one",
			arrival, agent.DefaultPeriod)
	}

	requireCleanStop(t, e, budget)
}

// TestRunShutsDownOnContextCancellation covers the binary's stop condition: cancelling the
// context returns run cleanly, ends the control plane's stream instead of abandoning it, and
// leaves no goroutine of the emulator behind.
func TestRunShutsDownOnContextCancellation(t *testing.T) {
	// Deliberately not parallel: the goroutine count is only meaningful while no other test is
	// running its own, and a test without t.Parallel() runs in the sequential phase.
	runCfg := testexec.Get(t)
	budget := runCfg.Wait(10 * time.Second)
	srv, addr := startWiringServer(t)
	e := startEmulator(t, newWiringConfig(t, addr, 3), budget)

	waitFor(t, "every device to register", budget, func() bool {
		return len(srv.registrations()) == 3
	})
	before := runtime.NumGoroutine()

	requireCleanStop(t, e, budget)

	// The stream ended on the control-plane side too: the client closed it rather than leaving
	// the server holding a live stream whose agent is gone.
	waitFor(t, "the control plane's stream to end", budget, func() bool {
		return srv.closedStreams() > 0
	})

	// A transport tear-down may hand a goroutine or two to the runtime's own bookkeeping, so
	// the count is allowed to settle slightly below the running emulator's rather than exactly
	// at it — a fleet that leaked a goroutine per device would miss this by far more.
	deadline := time.Now().Add(runCfg.Wait(5 * time.Second))
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			t.Errorf("goroutines after shutdown = %d, want about the %d before it: the emulator left one behind",
				runtime.NumGoroutine(), before)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// collapsePhases collapses consecutive duplicate phases, so the progress reports inside one
// phase do not read as a phase of their own.
func collapsePhases(phases []agentv1.UpdatePhase) []agentv1.UpdatePhase {
	collapsed := make([]agentv1.UpdatePhase, 0, len(phases))
	for _, phase := range phases {
		if len(collapsed) == 0 || collapsed[len(collapsed)-1] != phase {
			collapsed = append(collapsed, phase)
		}
	}
	return collapsed
}

// awaitStagedFirmware waits until the emulator staged the verified binary of firmwareID and
// returns the directory it staged it in with the bytes that landed there.
func awaitStagedFirmware(t *testing.T, firmwareID string, budget time.Duration) (string, []byte) {
	t.Helper()

	pattern := filepath.Join(os.TempDir(), "fleetops-firmware-*", firmwareID+".bin")
	deadline := time.Now().Add(budget)
	for {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("look for the staged firmware: %v", err)
		}
		if len(matches) > 0 {
			staged, err := os.ReadFile(matches[0])
			if err != nil {
				t.Fatalf("read the staged firmware %s: %v", matches[0], err)
			}
			return filepath.Dir(matches[0]), staged
		}
		if time.Now().After(deadline) {
			t.Fatalf("no firmware was staged under %s within %s", pattern, budget)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// sha256Hex returns the hex digest of b, the checksum a StartUpdate command carries and a
// transfer is verified against.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
