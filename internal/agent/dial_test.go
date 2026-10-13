package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// keepalivePingFloor is the shortest keepalive ping interval a gRPC client transport accepts
// (grpc clamps anything shorter to ten seconds), so it is the shortest honest keepalive timeout
// a test can wait out.
const keepalivePingFloor = 10 * time.Second

// startTCPAgentServer serves scripted on a real loopback listener — a transport a blackhole can
// silently cut, which the in-memory listener cannot — and returns its address.
func startTCPAgentServer(t *testing.T, scripted *scriptedAgentServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(server, scripted)
	go func() { _ = server.Serve(lis) }() // ends at Stop
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

// blackholeProxy carries TCP between the agent and the control plane until the test cuts it:
// relaying stops, established connections stay open, and new dials are refused. That leaves the
// agent with exactly the failure keepalive exists for — the peer is gone, but no stream error
// and no RST ever arrives — while the reconnect's next connection attempt fails instead of
// hanging.
type blackholeProxy struct {
	target   string
	listener net.Listener
	stop     chan struct{}
	once     sync.Once

	mu    sync.Mutex
	conns []net.Conn
}

// startBlackholeProxy forwards TCP to target and returns the proxy, ready to cut.
func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	p := &blackholeProxy{target: target, listener: lis, stop: make(chan struct{})}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

// addr returns the address the agent dials.
func (p *blackholeProxy) addr() string { return p.listener.Addr().String() }

// blackhole stops relaying. Established connections are left open and silent: closing them
// would hand the agent a stream error of its own, which is not the case under test.
func (p *blackholeProxy) blackhole() {
	p.once.Do(func() {
		close(p.stop)
		_ = p.listener.Close()
	})
}

func (p *blackholeProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.track(client, upstream)
		go p.relay(client, upstream)
		go p.relay(upstream, client)
	}
}

// relay copies src to dst until the proxy is blackholed, then returns without closing either
// connection.
func (p *blackholeProxy) relay(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		// The deadline bounds how long a relay blocks, so a cut stops it promptly; a read that
		// times out still returns the bytes it did receive.
		_ = src.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			continue
		}
		return
	}
}

func (p *blackholeProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, conns...)
}

// close cuts the proxy and closes every connection it holds.
func (p *blackholeProxy) close() {
	p.blackhole()
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// connectCounter wraps the generated stub and counts connection attempts: an attempt after the
// cut is the reconnect cycle starting, which is what the scenario asserts.
type connectCounter struct {
	agentv1.AgentServiceClient

	mu       sync.Mutex
	attempts int
}

func (c *connectCounter) Connect(ctx context.Context, opts ...grpc.CallOption) (
	grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope], error,
) {
	c.mu.Lock()
	c.attempts++
	c.mu.Unlock()
	return c.AgentServiceClient.Connect(ctx, opts...)
}

// count returns how many connection attempts the client has made.
func (c *connectCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

// streamLogRecorder records the client's stream-lifecycle logs, so the test can tell a stream
// the client treated as failed from one that ended in an orderly way.
type streamLogRecorder struct {
	mu      sync.Mutex
	records []streamLogRecord
}

type streamLogRecord struct {
	message string
	err     string
}

func (r *streamLogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *streamLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	record := streamLogRecord{message: rec.Message}
	rec.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "err" {
			record.err = attr.Value.String()
			return false
		}
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, record)
	return nil
}

func (r *streamLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *streamLogRecorder) WithGroup(string) slog.Handler      { return r }

// keepaliveFailure returns the stream error the client logged carrying the keepalive timeout,
// if it logged one: that is the agent reporting the silent connection as a stream failure
// rather than an orderly end.
func (r *streamLogRecorder) keepaliveFailure() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.records {
		if strings.Contains(record.err, "keepalive") {
			return record.err, true
		}
	}
	return "", false
}

// logged reports what the client logged so far, for a failure message that says what was seen.
func (r *streamLogRecorder) logged() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]string, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, fmt.Sprintf("%s (err=%q)", record.message, record.err))
	}
	return records
}

// awaitTrue waits for cond to hold, failing with what it was waiting for once budget expires.
func awaitTrue(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", budget, what)
}

// TestKeepaliveTurnsASilentlyDeadConnectionIntoAReconnect backs the agent-stream-client
// scenario "A dead connection becomes a reconnect": the connection is blackholed, so no clean
// stream error reaches the agent, and the keepalive on the dialed connection is the only thing
// that can turn that silence into the stream failure its reconnect cycle follows.
func TestKeepaliveTurnsASilentlyDeadConnectionIntoAReconnect(t *testing.T) {
	t.Parallel()

	scripted := &scriptedAgentServer{received: make(chan struct{}, 16)}
	proxy := startBlackholeProxy(t, startTCPAgentServer(t, scripted))

	// The connection is opened through the agent's own dial path; only the keepalive timing is
	// the test's, because waiting out the production interval is not a test.
	conn, err := dial(proxy.addr(), keepalive.ClientParameters{
		Time:                keepalivePingFloor,
		Timeout:             100 * time.Millisecond,
		PermitWithoutStream: true,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
	})

	ids, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	svc := &connectCounter{AgentServiceClient: agentv1.NewAgentServiceClient(conn)}
	logs := &streamLogRecorder{}
	c, err := NewClient(svc, Options{
		Devices:        []Identity{{ID: "device-0", Model: "oak-s3", Region: "eu-west", Firmware: "1.0.0"}},
		Handler:        &fakeHandler{},
		IDs:            ids,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Log:            slog.New(logs),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heartbeats := make(chan *agentv1.Heartbeat, 4)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, heartbeats) }()

	// The stream has to be live — registered and carrying a heartbeat through the proxy —
	// before the cut, so what follows is a reconnect and not a first connection.
	heartbeats <- &agentv1.Heartbeat{EventId: "evt-1", DeviceId: "device-0", CurrentFw: "1.0.0"}
	awaitTrue(t, 10*time.Second, "the live stream to register and deliver a heartbeat", func() bool {
		return scripted.registrationCount() >= 1 && len(scripted.recorded()) >= 1
	})

	// The connection dies with no clean stream error: the proxy goes silent and stays open.
	attemptsBefore := svc.count()
	proxy.blackhole()

	// Nothing but the keepalive can notice, so a further connection attempt is the agent
	// treating the dead connection as a stream failure and starting its reconnect cycle.
	awaitTrue(t, keepalivePingFloor+30*time.Second, "the reconnect cycle to start", func() bool {
		return svc.count() > attemptsBefore
	})

	failure, ok := logs.keepaliveFailure()
	if !ok {
		t.Errorf("the client logged no keepalive stream failure (%v): the dead connection was not treated as a stream failure",
			logs.logged())
	} else {
		t.Logf("the blackholed connection surfaced as: %s", failure)
	}
	select {
	case err := <-done:
		t.Fatalf("client Run returned %v while the connection was only dead, want the reconnect cycle running", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil on shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after ctx cancellation")
	}
}
