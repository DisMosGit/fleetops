package agentserver

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// TestServerOptionsInstallsBothInterceptors drives a real server built from ServerOptions through
// both interceptors it chains, which is the property the two interceptor tests cannot state on
// their own: a panic is contained as an internal error (recovery), and an ordinary stream failure
// is logged with its method and code (logging). A server that installed only one of them would
// fail one half of this test.
func TestServerOptionsInstallsBothInterceptors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// connect is what the served implementation does when the stream opens.
		connect func() error
		// wantCode is the status the agent sees.
		wantCode codes.Code
		// wantLogged is what the server's log must carry afterwards.
		wantLogged string
	}{
		{
			name:       "a panicking handler is contained as an internal error",
			connect:    func() error { panic("boom") },
			wantCode:   codes.Internal,
			wantLogged: "recovered stream panic",
		},
		{
			name:       "an ordinary failure is passed through and logged",
			connect:    func() error { return status.Error(codes.Unavailable, "backend down") },
			wantCode:   codes.Unavailable,
			wantLogged: "agent stream finished",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logs := &syncBuffer{}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			server := grpc.NewServer(ServerOptions(slog.New(slog.NewTextHandler(logs, nil)))...)
			agentv1.RegisterAgentServiceServer(server, &flakyAgentService{connect: tc.connect})
			go func() { _ = server.Serve(listener) }()
			// Stop closes the listener it was given, so the test does not close it again.
			t.Cleanup(server.Stop)

			conn, err := grpc.NewClient(listener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("dial the server: %v", err)
			}
			t.Cleanup(func() {
				if err := conn.Close(); err != nil {
					t.Logf("close the client connection: %v", err)
				}
			})

			stream, err := agentv1.NewAgentServiceClient(conn).Connect(context.Background())
			if err == nil {
				// A stream call answers with its status on the first exchange, not at open.
				_, err = stream.Recv()
			}
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("Connect surfaced %v (%v), want %v", got, err, tc.wantCode)
			}

			logged := logs.String()
			if !strings.Contains(logged, tc.wantLogged) {
				t.Errorf("server log %q does not carry %q", logged, tc.wantLogged)
			}
			if !strings.Contains(logged, agentConnectMethod) {
				t.Errorf("server log %q does not name %s", logged, agentConnectMethod)
			}
		})
	}
}

// agentConnectMethod is the full method name both interceptors report.
const agentConnectMethod = "/fleetops.agent.v1.AgentService/Connect"

// flakyAgentService is the served AgentService: its Connect answers with whatever the test
// configured, so one server built from ServerOptions is driven through both interceptors.
type flakyAgentService struct {
	agentv1.UnimplementedAgentServiceServer
	connect func() error
}

// Connect answers with the configured outcome.
func (s *flakyAgentService) Connect(
	grpc.BidiStreamingServer[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
) error {
	return s.connect()
}

// syncBuffer is a log destination safe for the server goroutine and the test that reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p to the buffer.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns what has been logged so far.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
