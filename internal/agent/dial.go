package agent

import (
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Keepalive settings that turn a silently dead connection into a stream failure the reconnect
// cycle can act on, instead of an invisible stall in heartbeat delivery.
const (
	keepaliveTime    = 20 * time.Second
	keepaliveTimeout = 5 * time.Second
)

// Dial opens the control-plane connection the agent streams over. It carries the keepalive
// parameters that detect half-open connections; callers own the returned conn and close it.
func Dial(addr string) (*grpc.ClientConn, error) {
	return dial(addr, keepalive.ClientParameters{
		Time:                keepaliveTime,
		Timeout:             keepaliveTimeout,
		PermitWithoutStream: true,
	})
}

// dial opens addr with the given keepalive parameters, which is the whole behavior under test:
// a connection carrying keepalive pings turns a silently dead peer into a stream failure the
// client's reconnect cycle can act on. Dial passes the production parameters; the parameter is
// a seam so a test can watch that timeout happen — the transport refuses ping intervals shorter
// than ten seconds, so no test can wait out the production interval.
func dial(addr string, kp keepalive.ClientParameters) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kp),
	)
	if err != nil {
		return nil, fmt.Errorf("dial control plane %s: %w", addr, err)
	}
	return conn, nil
}
