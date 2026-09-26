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
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial control plane %s: %w", addr, err)
	}
	return conn, nil
}
