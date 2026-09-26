package agentserver

import (
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// keepaliveMinTime is the shortest agent keepalive ping interval the server tolerates. It must
// stay at or below the agent's ping period (20s) or the agent gets disconnected for pinging
// too often.
const keepaliveMinTime = 10 * time.Second

// ServerOptions returns the gRPC server options the control plane installs: the stream
// interceptors in their documented order (recovery, then logging — metrics and tracing join at
// their stage) and the keepalive enforcement that accepts the agent's liveness pings.
func ServerOptions(log *slog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainStreamInterceptor(Recovery(log), Logging(log)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinTime,
			PermitWithoutStream: true,
		}),
	}
}

// Recovery wraps a stream handler with panic recovery: a panicking handler becomes a safe
// internal error for the agent and a logged incident for the operator, and the process keeps
// serving other streams.
func Recovery(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("recovered stream panic",
					"method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

// Logging logs every stream at its boundary: method, resulting status code, and duration.
func Logging(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		log.Info("agent stream finished",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration", time.Since(start),
		)
		return err
	}
}
