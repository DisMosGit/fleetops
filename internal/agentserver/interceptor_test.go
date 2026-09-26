package agentserver

import (
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptorContainsPanics(t *testing.T) {
	t.Parallel()

	interceptor := Recovery(slog.New(slog.DiscardHandler))
	info := &grpc.StreamServerInfo{FullMethod: "/fleetops.agent.v1.AgentService/Connect"}

	boom := func(any, grpc.ServerStream) error { panic("boom") }
	err := interceptor(nil, nil, info, boom)
	if status.Code(err) != codes.Internal {
		t.Errorf("panicking handler surfaced %v, want Internal", err)
	}

	// The interceptor stays usable after the panic and passes ordinary outcomes through.
	ok := func(any, grpc.ServerStream) error { return nil }
	if err := interceptor(nil, nil, info, ok); err != nil {
		t.Errorf("healthy handler returned %v, want nil", err)
	}

	sentinel := errors.New("stream lost")
	passthrough := func(any, grpc.ServerStream) error { return sentinel }
	if err := interceptor(nil, nil, info, passthrough); !errors.Is(err, sentinel) {
		t.Errorf("handler error = %v, want it unchanged", err)
	}
}

func TestLoggingInterceptorPassesErrorsThrough(t *testing.T) {
	t.Parallel()

	interceptor := Logging(slog.New(slog.DiscardHandler))
	info := &grpc.StreamServerInfo{FullMethod: "/fleetops.agent.v1.AgentService/Connect"}

	sentinel := errors.New("stream lost")
	err := interceptor(nil, nil, info, func(any, grpc.ServerStream) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("handler error = %v, want it unchanged", err)
	}
	if err := interceptor(nil, nil, info, func(any, grpc.ServerStream) error { return nil }); err != nil {
		t.Errorf("healthy handler returned %v, want nil", err)
	}
}
