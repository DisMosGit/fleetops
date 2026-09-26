package agentserver

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// sessionQueueBound bounds the per-session outbound queue: commands wait for queue space until
// the caller's deadline, never accumulate without limit.
const sessionQueueBound = 32

// Server is the control-plane side of AgentService: it accepts agent streams, completes the
// registration exchange, routes heartbeats to the hub, and lets the hub push commands back
// over the same stream. Report stays unimplemented until the command-result stage.
type Server struct {
	agentv1.UnimplementedAgentServiceServer

	hub *Hub
	log *slog.Logger
}

// NewServer returns an AgentService server backed by hub.
func NewServer(hub *Hub, log *slog.Logger) *Server {
	return &Server{hub: hub, log: log}
}

// Connect serves one agent stream end to end: registrations enroll devices, heartbeats route
// to the sink, and the session's writer carries responses and commands outbound. When the
// stream ends its devices stop being routable.
func (s *Server) Connect(stream agentv1.AgentService_ConnectServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	sess := &session{
		send:    make(chan *agentv1.ControlEnvelope, sessionQueueBound),
		devices: make(map[string]struct{}),
	}
	writerDone := make(chan error, 1)
	go func() { writerDone <- sess.writeLoop(ctx, stream) }()

	err := s.recvLoop(ctx, sess, stream)
	s.hub.unenroll(sess)
	cancel()
	if writeErr := <-writerDone; err == nil && writeErr != nil {
		err = writeErr
	}
	return err
}

// writeLoop owns the stream's Send calls: everything outbound — responses and commands —
// travels the bounded session queue. Its stop condition is ctx cancellation.
func (sess *session) writeLoop(ctx context.Context, stream agentv1.AgentService_ConnectServer) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case env := <-sess.send:
			if err := stream.Send(env); err != nil {
				return err
			}
		}
	}
}

// recvLoop validates and dispatches every inbound envelope, mapping unusable input to safe
// gRPC status errors that name no internals.
func (s *Server) recvLoop(ctx context.Context, sess *session, stream agentv1.AgentService_ConnectServer) error {
	for {
		env, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.handle(ctx, sess, env); err != nil {
			return err
		}
	}
}

// handle processes one agent envelope. Errors it returns fail the call with their status.
func (s *Server) handle(
	ctx context.Context,
	sess *session,
	env *agentv1.AgentEnvelope,
) error {
	switch payload := env.Payload.(type) {
	case *agentv1.AgentEnvelope_RegisterDevice:
		return s.register(ctx, sess, env.CorrelationId, payload.RegisterDevice)
	case *agentv1.AgentEnvelope_Heartbeat:
		return s.heartbeat(ctx, sess, payload.Heartbeat)
	case *agentv1.AgentEnvelope_FirmwareDownload:
		return status.Error(codes.Unimplemented, "firmware download is not served yet")
	case *agentv1.AgentEnvelope_UpdateStatus:
		return status.Error(codes.Unimplemented, "update status is not served yet")
	default:
		return status.Error(codes.InvalidArgument, "envelope carries no payload")
	}
}

// register answers one registration exchange and enrolls the device only when the request is
// complete and accepted.
func (s *Server) register(
	ctx context.Context,
	sess *session,
	correlationID string,
	req *agentv1.RegisterDeviceRequest,
) error {
	if correlationID == "" {
		return status.Error(codes.InvalidArgument, "registration without correlation id")
	}

	resp := &agentv1.RegisterDeviceResponse{DeviceId: req.GetDeviceId()}
	switch {
	case req.GetDeviceId() == "":
		resp.Reason = "device id required"
	case req.GetModel() == "":
		resp.Reason = "model required"
	case req.GetRegion() == "":
		resp.Reason = "region required"
	case req.GetCurrentFw() == "":
		resp.Reason = "current firmware required"
	default:
		resp.Accepted = true
		resp.Status = "online"
		s.hub.enroll(req.GetDeviceId(), sess)
	}

	return s.respond(ctx, sess, &agentv1.ControlEnvelope{
		CorrelationId: correlationID,
		Payload:       &agentv1.ControlEnvelope_RegisterDevice{RegisterDevice: resp},
	})
}

// heartbeat routes one heartbeat from an enrolled device; a heartbeat from a device this
// stream never enrolled is dropped, never routed.
func (s *Server) heartbeat(ctx context.Context, sess *session, hb *agentv1.Heartbeat) error {
	if hb.GetEventId() == "" || hb.GetDeviceId() == "" {
		return status.Error(codes.InvalidArgument, "heartbeat without event id or device id")
	}
	if !s.hub.enrolled(hb.GetDeviceId(), sess) {
		s.log.Warn("heartbeat from unenrolled device dropped",
			"device_id", hb.GetDeviceId(), "event_id", hb.GetEventId())
		return nil
	}
	if err := s.hub.route(ctx, hb); err != nil {
		// Heartbeats are fire-and-forget: a sink failure is logged at this boundary and the
		// stream stays up. Persistence and retry belong to the telemetry stage behind the
		// sink.
		s.log.Error("route heartbeat",
			"device_id", hb.GetDeviceId(), "event_id", hb.GetEventId(), "err", err)
	}
	return nil
}

// respond queues one correlated response onto the session's outbound queue.
func (s *Server) respond(ctx context.Context, sess *session, env *agentv1.ControlEnvelope) error {
	select {
	case sess.send <- env:
		return nil
	case <-ctx.Done():
		return status.Error(codes.Internal, "session congested")
	}
}
