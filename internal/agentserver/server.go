package agentserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// sessionQueueBound bounds the per-session outbound queue: commands wait for queue space until
// the caller's deadline, never accumulate without limit.
const sessionQueueBound = 32

// Server is the control-plane side of AgentService: it accepts agent streams, completes the
// registration exchange and records its device in the registry, routes heartbeats to the hub
// and on to the device workflow, lets the hub push commands back over the same stream, accepts
// command results through Report and update progress through UpdateStatus, and serves firmware
// binaries over the DownloadFirmware stream.
type Server struct {
	agentv1.UnimplementedAgentServiceServer

	hub      *Hub
	registry DeviceRegistry
	signals  DeviceSignaler
	firmware FirmwareReader
	log      *slog.Logger
}

// NewServer returns an AgentService server backed by hub, recording accepted devices in
// registry, signaling the device workflow through signals, and serving firmware downloads
// from firmware.
func NewServer(
	hub *Hub,
	registry DeviceRegistry,
	signals DeviceSignaler,
	firmware FirmwareReader,
	log *slog.Logger,
) *Server {
	return &Server{hub: hub, registry: registry, signals: signals, firmware: firmware, log: log}
}

// Connect serves one agent stream end to end: registrations enroll devices, heartbeats route
// to the sink, and the session's writer carries responses and commands outbound. When the
// stream ends its devices stop being routable.
func (s *Server) Connect(stream agentv1.AgentService_ConnectServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	sess := &session{
		send:    make(chan *agentv1.ControlEnvelope, sessionQueueBound),
		devices: make(map[string]devices.Record),
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
		return status.Error(codes.Unimplemented, "firmware downloads use the DownloadFirmware method")
	case *agentv1.AgentEnvelope_UpdateStatus:
		return s.updateStatus(ctx, sess, env.CorrelationId, payload.UpdateStatus)
	default:
		return status.Error(codes.InvalidArgument, "envelope carries no payload")
	}
}

// register answers one registration exchange, recording the device in the registry and
// enrolling it only when the request is complete and the record could be persisted: a
// registration the registry cannot store is rejected and enrolls nothing.
func (s *Server) register(
	ctx context.Context,
	sess *session,
	correlationID string,
	req *agentv1.RegisterDeviceRequest,
) error {
	if correlationID == "" {
		return status.Error(codes.InvalidArgument, "registration without correlation id")
	}

	rec := devices.Record{
		ID:        req.GetDeviceId(),
		Model:     req.GetModel(),
		Region:    req.GetRegion(),
		CurrentFw: req.GetCurrentFw(),
		Status:    devices.StatusOnline,
		LastSeen:  time.Now(),
	}

	resp := &agentv1.RegisterDeviceResponse{DeviceId: rec.ID}
	switch {
	case rec.ID == "":
		resp.Reason = "device id required"
	case rec.Model == "":
		resp.Reason = "model required"
	case rec.Region == "":
		resp.Reason = "region required"
	case rec.CurrentFw == "":
		resp.Reason = "current firmware required"
	default:
		if err := s.registry.Upsert(ctx, rec); err != nil {
			// The device record is the source of truth: an acceptance that could not be
			// recorded is a rejection, with an operator-safe reason and nothing enrolled.
			s.log.Error("record device registration", "device_id", rec.ID, "err", err)
			resp.Reason = "device registry unavailable"
			break
		}
		resp.Accepted = true
		resp.Status = rec.Status
		s.hub.enroll(rec, sess)
	}

	return s.respond(ctx, sess, &agentv1.ControlEnvelope{
		CorrelationId: correlationID,
		Payload:       &agentv1.ControlEnvelope_RegisterDevice{RegisterDevice: resp},
	})
}

// heartbeat routes one heartbeat from an enrolled device, together with the identity it
// registered with; a heartbeat from a device this stream never enrolled is dropped, never
// routed.
func (s *Server) heartbeat(ctx context.Context, sess *session, hb *agentv1.Heartbeat) error {
	switch {
	case hb.GetEventId() == "" || hb.GetDeviceId() == "":
		return status.Error(codes.InvalidArgument, "heartbeat without event id or device id")
	case hb.GetTs() == nil:
		// A measurement time is what keys the telemetry document; without it there is
		// nothing truthful to store, and a zero ts would expire out of the TTL index at once.
		return status.Error(codes.InvalidArgument, "heartbeat without measurement time")
	}
	rec, ok := s.hub.identity(hb.GetDeviceId(), sess)
	if !ok {
		s.log.Warn("heartbeat from unenrolled device dropped",
			"device_id", hb.GetDeviceId(), "event_id", hb.GetEventId())
		return nil
	}
	if err := s.hub.route(ctx, hb, rec); err != nil {
		// Heartbeats are fire-and-forget: a sink failure is logged at this boundary and the
		// stream stays up. Retry and idempotence belong to the ingest pipeline behind the
		// sink.
		s.log.Error("route heartbeat",
			"device_id", hb.GetDeviceId(), "event_id", hb.GetEventId(), "err", err)
	}
	// The device workflow is signaled alongside the sink, once per received message: the
	// event id makes the redeliveries an agent may send idempotent downstream.
	if err := s.signals.SignalHeartbeat(ctx, rec, hb); err != nil {
		s.log.Error("signal device workflow",
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
