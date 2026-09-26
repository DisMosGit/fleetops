package agentserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// ErrNotFound reports a command dispatched to a device that is unknown or not connected.
var ErrNotFound = errors.New("device not connected")

// HeartbeatSink receives routed heartbeats on their way to the telemetry path. It is the seam
// the stage-3 RabbitMQ publisher and the stage-2 workflow signal implement; today
// cmd/controlplane wires a minimal logging sink.
type HeartbeatSink interface {
	// Handle consumes one heartbeat exactly as it arrived on the stream.
	Handle(ctx context.Context, hb *agentv1.Heartbeat) error
}

// session is one connected agent stream: the devices it has enrolled and the bounded queue its
// writer drains onto the stream.
type session struct {
	send chan *agentv1.ControlEnvelope
	// devices is the set this stream has enrolled; guarded by the hub's mutex.
	devices map[string]struct{}
}

// Hub is the control-plane registry of enrolled devices: it routes their heartbeats to the
// sink and dispatches commands onto the right agent's stream. It is shared by the gRPC service
// and by whatever later stages send commands (Temporal activities).
type Hub struct {
	sink HeartbeatSink
	log  *slog.Logger

	mu      sync.Mutex
	devices map[string]*session
}

// NewHub returns a hub routing heartbeats to sink.
func NewHub(sink HeartbeatSink, log *slog.Logger) *Hub {
	return &Hub{
		sink:    sink,
		log:     log,
		devices: make(map[string]*session),
	}
}

// Send dispatches one command to its target device's live stream with its command id and
// device id. It fails with ErrNotFound when the device is unknown or its stream is gone, and
// with ctx's error when the session queue stays congested past the caller's deadline — a
// bounded per-session queue means backpressure, never unbounded buffering.
func (h *Hub) Send(ctx context.Context, cmd *agentv1.Command) error {
	if cmd.GetCommandId() == "" || cmd.GetDeviceId() == "" {
		return errors.New("dispatch command: command id and device id required")
	}

	h.mu.Lock()
	sess, ok := h.devices[cmd.GetDeviceId()]
	h.mu.Unlock()
	if !ok {
		return fmt.Errorf("dispatch command %s: %w", cmd.GetCommandId(), ErrNotFound)
	}

	env := &agentv1.ControlEnvelope{
		Payload: &agentv1.ControlEnvelope_Command{Command: cmd},
	}
	select {
	case sess.send <- env:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("dispatch command %s: %w", cmd.GetCommandId(), ctx.Err())
	}
}

// route hands one heartbeat to the sink. Redeliveries arrive with their original event id, so
// idempotence is ingestion's contract, not the hub's.
func (h *Hub) route(ctx context.Context, hb *agentv1.Heartbeat) error {
	return h.sink.Handle(ctx, hb)
}

// enroll records deviceID as living on sess. A device already enrolled elsewhere follows its
// most recent stream: a reconnecting agent must win over its own dying stream.
func (h *Hub) enroll(deviceID string, sess *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sess.devices[deviceID] = struct{}{}
	h.devices[deviceID] = sess
}

// unenroll drops every device of a finished stream, so later dispatches to them fail visibly
// instead of queueing into nothing.
func (h *Hub) unenroll(sess *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range sess.devices {
		if h.devices[id] == sess {
			delete(h.devices, id)
		}
	}
}

// enrolled reports whether deviceID currently lives on sess.
func (h *Hub) enrolled(deviceID string, sess *session) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.devices[deviceID] == sess
}
