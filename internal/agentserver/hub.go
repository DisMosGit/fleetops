package agentserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// ErrNotFound reports a command dispatched to a device that is unknown or not connected.
var ErrNotFound = errors.New("device not connected")

// DeviceRegistry persists the device records the stream establishes: one upsert per accepted
// registration exchange. *devices.Store satisfies it.
type DeviceRegistry interface {
	// Upsert registers or updates one device record in place.
	Upsert(ctx context.Context, rec devices.Record) error
}

// HeartbeatSink receives routed heartbeats on their way to the telemetry path. It is the seam the
// ingest writer, the stage-3 RabbitMQ publisher, and the stage-2 workflow signal implement;
// cmd/controlplane wires the ordered fan-out of the durable ingest write and the broker
// publication, so a heartbeat is stored before it is published.
type HeartbeatSink interface {
	// Handle consumes one heartbeat exactly as it arrived on the stream, plus the device
	// identity its registration established — the meta fields ingestion stores with it.
	Handle(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record) error
}

// session is one connected agent stream: the devices it has enrolled and the bounded queue its
// writer drains onto the stream.
type session struct {
	send chan *agentv1.ControlEnvelope
	// devices maps enrolled device ids to their registered identity; guarded by the hub's mutex.
	devices map[string]devices.Record
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

// route hands one heartbeat and its registered identity to the sink. Redeliveries arrive with
// their original event id, so idempotence is ingestion's contract, not the hub's.
func (h *Hub) route(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record) error {
	return h.sink.Handle(ctx, hb, rec)
}

// enroll records rec as living on sess. A device already enrolled elsewhere follows its most
// recent stream: a reconnecting agent must win over its own dying stream.
func (h *Hub) enroll(rec devices.Record, sess *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sess.devices[rec.ID] = rec
	h.devices[rec.ID] = sess
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

// identity reports the registered identity of deviceID on sess, and whether the device is
// currently enrolled there.
func (h *Hub) identity(deviceID string, sess *session) (devices.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.devices[deviceID] != sess {
		return devices.Record{}, false
	}
	rec, ok := sess.devices[deviceID]
	return rec, ok
}
