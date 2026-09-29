package telemetry

import (
	"context"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// HeartbeatIngest accepts one heartbeat for durable storage. Its error decides whether the
// heartbeat was accepted at all. *Writer satisfies it.
type HeartbeatIngest interface {
	// Handle stores one accepted heartbeat with the device identity its registration
	// established.
	Handle(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record) error
}

// Fanout is the ordered composition of the two heartbeat sinks: the durable ingest write first,
// the broker publication second. It is what the agent hub is wired with, and it keeps the
// fan-out a derived copy of what storage accepted rather than a second source of truth.
type Fanout struct {
	ingest  HeartbeatIngest
	publish HeartbeatPublisher
}

// NewFanout returns a sink storing heartbeats through ingest and publishing the accepted ones
// through publish.
func NewFanout(ingest HeartbeatIngest, publish HeartbeatPublisher) *Fanout {
	return &Fanout{ingest: ingest, publish: publish}
}

// Handle stores one heartbeat and then publishes it. A heartbeat the ingest path refuses is
// never published, so every published event has a durable counterpart; a publication problem
// never fails the heartbeat, because storage — not the broker — is the source of truth. The
// ingest error is returned as it is: the writer already names the heartbeat it refused.
func (f *Fanout) Handle(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record) error {
	if err := f.ingest.Handle(ctx, hb, rec); err != nil {
		return err
	}
	f.publish.Handle(ctx, hb, rec)
	return nil
}
