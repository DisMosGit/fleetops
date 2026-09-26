package agentserver

import (
	"context"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// DeviceSignaler delivers device events to the device's long-lived workflow. The seam is
// defined here at its consumer; internal/temporal.Signaler implements it over the Temporal
// client.
type DeviceSignaler interface {
	// SignalHeartbeat delivers one routed heartbeat for rec's device to its workflow.
	SignalHeartbeat(ctx context.Context, rec devices.Record, hb *agentv1.Heartbeat) error
	// SignalCommandResult delivers the terminal result of one command to its device's
	// workflow.
	SignalCommandResult(ctx context.Context, res *agentv1.ReportRequest) error
}
