package agent

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// Identity is the stable identity of one simulated device: exactly the fields the device
// registration exchange carries.
type Identity struct {
	// ID is the device id, unique within a fleet.
	ID string
	// Model is the device model, part of a rollout target group.
	Model string
	// Region is the device region, part of a rollout target group.
	Region string
	// Firmware is the firmware version the device currently runs (current_fw).
	Firmware string
}

// Device is one simulated IoT device: its identity plus the emission loop that turns the
// simulated condition into heartbeats.
type Device struct {
	// Identity is the device identity reported at registration and in every heartbeat.
	Identity Identity
	// Period is the interval between heartbeats.
	Period time.Duration
	// Source produces the device's simulated condition samples.
	Source Source
	// IDs mints the device's heartbeat event ids.
	IDs *IDGen
	// Now returns the heartbeat sample timestamp; nil selects time.Now.
	Now func() time.Time
}

// Run emits one heartbeat per tick received on ticks into out, until ctx is cancelled. The
// caller owns the ticker behind ticks and its stop condition; Run owns nothing but its own
// loop. A full out channel blocks emission — that backpressure is the fleet's queue bound at
// work, not a leak.
func (d *Device) Run(ctx context.Context, ticks <-chan time.Time, out chan<- *agentv1.Heartbeat) error {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
			sample := d.Source.Next(d.Identity.ID)
			hb := &agentv1.Heartbeat{
				EventId:   d.IDs.Next(d.Identity.ID),
				DeviceId:  d.Identity.ID,
				CurrentFw: d.Identity.Firmware,
				Status:    sample.Status,
				Ts:        timestamppb.New(now()),
				Cpu:       sample.CPU,
				Mem:       sample.Mem,
				Health:    sample.Health,
			}
			select {
			case out <- hb:
			case <-ctx.Done():
				return nil
			}
		}
	}
}
