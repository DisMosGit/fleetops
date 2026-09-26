package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// DefaultPeriod is the heartbeat period of a fleet built without an explicit one.
const DefaultPeriod = 5 * time.Second

// Fleet is the simulated device population one emulator process runs.
type Fleet struct {
	devices []*Device
}

// FleetOptions configures a simulated fleet. The zero value is not valid: IDs and Source must
// be set, and Period must be positive.
type FleetOptions struct {
	// IDs mints every device's heartbeat event ids.
	IDs *IDGen
	// Source produces every device's simulated condition samples.
	Source Source
	// Period is the heartbeat period of every device.
	Period time.Duration
	// Now returns device heartbeat timestamps; nil selects time.Now.
	Now func() time.Time
}

// NewFleet builds n simulated devices. Device ids are "device-<index>"; models and regions are
// drawn from the emulator's fixed sets so the fleet spans several rollout target groups.
func NewFleet(n int, opts FleetOptions) (*Fleet, error) {
	if n <= 0 {
		return nil, fmt.Errorf("fleet size %d: must be positive", n)
	}
	if opts.IDs == nil {
		return nil, errors.New("fleet ids: required")
	}
	if opts.Source == nil {
		return nil, errors.New("fleet source: required")
	}
	if opts.Period <= 0 {
		return nil, fmt.Errorf("fleet period %s: must be positive", opts.Period)
	}

	models := []string{"oak-s3", "oak-s5", "birch-x1"}
	regions := []string{"eu-west", "us-east", "ap-south"}

	f := &Fleet{devices: make([]*Device, n)}
	for i := range f.devices {
		f.devices[i] = &Device{
			Identity: Identity{
				ID:       fmt.Sprintf("device-%d", i),
				Model:    models[i%len(models)],
				Region:   regions[i%len(regions)],
				Firmware: "1.0.0",
			},
			Period: opts.Period,
			Source: opts.Source,
			IDs:    opts.IDs,
			Now:    opts.Now,
		}
	}
	return f, nil
}

// Identities returns the identities of the fleet's devices, in fleet order.
func (f *Fleet) Identities() []Identity {
	ids := make([]Identity, len(f.devices))
	for i, d := range f.devices {
		ids[i] = d.snapshot()
	}
	return ids
}

// SetFirmware adopts version as the current firmware of the named device — the outcome of a
// successful firmware apply — so the device's subsequent heartbeats report it. An unknown
// device fails: a command for one is a mismatch the caller must see, not silently absorb.
func (f *Fleet) SetFirmware(deviceID, version string) error {
	for _, d := range f.devices {
		if d.Identity.ID == deviceID {
			d.SetFirmware(version)
			return nil
		}
	}
	return fmt.Errorf("set firmware of %s: unknown device", deviceID)
}

// Run emits heartbeats from every device into out until ctx is cancelled: one goroutine per
// device, each owning its ticker, all joined through an errgroup so cancellation stops the
// whole fleet and Run returns only when every device goroutine has returned.
func (f *Fleet) Run(ctx context.Context, out chan<- *agentv1.Heartbeat) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, d := range f.devices {
		g.Go(func() error {
			ticker := time.NewTicker(d.Period)
			defer ticker.Stop()
			return d.Run(gctx, ticker.C, out)
		})
	}
	return g.Wait()
}
