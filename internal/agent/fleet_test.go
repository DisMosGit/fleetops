package agent

import (
	"context"
	"runtime"
	"testing"
	"time"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// testFleetOptions returns options with a scripted source and a seeded id nonce.
func testFleetOptions(t *testing.T) FleetOptions {
	t.Helper()
	ids, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	return FleetOptions{
		IDs:    ids,
		Source: &scriptedSource{samples: []Sample{{CPU: 0.3, Mem: 0.3, Health: 0.9, Status: StatusOnline}}},
		Period: time.Millisecond,
	}
}

func TestNewFleetValidation(t *testing.T) {
	t.Parallel()

	valid := testFleetOptions(t)
	tests := []struct {
		name    string
		n       int
		opts    FleetOptions
		wantErr bool
	}{
		{name: "valid fleet", n: 3, opts: valid, wantErr: false},
		{name: "zero size", n: 0, opts: valid, wantErr: true},
		{name: "negative size", n: -1, opts: valid, wantErr: true},
		{
			name: "missing ids", n: 3,
			opts:    FleetOptions{Source: valid.Source, Period: time.Second},
			wantErr: true,
		},
		{
			name: "missing source", n: 3,
			opts:    FleetOptions{IDs: valid.IDs, Period: time.Second},
			wantErr: true,
		},
		{
			name: "non-positive period", n: 3,
			opts:    FleetOptions{IDs: valid.IDs, Source: valid.Source},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewFleet(tc.n, tc.opts)
			if (err != nil) != tc.wantErr {
				t.Errorf("NewFleet error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestFleetIdentities(t *testing.T) {
	t.Parallel()

	fleet, err := NewFleet(6, testFleetOptions(t))
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	identities := fleet.Identities()
	if len(identities) != 6 {
		t.Fatalf("got %d identities, want 6", len(identities))
	}
	seenID := map[string]bool{}
	seenModel := map[string]bool{}
	seenRegion := map[string]bool{}
	for _, id := range identities {
		if id.ID == "" || id.Model == "" || id.Region == "" || id.Firmware == "" {
			t.Errorf("identity %+v has empty fields", id)
		}
		if seenID[id.ID] {
			t.Errorf("duplicate device id %q", id.ID)
		}
		seenID[id.ID] = true
		seenModel[id.Model] = true
		seenRegion[id.Region] = true
	}
	if len(seenModel) < 2 {
		t.Errorf("fleet must span more than one model, got %v", seenModel)
	}
	if len(seenRegion) < 2 {
		t.Errorf("fleet must span more than one region, got %v", seenRegion)
	}
}

func TestFleetRunLifecycle(t *testing.T) {
	t.Parallel()

	fleet, err := NewFleet(3, testFleetOptions(t))
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *agentv1.Heartbeat, 3)
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx, out) }()

	seen := map[string]bool{}
	for range 3 {
		select {
		case hb := <-out:
			seen[hb.DeviceId] = true
		case <-time.After(5 * time.Second):
			t.Fatal("fleet emitted no heartbeats")
		}
	}
	for id := range seen {
		if id != "device-0" && id != "device-1" && id != "device-2" {
			t.Errorf("unexpected emitting device %q", id)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fleet did not stop after ctx cancellation")
	}
}

// TestFleetEmitsOnTheConfiguredPeriod asserts the fleet's cadence is a real ticker at the
// configured period rather than a burst at startup: an operator's dashboards and a rollout's
// health windows both measure heartbeats that arrive on that cadence.
func TestFleetEmitsOnTheConfiguredPeriod(t *testing.T) {
	t.Parallel()

	const period = 40 * time.Millisecond
	opts := testFleetOptions(t)
	opts.Period = period
	fleet, err := NewFleet(1, opts)
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *agentv1.Heartbeat, 4)
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx, out) }()

	arrivals := make([]time.Time, 0, 4)
	for range 4 {
		select {
		case <-out:
			arrivals = append(arrivals, time.Now())
		case <-time.After(5 * time.Second):
			t.Fatal("the fleet emitted no heartbeat on its period")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the fleet did not stop after ctx cancellation")
	}

	for i := 1; i < len(arrivals); i++ {
		gap := arrivals[i].Sub(arrivals[i-1])
		// The lower bound is what makes this a cadence rather than a burst; the generous upper
		// bound keeps a loaded machine from turning the assertion into a flake.
		if gap < period/2 || gap > period*5 {
			t.Errorf("heartbeat %d arrived %v after the one before it, want about %v",
				i, gap, period)
		}
	}
}

// TestFleetScalesAndLeavesNoGoroutines runs the documented fleet sizes, waits until every device
// has reported, and cancels them: each must stop within the run's wait budget, and the process
// must be left with no goroutine watching a device. A run that cannot afford a size skips it
// saying so rather than simulating a smaller fleet than the test claims to verify.
func TestFleetScalesAndLeavesNoGoroutines(t *testing.T) {
	// Deliberately not parallel: the goroutine count is only meaningful while no other test is
	// running its own goroutines.

	tests := []struct {
		name string
		size int
	}{
		{name: "a hundred devices", size: 100},
		{name: "a thousand devices", size: 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testexec.Get(t)
			cfg.RequireFleetSize(t, tc.size)

			baseline := runtime.NumGoroutine()
			opts := testFleetOptions(t)
			fleet, err := NewFleet(tc.size, opts)
			if err != nil {
				t.Fatalf("NewFleet(%d): %v", tc.size, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := make(chan *agentv1.Heartbeat, tc.size)
			done := make(chan error, 1)
			go func() { done <- fleet.Run(ctx, out) }()

			started := time.Now()
			seen := make(map[string]bool, tc.size)
			deadline := time.Now().Add(cfg.Wait(30 * time.Second))
			for len(seen) < tc.size && time.Now().Before(deadline) {
				select {
				case hb := <-out:
					seen[hb.DeviceId] = true
				case <-time.After(10 * time.Millisecond):
				}
			}
			if len(seen) != tc.size {
				t.Fatalf("%d of %d devices reported within %v, want every device",
					len(seen), tc.size, time.Since(started))
			}

			cancel()
			stopped := time.Now()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned %v, want nil", err)
				}
			case <-time.After(cfg.Wait(30 * time.Second)):
				t.Fatal("the fleet did not stop after ctx cancellation")
			}
			t.Logf("%d devices reported within %v and stopped within %v",
				tc.size, stopped.Sub(started), time.Since(stopped))

			// The goroutines are joined by Run, so the count returns to what it was before the
			// fleet started. A little slack absorbs the runtime's own bookkeeping goroutines.
			settle := time.Now().Add(cfg.Wait(5 * time.Second))
			for {
				got := runtime.NumGoroutine()
				if got <= baseline+2 {
					return
				}
				if time.Now().After(settle) {
					t.Errorf("goroutines after the fleet stopped = %d, want at most %d",
						got, baseline+2)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
