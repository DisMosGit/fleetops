package agent

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
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
