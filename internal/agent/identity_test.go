package agent

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// TestDeviceIdentityIsStableAcrossHeartbeats backs the agent-emulator scenario "Identity is
// stable across heartbeats": one device emits several heartbeats, and every one of them names
// the same device id, model, region, and firmware version. A heartbeat names only its device id
// and current firmware, so the model and region are checked through the registration that names
// the same device — the identity the control plane attributes those samples to.
func TestDeviceIdentityIsStableAcrossHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	fleet, err := NewFleet(1, testFleetOptions(t))
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	identities := fleet.Identities()
	if len(identities) != 1 {
		t.Fatalf("fleet holds %d identities, want the one device the scenario emits from", len(identities))
	}
	want := identities[0]

	// The device emits over the real stream, so the heartbeats observed are the ones the
	// control plane receives, registered under the identity NewFleet gave the device.
	scripted := &scriptedAgentServer{received: make(chan struct{}, 16)}
	conn := startTestServer(t, scripted)
	c := newTestClient(t, agentv1.NewAgentServiceClient(conn), Options{Devices: identities})

	heartbeats := make(chan *agentv1.Heartbeat, 16)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, heartbeats) }()
	fleetDone := make(chan error, 1)
	go func() { fleetDone <- fleet.Run(ctx, heartbeats) }()

	// Several emissions, so an identity that drifted between heartbeats would be visible.
	const wantHeartbeats = 5
	deadline := time.Now().Add(10 * time.Second)
	for len(scripted.recorded()) < wantHeartbeats && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("client Run returned %v, want nil", err)
	}
	if err := <-fleetDone; err != nil {
		t.Errorf("fleet Run returned %v, want nil", err)
	}
	if diff := cmp.Diff(identities, fleet.Identities()); diff != "" {
		t.Errorf("the device identity changed while it was emitting (-before +after):\n%s", diff)
	}

	hbs := scripted.recorded()
	if len(hbs) < wantHeartbeats {
		t.Fatalf("the server received %d heartbeats, want at least %d", len(hbs), wantHeartbeats)
	}
	regs := scripted.recordedRegistrations()
	if len(regs) == 0 {
		t.Fatal("the device was never registered, so its heartbeats have no identity to match")
	}
	registered := make(map[string]Identity, len(regs))
	for _, reg := range regs {
		got := Identity{
			ID:       reg.GetDeviceId(),
			Model:    reg.GetModel(),
			Region:   reg.GetRegion(),
			Firmware: reg.GetCurrentFw(),
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("registration identity mismatch (-want +got):\n%s", diff)
		}
		registered[reg.GetDeviceId()] = got
	}

	// Every heartbeat has to name that one identity: same device id, same model and region as
	// the device registered under, same firmware version.
	fields := []struct {
		name string
		got  func(*agentv1.Heartbeat) string
		want string
	}{
		{name: "device id", got: (*agentv1.Heartbeat).GetDeviceId, want: want.ID},
		{
			name: "model",
			got:  func(hb *agentv1.Heartbeat) string { return registered[hb.GetDeviceId()].Model },
			want: want.Model,
		},
		{
			name: "region",
			got:  func(hb *agentv1.Heartbeat) string { return registered[hb.GetDeviceId()].Region },
			want: want.Region,
		},
		{name: "firmware version", got: (*agentv1.Heartbeat).GetCurrentFw, want: want.Firmware},
	}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for i, hb := range hbs {
				if got := tc.got(hb); got != tc.want {
					t.Errorf("heartbeat %d (%s) names %s %q, want %q: the identity must not change between heartbeats",
						i, hb.GetEventId(), tc.name, got, tc.want)
				}
			}
		})
	}
}
