//go:build integration

package rollout

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestResolutionReadsTheView asserts the reason the view exists: once a selector's entry is built,
// a device change reaches a wave resolution through the change stream, and the resolution pays no
// query over devices to find it. The stack's command monitor is the witness — a resolution that
// recomputed the pool records a find over devices and fails here.
func TestResolutionReadsTheView(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	ctx := context.Background()
	view := NewPoolView(harness.DB.Collection("devices"), slog.New(slog.DiscardHandler))
	watchView(t, view)
	store := NewStoreWithPools(harness.DB, view)

	sel := Selector{Region: "eu-west", Model: "oak-resolution"}
	for _, id := range []string{"view-a", "view-b"} {
		seedDevice(t, harness, id, sel)
	}

	// The first resolution builds the selector's entry, which is the one query the view pays.
	first, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-view", WaveID: WaveID("ro-view", 0, 50),
		Percent: 50, Region: sel.Region, Model: sel.Model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() error = %v", err)
	}
	if want := []string{"view-a"}; !slices.Equal(first.DeviceIDs, want) {
		t.Fatalf("the first wave targets %v, want %v", first.DeviceIDs, want)
	}
	if got := harness.FindCommands("devices"); got != 1 {
		t.Errorf("building the view paid %d finds over devices, want 1", got)
	}

	// A device becomes eligible while the rollout is running: the view takes the change from
	// the stream rather than from a query.
	seedDevice(t, harness, "view-c", sel)
	waitFor(t, "the new device to reach the view", func() bool {
		return len(resolved(t, view, sel)) == 3
	})

	// The next wave reflects the change, and the resolution paid no query to see it.
	finds := harness.FindCommands("devices")
	last, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-view", WaveID: WaveID("ro-view", 1, 100),
		Percent: 100, Region: sel.Region, Model: sel.Model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the last wave error = %v", err)
	}
	want := []string{"view-b", "view-c"}
	if !slices.Equal(last.DeviceIDs, want) {
		t.Errorf("the last wave targets %v, want %v: the resolution must reflect the change",
			last.DeviceIDs, want)
	}
	if got := harness.FindCommands("devices"); got != finds {
		t.Errorf("the resolution paid %d finds over devices, want 0: wave resolution reads the view",
			got-finds)
	}

	// The wave already recorded keeps its membership while the view moves on: a device that
	// becomes eligible does not move the denominator a canary decision rests on.
	again, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-view", WaveID: WaveID("ro-view", 0, 50),
		Percent: 50, Region: sel.Region, Model: sel.Model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the recorded wave error = %v", err)
	}
	if !slices.Equal(again.DeviceIDs, first.DeviceIDs) {
		t.Errorf("the re-resolved wave targets %v, want the recorded %v",
			again.DeviceIDs, first.DeviceIDs)
	}

	// The monitor does see a find over devices: a store that recomputes the pool pays one per
	// resolution. That is the control which makes the assertion above a witness rather than an
	// accident of a monitor that records nothing.
	queryStore := NewStore(harness.DB)
	finds = harness.FindCommands("devices")
	if _, err := queryStore.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-view-query", WaveID: WaveID("ro-view-query", 0, 50),
		Percent: 50, Region: sel.Region, Model: sel.Model, StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("ResolveWave() through the query-backed store error = %v", err)
	}
	if got := harness.FindCommands("devices"); got != finds+1 {
		t.Errorf("a query-backed resolution paid %d finds over devices, want 1", got-finds)
	}
}

// TestResolutionOnAnEmptySelector asserts what a selector no device matches resolves to: an empty
// pool rather than an error, a wave recorded with an empty target set rather than no wave, and no
// query over devices once the view holds the entry — even for the empty selector.
func TestResolutionOnAnEmptySelector(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	ctx := context.Background()
	view := NewPoolView(harness.DB.Collection("devices"), slog.New(slog.DiscardHandler))
	watchView(t, view)
	store := NewStoreWithPools(harness.DB, view)

	req := ResolveRequest{
		RolloutID: "ro-empty", WaveID: WaveID("ro-empty", 0, 25),
		Percent: 25, Region: "ap-south", Model: "oak-empty", StartedAt: startedAt,
	}
	rec, err := store.ResolveWave(ctx, req)
	if err != nil {
		t.Fatalf("ResolveWave() on an empty selector error = %v", err)
	}
	if len(rec.DeviceIDs) != 0 {
		t.Errorf("the wave targets %v, want an empty target set", rec.DeviceIDs)
	}
	if rec.DeviceIDs == nil {
		t.Error("the wave's target set is nil, want an empty one")
	}
	if got := harness.FindCommands("devices"); got != 1 {
		t.Errorf("the empty selector's first resolution paid %d finds, want 1", got)
	}

	// The recorded document carries the empty membership as an array, not as a missing field.
	var stored WaveRecord
	if err := harness.DB.Collection("waves").
		FindOne(ctx, bson.D{{Key: "_id", Value: rec.ID}}).Decode(&stored); err != nil {
		t.Fatalf("read wave %s: %v", rec.ID, err)
	}
	if stored.DeviceIDs == nil {
		t.Error("stored device_ids = null, want an empty array")
	}
	assertEmptyArrays(t, ctx, harness.DB.Collection("waves"), rec.ID)

	// The empty selector's entry is the view's answer from then on: the rollout advances to its
	// next wave without paying a query over devices.
	finds := harness.FindCommands("devices")
	next, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-empty", WaveID: WaveID("ro-empty", 1, 100),
		Percent: 100, Region: req.Region, Model: req.Model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the next wave error = %v", err)
	}
	if len(next.DeviceIDs) != 0 {
		t.Errorf("the next wave targets %v, want an empty target set", next.DeviceIDs)
	}
	if got := harness.FindCommands("devices"); got != finds {
		t.Errorf("the next resolution paid %d finds, want 0", got-finds)
	}
}
