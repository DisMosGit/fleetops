//go:build integration

package rollout

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// TestWavesRollUpToTheirRollout backs the mongo-data-model scenario "Waves roll up to their
// rollout": listing the waves of one rollout returns exactly that rollout's waves, in wave order.
// Both halves are asserted — a wave of one rollout resolves against its own rollout's membership
// only, and the listing the index serves answers with that rollout's waves alone.
func TestWavesRollUpToTheirRollout(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	devices := harness.DB.Collection("devices")
	waves := harness.DB.Collection("waves")

	const (
		region = "eu-west"
		model  = "oak-rollup-selector"
	)
	// Four devices one pool: the pool both rollouts draw their waves from.
	pool := make([]string, 0, 4)
	docs := make([]any, 0, 4)
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("rollup-%d", i)
		pool = append(pool, id)
		docs = append(docs, bson.D{
			{Key: "_id", Value: id},
			{Key: "model", Value: model},
			{Key: "region", Value: region},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "online"},
			{Key: "last_heartbeat", Value: startedAt},
		})
	}
	if _, err := devices.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the pool: %v", err)
	}

	// Another rollout records a wave over the same pool first. Nothing of it may reach this
	// rollout's own membership.
	other, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-rollup-other", WaveID: WaveID("ro-rollup-other", 0, 100),
		Percent: 100, Region: region, Model: model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the other rollout error = %v", err)
	}
	if want := pool; !slices.Equal(other.DeviceIDs, want) {
		t.Fatalf("the other rollout's wave targets %v, want the whole pool %v", other.DeviceIDs, want)
	}

	// This rollout's waves resolve against its own recorded waves only: had the resolution listed
	// the other rollout's waves too, the pool would already be fully targeted and this canary
	// share would resolve to nobody.
	first, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-rollup", WaveID: WaveID("ro-rollup", 0, 25),
		Percent: 25, Region: region, Model: model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the first wave error = %v", err)
	}
	if want := pool[:1]; !slices.Equal(first.DeviceIDs, want) {
		t.Errorf("the first wave targets %v, want %v", first.DeviceIDs, want)
	}
	last, err := store.ResolveWave(ctx, ResolveRequest{
		RolloutID: "ro-rollup", WaveID: WaveID("ro-rollup", 1, 100),
		Percent: 100, Region: region, Model: model, StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("ResolveWave() of the last wave error = %v", err)
	}
	if want := pool[1:]; !slices.Equal(last.DeviceIDs, want) {
		t.Errorf("the last wave targets %v, want %v", last.DeviceIDs, want)
	}

	// A consumer lists the waves of this rollout: exactly its waves, in wave order.
	filter := bson.D{{Key: "rollout_id", Value: "ro-rollup"}}
	sort := bson.D{{Key: "rollout_id", Value: 1}, {Key: "percent", Value: 1}}
	cursor, err := waves.Find(ctx, filter, options.Find().SetSort(sort))
	if err != nil {
		t.Fatalf("list the rollout's waves: %v", err)
	}
	var listed []WaveRecord
	if err := cursor.All(ctx, &listed); err != nil {
		t.Fatalf("decode the rollout's waves: %v", err)
	}
	gotIDs := make([]string, 0, len(listed))
	gotPercents := make([]int, 0, len(listed))
	for _, wave := range listed {
		gotIDs = append(gotIDs, wave.ID)
		gotPercents = append(gotPercents, wave.Percent)
	}
	if want := []string{first.ID, last.ID}; !slices.Equal(gotIDs, want) {
		t.Errorf("the rollout's waves = %v, want %v", gotIDs, want)
	}
	if want := []int{25, 100}; !slices.Equal(gotPercents, want) {
		t.Errorf("the rollout's waves carry percents %v, want the wave order %v", gotPercents, want)
	}
	total, err := waves.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("count the recorded waves: %v", err)
	}
	if want := int64(3); total != want {
		t.Errorf("the collection holds %d waves, want the 3 both rollouts recorded", total)
	}

	// The listing is the access pattern the waves index exists for: served by {rollout_id,
	// percent}, which also supplies the order instead of sorting in memory.
	plan := mongotest.ExplainFind(t, harness.DB, "waves", filter, sort)
	if !plan.Uses("idx_rollout_percent") {
		t.Errorf("listing a rollout's waves: plan = %+v, want idx_rollout_percent", plan)
	}
	if plan.InMemorySort {
		t.Errorf("listing a rollout's waves sorted in memory: plan = %+v, want the index order", plan)
	}
}
