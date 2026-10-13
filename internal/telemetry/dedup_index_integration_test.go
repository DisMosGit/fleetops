//go:build integration

package telemetry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// dedupCrowd is how many other claims the lookups are planned among, so the planner sees a
// populated ledger rather than an empty one.
const dedupCrowd = 30

// TestDedupLookupIsServedByAnIndex backs the mongo-data-model scenario "Dedup lookup is served by
// an index": a consumer checking whether it has already processed an event, and an operator
// inspecting the events of one device, are both served by processed_events indexes rather than by
// a collection scan.
func TestDedupLookupIsServedByAnIndex(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	ctx := context.Background()
	claims := harness.DB.Collection("processed_events")
	ledger := NewLedger(claims)
	claimedAt := time.Now().UTC().Truncate(time.Millisecond)

	// The documents come from the ledger's own claim path, so the lookups below run over the
	// shape the consumer writes rather than over hand-built documents.
	type processed struct {
		consumer string
		event    string
		device   string
	}
	written := make([]processed, 0, dedupCrowd+3)
	for i := range dedupCrowd {
		written = append(written, processed{
			consumer: "fleetops.heartbeat.analytics",
			event:    fmt.Sprintf("ev-crowd-%02d", i),
			device:   fmt.Sprintf("dev-crowd-%02d", i),
		})
	}
	written = append(written,
		processed{"fleetops.heartbeat.analytics", "ev-1", "dev-1"},
		processed{"fleetops.heartbeat.analytics", "ev-2", "dev-1"},
		processed{"fleetops.heartbeat.alerting", "ev-1", "dev-2"},
	)
	for _, claim := range written {
		state, err := ledger.Claim(ctx, claim.consumer, claim.event, claim.device, claimedAt)
		if err != nil {
			t.Fatalf("Claim(%s, %s) error = %v", claim.consumer, claim.event, err)
		}
		if state != ClaimNew {
			t.Fatalf("Claim(%s, %s) = %s, want a new claim", claim.consumer, claim.event, state)
		}
	}

	t.Run("the consumer's processed check is served by the pair index", func(t *testing.T) {
		filter := bson.D{
			{Key: "consumer", Value: "fleetops.heartbeat.analytics"},
			{Key: "event_id", Value: "ev-2"},
		}
		plan := mongotest.ExplainFind(t, harness.DB, "processed_events", filter, nil)
		if !plan.Uses("idx_consumer_event") {
			t.Errorf("checking whether an event was processed: plan = %+v, want idx_consumer_event", plan)
		}

		var claim struct {
			DeviceID string `bson:"device_id"`
		}
		if err := claims.FindOne(ctx, filter).Decode(&claim); err != nil {
			t.Fatalf("read the claim of ev-2: %v", err)
		}
		if claim.DeviceID != "dev-1" {
			t.Errorf("the claim of ev-2 names device %q, want dev-1", claim.DeviceID)
		}
	})

	t.Run("a device's events are served by the device index", func(t *testing.T) {
		filter := bson.D{{Key: "device_id", Value: "dev-1"}}
		plan := mongotest.ExplainFind(t, harness.DB, "processed_events", filter, nil)
		if !plan.Uses("idx_device_id") {
			t.Errorf("inspecting one device's events: plan = %+v, want idx_device_id", plan)
		}

		count, err := claims.CountDocuments(ctx, filter)
		if err != nil {
			t.Fatalf("count the events of dev-1: %v", err)
		}
		if count != 2 {
			t.Errorf("documents recorded for dev-1 = %d, want its own 2", count)
		}
	})

	// The control makes the two assertions above a witness rather than an accident: the same
	// explain reports a collection scan for a field no index carries.
	t.Run("the explain reports a collection scan when no index serves the query", func(t *testing.T) {
		plan := mongotest.ExplainFind(t, harness.DB, "processed_events",
			bson.D{{Key: "processed_at", Value: claimedAt}}, nil)
		if !plan.CollectionScan {
			t.Errorf("the control lookup over an unindexed field reported no collection scan: %+v", plan)
		}
	})
}
