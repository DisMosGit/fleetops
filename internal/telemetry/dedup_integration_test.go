//go:build integration

package telemetry

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// TestLedgerClaimAndComplete verifies the dedup ledger against the real schema: one document per
// consumer and event, a second claim recognised as a duplicate rather than an error, and
// processed_at absent until the side effect is durable.
func TestLedgerClaimAndComplete(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	ledger := NewLedger(harness.DB.Collection("processed_events"))
	claims := harness.DB.Collection("processed_events")
	ctx := context.Background()
	claimedAt := time.Now().UTC().Truncate(time.Millisecond)

	type record struct {
		Consumer    string     `bson:"consumer"`
		EventID     string     `bson:"event_id"`
		DeviceID    string     `bson:"device_id"`
		ClaimedAt   time.Time  `bson:"claimed_at"`
		ProcessedAt *time.Time `bson:"processed_at"`
	}
	read := func(t *testing.T, consumer, eventID string) record {
		t.Helper()
		var got record
		err := claims.FindOne(ctx, bson.D{{Key: "_id", Value: LedgerID(consumer, eventID)}}).Decode(&got)
		if err != nil {
			t.Fatalf("find claim %s: %v", LedgerID(consumer, eventID), err)
		}
		return got
	}
	count := func(t *testing.T, consumer, eventID string) int64 {
		t.Helper()
		got, err := claims.CountDocuments(ctx, bson.D{
			{Key: "consumer", Value: consumer},
			{Key: "event_id", Value: eventID},
		})
		if err != nil {
			t.Fatalf("count claims of %s: %v", eventID, err)
		}
		return got
	}

	t.Run("a first claim records an unfinished event", func(t *testing.T) {
		state, err := ledger.Claim(ctx, "fleetops.heartbeat.alerting", "ev-1", "dev-1", claimedAt)
		if err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		if state != ClaimNew {
			t.Errorf("Claim() state = %s, want new", state)
		}
		got := read(t, "fleetops.heartbeat.alerting", "ev-1")
		if got.DeviceID != "dev-1" || !got.ClaimedAt.Equal(claimedAt) {
			t.Errorf("claim = %+v, want the device and claim time it was recorded with", got)
		}
		if got.ProcessedAt != nil {
			t.Errorf("processed_at = %v, want it absent until the side effect is durable", got.ProcessedAt)
		}
		if got := count(t, "fleetops.heartbeat.alerting", "ev-1"); got != 1 {
			t.Errorf("documents for one pair = %d, want exactly 1", got)
		}
	})

	t.Run("a second claim is unfinished work rather than an error", func(t *testing.T) {
		state, err := ledger.Claim(ctx, "fleetops.heartbeat.alerting", "ev-1", "dev-1", time.Now())
		if err != nil {
			t.Fatalf("Claim() on an existing unfinished claim error = %v, want nil", err)
		}
		if state != ClaimUnfinished {
			t.Errorf("Claim() state = %s, want unfinished", state)
		}
		if got := count(t, "fleetops.heartbeat.alerting", "ev-1"); got != 1 {
			t.Errorf("documents for one pair = %d, want the uniqueness constraint to refuse a second", got)
		}
	})

	t.Run("a completed claim is a duplicate", func(t *testing.T) {
		processedAt := claimedAt.Add(time.Second)
		if err := ledger.MarkProcessed(ctx, "fleetops.heartbeat.alerting", "ev-1", processedAt); err != nil {
			t.Fatalf("MarkProcessed() error = %v", err)
		}
		state, err := ledger.Claim(ctx, "fleetops.heartbeat.alerting", "ev-1", "dev-1", time.Now())
		if err != nil {
			t.Fatalf("Claim() on a processed event error = %v", err)
		}
		if state != ClaimProcessed {
			t.Errorf("Claim() state = %s, want processed", state)
		}
		got := read(t, "fleetops.heartbeat.alerting", "ev-1")
		if got.ProcessedAt == nil || !got.ProcessedAt.Equal(processedAt) {
			t.Errorf("processed_at = %v, want the recorded completion %v", got.ProcessedAt, processedAt)
		}
		if got := count(t, "fleetops.heartbeat.alerting", "ev-1"); got != 1 {
			t.Errorf("documents for one pair = %d, want exactly 1", got)
		}
	})

	t.Run("each consumer keeps its own record", func(t *testing.T) {
		state, err := ledger.Claim(ctx, "telemetrytest.analytics", "ev-1", "dev-1", time.Now())
		if err != nil {
			t.Fatalf("Claim() for another consumer error = %v", err)
		}
		if state != ClaimNew {
			t.Errorf("Claim() for another consumer = %s, want its own new claim", state)
		}
		if got := count(t, "telemetrytest.analytics", "ev-1"); got != 1 {
			t.Errorf("documents for the second consumer = %d, want 1", got)
		}
	})

	t.Run("a completion without its claim is refused", func(t *testing.T) {
		if err := ledger.MarkProcessed(ctx, "fleetops.heartbeat.alerting", "ev-missing", time.Now()); err == nil {
			t.Error("MarkProcessed() without a claim = nil, want an error")
		}
	})
}
