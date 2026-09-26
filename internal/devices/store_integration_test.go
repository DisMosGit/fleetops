//go:build integration

package devices

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// deviceDoc is the stored shape of a devices document.
type deviceDoc struct {
	ID        string    `bson:"_id"`
	Model     string    `bson:"model"`
	Region    string    `bson:"region"`
	CurrentFw string    `bson:"current_fw"`
	Status    string    `bson:"status"`
	LastSeen  time.Time `bson:"last_heartbeat"`
}

func TestStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	devices := harness.DB.Collection("devices")

	find := func(t *testing.T, id string) deviceDoc {
		t.Helper()
		var doc deviceDoc
		if err := devices.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err != nil {
			t.Fatalf("find device %s: %v", id, err)
		}
		return doc
	}

	t.Run("upsert requires a device id", func(t *testing.T) {
		if err := store.Upsert(ctx, Record{Model: "v3"}); err == nil {
			t.Error("Upsert() without id = nil error, want an error")
		}
	})

	t.Run("first registration creates the record", func(t *testing.T) {
		seen := time.Now().UTC().Truncate(time.Millisecond)
		rec := Record{
			ID: "dev-created", Model: "v3", Region: "eu-west",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: seen,
		}
		if err := store.Upsert(ctx, rec); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		got := find(t, rec.ID)
		want := deviceDoc(rec)
		if got != want {
			t.Errorf("device record = %+v, want %+v", got, want)
		}
	})

	t.Run("reconnection updates the record in place", func(t *testing.T) {
		seen := time.Now().UTC().Truncate(time.Millisecond)
		rec := Record{
			ID: "dev-reconnect", Model: "v3", Region: "eu-west",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: seen,
		}
		if err := store.Upsert(ctx, rec); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		rec.CurrentFw = "2.0.0"
		rec.LastSeen = seen.Add(time.Second)
		if err := store.Upsert(ctx, rec); err != nil {
			t.Fatalf("Upsert() second call error = %v", err)
		}

		count, err := devices.CountDocuments(ctx, bson.D{{Key: "_id", Value: rec.ID}})
		if err != nil {
			t.Fatalf("CountDocuments() error = %v", err)
		}
		if count != 1 {
			t.Errorf("documents for %s = %d, want 1", rec.ID, count)
		}
		want := deviceDoc(rec)
		if got := find(t, rec.ID); got != want {
			t.Errorf("device record = %+v, want %+v", got, want)
		}
	})

	t.Run("apply batch refreshes state", func(t *testing.T) {
		seen := time.Now().UTC().Truncate(time.Millisecond)
		if err := store.Upsert(ctx, Record{
			ID: "dev-refresh", Model: "v3", Region: "us-east",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: seen,
		}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		refreshed := seen.Add(5 * time.Second)
		if err := store.ApplyBatch(ctx, []Update{
			{ID: "dev-refresh", CurrentFw: "3.0.0", Status: StatusOnline, LastSeen: refreshed},
		}); err != nil {
			t.Fatalf("ApplyBatch() error = %v", err)
		}
		want := deviceDoc{
			ID: "dev-refresh", Model: "v3", Region: "us-east",
			CurrentFw: "3.0.0", Status: StatusOnline, LastSeen: refreshed,
		}
		if got := find(t, "dev-refresh"); got != want {
			t.Errorf("device record = %+v, want %+v", got, want)
		}
	})

	t.Run("apply batch coalesces to the newest refresh", func(t *testing.T) {
		seen := time.Now().UTC().Truncate(time.Millisecond)
		if err := store.Upsert(ctx, Record{
			ID: "dev-coalesce", Model: "v3", Region: "us-east",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: seen,
		}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		newest := seen.Add(3 * time.Second)
		if err := store.ApplyBatch(ctx, []Update{
			{ID: "dev-coalesce", CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: seen},
			{ID: "dev-coalesce", CurrentFw: "2.0.0", Status: StatusOnline, LastSeen: newest},
		}); err != nil {
			t.Fatalf("ApplyBatch() error = %v", err)
		}
		if got := find(t, "dev-coalesce"); got.LastSeen != newest || got.CurrentFw != "2.0.0" {
			t.Errorf("device record = %+v, want last seen %v and firmware 2.0.0", got, newest)
		}
	})

	t.Run("mark stale flips once and counts once", func(t *testing.T) {
		stale := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
		if err := store.Upsert(ctx, Record{
			ID: "dev-stale", Model: "v3", Region: "us-east",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: stale,
		}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		cutoff := time.Now().Add(-time.Hour)
		transitions, err := store.MarkStale(ctx, cutoff)
		if err != nil {
			t.Fatalf("MarkStale() error = %v", err)
		}
		if transitions != 1 {
			t.Errorf("MarkStale() transitions = %d, want 1", transitions)
		}
		got := find(t, "dev-stale")
		if got.Status != StatusOffline {
			t.Errorf("device status = %q, want %q", got.Status, StatusOffline)
		}
		if !got.LastSeen.Equal(stale) {
			t.Errorf("device last seen = %v, want it unchanged at %v", got.LastSeen, stale)
		}

		transitions, err = store.MarkStale(ctx, cutoff)
		if err != nil {
			t.Fatalf("MarkStale() second call error = %v", err)
		}
		if transitions != 0 {
			t.Errorf("MarkStale() second call transitions = %d, want 0", transitions)
		}
	})

	t.Run("mark stale leaves reporting devices online", func(t *testing.T) {
		if err := store.Upsert(ctx, Record{
			ID: "dev-fresh", Model: "v3", Region: "us-east",
			CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: time.Now(),
		}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		transitions, err := store.MarkStale(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("MarkStale() error = %v", err)
		}
		if transitions != 0 {
			t.Errorf("MarkStale() transitions = %d, want 0", transitions)
		}
		if got := find(t, "dev-fresh"); got.Status != StatusOnline {
			t.Errorf("device status = %q, want %q", got.Status, StatusOnline)
		}
	})
}
