//go:build integration

package devices

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// snapshotDoc is the stored shape of a device_state_snapshots document: the projection fields
// with the configuration content decoded into its concrete document form.
type snapshotDoc struct {
	ID              string            `bson:"_id"`
	Region          string            `bson:"region"`
	Model           string            `bson:"model"`
	CurrentFw       string            `bson:"current_fw"`
	Online          bool              `bson:"online"`
	LastHeartbeatAt time.Time         `bson:"last_heartbeat"`
	Pending         *SnapshotCommand  `bson:"pending,omitempty"`
	Config          snapshotConfigDoc `bson:"config"`
	SnapshotAt      time.Time         `bson:"snapshot_at"`
}

// snapshotConfigDoc is the stored shape of the projected configuration snapshot.
type snapshotConfigDoc struct {
	Version int64          `bson:"version"`
	Data    map[string]any `bson:"data,omitempty"`
}

func TestSnapshotStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := NewSnapshotStore(harness.DB)
	ctx := context.Background()
	coll := harness.DB.Collection("device_state_snapshots")

	// Mongo stores millisecond precision; comparing at that precision keeps the assertions
	// honest about what round-trips.
	ms := func(t time.Time) time.Time { return t.UTC().Truncate(time.Millisecond) }
	decided := ms(time.Unix(5000, 0))
	heard := ms(time.Unix(4000, 0))

	snapshot := func(id string, at time.Time, fw string) Snapshot {
		return Snapshot{
			DeviceID:        id,
			Region:          "eu-west",
			Model:           "oak-s3",
			CurrentFw:       fw,
			Online:          true,
			LastHeartbeatAt: heard,
			Config:          SnapshotConfig{Version: 3, Data: map[string]any{"interval": "5s"}},
			SnapshotAt:      ms(at),
		}
	}
	wantDoc := func(id string, at time.Time, fw string) snapshotDoc {
		return snapshotDoc{
			ID:              id,
			Region:          "eu-west",
			Model:           "oak-s3",
			CurrentFw:       fw,
			Online:          true,
			LastHeartbeatAt: heard,
			Config:          snapshotConfigDoc{Version: 3, Data: map[string]any{"interval": "5s"}},
			SnapshotAt:      ms(at),
		}
	}

	find := func(t *testing.T, id string) snapshotDoc {
		t.Helper()
		var doc snapshotDoc
		if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err != nil {
			t.Fatalf("find snapshot %s: %v", id, err)
		}
		return doc
	}
	count := func(t *testing.T, id string) int64 {
		t.Helper()
		n, err := coll.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}})
		if err != nil {
			t.Fatalf("count snapshots %s: %v", id, err)
		}
		return n
	}

	t.Run("save requires a device id", func(t *testing.T) {
		t.Parallel()
		if err := store.SaveSnapshot(ctx, Snapshot{SnapshotAt: decided}); err == nil {
			t.Error("SaveSnapshot() without device id = nil error, want an error")
		}
	})

	t.Run("first snapshot creates the document", func(t *testing.T) {
		t.Parallel()
		if err := store.SaveSnapshot(ctx, snapshot("dev-created", decided, "fw-2")); err != nil {
			t.Fatalf("SaveSnapshot() error = %v", err)
		}
		want := wantDoc("dev-created", decided, "fw-2")
		if diff := cmp.Diff(want, find(t, "dev-created")); diff != "" {
			t.Errorf("stored snapshot mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("retried snapshot converges to one document", func(t *testing.T) {
		t.Parallel()
		snap := snapshot("dev-retry", decided, "fw-2")
		for range 3 {
			if err := store.SaveSnapshot(ctx, snap); err != nil {
				t.Fatalf("SaveSnapshot() error = %v", err)
			}
		}
		if got := count(t, "dev-retry"); got != 1 {
			t.Errorf("document count = %d, want 1", got)
		}
	})

	t.Run("newer snapshot replaces in place", func(t *testing.T) {
		t.Parallel()
		if err := store.SaveSnapshot(ctx, snapshot("dev-replace", decided, "fw-2")); err != nil {
			t.Fatalf("SaveSnapshot(older) error = %v", err)
		}
		if err := store.SaveSnapshot(ctx, snapshot("dev-replace", decided.Add(time.Second), "fw-3")); err != nil {
			t.Fatalf("SaveSnapshot(newer) error = %v", err)
		}
		if got := count(t, "dev-replace"); got != 1 {
			t.Errorf("document count = %d, want 1", got)
		}
		want := wantDoc("dev-replace", decided.Add(time.Second), "fw-3")
		if diff := cmp.Diff(want, find(t, "dev-replace")); diff != "" {
			t.Errorf("stored snapshot mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("older snapshot never regresses the projection", func(t *testing.T) {
		t.Parallel()
		newer := snapshot("dev-regress", decided.Add(2*time.Second), "fw-3")
		if err := store.SaveSnapshot(ctx, newer); err != nil {
			t.Fatalf("SaveSnapshot(newer) error = %v", err)
		}
		if err := store.SaveSnapshot(ctx, snapshot("dev-regress", decided, "fw-2")); err != nil {
			t.Fatalf("SaveSnapshot(older) error = %v", err)
		}
		if got := count(t, "dev-regress"); got != 1 {
			t.Errorf("document count = %d, want 1", got)
		}
		want := wantDoc("dev-regress", decided.Add(2*time.Second), "fw-3")
		if diff := cmp.Diff(want, find(t, "dev-regress")); diff != "" {
			t.Errorf("out-of-order write regressed the projection (-want +got):\n%s", diff)
		}
	})

	t.Run("pending command projects with its delivery state", func(t *testing.T) {
		t.Parallel()
		snap := snapshot("dev-pending", decided, "fw-2")
		snap.Pending = &SnapshotCommand{
			CommandID:  "cmd-1",
			DeviceID:   "dev-pending",
			Kind:       "update",
			FirmwareID: "fw-4",
			Version:    "fw-4",
			Dispatched: true,
		}
		if err := store.SaveSnapshot(ctx, snap); err != nil {
			t.Fatalf("SaveSnapshot() error = %v", err)
		}
		want := wantDoc("dev-pending", decided, "fw-2")
		want.Pending = &SnapshotCommand{
			CommandID:  "cmd-1",
			DeviceID:   "dev-pending",
			Kind:       "update",
			FirmwareID: "fw-4",
			Version:    "fw-4",
			Dispatched: true,
		}
		if diff := cmp.Diff(want, find(t, "dev-pending")); diff != "" {
			t.Errorf("stored snapshot mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("documents missing required fields are rejected", func(t *testing.T) {
		t.Parallel()
		// The projection always writes complete documents; the schema validator is the
		// backstop for any other writer.
		if _, err := coll.InsertOne(ctx, bson.D{
			{Key: "_id", Value: "dev-partial"},
			{Key: "region", Value: "eu-west"},
		}); err == nil {
			t.Fatal("insert of a partial snapshot document succeeded, want a validator rejection")
		}
		if n := count(t, "dev-partial"); n != 0 {
			t.Errorf("partial document count = %d, want 0", n)
		}
	})
}
