//go:build integration

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/mongotest"
)

func TestWriterIngest(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	db := harness.DB
	store := devices.NewStore(db)
	telemetry := db.Collection("telemetry")
	deviceDocs := db.Collection("devices")
	ctx := context.Background()

	// countEvents returns how many telemetry documents exist for one device.
	countEvents := func(t *testing.T, deviceID string) int64 {
		t.Helper()
		count, err := telemetry.CountDocuments(ctx, bson.D{{Key: "meta.device_id", Value: deviceID}})
		if err != nil {
			t.Fatalf("CountDocuments(%s) error = %v", deviceID, err)
		}
		return count
	}

	// waitEvents waits until one device has want stored events.
	waitEvents := func(t *testing.T, deviceID string, want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			got := countEvents(t, deviceID)
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("stored events for %s = %d, want %d", deviceID, got, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	t.Run("steady heartbeats land in one batched write", func(t *testing.T) {
		const events = 12
		rec := devices.Record{ID: "dev-batch", Model: "v3", Region: "eu-west"}
		older := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
		if err := store.Upsert(ctx, devices.Record{
			ID: rec.ID, Model: rec.Model, Region: rec.Region,
			CurrentFw: "1.0.0", Status: devices.StatusOnline, LastSeen: older,
		}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}

		// One huge flush interval and one full batch: every heartbeat must reach storage in a
		// single round trip, never one write per message.
		w := NewWriter(telemetry, store, events, time.Hour, slog.Default())
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.Run(runCtx) }()

		base := time.Now().UTC().Truncate(time.Millisecond)
		for i := 0; i < events; i++ {
			hb := heartbeat(fmt.Sprintf("ev-batch-%d", i), rec.ID, base.Add(time.Duration(i)*time.Second))
			if err := w.Handle(runCtx, hb, rec); err != nil {
				t.Fatalf("Handle(ev-batch-%d) error = %v", i, err)
			}
		}
		waitEvents(t, rec.ID, events)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}

		if got := harness.InsertCommands("telemetry"); got != 1 {
			t.Errorf("insert commands = %d, want 1 batched write for %d events", got, events)
		}

		// The first event carries its measurement time and the identity meta recorded at
		// registration.
		var doc struct {
			TS   time.Time `bson:"ts"`
			Meta struct {
				DeviceID string `bson:"device_id"`
				Region   string `bson:"region"`
				Model    string `bson:"model"`
			} `bson:"meta"`
		}
		err := telemetry.FindOne(ctx, bson.D{{Key: "_id", Value: "ev-batch-0"}}).Decode(&doc)
		if err != nil {
			t.Fatalf("find ev-batch-0: %v", err)
		}
		if !doc.TS.Equal(base) {
			t.Errorf("stored ts = %v, want the measurement time %v", doc.TS, base)
		}
		if doc.Meta.DeviceID != rec.ID || doc.Meta.Region != "eu-west" || doc.Meta.Model != "v3" {
			t.Errorf("stored meta = %+v, want the registered identity of %s", doc.Meta, rec.ID)
		}

		// Ingestion refreshed the device record's last-seen time past the registration value.
		var device struct {
			LastSeen time.Time `bson:"last_heartbeat"`
		}
		err = deviceDocs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.ID}}).Decode(&device)
		if err != nil {
			t.Fatalf("find device %s: %v", rec.ID, err)
		}
		if !device.LastSeen.After(older) {
			t.Errorf("device last_heartbeat = %v, want it refreshed past %v", device.LastSeen, older)
		}
	})

	t.Run("quiet fleet persists promptly", func(t *testing.T) {
		rec := devices.Record{ID: "dev-quiet", Model: "v3", Region: "eu-west"}
		w := NewWriter(telemetry, store, 100, 20*time.Millisecond, slog.Default())
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.Run(runCtx) }()

		if err := w.Handle(runCtx, heartbeat("ev-quiet-0", rec.ID, time.Now()), rec); err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		waitEvents(t, rec.ID, 1)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}
	})

	t.Run("redelivery and mixed batches are no-ops", func(t *testing.T) {
		rec := devices.Record{ID: "dev-redelivery", Model: "v3", Region: "eu-west"}
		w := NewWriter(telemetry, store, 2, 20*time.Millisecond, slog.Default())
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.Run(runCtx) }()

		if err := w.Handle(runCtx, heartbeat("ev-r-0", rec.ID, time.Now()), rec); err != nil {
			t.Fatalf("Handle(ev-r-0) error = %v", err)
		}
		waitEvents(t, rec.ID, 1)

		// One redelivered event and one new event in a single batch: the new one lands, the
		// duplicate is a no-op, and ingestion keeps running.
		if err := w.Handle(runCtx, heartbeat("ev-r-0", rec.ID, time.Now()), rec); err != nil {
			t.Fatalf("Handle(ev-r-0 redelivery) error = %v", err)
		}
		if err := w.Handle(runCtx, heartbeat("ev-r-1", rec.ID, time.Now()), rec); err != nil {
			t.Fatalf("Handle(ev-r-1) error = %v", err)
		}
		waitEvents(t, rec.ID, 2)

		if err := w.Handle(runCtx, heartbeat("ev-r-2", rec.ID, time.Now()), rec); err != nil {
			t.Fatalf("Handle(ev-r-2) error = %v", err)
		}
		waitEvents(t, rec.ID, 3)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}

		for _, eventID := range []string{"ev-r-0", "ev-r-1", "ev-r-2"} {
			count, err := telemetry.CountDocuments(ctx, bson.D{{Key: "_id", Value: eventID}})
			if err != nil {
				t.Fatalf("CountDocuments(%s) error = %v", eventID, err)
			}
			if count != 1 {
				t.Errorf("documents for %s = %d, want exactly 1", eventID, count)
			}
		}
	})

	t.Run("full pipeline blocks instead of buffering", func(t *testing.T) {
		rec := devices.Record{ID: "dev-backpressure", Model: "v3", Region: "eu-west"}
		// With Run stopped the queue fills at 2 * batchSize and the next Handle blocks.
		w := NewWriter(telemetry, store, 2, time.Hour, slog.Default())
		for i := 0; i < 4; i++ {
			hb := heartbeat(fmt.Sprintf("ev-bp-%d", i), rec.ID, time.Now())
			if err := w.Handle(ctx, hb, rec); err != nil {
				t.Fatalf("Handle(ev-bp-%d) error = %v", i, err)
			}
		}
		blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := w.Handle(blocked, heartbeat("ev-bp-overflow", rec.ID, time.Now()), rec)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Handle() on a full pipeline error = %v, want context.DeadlineExceeded", err)
		}
	})
}
