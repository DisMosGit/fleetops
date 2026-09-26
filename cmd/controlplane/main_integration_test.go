//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// TestSweepMarksSilentDevicesOfflineAndCounts runs the wired liveness path against the real
// schema: a device silent past the threshold flips to offline and its transition is counted
// exactly once, while a device still reporting stays online.
func TestSweepMarksSilentDevicesOfflineAndCounts(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := devices.NewStore(harness.DB)
	_, counter := newMetrics()
	sweeper := devices.NewSweeper(
		store,
		50*time.Millisecond,
		10*time.Millisecond,
		func(transitions int64) { counter.Add(float64(transitions)) },
		slog.New(slog.DiscardHandler),
	)
	ctx := context.Background()
	deviceDocs := harness.DB.Collection("devices")

	for _, rec := range []devices.Record{
		{
			ID: "dev-silent", Model: "v3", Region: "eu-west",
			CurrentFw: "1.0.0", Status: devices.StatusOnline,
			LastSeen: time.Now().Add(-time.Hour),
		},
		{
			ID: "dev-reporting", Model: "v3", Region: "eu-west",
			CurrentFw: "1.0.0", Status: devices.StatusOnline,
			LastSeen: time.Now(),
		},
	} {
		if err := store.Upsert(ctx, rec); err != nil {
			t.Fatalf("Upsert(%s) error = %v", rec.ID, err)
		}
	}

	statusOf := func(t *testing.T, id string) string {
		t.Helper()
		var doc struct {
			Status string `bson:"status"`
		}
		if err := deviceDocs.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err != nil {
			t.Fatalf("find device %s: %v", id, err)
		}
		return doc.Status
	}

	// The reporting device keeps refreshing its last-seen time the way heartbeat ingestion
	// does, so it is never stale while the sweep runs.
	reportCtx, stopReporting := context.WithCancel(ctx)
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-reportCtx.Done():
				return
			case <-ticker.C:
				err := store.ApplyBatch(reportCtx, []devices.Update{{
					ID: "dev-reporting", CurrentFw: "1.0.0",
					Status: devices.StatusOnline, LastSeen: time.Now(),
				}})
				if err != nil && reportCtx.Err() == nil {
					t.Errorf("refresh reporting device: %v", err)
				}
			}
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(runCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for statusOf(t, "dev-silent") != devices.StatusOffline {
		if time.Now().After(deadline) {
			t.Fatal("silent device was never swept to offline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Let further sweep passes run: an already-offline device must not count again.
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("sweeper Run() error = %v, want nil", err)
	}
	stopReporting()
	<-reportDone

	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("fleetops_device_offline_transitions_total = %v, want exactly 1", got)
	}
	if got := statusOf(t, "dev-reporting"); got != devices.StatusOnline {
		t.Errorf("reporting device status = %q, want %q", got, devices.StatusOnline)
	}
}
