//go:build integration

package wavehealth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// insertRollout stores the rollout document a wave's parent lookup resolves.
func insertRollout(t *testing.T, db *mongo.Database, id string) {
	t.Helper()
	doc := bson.D{
		{Key: "_id", Value: id},
		{Key: "firmware_id", Value: "fw-1"},
		{Key: "status", Value: "running"},
		{Key: "temporal_wf_id", Value: "wf-" + id},
		{Key: "region", Value: "eu-west"},
		{Key: "model", Value: "oak-s3"},
	}
	if _, err := db.Collection("rollouts").InsertOne(context.Background(), doc); err != nil {
		t.Fatalf("insert rollout %s: %v", id, err)
	}
}

// insertWave stores a wave document through the real validator, so the field names and required
// fields the evaluation reads are the ones the schema enforces.
func insertWave(t *testing.T, db *mongo.Database, id, rolloutID string, deviceIDs []string, startedAt time.Time) {
	t.Helper()
	members := bson.A{}
	for _, deviceID := range deviceIDs {
		members = append(members, deviceID)
	}
	doc := bson.D{
		{Key: "_id", Value: id},
		{Key: "rollout_id", Value: rolloutID},
		{Key: "percent", Value: 5},
		{Key: "status", Value: "running"},
		{Key: "success_rate", Value: 0.0},
		{Key: "device_ids", Value: members},
		{Key: "started_at", Value: startedAt},
	}
	if _, err := db.Collection("waves").InsertOne(context.Background(), doc); err != nil {
		t.Fatalf("insert wave %s: %v", id, err)
	}
}

// insertSample stores one heartbeat sample through the ingestion contract: _id is the event id, so
// storing the same event again is a no-op rather than a second row.
func insertSample(t *testing.T, db *mongo.Database, eventID, deviceID string, at time.Time, health float64) {
	t.Helper()
	doc := bson.D{
		{Key: "_id", Value: eventID},
		{Key: "ts", Value: at},
		{Key: "meta", Value: bson.D{
			{Key: "device_id", Value: deviceID},
			{Key: "region", Value: "eu-west"},
			{Key: "model", Value: "oak-s3"},
		}},
		{Key: "cpu", Value: 0.2},
		{Key: "mem", Value: 0.3},
		{Key: "health", Value: health},
	}
	res, err := db.Collection("telemetry").UpdateOne(context.Background(),
		bson.D{{Key: "_id", Value: eventID}},
		bson.D{{Key: "$setOnInsert", Value: doc}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("insert sample %s: %v", eventID, err)
	}
	if res.UpsertedCount == 0 && res.MatchedCount == 0 {
		t.Fatalf("insert sample %s: neither stored nor matched", eventID)
	}
}

// TestStore proves the Mongo-backed store against the real schema: the sample counts come from the
// wave's own devices and only from inside the effective window, and the three ways a (rollout,
// wave) pair can miss are all misses.
func TestStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := wavehealth.NewStore(harness.DB)
	db := harness.DB
	ctx := context.Background()
	// MongoDB keeps millisecond precision, so evaluate on a millisecond boundary to compare the
	// window the store resolved with the wave start that was stored.
	now := time.Now().UTC().Truncate(time.Millisecond)

	settings := wavehealth.Settings{
		HealthWindow:          5 * time.Minute,
		SampleHealthThreshold: 0.6,
		MinSuccessRatio:       0.95,
		MinSamples:            5,
	}
	query := func(rolloutID, waveID string) wavehealth.Query {
		return wavehealth.Query{RolloutID: rolloutID, WaveID: waveID}
	}

	t.Run("counts only the wave's own devices inside the window", func(t *testing.T) {
		insertRollout(t, db, "roll-scope")
		insertWave(t, db, "wave-scope", "roll-scope", []string{"dev-a", "dev-b"}, now.Add(-time.Hour))

		// Five in-window samples from the wave's own devices, all healthy.
		for i, at := range []time.Time{now.Add(-4 * time.Minute), now.Add(-3 * time.Minute), now.Add(-2 * time.Minute)} {
			insertSample(t, db, eventID("scope-a", i), "dev-a", at, 0.95)
		}
		insertSample(t, db, "scope-b-0", "dev-b", now.Add(-2*time.Minute), 0.9)
		insertSample(t, db, "scope-b-1", "dev-b", now.Add(-time.Minute), 0.85)

		// A device the wave never targeted, and in-window samples it emitted.
		for i := 0; i < 5; i++ {
			insertSample(t, db, eventID("scope-outsider", i), "dev-outsider", now.Add(-3*time.Minute), 0.1)
		}
		// The wave's own device, but outside the window on both sides: before it and still ahead.
		insertSample(t, db, "scope-old-0", "dev-a", now.Add(-30*time.Minute), 0.1)
		insertSample(t, db, "scope-future-0", "dev-a", now.Add(5*time.Minute), 0.1)

		got, err := wavehealth.New(store, store, settings).Evaluate(ctx, query("roll-scope", "wave-scope"), now)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if got.SampleSize != 5 || got.SuccessRatio != 1 {
			t.Errorf("Evaluate() = ratio %v over %d samples, want 1 over 5 (only the wave's devices, only in the window)",
				got.SuccessRatio, got.SampleSize)
		}
		if got.Verdict != wavehealth.VerdictHealthy {
			t.Errorf("Evaluate() verdict = %s, want healthy", got.Verdict)
		}

		// Evaluation is read-only: the wave document is exactly as it was written. Persisting the
		// measured ratio is the rollout workflow's write, not this query's.
		var stored struct {
			SuccessRate float64 `bson:"success_rate"`
		}
		if err := db.Collection("waves").
			FindOne(ctx, bson.D{{Key: "_id", Value: "wave-scope"}}).
			Decode(&stored); err != nil {
			t.Fatalf("read wave after evaluation: %v", err)
		}
		if stored.SuccessRate != 0 {
			t.Errorf("wave success_rate = %v after evaluation, want it untouched at 0", stored.SuccessRate)
		}

		// The same scope, counted directly: the pipeline's threshold decides the successful
		// subset, and a period with no samples is zero rather than an error.
		total, successful, err := store.CountSamples(ctx, wavehealth.SampleWindow{
			DeviceIDs:      []string{"dev-a", "dev-b"},
			From:           now.Add(-31 * time.Minute),
			To:             now.Add(-29 * time.Minute),
			SuccessAtLeast: settings.SampleHealthThreshold,
		})
		if err != nil {
			t.Fatalf("CountSamples() error = %v", err)
		}
		if total != 1 || successful != 0 {
			t.Errorf("CountSamples() = %d total, %d successful, want 1 and 0 for the aged-out degraded sample",
				total, successful)
		}

		emptyTotal, _, err := store.CountSamples(ctx, wavehealth.SampleWindow{
			DeviceIDs:      nil,
			From:           now.Add(-settings.HealthWindow),
			To:             now,
			SuccessAtLeast: settings.SampleHealthThreshold,
		})
		if err != nil {
			t.Fatalf("CountSamples(no devices) error = %v", err)
		}
		if emptyTotal != 0 {
			t.Errorf("CountSamples(no devices) = %d total, want 0", emptyTotal)
		}
	})

	t.Run("counts a redelivered heartbeat once", func(t *testing.T) {
		insertRollout(t, db, "roll-redelivery")
		insertWave(t, db, "wave-redelivery", "roll-redelivery", []string{"dev-r"}, now.Add(-time.Hour))

		insertSample(t, db, "redelivery-1", "dev-r", now.Add(-time.Minute), 0.9)
		insertSample(t, db, "redelivery-1", "dev-r", now.Add(-time.Minute), 0.9)

		total, successful, err := store.CountSamples(ctx, wavehealth.SampleWindow{
			DeviceIDs:      []string{"dev-r"},
			From:           now.Add(-settings.HealthWindow),
			To:             now,
			SuccessAtLeast: settings.SampleHealthThreshold,
		})
		if err != nil {
			t.Fatalf("CountSamples() error = %v", err)
		}
		if total != 1 || successful != 1 {
			t.Errorf("CountSamples() = %d total, %d successful, want 1 and 1: a redelivery is not a second sample",
				total, successful)
		}
	})

	t.Run("the window never reaches back before the wave started", func(t *testing.T) {
		insertRollout(t, db, "roll-clip")
		startedAt := now.Add(-2 * time.Minute)
		insertWave(t, db, "wave-clip", "roll-clip", []string{"dev-c"}, startedAt)

		// Pre-wave heartbeats from the same device: it was still healthy on the old firmware.
		insertSample(t, db, "clip-pre-0", "dev-c", now.Add(-4*time.Minute), 0.95)
		insertSample(t, db, "clip-pre-1", "dev-c", now.Add(-3*time.Minute), 0.95)
		// The wave's own period, where the device is degraded.
		insertSample(t, db, "clip-post-0", "dev-c", now.Add(-90*time.Second), 0.1)
		insertSample(t, db, "clip-post-1", "dev-c", now.Add(-30*time.Second), 0.1)

		clipped := settings
		clipped.MinSamples = 2
		clipped.MinSuccessRatio = 0.5
		got, err := wavehealth.New(store, store, clipped).Evaluate(ctx, query("roll-clip", "wave-clip"), now)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if !got.WindowStart.Equal(startedAt) {
			t.Errorf("Evaluate() window start = %v, want the wave start %v", got.WindowStart, startedAt)
		}
		if got.SampleSize != 2 || got.SuccessRatio != 0 {
			t.Errorf("Evaluate() = ratio %v over %d samples, want 0 over 2: pre-wave samples must not count",
				got.SuccessRatio, got.SampleSize)
		}
		if got.Verdict != wavehealth.VerdictUnhealthy {
			t.Errorf("Evaluate() verdict = %s, want unhealthy", got.Verdict)
		}
	})

	t.Run("a wave without target devices reports an empty window", func(t *testing.T) {
		insertRollout(t, db, "roll-empty")
		insertWave(t, db, "wave-empty", "roll-empty", nil, now.Add(-time.Hour))

		got, err := wavehealth.New(store, store, settings).Evaluate(ctx, query("roll-empty", "wave-empty"), now)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if got.SampleSize != 0 || got.SuccessRatio != 0 {
			t.Errorf("Evaluate() = ratio %v over %d samples, want 0 over 0", got.SuccessRatio, got.SampleSize)
		}
		if got.Verdict != wavehealth.VerdictUndecided {
			t.Errorf("Evaluate() verdict = %s, want undecided rather than healthy", got.Verdict)
		}
	})

	t.Run("unknown rollouts and waves are not found", func(t *testing.T) {
		insertRollout(t, db, "roll-other")
		insertWave(t, db, "wave-other", "roll-other", []string{"dev-x"}, now.Add(-time.Hour))
		insertRollout(t, db, "roll-known")
		insertWave(t, db, "wave-known", "roll-known", []string{"dev-x"}, now.Add(-time.Hour))

		tests := []struct {
			name      string
			rolloutID string
			waveID    string
		}{
			{name: "unknown rollout", rolloutID: "roll-unknown", waveID: "wave-known"},
			{name: "unknown wave", rolloutID: "roll-known", waveID: "wave-unknown"},
			{name: "wave of another rollout", rolloutID: "roll-known", waveID: "wave-other"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := store.Wave(ctx, tc.rolloutID, tc.waveID)
				if !errors.Is(err, wavehealth.ErrNotFound) {
					t.Fatalf("Wave() error = %v, want ErrNotFound", err)
				}
				_, err = wavehealth.New(store, store, settings).
					Evaluate(ctx, query(tc.rolloutID, tc.waveID), now)
				if !errors.Is(err, wavehealth.ErrNotFound) {
					t.Errorf("Evaluate() error = %v, want ErrNotFound", err)
				}
			})
		}

		// The same wave under its own rollout resolves, so the misses above are the mismatch and
		// not a broken lookup.
		if _, err := store.Wave(ctx, "roll-other", "wave-other"); err != nil {
			t.Errorf("Wave() for a matching pair error = %v, want nil", err)
		}
	})
}

// eventID builds a distinct event id for one sample of a scenario.
func eventID(prefix string, i int) string {
	return prefix + "-" + string(rune('0'+i))
}
