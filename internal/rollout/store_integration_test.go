//go:build integration

package rollout

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// assertEmptyArrays fails the test unless the named wave document stores both outcome sets as
// empty BSON arrays. Reading through the driver's decoder cannot tell an empty array from a
// missing field — both decode to an empty slice — so the stored document is inspected directly.
func assertEmptyArrays(t *testing.T, ctx context.Context, waves *mongo.Collection, waveID string) {
	t.Helper()
	var raw bson.M
	if err := waves.FindOne(ctx, bson.D{{Key: "_id", Value: waveID}}).Decode(&raw); err != nil {
		t.Fatalf("read raw wave %s: %v", waveID, err)
	}
	for _, field := range []string{"failed_device_ids", "unreported_device_ids"} {
		value, ok := raw[field]
		if !ok {
			t.Errorf("stored wave %s has no %s field, want an empty array", waveID, field)
			continue
		}
		ids, ok := value.(bson.A)
		if !ok {
			t.Errorf("stored wave %s field %s = %#v, want an array", waveID, field, value)
			continue
		}
		if len(ids) != 0 {
			t.Errorf("stored wave %s field %s = %v, want an empty array", waveID, field, ids)
		}
	}
}

// startedAt is the workflow-decided start time every resolution in these tests records. It is
// fixed and millisecond-aligned so the stored value can be compared exactly — MongoDB keeps
// milliseconds, and a wall-clock timestamp would only ever compare approximately.
var startedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	devices := harness.DB.Collection("devices")
	rollouts := harness.DB.Collection("rollouts")
	waves := harness.DB.Collection("waves")

	// Every subtest draws from its own selector — its own region and model no other subtest
	// seeds — so each pool holds exactly the devices its subtest wrote.
	seed := func(t *testing.T, id, region, model string) {
		t.Helper()
		_, err := devices.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id},
			{Key: "model", Value: model},
			{Key: "region", Value: region},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "online"},
			{Key: "last_heartbeat", Value: startedAt},
		})
		if err != nil {
			t.Fatalf("seed device %s: %v", id, err)
		}
	}
	readWave := func(t *testing.T, id string) WaveRecord {
		t.Helper()
		var rec WaveRecord
		if err := waves.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec); err != nil {
			t.Fatalf("read wave %s: %v", id, err)
		}
		return rec
	}
	readRollout := func(t *testing.T, id string) RolloutRecord {
		t.Helper()
		var rec RolloutRecord
		if err := rollouts.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec); err != nil {
			t.Fatalf("read rollout %s: %v", id, err)
		}
		return rec
	}
	rolloutRecord := func(id string, status RolloutStatus) RolloutRecord {
		return RolloutRecord{
			ID: id, FirmwareID: "fw-1", Status: status,
			WorkflowID: "rollout-" + id, Region: "eu-west", Model: "oak-s3",
		}
	}

	t.Run("the pool query selects exactly the selector's devices in identity order", func(t *testing.T) {
		// Seeded out of order and across selectors: only the eu-west oak-s3-pool devices
		// belong to the pool, and the order is the identity order resolution slices from.
		seed(t, "pool-3", "eu-west", "oak-s3-pool")
		seed(t, "pool-1", "eu-west", "oak-s3-pool")
		seed(t, "pool-2", "eu-west", "oak-s3-pool")
		seed(t, "pool-other-region", "us-east", "oak-s3-pool")
		seed(t, "pool-other-model", "eu-west", "oak-s3-pool-mini")

		pool, err := store.eligiblePool(ctx, "eu-west", "oak-s3-pool")
		if err != nil {
			t.Fatalf("eligiblePool() error = %v", err)
		}
		want := []string{"pool-1", "pool-2", "pool-3"}
		if len(pool) != len(want) {
			t.Fatalf("eligiblePool() = %v, want %v", pool, want)
		}
		for i, id := range want {
			if pool[i] != id {
				t.Fatalf("eligiblePool() = %v, want %v", pool, want)
			}
		}
	})

	t.Run("resolution records membership and start time", func(t *testing.T) {
		for _, id := range []string{"res-a", "res-b", "res-c", "res-d"} {
			seed(t, id, "eu-west", "oak-s3-resolve")
		}

		req := ResolveRequest{
			RolloutID: "ro-resolve", WaveID: WaveID("ro-resolve", 0, 25),
			Percent: 25, Region: "eu-west", Model: "oak-s3-resolve", StartedAt: startedAt,
		}
		got, err := store.ResolveWave(ctx, req)
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}
		// 25% of the four seeded devices, in identity order.
		if len(got.DeviceIDs) != 1 || got.DeviceIDs[0] != "res-a" {
			t.Errorf("resolved membership = %v, want [res-a]", got.DeviceIDs)
		}

		stored := readWave(t, req.WaveID)
		if stored.RolloutID != req.RolloutID || stored.Percent != 25 {
			t.Errorf("stored wave = %+v, want rollout %s at 25%%", stored, req.RolloutID)
		}
		if stored.Status != WaveDispatching {
			t.Errorf("stored wave status = %q, want %q", stored.Status, WaveDispatching)
		}
		if !stored.StartedAt.Equal(startedAt) {
			t.Errorf("stored wave started_at = %v, want %v", stored.StartedAt, startedAt)
		}
		if len(stored.DeviceIDs) != 1 || stored.DeviceIDs[0] != "res-a" {
			t.Errorf("stored wave membership = %v, want [res-a]", stored.DeviceIDs)
		}
	})

	t.Run("a second resolution returns the recorded membership", func(t *testing.T) {
		seed(t, "again-a", "eu-west", "oak-s3-again")
		seed(t, "again-b", "eu-west", "oak-s3-again")
		req := ResolveRequest{
			RolloutID: "ro-again", WaveID: WaveID("ro-again", 0, 50),
			Percent: 50, Region: "eu-west", Model: "oak-s3-again", StartedAt: startedAt,
		}
		first, err := store.ResolveWave(ctx, req)
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}

		// The pool grows between the attempts, and the retry asks for a later start: neither
		// may move membership or the recorded window, because the canary denominator cannot
		// move under a decision.
		seed(t, "again-c", "eu-west", "oak-s3-again")
		req.StartedAt = startedAt.Add(time.Hour)
		second, err := store.ResolveWave(ctx, req)
		if err != nil {
			t.Fatalf("ResolveWave() retry error = %v", err)
		}
		if len(second.DeviceIDs) != len(first.DeviceIDs) {
			t.Fatalf("retried membership = %v, want the recorded %v", second.DeviceIDs, first.DeviceIDs)
		}
		for i := range first.DeviceIDs {
			if second.DeviceIDs[i] != first.DeviceIDs[i] {
				t.Fatalf("retried membership = %v, want the recorded %v", second.DeviceIDs, first.DeviceIDs)
			}
		}
		if !second.StartedAt.Equal(startedAt) {
			t.Errorf("retried start = %v, want the recorded %v", second.StartedAt, startedAt)
		}
		count, err := waves.CountDocuments(ctx, bson.D{{Key: "rollout_id", Value: "ro-again"}})
		if err != nil {
			t.Fatalf("count waves: %v", err)
		}
		if count != 1 {
			t.Errorf("waves of ro-again = %d, want 1", count)
		}
	})

	t.Run("a wave at 100% takes the rest of the pool", func(t *testing.T) {
		for _, id := range []string{"rest-a", "rest-b", "rest-c", "rest-d"} {
			seed(t, id, "eu-west", "oak-s3-rest")
		}
		first, err := store.ResolveWave(ctx, ResolveRequest{
			RolloutID: "ro-rest", WaveID: WaveID("ro-rest", 0, 50),
			Percent: 50, Region: "eu-west", Model: "oak-s3-rest", StartedAt: startedAt,
		})
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}
		last, err := store.ResolveWave(ctx, ResolveRequest{
			RolloutID: "ro-rest", WaveID: WaveID("ro-rest", 1, 100),
			Percent: 100, Region: "eu-west", Model: "oak-s3-rest", StartedAt: startedAt,
		})
		if err != nil {
			t.Fatalf("ResolveWave() last wave error = %v", err)
		}
		if len(first.DeviceIDs)+len(last.DeviceIDs) != 4 {
			t.Errorf("waves targeted %d devices together, want the whole pool of 4",
				len(first.DeviceIDs)+len(last.DeviceIDs))
		}
		for _, id := range last.DeviceIDs {
			for _, earlier := range first.DeviceIDs {
				if id == earlier {
					t.Errorf("device %s is targeted by two waves", id)
				}
			}
		}
	})

	t.Run("a share that adds no device is recorded with an empty target set", func(t *testing.T) {
		seed(t, "tiny-1", "eu-west", "oak-s3-tiny")
		rec, err := store.ResolveWave(ctx, ResolveRequest{
			RolloutID: "ro-tiny", WaveID: WaveID("ro-tiny", 0, 1),
			Percent: 1, Region: "eu-west", Model: "oak-s3-tiny", StartedAt: startedAt,
		})
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}
		if len(rec.DeviceIDs) != 0 {
			t.Errorf("resolved membership = %v, want none", rec.DeviceIDs)
		}
		// The validator requires the array: an empty membership is stored as an empty array,
		// not as a missing field.
		stored := readWave(t, rec.ID)
		if stored.DeviceIDs == nil {
			t.Error("stored device_ids = nil, want an empty array")
		}
	})

	t.Run("a wave document missing a required field is rejected by the validator", func(t *testing.T) {
		_, err := waves.InsertOne(ctx, bson.D{
			{Key: "_id", Value: "wave-incomplete"},
			{Key: "rollout_id", Value: "ro-incomplete"},
			{Key: "percent", Value: 1},
			{Key: "status", Value: string(WaveDispatching)},
			{Key: "success_rate", Value: 0.0},
			{Key: "device_ids", Value: bson.A{}},
			// started_at is required and deliberately absent.
		})
		if err == nil {
			t.Fatal("inserting a wave without started_at = nil error, want a validator rejection")
		}
		if !isValidatorError(err) {
			t.Fatalf("insert error = %v, want the schema validator to reject it", err)
		}
	})

	t.Run("wave state writes move the record and converge", func(t *testing.T) {
		seed(t, "state-a", "eu-west", "oak-s3-state")
		rec, err := store.ResolveWave(ctx, ResolveRequest{
			RolloutID: "ro-state", WaveID: WaveID("ro-state", 0, 100),
			Percent: 100, Region: "eu-west", Model: "oak-s3-state", StartedAt: startedAt,
		})
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}
		update := WaveStateUpdate{
			RolloutID: "ro-state", WaveID: rec.ID,
			Status: WaveHealthy, SuccessRate: 0.97,
		}
		if err := store.RecordWaveState(ctx, update); err != nil {
			t.Fatalf("RecordWaveState() error = %v", err)
		}
		if err := store.RecordWaveState(ctx, update); err != nil {
			t.Fatalf("RecordWaveState() retry error = %v", err)
		}

		stored := readWave(t, rec.ID)
		if stored.Status != WaveHealthy || stored.SuccessRate != 0.97 {
			t.Errorf("stored wave = %+v, want healthy at 0.97", stored)
		}
		// The membership and the window a decision was measured over are never rewritten.
		if !stored.StartedAt.Equal(startedAt) || len(stored.DeviceIDs) != 1 {
			t.Errorf("stored wave = %+v, want its membership and start time unchanged", stored)
		}
	})

	t.Run("a wave records the devices that did not take the update", func(t *testing.T) {
		seed(t, "outcome-a", "eu-west", "oak-s3-outcome")
		seed(t, "outcome-b", "eu-west", "oak-s3-outcome")
		rec, err := store.ResolveWave(ctx, ResolveRequest{
			RolloutID: "ro-outcome", WaveID: WaveID("ro-outcome", 0, 100),
			Percent: 100, Region: "eu-west", Model: "oak-s3-outcome", StartedAt: startedAt,
		})
		if err != nil {
			t.Fatalf("ResolveWave() error = %v", err)
		}
		// A wave that has just been resolved has no outcomes yet, and both sets are stored
		// as empty arrays rather than null or absent.
		assertEmptyArrays(t, ctx, waves, rec.ID)

		update := WaveStateUpdate{
			RolloutID: "ro-outcome", WaveID: rec.ID,
			Status:              WaveEvaluating,
			FailedDeviceIDs:     []string{"outcome-a"},
			UnreportedDeviceIDs: []string{"outcome-b"},
		}
		if err := store.RecordWaveState(ctx, update); err != nil {
			t.Fatalf("RecordWaveState() error = %v", err)
		}
		stored := readWave(t, rec.ID)
		if want := []string{"outcome-a"}; !slices.Equal(stored.FailedDeviceIDs, want) {
			t.Errorf("stored failed_device_ids = %v, want %v", stored.FailedDeviceIDs, want)
		}
		if want := []string{"outcome-b"}; !slices.Equal(stored.UnreportedDeviceIDs, want) {
			t.Errorf("stored unreported_device_ids = %v, want %v", stored.UnreportedDeviceIDs, want)
		}

		// A wave whose devices all succeeded: the sets are written empty, not left behind.
		update.FailedDeviceIDs, update.UnreportedDeviceIDs = nil, nil
		if err := store.RecordWaveState(ctx, update); err != nil {
			t.Fatalf("RecordWaveState() clearing outcomes error = %v", err)
		}
		stored = readWave(t, rec.ID)
		if len(stored.FailedDeviceIDs) != 0 || len(stored.UnreportedDeviceIDs) != 0 {
			t.Errorf("stored wave = %+v, want both outcome sets empty", stored)
		}
		assertEmptyArrays(t, ctx, waves, rec.ID)
	})

	t.Run("a document written before the outcome sets existed stays updatable", func(t *testing.T) {
		// A wave document as the previous build wrote it: no outcome arrays at all. The new
		// build must still be able to record its state, and must fill the arrays in.
		waveID := WaveID("ro-legacy", 0, 100)
		if _, err := waves.InsertOne(ctx, bson.D{
			{Key: "_id", Value: waveID},
			{Key: "rollout_id", Value: "ro-legacy"},
			{Key: "percent", Value: 100},
			{Key: "status", Value: "dispatching"},
			{Key: "success_rate", Value: 0.0},
			{Key: "device_ids", Value: []string{}},
			{Key: "started_at", Value: startedAt},
		}); err != nil {
			t.Fatalf("insert legacy wave %s: %v", waveID, err)
		}

		if err := store.RecordWaveState(ctx, WaveStateUpdate{
			RolloutID: "ro-legacy", WaveID: waveID, Status: WaveFailed,
			FailedDeviceIDs: []string{"legacy-a"},
		}); err != nil {
			t.Fatalf("RecordWaveState() on a legacy document error = %v", err)
		}
		stored := readWave(t, waveID)
		if stored.Status != WaveFailed {
			t.Errorf("stored status = %q, want %q", stored.Status, WaveFailed)
		}
		if want := []string{"legacy-a"}; !slices.Equal(stored.FailedDeviceIDs, want) {
			t.Errorf("stored failed_device_ids = %v, want %v", stored.FailedDeviceIDs, want)
		}
		if stored.UnreportedDeviceIDs == nil {
			t.Error("stored unreported_device_ids is null, want an empty array")
		}
	})

	t.Run("recording an unknown wave fails", func(t *testing.T) {
		err := store.RecordWaveState(ctx, WaveStateUpdate{
			RolloutID: "ro-none", WaveID: "ro-none-w0-1", Status: WaveHealthy,
		})
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("RecordWaveState() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("recording a rollout creates it once and moves its status", func(t *testing.T) {
		rec := rolloutRecord("ro-life", RolloutRunning)
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() error = %v", err)
		}
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() retry error = %v", err)
		}
		count, err := rollouts.CountDocuments(ctx, bson.D{{Key: "_id", Value: rec.ID}})
		if err != nil {
			t.Fatalf("count rollouts: %v", err)
		}
		if count != 1 {
			t.Errorf("rollout documents = %d, want 1", count)
		}

		rec.Status = RolloutAwaitingApproval
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() status move error = %v", err)
		}
		stored := readRollout(t, rec.ID)
		if stored.Status != RolloutAwaitingApproval {
			t.Errorf("stored status = %q, want %q", stored.Status, RolloutAwaitingApproval)
		}
		if stored.FirmwareID != "fw-1" || stored.WorkflowID != "rollout-ro-life" ||
			stored.Region != "eu-west" || stored.Model != "oak-s3" {
			t.Errorf("stored rollout = %+v, want its identity fields unchanged", stored)
		}
	})

	t.Run("a terminal rollout status is not overwritten", func(t *testing.T) {
		rec := rolloutRecord("ro-terminal", RolloutRunning)
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() error = %v", err)
		}
		rec.Status = RolloutRolledBack
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() rollback error = %v", err)
		}

		// A late write of a state the concluded rollout has left changes nothing.
		rec.Status = RolloutRunning
		if err := store.RecordRollout(ctx, rec); err != nil {
			t.Fatalf("RecordRollout() after conclusion error = %v", err)
		}
		if stored := readRollout(t, rec.ID); stored.Status != RolloutRolledBack {
			t.Errorf("stored status = %q, want the terminal %q", stored.Status, RolloutRolledBack)
		}
	})

	t.Run("recording without an id is refused", func(t *testing.T) {
		if err := store.RecordRollout(ctx, RolloutRecord{Status: RolloutRunning}); err == nil {
			t.Error("RecordRollout() without id = nil error, want an error")
		}
		if _, err := store.ResolveWave(ctx, ResolveRequest{WaveID: "w"}); err == nil {
			t.Error("ResolveWave() without rollout id = nil error, want an error")
		}
	})
}

// isValidatorError reports whether err is a document-validation failure: the collection's schema
// validator rejects a malformed write with the server's validation error code.
func isValidatorError(err error) bool {
	var writeErr mongo.WriteException
	if !errors.As(err, &writeErr) {
		return false
	}
	for _, writeErr := range writeErr.WriteErrors {
		if writeErr.Code == 121 {
			return true
		}
	}
	return false
}
