//go:build integration

package mongotest

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestSchema verifies that Start hands out the real FleetOps schema: the fleet collections,
// the required-field validators, and the documented index set including telemetry retention
// and the consumer dedup ledger.
func TestSchema(t *testing.T) {
	t.Parallel()

	db := Start(t).DB
	ctx := context.Background()

	t.Run("fleet collections exist", func(t *testing.T) {
		names, err := db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			t.Fatalf("ListCollectionNames() error = %v", err)
		}
		present := make(map[string]bool, len(names))
		for _, name := range names {
			present[name] = true
		}
		for _, want := range []string{
			"devices", "firmware", "rollouts", "waves", "telemetry", "processed_events", "device_alerts",
		} {
			if !present[want] {
				t.Errorf("collection %q missing (have %v)", want, names)
			}
		}
	})

	t.Run("devices validator rejects incomplete documents", func(t *testing.T) {
		coll := db.Collection("devices")
		incomplete := bson.D{
			{Key: "_id", Value: "dev-incomplete"},
			{Key: "model", Value: "v3"},
		}
		if _, err := coll.InsertOne(ctx, incomplete); err == nil {
			t.Error("InsertOne(incomplete device) = nil error, want a validation error")
		}
		complete := bson.D{
			{Key: "_id", Value: "dev-complete"},
			{Key: "model", Value: "v3"},
			{Key: "region", Value: "eu-west"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "online"},
			{Key: "last_heartbeat", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, complete); err != nil {
			t.Errorf("InsertOne(complete device) error = %v, want nil", err)
		}
	})

	t.Run("waves validator requires membership and start time", func(t *testing.T) {
		coll := db.Collection("waves")
		complete := bson.D{
			{Key: "_id", Value: "wave-complete"},
			{Key: "rollout_id", Value: "roll-complete"},
			{Key: "percent", Value: 5},
			{Key: "status", Value: "running"},
			{Key: "success_rate", Value: 0.0},
			{Key: "device_ids", Value: bson.A{"dev-1", "dev-2"}},
			{Key: "started_at", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, complete); err != nil {
			t.Fatalf("InsertOne(complete wave) error = %v, want nil", err)
		}

		// A canary share smaller than one device targets nobody: that wave is recorded with an
		// empty membership rather than refused, so the validator must allow the empty array.
		empty := bson.D{
			{Key: "_id", Value: "wave-empty"},
			{Key: "rollout_id", Value: "roll-complete"},
			{Key: "percent", Value: 1},
			{Key: "status", Value: "running"},
			{Key: "success_rate", Value: 0.0},
			{Key: "device_ids", Value: bson.A{}},
			{Key: "started_at", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, empty); err != nil {
			t.Errorf("InsertOne(wave targeting no devices) error = %v, want nil", err)
		}

		// Each field is required on its own, so a partial wave that would make health
		// evaluation guess its membership or its window cannot be stored.
		for _, missing := range []string{"device_ids", "started_at"} {
			doc := bson.D{{Key: "_id", Value: "wave-missing-" + missing}}
			for _, el := range complete {
				if el.Key != missing && el.Key != "_id" {
					doc = append(doc, el)
				}
			}
			if _, err := coll.InsertOne(ctx, doc); err == nil {
				t.Errorf("InsertOne(wave without %s) = nil error, want a validation error", missing)
			}
		}
	})

	t.Run("index set is present", func(t *testing.T) {
		want := map[string]bool{
			"idx_region_model": false,
			"idx_status":       false,
		}
		assertIndexNames(t, db.Collection("devices"), want)

		telemetry := map[string]bool{
			"idx_device_ts":       false,
			"idx_region_model_ts": false,
			"idx_ts_ttl":          false,
		}
		assertIndexNames(t, db.Collection("telemetry"), telemetry)
	})

	t.Run("telemetry ttl retention is configured", func(t *testing.T) {
		for _, spec := range listIndexes(t, db.Collection("telemetry")) {
			if spec["name"] != "idx_ts_ttl" {
				continue
			}
			if _, ok := spec["expireAfterSeconds"]; !ok {
				t.Error("idx_ts_ttl has no expireAfterSeconds")
			}
			return
		}
		t.Error("idx_ts_ttl not found")
	})

	t.Run("processed events validator requires the ledger fields", func(t *testing.T) {
		coll := db.Collection("processed_events")
		incomplete := bson.D{
			{Key: "_id", Value: "mongotest:ev-incomplete"},
			{Key: "consumer", Value: "mongotest"},
		}
		if _, err := coll.InsertOne(ctx, incomplete); err == nil {
			t.Error("InsertOne(incomplete ledger claim) = nil error, want a validation error")
		}
		complete := bson.D{
			{Key: "_id", Value: "mongotest:ev-complete"},
			{Key: "consumer", Value: "mongotest"},
			{Key: "event_id", Value: "ev-complete"},
			{Key: "device_id", Value: "dev-ledger"},
			{Key: "claimed_at", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, complete); err != nil {
			t.Errorf("InsertOne(complete ledger claim) error = %v, want nil", err)
		}
	})

	t.Run("processed events uniqueness is enforced by the store", func(t *testing.T) {
		coll := db.Collection("processed_events")
		claim := bson.D{
			{Key: "_id", Value: "mongotest:ev-duplicate"},
			{Key: "consumer", Value: "mongotest"},
			{Key: "event_id", Value: "ev-duplicate"},
			{Key: "device_id", Value: "dev-ledger"},
			{Key: "claimed_at", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, claim); err != nil {
			t.Fatalf("InsertOne(first claim) error = %v", err)
		}
		// A different _id, same (consumer, event_id): the unique index must refuse it.
		second := bson.D{
			{Key: "_id", Value: "mongotest:ev-duplicate-other-id"},
			{Key: "consumer", Value: "mongotest"},
			{Key: "event_id", Value: "ev-duplicate"},
			{Key: "device_id", Value: "dev-ledger"},
			{Key: "claimed_at", Value: time.Now()},
		}
		if _, err := coll.InsertOne(ctx, second); !mongo.IsDuplicateKeyError(err) {
			t.Errorf("InsertOne(second claim) error = %v, want a duplicate-key error", err)
		}
	})

	t.Run("processed events ttl retention is configured", func(t *testing.T) {
		for _, spec := range listIndexes(t, db.Collection("processed_events")) {
			if spec["name"] != "idx_claimed_at_ttl" {
				continue
			}
			if _, ok := spec["expireAfterSeconds"]; !ok {
				t.Error("idx_claimed_at_ttl has no expireAfterSeconds")
			}
			return
		}
		t.Error("idx_claimed_at_ttl not found")
	})

	t.Run("device alerts carry no ttl", func(t *testing.T) {
		want := map[string]bool{"idx_region_model": false}
		assertIndexNames(t, db.Collection("device_alerts"), want)
		for _, spec := range listIndexes(t, db.Collection("device_alerts")) {
			if _, ok := spec["expireAfterSeconds"]; ok {
				t.Errorf("device_alerts index %v expires, want a permanent domain record", spec["name"])
			}
		}
	})
}

// assertIndexNames reports any wanted index name missing from coll.
func assertIndexNames(t *testing.T, coll *mongo.Collection, want map[string]bool) {
	t.Helper()
	for _, spec := range listIndexes(t, coll) {
		name, _ := spec["name"].(string)
		if _, known := want[name]; known {
			want[name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("index %q missing", name)
		}
	}
}

// listIndexes returns raw index specifications of coll.
func listIndexes(t *testing.T, coll *mongo.Collection) []bson.M {
	t.Helper()
	ctx := context.Background()
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var specs []bson.M
	if err := cursor.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	return specs
}
