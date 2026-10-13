//go:build integration

package devices

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// identityCrowd is how many other devices the lookup is planned among, so the plan is chosen for
// a populated collection rather than for an empty one.
const identityCrowd = 50

// TestDeviceIdentityLookup backs the mongo-data-model scenario "Device identity lookup": code
// resolving a device by its identity is served by the unique `_id` index on devices, and a second
// device document with the same identity cannot be inserted.
func TestDeviceIdentityLookup(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	devices := harness.DB.Collection("devices")

	now := time.Now().UTC()
	docs := make([]any, 0, identityCrowd+1)
	for i := range identityCrowd {
		docs = append(docs, bson.D{
			{Key: "_id", Value: fmt.Sprintf("identity-%02d", i)},
			{Key: "model", Value: "oak-s3"},
			{Key: "region", Value: "eu-west"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: StatusOnline},
			{Key: "last_heartbeat", Value: now},
		})
	}
	docs = append(docs, bson.D{
		{Key: "_id", Value: "dev-identity"},
		{Key: "model", Value: "oak-s3"},
		{Key: "region", Value: "eu-west"},
		{Key: "current_fw", Value: "1.0.0"},
		{Key: "status", Value: StatusOnline},
		{Key: "last_heartbeat", Value: now},
	})
	if _, err := devices.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the device registry: %v", err)
	}

	// The store resolves the device by its identity: the firmware reconciliation reads the
	// record's current version through the _id lookup this test asserts the plan of.
	state, err := store.ReconcileFirmware(ctx, "dev-identity", "1.0.0")
	if err != nil {
		t.Fatalf("ReconcileFirmware() error = %v", err)
	}
	if state != FirmwareAgreed {
		t.Errorf("ReconcileFirmware() = %s, want %s for the recorded version", state, FirmwareAgreed)
	}

	plan := mongotest.ExplainFind(t, harness.DB, "devices",
		bson.D{{Key: "_id", Value: "dev-identity"}}, nil)
	if !plan.Uses("_id_") {
		t.Errorf("resolving a device by its identity: plan = %+v, want the unique _id index", plan)
	}

	// The same identity cannot be written twice, which is what makes the resolution a point
	// lookup rather than the first of several answers.
	if _, err := devices.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "dev-identity"},
		{Key: "model", Value: "oak-s3"},
		{Key: "region", Value: "eu-west"},
		{Key: "current_fw", Value: "2.0.0"},
		{Key: "status", Value: StatusOnline},
		{Key: "last_heartbeat", Value: now},
	}); !mongo.IsDuplicateKeyError(err) {
		t.Errorf("inserting a second document for one identity = %v, want a duplicate-key error", err)
	}
	count, err := devices.CountDocuments(ctx, bson.D{{Key: "_id", Value: "dev-identity"}})
	if err != nil {
		t.Fatalf("count the device's documents: %v", err)
	}
	if count != 1 {
		t.Errorf("documents for one device identity = %d, want exactly 1", count)
	}
}
