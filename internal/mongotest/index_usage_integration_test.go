//go:build integration

package mongotest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// statusCrowd is how many documents each case writes without the filtered value, so the planner
// has a reason to reach for the status index rather than scanning the collection.
const statusCrowd = 50

// TestStatusQueriesUseStatusIndexes backs the mongo-data-model scenario "Status queries": devices
// listed by status, rollouts listed by status, and state snapshots listed by online are each
// served by a status index on the queried collection rather than by a collection scan.
func TestStatusQueriesUseStatusIndexes(t *testing.T) {
	t.Parallel()

	db := For(t).DB

	cases := []struct {
		// name is the subtest's name.
		name string
		// collection is the collection whose status index must serve the query.
		collection string
		// filter is the lookup the scenario describes: a status filter on the collection.
		filter bson.D
		// index is the index the winning plan must scan.
		index string
		// seed writes the documents the explain plans over.
		seed func(t *testing.T)
	}{
		{
			name: "devices listed by status", collection: "devices",
			filter: bson.D{{Key: "status", Value: "online"}}, index: "idx_status",
			seed: func(t *testing.T) { seedDevicesByStatus(t, db, "online", "offline") },
		},
		{
			name: "rollouts listed by status", collection: "rollouts",
			filter: bson.D{{Key: "status", Value: "running"}}, index: "idx_status",
			seed: func(t *testing.T) { seedRolloutsByStatus(t, db, "running", "completed") },
		},
		{
			name: "state snapshots listed by online", collection: "device_state_snapshots",
			filter: bson.D{{Key: "online", Value: true}}, index: "idx_online",
			seed: func(t *testing.T) { seedSnapshotsByOnline(t, db) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.seed(t)
			plan := ExplainFind(t, db, tc.collection, tc.filter, nil)
			if !plan.Uses(tc.index) {
				t.Errorf("%s filtered by %v: plan = %+v, want it served by %s",
					tc.collection, tc.filter, plan, tc.index)
			}
		})
	}

	// The control makes the assertions above a witness rather than an accident: the same explain
	// does report a collection scan when the collection carries no index for the query.
	t.Run("the explain reports a collection scan when no index serves the query", func(t *testing.T) {
		if _, err := db.Collection("devices").InsertOne(context.Background(), bson.D{
			{Key: "_id", Value: "status-control"},
			{Key: "model", Value: "oak-s3"},
			{Key: "region", Value: "eu-west"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "online"},
			{Key: "last_heartbeat", Value: time.Now().UTC()},
		}); err != nil {
			t.Fatalf("seed the control device: %v", err)
		}
		plan := ExplainFind(t, db, "devices", bson.D{{Key: "current_fw", Value: "1.0.0"}}, nil)
		if !plan.CollectionScan {
			t.Errorf("the control lookup over an unindexed field reported no collection scan: %+v", plan)
		}
	})
}

// seedDevicesByStatus writes one device carrying status among a crowd carrying other.
func seedDevicesByStatus(t *testing.T, db *mongo.Database, status, other string) {
	t.Helper()
	now := time.Now().UTC()
	docs := make([]any, 0, statusCrowd+1)
	for i := range statusCrowd {
		docs = append(docs, bson.D{
			{Key: "_id", Value: fmt.Sprintf("status-dev-%02d", i)},
			{Key: "model", Value: "oak-s3"},
			{Key: "region", Value: "eu-west"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: other},
			{Key: "last_heartbeat", Value: now},
		})
	}
	docs = append(docs, bson.D{
		{Key: "_id", Value: "status-dev-target"},
		{Key: "model", Value: "oak-s3"},
		{Key: "region", Value: "eu-west"},
		{Key: "current_fw", Value: "1.0.0"},
		{Key: "status", Value: status},
		{Key: "last_heartbeat", Value: now},
	})
	insertAll(t, db.Collection("devices"), docs)
}

// seedRolloutsByStatus writes one rollout carrying status among a crowd carrying other.
func seedRolloutsByStatus(t *testing.T, db *mongo.Database, status, other string) {
	t.Helper()
	docs := make([]any, 0, statusCrowd+1)
	for i := range statusCrowd {
		docs = append(docs, rolloutDocument(fmt.Sprintf("status-ro-%02d", i), other))
	}
	docs = append(docs, rolloutDocument("status-ro-target", status))
	insertAll(t, db.Collection("rollouts"), docs)
}

// rolloutDocument returns one complete rollout document in the given status.
func rolloutDocument(id, status string) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "firmware_id", Value: "fw-status"},
		{Key: "status", Value: status},
		{Key: "temporal_wf_id", Value: "rollout-" + id},
		{Key: "region", Value: "eu-west"},
		{Key: "model", Value: "oak-s3"},
	}
}

// seedSnapshotsByOnline writes one online snapshot among a crowd of offline ones.
func seedSnapshotsByOnline(t *testing.T, db *mongo.Database) {
	t.Helper()
	now := time.Now().UTC()
	snapshot := func(id string, online bool) bson.D {
		return bson.D{
			{Key: "_id", Value: id},
			{Key: "region", Value: "eu-west"},
			{Key: "model", Value: "oak-s3"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "online", Value: online},
			{Key: "last_heartbeat", Value: now},
			{Key: "config", Value: bson.D{{Key: "version", Value: 1}}},
			{Key: "snapshot_at", Value: now},
		}
	}
	docs := make([]any, 0, statusCrowd+1)
	for i := range statusCrowd {
		docs = append(docs, snapshot(fmt.Sprintf("status-snap-%02d", i), false))
	}
	docs = append(docs, snapshot("status-snap-target", true))
	insertAll(t, db.Collection("device_state_snapshots"), docs)
}

// insertAll writes every document in one batch, failing the test when the schema refuses any.
func insertAll(t *testing.T, coll *mongo.Collection, docs []any) {
	t.Helper()
	if _, err := coll.InsertMany(context.Background(), docs); err != nil {
		t.Fatalf("seed %s: %v", coll.Name(), err)
	}
}
