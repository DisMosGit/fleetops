//go:build integration

package mongotest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// ttlDeadline bounds how long a test waits for the server's TTL sweeps to remove a document. A
// server whose sweep cadence the test could accelerate answers in about a second; the deadline
// covers one that refused and kept its default one-minute cadence.
const ttlDeadline = 2 * time.Minute

// TestDomainRecordsNeverExpire backs the mongo-data-model scenario "Domain records never expire":
// devices, firmware metadata, rollouts, waves, state snapshots, alerts, and the GridFS binary a
// firmware document references all outlive the retention period many times over, because no
// domain collection carries a TTL index.
func TestDomainRecordsNeverExpire(t *testing.T) {
	t.Parallel()

	db := For(t).DB
	ctx := context.Background()
	accelerateTTLMonitor(t, db)

	// Every domain record is dated ten years back — many times the configured seven-day retention
	// — so a collection that had acquired a TTL index would lose its document on the next sweep,
	// which the assertions below observe.
	aged := time.Now().UTC().AddDate(-10, 0, 0)
	const (
		binaryRef  = "fwbin-aging"
		binaryName = "firmware-aging"
		deviceID   = "dev-aging"
	)
	binary := []byte("fleetops domain retention binary")

	type domainRecord struct {
		collection string
		id         string
		document   bson.D
	}
	records := []domainRecord{
		{collection: "devices", id: deviceID, document: bson.D{
			{Key: "_id", Value: deviceID},
			{Key: "model", Value: "oak-s3"},
			{Key: "region", Value: "eu-west"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "offline"},
			{Key: "last_heartbeat", Value: aged},
		}},
		{collection: "device_state_snapshots", id: deviceID, document: bson.D{
			{Key: "_id", Value: deviceID},
			{Key: "region", Value: "eu-west"},
			{Key: "model", Value: "oak-s3"},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "online", Value: false},
			{Key: "last_heartbeat", Value: aged},
			{Key: "config", Value: bson.D{{Key: "version", Value: 1}}},
			{Key: "snapshot_at", Value: aged},
		}},
		{collection: "firmware", id: "fw-aging", document: bson.D{
			{Key: "_id", Value: "fw-aging"},
			{Key: "version", Value: "1.0.0"},
			{Key: "models", Value: bson.A{"oak-s3"}},
			{Key: "checksum", Value: "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"},
			{Key: "size", Value: int64(len(binary))},
			{Key: "created_at", Value: aged},
			{Key: "gridfs_id", Value: binaryRef},
		}},
		{collection: "rollouts", id: "ro-aging", document: bson.D{
			{Key: "_id", Value: "ro-aging"},
			{Key: "firmware_id", Value: "fw-aging"},
			{Key: "status", Value: "completed"},
			{Key: "temporal_wf_id", Value: "rollout-ro-aging"},
			{Key: "region", Value: "eu-west"},
			{Key: "model", Value: "oak-s3"},
		}},
		{collection: "waves", id: "ro-aging-w0-100", document: bson.D{
			{Key: "_id", Value: "ro-aging-w0-100"},
			{Key: "rollout_id", Value: "ro-aging"},
			{Key: "percent", Value: 100},
			{Key: "status", Value: "healthy"},
			{Key: "success_rate", Value: 1.0},
			{Key: "device_ids", Value: bson.A{deviceID}},
			{Key: "started_at", Value: aged},
		}},
		{collection: "device_alerts", id: deviceID, document: bson.D{
			{Key: "_id", Value: deviceID},
			{Key: "device_id", Value: deviceID},
			{Key: "region", Value: "eu-west"},
			{Key: "model", Value: "oak-s3"},
			{Key: "threshold", Value: 0.8},
			{Key: "min_health", Value: 0.5},
			{Key: "first_seen_at", Value: aged},
			{Key: "last_seen_at", Value: aged},
		}},
	}
	for _, rec := range records {
		if _, err := db.Collection(rec.collection).InsertOne(ctx, rec.document); err != nil {
			t.Fatalf("insert the aged %s record: %v", rec.collection, err)
		}
	}

	// The firmware metadata references a GridFS binary, which must stay stored as long as the
	// metadata does.
	stream, err := db.GridFSBucket().OpenUploadStreamWithID(ctx, binaryRef, binaryName)
	if err != nil {
		t.Fatalf("open the GridFS upload: %v", err)
	}
	if _, err := io.Copy(stream, bytes.NewReader(binary)); err != nil {
		if abortErr := stream.Abort(); abortErr != nil {
			t.Logf("abort the GridFS upload: %v", abortErr)
		}
		t.Fatalf("write the GridFS upload: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("finalize the GridFS upload: %v", err)
	}

	// Wait for a sweep the server really ran, rather than for a fixed pause that proves nothing
	// happened. The probe's own zero-second TTL is what observes the sweep.
	waitForTTLSweep(t, db)

	for _, rec := range records {
		err := db.Collection(rec.collection).FindOne(ctx, bson.D{{Key: "_id", Value: rec.id}}).Err()
		if err != nil {
			t.Errorf("after a TTL sweep the %s record %s is gone: %v", rec.collection, rec.id, err)
		}
	}

	reader, err := db.GridFSBucket().OpenDownloadStream(ctx, binaryRef)
	if err != nil {
		t.Errorf("after a TTL sweep the referenced GridFS binary is gone: %v", err)
	} else {
		stored, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		switch {
		case readErr != nil:
			t.Errorf("read the stored GridFS binary: %v", readErr)
		case closeErr != nil:
			t.Errorf("close the stored GridFS binary: %v", closeErr)
		case !bytes.Equal(stored, binary):
			t.Errorf("the stored GridFS binary = %q, want %q", stored, binary)
		}
	}

	// The records survive because nothing expires them: no index of a domain collection — nor of
	// the two GridFS collections — is a TTL index.
	for _, name := range []string{
		"devices", "device_state_snapshots", "firmware", "rollouts", "waves", "device_alerts",
		"fs.files", "fs.chunks",
	} {
		assertNoExpiringIndex(t, db.Collection(name))
	}
}

// TestLedgerRetentionIsBoundedAndConfigurable backs the mongo-data-model scenario "Retention is
// bounded and configurable in one place": the dedup ledger's retention is the one named value the
// bootstrap builds its TTL index from, a deployment bootstrapped with a changed value carries the
// new period, and the database removes a ledger document that ages past it with no cleanup job.
func TestLedgerRetentionIsBoundedAndConfigurable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := startSharedStack(ctx, testexec.Get(t))
	if err != nil {
		t.Fatalf("shared mongodb stack: %v", err)
	}

	t.Run("a changed retention value is what a new deployment builds", func(t *testing.T) {
		db := bootstrapWithLedgerRetention(t, ctx, st, "3")
		assertIndexTTL(t, db.Collection("processed_events"), "idx_claimed_at_ttl", 3*24*60*60)
		// Heartbeat retention is a value of its own: building the ledger's index from the changed
		// value leaves the telemetry period where it was rather than silently moving both.
		assertIndexTTL(t, db.Collection("telemetry"), "idx_ts_ttl", 7*24*60*60)
	})

	t.Run("the ledger removes a document that ages past the retention", func(t *testing.T) {
		db := bootstrapWithLedgerRetention(t, ctx, st, "0")
		assertIndexTTL(t, db.Collection("processed_events"), "idx_claimed_at_ttl", 0)
		accelerateTTLMonitor(t, db)

		claims := db.Collection("processed_events")
		const id = "retention-consumer:ev-aged"
		if _, err := claims.InsertOne(ctx, bson.D{
			{Key: "_id", Value: id},
			{Key: "consumer", Value: "retention-consumer"},
			{Key: "event_id", Value: "ev-aged"},
			{Key: "device_id", Value: "dev-aged"},
			{Key: "claimed_at", Value: time.Now().UTC().Add(-time.Hour)},
		}); err != nil {
			t.Fatalf("insert the aged claim: %v", err)
		}
		// Nothing runs a cleanup job here: the only thing that can remove the claim is the TTL
		// index the bootstrap built from the named retention value.
		waitGone(t, claims, id)
	})
}

// bootstrapWithLedgerRetention runs the schema bootstrap against a fresh database with the
// ledger's retention set to days, which is what a deployment bootstrapped with a changed value
// builds.
func bootstrapWithLedgerRetention(
	t *testing.T,
	ctx context.Context,
	st *stack,
	days string,
) *mongo.Database {
	t.Helper()
	name := st.newDatabaseName(t)
	assignment := "FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS=" + days
	if err := applyBootstrapWithEnv(ctx, st.container, name, []string{assignment}); err != nil {
		t.Fatalf("bootstrap %s with %s: %v", name, assignment, err)
	}
	return st.client.Database(name)
}

// accelerateTTLMonitor asks the server to run its TTL sweeps once a second instead of once a
// minute, so a test can watch a retention period elapse in seconds. A server that refuses the
// parameter keeps its own cadence: the waiters are bounded by their own deadline, so the
// assertion is the same either way, only slower.
func accelerateTTLMonitor(t *testing.T, db *mongo.Database) {
	t.Helper()
	err := db.Client().Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "setParameter", Value: 1},
		{Key: "ttlMonitorSleepSecs", Value: 1},
	}).Err()
	if err != nil {
		t.Logf("the server keeps its own TTL sweep cadence: %v", err)
	}
}

// waitForTTLSweep waits until the server has run a TTL sweep, which it observes through a probe
// document whose own zero-second TTL removes it. Only after a sweep has really run is "the domain
// records are still present" an observation rather than a pause that proved nothing.
func waitForTTLSweep(t *testing.T, db *mongo.Database) {
	t.Helper()
	ctx := context.Background()
	if err := db.CreateCollection(ctx, "ttl_sweep_probe"); err != nil {
		t.Fatalf("create the TTL sweep probe: %v", err)
	}
	probe := db.Collection("ttl_sweep_probe")
	if _, err := probe.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ts", Value: 1}},
		Options: options.Index().SetName("probe_ttl").SetExpireAfterSeconds(0),
	}); err != nil {
		t.Fatalf("index the TTL sweep probe: %v", err)
	}
	if _, err := probe.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "probe"},
		{Key: "ts", Value: time.Now().UTC().Add(-time.Hour)},
	}); err != nil {
		t.Fatalf("insert the TTL sweep probe: %v", err)
	}
	waitGone(t, probe, "probe")
}

// waitGone waits for the server to remove one document through a TTL sweep, failing the test when
// no sweep removes it within the deadline.
func waitGone(t *testing.T, coll *mongo.Collection, id string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(ttlDeadline)
	for {
		err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err()
		if errors.Is(err, mongo.ErrNoDocuments) {
			return
		}
		if err != nil {
			t.Fatalf("read %s of %s while waiting for its TTL sweep: %v", id, coll.Name(), err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still holds %s after %v: no TTL sweep removed it", coll.Name(), id, ttlDeadline)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// assertIndexTTL fails unless the named index of coll exists and expires documents after
// wantSeconds.
func assertIndexTTL(t *testing.T, coll *mongo.Collection, index string, wantSeconds int32) {
	t.Helper()
	for _, spec := range listIndexes(t, coll) {
		if spec["name"] != index {
			continue
		}
		got, ok := expireAfterSeconds(spec)
		if !ok {
			t.Errorf("%s.%s carries no expireAfterSeconds, want %d", coll.Name(), index, wantSeconds)
			return
		}
		if got != wantSeconds {
			t.Errorf("%s.%s expires after %ds, want %ds", coll.Name(), index, got, wantSeconds)
		}
		return
	}
	t.Errorf("%s.%s is missing: the collection has no retention", coll.Name(), index)
}

// assertNoExpiringIndex fails when any index of coll expires documents, which is what keeps a
// domain record a domain record.
func assertNoExpiringIndex(t *testing.T, coll *mongo.Collection) {
	t.Helper()
	for _, spec := range listIndexes(t, coll) {
		if _, ok := expireAfterSeconds(spec); ok {
			t.Errorf("%s index %v expires documents, want a permanent domain record",
				coll.Name(), spec["name"])
		}
	}
}

// expireAfterSeconds returns the TTL an index specification configures, if it configures one.
func expireAfterSeconds(spec bson.M) (int32, bool) {
	switch ttl := spec["expireAfterSeconds"].(type) {
	case int32:
		return ttl, true
	case int64:
		return int32(ttl), true
	default:
		return 0, false
	}
}
