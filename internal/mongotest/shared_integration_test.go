//go:build integration

package mongotest

import (
	"context"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's MongoDB stack once, before the first test body, and releases it
// when the run ends.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m, SharedStack()))
}

// TestSharedGivesEveryTestItsOwnDatabase asserts what a shared stack is for: one container serves
// the whole package, and no test can see another test's documents. The two halves run as separate
// tests, which is exactly the situation the isolation has to hold in.
func TestSharedGivesEveryTestItsOwnDatabase(t *testing.T) {
	t.Parallel()

	var firstDatabase string
	var firstClient, secondClient *mongo.Client

	t.Run("the first test writes a device of its own", func(t *testing.T) {
		harness := Shared(t)
		firstDatabase, firstClient = harness.Database, harness.DB.Client()

		if harness.Database == database {
			t.Errorf("shared database = %q, want a database of this test's own", harness.Database)
		}
		if got := harness.DB.Name(); got != harness.Database {
			t.Errorf("DB.Name() = %q, want the harness's Database %q", got, harness.Database)
		}
		if got := harness.InsertCommands("devices"); got != 0 {
			t.Errorf("a fresh database reports %d insert commands, want 0", got)
		}
		insertDevice(t, harness, "dev-shared-first")
		if got := harness.InsertCommands("devices"); got != 1 {
			t.Errorf("after one insert the database reports %d insert commands, want 1", got)
		}
	})

	t.Run("the second test sees the schema and none of the first test's documents", func(t *testing.T) {
		harness := Shared(t)
		secondClient = harness.DB.Client()

		if harness.Database == firstDatabase {
			t.Fatalf("both tests ran in database %q, want one database per test", harness.Database)
		}
		err := harness.DB.Collection("devices").
			FindOne(context.Background(), bson.D{{Key: "_id", Value: "dev-shared-first"}}).Err()
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Errorf("reading the other test's device: %v, want no documents", err)
		}
		// The database is empty but not bare: the bootstrap ran against it too, so a test
		// writes through the real validators rather than into an unvalidated collection.
		if _, err := harness.DB.Collection("devices").
			InsertOne(context.Background(), bson.D{{Key: "_id", Value: "dev-shared-incomplete"}}); err == nil {
			t.Error("the shared database accepted an incomplete device, want the devices validator")
		}
		insertDevice(t, harness, "dev-shared-second")
	})

	if firstClient != secondClient {
		t.Error("the two tests did not share one container and client")
	}
}

// TestForHonoursTheContainerUse asserts the profile decides how a test obtains MongoDB: a run
// that shares gets a database on the package's stack, and a run that asks for a private stack per
// test gets a container of its own in the default database.
func TestForHonoursTheContainerUse(t *testing.T) {
	shared := For(t)
	if !testexec.Get(t).SharesContainers() {
		t.Fatal("an unconfigured run did not share the package's stack")
	}
	if shared.Database == database {
		t.Errorf("the shared path handed out the default database %q", database)
	}

	t.Run("a run asking for a private stack gets one", func(t *testing.T) {
		t.Setenv(testexec.EnvContainerUse, testexec.ContainersPerTest)
		private := For(t)
		if private.Database != database {
			t.Errorf("private stack database = %q, want %q", private.Database, database)
		}
		if private.DB.Client() == shared.DB.Client() {
			t.Error("the private stack reused the shared stack's client")
		}
		insertDevice(t, private, "dev-private-stack")
	})
}

// TestSharedDatabaseNamesAreSane asserts the naming rule the isolation rests on: every database a
// stack hands out is distinct, and a subtest's name survives the trip without characters the
// server would refuse.
func TestSharedDatabaseNamesAreSane(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for range 3 {
		name := Shared(t).Database
		if seen[name] {
			t.Fatalf("database %q was handed out twice", name)
		}
		seen[name] = true
		if !databaseNamePattern.MatchString(name) {
			t.Errorf("database name %q is not a name the server accepts", name)
		}
	}
}

// databaseNamePattern is what a database name may hold, within the server's 63-byte limit: the
// shared stack rewrites a test name into this set rather than trusting it.
var databaseNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,63}$`)

// insertDevice writes one complete device document, which is what the devices validator accepts.
func insertDevice(t *testing.T, harness *Harness, id string) {
	t.Helper()
	_, err := harness.DB.Collection("devices").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: id},
		{Key: "model", Value: "v3"},
		{Key: "region", Value: "eu-west"},
		{Key: "current_fw", Value: "1.0.0"},
		{Key: "status", Value: "online"},
		{Key: "last_heartbeat", Value: time.Now().UTC()},
	})
	if err != nil {
		t.Fatalf("insert device %s: %v", id, err)
	}
}

// TestSharedDatabaseCarriesTheTemplatesSchema asserts that the copy which makes a shared database
// cheap is faithful: every collection a test gets carries the template's validator options and
// index set, so the schema stays defined once in deploy/mongo/init.js instead of being restated in
// Go, and a collection option the clone does not understand fails here rather than going missing.
func TestSharedDatabaseCarriesTheTemplatesSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := startSharedStack(ctx, testexec.Get(t))
	if err != nil {
		t.Fatalf("shared stack: %v", err)
	}
	harness := Shared(t)

	templateInfos, err := collectionInfos(ctx, st.template())
	if err != nil {
		t.Fatalf("read the template schema: %v", err)
	}
	cloneInfos, err := collectionInfos(ctx, harness.DB)
	if err != nil {
		t.Fatalf("read the shared database's schema: %v", err)
	}
	if len(templateInfos) == 0 {
		t.Fatal("the template database carries no collections")
	}

	clone := make(map[string]collectionInfo, len(cloneInfos))
	for _, info := range cloneInfos {
		clone[info.Name] = info
	}
	for _, want := range templateInfos {
		got, ok := clone[want.Name]
		if !ok {
			t.Errorf("collection %s is missing from the shared database", want.Name)
			continue
		}
		if diff := cmp.Diff(want.Options, got.Options); diff != "" {
			t.Errorf("collection %s options mismatch (-template +clone):\n%s", want.Name, diff)
		}
		wantIndexes := indexShapes(t, st.template().Collection(want.Name))
		gotIndexes := indexShapes(t, harness.DB.Collection(want.Name))
		if diff := cmp.Diff(wantIndexes, gotIndexes); diff != "" {
			t.Errorf("collection %s index mismatch (-template +clone):\n%s", want.Name, diff)
		}
	}
}

// indexShape is the part of an index a clone has to reproduce, with the server's own bookkeeping
// fields left out.
type indexShape struct {
	// Key is the index's key document, printed by the driver's own extended JSON.
	Key string
	// Unique reports whether the index enforces uniqueness.
	Unique bool
	// Sparse reports whether the index skips documents without its key.
	Sparse bool
	// TTL is the index's expireAfterSeconds, or zero when it does not expire.
	TTL int32
	// Partial reports whether the index carries a partial filter expression.
	Partial bool
}

// indexShapes returns each index of coll by name, in a form two databases can be compared by.
func indexShapes(t *testing.T, coll *mongo.Collection) map[string]indexShape {
	t.Helper()
	ctx := context.Background()
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes of %s: %v", coll.Name(), err)
	}
	var specs []bson.M
	if err := cursor.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes of %s: %v", coll.Name(), err)
	}
	shapes := make(map[string]indexShape, len(specs))
	for _, spec := range specs {
		name, _ := spec["name"].(string)
		shape := indexShape{}
		if key, err := bson.MarshalExtJSON(spec["key"], false, false); err == nil {
			shape.Key = string(key)
		}
		shape.Unique, _ = spec["unique"].(bool)
		shape.Sparse, _ = spec["sparse"].(bool)
		switch ttl := spec["expireAfterSeconds"].(type) {
		case int32:
			shape.TTL = ttl
		case int64:
			shape.TTL = int32(ttl)
		}
		_, shape.Partial = spec["partialFilterExpression"]
		shapes[name] = shape
	}
	return shapes
}
