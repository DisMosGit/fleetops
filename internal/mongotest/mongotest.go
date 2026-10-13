//go:build integration

// Package mongotest boots a MongoDB test container carrying the real FleetOps schema: every
// Start applies deploy/mongo/init.js so integration tests write through the same validators,
// indexes, and TTL retention the local stack runs with. The harness also counts write
// commands, so tests can assert that ingestion batches instead of paying one round trip per
// event.
//
// A package's integration tests normally share one container through Shared — wired into
// testexec.Main as SharedStack, so it is started once before the package's first test body — and
// isolate themselves from each other by database name rather than by container. Start remains for
// a test that needs a service of its own, and For picks between the two from the run's
// configuration.
package mongotest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	// image is the MongoDB image the local stack runs.
	image = "mongo:7"
	// initPath is the schema bootstrap, relative to the working directory of the consuming
	// test package (every consumer sits at internal/<package>).
	initPath = "../../deploy/mongo/init.js"
	// initMode is the file mode the bootstrap script is copied into the container with.
	initMode = 0o600
	// startupTimeout bounds container and schema boot in tests.
	startupTimeout = 60 * time.Second
	// database is the fleet database name the bootstrap populates, and the prefix of the
	// per-test databases a shared stack hands out.
	database = "fleetops"
	// databaseEnv is the variable the bootstrap reads its target database from.
	databaseEnv = "FLEETOPS_DB"
	// cacheSizeGB caps the container's WiredTiger cache. MongoDB defaults to half the host's
	// memory per instance, which over-commits badly once several integration tests run their
	// containers in parallel on one machine: a capped cache keeps the whole suite reliable.
	cacheSizeGB = "0.25"
)

// Harness is a booted MongoDB carrying the FleetOps schema, with write-command counters that
// expose how many round trips the code under test actually paid.
type Harness struct {
	// DB is the fleet database this harness's test owns.
	DB *mongo.Database
	// Database is the name of the database DB points at. A caller that builds a configuration
	// from the harness names it, so the code under test writes where the test reads.
	Database string

	counts *counters
}

// counters counts commands per "database.collection", shared by every harness a stack hands out
// so one client's command stream serves them all. Inserts expose how many write round trips the
// code under test paid; finds expose whether it paid a query at all.
type counters struct {
	mu      sync.Mutex
	inserts map[string]int
	finds   map[string]int
}

// newCounters returns the counters a stack's client is monitored with.
func newCounters() *counters {
	return &counters{inserts: make(map[string]int), finds: make(map[string]int)}
}

// InsertCommands reports how many insert commands reached collection since the harness's
// database was created — one per batched write round trip, so fewer commands than events proves
// batching.
func (h *Harness) InsertCommands(collection string) int {
	h.counts.mu.Lock()
	defer h.counts.mu.Unlock()
	return h.counts.inserts[h.Database+"."+collection]
}

// FindCommands reports how many find commands reached collection since the harness's database was
// created. A wave resolution that reads a maintained view pays none; one that recomputes the pool
// pays a query per resolution, which is what a test asserting the view is read checks.
func (h *Harness) FindCommands(collection string) int {
	h.counts.mu.Lock()
	defer h.counts.mu.Unlock()
	return h.counts.finds[h.Database+"."+collection]
}

// countInsert records one insert command against a collection of the event's database.
func (c *counters) countInsert(databaseName, collection string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inserts[databaseName+"."+collection]++
}

// countFind records one find command against a collection of the event's database.
func (c *counters) countFind(databaseName, collection string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finds[databaseName+"."+collection]++
}

// commandMonitor feeds the stack counters from the driver's command stream. Both counted
// commands name their target collection in a field of their own name.
func commandMonitor(c *counters) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			switch evt.CommandName {
			case "insert", "find":
			default:
				return
			}
			collection := evt.Command.Lookup(evt.CommandName)
			if collection.Type != bson.TypeString {
				return
			}
			if evt.CommandName == "insert" {
				c.countInsert(evt.DatabaseName, collection.StringValue())
				return
			}
			c.countFind(evt.DatabaseName, collection.StringValue())
		},
	}
}

// Start boots a MongoDB container of its own, applies the schema bootstrap, and returns the
// harness. The container and the driver client are torn down via t.Cleanup. A test that does not
// need a private container should call For, which shares the package's stack instead.
func Start(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Cmd:          []string{"--wiredTigerCacheSizeGB", cacheSizeGB},
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(startupBudget(t)),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mongo container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate mongo container: %v", err)
		}
	})

	uri, err := containerURI(ctx, container)
	if err != nil {
		t.Fatalf("resolve mongo container address: %v", err)
	}

	counts := newCounters()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMonitor(commandMonitor(counts)))
	if err != nil {
		t.Fatalf("connect to mongo container: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Disconnect(context.Background()); err != nil {
			t.Logf("disconnect from mongo container: %v", err)
		}
	})

	bootstrap(ctx, t, container, database)
	return &Harness{DB: client.Database(database), Database: database, counts: counts}
}

// containerURI resolves a container's MongoDB URI from its mapped host port.
func containerURI(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("mongo container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "27017")
	if err != nil {
		return "", fmt.Errorf("mongo container port: %w", err)
	}
	return fmt.Sprintf("mongodb://%s:%s", host, port.Port()), nil
}
