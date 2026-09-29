//go:build integration

// Package mongotest boots a MongoDB test container carrying the real FleetOps schema: every
// Start applies deploy/mongo/init.js so integration tests write through the same validators,
// indexes, and TTL retention the local stack runs with. The harness also counts write
// commands, so tests can assert that ingestion batches instead of paying one round trip per
// event.
package mongotest

import (
	"context"
	"fmt"
	"io"
	"os"
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
	// database is the fleet database name the bootstrap populates.
	database = "fleetops"
	// cacheSizeGB caps the container's WiredTiger cache. MongoDB defaults to half the host's
	// memory per instance, which over-commits badly once several integration tests run their
	// containers in parallel on one machine: a capped cache keeps the whole suite reliable.
	cacheSizeGB = "0.25"
)

// Harness is a booted MongoDB carrying the FleetOps schema, with write-command counters that
// expose how many round trips the code under test actually paid.
type Harness struct {
	// DB is the fleet database.
	DB *mongo.Database

	mu      sync.Mutex
	inserts map[string]int
}

// InsertCommands reports how many insert commands reached collection since Start — one per
// batched write round trip, so fewer commands than events proves batching.
func (h *Harness) InsertCommands(collection string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inserts[collection]
}

// countInsert records one insert command against a collection.
func (h *Harness) countInsert(collection string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inserts[collection]++
}

// commandMonitor feeds the harness counters from the driver's command stream.
func commandMonitor(harness *Harness) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			if evt.CommandName != "insert" {
				return
			}
			if name := evt.Command.Lookup("insert"); name.Type == bson.TypeString {
				harness.countInsert(name.StringValue())
			}
		},
	}
}

// Start boots a MongoDB container, applies the schema bootstrap, and returns the harness. The
// container and the driver client are torn down via t.Cleanup.
func Start(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Cmd:          []string{"--wiredTigerCacheSizeGB", cacheSizeGB},
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(startupTimeout),
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

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("mongo container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "27017")
	if err != nil {
		t.Fatalf("mongo container port: %v", err)
	}

	harness := &Harness{inserts: make(map[string]int)}
	client, err := mongo.Connect(options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:%s", host, port.Port())).
		SetMonitor(commandMonitor(harness)))
	if err != nil {
		t.Fatalf("connect to mongo container: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Disconnect(context.Background()); err != nil {
			t.Logf("disconnect from mongo container: %v", err)
		}
	})

	applyBootstrap(ctx, t, container)
	harness.DB = client.Database(database)
	return harness
}

// applyBootstrap copies deploy/mongo/init.js into the container and runs it with mongosh.
func applyBootstrap(ctx context.Context, t *testing.T, container testcontainers.Container) {
	t.Helper()
	if _, err := os.Stat(initPath); err != nil {
		t.Fatalf("locate schema bootstrap %s: %v", initPath, err)
	}
	if err := container.CopyFileToContainer(ctx, initPath, "/tmp/init.js", initMode); err != nil {
		t.Fatalf("copy schema bootstrap: %v", err)
	}
	code, out, err := container.Exec(ctx, []string{"mongosh", "--quiet", "--file", "/tmp/init.js"})
	if err != nil {
		t.Fatalf("run schema bootstrap: %v", err)
	}
	if code != 0 {
		logged, readErr := io.ReadAll(out)
		if readErr != nil {
			t.Fatalf("schema bootstrap exited %d (output unreadable: %v)", code, readErr)
		}
		t.Fatalf("schema bootstrap exited %d: %s", code, logged)
	}
}
