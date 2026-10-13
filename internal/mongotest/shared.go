//go:build integration

package mongotest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The shared stack is package-level state, which is what a container shared by a package's test
// functions requires: Go gives a test no way to hand a value to the next test, so the one stack a
// package starts lives here and every test reaches it through Shared or For.
var (
	// sharedOnce guards the standalone stack's single start.
	sharedOnce sync.Once
	// sharedStack is the started standalone stack, valid once sharedErr is nil.
	sharedStack *stack
	// sharedErr is why the standalone stack could not start, kept so every test reports the
	// same reason rather than starting a container of its own to fail differently.
	sharedErr error
)

// stack is one container and the client a package's tests share through it.
type stack struct {
	container testcontainers.Container
	client    *mongo.Client
	counts    *counters
	// databases numbers the databases the stack hands out, so no two tests share one.
	databases atomic.Int64
}

// stackOptions configure how a shared container is started.
type stackOptions struct {
	// replSet names the replica set the container initiates itself as. Change streams need a
	// replica set, so a stack that must serve one is started this way; empty means a
	// standalone, which is what most tests need.
	replSet string
}

// SharedStack is the stack an integration package wires into testexec.Main, so this package's
// container is started once before the first test body and released when the run ends:
//
//	func TestMain(m *testing.M) { os.Exit(testexec.Main(m, mongotest.SharedStack())) }
//
// A package that does not wire it still shares one stack: Shared starts it on first use.
func SharedStack() testexec.Stack {
	return testexec.Stack{Name: "mongodb", Start: func(ctx context.Context) (func(), error) {
		cfg, err := testexec.Resolve()
		if err != nil {
			return nil, err
		}
		st, err := startSharedStack(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return func() { st.close() }, nil
	}}
}

// Shared returns the package's shared stack with a database of this test's own, carrying the
// schema bootstrap. The container is started on first use and reused by every later test, so a
// package starts one stack however many tests it has.
func Shared(t *testing.T) *Harness {
	t.Helper()
	st, err := startSharedStack(context.Background(), testexec.Get(t))
	if err != nil {
		t.Fatalf("shared mongodb stack: %v", err)
	}
	return st.newHarness(t)
}

// For returns the harness a test should use: a database of its own on the package's shared stack
// by default, or a container of its own when the run asked for a private stack per test. It is
// what an integration test calls, so the run's configuration decides how the stack is obtained
// and the test's own assertions are the same either way.
func For(t *testing.T) *Harness {
	t.Helper()
	if !testexec.Get(t).SharesContainers() {
		return Start(t)
	}
	return Shared(t)
}

// startSharedStack starts the package's standalone stack once, returning the same stack — or the
// same failure — to every caller.
func startSharedStack(ctx context.Context, cfg testexec.Config) (*stack, error) {
	sharedOnce.Do(func() { sharedStack, sharedErr = startStack(ctx, stackOptions{}, cfg) })
	return sharedStack, sharedErr
}

// startStack boots the container and connects the client the stack's harnesses share. Nothing is
// released here: the caller owns the stack's lifetime, and the databases it hands out are created
// as tests ask for them.
func startStack(ctx context.Context, opts stackOptions, cfg testexec.Config) (*stack, error) {
	cmd := []string{"--wiredTigerCacheSizeGB", cacheSizeGB}
	if opts.replSet != "" {
		cmd = append(cmd, "--replSet", opts.replSet)
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Cmd:          cmd,
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(cfg.Startup(startupTimeout)),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start mongo container: %w", err)
	}
	uri, err := containerURI(ctx, container)
	if err != nil {
		terminate(ctx, container)
		return nil, err
	}
	if opts.replSet != "" {
		// The initiated member advertises localhost:27017, which is the port inside the
		// container: the client dials the mapped port, so it must connect to that one address
		// directly instead of following the member's own.
		uri += "/?directConnection=true"
	}
	counts := newCounters()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMonitor(commandMonitor(counts)))
	if err != nil {
		terminate(ctx, container)
		return nil, fmt.Errorf("connect to mongo container: %w", err)
	}
	st := &stack{container: container, client: client, counts: counts}
	if opts.replSet != "" {
		if err := initiateReplicaSet(ctx, container, opts.replSet); err != nil {
			st.close()
			return nil, err
		}
	}
	// The bootstrap runs once per stack, against the template database every test database is
	// cloned from. Running it per test instead would pay a mongosh execution for every test in
	// the package, which costs more time than sharing the container saves.
	if err := applyBootstrap(ctx, container, database); err != nil {
		st.close()
		return nil, err
	}
	return st, nil
}

// template is the bootstrapped database a stack clones each test's database from.
func (s *stack) template() *mongo.Database {
	return s.client.Database(database)
}

// newHarness returns a harness over a database of this test's own, carrying the schema the
// bootstrap defined, so a test sees the real validators and indexes and none of another test's
// documents.
func (s *stack) newHarness(t *testing.T) *Harness {
	t.Helper()
	name := s.newDatabaseName(t)
	target := s.client.Database(name)
	if err := cloneSchema(context.Background(), s.template(), target); err != nil {
		t.Fatalf("prepare shared database %s: %v", name, err)
	}
	return &Harness{DB: target, Database: name, counts: s.counts}
}

// cloneSchema copies the template database's collection options and indexes into a fresh database.
// The schema itself stays defined in deploy/mongo/init.js: this copies what the server reports the
// bootstrap produced, and the schema test asserts a clone carries exactly that, so a collection
// option this copy does not understand fails the suite instead of going missing unnoticed.
func cloneSchema(ctx context.Context, template, target *mongo.Database) error {
	infos, err := collectionInfos(ctx, template)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if err := createCollectionLike(ctx, target, info); err != nil {
			return err
		}
		if err := createIndexesLike(ctx, template, target, info.Name); err != nil {
			return err
		}
	}
	return nil
}

// collectionInfo is what a template collection carries that a clone has to reproduce: its name and
// the validator options the bootstrap declares.
type collectionInfo struct {
	// Name is the collection's name.
	Name string `bson:"name"`
	// Type is the server's kind for it; only plain collections are cloned.
	Type string `bson:"type"`
	// Options holds the collection's validator settings.
	Options struct {
		// Validator is the $jsonSchema document, or nil when the collection has none.
		Validator any `bson:"validator"`
		// ValidationLevel is the level the bootstrap set (strict, for the fleet collections).
		ValidationLevel string `bson:"validationLevel"`
		// ValidationAction is the action the bootstrap set (error, never warn).
		ValidationAction string `bson:"validationAction"`
	} `bson:"options"`
}

// collectionInfos lists the template's collections with their options.
func collectionInfos(ctx context.Context, template *mongo.Database) ([]collectionInfo, error) {
	cursor, err := template.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list template collections: %w", err)
	}
	var infos []collectionInfo
	if err := cursor.All(ctx, &infos); err != nil {
		return nil, fmt.Errorf("decode template collections: %w", err)
	}
	return infos, nil
}

// createCollectionLike creates the clone's collection with the template's validator options.
func createCollectionLike(ctx context.Context, target *mongo.Database, info collectionInfo) error {
	if info.Type != "" && info.Type != "collection" {
		return nil
	}
	opts := options.CreateCollection()
	if info.Options.Validator != nil {
		opts.SetValidator(info.Options.Validator)
	}
	if info.Options.ValidationLevel != "" {
		opts.SetValidationLevel(info.Options.ValidationLevel)
	}
	if info.Options.ValidationAction != "" {
		opts.SetValidationAction(info.Options.ValidationAction)
	}
	if err := target.CreateCollection(ctx, info.Name, opts); err != nil {
		return fmt.Errorf("create collection %s: %w", info.Name, err)
	}
	return nil
}

// createIndexesLike recreates the template collection's indexes on the clone, keeping the names,
// keys, uniqueness, and retention the bootstrap gave them.
func createIndexesLike(ctx context.Context, template, target *mongo.Database, name string) error {
	cursor, err := template.Collection(name).Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes of %s: %w", name, err)
	}
	var specs []bson.M
	if err := cursor.All(ctx, &specs); err != nil {
		return fmt.Errorf("decode indexes of %s: %w", name, err)
	}
	models := make([]mongo.IndexModel, 0, len(specs))
	for _, spec := range specs {
		if spec["name"] == "_id_" {
			continue
		}
		models = append(models, mongo.IndexModel{Keys: spec["key"], Options: indexOptions(spec)})
	}
	if len(models) == 0 {
		return nil
	}
	if _, err := target.Collection(name).Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("create indexes of %s: %w", name, err)
	}
	return nil
}

// indexOptions maps the index settings a clone must keep onto the driver's options. It handles the
// settings the bootstrap uses and no others: an index the bootstrap gains later either lands in
// this list or fails the schema test that compares a clone against its template.
func indexOptions(spec bson.M) *options.IndexOptionsBuilder {
	opts := options.Index()
	if name, ok := spec["name"].(string); ok {
		opts.SetName(name)
	}
	if unique, ok := spec["unique"].(bool); ok {
		opts.SetUnique(unique)
	}
	if sparse, ok := spec["sparse"].(bool); ok {
		opts.SetSparse(sparse)
	}
	switch ttl := spec["expireAfterSeconds"].(type) {
	case int32:
		opts.SetExpireAfterSeconds(ttl)
	case int64:
		opts.SetExpireAfterSeconds(int32(ttl))
	}
	if partial, ok := spec["partialFilterExpression"]; ok {
		opts.SetPartialFilterExpression(partial)
	}
	return opts
}

// newDatabaseName returns a database name no other test of this run uses. The name carries the
// test it belongs to, so a database left behind in a failed run says where it came from.
func (s *stack) newDatabaseName(t *testing.T) string {
	t.Helper()
	name := sanitizeDatabaseName(t.Name())
	return fmt.Sprintf("%s_test_%d_%s", database, s.databases.Add(1), name)
}

// close releases the stack's client and container. A stack is released once, when the run ends,
// so failures are reported where a test binary's own output can be read.
func (s *stack) close() {
	if s.client != nil {
		if err := s.client.Disconnect(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "shared mongodb stack: disconnect: %v\n", err)
		}
	}
	if s.container != nil {
		terminate(context.Background(), s.container)
	}
}

// terminate removes a container, which is the only cleanup a stack failure has left to try.
func terminate(ctx context.Context, container testcontainers.Container) {
	if err := container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "shared mongodb stack: terminate container: %v\n", err)
	}
}

// sanitizeDatabaseName reduces a test name to what a database name may carry, so a subtest's
// slashes and spaces do not end up in a name the server rejects.
func sanitizeDatabaseName(name string) string {
	mapped := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
	if len(mapped) > maxDatabaseNameSuffix {
		mapped = mapped[:maxDatabaseNameSuffix]
	}
	if mapped == "" {
		return "test"
	}
	return mapped
}

// maxDatabaseNameSuffix bounds the test-name part of a database name, leaving room for the
// prefix and the counter inside the server's 63-byte limit.
const maxDatabaseNameSuffix = 40

// startupBudget is the budget a container started by Start is given to become ready.
func startupBudget(t *testing.T) time.Duration {
	t.Helper()
	return testexec.Get(t).Startup(startupTimeout)
}

// initiateReplicaSet makes the container's single member the primary of its replica set, which is
// the state a change stream requires. It is idempotent: a member that is already initiated is
// left alone.
func initiateReplicaSet(ctx context.Context, container testcontainers.Container, name string) error {
	eval := fmt.Sprintf("rs.initiate({_id: %q, members: [{_id: 0, host: 'localhost:27017'}]})", name)
	code, out, err := execOutput(ctx, container, []string{"mongosh", "--quiet", "--eval", eval})
	if err != nil {
		return fmt.Errorf("initiate replica set %s: %w", name, err)
	}
	if code != 0 {
		return fmt.Errorf("initiate replica set %s: mongosh exited %d: %s", name, code, out)
	}
	return awaitPrimary(ctx, container)
}

// awaitPrimary waits until the initiated member reports itself as the primary, so a stack is
// never handed to a test before it can serve a change stream.
func awaitPrimary(ctx context.Context, container testcontainers.Container) error {
	deadline := time.Now().Add(primaryBudget)
	var last string
	for {
		code, out, err := execOutput(ctx, container,
			[]string{"mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary"})
		if err == nil && code == 0 && strings.Contains(out, "true") {
			return nil
		}
		last = fmt.Sprintf("exit %d: %s (%v)", code, strings.TrimSpace(out), err)
		if time.Now().After(deadline) {
			return fmt.Errorf("replica set did not elect a primary within %v: %s", primaryBudget, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("replica set did not elect a primary: %w", ctx.Err())
		case <-time.After(primaryPoll):
		}
	}
}

// applyBootstrap copies deploy/mongo/init.js into the container and runs it against the named
// database with mongosh, so that database carries the real validators, indexes, and retention. It
// reports why it could not, because a stack is also bootstrapped before any test body exists to
// fail.
func applyBootstrap(ctx context.Context, container testcontainers.Container, name string) error {
	if _, err := os.Stat(initPath); err != nil {
		return fmt.Errorf("locate schema bootstrap %s: %w", initPath, err)
	}
	if err := container.CopyFileToContainer(ctx, initPath, "/tmp/init.js", initMode); err != nil {
		return fmt.Errorf("copy schema bootstrap: %w", err)
	}
	// The database is named through the environment rather than the command line, because the
	// bootstrap is a file: the shell prefix is what mongosh reads the variable from.
	code, out, err := execOutput(ctx, container, []string{
		"sh", "-c", fmt.Sprintf("%s=%s mongosh --quiet --file /tmp/init.js", databaseEnv, name),
	})
	if err != nil {
		return fmt.Errorf("run schema bootstrap on %s: %w", name, err)
	}
	if code != 0 {
		return fmt.Errorf("schema bootstrap on %s exited %d: %s", name, code, strings.TrimSpace(out))
	}
	return nil
}

// bootstrap runs the schema bootstrap against name, failing t when it cannot: Start has a test to
// report the failure through.
func bootstrap(ctx context.Context, t *testing.T, container testcontainers.Container, name string) {
	t.Helper()
	if err := applyBootstrap(ctx, container, name); err != nil {
		t.Fatalf("%v", err)
	}
}

// primaryBudget bounds waiting for a replica set to elect its primary.
const primaryBudget = 30 * time.Second

// primaryPoll is the pause between primary checks.
const primaryPoll = 200 * time.Millisecond

// execOutput runs a command in the container and returns its exit code with its output as text.
func execOutput(ctx context.Context, container testcontainers.Container, cmd []string) (int, string, error) {
	code, out, err := container.Exec(ctx, cmd)
	if out == nil {
		return code, "", err
	}
	logged, readErr := io.ReadAll(out)
	if readErr != nil {
		return code, "", errors.Join(err, fmt.Errorf("read command output: %w", readErr))
	}
	return code, string(logged), err
}
