//go:build integration

package rollout

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The two selectors the view tests draw from. Each test seeds its own devices in its own
// database, so the selectors can repeat across tests without one seeing another's pool.
var (
	watchedPool = Selector{Region: "eu-west", Model: "oak-view"}
	otherPool   = Selector{Region: "eu-west", Model: "elk-view"}
)

// newView returns a view over the harness's devices collection, discarding the watcher's log: what
// these tests assert is the view, not what was written about it.
func newView(harness *mongotest.Harness) *PoolView {
	return NewPoolView(harness.DB.Collection("devices"), slog.New(slog.DiscardHandler))
}

// seedDevice writes one complete device document, which is what the devices validator accepts.
func seedDevice(t *testing.T, harness *mongotest.Harness, id string, sel Selector) {
	t.Helper()
	_, err := harness.DB.Collection("devices").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: id},
		{Key: "model", Value: sel.Model},
		{Key: "region", Value: sel.Region},
		{Key: "current_fw", Value: "1.0.0"},
		{Key: "status", Value: "online"},
		{Key: "last_heartbeat", Value: startedAt},
	})
	if err != nil {
		t.Fatalf("seed device %s: %v", id, err)
	}
}

// watchView starts the view's watcher and returns what stops it: it cancels the watcher's context
// and reports what Watch returned. The watcher is stopped for the test during cleanup, so no
// stream outlives the test that opened it.
func watchView(t *testing.T, view *PoolView) func() error {
	t.Helper()
	budget := testexec.Get(t).Wait(watchBudget)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- view.Watch(ctx, ready) }()

	var (
		once sync.Once
		err  error
	)
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(budget):
				err = errors.New("the watcher did not stop with its context")
			}
		})
		return err
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("the watcher ended with %v, want nil", err)
		}
	})

	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the watcher could not establish its stream: %v", err)
	case <-time.After(budget):
		t.Fatal("the watcher did not establish its stream")
	}
	return stop
}

// waitFor polls cond until it holds or the stream's budget expires, which is how a test waits for
// a change the stream delivers rather than for a fixed sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitForBudget(t, watchBudget, what, cond)
}

// waitForBudget is waitFor on a budget of its own, for a wait whose cost is not the stream's.
// The budget is scaled by the run's configuration, as every wait budget is.
func waitForBudget(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	budget = testexec.Get(t).Wait(budget)
	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %v for %s", budget, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// resolved returns the selector's pool as the view answers it, failing the test on an error.
func resolved(t *testing.T, view *PoolView, sel Selector) []string {
	t.Helper()
	ids, err := view.Resolve(context.Background(), sel)
	if err != nil {
		t.Fatalf("Resolve(%+v): %v", sel, err)
	}
	return ids
}

// fullRead returns the selector's pool by querying devices, which is what a view rebuilt from the
// collection must agree with.
func fullRead(t *testing.T, harness *mongotest.Harness, sel Selector) []string {
	t.Helper()
	cursor, err := harness.DB.Collection("devices").Find(context.Background(),
		bson.D{{Key: "region", Value: sel.Region}, {Key: "model", Value: sel.Model}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		t.Fatalf("read devices in %+v: %v", sel, err)
	}
	var docs []struct {
		ID string `bson:"_id"`
	}
	if err := cursor.All(context.Background(), &docs); err != nil {
		t.Fatalf("decode devices in %+v: %v", sel, err)
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids
}

// entryCount returns how many selectors the view holds an entry for.
func entryCount(view *PoolView) int {
	view.mu.Lock()
	defer view.mu.Unlock()
	return len(view.pools)
}

// setToken forces the position the view holds, which is how a test drives the unusable-position
// path without waiting for a server to lose an oplog entry.
func setToken(view *PoolView, token bson.Raw) {
	view.mu.Lock()
	defer view.mu.Unlock()
	view.token = token
}

// TestPoolViewTracksDeviceChanges asserts what the fleet's own writes do to the view: a device
// that appears enters its selector's entry, one whose model or region changes moves between
// entries, one that leaves the fleet leaves the view, and a change applied twice changes nothing
// beyond the first application. Every one of them is applied from the stream: the find counter
// shows the view paid no query to learn about a change.
func TestPoolViewTracksDeviceChanges(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	view := newView(harness)
	watchView(t, view)

	t.Run("a new device enters its selector's view", func(t *testing.T) {
		sel := watchedPool
		seedDevice(t, harness, "kept-1", sel)
		if got := resolved(t, view, sel); len(got) != 1 {
			t.Fatalf("pool = %v, want the seeded device", got)
		}
		finds := harness.FindCommands("devices")

		seedDevice(t, harness, "kept-2", sel)
		waitFor(t, "the new device to enter the view", func() bool {
			return len(resolved(t, view, sel)) == 2
		})
		if got := harness.FindCommands("devices"); got != finds {
			t.Errorf("the view paid %d finds to learn about one device, want %d", got-finds, 0)
		}
	})

	t.Run("a device that changes model moves between views", func(t *testing.T) {
		from, to := Selector{Region: "eu-west", Model: "oak-move"}, Selector{Region: "eu-west", Model: "elk-move"}
		seedDevice(t, harness, "moved-1", from)
		if got := resolved(t, view, from); len(got) != 1 {
			t.Fatalf("pool = %v, want the seeded device", got)
		}
		if got := resolved(t, view, to); len(got) != 0 {
			t.Fatalf("the target pool = %v, want it empty", got)
		}
		finds := harness.FindCommands("devices")

		if _, err := harness.DB.Collection("devices").UpdateOne(context.Background(),
			bson.D{{Key: "_id", Value: "moved-1"}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "model", Value: to.Model}}}}); err != nil {
			t.Fatalf("change the device's model: %v", err)
		}
		waitFor(t, "the device to move to the new model's view", func() bool {
			return len(resolved(t, view, from)) == 0 && len(resolved(t, view, to)) == 1
		})
		if got := harness.FindCommands("devices"); got != finds {
			t.Errorf("the move paid %d finds, want %d", got-finds, 0)
		}
	})

	t.Run("a device that changes region moves between views", func(t *testing.T) {
		from := Selector{Region: "eu-west", Model: "oak-region"}
		to := Selector{Region: "us-east", Model: "oak-region"}
		seedDevice(t, harness, "region-1", from)
		if got := resolved(t, view, from); len(got) != 1 {
			t.Fatalf("pool = %v, want the seeded device", got)
		}
		if got := resolved(t, view, to); len(got) != 0 {
			t.Fatalf("the target pool = %v, want it empty", got)
		}

		if _, err := harness.DB.Collection("devices").UpdateOne(context.Background(),
			bson.D{{Key: "_id", Value: "region-1"}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "region", Value: to.Region}}}}); err != nil {
			t.Fatalf("change the device's region: %v", err)
		}
		waitFor(t, "the device to move to the new region's view", func() bool {
			return len(resolved(t, view, from)) == 0 && len(resolved(t, view, to)) == 1
		})
	})

	t.Run("a device that leaves the fleet leaves the view", func(t *testing.T) {
		sel := Selector{Region: "eu-west", Model: "oak-removed"}
		seedDevice(t, harness, "gone-1", sel)
		if got := resolved(t, view, sel); len(got) != 1 {
			t.Fatalf("pool = %v, want the seeded device", got)
		}
		finds := harness.FindCommands("devices")

		if _, err := harness.DB.Collection("devices").
			DeleteOne(context.Background(), bson.D{{Key: "_id", Value: "gone-1"}}); err != nil {
			t.Fatalf("remove the device: %v", err)
		}
		waitFor(t, "the removed device to leave the view", func() bool {
			return len(resolved(t, view, sel)) == 0
		})
		if got := harness.FindCommands("devices"); got != finds {
			t.Errorf("the removal paid %d finds, want %d", got-finds, 0)
		}
	})

	t.Run("a redelivered change does not duplicate an entry", func(t *testing.T) {
		sel := Selector{Region: "eu-west", Model: "oak-again"}
		seedDevice(t, harness, "again-1", sel)
		if got := resolved(t, view, sel); len(got) != 1 {
			t.Fatalf("pool = %v, want the seeded device", got)
		}

		// The same change written twice: a stream that redelivers either event applies it
		// to an entry that already holds it, which must leave the entry as it stands.
		for range 2 {
			if _, err := harness.DB.Collection("devices").UpdateOne(context.Background(),
				bson.D{{Key: "_id", Value: "again-1"}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "online"}}}}); err != nil {
				t.Fatalf("rewrite the device: %v", err)
			}
		}
		waitFor(t, "the redelivered change to be applied", func() bool {
			return len(resolved(t, view, sel)) == 1
		})
		if got := resolved(t, view, sel); got[0] != "again-1" {
			t.Errorf("pool = %v, want exactly the one device", got)
		}
	})
}

// TestPoolViewResumesFromItsToken asserts the property an interruption must not cost: the view
// keeps the position its stream reached, so a stream established after the interruption resumes
// from it and the changes made while it was down are applied without a query — and the view still
// agrees with what a full read of the collection returns.
func TestPoolViewResumesFromItsToken(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	view := newView(harness)
	sel := Selector{Region: "eu-west", Model: "oak-resume"}
	stop := watchView(t, view)

	// The entry is built first, so what follows is the stream applying changes to it rather
	// than a resolution reading the collection.
	seedDevice(t, harness, "resume-1", sel)
	if got := resolved(t, view, sel); len(got) != 1 {
		t.Fatalf("pool = %v, want the seeded device", got)
	}
	seedDevice(t, harness, "resume-2", sel)
	waitFor(t, "the stream to apply the first change", func() bool {
		return len(resolved(t, view, sel)) == 2
	})

	// The stream is interrupted and a device changes while no stream is running.
	if err := stop(); err != nil {
		t.Fatalf("the watcher ended with %v, want nil on a clean stop", err)
	}
	seedDevice(t, harness, "resume-3", sel)

	finds := harness.FindCommands("devices")
	stopAgain := watchView(t, view)
	waitFor(t, "the resumed stream to apply the change made while it was down", func() bool {
		return len(resolved(t, view, sel)) == 3
	})
	if got := harness.FindCommands("devices"); got != finds {
		t.Errorf("resuming the stream paid %d finds, want %d", got-finds, 0)
	}
	if err := stopAgain(); err != nil {
		t.Fatalf("the watcher ended with %v, want nil on a clean stop", err)
	}

	// Nothing was lost in either direction: the view holds exactly what the collection does.
	if diff := cmp.Diff(fullRead(t, harness, sel), resolved(t, view, sel)); diff != "" {
		t.Errorf("the resumed view differs from a full read (-read +view):\n%s", diff)
	}
}

// TestPoolViewFlushesAnUnusablePosition asserts what the view does with a position the server will
// not serve: it drops every built entry, keeps serving by rebuilding each one from the collection,
// and continues from a fresh stream, so staleness cannot outlive the position it was built on.
func TestPoolViewFlushesAnUnusablePosition(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	view := newView(harness)
	sel := Selector{Region: "eu-west", Model: "oak-flush"}

	seedDevice(t, harness, "flush-1", sel)
	seedDevice(t, harness, "flush-2", sel)
	if got := resolved(t, view, sel); len(got) != 2 {
		t.Fatalf("pool = %v, want both seeded devices", got)
	}

	// A token the server does not hold: the stream cannot resume from it, which is the state a
	// position the oplog has rolled past leaves behind.
	bogus, err := bson.Marshal(bson.D{{Key: "_data", Value: "not-a-resume-token"}})
	if err != nil {
		t.Fatalf("marshal the unusable position: %v", err)
	}
	setToken(view, bogus)

	watchView(t, view)

	// The unusable position dropped what the view held before it established its fresh stream,
	// so the next resolution rebuilds the entry — from the collection, exactly as a full read
	// answers.
	if got := entryCount(view); got != 0 {
		t.Errorf("the view holds %d entries after an unusable position, want none", got)
	}
	want := fullRead(t, harness, sel)
	finds := harness.FindCommands("devices")
	if diff := cmp.Diff(want, resolved(t, view, sel)); diff != "" {
		t.Errorf("the rebuilt view differs from a full read (-read +view):\n%s", diff)
	}
	if got := harness.FindCommands("devices"); got != finds+1 {
		t.Errorf("rebuilding the entry paid %d finds, want 1", got-finds)
	}

	// The fresh stream is live: the next change reaches the rebuilt entry without a query.
	finds = harness.FindCommands("devices")
	seedDevice(t, harness, "flush-3", sel)
	waitFor(t, "the fresh stream to deliver a change", func() bool {
		return len(resolved(t, view, sel)) == 3
	})
	if got := harness.FindCommands("devices"); got != finds {
		t.Errorf("the fresh stream paid %d finds, want %d", got-finds, 0)
	}

	if token := view.resumeToken(); len(token) == 0 {
		t.Error("the view holds no position after establishing a fresh stream")
	}
}

// TestPoolViewDerivesFromDevices asserts the view's starting point: a selector's entry is exactly
// what a full read of devices returns for it, across several regions and models, and a selector no
// device matches resolves to an empty pool rather than to an error.
func TestPoolViewDerivesFromDevices(t *testing.T) {
	t.Parallel()

	harness := mongotest.ReplicaSet(t)
	view := newView(harness)
	selectors := []Selector{
		{Region: "eu-west", Model: "oak-derive"},
		{Region: "eu-west", Model: "elk-derive"},
		{Region: "us-east", Model: "oak-derive"},
	}
	seeded := []struct {
		id  string
		sel Selector
	}{
		{id: "derive-a", sel: selectors[0]},
		{id: "derive-b", sel: selectors[0]},
		{id: "derive-c", sel: selectors[1]},
		{id: "derive-d", sel: selectors[2]},
		{id: "derive-e", sel: selectors[0]},
	}
	for _, device := range seeded {
		seedDevice(t, harness, device.id, device.sel)
	}

	for _, sel := range selectors {
		want := fullRead(t, harness, sel)
		if len(want) == 0 {
			t.Fatalf("selector %+v holds no devices to derive from", sel)
		}
		if diff := cmp.Diff(want, resolved(t, view, sel)); diff != "" {
			t.Errorf("the view over %+v differs from a full read (-read +view):\n%s", sel, diff)
		}
	}

	// A selector nothing matches is an empty pool, not an error and not a missing answer.
	empty := Selector{Region: "ap-south", Model: "oak-derive"}
	pool := resolved(t, view, empty)
	if pool == nil {
		t.Error("an empty selector resolved to a nil pool, want an empty one")
	}
	if len(pool) != 0 {
		t.Errorf("an empty selector resolved to %v, want no devices", pool)
	}
}

// TestPoolViewStopsWithItsContext asserts what the worker's shutdown rests on: the watcher returns
// when its context ends, the cursor its stream held is released on the server, and it leaves no
// goroutine watching the collection. It runs without t.Parallel so the process's goroutines and
// the server's open cursors are quiet while they are measured.
func TestPoolViewStopsWithItsContext(t *testing.T) {
	harness := mongotest.ReplicaSet(t)
	view := newView(harness)

	// The first stream may grow the client's connection pool once; what must not grow is the
	// goroutine count per stream, which is what a watcher left behind would raise.
	afterFirst := runAndStopWatcher(t, view, harness)
	afterSecond := runAndStopWatcher(t, view, harness)
	if afterSecond > afterFirst {
		t.Errorf("a second watcher left %d goroutines, want no more than the %d left by the first",
			afterSecond, afterFirst)
	}
}

// runAndStopWatcher establishes a stream, waits for its cursor to be open on the server, stops the
// watcher with its context, and returns the process's goroutine count once it has settled. It is
// the observation the shutdown assertion is made of.
func runAndStopWatcher(t *testing.T, view *PoolView, harness *mongotest.Harness) int {
	t.Helper()
	budget := testexec.Get(t).Wait(watchBudget)
	baseline := openCursors(t, harness)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- view.Watch(ctx, ready) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the watcher could not establish its stream: %v", err)
	case <-time.After(budget):
		t.Fatal("the watcher did not establish its stream")
	}
	// The stream holds a cursor on the server, which is what makes the release below an
	// observation of the stream rather than of a count that never moved.
	waitFor(t, "the stream's cursor to be open on the server", func() bool {
		return openCursors(t, harness) > baseline
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Watch() = %v, want nil on a graceful stop", err)
		}
	case <-time.After(budget):
		t.Fatal("the watcher did not stop with its context")
	}
	// Releasing the cursor is the request's own round trip to a shared server, so it waits on
	// releaseBudget rather than on the stream's: the delay logged here is what the wait cost,
	// which is what a future failure needs to be read against.
	releasing := time.Now()
	waitForBudget(t, releaseBudget, "the stream's cursor to be released", func() bool {
		return openCursors(t, harness) <= baseline
	})
	t.Logf("the stream's cursor was released after %v", time.Since(releasing))

	// The watcher's goroutine ends with it; the count is polled rather than read once because
	// shutdown is asynchronous.
	settled := runtime.NumGoroutine()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if now := runtime.NumGoroutine(); now < settled {
			settled = now
		}
		time.Sleep(20 * time.Millisecond)
	}
	return settled
}

// openCursors returns how many cursors the server reports open, which is how a test observes that
// a change stream's cursor was released rather than leaked.
func openCursors(t *testing.T, harness *mongotest.Harness) int {
	t.Helper()
	var status struct {
		Metrics struct {
			Cursor struct {
				Open struct {
					Total int `bson:"total"`
				} `bson:"open"`
			} `bson:"cursor"`
		} `bson:"metrics"`
	}
	if err := harness.DB.RunCommand(context.Background(),
		bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		t.Fatalf("read the server's open-cursor count: %v", err)
	}
	return status.Metrics.Cursor.Open.Total
}

// watchBudget bounds waiting for a change stream to establish, a change to be delivered, or a
// watcher to stop.
const watchBudget = 20 * time.Second

// releaseBudget bounds waiting for a stream's cursor to be released once its watcher has closed
// it. The release is one killCursors round trip — milliseconds on an idle host — but its
// observation is the server's own cursor accounting, and a loaded host can delay that well past
// the stream's budget: the wait is for the server to finish what the watcher asked for, not for
// the watcher, which has already returned by the time it starts.
const releaseBudget = 60 * time.Second
