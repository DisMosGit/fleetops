//go:build integration

package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// deviceState is the stored shape of a devices document this suite asserts on.
type deviceState struct {
	ID        string    `bson:"_id"`
	Model     string    `bson:"model"`
	Region    string    `bson:"region"`
	CurrentFw string    `bson:"current_fw"`
	Status    string    `bson:"status"`
	LastSeen  time.Time `bson:"last_heartbeat"`
}

// runWriter starts a writer's flush loop and returns the context its handle path uses and a stop
// function that cancels the writer and waits for Run to return. Stopping is idempotent and also
// runs at cleanup, so a failing test still stops its writer — and a writer whose Run fails on
// shutdown (an interrupted, unaccepted write) fails the test instead of disappearing.
func runWriter(t *testing.T, w *Writer) (context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Writer.Run() error = %v, want nil on shutdown", err)
				}
			case <-time.After(testexec.Get(t).Wait(30 * time.Second)):
				t.Error("Writer.Run() did not return after shutdown")
			}
		})
	}
	t.Cleanup(stop)
	return ctx, stop
}

// readDevice returns one devices document.
func readDevice(t *testing.T, devicesColl *mongo.Collection, deviceID string) deviceState {
	t.Helper()

	var got deviceState
	if err := devicesColl.FindOne(context.Background(),
		bson.D{{Key: "_id", Value: deviceID}}).Decode(&got); err != nil {
		t.Fatalf("find device %s: %v", deviceID, err)
	}
	return got
}

// awaitDeviceEvents waits until one device has want telemetry documents, so a test asserts on the
// settled state instead of sleeping.
func awaitDeviceEvents(t *testing.T, events *mongo.Collection, deviceID string, want int64) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(testexec.Get(t).Wait(10 * time.Second))
	var got int64
	for {
		count, err := events.CountDocuments(ctx, bson.D{{Key: "meta.device_id", Value: deviceID}})
		if err != nil {
			t.Fatalf("count telemetry for %s: %v", deviceID, err)
		}
		got = count
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("telemetry documents for %s = %d, want %d", deviceID, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWriterMarksReturningDeviceOnline backs the scenario "Returning device goes online"
// (device-liveness): a device the sweep marked offline is marked online again by an accepted
// heartbeat, so its recorded status follows its actual activity.
func TestWriterMarksReturningDeviceOnline(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := devices.NewStore(harness.DB)
	events := harness.DB.Collection("telemetry")
	deviceDocs := harness.DB.Collection("devices")
	ctx := context.Background()

	rec := devices.Record{ID: "dev-returning", Model: "v3", Region: "eu-west"}
	if err := store.Upsert(ctx, devices.Record{
		ID: rec.ID, Model: rec.Model, Region: rec.Region, CurrentFw: "1.0.0",
		Status: devices.StatusOnline, LastSeen: time.Now().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	// The sweep is what marks a silent device offline; the heartbeat below is the device coming
	// back, which is the trigger this scenario names.
	if _, err := store.MarkStale(ctx, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("MarkStale() error = %v", err)
	}
	if got := readDevice(t, deviceDocs, rec.ID); got.Status != devices.StatusOffline {
		t.Fatalf("device status before the heartbeat = %q, want %q", got.Status, devices.StatusOffline)
	}

	w := NewWriter(events, store, 1, 20*time.Millisecond, slog.Default())
	runCtx, _ := runWriter(t, w)
	if err := w.Handle(runCtx, heartbeat("ev-returning", rec.ID, time.Now()), rec); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	// The heartbeat was accepted for storage — not merely refused — before the status is read.
	awaitDeviceEvents(t, events, rec.ID, 1)

	if got := readDevice(t, deviceDocs, rec.ID); got.Status != devices.StatusOnline {
		t.Errorf("device status after the accepted heartbeat = %q, want %q", got.Status, devices.StatusOnline)
	}
}

// TestWriterHeartbeatRecordsTheReportedFirmware backs the scenario "Firmware change is picked up
// from a heartbeat" (device-registry): the device document records the firmware version the
// heartbeat reports, not the version the registration recorded.
func TestWriterHeartbeatRecordsTheReportedFirmware(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := devices.NewStore(harness.DB)
	events := harness.DB.Collection("telemetry")
	deviceDocs := harness.DB.Collection("devices")
	ctx := context.Background()

	rec := devices.Record{ID: "dev-firmware", Model: "v3", Region: "eu-west"}
	if err := store.Upsert(ctx, devices.Record{
		ID: rec.ID, Model: rec.Model, Region: rec.Region, CurrentFw: "1.0.0",
		Status: devices.StatusOnline, LastSeen: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	// heartbeat() reports firmware 2.0.0 against the 1.0.0 the record was registered with.
	hb := heartbeat("ev-firmware", rec.ID, time.Now())
	w := NewWriter(events, store, 1, 20*time.Millisecond, slog.Default())
	runCtx, _ := runWriter(t, w)
	if err := w.Handle(runCtx, hb, rec); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	awaitDeviceEvents(t, events, rec.ID, 1)

	got := readDevice(t, deviceDocs, rec.ID)
	if got.CurrentFw != "2.0.0" {
		t.Errorf("recorded firmware = %q, want the reported 2.0.0", got.CurrentFw)
	}
	// The refresh updates the registration's document in place rather than creating a second one.
	count, err := deviceDocs.CountDocuments(ctx, bson.D{{Key: "_id", Value: rec.ID}})
	if err != nil {
		t.Fatalf("count device documents: %v", err)
	}
	if count != 1 {
		t.Errorf("documents for %s = %d, want the single registered record", rec.ID, count)
	}
}

// TestWriterHeartbeatsAreQueryableByDeviceAndTimeRange backs the scenario "Heartbeats are
// queryable by device and time range" (telemetry-ingest): the query a consumer makes for one
// device over a range returns exactly that device's in-range events in measurement-time order.
func TestWriterHeartbeatsAreQueryableByDeviceAndTimeRange(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := devices.NewStore(harness.DB)
	events := harness.DB.Collection("telemetry")
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	deviceA := devices.Record{ID: "dev-range-a", Model: "v3", Region: "eu-west"}
	deviceB := devices.Record{ID: "dev-range-b", Model: "v4", Region: "us-east"}

	w := NewWriter(events, store, 4, 20*time.Millisecond, slog.Default())
	runCtx, _ := runWriter(t, w)
	// Three events inside the queried range — two for one device and one for another — plus one
	// of the queried device's own events outside it.
	for _, hb := range []struct {
		rec     devices.Record
		eventID string
		ts      time.Time
	}{
		{rec: deviceA, eventID: "ev-a-1", ts: base.Add(time.Minute)},
		{rec: deviceB, eventID: "ev-b-1", ts: base.Add(90 * time.Second)},
		{rec: deviceA, eventID: "ev-a-2", ts: base.Add(2 * time.Minute)},
		{rec: deviceA, eventID: "ev-a-late", ts: base.Add(10 * time.Minute)},
	} {
		if err := w.Handle(runCtx, heartbeat(hb.eventID, hb.rec.ID, hb.ts), hb.rec); err != nil {
			t.Fatalf("Handle(%s) error = %v", hb.eventID, err)
		}
	}
	awaitDeviceEvents(t, events, deviceA.ID, 3)
	awaitDeviceEvents(t, events, deviceB.ID, 1)

	// The query a consumer makes: one device, one time range, measurement-time order.
	from, to := base, base.Add(5*time.Minute)
	cursor, err := events.Find(ctx, bson.D{
		{Key: "meta.device_id", Value: deviceA.ID},
		{Key: "ts", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lt", Value: to}}},
	}, options.Find().SetSort(bson.D{{Key: "ts", Value: 1}}))
	if err != nil {
		t.Fatalf("query telemetry: %v", err)
	}
	var found []struct {
		ID string    `bson:"_id"`
		TS time.Time `bson:"ts"`
	}
	if err := cursor.All(ctx, &found); err != nil {
		t.Fatalf("read telemetry query: %v", err)
	}

	ids := make([]string, 0, len(found))
	for _, doc := range found {
		ids = append(ids, doc.ID)
	}
	want := []string{"ev-a-1", "ev-a-2"}
	if diff := cmp.Diff(want, ids); diff != "" {
		t.Errorf("query result mismatch (-want +got):\n%s", diff)
	}
}

// TestWriterRedeliveryAfterRestartStoresOneDocumentPerEvent backs the scenario "Redelivery after a
// control-plane restart" (telemetry-ingest): after the control plane stopped mid-stream, a second
// writer over the same collection — the restarted control plane — takes the agent's resent event
// ids and the collection still holds exactly one document per event id.
func TestWriterRedeliveryAfterRestartStoresOneDocumentPerEvent(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := devices.NewStore(harness.DB)
	events := harness.DB.Collection("telemetry")
	ctx := context.Background()

	rec := devices.Record{ID: "dev-restart", Model: "v3", Region: "eu-west"}
	if err := store.Upsert(ctx, devices.Record{
		ID: rec.ID, Model: rec.Model, Region: rec.Region, CurrentFw: "1.0.0",
		Status: devices.StatusOnline, LastSeen: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	sent := []*agentv1.Heartbeat{
		heartbeat("ev-restart-0", rec.ID, base),
		heartbeat("ev-restart-1", rec.ID, base.Add(time.Second)),
		heartbeat("ev-restart-2", rec.ID, base.Add(2*time.Second)),
	}

	// The control plane's first lifetime: the heartbeats reach the collection and the writer is
	// shut down mid-stream, the way a restart stops it.
	first := NewWriter(events, store, 2, 20*time.Millisecond, slog.Default())
	firstCtx, stopFirst := runWriter(t, first)
	for _, hb := range sent {
		if err := first.Handle(firstCtx, hb, rec); err != nil {
			t.Fatalf("Handle(%s) error = %v", hb.GetEventId(), err)
		}
	}
	awaitDeviceEvents(t, events, rec.ID, int64(len(sent)))
	stopFirst()

	// The control plane's second lifetime: the agent resends the heartbeats it never saw accepted
	// with their original event ids, beside one event that had not got through at all.
	second := NewWriter(events, store, 2, 20*time.Millisecond, slog.Default())
	secondCtx, _ := runWriter(t, second)
	for _, hb := range sent {
		if err := second.Handle(secondCtx, hb, rec); err != nil {
			t.Fatalf("Handle(%s redelivery) error = %v", hb.GetEventId(), err)
		}
	}
	fresh := heartbeat("ev-restart-3", rec.ID, base.Add(3*time.Second))
	if err := second.Handle(secondCtx, fresh, rec); err != nil {
		t.Fatalf("Handle(%s) error = %v", fresh.GetEventId(), err)
	}
	awaitDeviceEvents(t, events, rec.ID, int64(len(sent)+1))

	for _, hb := range append(append([]*agentv1.Heartbeat(nil), sent...), fresh) {
		count, err := events.CountDocuments(ctx, bson.D{{Key: "_id", Value: hb.GetEventId()}})
		if err != nil {
			t.Fatalf("count documents for %s: %v", hb.GetEventId(), err)
		}
		if count != 1 {
			t.Errorf("documents for %s = %d, want exactly 1 after the restart", hb.GetEventId(), count)
		}
	}
}
