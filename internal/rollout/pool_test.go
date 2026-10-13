package rollout

import (
	"log/slog"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The two selectors every fold test draws from. The view's own tests build entries without a
// collection: what the fold applies is memory, and it must not depend on a server.
var (
	oakPool = Selector{Region: "eu-west", Model: "oak-s3"}
	elkPool = Selector{Region: "eu-west", Model: "elk-s2"}
)

// testView returns a view holding the given entries, as a resolution would have built them.
func testView(entries map[Selector][]string) *PoolView {
	v := NewPoolView(nil, slog.New(slog.DiscardHandler))
	for sel, ids := range entries {
		v.pools[sel] = slices.Clone(ids)
		for _, id := range ids {
			v.where[id] = sel
		}
	}
	return v
}

// entriesOf returns what the view holds, taken under its lock so the comparison is a snapshot.
func entriesOf(v *PoolView) map[Selector][]string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[Selector][]string, len(v.pools))
	for sel, ids := range v.pools {
		out[sel] = slices.Clone(ids)
	}
	return out
}

// refreshesOf returns how many entry write-backs the view has performed.
func refreshesOf(v *PoolView) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.refreshes
}

// putEvent returns an insert or replace event for a device in a selector: the event carries the
// document, which is what lets the view place the device without reading the collection.
func putEvent(t *testing.T, operation, id string, sel Selector) changeEvent {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "model", Value: sel.Model},
		{Key: "region", Value: sel.Region},
	})
	if err != nil {
		t.Fatalf("marshal the event's document: %v", err)
	}
	return changeEvent{
		OperationType: operation,
		FullDocument:  doc,
		DocumentKey:   documentKey{ID: id},
	}
}

// deleteEvent returns a delete event for a device: the event names the document it removed and
// carries no document of its own.
func deleteEvent(id string) changeEvent {
	return changeEvent{OperationType: operationDelete, DocumentKey: documentKey{ID: id}}
}

// opaqueEvent returns an event whose lookup returned no document: the device it names cannot be
// placed, which is what a delete racing its own update produces.
func opaqueEvent(id string) changeEvent {
	return changeEvent{OperationType: operationUpdate, DocumentKey: documentKey{ID: id}}
}

// TestPoolViewFold covers what a drained batch of change events does to the built entries.
func TestPoolViewFold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries map[Selector][]string
		events  []changeEvent
		want    map[Selector][]string
	}{
		{
			name:    "an inserted device enters its selector's entry in identity order",
			entries: map[Selector][]string{oakPool: {"dev-b"}},
			events: []changeEvent{
				putEvent(t, operationInsert, "dev-a", oakPool),
				putEvent(t, operationInsert, "dev-c", oakPool),
			},
			want: map[Selector][]string{oakPool: {"dev-a", "dev-b", "dev-c"}},
		},
		{
			name: "a model change moves a device to the entry it now matches",
			entries: map[Selector][]string{
				oakPool: {"dev-a", "dev-b"},
				elkPool: {"dev-c"},
			},
			events: []changeEvent{putEvent(t, operationUpdate, "dev-a", elkPool)},
			want: map[Selector][]string{
				oakPool: {"dev-b"},
				elkPool: {"dev-a", "dev-c"},
			},
		},
		{
			name: "a region change moves a device to the entry it now matches",
			entries: map[Selector][]string{
				oakPool:                              {"dev-a", "dev-b"},
				{Region: "us-east", Model: "oak-s3"}: {"dev-c"},
			},
			events: []changeEvent{putEvent(t, operationReplace, "dev-a", Selector{Region: "us-east", Model: "oak-s3"})},
			want: map[Selector][]string{
				oakPool:                              {"dev-b"},
				{Region: "us-east", Model: "oak-s3"}: {"dev-a", "dev-c"},
			},
		},
		{
			name:    "a removed device leaves its selector's entry",
			entries: map[Selector][]string{oakPool: {"dev-a", "dev-b"}},
			events:  []changeEvent{deleteEvent("dev-a")},
			want:    map[Selector][]string{oakPool: {"dev-b"}},
		},
		{
			name:    "a redelivered insert does not duplicate an entry",
			entries: map[Selector][]string{oakPool: {"dev-a"}},
			events: []changeEvent{
				putEvent(t, operationInsert, "dev-b", oakPool),
				putEvent(t, operationInsert, "dev-b", oakPool),
			},
			want: map[Selector][]string{oakPool: {"dev-a", "dev-b"}},
		},
		{
			name:    "a redelivered delete of an absent device is a no-op",
			entries: map[Selector][]string{oakPool: {"dev-a"}},
			events:  []changeEvent{deleteEvent("dev-gone"), deleteEvent("dev-gone")},
			want:    map[Selector][]string{oakPool: {"dev-a"}},
		},
		{
			name:    "an event about an unbuilt selector builds nothing",
			entries: map[Selector][]string{oakPool: {"dev-a"}},
			events:  []changeEvent{putEvent(t, operationInsert, "dev-z", elkPool)},
			want:    map[Selector][]string{oakPool: {"dev-a"}},
		},
		{
			name:    "an event whose lookup returned no document drops the entry it was in",
			entries: map[Selector][]string{oakPool: {"dev-a", "dev-b"}},
			events:  []changeEvent{opaqueEvent("dev-a")},
			want:    map[Selector][]string{},
		},
		{
			name: "a burst for one selector is applied whole, in order",
			entries: map[Selector][]string{
				oakPool: {"dev-a"},
				elkPool: {"dev-x"},
			},
			events: []changeEvent{
				putEvent(t, operationInsert, "dev-b", oakPool),
				putEvent(t, operationUpdate, "dev-a", elkPool),
				deleteEvent("dev-x"),
				putEvent(t, operationInsert, "dev-c", oakPool),
			},
			want: map[Selector][]string{
				oakPool: {"dev-b", "dev-c"},
				elkPool: {"dev-a"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			view := testView(tc.entries)
			view.fold(tc.events)
			if diff := cmp.Diff(tc.want, entriesOf(view)); diff != "" {
				t.Errorf("view entries mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestPoolViewFoldIsIdempotent asserts the property the stream's redeliveries rest on: applying a
// batch the view has already applied changes nothing, so a resumed stream that repeats an event
// neither duplicates an entry nor reverts a later change.
func TestPoolViewFoldIsIdempotent(t *testing.T) {
	t.Parallel()

	view := testView(map[Selector][]string{oakPool: {"dev-a"}})
	batch := []changeEvent{
		putEvent(t, operationInsert, "dev-b", oakPool),
		putEvent(t, operationUpdate, "dev-a", elkPool),
		deleteEvent("dev-b"),
	}

	view.fold(batch)
	once := entriesOf(view)
	view.fold(batch)
	if diff := cmp.Diff(once, entriesOf(view)); diff != "" {
		t.Errorf("a redelivered batch changed the view (-first +second):\n%s", diff)
	}
}

// TestPoolViewFoldCoalescesABurst asserts the refresh accounting: one batch writes each affected
// selector's entry once, however many events the batch carries for it, which is what makes a
// burst of device changes one refresh rather than one per event.
func TestPoolViewFoldCoalescesABurst(t *testing.T) {
	t.Parallel()

	view := testView(map[Selector][]string{
		oakPool: {"dev-a"},
		elkPool: {"dev-x"},
	})
	view.fold([]changeEvent{
		putEvent(t, operationInsert, "dev-b", oakPool),
		putEvent(t, operationInsert, "dev-c", oakPool),
		putEvent(t, operationUpdate, "dev-a", elkPool),
		putEvent(t, operationInsert, "dev-y", elkPool),
		putEvent(t, operationInsert, "dev-d", oakPool),
	})

	if got := refreshesOf(view); got != 2 {
		t.Errorf("a burst affecting 2 selectors cost %d refreshes, want 2", got)
	}
	want := map[Selector][]string{
		oakPool: {"dev-b", "dev-c", "dev-d"},
		elkPool: {"dev-a", "dev-x", "dev-y"},
	}
	if diff := cmp.Diff(want, entriesOf(view)); diff != "" {
		t.Errorf("view entries mismatch (-want +got):\n%s", diff)
	}
}

// TestPoolViewKeepsTokenAndFlushes asserts the two pieces of watcher state a test can drive
// without a server: a position is kept once recorded and forgotten on demand, and a flush drops
// every built entry — including the reverse index — so each selector rebuilds from devices.
func TestPoolViewKeepsTokenAndFlushes(t *testing.T) {
	t.Parallel()

	view := testView(map[Selector][]string{oakPool: {"dev-a"}})
	position := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}
	view.setToken(position)
	if got := view.resumeToken(); !slices.Equal(got, position) {
		t.Fatalf("resumeToken() = %v, want the recorded position", got)
	}

	view.flush()
	if diff := cmp.Diff(map[Selector][]string{}, entriesOf(view)); diff != "" {
		t.Errorf("flushed view entries mismatch (-want +got):\n%s", diff)
	}
	if len(view.where) != 0 {
		t.Errorf("flushed view still tracks %d devices", len(view.where))
	}
	if got := view.resumeToken(); !slices.Equal(got, position) {
		t.Errorf("flush forgot the position: %v, want %v", got, position)
	}

	view.dropToken()
	if got := view.resumeToken(); got != nil {
		t.Errorf("dropToken left position %v, want none", got)
	}
}

// TestChangeEventSelector covers what an event can say about the document it names.
func TestChangeEventSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		evt  changeEvent
		want Selector
		ok   bool
	}{
		{
			name: "an insert names the document's selector",
			evt:  putEvent(t, operationInsert, "dev-a", elkPool),
			want: elkPool,
			ok:   true,
		},
		{
			name: "a delete carries no document to place",
			evt:  deleteEvent("dev-a"),
			ok:   false,
		},
		{
			name: "an unresolved lookup carries no document to place",
			evt:  opaqueEvent("dev-a"),
			ok:   false,
		},
		{
			name: "a malformed document cannot be placed",
			evt: changeEvent{
				OperationType: operationUpdate,
				FullDocument:  bson.Raw{0xff, 0x00},
				DocumentKey:   documentKey{ID: "dev-a"},
			},
			ok: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := tc.evt.selector()
			if ok != tc.ok {
				t.Fatalf("selector() ok = %t, want %t", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("selector() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestOrderedIDs asserts the ordering a pool's reproducibility rests on: membership is returned
// by device identity whatever order the events arrived in.
func TestOrderedIDs(t *testing.T) {
	t.Parallel()

	got := orderedIDs(map[string]struct{}{"dev-c": {}, "dev-a": {}, "dev-b": {}})
	want := []string{"dev-a", "dev-b", "dev-c"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("orderedIDs() mismatch (-want +got):\n%s", diff)
	}
	if len(orderedIDs(map[string]struct{}{})) != 0 {
		t.Error("orderedIDs() of an empty membership is not empty")
	}
}

// TestBackoff pins the reconnect policy's shape: exponential growth from the initial delay to the
// cap, a reset after a stream the watcher could serve from, and no delay above the cap.
func TestBackoff(t *testing.T) {
	t.Parallel()

	t.Run("delays double up to the cap", func(t *testing.T) {
		t.Parallel()

		b := newBackoff(time.Second, 5*time.Second, 0, rand.New(rand.NewSource(1)))
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
		for i, expected := range want {
			if got := b.Delay(); got != expected {
				t.Fatalf("delay %d = %v, want %v", i, got, expected)
			}
		}
	})

	t.Run("a reset restarts the sequence", func(t *testing.T) {
		t.Parallel()

		b := newBackoff(time.Second, 5*time.Second, 0, rand.New(rand.NewSource(1)))
		_ = b.Delay()
		_ = b.Delay()
		b.Reset()
		if got := b.Delay(); got != time.Second {
			t.Errorf("delay after Reset() = %v, want %v", got, time.Second)
		}
	})

	t.Run("jitter never exceeds the cap", func(t *testing.T) {
		t.Parallel()

		b := newBackoff(4*time.Second, 5*time.Second, 0.5, rand.New(rand.NewSource(7)))
		for i := 0; i < 10; i++ {
			if got := b.Delay(); got > 5*time.Second || got <= 0 {
				t.Fatalf("delay %d = %v, want it inside (0, %v]", i, got, 5*time.Second)
			}
		}
	})
}
