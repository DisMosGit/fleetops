package telemetry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// fakeAlertStore records the alert upserts the handler asked for.
type fakeAlertStore struct {
	err error

	mu      sync.Mutex
	filters []any
	updates []any
	upserts []bool
}

// UpdateOne records the upsert and returns the scripted error.
func (s *fakeAlertStore) UpdateOne(_ context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filters = append(s.filters, filter)
	s.updates = append(s.updates, update)
	settings := options.UpdateOneOptions{}
	for _, lister := range opts {
		if lister == nil {
			continue
		}
		for _, apply := range lister.List() {
			if err := apply(&settings); err != nil {
				return nil, err
			}
		}
	}
	s.upserts = append(s.upserts, settings.Upsert != nil && *settings.Upsert)
	return &mongo.UpdateResult{}, s.err
}

// writes returns how many upserts were recorded.
func (s *fakeAlertStore) writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.updates)
}

// lastUpdate returns the last recorded update document.
func (s *fakeAlertStore) lastUpdate(t *testing.T) bson.D {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.updates) == 0 {
		t.Fatal("no alert upsert was recorded")
	}
	update, ok := s.updates[len(s.updates)-1].(bson.D)
	if !ok {
		t.Fatalf("update of type %T, want bson.D", s.updates[len(s.updates)-1])
	}
	return update
}

// TestAlertingThreshold pins when an alert is recorded: below the threshold it is, at or above
// it nothing is written.
func TestAlertingThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		health    float64
		wantWrite bool
	}{
		{name: "well below the threshold", health: 0.2, wantWrite: true},
		{name: "just below the threshold", health: 0.59, wantWrite: true},
		{name: "exactly at the threshold", health: 0.6},
		{name: "above the threshold", health: 0.9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &fakeAlertStore{}
			handler := NewAlerting(store, 0.6)
			event := testEvent()
			event.Payload.Health = tc.health

			if err := handler.Apply(context.Background(), event); err != nil {
				t.Fatalf("Apply() error = %v, want nil", err)
			}
			if got := store.writes(); (got == 1) != tc.wantWrite {
				t.Fatalf("alert upserts = %d, want a write: %t", got, tc.wantWrite)
			}
			if !tc.wantWrite {
				return
			}

			// The upsert is monotone and carries the identity the operator needs; nothing of the
			// raw heartbeat payload is stored.
			update := store.lastUpdate(t)
			operators := operatorsOf(t, update)
			for _, want := range []string{"$min", "$max", "$set"} {
				if _, ok := operators[want]; !ok {
					t.Errorf("update operators = %v, want %s", keysOf(operators), want)
				}
			}
			minFields := fieldNames(t, operators["$min"])
			if !contains(minFields, "first_seen_at") || !contains(minFields, "min_health") {
				t.Errorf("$min fields = %v, want first_seen_at and min_health", minFields)
			}
			if got := fieldNames(t, operators["$max"]); !contains(got, "last_seen_at") {
				t.Errorf("$max fields = %v, want last_seen_at", got)
			}
			setFields := fieldNames(t, operators["$set"])
			for _, want := range []string{"device_id", "region", "model", "threshold"} {
				if !contains(setFields, want) {
					t.Errorf("$set fields = %v, want %s", setFields, want)
				}
			}
			if got := valueOf(t, operators["$set"], "threshold"); got != 0.6 {
				t.Errorf("recorded threshold = %v, want the configured 0.6", got)
			}
			if got := valueOf(t, operators["$min"], "min_health"); got != tc.health {
				t.Errorf("recorded min_health = %v, want the observed %v", got, tc.health)
			}
			if got := valueOf(t, operators["$min"], "first_seen_at"); got != event.OccurredAt {
				t.Errorf("recorded first_seen_at = %v, want the observation time %v", got, event.OccurredAt)
			}
		})
	}
}

// TestAlertingRefusesOtherEventTypes pins that the handler never mistakes another family's event
// for a heartbeat.
func TestAlertingRefusesOtherEventTypes(t *testing.T) {
	t.Parallel()

	store := &fakeAlertStore{}
	handler := NewAlerting(store, 0.6)
	event := testEvent()
	event.EventType = RolloutEventType

	err := handler.Apply(context.Background(), event)
	if err == nil {
		t.Fatal("Apply() = nil, want an error for another event type")
	}
	if !strings.Contains(err.Error(), RolloutEventType) {
		t.Errorf("Apply() error = %q, want it to name the event type", err)
	}
	if got := store.writes(); got != 0 {
		t.Errorf("alert upserts = %d, want none", got)
	}
}

// TestAlertingReportsAStoreFailure pins that a failed alert write is an error, so the consumer
// retries the delivery instead of acknowledging it.
func TestAlertingReportsAStoreFailure(t *testing.T) {
	t.Parallel()

	store := &fakeAlertStore{err: errors.New("mongo unavailable")}
	handler := NewAlerting(store, 0.6)
	event := testEvent()
	event.Payload.Health = 0.1

	err := handler.Apply(context.Background(), event)
	if err == nil {
		t.Fatal("Apply() = nil, want the store error")
	}
	if !strings.Contains(err.Error(), event.DeviceID) {
		t.Errorf("Apply() error = %q, want it to name the device", err)
	}
}

// operatorsOf returns the update document's operators.
func operatorsOf(t *testing.T, update bson.D) map[string]any {
	t.Helper()
	operators := make(map[string]any, len(update))
	for _, element := range update {
		operators[element.Key] = element.Value
	}
	return operators
}

// keysOf returns the operator names of an update document.
func keysOf(operators map[string]any) []string {
	keys := make([]string, 0, len(operators))
	for key := range operators {
		keys = append(keys, key)
	}
	return keys
}

// fieldNames returns the field names of one update operator's document.
func fieldNames(t *testing.T, operator any) []string {
	t.Helper()
	document, ok := operator.(bson.D)
	if !ok {
		t.Fatalf("operator of type %T, want bson.D", operator)
	}
	names := make([]string, 0, len(document))
	for _, element := range document {
		names = append(names, element.Key)
	}
	return names
}

// valueOf returns one field's value in an update operator's document.
func valueOf(t *testing.T, operator any, field string) any {
	t.Helper()
	document, ok := operator.(bson.D)
	if !ok {
		t.Fatalf("operator of type %T, want bson.D", operator)
	}
	for _, element := range document {
		if element.Key == field {
			return element.Value
		}
	}
	t.Fatalf("operator has no field %s", field)
	return nil
}
