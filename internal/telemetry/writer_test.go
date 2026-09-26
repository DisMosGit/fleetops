package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// fakeEvents records InsertMany calls and returns queued errors first-come.
type fakeEvents struct {
	mu      sync.Mutex
	batches [][]any
	errs    []error
	calls   chan struct{}
}

func newFakeEvents(errs ...error) *fakeEvents {
	return &fakeEvents{errs: errs, calls: make(chan struct{}, 64)}
}

func (f *fakeEvents) InsertMany(
	_ context.Context,
	documents any,
	_ ...options.Lister[options.InsertManyOptions],
) (*mongo.InsertManyResult, error) {
	docs, ok := documents.([]any)
	if !ok {
		return nil, fmt.Errorf("documents of type %T, want []any", documents)
	}
	f.mu.Lock()
	f.batches = append(f.batches, docs)
	var err error
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return &mongo.InsertManyResult{}, err
}

func (f *fakeEvents) recorded() [][]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]any(nil), f.batches...)
}

// fakeApplier records ApplyBatch calls and returns a fixed error.
type fakeApplier struct {
	mu      sync.Mutex
	batches [][]devices.Update
	err     error
	calls   chan struct{}
}

func newFakeApplier() *fakeApplier {
	return &fakeApplier{calls: make(chan struct{}, 64)}
}

func (f *fakeApplier) ApplyBatch(_ context.Context, updates []devices.Update) error {
	f.mu.Lock()
	f.batches = append(f.batches, append([]devices.Update(nil), updates...))
	err := f.err
	f.mu.Unlock()
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return err
}

func (f *fakeApplier) recorded() [][]devices.Update {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]devices.Update(nil), f.batches...)
}

// heartbeat builds one accepted heartbeat.
func heartbeat(eventID, deviceID string, ts time.Time) *agentv1.Heartbeat {
	return &agentv1.Heartbeat{
		EventId:   eventID,
		DeviceId:  deviceID,
		CurrentFw: "2.0.0",
		Ts:        timestamppb.New(ts),
		Cpu:       0.5,
		Mem:       0.4,
		Health:    0.9,
	}
}

// waitCalls waits for n more signals on calls.
func waitCalls(t *testing.T, calls chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for call %d of %d", i+1, n)
		}
	}
}

func TestWriterFlushesOnBatchSize(t *testing.T) {
	t.Parallel()

	events := newFakeEvents()
	applier := newFakeApplier()
	w := NewWriter(events, applier, 2, time.Hour, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	if err := w.Handle(ctx, heartbeat("ev-1", "dev-1", base), rec); err != nil {
		t.Fatalf("Handle(ev-1) error = %v", err)
	}
	if err := w.Handle(ctx, heartbeat("ev-2", "dev-1", base.Add(time.Second)), rec); err != nil {
		t.Fatalf("Handle(ev-2) error = %v", err)
	}
	waitCalls(t, events.calls, 1)

	batches := events.recorded()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("inserted batches = %v, want one batch of 2", batches)
	}
	want := event{
		ID:     "ev-1",
		TS:     base,
		Meta:   meta{DeviceID: "dev-1", Region: "eu-west", Model: "v3"},
		CPU:    0.5,
		Mem:    0.4,
		Health: 0.9,
	}
	if diff := cmp.Diff(want, batches[0][0].(event)); diff != "" {
		t.Errorf("event mismatch (-want +got):\n%s", diff)
	}

	updates := applier.recorded()
	if len(updates) != 1 || len(updates[0]) != 2 {
		t.Fatalf("applied batches = %v, want one batch of 2", updates)
	}
	if updates[0][0].ID != "dev-1" || updates[0][0].Status != devices.StatusOnline {
		t.Errorf("applied update = %+v, want an online refresh for dev-1", updates[0][0])
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestWriterFlushesOnInterval(t *testing.T) {
	t.Parallel()

	events := newFakeEvents()
	w := NewWriter(events, newFakeApplier(), 100, 10*time.Millisecond, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if err := w.Handle(ctx, heartbeat("ev-1", "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
		t.Fatalf("Handle(ev-1) error = %v", err)
	}
	waitCalls(t, events.calls, 1)
	if got := events.recorded(); len(got) != 1 || len(got[0]) != 1 {
		t.Errorf("inserted batches = %v, want one batch of 1", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestWriterTreatsRedeliveriesAsNoOps(t *testing.T) {
	t.Parallel()

	duplicate := mongo.WriteException{
		WriteErrors: mongo.WriteErrors{{Code: errDuplicateKey, Message: "dup"}},
	}
	events := newFakeEvents(duplicate)
	applier := newFakeApplier()
	w := NewWriter(events, applier, 1, 5*time.Millisecond, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if err := w.Handle(ctx, heartbeat("ev-1", "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
		t.Fatalf("Handle(ev-1) error = %v", err)
	}
	// A redelivered event must not stop ingestion: the next event is still written.
	if err := w.Handle(ctx, heartbeat("ev-2", "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
		t.Fatalf("Handle(ev-2) error = %v", err)
	}
	waitCalls(t, events.calls, 2)
	waitCalls(t, applier.calls, 2)

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestWriterRetriesFailedFlush(t *testing.T) {
	t.Parallel()

	events := newFakeEvents(errors.New("connection reset"))
	applier := newFakeApplier()
	w := NewWriter(events, applier, 1, 5*time.Millisecond, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if err := w.Handle(ctx, heartbeat("ev-1", "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
		t.Fatalf("Handle(ev-1) error = %v", err)
	}
	waitCalls(t, events.calls, 2)

	batches := events.recorded()
	if len(batches) != 2 {
		t.Fatalf("insert attempts = %d, want 2", len(batches))
	}
	if diff := cmp.Diff(batches[0][0].(event), batches[1][0].(event)); diff != "" {
		t.Errorf("retried batch differs (-first +second):\n%s", diff)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestWriterDrainsOnShutdown(t *testing.T) {
	t.Parallel()

	events := newFakeEvents()
	applier := newFakeApplier()
	w := NewWriter(events, applier, 100, time.Hour, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if err := w.Handle(ctx, heartbeat("ev-1", "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
		t.Fatalf("Handle(ev-1) error = %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := events.recorded(); len(got) != 1 || len(got[0]) != 1 {
		t.Errorf("inserted batches = %v, want the queued event flushed at shutdown", got)
	}
}

func TestWriterRefusesHeartbeatWithoutMeasurementTime(t *testing.T) {
	t.Parallel()

	events := newFakeEvents()
	applier := newFakeApplier()
	w := NewWriter(events, applier, 10, time.Hour, slog.Default())

	hb := heartbeat("ev-1", "dev-1", time.Now())
	hb.Ts = nil
	err := w.Handle(context.Background(), hb, devices.Record{ID: "dev-1"})
	if err == nil {
		t.Fatal("Handle() = nil error, want an error for a missing measurement time")
	}
	if len(events.recorded()) != 0 || len(applier.recorded()) != 0 {
		t.Error("a heartbeat without measurement time reached persistence")
	}
}

func TestWriterBackpressureBlocksInsteadOfBuffering(t *testing.T) {
	t.Parallel()

	events := newFakeEvents()
	w := NewWriter(events, newFakeApplier(), 2, time.Hour, slog.Default())

	// With Run stopped the queue fills at 2 * batchSize and the next Handle blocks.
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := w.Handle(ctx, heartbeat(fmt.Sprintf("ev-%d", i), "dev-1", time.Now()), devices.Record{ID: "dev-1"}); err != nil {
			t.Fatalf("Handle(ev-%d) error = %v", i, err)
		}
	}
	blocked, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	err := w.Handle(blocked, heartbeat("ev-overflow", "dev-1", time.Now()), devices.Record{ID: "dev-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Handle() on a full pipeline error = %v, want context.DeadlineExceeded", err)
	}
}

func TestOnlyRedeliveries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "duplicate key only",
			err:  mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: errDuplicateKey}}},
			want: true,
		},
		{
			name: "wrapped duplicate key only",
			err: fmt.Errorf("insert: %w",
				mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: errDuplicateKey}}}),
			want: true,
		},
		{
			name: "duplicate key with a real write error",
			err: mongo.WriteException{WriteErrors: mongo.WriteErrors{
				{Code: errDuplicateKey}, {Code: 121},
			}},
			want: false,
		},
		{
			name: "no write errors",
			err:  mongo.WriteException{},
			want: false,
		},
		{
			name: "write concern error alongside duplicates",
			err: mongo.WriteException{
				WriteErrors:       mongo.WriteErrors{{Code: errDuplicateKey}},
				WriteConcernError: &mongo.WriteConcernError{Code: 64},
			},
			want: false,
		},
		{
			name: "bulk write duplicates only",
			err:  mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{Code: errDuplicateKey}}},
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection reset"),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := onlyRedeliveries(tc.err); got != tc.want {
				t.Errorf("onlyRedeliveries(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
