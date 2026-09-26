package devices

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeMarker records MarkStale calls and replays queued results and errors.
type fakeMarker struct {
	mu      sync.Mutex
	cutoffs []time.Time
	results []int64
	errs    []error
	calls   chan struct{}
}

func newFakeMarker(results ...int64) *fakeMarker {
	return &fakeMarker{results: results, calls: make(chan struct{}, 64)}
}

func (f *fakeMarker) MarkStale(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	f.cutoffs = append(f.cutoffs, cutoff)
	var (
		result int64
		err    error
	)
	if len(f.results) > 0 {
		result = f.results[0]
		f.results = f.results[1:]
	}
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return result, err
}

// waitCalls waits for n more signals on calls.
func waitCalls(t *testing.T, calls chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sweep pass %d of %d", i+1, n)
		}
	}
}

func TestSweeperReportsTransitionsOncePerPass(t *testing.T) {
	t.Parallel()

	marker := newFakeMarker(3, 0, 2)
	reported := make(chan int64, 8)
	sweeper := NewSweeper(marker, time.Minute, 5*time.Millisecond, func(n int64) {
		reported <- n
	}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	waitCalls(t, marker.calls, 3)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}

	// A pass that finds nothing to flip must not report: only 3 and 2 are transitions.
	want := []int64{3, 2}
	for _, w := range want {
		select {
		case got := <-reported:
			if got != w {
				t.Errorf("reported transition count = %d, want %d", got, w)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for reported count %d", w)
		}
	}
	select {
	case got := <-reported:
		t.Errorf("reported transition count = %d, want no further reports", got)
	default:
	}
}

func TestSweeperPassesStaleCutoff(t *testing.T) {
	t.Parallel()

	threshold := 100 * time.Millisecond
	marker := newFakeMarker()
	sweeper := NewSweeper(marker, threshold, 5*time.Millisecond, func(int64) {}, slog.Default())

	started := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	waitCalls(t, marker.calls, 1)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	marker.mu.Lock()
	cutoff := marker.cutoffs[0]
	marker.mu.Unlock()
	// cutoff = pass time - threshold, so devices heard within the threshold stay online.
	if cutoff.Before(started.Add(-threshold)) {
		t.Errorf("cutoff %v predates the sweep start minus the threshold", cutoff)
	}
	if cutoff.After(time.Now().Add(-threshold)) {
		t.Errorf("cutoff %v postdates now minus the threshold", cutoff)
	}
}

func TestSweeperSurvivesFailedPass(t *testing.T) {
	t.Parallel()

	marker := newFakeMarker(0, 5)
	marker.errs = []error{errors.New("connection reset")}
	reported := make(chan int64, 8)
	sweeper := NewSweeper(marker, time.Minute, 5*time.Millisecond, func(n int64) {
		reported <- n
	}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	waitCalls(t, marker.calls, 2)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}

	select {
	case got := <-reported:
		if got != 5 {
			t.Errorf("reported transition count = %d, want 5 from the successful pass", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the successful pass to report")
	}
}

func TestSweeperStopsOnCancel(t *testing.T) {
	t.Parallel()

	sweeper := NewSweeper(newFakeMarker(), time.Minute, time.Hour, func(int64) {}, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after ctx cancellation")
	}
}
