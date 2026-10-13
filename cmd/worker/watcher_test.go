package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// fakePoolWatcher is a hand-written poolWatcher double: it reports when it has been started and
// when it has returned, and the test decides when its stream is established, so each moment of
// the watcher's lifecycle is drivable without a database.
type fakePoolWatcher struct {
	// established is closed by the test to establish the fake's stream. A fake whose stream
	// cannot be established returns before it is ever closed.
	established chan struct{}
	// err is what the fake's stream ends in. A non-nil err on a fake that is not asked to fail
	// after establishment is the startup failure.
	err error
	// failAfterEstablishment makes the fake report err only once its stream was established,
	// which is a stream lost while the worker is running rather than a stream that never came up.
	failAfterEstablishment bool

	started chan struct{}
	stopped chan struct{}
}

func newFakePoolWatcher() *fakePoolWatcher {
	return &fakePoolWatcher{
		established: make(chan struct{}),
		started:     make(chan struct{}),
		stopped:     make(chan struct{}),
	}
}

func (w *fakePoolWatcher) Watch(ctx context.Context, ready chan<- struct{}) error {
	close(w.started)
	defer close(w.stopped)
	if w.err != nil && !w.failAfterEstablishment {
		return w.err
	}
	select {
	case <-w.established:
		close(ready)
	case <-ctx.Done():
		return nil
	}
	if w.err != nil {
		return w.err
	}
	<-ctx.Done()
	return nil
}

// establish closes the fake's establishment signal, as a watcher does once its stream is up.
func (w *fakePoolWatcher) establish() { close(w.established) }

// awaitWatcherStarted fails the test unless the fake has been started within the budget.
func awaitWatcherStarted(t *testing.T, w *fakePoolWatcher) {
	t.Helper()
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher was never started")
	}
}

// TestStartPoolWatcher pins the worker's startup contract with the eligible-device view: the
// watcher is running and its change stream is established before the worker proceeds to poll, and
// a stream that cannot be established fails the startup rather than leaving resolutions answered
// from a view nobody maintains.
func TestStartPoolWatcher(t *testing.T) {
	t.Parallel()

	t.Run("the worker proceeds only once the stream is established", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g, gctx := errgroup.WithContext(ctx)
		watcher := newFakePoolWatcher()

		done := make(chan error, 1)
		go func() { done <- startPoolWatcher(gctx, g, watcher) }()
		awaitWatcherStarted(t, watcher)

		select {
		case err := <-done:
			t.Fatalf("startPoolWatcher returned %v before the stream was established", err)
		case <-time.After(50 * time.Millisecond):
		}
		watcher.establish()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("startPoolWatcher = %v, want nil once the stream is established", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("startPoolWatcher did not return after the stream was established")
		}

		cancel()
		if err := g.Wait(); err != nil {
			t.Errorf("the run ended with %v, want nil so a graceful stop is not a failure", err)
		}
	})

	t.Run("a stream that cannot be established fails the startup with its reason", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g, gctx := errgroup.WithContext(ctx)
		watcher := newFakePoolWatcher()
		watcher.err = errors.New("devices change stream unavailable")

		err := startPoolWatcher(gctx, g, watcher)
		if !errors.Is(err, watcher.err) {
			t.Fatalf("startPoolWatcher = %v, want the watcher's own failure", err)
		}
		if err := g.Wait(); !errors.Is(err, watcher.err) {
			t.Errorf("the run ended with %v, want the watcher's failure", err)
		}
	})

	t.Run("a stream lost after startup fails the run it belongs to", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g, gctx := errgroup.WithContext(ctx)
		watcher := newFakePoolWatcher()
		watcher.failAfterEstablishment = true
		watcher.err = errors.New("devices change stream lost")
		go watcher.establish()

		if err := startPoolWatcher(gctx, g, watcher); err != nil {
			t.Fatalf("startPoolWatcher = %v, want nil while the stream is up", err)
		}
		if err := g.Wait(); !errors.Is(err, watcher.err) {
			t.Errorf("the run ended with %v, want the watcher's failure", err)
		}
	})

	t.Run("the watcher stops with the worker's context", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		g, gctx := errgroup.WithContext(ctx)
		watcher := newFakePoolWatcher()
		go watcher.establish()

		if err := startPoolWatcher(gctx, g, watcher); err != nil {
			t.Fatalf("startPoolWatcher = %v, want nil once the stream is established", err)
		}
		cancel()
		select {
		case <-watcher.stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("the watcher did not stop with the worker's context")
		}
		if err := g.Wait(); err != nil {
			t.Errorf("the run ended with %v, want nil on a graceful stop", err)
		}
	})
}
