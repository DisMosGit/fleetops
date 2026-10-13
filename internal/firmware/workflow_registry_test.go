package firmware

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// uploadRequestOf records one StartOrAttach call: the request it was handed and the file the
// request's spooled binary held at that moment.
type uploadRequestOf struct {
	request UploadRequest
	content []byte
}

// fakeStarter is a hand-written UploadStarter double. It stands in for the workflow execution:
// one upload of a version starts (the others attach), and the run it returns drives the real
// store steps an upload's workflow drives, so a test asserts the registry's behavior against the
// record the upload actually leaves.
type fakeStarter struct {
	store   *Store
	mu      sync.Mutex
	started map[string]bool
	calls   []uploadRequestOf
	err     error
	// wait blocks every run's Await until its context ends, standing in for an upload that
	// outlives the endpoint's wait.
	wait bool
}

// StartOrAttach records the request, letting exactly one caller start a version's upload.
func (f *fakeStarter) StartOrAttach(_ context.Context, req UploadRequest) (UploadRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	content, err := os.ReadFile(req.SpoolPath)
	if err != nil {
		return nil, err
	}
	f.calls = append(f.calls, uploadRequestOf{request: req, content: content})
	if f.started[req.Version] {
		return fakeRun{attached: true}, nil
	}
	f.started[req.Version] = true
	return fakeRun{store: f.store, req: req, wait: f.wait}, nil
}

// requests returns the recorded calls.
func (f *fakeStarter) requests() []uploadRequestOf {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uploadRequestOf(nil), f.calls...)
}

// fakeRun is the execution handle a started or attached upload is answered with.
type fakeRun struct {
	store    *Store
	req      UploadRequest
	attached bool
	wait     bool
}

// Attached reports whether the upload reached an execution another request started.
func (r fakeRun) Attached() bool { return r.attached }

// Await drives the upload's steps — storage addressed by version, then the record that makes it
// available — and reports the state the upload concluded with.
func (r fakeRun) Await(ctx context.Context) (UploadState, error) {
	if r.wait {
		<-ctx.Done()
		return UploadState{}, ctx.Err()
	}
	stored, err := r.store.StoreBinary(ctx, r.req.Version, r.req.SpoolPath)
	if err != nil {
		return UploadState{}, err
	}
	rec, err := r.store.UpsertMetadata(ctx, UploadMetadata{
		Version: r.req.Version, Models: r.req.Models,
		WorkflowID: "firmware-upload-" + r.req.Version,
		State:      UploadAvailable, Binary: &stored,
	})
	if err != nil {
		return UploadState{}, err
	}
	return UploadState{
		Version: rec.Version, State: UploadAvailable, Concluded: true,
		FirmwareID: rec.ID, Models: rec.Models, Checksum: rec.Checksum, Size: rec.Size,
	}, nil
}

// fakeStates is a hand-written UploadStates double answering a scripted state.
type fakeStates struct {
	state UploadState
	err   error
}

// State returns the scripted state.
func (f *fakeStates) State(context.Context, string) (UploadState, error) {
	return f.state, f.err
}

// newUploadRegistry returns the workflow-backed registry over a fake workflow backend.
func newUploadRegistry(
	t *testing.T,
	store *Store,
	starter UploadStarter,
	states UploadStates,
	wait time.Duration,
) *WorkflowRegistry {
	t.Helper()
	return NewWorkflowRegistry(starter, states, NewSpool(t.TempDir()), wait, slog.New(slog.DiscardHandler))
}

func TestWorkflowRegistrySave(t *testing.T) {
	t.Parallel()

	t.Run("the binary is spooled before the workflow starts", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		starter := &fakeStarter{store: store, started: map[string]bool{}}
		registry := newUploadRegistry(t, store, starter, &fakeStates{}, 30*time.Second)

		rec, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
			strings.NewReader("firmware bytes"))
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		calls := starter.requests()
		if len(calls) != 1 {
			t.Fatalf("starts = %d, want 1", len(calls))
		}
		got := calls[0]
		if got.request.Version != "2.0.0" {
			t.Errorf("start request version = %q, want 2.0.0", got.request.Version)
		}
		if diff := cmp.Diff([]string{"oak-s3"}, got.request.Models); diff != "" {
			t.Errorf("start request models mismatch (-want +got):\n%s", diff)
		}
		if string(got.content) != "firmware bytes" {
			t.Errorf("spooled content at start = %q, want the request body", got.content)
		}
		if got.request.SpoolPath == "" {
			t.Error("start request carries no spool path")
		}
		if rec.ID == "" || rec.Checksum != checksumOf([]byte("firmware bytes")) {
			t.Errorf("record = %+v, want the stored upload's record", rec)
		}
	})

	t.Run("the spool file is removed when the upload concluded", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		starter := &fakeStarter{store: store, started: map[string]bool{}}
		registry := newUploadRegistry(t, store, starter, &fakeStates{}, 30*time.Second)

		if _, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
			strings.NewReader("firmware bytes")); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		path := starter.requests()[0].request.SpoolPath
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("spool file = %v, want it removed once the upload concluded", err)
		}
	})

	t.Run("an upload that outlives the wait keeps its spool file", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		starter := &fakeStarter{store: store, started: map[string]bool{}, wait: true}
		registry := newUploadRegistry(t, store, starter, &fakeStates{}, 10*time.Millisecond)

		_, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
			strings.NewReader("firmware bytes"))
		if !errors.Is(err, ErrUploadContinues) {
			t.Fatalf("Save() error = %v, want the upload reported as still running", err)
		}
		path := starter.requests()[0].request.SpoolPath
		if _, err := os.Stat(path); err != nil {
			t.Errorf("spool file = %v, want it kept for the upload still storing from it", err)
		}
	})

	t.Run("an attached upload is the version conflict and stores nothing", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		starter := &fakeStarter{store: store, started: map[string]bool{"2.0.0": true}}
		registry := newUploadRegistry(t, store, starter, &fakeStates{}, 30*time.Second)

		_, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
			strings.NewReader("firmware bytes"))
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Save() error = %v, want ErrVersionConflict", err)
		}
		path := starter.requests()[0].request.SpoolPath
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("spool file = %v, want the losing upload's file removed", err)
		}
	})

	t.Run("a failing start removes the spooled file", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		starter := &fakeStarter{store: store, started: map[string]bool{}, err: errors.New("frontend down")}
		registry := newUploadRegistry(t, store, starter, &fakeStates{}, 30*time.Second)

		if _, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
			strings.NewReader("firmware bytes")); err == nil {
			t.Fatal("Save() error = nil, want the start failure")
		}
		entries, err := os.ReadDir(registry.spool.dir)
		if err != nil {
			t.Fatalf("read spool directory: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("spool files = %d, want none after a failed start", len(entries))
		}
	})

	t.Run("a failed upload is reported as the sentinel its class names", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name    string
			state   UploadState
			wantErr error
		}{
			{
				name: "a refused model",
				state: UploadState{
					Version: "2.0.0", State: UploadFailed, Concluded: true,
					Failure: FailureRefused, Reason: `firmware incompatible with target device models: unknown model "walnut-9"`,
				},
				wantErr: ErrIncompatible,
			},
			{
				name: "a version another upload owns",
				state: UploadState{
					Version: "2.0.0", State: UploadFailed, Concluded: true,
					Failure: FailureConflict, Reason: "firmware version already registered",
				},
				wantErr: ErrVersionConflict,
			},
			{
				name: "a step that could not be completed",
				state: UploadState{
					Version: "2.0.0", State: UploadFailed, Concluded: true,
					Failure: FailureInternal, Reason: "the upload could not be completed",
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
				registry := newUploadRegistry(t, store,
					&failingStarter{state: tc.state}, &fakeStates{}, 30*time.Second)

				_, err := registry.Save(context.Background(), "2.0.0", []string{"oak-s3"},
					strings.NewReader("firmware bytes"))
				if err == nil {
					t.Fatal("Save() error = nil, want the upload's failure")
				}
				if tc.wantErr == nil {
					if errors.Is(err, ErrIncompatible) || errors.Is(err, ErrVersionConflict) {
						t.Errorf("Save() error = %v, want a plain internal failure", err)
					}
					return
				}
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("Save() error = %v, want %v", err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.state.Reason) {
					t.Errorf("Save() error = %q, want it to carry the reason %q", err, tc.state.Reason)
				}
			})
		}
	})

	t.Run("the state query is answered by the workflow backend", func(t *testing.T) {
		t.Parallel()

		want := UploadState{Version: "2.0.0", State: UploadStoring}
		registry := newUploadRegistry(t, &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()},
			&fakeStarter{started: map[string]bool{}}, &fakeStates{state: want}, 30*time.Second)

		got, err := registry.UploadState(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})
}

// failingStarter is an UploadStarter whose started upload concludes with a scripted failure.
type failingStarter struct {
	state UploadState
}

// StartOrAttach returns a run that concludes with the scripted state.
func (f *failingStarter) StartOrAttach(context.Context, UploadRequest) (UploadRun, error) {
	return failingRun{state: f.state}, nil
}

// failingRun is a started upload that concludes with a scripted failure.
type failingRun struct {
	state UploadState
}

// Attached reports that this request started the upload.
func (r failingRun) Attached() bool { return false }

// Await returns the scripted failure.
func (r failingRun) Await(context.Context) (UploadState, error) { return r.state, nil }

// TestUploadConcurrentVersionsCollapseToOneRecord pins the endpoint's answer when two uploads of
// one version race: exactly one of them starts the upload that stores the record, the other
// attaches to that execution and is answered the conflict, and one firmware record exists
// afterwards. The backend's single winner is what the test's starter arbitrates, exactly as
// Temporal's workflow-id conflict policy arbitrates it in the running system.
func TestUploadConcurrentVersionsCollapseToOneRecord(t *testing.T) {
	t.Parallel()

	meta := newFakeMetadata()
	bins := newFakeBinaries()
	store := &Store{meta: meta, binaries: bins}
	registry := newUploadRegistry(t, store,
		&fakeStarter{store: store, started: map[string]bool{}}, &fakeStates{}, 30*time.Second)
	handler := NewHandler(registry, fakeCatalog{models: []string{"oak-s3"}},
		slog.New(slog.DiscardHandler))
	fields := map[string][]string{"version": {"2.0.0"}, "models": {"oak-s3"}}

	const uploads = 2
	requests := make([]*http.Request, uploads)
	recorders := make([]*httptest.ResponseRecorder, uploads)
	for i := range uploads {
		requests[i] = uploadRequest(t, fields, true, []byte("bin"))
		recorders[i] = httptest.NewRecorder()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range uploads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			handler.ServeHTTP(recorders[i], requests[i])
		}()
	}
	close(start)
	wg.Wait()

	statuses := map[int]int{}
	for i, rec := range recorders {
		statuses[rec.Code]++
		if rec.Code != http.StatusCreated && rec.Code != http.StatusConflict {
			t.Errorf("upload %d status = %d (body %s), want 201 or 409", i, rec.Code, rec.Body)
		}
	}
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusConflict] != 1 {
		t.Errorf("statuses = %v, want exactly one created and one conflict", statuses)
	}
	meta.mu.Lock()
	records := len(meta.docs)
	meta.mu.Unlock()
	if records != 1 {
		t.Errorf("firmware records = %d, want exactly one", records)
	}
	if got := bins.puts; got != 1 {
		t.Errorf("binary writes = %d, want exactly one", got)
	}
}
