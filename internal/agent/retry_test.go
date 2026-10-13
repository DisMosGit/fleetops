package agent

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// attemptFetcher is a hand-written FirmwareFetcher double replaying one scripted outcome per
// attempt and holding the last outcome for every attempt beyond the script.
type attemptFetcher struct {
	mu       sync.Mutex
	outcomes []error
	path     string
	attempts int
	onFetch  func() // runs after the attempt is recorded
}

// Fetch records the attempt, replays its scripted outcome, and runs the hook.
func (f *attemptFetcher) Fetch(_ context.Context, _, _, _ string, _ func(int32)) (string, error) {
	f.mu.Lock()
	attempt := f.attempts
	f.attempts++
	var err error
	if len(f.outcomes) > 0 {
		err = f.outcomes[min(attempt, len(f.outcomes)-1)]
	}
	path, onFetch := f.path, f.onFetch
	f.mu.Unlock()

	if onFetch != nil {
		onFetch()
	}
	if err != nil {
		return "", err
	}
	return path, nil
}

// calls returns how many attempts reached the fetcher.
func (f *attemptFetcher) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// delayRecorder is the handler's injectable retry wait: it records each delay and returns
// without waiting, so a test asserts the delay sequence instead of sleeping through it.
type delayRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
}

// sleep records d and returns immediately.
func (r *delayRecorder) sleep(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delays = append(r.delays, d)
	return nil
}

// recorded returns the delays observed so far.
func (r *delayRecorder) recorded() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.delays...)
}

// wantDownloadingPhases returns the downloading report each attempt announces, followed by the
// one phase the concluded update reports.
func wantDownloadingPhases(attempts int, succeeded bool) []string {
	want := make([]string, 0, attempts+2)
	for range attempts {
		want = append(want, "UPDATE_PHASE_DOWNLOADING")
	}
	if succeeded {
		return append(want, "UPDATE_PHASE_APPLYING", "UPDATE_PHASE_COMPLETED")
	}
	return append(want, "UPDATE_PHASE_FAILED")
}

func TestUpdateHandlerRetriesTheFetch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		outcomes    []error
		retry       FetchRetry
		wantFetches int
		wantDelays  []time.Duration
		wantOutcome agentv1.CommandOutcome
		wantDetail  string
	}{
		{
			name:        "a transfer fault succeeds on the next attempt",
			outcomes:    []error{ErrTransfer, nil},
			retry:       FetchRetry{Attempts: 3, Max: 4 * RetryInitial},
			wantFetches: 2,
			wantDelays:  []time.Duration{RetryInitial},
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		},
		{
			name:        "a stall succeeds on a later attempt",
			outcomes:    []error{ErrStall, ErrStall, nil},
			retry:       FetchRetry{Attempts: 3, Max: 4 * RetryInitial},
			wantFetches: 3,
			wantDelays:  []time.Duration{RetryInitial, 2 * RetryInitial},
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		},
		{
			name:        "an exhausted budget reports exactly one failure",
			outcomes:    []error{ErrTransfer},
			retry:       FetchRetry{Attempts: 3, Max: 4 * RetryInitial},
			wantFetches: 3,
			wantDelays:  []time.Duration{RetryInitial, 2 * RetryInitial},
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "download failed",
		},
		{
			name:        "the delays grow and are capped",
			outcomes:    []error{ErrTransfer},
			retry:       FetchRetry{Attempts: 5, Max: 2 * RetryInitial},
			wantFetches: 5,
			wantDelays: []time.Duration{
				RetryInitial, 2 * RetryInitial, 2 * RetryInitial, 2 * RetryInitial,
			},
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "download failed",
		},
		{
			name:        "a checksum mismatch is not retried",
			outcomes:    []error{ErrChecksumMismatch},
			retry:       FetchRetry{Attempts: 3, Max: 4 * RetryInitial},
			wantFetches: 1,
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "checksum mismatch",
		},
		{
			name:        "a truncated transfer is not retried",
			outcomes:    []error{ErrTruncatedTransfer},
			retry:       FetchRetry{Attempts: 3, Max: 4 * RetryInitial},
			wantFetches: 1,
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "truncated transfer",
		},
		{
			name:        "a zero budget makes a single attempt",
			outcomes:    []error{ErrTransfer},
			retry:       FetchRetry{},
			wantFetches: 1,
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "download failed",
		},
		{
			name:        "a stall that outlasts the budget names itself",
			outcomes:    []error{ErrStall},
			retry:       FetchRetry{Attempts: 2, Max: RetryInitial},
			wantFetches: 2,
			wantDelays:  []time.Duration{RetryInitial},
			wantOutcome: agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
			wantDetail:  "firmware download stalled",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status := &fakeStatusReporter{}
			results := &fakeResults{}
			fetcher := &attemptFetcher{outcomes: tc.outcomes, path: "/tmp/fw-1.bin"}
			delays := &delayRecorder{}
			handler := NewUpdateHandlerWithRetry(fetcher, &fakeApplier{}, &fakeAdopter{},
				NewUpdateReporter(status, results), tc.retry, slog.New(slog.DiscardHandler))
			handler.sleep = delays.sleep

			if err := handler.Handle(context.Background(), updateCommand()); err != nil {
				t.Fatalf("Handle() error = %v (a failed update is an outcome, not a fault)", err)
			}

			if got := fetcher.calls(); got != tc.wantFetches {
				t.Errorf("fetch attempts = %d, want %d", got, tc.wantFetches)
			}
			if diff := cmp.Diff(tc.wantDelays, delays.recorded()); diff != "" {
				t.Errorf("retry delays mismatch (-want +got):\n%s", diff)
			}
			// One command, one terminal result — however many attempts it took to conclude.
			reports := results.recorded()
			if len(reports) != 1 {
				t.Fatalf("terminal reports = %d, want exactly one", len(reports))
			}
			if got := reports[0].GetOutcome(); got != tc.wantOutcome {
				t.Errorf("terminal outcome = %v, want %v", got, tc.wantOutcome)
			}
			if got := reports[0].GetDetail(); got != tc.wantDetail {
				t.Errorf("terminal detail = %q, want %q", got, tc.wantDetail)
			}
			// Every attempt announces itself with the downloading phase; the phases after the
			// fetch stay outside the retry loop and are reported once.
			want := wantDownloadingPhases(tc.wantFetches,
				tc.wantOutcome == agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED)
			if diff := cmp.Diff(want, phases(status)); diff != "" {
				t.Errorf("reported phases mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUpdateHandlerStopsOnCancellationDuringARetryWait(t *testing.T) {
	t.Parallel()

	// The context ends while the handler waits to retry: the wait stops, and with nothing left
	// to report to, the handler returns the command's error without a terminal result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status := &fakeStatusReporter{}
	results := &fakeResults{}
	fetcher := &attemptFetcher{outcomes: []error{ErrTransfer}, onFetch: cancel}
	handler := NewUpdateHandlerWithRetry(fetcher, &fakeApplier{}, &fakeAdopter{},
		NewUpdateReporter(status, results),
		FetchRetry{Attempts: 5, Max: time.Minute}, slog.New(slog.DiscardHandler))
	// The real ctx-aware wait is what has to stop; the budget would otherwise hold the
	// handler far beyond the assertion's timeout.
	handler.sleep = sleepCtx

	done := make(chan error, 1)
	go func() { done <- handler.Handle(ctx, updateCommand()) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle() error = %v, want the interruption", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle() kept waiting after the context ended")
	}
	if got := fetcher.calls(); got != 1 {
		t.Errorf("fetch attempts = %d, want the one the wait interrupted", got)
	}
	if reports := results.recorded(); len(reports) != 0 {
		t.Errorf("terminal reports = %d, want none once the context is gone", len(reports))
	}
}

func TestUpdateHandlerRetriesInjectedFaults(t *testing.T) {
	t.Parallel()

	// The real injector under the handler's retry: a faulted attempt never reaches the
	// download, is retried while the budget lasts, and ends in exactly one failed result.
	tests := []struct {
		name        string
		kinds       []FaultKind
		stall       time.Duration
		retry       FetchRetry
		wantFetches int
		wantDetail  string
	}{
		{
			name:        "every attempt fails with a transfer fault",
			kinds:       []FaultKind{FaultTransfer},
			retry:       FetchRetry{Attempts: 3, Max: time.Second},
			wantFetches: 3,
			wantDetail:  "download failed",
		},
		{
			name:        "a stall is retried until the budget is spent",
			kinds:       []FaultKind{FaultStall},
			stall:       time.Millisecond,
			retry:       FetchRetry{Attempts: 2, Max: time.Second},
			wantFetches: 2,
			wantDetail:  "firmware download stalled",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &attemptFetcher{path: "/tmp/fw-1.bin"}
			fetcher, err := NewFaultFetcher(base, FaultConfig{
				Share: 1, Kinds: tc.kinds, StallDuration: tc.stall,
			}, rand.NewSource(5))
			if err != nil {
				t.Fatalf("NewFaultFetcher: %v", err)
			}
			status := &fakeStatusReporter{}
			results := &fakeResults{}
			handler := NewUpdateHandlerWithRetry(fetcher, &fakeApplier{}, &fakeAdopter{},
				NewUpdateReporter(status, results), tc.retry, slog.New(slog.DiscardHandler))
			delays := &delayRecorder{}
			handler.sleep = delays.sleep

			if err := handler.Handle(context.Background(), updateCommand()); err != nil {
				t.Fatalf("Handle() error = %v", err)
			}

			if got := base.calls(); got != 0 {
				t.Errorf("decorated downloader calls = %d, want none for a faulted attempt", got)
			}
			if diff := cmp.Diff(wantDownloadingPhases(tc.wantFetches, false), phases(status)); diff != "" {
				t.Errorf("reported phases mismatch (-want +got):\n%s", diff)
			}
			reports := results.recorded()
			if len(reports) != 1 ||
				reports[0].GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED {
				t.Fatalf("terminal reports = %+v, want exactly one failed result", reports)
			}
			if got := reports[0].GetDetail(); got != tc.wantDetail {
				t.Errorf("terminal detail = %q, want %q", got, tc.wantDetail)
			}
		})
	}
}

func TestUpdateHandlerStallEndsWithTheCommandContext(t *testing.T) {
	t.Parallel()

	// An injected stall that outlasts its command ends with the context: the handler returns
	// rather than waiting it out, reports nothing more, and leaves no partial download behind.
	dir := t.TempDir()
	downloader, err := NewDownloader(&fakeService{}, dir)
	if err != nil {
		t.Fatalf("NewDownloader: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher, err := NewFaultFetcher(downloader, FaultConfig{
		Share: 1, Kinds: []FaultKind{FaultStall}, StallDuration: time.Hour,
	}, rand.NewSource(5))
	if err != nil {
		t.Fatalf("NewFaultFetcher: %v", err)
	}
	// The stall ends when the test cancels: the injector keeps its ctx-aware wait, so this is
	// the shutdown the production path performs, without the hour on the clock.
	fetcher.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return sleepCtx(ctx, time.Hour)
	}

	status := &fakeStatusReporter{}
	results := &fakeResults{}
	handler := NewUpdateHandlerWithRetry(fetcher, &fakeApplier{}, &fakeAdopter{},
		NewUpdateReporter(status, results),
		FetchRetry{Attempts: 3, Max: time.Second}, slog.New(slog.DiscardHandler))

	done := make(chan error, 1)
	go func() { done <- handler.Handle(ctx, updateCommand()) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle() error = %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle() kept waiting inside the stall")
	}
	if reports := results.recorded(); len(reports) != 0 {
		t.Errorf("terminal reports = %d, want none once the context is gone", len(reports))
	}
	if files := dirFiles(t, dir); len(files) != 0 {
		t.Errorf("download directory holds %v, want no partial file after a stall", files)
	}
}
