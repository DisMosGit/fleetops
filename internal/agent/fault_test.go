package agent

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// faultAttempt runs one fetch through fetcher and describes what it observed: the delegated
// path on success, or the error class and kind the attempt ended in.
func faultAttempt(fetcher *FaultFetcher, deviceID string) string {
	path, err := fetcher.Fetch(context.Background(), deviceID, "fw-1", "sum", nil)
	if err != nil {
		return "error: " + err.Error()
	}
	return "path: " + path
}

// newTestFaultFetcher returns an injector over a recording base fetcher, with a stall that
// costs the test no time unless the test overrides it.
func newTestFaultFetcher(
	t *testing.T,
	cfg FaultConfig,
	src rand.Source,
) (*FaultFetcher, *fakeFetcher) {
	t.Helper()

	base := &fakeFetcher{path: "/tmp/fw-1.bin"}
	fetcher, err := NewFaultFetcher(base, cfg, src)
	if err != nil {
		t.Fatalf("NewFaultFetcher: %v", err)
	}
	fetcher.sleep = func(context.Context, time.Duration) error { return nil }
	return fetcher, base
}

func TestNewFaultFetcherValidation(t *testing.T) {
	t.Parallel()

	base := &fakeFetcher{path: "/tmp/fw-1.bin"}
	tests := []struct {
		name    string
		base    FirmwareFetcher
		cfg     FaultConfig
		src     rand.Source
		wantErr string // substring the error must contain; empty means valid
	}{
		{name: "missing base", base: nil, cfg: FaultConfig{}, src: rand.NewSource(1), wantErr: "base"},
		{name: "missing source", base: base, cfg: FaultConfig{}, src: nil, wantErr: "source"},
		{name: "share above one", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: 1.5, Kinds: FaultKinds()}, wantErr: "share"},
		{name: "negative share", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: -0.1, Kinds: FaultKinds()}, wantErr: "share"},
		{name: "positive share without kinds", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: 0.5}, wantErr: "kinds"},
		{name: "unknown kind", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: 0.5, Kinds: []FaultKind{"corrupt"}}, wantErr: "kind"},
		{name: "stall kind without a stall duration", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: 1, Kinds: []FaultKind{FaultStall}}, wantErr: "stall duration"},
		{name: "zero configuration injects nothing", base: base,
			cfg: FaultConfig{}, src: rand.NewSource(1)},
		{name: "configured faults are valid", base: base, src: rand.NewSource(1),
			cfg: FaultConfig{Share: 0.5, Kinds: FaultKinds(), StallDuration: time.Second}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewFaultFetcher(tc.base, tc.cfg, tc.src)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("NewFaultFetcher() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("NewFaultFetcher() error = nil, want a validation error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("NewFaultFetcher() error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestFaultFetcherZeroConfigDelegates(t *testing.T) {
	t.Parallel()

	// No fault settings means no faults: every attempt reaches the decorated fetcher and
	// returns exactly what it returns.
	fetcher, base := newTestFaultFetcher(t, FaultConfig{}, rand.NewSource(1))
	for range 5 {
		got := faultAttempt(fetcher, "device-0")
		if want := "path: /tmp/fw-1.bin"; got != want {
			t.Fatalf("Fetch() = %q, want %q", got, want)
		}
	}
	if calls := base.calls; len(calls) != 5 {
		t.Errorf("decorated fetcher calls = %d, want 5", len(calls))
	}
}

func TestFaultFetcherShareBounds(t *testing.T) {
	t.Parallel()

	t.Run("a full share faults every attempt", func(t *testing.T) {
		t.Parallel()

		fetcher, base := newTestFaultFetcher(t, FaultConfig{
			Share: 1, Kinds: []FaultKind{FaultTransfer},
		}, rand.NewSource(1))
		for range 10 {
			_, err := fetcher.Fetch(context.Background(), "device-0", "fw-1", "sum", nil)
			if !errors.Is(err, ErrTransfer) {
				t.Fatalf("Fetch() error = %v, want the injected transfer fault", err)
			}
		}
		if calls := base.calls; len(calls) != 0 {
			t.Errorf("decorated fetcher calls = %d, want none under a full fault share", len(calls))
		}
	})

	t.Run("a zero share faults no attempt", func(t *testing.T) {
		t.Parallel()

		fetcher, base := newTestFaultFetcher(t, FaultConfig{
			Share: 0, Kinds: FaultKinds(), StallDuration: time.Second,
		}, rand.NewSource(1))
		for range 10 {
			if _, err := fetcher.Fetch(context.Background(), "device-0", "fw-1", "sum", nil); err != nil {
				t.Fatalf("Fetch() error = %v, want the decorated fetcher's outcome", err)
			}
		}
		if calls := base.calls; len(calls) != 10 {
			t.Errorf("decorated fetcher calls = %d, want 10", len(calls))
		}
	})
}

func TestFaultFetcherSameSeedReplaysTheSameFaults(t *testing.T) {
	t.Parallel()

	// The same seed and the same attempt sequence replay the same faults, kinds included, so a
	// test pins a device's fault by seed rather than by timing.
	run := func(seed int64) []string {
		fetcher, _ := newTestFaultFetcher(t, FaultConfig{
			Share: 0.5, Kinds: FaultKinds(), StallDuration: time.Second,
		}, rand.NewSource(seed))
		var got []string
		for _, device := range []string{"device-0", "device-1", "device-2"} {
			for range 5 {
				got = append(got, faultAttempt(fetcher, device))
			}
		}
		return got
	}

	first, second := run(7), run(7)
	if diff := cmp.Diff(first, second); diff != "" {
		t.Errorf("fault sequence mismatch across runs of one seed (-first +second):\n%s", diff)
	}
	// A sequence of nothing but faults, or nothing but delegations, would replay trivially:
	// pin that this seed draws a mix of both kinds and clean attempts.
	stalls, transfers, clean := 0, 0, 0
	for _, attempt := range first {
		switch {
		case strings.Contains(attempt, ErrStall.Error()):
			stalls++
		case strings.Contains(attempt, ErrTransfer.Error()):
			transfers++
		default:
			clean++
		}
	}
	if stalls == 0 || transfers == 0 || clean == 0 {
		t.Errorf("fault sequence = %d stalls, %d transfers, %d clean, want a mix of all three",
			stalls, transfers, clean)
	}
}

func TestFaultFetcherDecisionsArePerDevice(t *testing.T) {
	t.Parallel()

	// A device's draws come from its own stream, so another device's attempts — interleaved
	// however they arrive — cannot move them. Two injectors share the seed; one sees only
	// device-1, the other sees device-2 drawing between device-1's attempts.
	cfg := FaultConfig{Share: 0.5, Kinds: FaultKinds(), StallDuration: time.Second}
	aloneFetcher, _ := newTestFaultFetcher(t, cfg, rand.NewSource(11))
	sharedFetcher, _ := newTestFaultFetcher(t, cfg, rand.NewSource(11))

	var alone, interleaved, other []string
	for range 6 {
		alone = append(alone, faultAttempt(aloneFetcher, "device-1"))
		other = append(other, faultAttempt(sharedFetcher, "device-2"))
		interleaved = append(interleaved, faultAttempt(sharedFetcher, "device-1"))
	}
	if diff := cmp.Diff(alone, interleaved); diff != "" {
		t.Errorf("device-1 faults moved when device-2 drew between them (-alone +interleaved):\n%s", diff)
	}
	// Both devices drawing at the same share keeps the sequences non-vacuous: the seed pins
	// at least one fault across them.
	faults := 0
	for _, attempt := range append(append([]string(nil), alone...), other...) {
		if strings.HasPrefix(attempt, "error:") {
			faults++
		}
	}
	if faults == 0 {
		t.Error("no attempt faulted, want the pinned seed to draw at least one fault")
	}
}

func TestFaultFetcherStallEndsWithTheContext(t *testing.T) {
	t.Parallel()

	// A stall waits out its duration only while the command is alive: cancellation ends it
	// promptly, and a faulted attempt never reaches the download on disk.
	fetcher, base := newTestFaultFetcher(t, FaultConfig{
		Share: 1, Kinds: []FaultKind{FaultStall}, StallDuration: time.Hour,
	}, rand.NewSource(1))
	fetcher.sleep = sleepCtx

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := fetcher.Fetch(ctx, "device-0", "fw-1", "sum", nil)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Fetch() error = %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stall did not end with the context")
	}
	if calls := base.calls; len(calls) != 0 {
		t.Errorf("decorated fetcher calls = %d, want none during a stall", len(calls))
	}
}

func TestFaultFetcherStallFailsTheAttempt(t *testing.T) {
	t.Parallel()

	fetcher, _ := newTestFaultFetcher(t, FaultConfig{
		Share: 1, Kinds: []FaultKind{FaultStall}, StallDuration: 250 * time.Millisecond,
	}, rand.NewSource(1))

	_, err := fetcher.Fetch(context.Background(), "device-0", "fw-1", "sum", nil)
	if !errors.Is(err, ErrStall) {
		t.Fatalf("Fetch() error = %v, want the stalled attempt", err)
	}
	if !retryableFetchError(err) {
		t.Errorf("retryableFetchError(%v) = false, want a stall to be retryable", err)
	}
}

func TestFaultFetcherStallLeavesNoPartialDownload(t *testing.T) {
	t.Parallel()

	// The decorated fetcher is a real downloader over a real directory: a stalled attempt must
	// leave nothing behind, not even a partial file.
	dir := t.TempDir()
	downloader, err := NewDownloader(&fakeService{}, dir)
	if err != nil {
		t.Fatalf("NewDownloader: %v", err)
	}
	fetcher, err := NewFaultFetcher(downloader, FaultConfig{
		Share: 1, Kinds: []FaultKind{FaultStall}, StallDuration: time.Millisecond,
	}, rand.NewSource(3))
	if err != nil {
		t.Fatalf("NewFaultFetcher: %v", err)
	}

	_, err = fetcher.Fetch(context.Background(), "device-0", "fw-1", "sum", nil)
	if !errors.Is(err, ErrStall) {
		t.Fatalf("Fetch() error = %v, want the injected stall", err)
	}
	if files := dirFiles(t, dir); len(files) != 0 {
		t.Errorf("download directory holds %v, want no partial file after a stalled attempt", files)
	}
}

func TestRetryableFetchError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "transfer fault", err: ErrTransfer, want: true},
		{name: "wrapped transfer fault", err: fmt.Errorf("fetch fw-1: %w", ErrTransfer), want: true},
		{name: "stall fault", err: ErrStall, want: true},
		{name: "wrapped stall fault", err: fmt.Errorf("fetch fw-1: %w", ErrStall), want: true},
		{name: "checksum mismatch", err: ErrChecksumMismatch, want: false},
		{name: "wrapped checksum mismatch",
			err: fmt.Errorf("fetch fw-1: %w", ErrChecksumMismatch), want: false},
		{name: "truncated transfer", err: ErrTruncatedTransfer, want: false},
		{name: "wrapped truncated transfer",
			err: fmt.Errorf("fetch fw-1: %w", ErrTruncatedTransfer), want: false},
		{name: "context ended", err: context.Canceled, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: false},
		{name: "unknown failure", err: errors.New("unknown failure"), want: false},
		{name: "no failure", err: nil, want: false},
	}

	got := make(map[string]bool, len(tests))
	want := make(map[string]bool, len(tests))
	for _, tc := range tests {
		got[tc.name] = retryableFetchError(tc.err)
		want[tc.name] = tc.want
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("classification mismatch (-want +got):\n%s", diff)
	}
}
