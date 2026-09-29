package wavehealth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	// testRollout and testWave are the ids every test evaluates.
	testRollout = "roll-1"
	testWave    = "wave-1"
	// healthyHealth and degradedHealth are health scores inside the emulator's healthy
	// (0.85–1.0) and degraded (0.2–0.5) bands.
	healthyHealth  = 0.95
	degradedHealth = 0.3
	// emulatorPeriod is the agent emulator's heartbeat period, so a fake timeline emits samples
	// at the cadence the real fleet does.
	emulatorPeriod = 5 * time.Second
)

// sample is one stored heartbeat the fake store counts: which device emitted it, when, and what
// health it reported.
type sample struct {
	deviceID string
	at       time.Time
	health   float64
}

// fakeWaves is a hand-written WaveSource double replaying one scripted wave or failure, with the
// lookups it was asked for recorded.
type fakeWaves struct {
	wave Wave
	err  error

	mu    sync.Mutex
	asked []Query
}

// Wave records the lookup and returns the scripted wave or failure.
func (f *fakeWaves) Wave(_ context.Context, rolloutID, waveID string) (Wave, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, Query{RolloutID: rolloutID, WaveID: waveID})
	if f.err != nil {
		return Wave{}, f.err
	}
	return f.wave, nil
}

// lookups returns the queries the source was asked to resolve.
func (f *fakeWaves) lookups() []Query {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Query(nil), f.asked...)
}

// fakeSamples is a hand-written SampleStore double over a sample timeline. It counts the way the
// window it is handed describes — device membership, measurement time, health score — so a test
// can prove that aging out and window clipping change what the evaluation sees, and it records
// every window so the query's scope can be asserted directly.
type fakeSamples struct {
	samples []sample
	err     error

	mu      sync.Mutex
	windows []SampleWindow
}

// CountSamples counts the timeline inside the window, or returns the scripted failure.
func (f *fakeSamples) CountSamples(_ context.Context, window SampleWindow) (int64, int64, error) {
	f.mu.Lock()
	f.windows = append(f.windows, window)
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return 0, 0, err
	}

	members := make(map[string]bool, len(window.DeviceIDs))
	for _, id := range window.DeviceIDs {
		members[id] = true
	}
	var total, successful int64
	for _, s := range f.samples {
		if !members[s.deviceID] || s.at.Before(window.From) || s.at.After(window.To) {
			continue
		}
		total++
		if s.health >= window.SuccessAtLeast {
			successful++
		}
	}
	return total, successful, nil
}

// queried returns the windows the store was asked to count.
func (f *fakeSamples) queried() []SampleWindow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]SampleWindow(nil), f.windows...)
}

// testSettings is the gating policy the tests evaluate against: a five-minute window, a 0.6
// per-sample threshold, a 0.95 minimum success ratio, and a ten-sample minimum.
func testSettings() Settings {
	return Settings{
		HealthWindow:          5 * time.Minute,
		SampleHealthThreshold: 0.6,
		MinSuccessRatio:       0.95,
		MinSamples:            10,
	}
}

// testAt is the fixed moment every test evaluates at, so no test depends on the wall clock.
var testAt = time.Unix(1737500000, 0)

// devices returns n device identities.
func devices(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, "dev-"+string(rune('a'+i)))
	}
	return out
}

// mix returns one device's samples ending at end and walking backwards a second at a time:
// healthy successful samples first, then degraded failing ones. Every sample lands inside a
// window ending at end.
func mix(deviceID string, end time.Time, healthy, degraded int) []sample {
	out := make([]sample, 0, healthy+degraded)
	for i := 0; i < healthy+degraded; i++ {
		health := healthyHealth
		if i >= healthy {
			health = degradedHealth
		}
		out = append(out, sample{deviceID: deviceID, at: end.Add(-time.Duration(i) * time.Second), health: health})
	}
	return out
}

// cadence returns one device's samples emitted every emulator period from start to end inclusive,
// each reporting health.
func cadence(deviceID string, start, end time.Time, health float64) []sample {
	out := make([]sample, 0, int(end.Sub(start)/emulatorPeriod)+1)
	for at := start; !at.After(end); at = at.Add(emulatorPeriod) {
		out = append(out, sample{deviceID: deviceID, at: at, health: health})
	}
	return out
}

// TestEvaluate covers the outcome of one evaluation: the ratio and sample size it reports, the
// verdict those produce, and the effective window it read them from.
func TestEvaluate(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	tenDevices := devices(10)
	// startedLongAgo keeps the window unclipped: the wave is far older than the health window.
	startedLongAgo := testAt.Add(-time.Hour)
	// startedTwoMinutesAgo clips the window: the wave is younger than the health window.
	startedTwoMinutesAgo := testAt.Add(-2 * time.Minute)

	tests := []struct {
		name         string
		wave         Wave
		samples      []sample
		wantRatio    float64
		wantSize     int64
		wantVerdict  Verdict
		wantWindowAt time.Time // effective window start; zero means unclipped testAt-5m
		wantNoQuery  bool      // the evaluation must not reach storage at all
	}{
		{
			name: "ratio and sample size over the wave's devices",
			wave: Wave{DeviceIDs: tenDevices, StartedAt: startedLongAgo},
			// Ten targeted devices, one of which reported 95 successful and 5 failing samples.
			samples:     mix("dev-a", testAt, 95, 5),
			wantRatio:   0.95,
			wantSize:    100,
			wantVerdict: VerdictHealthy,
		},
		{
			name: "only the wave's target devices count",
			wave: Wave{DeviceIDs: tenDevices, StartedAt: startedLongAgo},
			samples: append(
				mix("dev-a", testAt, 95, 5),
				cadence("dev-outsider", testAt.Add(-4*time.Minute), testAt, degradedHealth)...,
			),
			wantRatio:   0.95,
			wantSize:    100,
			wantVerdict: VerdictHealthy,
		},
		{
			name: "samples older than the window age out",
			wave: Wave{DeviceIDs: tenDevices, StartedAt: startedLongAgo},
			samples: append(
				// A failing burst six minutes back, already outside the five-minute window.
				cadence("dev-a", testAt.Add(-6*time.Minute), testAt.Add(-5*time.Minute-time.Second), degradedHealth),
				cadence("dev-a", testAt.Add(-4*time.Minute), testAt, healthyHealth)...,
			),
			wantRatio:   1,
			wantSize:    49,
			wantVerdict: VerdictHealthy,
		},
		{
			name: "the window never reaches back before the wave started",
			wave: Wave{DeviceIDs: tenDevices, StartedAt: startedTwoMinutesAgo},
			samples: append(
				// Heartbeats from before the wave started: the same devices on the old firmware.
				cadence("dev-a", testAt.Add(-4*time.Minute), startedTwoMinutesAgo.Add(-emulatorPeriod), healthyHealth),
				cadence("dev-a", startedTwoMinutesAgo, testAt, degradedHealth)...,
			),
			wantRatio:    0,
			wantSize:     25,
			wantVerdict:  VerdictUnhealthy,
			wantWindowAt: startedTwoMinutesAgo,
		},
		{
			name:        "no target devices",
			wave:        Wave{DeviceIDs: nil, StartedAt: startedLongAgo},
			wantRatio:   0,
			wantSize:    0,
			wantVerdict: VerdictUndecided,
			wantNoQuery: true,
		},
		{
			name:         "wave has not started",
			wave:         Wave{DeviceIDs: tenDevices, StartedAt: testAt.Add(time.Minute)},
			wantRatio:    0,
			wantSize:     0,
			wantVerdict:  VerdictUndecided,
			wantWindowAt: testAt.Add(time.Minute),
			wantNoQuery:  true,
		},
		{
			name:        "no samples in the window",
			wave:        Wave{DeviceIDs: tenDevices, StartedAt: startedLongAgo},
			wantRatio:   0,
			wantSize:    0,
			wantVerdict: VerdictUndecided,
		},
		{
			name:        "fewer samples than the minimum",
			wave:        Wave{DeviceIDs: tenDevices, StartedAt: startedLongAgo},
			samples:     mix("dev-a", testAt, 5, 0),
			wantRatio:   1,
			wantSize:    5,
			wantVerdict: VerdictUndecided,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			waves := &fakeWaves{wave: tc.wave}
			samples := &fakeSamples{samples: tc.samples}
			got, err := New(waves, samples, settings).Evaluate(context.Background(), Query{
				RolloutID: testRollout,
				WaveID:    testWave,
			}, testAt)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			wantStart := tc.wantWindowAt
			if wantStart.IsZero() {
				wantStart = testAt.Add(-settings.HealthWindow)
			}
			want := Result{
				RolloutID:    testRollout,
				WaveID:       testWave,
				SuccessRatio: tc.wantRatio,
				SampleSize:   tc.wantSize,
				Verdict:      tc.wantVerdict,
				WindowStart:  wantStart,
				WindowEnd:    testAt,
				Settings:     settings,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Evaluate() mismatch (-want +got):\n%s", diff)
			}

			if tc.wantNoQuery {
				if got := samples.queried(); len(got) != 0 {
					t.Errorf("CountSamples() called %d times for an empty window, want no query", len(got))
				}
			} else if got := samples.queried(); len(got) != 1 {
				t.Errorf("CountSamples() called %d times, want exactly one", len(got))
			}

			// Both ids are echoed, and a wave id alone is never resolved: the lookup carries the
			// rollout it was asked about.
			wantLookup := []Query{{RolloutID: testRollout, WaveID: testWave}}
			if diff := cmp.Diff(wantLookup, waves.lookups()); diff != "" {
				t.Errorf("Wave() lookups mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestEvaluateQueriesTheConfiguredWindow pins the scope handed to storage: exactly the wave's
// membership, the effective window, and the configured per-sample health threshold.
func TestEvaluateQueriesTheConfiguredWindow(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	membership := []string{"dev-a", "dev-b"}

	tests := []struct {
		name       string
		startedAt  time.Time
		wantWindow SampleWindow
	}{
		{
			name:      "full window when the wave is older than it",
			startedAt: testAt.Add(-time.Hour),
			wantWindow: SampleWindow{
				DeviceIDs:      membership,
				From:           testAt.Add(-settings.HealthWindow),
				To:             testAt,
				SuccessAtLeast: settings.SampleHealthThreshold,
			},
		},
		{
			name:      "clipped at the wave start when the wave is younger",
			startedAt: testAt.Add(-90 * time.Second),
			wantWindow: SampleWindow{
				DeviceIDs:      membership,
				From:           testAt.Add(-90 * time.Second),
				To:             testAt,
				SuccessAtLeast: settings.SampleHealthThreshold,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			samples := &fakeSamples{}
			waves := &fakeWaves{wave: Wave{DeviceIDs: membership, StartedAt: tc.startedAt}}
			if _, err := New(waves, samples, settings).Evaluate(
				context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, testAt,
			); err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			queried := samples.queried()
			if len(queried) != 1 {
				t.Fatalf("CountSamples() called %d times, want exactly one", len(queried))
			}
			if diff := cmp.Diff(tc.wantWindow, queried[0]); diff != "" {
				t.Errorf("SampleWindow mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTransientFailuresDoNotReadAsRegression walks one wave's timeline through a short dip, a
// sustained degradation, and the recovery that follows once the degraded samples age out. Because
// the ratio counts samples, a wave's health degrades in proportion to how many of them are
// degraded — a brief dip must not by itself read as a regression.
func TestTransientFailuresDoNotReadAsRegression(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	wave := Wave{DeviceIDs: []string{"dev-a"}, StartedAt: testAt.Add(-time.Hour)}

	tests := []struct {
		name        string
		samples     []sample
		evaluateAt  time.Time
		wantRatio   float64
		wantSize    int64
		wantVerdict Verdict
	}{
		{
			// A three-sample dip inside an otherwise healthy window: 61 of 64 samples succeed,
			// so the ratio stays above the minimum.
			name: "a brief dip leaves a healthy wave healthy",
			samples: append(
				cadence("dev-a", testAt.Add(-5*time.Minute), testAt, healthyHealth),
				cadence("dev-a", testAt.Add(-5*time.Minute), testAt.Add(-5*time.Minute+10*time.Second), degradedHealth)...,
			),
			evaluateAt:  testAt,
			wantRatio:   61.0 / 64.0,
			wantSize:    64,
			wantVerdict: VerdictHealthy,
		},
		{
			// A one-minute dip: 13 of 74 samples in the window are degraded, below the minimum.
			name: "sustained degradation crosses the boundary",
			samples: append(
				cadence("dev-a", testAt.Add(-5*time.Minute), testAt, healthyHealth),
				cadence("dev-a", testAt.Add(-5*time.Minute), testAt.Add(-4*time.Minute), degradedHealth)...,
			),
			evaluateAt:  testAt,
			wantRatio:   61.0 / 74.0,
			wantSize:    74,
			wantVerdict: VerdictUnhealthy,
		},
		{
			// Ten minutes on, the dip has aged past the window's start: only recovered samples
			// are counted, and the verdict follows the ratio.
			name: "recovery is visible once the dip ages out",
			samples: append(
				cadence("dev-a", testAt.Add(-10*time.Minute), testAt.Add(5*time.Minute), healthyHealth),
				cadence("dev-a", testAt.Add(-10*time.Minute), testAt.Add(-9*time.Minute), degradedHealth)...,
			),
			evaluateAt:  testAt.Add(5 * time.Minute),
			wantRatio:   1,
			wantSize:    61,
			wantVerdict: VerdictHealthy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(&fakeWaves{wave: wave}, &fakeSamples{samples: tc.samples}, settings).
				Evaluate(context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, tc.evaluateAt)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if got.SuccessRatio != tc.wantRatio || got.SampleSize != tc.wantSize {
				t.Errorf("Evaluate() ratio %v over %d samples, want %v over %d",
					got.SuccessRatio, got.SampleSize, tc.wantRatio, tc.wantSize)
			}
			if got.Verdict != tc.wantVerdict {
				t.Errorf("Evaluate() verdict = %s, want %s", got.Verdict, tc.wantVerdict)
			}
		})
	}
}

// TestBoundaryIsInclusive pins the decision boundary: a ratio exactly at the minimum success
// ratio is healthy, and a single further failing sample — still above the sample minimum — flips
// the verdict.
func TestBoundaryIsInclusive(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	settings.MinSuccessRatio = 0.9
	wave := Wave{DeviceIDs: []string{"dev-a"}, StartedAt: testAt.Add(-time.Hour)}
	aggregator := func(samples []sample) *Aggregator {
		return New(&fakeWaves{wave: wave}, &fakeSamples{samples: samples}, settings)
	}

	t.Run("exactly at the boundary is healthy", func(t *testing.T) {
		t.Parallel()

		got, err := aggregator(mix("dev-a", testAt, 9, 1)).Evaluate(
			context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, testAt)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if got.SuccessRatio != 0.9 {
			t.Fatalf("Evaluate() ratio = %v, want exactly the 0.9 minimum", got.SuccessRatio)
		}
		if got.Verdict != VerdictHealthy {
			t.Errorf("Evaluate() verdict = %s, want healthy at the boundary", got.Verdict)
		}
	})

	t.Run("one further failing sample flips the verdict", func(t *testing.T) {
		t.Parallel()

		got, err := aggregator(mix("dev-a", testAt, 9, 2)).Evaluate(
			context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, testAt)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		if got.SuccessRatio >= 0.9 {
			t.Fatalf("Evaluate() ratio = %v, want it below the 0.9 minimum", got.SuccessRatio)
		}
		if got.SampleSize < int64(settings.MinSamples) {
			t.Fatalf("Evaluate() sample size = %d, want at least the %d minimum", got.SampleSize, settings.MinSamples)
		}
		if got.Verdict != VerdictUnhealthy {
			t.Errorf("Evaluate() verdict = %s, want unhealthy below the boundary", got.Verdict)
		}
	})
}

// TestTooFewSamplesCannotBeHealthy pins the evidence gate: however high the ratio, a window below
// the minimum sample count is undecided rather than healthy.
func TestTooFewSamplesCannotBeHealthy(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	wave := Wave{DeviceIDs: []string{"dev-a"}, StartedAt: testAt.Add(-time.Hour)}
	got, err := New(&fakeWaves{wave: wave},
		&fakeSamples{samples: mix("dev-a", testAt, int(settings.MinSamples)-1, 0)}, settings).
		Evaluate(context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, testAt)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if got.SuccessRatio != 1 {
		t.Fatalf("Evaluate() ratio = %v, want a perfect ratio over too few samples", got.SuccessRatio)
	}
	if got.Verdict != VerdictUndecided {
		t.Errorf("Evaluate() verdict = %s, want undecided below the sample minimum", got.Verdict)
	}
}

// TestEvaluateErrors pins the failure contract the gRPC adapter maps onto status codes: an unknown
// wave carries the not-found sentinel, and every other failure stays distinguishable from it so a
// storage fault is never reported to a client as "wave not found".
func TestEvaluateErrors(t *testing.T) {
	t.Parallel()

	transient := errors.New("connection refused")
	tests := []struct {
		name     string
		waves    *fakeWaves
		samples  *fakeSamples
		wantErr  error
		wantIsNF bool
	}{
		{
			name:     "unknown rollout or wave is the not-found sentinel",
			waves:    &fakeWaves{err: ErrNotFound},
			wantErr:  ErrNotFound,
			wantIsNF: true,
		},
		{
			name:    "a wave source failure that is not a miss is not a miss",
			waves:   &fakeWaves{err: transient},
			wantErr: transient,
		},
		{
			name:    "a sample store failure surfaces as itself",
			waves:   &fakeWaves{wave: Wave{DeviceIDs: []string{"dev-a"}, StartedAt: testAt.Add(-time.Hour)}},
			samples: &fakeSamples{err: transient},
			wantErr: transient,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.samples == nil {
				tc.samples = &fakeSamples{}
			}
			got, err := New(tc.waves, tc.samples, testSettings()).Evaluate(
				context.Background(), Query{RolloutID: testRollout, WaveID: testWave}, testAt)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Evaluate() error = %v, want %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrNotFound) != tc.wantIsNF {
				t.Errorf("errors.Is(err, ErrNotFound) = %v, want %v", !tc.wantIsNF, tc.wantIsNF)
			}
			if diff := cmp.Diff(Result{}, got); diff != "" {
				t.Errorf("Evaluate() result on failure mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestVerdictString pins the lowercase names the logs and error messages carry.
func TestVerdictString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verdict Verdict
		want    string
	}{
		{VerdictUndecided, "undecided"},
		{VerdictHealthy, "healthy"},
		{VerdictUnhealthy, "unhealthy"},
		{Verdict(42), "verdict(42)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			if got := tc.verdict.String(); got != tc.want {
				t.Errorf("Verdict(%d).String() = %q, want %q", int(tc.verdict), got, tc.want)
			}
		})
	}
}
