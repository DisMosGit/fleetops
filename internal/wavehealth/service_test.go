package wavehealth

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
)

// newService returns the adapter under test over the given doubles, logging to nowhere so a test
// asserts on the response rather than on output.
func newService(waves *fakeWaves, samples *fakeSamples, settings Settings) *Service {
	return NewService(New(waves, samples, settings), slog.New(slog.DiscardHandler))
}

// request is a well-formed query for the wave every test evaluates.
func request() *rolloutv1.GetWaveHealthRequest {
	return &rolloutv1.GetWaveHealthRequest{RolloutId: testRollout, WaveId: testWave}
}

// TestGetWaveHealthStatusMapping pins the failure contract: a malformed request is rejected before
// any state is read, a miss is NOT_FOUND whichever way it missed, and a storage fault is INTERNAL
// with a message that carries no internals.
func TestGetWaveHealthStatusMapping(t *testing.T) {
	t.Parallel()

	storageFault := errors.New(
		"aggregate telemetry samples: connection refused: server selection error for " +
			"collection telemetry on cluster fleetops-shard-0",
	)
	wave := Wave{DeviceIDs: []string{"dev-a"}, StartedAt: time.Now().Add(-time.Hour)}

	tests := []struct {
		name        string
		req         *rolloutv1.GetWaveHealthRequest
		waves       *fakeWaves
		samples     *fakeSamples
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name:        "empty rollout id is rejected",
			req:         &rolloutv1.GetWaveHealthRequest{WaveId: testWave},
			waves:       &fakeWaves{wave: wave},
			wantCode:    codes.InvalidArgument,
			wantMessage: "rollout_id and wave_id are required",
		},
		{
			name:        "empty wave id is rejected",
			req:         &rolloutv1.GetWaveHealthRequest{RolloutId: testRollout},
			waves:       &fakeWaves{wave: wave},
			wantCode:    codes.InvalidArgument,
			wantMessage: "rollout_id and wave_id are required",
		},
		{
			name:        "unknown rollout is not found",
			req:         request(),
			waves:       &fakeWaves{err: ErrNotFound},
			wantCode:    codes.NotFound,
			wantMessage: "rollout or wave not found",
		},
		{
			name: "wave of another rollout is not found",
			req:  request(),
			// The store's miss for a wave that belongs to a different rollout wraps the sentinel
			// exactly as the Mongo-backed source does.
			waves:       &fakeWaves{err: wrappedNotFound{}},
			wantCode:    codes.NotFound,
			wantMessage: "rollout or wave not found",
		},
		{
			name:        "storage failure is internal",
			req:         request(),
			waves:       &fakeWaves{wave: wave},
			samples:     &fakeSamples{err: storageFault},
			wantCode:    codes.Internal,
			wantMessage: "wave health evaluation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.samples == nil {
				tc.samples = &fakeSamples{}
			}
			resp, err := newService(tc.waves, tc.samples, testSettings()).
				GetWaveHealth(context.Background(), tc.req)

			if resp != nil {
				t.Errorf("GetWaveHealth() response = %v, want nil on failure", resp)
			}
			got, ok := status.FromError(err)
			if !ok {
				t.Fatalf("GetWaveHealth() error = %v, want a gRPC status", err)
			}
			if got.Code() != tc.wantCode {
				t.Errorf("GetWaveHealth() code = %v, want %v (message %q)", got.Code(), tc.wantCode, got.Message())
			}
			if got.Message() != tc.wantMessage {
				t.Errorf("GetWaveHealth() message = %q, want %q", got.Message(), tc.wantMessage)
			}
		})
	}
}

// TestGetWaveHealthInvalidRequestRunsNoQuery pins that validation happens before any state is read:
// a malformed request must not reach the wave source or the sample store.
func TestGetWaveHealthInvalidRequestRunsNoQuery(t *testing.T) {
	t.Parallel()

	for _, req := range []*rolloutv1.GetWaveHealthRequest{
		{WaveId: testWave},
		{RolloutId: testRollout},
		{},
	} {
		waves := &fakeWaves{wave: Wave{DeviceIDs: []string{"dev-a"}, StartedAt: time.Now().Add(-time.Hour)}}
		samples := &fakeSamples{}
		if _, err := newService(waves, samples, testSettings()).GetWaveHealth(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("GetWaveHealth(%v) error = %v, want INVALID_ARGUMENT", req, err)
		}
		if got := waves.lookups(); len(got) != 0 {
			t.Errorf("GetWaveHealth(%v) resolved %d waves, want no lookup for an invalid request", req, len(got))
		}
		if got := samples.queried(); len(got) != 0 {
			t.Errorf("GetWaveHealth(%v) ran %d sample queries, want none for an invalid request", req, len(got))
		}
	}
}

// TestGetWaveHealthLeaksNothing pins the operator-safe boundary: an internal failure's message
// names neither the driver error nor a collection, and the detail stays in the log.
func TestGetWaveHealthLeaksNothing(t *testing.T) {
	t.Parallel()

	waves := &fakeWaves{wave: Wave{DeviceIDs: []string{"dev-a"}, StartedAt: time.Now().Add(-time.Hour)}}
	samples := &fakeSamples{err: errors.New(
		"aggregate telemetry samples: connection refused: server selection error for collection waves",
	)}

	_, err := newService(waves, samples, testSettings()).GetWaveHealth(context.Background(), request())
	if status.Code(err) != codes.Internal {
		t.Fatalf("GetWaveHealth() error = %v, want INTERNAL", err)
	}
	for _, leak := range []string{"telemetry", "waves", "collection", "connection", "server selection", "aggregate"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("GetWaveHealth() error = %q, want it to leak no %q", err, leak)
		}
	}
}

// TestGetWaveHealthServesTheEvaluation pins what a served result carries: the echoed ids, the
// measurement, the effective window, the thresholds the verdict used, and a verdict that is never
// the unset one.
func TestGetWaveHealthServesTheEvaluation(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	// The samples sit relative to the wall clock because the window ends when the call is served.
	now := time.Now()
	clippedStart := now.Add(-2 * time.Minute)

	tests := []struct {
		name        string
		wave        Wave
		samples     []sample
		wantRatio   float64
		wantSize    int64
		wantVerdict rolloutv1.WaveHealthVerdict
		// wantStart is the effective window start when the window is clipped at the wave start;
		// zero means the window was expected to run its full configured width.
		wantStart   time.Time
		wantQueries int
	}{
		{
			name:        "a decided wave is served healthy",
			wave:        Wave{DeviceIDs: devices(10), StartedAt: now.Add(-time.Hour)},
			samples:     mix("dev-a", now, 95, 5),
			wantRatio:   0.95,
			wantSize:    100,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY,
			wantQueries: 1,
		},
		{
			name:        "a degraded wave is served unhealthy",
			wave:        Wave{DeviceIDs: devices(10), StartedAt: now.Add(-time.Hour)},
			samples:     mix("dev-a", now, 80, 20),
			wantRatio:   0.8,
			wantSize:    100,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNHEALTHY,
			wantQueries: 1,
		},
		{
			name:        "an empty window is served undecided",
			wave:        Wave{DeviceIDs: devices(10), StartedAt: now.Add(-time.Hour)},
			wantRatio:   0,
			wantSize:    0,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED,
			wantQueries: 1,
		},
		{
			name:        "a wave without targets is served undecided without a query",
			wave:        Wave{DeviceIDs: nil, StartedAt: now.Add(-time.Hour)},
			wantRatio:   0,
			wantSize:    0,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED,
			wantQueries: 0,
		},
		{
			name:        "too few samples are served undecided",
			wave:        Wave{DeviceIDs: devices(10), StartedAt: now.Add(-time.Hour)},
			samples:     mix("dev-a", now, int(settings.MinSamples)-1, 0),
			wantRatio:   1,
			wantSize:    int64(settings.MinSamples) - 1,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED,
			wantQueries: 1,
		},
		{
			name:        "a young wave has its window clipped at its start",
			wave:        Wave{DeviceIDs: devices(10), StartedAt: clippedStart},
			samples:     mix("dev-a", now, 20, 0),
			wantRatio:   1,
			wantSize:    20,
			wantVerdict: rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY,
			wantStart:   clippedStart,
			wantQueries: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			samples := &fakeSamples{samples: tc.samples}
			resp, err := newService(&fakeWaves{wave: tc.wave}, samples, settings).
				GetWaveHealth(context.Background(), request())
			if err != nil {
				t.Fatalf("GetWaveHealth() error = %v", err)
			}

			// Both ids come back, so a client with several queries in flight can match results.
			if resp.GetRolloutId() != testRollout || resp.GetWaveId() != testWave {
				t.Errorf("GetWaveHealth() ids = %q/%q, want %q/%q",
					resp.GetRolloutId(), resp.GetWaveId(), testRollout, testWave)
			}
			if resp.GetSuccessRatio() != tc.wantRatio || resp.GetSampleSize() != tc.wantSize {
				t.Errorf("GetWaveHealth() = ratio %v over %d samples, want %v over %d",
					resp.GetSuccessRatio(), resp.GetSampleSize(), tc.wantRatio, tc.wantSize)
			}
			if resp.GetVerdict() != tc.wantVerdict {
				t.Errorf("GetWaveHealth() verdict = %v, want %v", resp.GetVerdict(), tc.wantVerdict)
			}
			// The verdict set is closed: a served response never carries the unset verdict.
			if resp.GetVerdict() == rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNSPECIFIED {
				t.Error("GetWaveHealth() verdict = UNSPECIFIED, want a reported verdict")
			}

			// The response carries the values the verdict was computed against.
			if resp.GetSampleHealthThreshold() != settings.SampleHealthThreshold ||
				resp.GetMinSuccessRatio() != settings.MinSuccessRatio ||
				resp.GetMinSamples() != int64(settings.MinSamples) {
				t.Errorf("GetWaveHealth() thresholds = %v/%v/%d, want %v/%v/%d",
					resp.GetSampleHealthThreshold(), resp.GetMinSuccessRatio(), resp.GetMinSamples(),
					settings.SampleHealthThreshold, settings.MinSuccessRatio, settings.MinSamples)
			}

			end := resp.GetWindowEnd().AsTime()
			start := resp.GetWindowStart().AsTime()
			if delta := time.Since(end); delta < 0 || delta > time.Minute {
				t.Errorf("GetWaveHealth() window end = %v, want the moment the call was served", end)
			}
			if tc.wantStart.IsZero() {
				if got := end.Sub(start); got != settings.HealthWindow {
					t.Errorf("GetWaveHealth() window = %v wide, want the configured %v", got, settings.HealthWindow)
				}
			} else if !start.Equal(tc.wantStart) {
				t.Errorf("GetWaveHealth() window start = %v, want the wave start %v", start, tc.wantStart)
			}

			if got := len(samples.queried()); got != tc.wantQueries {
				t.Errorf("CountSamples() called %d times, want %d", got, tc.wantQueries)
			}
		})
	}
}

// wrappedNotFound is the not-found miss as the Mongo-backed store reports it: a sentinel wrapped
// with the ids the lookup was about, so the adapter's errors.Is check is exercised rather than a
// bare sentinel comparison.
type wrappedNotFound struct{}

// Error implements error.
func (wrappedNotFound) Error() string {
	return "wave " + testWave + " of rollout " + testRollout + ": " + ErrNotFound.Error()
}

// Unwrap returns the package sentinel.
func (wrappedNotFound) Unwrap() error { return ErrNotFound }
