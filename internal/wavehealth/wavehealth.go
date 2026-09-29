package wavehealth

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound reports that the requested rollout or wave does not exist, or that the wave does
// not belong to the rollout it was requested under. It is the only expected failure of an
// evaluation; callers map it onto NOT_FOUND and every other error onto INTERNAL.
var ErrNotFound = errors.New("rollout or wave not found")

// Settings is the configured gating policy one evaluation applies. It is carried on the result so
// a caller can see the thresholds a verdict was computed against instead of inferring them from
// whichever configuration happens to be deployed.
type Settings struct {
	// HealthWindow is the width of the sliding window a wave's health is measured over.
	HealthWindow time.Duration
	// SampleHealthThreshold is the health score at or above which a sample counts as a success.
	SampleHealthThreshold float64
	// MinSuccessRatio is the success ratio at or above which a decided window is healthy.
	MinSuccessRatio float64
	// MinSamples is the sample count a window must hold before its verdict is decided.
	MinSamples int
}

// Verdict is the closed set of outcomes one evaluation reports. The zero value is undecided, so a
// result that was never computed can never read as healthy.
type Verdict int

const (
	// VerdictUndecided means the window holds fewer samples than the configured minimum, so the
	// ratio is not promotion evidence. It is a verdict, not a failure: a gate should wait.
	VerdictUndecided Verdict = iota
	// VerdictHealthy means the window holds enough samples and met the minimum success ratio.
	VerdictHealthy
	// VerdictUnhealthy means the window holds enough samples and fell below it.
	VerdictUnhealthy
)

// String returns the verdict's lowercase name, for logs.
func (v Verdict) String() string {
	switch v {
	case VerdictHealthy:
		return "healthy"
	case VerdictUnhealthy:
		return "unhealthy"
	case VerdictUndecided:
		return "undecided"
	default:
		return fmt.Sprintf("verdict(%d)", int(v))
	}
}

// Query names the wave to evaluate. A wave is only meaningful inside its rollout, so both ids are
// required and a wave id alone is never resolved.
type Query struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string
	// WaveID is the wave of that rollout to evaluate.
	WaveID string
}

// Wave is what a query resolved to: the devices the wave dispatched to and the time its health
// window opened. Both facts are recorded on the wave document rather than re-derived, so the
// denominator of the ratio cannot move under a re-resolution of the target group.
type Wave struct {
	// DeviceIDs are the devices the wave targeted; it may be empty.
	DeviceIDs []string
	// StartedAt is when the wave started — the earliest time its health window may read from.
	StartedAt time.Time
}

// SampleWindow is one sample-counting request: which devices to count, over which period, and what
// health score counts as a success.
type SampleWindow struct {
	// DeviceIDs are the devices whose samples are counted.
	DeviceIDs []string
	// From is the inclusive start of the period.
	From time.Time
	// To is the inclusive end of the period.
	To time.Time
	// SuccessAtLeast is the health score at or above which a sample counts as a success.
	SuccessAtLeast float64
}

// WaveSource resolves a rollout and wave into the wave's membership and start time. The
// Mongo-backed implementation is built by NewWaveSource; tests hand-write a small fake.
type WaveSource interface {
	// Wave returns the wave the query names. It reports ErrNotFound when the rollout is
	// unknown, when the wave is unknown, and when the wave belongs to a different rollout.
	Wave(ctx context.Context, rolloutID, waveID string) (Wave, error)
}

// SampleStore counts a device set's heartbeat samples over a window. Both counts come from one
// call so the ratio's numerator and denominator describe the same read: two queries could
// disagree under concurrent writes.
type SampleStore interface {
	// CountSamples returns how many samples the window's devices emitted inside the period and
	// how many of them reported a health score at or above the success threshold. Both counts
	// are zero for an empty device set, which the store answers without reading anything.
	CountSamples(ctx context.Context, window SampleWindow) (total, successful int64, err error)
}

// Result is one wave's evaluated health: the measurement, the evidence behind it, the effective
// window it was read from, and the policy it was judged against.
type Result struct {
	// RolloutID is the rollout that was evaluated, echoed for callers with queries in flight.
	RolloutID string
	// WaveID is the wave that was evaluated, echoed likewise.
	WaveID string
	// SuccessRatio is the share of samples that succeeded, in [0, 1]; it is zero for an empty
	// window, where it carries no evidence either way.
	SuccessRatio float64
	// SampleSize is the number of heartbeat samples the ratio was computed over.
	SampleSize int64
	// Verdict is the decision the ratio and sample size produced.
	Verdict Verdict
	// WindowStart is the start of the effective window: the configured health window ending at
	// the evaluation, never earlier than the wave's start. It is after WindowEnd for a wave that
	// has not started, whose window is empty.
	WindowStart time.Time
	// WindowEnd is the end of the effective window, at the moment the evaluation ran.
	WindowEnd time.Time
	// Settings is the policy the verdict was computed against.
	Settings Settings
}

// Aggregator evaluates wave health from recorded membership and stored heartbeat samples.
type Aggregator struct {
	waves    WaveSource
	samples  SampleStore
	settings Settings
}

// New returns an aggregator evaluating waves from waves and counting samples through samples,
// applying settings to every evaluation.
func New(waves WaveSource, samples SampleStore, settings Settings) *Aggregator {
	return &Aggregator{waves: waves, samples: samples, settings: settings}
}

// Evaluate measures the health of the wave query names over the effective window ending at at: the
// configured health window clipped so it never reaches back before the wave started. It reports
// the success ratio, the sample size it rests on, the window it read, and the verdict — undecided
// while the window holds fewer samples than the configured minimum, and otherwise healthy exactly
// when the ratio is at or above the minimum success ratio.
//
// An empty window is a successful result rather than a failure: a wave that targets nobody, has
// not started, or has not reported is undecided with sample size zero. The only expected failure is
// ErrNotFound, which the wave source reports for an unknown rollout or wave.
func (a *Aggregator) Evaluate(ctx context.Context, query Query, at time.Time) (Result, error) {
	wave, err := a.waves.Wave(ctx, query.RolloutID, query.WaveID)
	if err != nil {
		return Result{}, fmt.Errorf("resolve wave %s of rollout %s: %w", query.WaveID, query.RolloutID, err)
	}

	start := at.Add(-a.settings.HealthWindow)
	if wave.StartedAt.After(start) {
		start = wave.StartedAt
	}
	result := Result{
		RolloutID:   query.RolloutID,
		WaveID:      query.WaveID,
		Verdict:     VerdictUndecided,
		WindowStart: start,
		WindowEnd:   at,
		Settings:    a.settings,
	}

	// A wave that targets nobody has an empty window by construction: answering it costs nothing,
	// so skip the round trip that could only return zero.
	if len(wave.DeviceIDs) == 0 {
		return result, nil
	}
	// A wave that has not started has an empty window too: the window opens at the wave's start,
	// which is still ahead of the evaluation, so no sample can fall inside it.
	if start.After(at) {
		return result, nil
	}

	total, successful, err := a.samples.CountSamples(ctx, SampleWindow{
		DeviceIDs:      wave.DeviceIDs,
		From:           start,
		To:             at,
		SuccessAtLeast: a.settings.SampleHealthThreshold,
	})
	if err != nil {
		return Result{}, fmt.Errorf("count samples for wave %s of rollout %s: %w", query.WaveID, query.RolloutID, err)
	}

	result.SampleSize = total
	if total > 0 {
		result.SuccessRatio = float64(successful) / float64(total)
	}
	result.Verdict = verdictFor(result.SuccessRatio, total, a.settings)
	return result, nil
}

// verdictFor applies the decision boundary. Below the minimum sample count the window is not
// evidence enough to decide; at or above it the ratio decides, and the boundary itself — a ratio
// exactly at the minimum success ratio — is healthy.
func verdictFor(ratio float64, sampleSize int64, settings Settings) Verdict {
	if sampleSize < int64(settings.MinSamples) {
		return VerdictUndecided
	}
	if ratio >= settings.MinSuccessRatio {
		return VerdictHealthy
	}
	return VerdictUnhealthy
}
