package devices

import (
	"context"
	"log/slog"
	"time"
)

// StaleMarker is the registry surface the sweep needs: flip stale online devices to offline
// and report how many transitioned.
type StaleMarker interface {
	// MarkStale flips online devices last heard from before cutoff to offline.
	MarkStale(ctx context.Context, cutoff time.Time) (int64, error)
}

// Sweeper runs the offline-staleness sweep at a fixed cadence: every pass marks devices
// silent past the threshold offline, and every batch of transitions is reported through
// onTransitions — the metric source. onTransitions must not be nil.
type Sweeper struct {
	marker        StaleMarker
	threshold     time.Duration
	interval      time.Duration
	onTransitions func(int64)
	log           *slog.Logger
}

// NewSweeper returns a sweep marking devices offline after threshold of silence, running
// every interval, and reporting each batch of transitions to onTransitions.
func NewSweeper(
	marker StaleMarker,
	threshold, interval time.Duration,
	onTransitions func(int64),
	log *slog.Logger,
) *Sweeper {
	return &Sweeper{
		marker:        marker,
		threshold:     threshold,
		interval:      interval,
		onTransitions: onTransitions,
		log:           log,
	}
}

// Run sweeps until ctx is cancelled. A failed pass is logged and retried at the next tick —
// one bad pass must not stop detection — and its transitions are not counted.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			transitions, err := s.marker.MarkStale(ctx, time.Now().Add(-s.threshold))
			if err != nil {
				s.log.Error("sweep stale devices", "err", err)
				continue
			}
			if transitions > 0 {
				s.log.Info("devices transitioned to offline", "count", transitions)
				s.onTransitions(transitions)
			}
		}
	}
}
