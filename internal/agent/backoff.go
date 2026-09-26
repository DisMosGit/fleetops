package agent

import (
	"context"
	"math/rand"
	"time"
)

// backoff computes reconnect delays: exponential growth from an initial delay to a fixed
// maximum, with jitter drawn from an injected rand.Source so tests can assert the sequence
// deterministically. It is owned by the reconnect loop and needs no locking.
type backoff struct {
	initial time.Duration
	max     time.Duration
	jitter  float64 // fraction of the delay the jitter may add or subtract
	r       *rand.Rand
	next    time.Duration
}

// newBackoff returns a backoff sequence starting at initial and doubling, with jitter as a
// fraction in [0, 1], until max.
func newBackoff(initial, max time.Duration, jitter float64, r *rand.Rand) *backoff {
	return &backoff{initial: initial, max: max, jitter: jitter, r: r, next: initial}
}

// Delay returns the next reconnect delay and advances the sequence. The result never exceeds
// max, even with jitter applied.
func (b *backoff) Delay() time.Duration {
	d := b.next
	if b.jitter > 0 {
		delta := (b.r.Float64()*2 - 1) * b.jitter * float64(d)
		d += time.Duration(delta)
	}
	if d > b.max {
		d = b.max
	}
	if b.next *= 2; b.next > b.max {
		b.next = b.max
	}
	return d
}

// Reset restarts the sequence at the initial delay, after a successful connection.
func (b *backoff) Reset() {
	b.next = b.initial
}

// sleepCtx waits for d or ctx cancellation, whichever comes first. It is the injectable
// reconnect wait: tests substitute a recorder to observe the delay sequence instantly.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
