package agent

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ErrApplyFailed reports a simulated firmware application that did not take. Its message is
// operator-safe by construction: the update report echoes it as its failure detail.
var ErrApplyFailed = errors.New("firmware apply failed")

// Applier is the deterministic firmware-application stub: it simulates applying a downloaded
// binary — a configurable delay and a configurable success rate — standing in for flashing
// real hardware so rollout behavior can be exercised. Outcomes are drawn from the emulator's
// seeded source: the same seed replays the same sequence of successes and failures across
// runs. It is safe for concurrent use across the fleet's devices.
type Applier struct {
	delay       time.Duration
	successRate float64
	sleep       func(ctx context.Context, d time.Duration) error

	mu  sync.Mutex
	rng *rand.Rand
}

// NewApplier returns an apply stub whose simulated applications take delay and then succeed
// with probability successRate, drawing their outcomes from src.
func NewApplier(delay time.Duration, successRate float64, src rand.Source) (*Applier, error) {
	if delay <= 0 {
		return nil, errors.New("applier delay: must be positive")
	}
	if !(successRate >= 0 && successRate <= 1) {
		return nil, errors.New("applier success rate: must be in [0, 1]")
	}
	if src == nil {
		return nil, errors.New("applier source: required")
	}
	return &Applier{
		delay:       delay,
		successRate: successRate,
		rng:         rand.New(src),
		sleep:       sleepCtx,
	}, nil
}

// Apply simulates applying one downloaded firmware to one device: the configured delay first,
// then success or failure at the configured rate. It returns promptly when ctx is cancelled,
// so shutdown interrupts an in-flight application.
func (a *Applier) Apply(ctx context.Context, deviceID, version string) error {
	if err := a.sleep(ctx, a.delay); err != nil {
		return fmt.Errorf("apply firmware %s to %s: %w", version, deviceID, err)
	}
	a.mu.Lock()
	applied := a.rng.Float64() < a.successRate
	a.mu.Unlock()
	if !applied {
		return fmt.Errorf("apply firmware %s to %s: %w", version, deviceID, ErrApplyFailed)
	}
	return nil
}
