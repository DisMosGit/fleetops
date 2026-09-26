package agent

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func TestNewApplierValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		delay       time.Duration
		successRate float64
		src         rand.Source
	}{
		{name: "no delay", delay: 0, successRate: 1, src: rand.NewSource(1)},
		{name: "negative delay", delay: -time.Second, successRate: 1, src: rand.NewSource(1)},
		{name: "rate above one", delay: time.Second, successRate: 1.5, src: rand.NewSource(1)},
		{name: "negative rate", delay: time.Second, successRate: -0.1, src: rand.NewSource(1)},
		{name: "no source", delay: time.Second, successRate: 1, src: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewApplier(tc.delay, tc.successRate, tc.src); err == nil {
				t.Fatal("NewApplier() error = nil, want a validation error")
			}
		})
	}
}

func TestApplierDeterministicOutcomes(t *testing.T) {
	t.Parallel()

	// The same seed replays the same pass/fail sequence: rollout experiments are
	// reproducible runs, not draws from a fresh entropy source.
	const rate = 0.5
	run := func(seed int64) []error {
		applier, err := NewApplier(time.Millisecond, rate, rand.NewSource(seed))
		if err != nil {
			t.Fatalf("NewApplier: %v", err)
		}
		var outcomes []error
		for range 20 {
			outcomes = append(outcomes, applier.Apply(context.Background(), "dev-1", "2.0.0"))
		}
		return outcomes
	}

	first, second := run(42), run(42)
	sawSuccess, sawFailure := false, false
	for i := range first {
		if (first[i] == nil) != (second[i] == nil) {
			t.Fatalf("outcome %d differs across runs with the same seed: %v vs %v", i, first[i], second[i])
		}
		if first[i] == nil {
			sawSuccess = true
		} else {
			sawFailure = true
		}
	}
	if !sawSuccess || !sawFailure {
		t.Errorf("outcomes = %v successes=%v failures=%v, want a mixed sequence at rate %v",
			len(first), sawSuccess, sawFailure, rate)
	}
}

func TestApplierRateBounds(t *testing.T) {
	t.Parallel()

	t.Run("rate one always succeeds", func(t *testing.T) {
		t.Parallel()
		applier, err := NewApplier(time.Millisecond, 1.0, rand.NewSource(7))
		if err != nil {
			t.Fatalf("NewApplier: %v", err)
		}
		for range 10 {
			if err := applier.Apply(context.Background(), "dev-1", "2.0.0"); err != nil {
				t.Fatalf("Apply() error = %v, want success at rate 1.0", err)
			}
		}
	})

	t.Run("rate zero always fails", func(t *testing.T) {
		t.Parallel()
		applier, err := NewApplier(time.Millisecond, 0, rand.NewSource(7))
		if err != nil {
			t.Fatalf("NewApplier: %v", err)
		}
		for range 10 {
			if err := applier.Apply(context.Background(), "dev-1", "2.0.0"); !errors.Is(err, ErrApplyFailed) {
				t.Fatalf("Apply() error = %v, want ErrApplyFailed at rate 0", err)
			}
		}
	})
}

func TestApplierInterruptsPromptly(t *testing.T) {
	t.Parallel()

	applier, err := NewApplier(time.Hour, 1.0, rand.NewSource(7))
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Apply(ctx, "dev-1", "2.0.0") }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply() error = %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Apply() did not stop on cancellation")
	}
}
