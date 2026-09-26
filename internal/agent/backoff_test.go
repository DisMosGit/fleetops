package agent

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestBackoffDelaySequence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		jitter float64
		want   []time.Duration
	}{
		{
			name:   "doubling without jitter",
			jitter: 0,
			want: []time.Duration{
				time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackoff(time.Second, 5*time.Second, tc.jitter, rand.New(rand.NewSource(1)))
			got := make([]time.Duration, len(tc.want))
			for i := range got {
				got[i] = b.Delay()
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("delay sequence mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffJitterStaysBounded(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Second, 5*time.Second, 0.2, rand.New(rand.NewSource(7)))
	for range 20 {
		d := b.Delay()
		if d > 5*time.Second {
			t.Errorf("delay %s exceeds the maximum", d)
		}
		if d <= 0 {
			t.Errorf("delay %s must be positive", d)
		}
	}
}

func TestBackoffReset(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Second, 5*time.Second, 0, rand.New(rand.NewSource(1)))
	_ = b.Delay()
	_ = b.Delay()
	b.Reset()
	if got, want := b.Delay(), time.Second; got != want {
		t.Errorf("delay after reset = %s, want %s", got, want)
	}
}
