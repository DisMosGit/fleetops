package agent

import (
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// diffStrings reports the cmp.Diff of two string slices.
func diffStrings(want, got []string) string {
	return cmp.Diff(want, got)
}

// unitSource is a scripted rand.Source that always yields the same unit value, turning the
// Simulation's randomness into a deterministic sequence.
type unitSource struct{ raw int64 }

func (s unitSource) Int63() int64 { return s.raw }

func (s unitSource) Seed(int64) {}

const (
	minInt63 = int64(0)
	// halfInt63 makes Float64 return exactly 0.5: strictly fractional (Go's Float64
	// resamples forever on a source that rounds up to 1.0) and above the episode chance.
	halfInt63 = int64(1) << 62
)

func TestSimulationStatusPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  rand.Source
		want []string
	}{
		{
			name: "always-zero source degrades every third sample",
			src:  unitSource{raw: minInt63},
			// Every healthy sample triggers a two-sample episode: online, degraded, degraded.
			want: []string{
				StatusOnline, StatusDegraded, StatusDegraded,
				StatusOnline, StatusDegraded, StatusDegraded,
			},
		},
		{
			name: "always-half source never degrades",
			src:  unitSource{raw: halfInt63},
			want: []string{
				StatusOnline, StatusOnline, StatusOnline,
				StatusOnline, StatusOnline, StatusOnline,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sim := NewSimulation(tc.src)
			got := make([]string, len(tc.want))
			for i := range got {
				got[i] = sim.Next("device-0").Status
			}
			if diff := diffStrings(tc.want, got); diff != "" {
				t.Errorf("status sequence mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSimulationMetricRanges(t *testing.T) {
	t.Parallel()

	sim := NewSimulation(rand.NewSource(42))
	for range 1000 {
		s := sim.Next("device-0")
		if s.CPU < 0 || s.CPU > 1 {
			t.Errorf("cpu %v out of [0,1]", s.CPU)
		}
		if s.Mem < 0 || s.Mem > 1 {
			t.Errorf("mem %v out of [0,1]", s.Mem)
		}
		switch s.Status {
		case StatusOnline:
			if s.Health < 0.85 || s.Health > 1.0 {
				t.Errorf("online health %v out of [0.85,1.0]", s.Health)
			}
		case StatusDegraded:
			if s.Health < 0.2 || s.Health > 0.5 {
				t.Errorf("degraded health %v out of [0.2,0.5]", s.Health)
			}
		default:
			t.Errorf("unexpected status %q", s.Status)
		}
	}
}

func TestSimulationEpisodesArePerDevice(t *testing.T) {
	t.Parallel()

	sim := NewSimulation(unitSource{raw: minInt63})
	want := map[string][]string{
		"device-0": {StatusOnline, StatusDegraded, StatusDegraded},
		"device-1": {StatusOnline, StatusDegraded, StatusDegraded},
	}
	got := map[string][]string{}
	for range 3 {
		for id := range want {
			got[id] = append(got[id], sim.Next(id).Status)
		}
	}
	for id, wantSeq := range want {
		if diff := diffStrings(wantSeq, got[id]); diff != "" {
			t.Errorf("device %s status sequence mismatch (-want +got):\n%s", id, diff)
		}
	}
}
