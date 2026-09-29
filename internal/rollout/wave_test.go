package rollout

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestWaveID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		rollout  string
		position int
		percent  int
		want     string
	}{
		{name: "first wave", rollout: "ro-1", position: 0, percent: 1, want: "ro-1-w0-1"},
		{name: "last wave", rollout: "ro-1", position: 3, percent: 100, want: "ro-1-w3-100"},
		{name: "position distinguishes waves of one share", rollout: "ro-1", position: 1, percent: 25,
			want: "ro-1-w1-25"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := WaveID(tc.rollout, tc.position, tc.percent)
			if got != tc.want {
				t.Errorf("WaveID(%q, %d, %d) = %q, want %q",
					tc.rollout, tc.position, tc.percent, got, tc.want)
			}
			// Stability is the point of the derivation: the same wave always addresses the
			// same document, however many times its resolution is retried.
			if again := WaveID(tc.rollout, tc.position, tc.percent); again != got {
				t.Errorf("WaveID is not stable: %q then %q", got, again)
			}
		})
	}

	t.Run("distinct rollouts and positions never share an id", func(t *testing.T) {
		t.Parallel()

		seen := map[string]string{}
		for _, rolloutID := range []string{"ro-1", "ro-2"} {
			for position := 0; position < 4; position++ {
				id := WaveID(rolloutID, position, 25)
				if other, ok := seen[id]; ok {
					t.Fatalf("wave id %q is shared by %s and %s/%d", id, other, rolloutID, position)
				}
				seen[id] = fmt.Sprintf("%s/%d", rolloutID, position)
			}
		}
	})
}

func TestShare(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		pool    int
		percent int
		want    int
	}{
		{name: "one percent of a thousand", pool: 1000, percent: 1, want: 10},
		{name: "five percent of a thousand", pool: 1000, percent: 5, want: 50},
		{name: "twenty five percent of a thousand", pool: 1000, percent: 25, want: 250},
		{name: "everything", pool: 1000, percent: 100, want: 1000},
		{name: "rounds down", pool: 7, percent: 25, want: 1},
		{name: "a share smaller than one device", pool: 10, percent: 1, want: 0},
		{name: "an empty pool", pool: 0, percent: 100, want: 0},
		{name: "a share above the whole pool", pool: 5, percent: 150, want: 5},
		{name: "a zero share", pool: 1000, percent: 0, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := Share(tc.pool, tc.percent); got != tc.want {
				t.Errorf("Share(%d, %d) = %d, want %d", tc.pool, tc.percent, got, tc.want)
			}
		})
	}
}

func TestWaveTargets(t *testing.T) {
	t.Parallel()

	t.Run("the canary sequence covers the pool once", func(t *testing.T) {
		t.Parallel()

		pool := devicePool(1000)
		cases := []struct {
			percent int
			want    int
		}{
			{percent: 1, want: 10},
			{percent: 5, want: 40},
			{percent: 25, want: 200},
			{percent: 100, want: 750},
		}

		var targeted []string
		covered := map[string]int{}
		for _, tc := range cases {
			targets := WaveTargets(pool, targeted, tc.percent)
			if len(targets) != tc.want {
				t.Fatalf("wave %d%% targeted %d devices, want %d", tc.percent, len(targets), tc.want)
			}
			for _, id := range targets {
				if wave, ok := covered[id]; ok {
					t.Fatalf("device %s is targeted by wave %d%% and again by wave %d%%",
						id, wave, tc.percent)
				}
				covered[id] = tc.percent
			}
			targeted = append(targeted, targets...)
		}

		// The last wave takes the rest of the pool, so a rollout that promoted every wave
		// targeted the whole eligible pool exactly once.
		if len(covered) != len(pool) {
			t.Errorf("the sequence covered %d devices, want the whole pool of %d",
				len(covered), len(pool))
		}
		for _, id := range pool {
			if _, ok := covered[id]; !ok {
				t.Errorf("device %s is targeted by no wave of the sequence", id)
			}
		}
	})

	t.Run("a share that adds no device is an empty set", func(t *testing.T) {
		t.Parallel()

		targets := WaveTargets(devicePool(10), nil, 1)
		if len(targets) != 0 {
			t.Errorf("WaveTargets() = %v, want no devices", targets)
		}
		// The empty set is a recorded membership: the workflow writes it, so it must never be
		// nil, which the database would store as a missing field.
		if targets == nil {
			t.Error("WaveTargets() = nil, want an empty non-nil target set")
		}
	})

	t.Run("a share earlier waves already covered adds nothing", func(t *testing.T) {
		t.Parallel()

		pool := devicePool(100)
		// The 10% share is the first ten devices; a 10% wave after they were already
		// targeted resolves to nobody rather than re-commanding them.
		targeted := WaveTargets(pool, nil, 10)
		if got := WaveTargets(pool, targeted, 10); len(got) != 0 {
			t.Errorf("WaveTargets() = %v, want no devices", got)
		}
	})

	t.Run("only the devices a share adds are targeted", func(t *testing.T) {
		t.Parallel()

		pool := []string{"dev-1", "dev-2", "dev-3", "dev-4"}
		// dev-1 was targeted by an earlier wave; the 50% share covers dev-1 and dev-2, so this
		// wave adds dev-2 alone.
		if diff := cmp.Diff([]string{"dev-2"}, WaveTargets(pool, []string{"dev-1"}, 50)); diff != "" {
			t.Errorf("WaveTargets() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("an empty pool resolves to nobody at any share", func(t *testing.T) {
		t.Parallel()

		for _, percent := range []int{1, 50, 100} {
			if got := WaveTargets(nil, nil, percent); len(got) != 0 {
				t.Errorf("WaveTargets(nil, nil, %d) = %v, want no devices", percent, got)
			}
		}
	})
}

func TestRolloutStatusTerminal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status RolloutStatus
		want   bool
	}{
		{status: RolloutRunning},
		{status: RolloutAwaitingApproval},
		{status: RolloutRolledBack, want: true},
		{status: RolloutCompleted, want: true},
		{status: RolloutFailed, want: true},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()

			if got := tc.status.Terminal(); got != tc.want {
				t.Errorf("%s.Terminal() = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

// devicePool returns n device ids in the order the store's pool query returns them: ascending by
// identity.
func devicePool(n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, fmt.Sprintf("dev-%04d", i))
	}
	return ids
}
