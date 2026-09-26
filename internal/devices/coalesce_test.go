package devices

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestCoalesce(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		updates []Update
		want    []Update
	}{
		{
			name: "empty",
			want: []Update{},
		},
		{
			name: "single update passes through",
			updates: []Update{
				{ID: "dev-1", CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: base},
			},
			want: []Update{
				{ID: "dev-1", CurrentFw: "1.0.0", Status: StatusOnline, LastSeen: base},
			},
		},
		{
			name: "newest wins over older",
			updates: []Update{
				{ID: "dev-1", CurrentFw: "1.0.0", LastSeen: base},
				{ID: "dev-1", CurrentFw: "2.0.0", LastSeen: base.Add(time.Second)},
			},
			want: []Update{
				{ID: "dev-1", CurrentFw: "2.0.0", LastSeen: base.Add(time.Second)},
			},
		},
		{
			name: "older arriving later does not regress",
			updates: []Update{
				{ID: "dev-1", CurrentFw: "2.0.0", LastSeen: base.Add(time.Second)},
				{ID: "dev-1", CurrentFw: "1.0.0", LastSeen: base},
			},
			want: []Update{
				{ID: "dev-1", CurrentFw: "2.0.0", LastSeen: base.Add(time.Second)},
			},
		},
		{
			name: "distinct devices keep first-seen order",
			updates: []Update{
				{ID: "dev-2", LastSeen: base},
				{ID: "dev-1", LastSeen: base},
				{ID: "dev-2", LastSeen: base.Add(time.Second)},
			},
			want: []Update{
				{ID: "dev-2", LastSeen: base.Add(time.Second)},
				{ID: "dev-1", LastSeen: base},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := coalesce(tc.updates)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("coalesce() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
