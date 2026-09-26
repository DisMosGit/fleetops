package firmware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeCatalog is the model-catalog seam with a fixed model set and an optional failure.
type fakeCatalog struct {
	models []string
	err    error
}

// DeviceModels reports the fake's model set or its queued error.
func (f fakeCatalog) DeviceModels(context.Context) ([]string, error) {
	return f.models, f.err
}

func TestValidateUpload(t *testing.T) {
	t.Parallel()

	catalog := fakeCatalog{models: []string{"oak-s3", "oak-s5", "birch-x1"}}
	cases := []struct {
		name    string
		version string
		models  []string
		want    error
	}{
		{
			name:    "known models pass",
			version: "2.0.0",
			models:  []string{"oak-s3", "birch-x1"},
		},
		{
			name:    "version is required",
			version: "",
			models:  []string{"oak-s3"},
			want:    ErrInvalidMetadata,
		},
		{
			name:    "blank version is required",
			version: "   ",
			models:  []string{"oak-s3"},
			want:    ErrInvalidMetadata,
		},
		{
			name:    "empty model list is incompatible",
			version: "2.0.0",
			models:  nil,
			want:    ErrIncompatible,
		},
		{
			name:    "duplicate model is incompatible",
			version: "2.0.0",
			models:  []string{"oak-s3", "oak-s3"},
			want:    ErrIncompatible,
		},
		{
			name:    "unknown model is incompatible",
			version: "2.0.0",
			models:  []string{"oak-s3", "walnut-9"},
			want:    ErrIncompatible,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateUpload(context.Background(), catalog, tc.version, tc.models)
			switch {
			case tc.want == nil && err != nil:
				t.Fatalf("ValidateUpload() error = %v, want nil", err)
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("ValidateUpload() error = %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("rejection names the offending model", func(t *testing.T) {
		t.Parallel()
		err := ValidateUpload(context.Background(), catalog, "2.0.0", []string{"walnut-9"})
		if !strings.Contains(err.Error(), "walnut-9") {
			t.Errorf("ValidateUpload() error = %q, want it to name %q", err, "walnut-9")
		}
	})

	t.Run("catalog failure is not an incompatibility", func(t *testing.T) {
		t.Parallel()
		broken := fakeCatalog{err: fmt.Errorf("registry down")}
		err := ValidateUpload(context.Background(), broken, "2.0.0", []string{"oak-s3"})
		if err == nil || errors.Is(err, ErrIncompatible) || errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("ValidateUpload() error = %v, want a plain catalog failure", err)
		}
	})
}

func TestParseModels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		values []string
		want   []string
	}{
		{name: "empty input", values: nil, want: nil},
		{name: "single value", values: []string{"oak-s3"}, want: []string{"oak-s3"}},
		{
			name:   "comma separated with spaces",
			values: []string{"oak-s3, birch-x1 ,oak-s5"},
			want:   []string{"oak-s3", "birch-x1", "oak-s5"},
		},
		{
			name:   "repeated fields concatenate",
			values: []string{"oak-s3", "birch-x1"},
			want:   []string{"oak-s3", "birch-x1"},
		},
		{
			name:   "empty entries are dropped",
			values: []string{"oak-s3,,", " , birch-x1"},
			want:   []string{"oak-s3", "birch-x1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseModels(tc.values)
			if len(got) != len(tc.want) {
				t.Fatalf("ParseModels() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ParseModels() = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
