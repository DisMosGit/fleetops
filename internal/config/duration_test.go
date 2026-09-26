package config

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.yaml.in/yaml/v3"
)

func TestDurationUnmarshalYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  Duration
	}{
		{name: "milliseconds", input: "d: 250ms\n", want: Duration{Duration: 250 * time.Millisecond}},
		{name: "minutes", input: "d: 2m\n", want: Duration{Duration: 2 * time.Minute}},
		{name: "unparseable string is invalid", input: "d: soon\n", want: Duration{invalid: true}},
		{name: "number is invalid", input: "d: 30\n", want: Duration{invalid: true}},
		{name: "empty string is invalid", input: "d: \"\"\n", want: Duration{invalid: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got struct {
				D Duration `yaml:"d"`
			}
			if err := yaml.Unmarshal([]byte(tc.input), &got); err != nil {
				t.Fatalf("Unmarshal(%q) error = %v", tc.input, err)
			}
			if diff := cmp.Diff(tc.want, got.D); diff != "" {
				t.Errorf("Unmarshal(%q) mismatch (-want +got):\n%s", tc.input, diff)
			}
		})
	}
}

func TestDurationEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b Duration
		want bool
	}{
		{name: "same value", a: Duration{Duration: time.Second}, b: Duration{Duration: time.Second}, want: true},
		{name: "different value", a: Duration{Duration: time.Second}, b: Duration{Duration: 2 * time.Second}, want: false},
		{name: "zero and invalid differ", a: Duration{}, b: Duration{invalid: true}, want: false},
		{name: "both invalid differ by value only", a: Duration{invalid: true}, b: Duration{invalid: true}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("Duration.Equal() = %v, want %v", got, tc.want)
			}
		})
	}
}
