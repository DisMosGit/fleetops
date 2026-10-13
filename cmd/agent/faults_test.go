package main

import (
	"context"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.yaml.in/yaml/v3"

	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/config"
)

// faultStubFetcher is a hand-written FirmwareFetcher double standing in for the downloader the
// binary installs, so the wiring tests exercise the decoration without a control plane.
type faultStubFetcher struct{}

// Fetch returns a fixed path without downloading anything.
func (faultStubFetcher) Fetch(
	context.Context,
	string, string, string,
	func(int32),
) (string, error) {
	return "/tmp/fw-1.bin", nil
}

func TestNewFetcherInstallsFaultsOnlyWhenConfigured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		faults       config.Faults
		wantInjector bool
		wantErr      string // substring the error must contain; empty means success
	}{
		{
			name:   "no fault settings download through the real path",
			faults: config.Faults{},
		},
		{
			name:   "the default section injects nothing",
			faults: config.Defaults().Simulation.Faults,
		},
		{
			name: "a zero share injects nothing whatever the kinds say",
			faults: config.Faults{
				Share:         0,
				Kinds:         []string{"stall"},
				StallDuration: config.Duration{Duration: time.Second},
			},
		},
		{
			name: "a configured share installs the seeded injector",
			faults: config.Faults{
				Share:         0.5,
				Kinds:         []string{"stall", "transfer"},
				StallDuration: config.Duration{Duration: time.Second},
			},
			wantInjector: true,
		},
		{
			name: "a full share installs the seeded injector",
			faults: config.Faults{
				Share: 1, Kinds: []string{"transfer"},
			},
			wantInjector: true,
		},
		{
			name:    "an unknown kind is refused rather than injected",
			faults:  config.Faults{Share: 0.5, Kinds: []string{"corrupt"}},
			wantErr: "kind",
		},
		{
			name:    "a stall without a stall duration is refused",
			faults:  config.Faults{Share: 0.5, Kinds: []string{"stall"}},
			wantErr: "stall duration",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &faultStubFetcher{}
			fetcher, err := newFetcher(tc.faults, base, rand.NewSource(1))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("newFetcher() = %T, want error containing %q", fetcher, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("newFetcher() error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("newFetcher() error = %v", err)
			}

			_, isInjector := fetcher.(*agent.FaultFetcher)
			if isInjector != tc.wantInjector {
				t.Errorf("fetcher = %T, want the injector installed = %v", fetcher, tc.wantInjector)
			}
			if !tc.wantInjector && fetcher != agent.FirmwareFetcher(base) {
				t.Errorf("fetcher = %T, want the downloader itself when no fault is configured", fetcher)
			}
		})
	}
}

// TestConfiguredFaultKindsMatchTheInjector pins the two fault-kind sets together: the
// configuration validates the names a file spells, and the injector draws the typed set, so a
// kind added to one without the other would be a configuration the emulator refuses at startup.
func TestConfiguredFaultKindsMatchTheInjector(t *testing.T) {
	t.Parallel()

	injector := make([]string, 0, len(agent.FaultKinds()))
	for _, kind := range agent.FaultKinds() {
		injector = append(injector, string(kind))

		cfg := config.Defaults()
		cfg.Simulation.Faults.Share = 0.5
		cfg.Simulation.Faults.Kinds = []string{string(kind)}
		if err := cfg.Validate(); err != nil {
			t.Errorf("kind %q: Validate() error = %v, want the injector's kind accepted", kind, err)
		}
	}
	if diff := cmp.Diff(injector, config.Defaults().Simulation.Faults.Kinds); diff != "" {
		t.Errorf("configured fault kinds mismatch (-injector +config):\n%s", diff)
	}
}

func TestDocumentedFaultSettingsMatchConfiguration(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "README.md")
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	snippet := faultSettingsSnippet(t, string(doc))

	// Every documented setting names a configuration field and carries its default: strict
	// decoding rejects a documented key the schema does not have, and the comparison rejects a
	// documented value the defaults do not produce.
	var documented struct {
		Simulation struct {
			Faults config.Faults `yaml:"faults"`
		} `yaml:"simulation"`
	}
	dec := yaml.NewDecoder(strings.NewReader(snippet))
	dec.KnownFields(true)
	if err := dec.Decode(&documented); err != nil {
		t.Fatalf("decode the documented fault settings: %v", err)
	}
	defaults := config.Defaults().Simulation.Faults
	if diff := cmp.Diff(defaults, documented.Simulation.Faults); diff != "" {
		t.Errorf("documented fault settings mismatch (-want defaults +got):\n%s", diff)
	}
	if documented.Simulation.Faults.Enabled() {
		t.Error("the documented default share injects faults, want the default to inject none")
	}

	// And the reverse: every configuration field is documented, so a field added without a
	// documented setting fails here.
	var raw struct {
		Simulation struct {
			Faults map[string]any `yaml:"faults"`
		} `yaml:"simulation"`
	}
	if err := yaml.Unmarshal([]byte(snippet), &raw); err != nil {
		t.Fatalf("decode the documented fault settings: %v", err)
	}
	documentedFields := slices.Sorted(maps.Keys(raw.Simulation.Faults))
	if diff := cmp.Diff(faultFieldNames(t), documentedFields); diff != "" {
		t.Errorf("documented fault settings mismatch (-configuration fields +documented):\n%s", diff)
	}
}

// faultSettingsSnippet returns the yaml block of cmd/README.md that documents the emulator's
// fault settings.
func faultSettingsSnippet(t *testing.T, doc string) string {
	t.Helper()

	for _, block := range strings.Split(doc, "```yaml")[1:] {
		body, _, ok := strings.Cut(block, "```")
		if ok && strings.Contains(body, "faults:") {
			return body
		}
	}
	t.Fatal("cmd/README.md documents no fault settings in a yaml block")
	return ""
}

// faultFieldNames returns the yaml names of the fault configuration's fields, sorted.
func faultFieldNames(t *testing.T) []string {
	t.Helper()

	typ := reflect.TypeFor[config.Faults]()
	names := make([]string, 0, typ.NumField())
	for i := range typ.NumField() {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("yaml"), ",")
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
