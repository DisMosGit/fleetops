package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// TestFirmwareDefaults pins the upload path's defaults: an absent firmware section spools under
// the OS temporary directory, waits a bounded time for the upload's workflow, and sweeps files
// abandoned more than an hour ago.
func TestFirmwareDefaults(t *testing.T) {
	t.Parallel()

	got, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") error = %v", err)
	}

	want := Firmware{
		SpoolDir:    filepath.Join(os.TempDir(), "fleetops-firmware"),
		WaitTimeout: Duration{Duration: 30 * time.Second},
		MaxSpoolAge: Duration{Duration: time.Hour},
	}
	if diff := cmp.Diff(want, got.Firmware); diff != "" {
		t.Errorf("firmware defaults mismatch (-want +got):\n%s", diff)
	}
	if !strings.HasPrefix(got.Firmware.SpoolDir, os.TempDir()) {
		t.Errorf("spool directory = %q, want it under %q", got.Firmware.SpoolDir, os.TempDir())
	}
}

// TestFirmwareSectionLoads pins that the configured upload path reaches the loaded configuration:
// the spool directory and both durations are the file's values.
func TestFirmwareSectionLoads(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	file := strings.Join([]string{
		"firmware:",
		"  spool_dir: \"/var/spool/fleetops\"",
		"  wait_timeout: \"45s\"",
		"  max_spool_age: \"15m\"",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Firmware{
		SpoolDir:    "/var/spool/fleetops",
		WaitTimeout: Duration{Duration: 45 * time.Second},
		MaxSpoolAge: Duration{Duration: 15 * time.Minute},
	}
	if diff := cmp.Diff(want, got.Firmware); diff != "" {
		t.Errorf("firmware section mismatch (-want +got):\n%s", diff)
	}
}

// TestValidateFirmware pins the upload path's validation: the spool directory must be named and
// both durations must be parsed, positive values, so a misconfiguration fails at startup instead
// of when the first upload arrives.
func TestValidateFirmware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*Config)
		wantErrs []string
	}{
		{name: "the defaults are valid"},
		{name: "an empty spool directory", mutate: func(c *Config) {
			c.Firmware.SpoolDir = ""
		}, wantErrs: []string{"firmware.spool_dir"}},
		{name: "a whitespace spool directory", mutate: func(c *Config) {
			c.Firmware.SpoolDir = "   "
		}, wantErrs: []string{"firmware.spool_dir"}},
		{name: "a zero wait timeout", mutate: func(c *Config) {
			c.Firmware.WaitTimeout = Duration{}
		}, wantErrs: []string{"firmware.wait_timeout"}},
		{name: "a negative wait timeout", mutate: func(c *Config) {
			c.Firmware.WaitTimeout = Duration{Duration: -time.Second}
		}, wantErrs: []string{"firmware.wait_timeout"}},
		{name: "an unparseable wait timeout", mutate: func(c *Config) {
			c.Firmware.WaitTimeout = Duration{invalid: true}
		}, wantErrs: []string{"firmware.wait_timeout"}},
		{name: "a zero maximum spool age", mutate: func(c *Config) {
			c.Firmware.MaxSpoolAge = Duration{}
		}, wantErrs: []string{"firmware.max_spool_age"}},
		{name: "an unparseable maximum spool age", mutate: func(c *Config) {
			c.Firmware.MaxSpoolAge = Duration{invalid: true}
		}, wantErrs: []string{"firmware.max_spool_age"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := Defaults()
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			err := cfg.Validate()
			if len(tc.wantErrs) == 0 {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %v", tc.wantErrs)
			}
			for _, want := range tc.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Validate() error = %q, want it to name %q", err, want)
				}
			}
		})
	}
}
