package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestLoadWithoutPathUsesDefaults(t *testing.T) {
	t.Parallel()

	got, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") error = %v", err)
	}
	if diff := cmp.Diff(Defaults(), got); diff != "" {
		t.Errorf("Load(\"\") mismatch (-want +got):\n%s", diff)
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		mutate  func(*Config) // applied to Defaults() to build the expected config
		wantErr string        // substring the error must contain; empty means success
	}{
		{
			name: "empty file takes defaults",
			file: "",
		},
		{
			name: "comments only take defaults",
			file: "# FleetOps configuration\n",
		},
		{
			name: "present fields override defaults",
			file: "simulation:\n" +
				"  fleet_size: 500\n" +
				"mongodb:\n" +
				"  uri: mongodb://mongo.infra:27017\n",
			mutate: func(c *Config) {
				c.Simulation.FleetSize = 500
				c.MongoDB.URI = "mongodb://mongo.infra:27017"
			},
		},
		{
			name: "apply stub knobs override defaults",
			file: "simulation:\n" +
				"  apply_delay: 250ms\n" +
				"  apply_success_rate: 0.25\n",
			mutate: func(c *Config) {
				c.Simulation.ApplyDelay = Duration{Duration: 250 * time.Millisecond}
				c.Simulation.ApplySuccessRate = 0.25
			},
		},
		{
			name: "partial file keeps other defaults",
			file: "temporal:\n  namespace: fleetops-dev\n",
			mutate: func(c *Config) {
				c.Temporal.Namespace = "fleetops-dev"
			},
		},
		{
			name: "liveness and telemetry absent keep defaults",
			file: "simulation:\n  fleet_size: 250\n",
			mutate: func(c *Config) {
				c.Simulation.FleetSize = 250
			},
		},
		{
			name: "liveness and telemetry override defaults",
			file: "liveness:\n  offline_threshold: 2m\n  sweep_interval: 5s\n" +
				"telemetry:\n  batch_size: 100\n  flush_interval: 250ms\n",
			mutate: func(c *Config) {
				c.Liveness.OfflineThreshold = Duration{Duration: 2 * time.Minute}
				c.Liveness.SweepInterval = Duration{Duration: 5 * time.Second}
				c.Telemetry.BatchSize = 100
				c.Telemetry.FlushInterval = Duration{Duration: 250 * time.Millisecond}
			},
		},
		{
			name: "snapshots section absent keeps defaults",
			file: "simulation:\n  fleet_size: 250\n",
			mutate: func(c *Config) {
				c.Simulation.FleetSize = 250
			},
		},
		{
			name: "snapshots interval overrides default",
			file: "snapshots:\n  interval: 30s\n",
			mutate: func(c *Config) {
				c.Snapshots.Interval = Duration{Duration: 30 * time.Second}
			},
		},
		{
			name:    "malformed duration is rejected by field name",
			file:    "liveness:\n  offline_threshold: soon\n",
			wantErr: "liveness.offline_threshold",
		},
		{
			name:    "malformed snapshots interval is rejected by field name",
			file:    "snapshots:\n  interval: soon\n",
			wantErr: "snapshots.interval",
		},
		{
			name:    "non-string duration is rejected by field name",
			file:    "telemetry:\n  flush_interval: 30\n",
			wantErr: "telemetry.flush_interval",
		},
		{
			name:    "unknown key is rejected",
			file:    "grpc:\n  port: 1234\n",
			wantErr: "port",
		},
		{
			name:    "malformed yaml is rejected",
			file:    "grpc: [\n",
			wantErr: "decode config",
		},
		{
			name:    "invalid value is rejected",
			file:    "simulation:\n  fleet_size: 0\n",
			wantErr: "simulation.fleet_size",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.file), 0o600); err != nil {
				t.Fatalf("write test config: %v", err)
			}

			got, err := Load(path)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Load() = %+v, want error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Load() error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			want := Defaults()
			if tc.mutate != nil {
				tc.mutate(&want)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Load() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nope.yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() on a missing file = nil error, want an error")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("Load() error = %q, want it to name the path %q", err, path)
	}
}

func TestSampleConfigLoads(t *testing.T) {
	t.Parallel()

	// The committed sample documents the defaults, so loading it must yield exactly them —
	// this test fails when the sample and the defaults table drift apart.
	got, err := Load(filepath.Join("..", "..", "deploy", "config.yaml"))
	if err != nil {
		t.Fatalf("Load(deploy/config.yaml) error = %v", err)
	}
	if diff := cmp.Diff(Defaults(), got); diff != "" {
		t.Errorf("deploy/config.yaml mismatch (-want defaults +got):\n%s", diff)
	}
}
