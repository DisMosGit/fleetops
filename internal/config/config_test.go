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

// TestBrokerAndAlertingDefaults pins every pipeline default the file may omit: each field is
// asserted on its own, so a missing default fails with the field's name.
func TestBrokerAndAlertingDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	file := "simulation:\n  fleet_size: 250\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	defaults := []struct {
		field string
		got   any
		want  any
	}{
		{"rabbitmq.prefetch", got.RabbitMQ.Prefetch, 32},
		{"rabbitmq.publish_buffer", got.RabbitMQ.PublishBuffer, 1024},
		{"rabbitmq.max_attempts", got.RabbitMQ.MaxAttempts, 3},
		{"rabbitmq.retry_base", got.RabbitMQ.RetryBase.Duration, 5 * time.Second},
		{"rabbitmq.retry_max", got.RabbitMQ.RetryMax.Duration, time.Minute},
		{"rabbitmq.queue_depth_interval", got.RabbitMQ.QueueDepthInterval.Duration, 15 * time.Second},
		{"alerting.health_threshold", got.Alerting.HealthThreshold, 0.6},
	}
	for _, tc := range defaults {
		t.Run(tc.field, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tc.want, tc.got); diff != "" {
				t.Errorf("%s default mismatch (-want +got):\n%s", tc.field, diff)
			}
		})
	}
}

// TestRolloutDefaults pins the canary gating defaults an absent rollout section yields: each
// field is asserted on its own, so a drifted default fails with the field's name.
func TestRolloutDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	file := "simulation:\n  fleet_size: 250\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	defaults := []struct {
		field string
		got   any
		want  any
	}{
		{"rollout.health_window", got.Rollout.HealthWindow.Duration, 5 * time.Minute},
		{"rollout.sample_health_threshold", got.Rollout.SampleHealthThreshold, 0.6},
		{"rollout.min_success_ratio", got.Rollout.MinSuccessRatio, 0.95},
		{"rollout.min_samples", got.Rollout.MinSamples, 10},
		{"rollout.decision_timeout", got.Rollout.DecisionTimeout.Duration, 30 * time.Minute},
		{"rollout.result_timeout", got.Rollout.ResultTimeout.Duration, 5 * time.Minute},
		{"rollout.waves", got.Rollout.Waves, []Wave{
			{Percent: 1},
			{Percent: 5},
			{Percent: 25, RequireApproval: true},
			{Percent: 100, RequireApproval: true},
		}},
	}
	for _, tc := range defaults {
		t.Run(tc.field, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tc.want, tc.got); diff != "" {
				t.Errorf("%s default mismatch (-want +got):\n%s", tc.field, diff)
			}
		})
	}
}

// TestTemporalQueueDefaults pins the two queue names an absent temporal section yields: the work
// queue the workers poll and the separate queue the control plane polls for command dispatch.
// Each is asserted on its own, so a drifted default fails with the queue's name.
func TestTemporalQueueDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	file := "simulation:\n  fleet_size: 250\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	defaults := []struct {
		field string
		got   any
		want  any
	}{
		{"temporal.task_queue", got.Temporal.TaskQueue, "fleetops"},
		{"temporal.dispatch_task_queue", got.Temporal.DispatchTaskQueue, "fleetops-controlplane"},
	}
	for _, tc := range defaults {
		t.Run(tc.field, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tc.want, tc.got); diff != "" {
				t.Errorf("%s default mismatch (-want +got):\n%s", tc.field, diff)
			}
		})
	}
	// The separation only holds while the two names differ: a config whose dispatch queue
	// collapsed onto the work queue would put dispatch tasks back in front of every worker
	// poller, which is the defect this value exists to remove.
	if got.Temporal.DispatchTaskQueue == got.Temporal.TaskQueue {
		t.Errorf("dispatch queue = work queue %q, want two distinct queues",
			got.Temporal.TaskQueue)
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
			name: "rollout gating section overrides defaults",
			file: "rollout:\n" +
				"  health_window: 2m\n" +
				"  sample_health_threshold: 0.5\n" +
				"  min_success_ratio: 0.9\n" +
				"  min_samples: 25\n",
			mutate: func(c *Config) {
				c.Rollout.HealthWindow = Duration{Duration: 2 * time.Minute}
				c.Rollout.SampleHealthThreshold = 0.5
				c.Rollout.MinSuccessRatio = 0.9
				c.Rollout.MinSamples = 25
			},
		},
		{
			name: "rollout sequence overrides the default",
			file: "rollout:\n" +
				"  waves:\n" +
				"    - percent: 10\n" +
				"      require_approval: false\n" +
				"    - percent: 100\n" +
				"      require_approval: false\n",
			mutate: func(c *Config) {
				c.Rollout.Waves = []Wave{{Percent: 10}, {Percent: 100}}
			},
		},
		{
			name: "rollout decision timeout overrides the default",
			file: "rollout:\n  decision_timeout: 10m\n",
			mutate: func(c *Config) {
				c.Rollout.DecisionTimeout = Duration{Duration: 10 * time.Minute}
			},
		},
		{
			name: "rollout result timeout overrides the default",
			file: "rollout:\n  result_timeout: 2m\n",
			mutate: func(c *Config) {
				c.Rollout.ResultTimeout = Duration{Duration: 2 * time.Minute}
			},
		},
		{
			name:    "zero rollout result timeout is rejected by field name",
			file:    "rollout:\n  result_timeout: 0s\n",
			wantErr: "rollout.result_timeout",
		},
		{
			name:    "unparseable rollout result timeout is rejected by field name",
			file:    "rollout:\n  result_timeout: soon\n",
			wantErr: "rollout.result_timeout",
		},
		{
			name: "rollout result timeout above the decision timeout is rejected",
			file: "rollout:\n  decision_timeout: 10m\n  result_timeout: 11m\n",
			// A result wait longer than the wave's own decision timeout would let the
			// deadline pass while the wave still waits on devices.
			wantErr: "rollout.result_timeout",
		},
		{
			name: "temporal dispatch queue overrides the default",
			file: "temporal:\n" +
				"  task_queue: work\n" +
				"  dispatch_task_queue: dispatch\n",
			mutate: func(c *Config) {
				c.Temporal.TaskQueue = "work"
				c.Temporal.DispatchTaskQueue = "dispatch"
			},
		},
		{
			name:    "empty dispatch task queue is rejected by field name",
			file:    "temporal:\n  dispatch_task_queue: \"\"\n",
			wantErr: "temporal.dispatch_task_queue",
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
