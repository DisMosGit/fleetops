package config

import (
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*Config)
		wantErrs []string // substrings the joined error must contain; empty means valid
	}{
		{name: "defaults are valid"},
		{name: "empty otel endpoint is valid", mutate: func(c *Config) {
			c.Observability.OTelEndpoint = ""
		}},
		{name: "zero fleet size", mutate: func(c *Config) {
			c.Simulation.FleetSize = 0
		}, wantErrs: []string{"simulation.fleet_size"}},
		{name: "negative fleet size", mutate: func(c *Config) {
			c.Simulation.FleetSize = -1
		}, wantErrs: []string{"simulation.fleet_size"}},
		{name: "zero apply delay", mutate: func(c *Config) {
			c.Simulation.ApplyDelay = Duration{}
		}, wantErrs: []string{"simulation.apply_delay"}},
		{name: "unparseable apply delay", mutate: func(c *Config) {
			c.Simulation.ApplyDelay = Duration{invalid: true}
		}, wantErrs: []string{"simulation.apply_delay"}},
		{name: "apply success rate above one", mutate: func(c *Config) {
			c.Simulation.ApplySuccessRate = 1.5
		}, wantErrs: []string{"simulation.apply_success_rate"}},
		{name: "negative apply success rate", mutate: func(c *Config) {
			c.Simulation.ApplySuccessRate = -0.1
		}, wantErrs: []string{"simulation.apply_success_rate"}},
		{name: "listen addr without port", mutate: func(c *Config) {
			c.GRPC.ListenAddr = "localhost"
		}, wantErrs: []string{"grpc.listen_addr"}},
		{name: "listen addr with non numeric port", mutate: func(c *Config) {
			c.GRPC.ListenAddr = "localhost:grpc"
		}, wantErrs: []string{"grpc.listen_addr"}},
		{name: "empty control plane addr", mutate: func(c *Config) {
			c.GRPC.ControlPlaneAddr = ""
		}, wantErrs: []string{"grpc.control_plane_addr"}},
		{name: "mongodb uri without scheme", mutate: func(c *Config) {
			c.MongoDB.URI = "localhost:27017"
		}, wantErrs: []string{"mongodb.uri"}},
		{name: "mongodb uri with wrong scheme", mutate: func(c *Config) {
			c.MongoDB.URI = "http://localhost:27017"
		}, wantErrs: []string{"mongodb.uri"}},
		{name: "mongodb uri without host", mutate: func(c *Config) {
			c.MongoDB.URI = "mongodb://"
		}, wantErrs: []string{"mongodb.uri"}},
		{name: "blank database", mutate: func(c *Config) {
			c.MongoDB.Database = "  "
		}, wantErrs: []string{"mongodb.database"}},
		{name: "zero offline threshold", mutate: func(c *Config) {
			c.Liveness.OfflineThreshold = Duration{}
		}, wantErrs: []string{"liveness.offline_threshold"}},
		{name: "negative sweep interval", mutate: func(c *Config) {
			c.Liveness.SweepInterval = Duration{Duration: -time.Second}
		}, wantErrs: []string{"liveness.sweep_interval"}},
		{name: "unparseable duration", mutate: func(c *Config) {
			c.Liveness.OfflineThreshold = Duration{invalid: true}
		}, wantErrs: []string{"liveness.offline_threshold"}},
		{name: "zero batch size", mutate: func(c *Config) {
			c.Telemetry.BatchSize = 0
		}, wantErrs: []string{"telemetry.batch_size"}},
		{name: "negative batch size", mutate: func(c *Config) {
			c.Telemetry.BatchSize = -1
		}, wantErrs: []string{"telemetry.batch_size"}},
		{name: "zero flush interval", mutate: func(c *Config) {
			c.Telemetry.FlushInterval = Duration{}
		}, wantErrs: []string{"telemetry.flush_interval"}},
		{name: "zero snapshot interval", mutate: func(c *Config) {
			c.Snapshots.Interval = Duration{}
		}, wantErrs: []string{"snapshots.interval"}},
		{name: "negative snapshot interval", mutate: func(c *Config) {
			c.Snapshots.Interval = Duration{Duration: -time.Minute}
		}, wantErrs: []string{"snapshots.interval"}},
		{name: "rabbitmq url with wrong scheme", mutate: func(c *Config) {
			c.RabbitMQ.URL = "https://localhost:5672/"
		}, wantErrs: []string{"rabbitmq.url"}},
		{name: "zero prefetch", mutate: func(c *Config) {
			c.RabbitMQ.Prefetch = 0
		}, wantErrs: []string{"rabbitmq.prefetch"}},
		{name: "negative publish buffer", mutate: func(c *Config) {
			c.RabbitMQ.PublishBuffer = -1
		}, wantErrs: []string{"rabbitmq.publish_buffer"}},
		{name: "negative max attempts", mutate: func(c *Config) {
			c.RabbitMQ.MaxAttempts = -1
		}, wantErrs: []string{"rabbitmq.max_attempts"}},
		{name: "unparseable retry base", mutate: func(c *Config) {
			c.RabbitMQ.RetryBase = Duration{invalid: true}
		}, wantErrs: []string{"rabbitmq.retry_base"}},
		{name: "retry max below retry base", mutate: func(c *Config) {
			c.RabbitMQ.RetryMax = Duration{Duration: time.Second}
		}, wantErrs: []string{"rabbitmq.retry_max"}},
		{name: "zero queue depth interval", mutate: func(c *Config) {
			c.RabbitMQ.QueueDepthInterval = Duration{}
		}, wantErrs: []string{"rabbitmq.queue_depth_interval"}},
		{name: "health threshold above one", mutate: func(c *Config) {
			c.Alerting.HealthThreshold = 1.5
		}, wantErrs: []string{"alerting.health_threshold"}},
		{name: "negative health threshold", mutate: func(c *Config) {
			c.Alerting.HealthThreshold = -0.1
		}, wantErrs: []string{"alerting.health_threshold"}},
		{name: "unparseable rollout health window", mutate: func(c *Config) {
			c.Rollout.HealthWindow = Duration{invalid: true}
		}, wantErrs: []string{"rollout.health_window"}},
		{name: "zero rollout health window", mutate: func(c *Config) {
			c.Rollout.HealthWindow = Duration{}
		}, wantErrs: []string{"rollout.health_window"}},
		{name: "negative rollout health window", mutate: func(c *Config) {
			c.Rollout.HealthWindow = Duration{Duration: -time.Minute}
		}, wantErrs: []string{"rollout.health_window"}},
		{name: "zero min samples", mutate: func(c *Config) {
			c.Rollout.MinSamples = 0
		}, wantErrs: []string{"rollout.min_samples"}},
		{name: "negative min samples", mutate: func(c *Config) {
			c.Rollout.MinSamples = -1
		}, wantErrs: []string{"rollout.min_samples"}},
		{name: "sample health threshold above one", mutate: func(c *Config) {
			c.Rollout.SampleHealthThreshold = 1.5
		}, wantErrs: []string{"rollout.sample_health_threshold"}},
		{name: "negative sample health threshold", mutate: func(c *Config) {
			c.Rollout.SampleHealthThreshold = -0.1
		}, wantErrs: []string{"rollout.sample_health_threshold"}},
		{name: "min success ratio above one", mutate: func(c *Config) {
			c.Rollout.MinSuccessRatio = 1.5
		}, wantErrs: []string{"rollout.min_success_ratio"}},
		{name: "negative min success ratio", mutate: func(c *Config) {
			c.Rollout.MinSuccessRatio = -0.1
		}, wantErrs: []string{"rollout.min_success_ratio"}},
		{name: "empty wave sequence", mutate: func(c *Config) {
			c.Rollout.Waves = nil
		}, wantErrs: []string{"rollout.waves"}},
		{name: "zero wave percentage", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 0}, {Percent: 100}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "wave percentage above one hundred", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 101}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "negative wave percentage", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: -5}, {Percent: 100}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "repeated wave percentage", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 25}, {Percent: 25}, {Percent: 100}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "descending wave percentage", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 50}, {Percent: 25}, {Percent: 100}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "wave sequence ends below one hundred", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 10}, {Percent: 50}}
		}, wantErrs: []string{"rollout.waves"}},
		{name: "single full wave is valid", mutate: func(c *Config) {
			c.Rollout.Waves = []Wave{{Percent: 100}}
		}},
		{name: "unparseable decision timeout", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = Duration{invalid: true}
		}, wantErrs: []string{"rollout.decision_timeout"}},
		{name: "zero decision timeout", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = Duration{}
		}, wantErrs: []string{"rollout.decision_timeout"}},
		{name: "decision timeout below the health window", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = Duration{Duration: time.Minute}
		}, wantErrs: []string{"rollout.decision_timeout"}},
		{name: "decision timeout at the health window is valid", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = c.Rollout.HealthWindow
		}},
		{name: "unparseable result timeout", mutate: func(c *Config) {
			c.Rollout.ResultTimeout = Duration{invalid: true}
		}, wantErrs: []string{"rollout.result_timeout"}},
		{name: "zero result timeout", mutate: func(c *Config) {
			c.Rollout.ResultTimeout = Duration{}
		}, wantErrs: []string{"rollout.result_timeout"}},
		{name: "negative result timeout", mutate: func(c *Config) {
			c.Rollout.ResultTimeout = Duration{Duration: -time.Minute}
		}, wantErrs: []string{"rollout.result_timeout"}},
		{name: "result timeout above the decision timeout", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = Duration{Duration: 10 * time.Minute}
			c.Rollout.ResultTimeout = Duration{Duration: 11 * time.Minute}
		}, wantErrs: []string{"rollout.result_timeout"}},
		{name: "result timeout at the decision timeout is valid", mutate: func(c *Config) {
			c.Rollout.DecisionTimeout = Duration{Duration: 10 * time.Minute}
			c.Rollout.ResultTimeout = Duration{Duration: 10 * time.Minute}
		}},
		{name: "temporal addr without port", mutate: func(c *Config) {
			c.Temporal.Address = "temporal"
		}, wantErrs: []string{"temporal.address"}},
		{name: "empty namespace", mutate: func(c *Config) {
			c.Temporal.Namespace = ""
		}, wantErrs: []string{"temporal.namespace"}},
		{name: "empty task queue", mutate: func(c *Config) {
			c.Temporal.TaskQueue = ""
		}, wantErrs: []string{"temporal.task_queue"}},
		{name: "empty dispatch task queue", mutate: func(c *Config) {
			c.Temporal.DispatchTaskQueue = ""
		}, wantErrs: []string{"temporal.dispatch_task_queue"}},
		{name: "blank dispatch task queue", mutate: func(c *Config) {
			c.Temporal.DispatchTaskQueue = "   "
		}, wantErrs: []string{"temporal.dispatch_task_queue"}},
		{name: "both queues empty are reported together", mutate: func(c *Config) {
			c.Temporal.TaskQueue = ""
			c.Temporal.DispatchTaskQueue = ""
		}, wantErrs: []string{"temporal.task_queue", "temporal.dispatch_task_queue"}},
		{name: "bad otel endpoint", mutate: func(c *Config) {
			c.Observability.OTelEndpoint = "collector:grpc"
		}, wantErrs: []string{"observability.otel_endpoint"}},
		{name: "bad metrics addr", mutate: func(c *Config) {
			c.Observability.MetricsAddr = ":"
		}, wantErrs: []string{"observability.metrics_addr"}},
		{name: "bad health addr", mutate: func(c *Config) {
			c.Observability.HealthAddr = "8081"
		}, wantErrs: []string{"observability.health_addr"}},
		{name: "violations are joined", mutate: func(c *Config) {
			c.Simulation.FleetSize = 0
			c.MongoDB.Database = ""
			c.Temporal.Namespace = ""
		}, wantErrs: []string{"simulation.fleet_size", "mongodb.database", "temporal.namespace"}},
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
				t.Fatal("Validate() = nil, want an error")
			}
			for _, want := range tc.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Validate() error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}

func TestValidateDoesNotEchoConfiguredValues(t *testing.T) {
	t.Parallel()

	cfg := Defaults()
	cfg.RabbitMQ.URL = "amqp://operator:secret@localhost:notaport"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error")
	}
	for _, secret := range []string{"operator", "secret", "notaport"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("Validate() error = %q, want it not to echo %q", err, secret)
		}
	}
}
