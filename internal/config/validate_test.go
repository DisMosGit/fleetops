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
		{name: "rabbitmq url with wrong scheme", mutate: func(c *Config) {
			c.RabbitMQ.URL = "https://localhost:5672/"
		}, wantErrs: []string{"rabbitmq.url"}},
		{name: "temporal addr without port", mutate: func(c *Config) {
			c.Temporal.Address = "temporal"
		}, wantErrs: []string{"temporal.address"}},
		{name: "empty namespace", mutate: func(c *Config) {
			c.Temporal.Namespace = ""
		}, wantErrs: []string{"temporal.namespace"}},
		{name: "empty task queue", mutate: func(c *Config) {
			c.Temporal.TaskQueue = ""
		}, wantErrs: []string{"temporal.task_queue"}},
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
