// Package config loads and validates the single FleetOps YAML configuration file.
//
// The file is one YAML document whose sections map one-to-one onto the fields of Config.
// Load decodes over a fully populated default Config, so a field absent from the file keeps its
// documented default, and validation rejects unknown keys and malformed values at startup
// instead of letting a typo run the service with the wrong endpoint or scale.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the complete FleetOps service configuration: one shared file for every binary.
type Config struct {
	// Simulation configures the emulated device fleet.
	Simulation Simulation `yaml:"simulation"`
	// GRPC configures the agent-facing gRPC transport.
	GRPC GRPC `yaml:"grpc"`
	// MongoDB configures the fleet database.
	MongoDB MongoDB `yaml:"mongodb"`
	// Liveness configures heartbeat-staleness detection.
	Liveness Liveness `yaml:"liveness"`
	// Telemetry configures heartbeat ingestion.
	Telemetry Telemetry `yaml:"telemetry"`
	// Snapshots configures device-workflow state persistence.
	Snapshots Snapshots `yaml:"snapshots"`
	// RabbitMQ configures the telemetry broker.
	RabbitMQ RabbitMQ `yaml:"rabbitmq"`
	// Temporal configures the orchestration backend.
	Temporal Temporal `yaml:"temporal"`
	// Observability configures metrics, tracing, and probe endpoints.
	Observability Observability `yaml:"observability"`
}

// Simulation configures the emulated device fleet.
type Simulation struct {
	// FleetSize is the number of simulated devices one agent emulator process runs.
	FleetSize int `yaml:"fleet_size"`
}

// GRPC configures the agent-facing gRPC transport.
type GRPC struct {
	// ListenAddr is the control plane's AgentService listen address, in host:port form.
	ListenAddr string `yaml:"listen_addr"`
	// ControlPlaneAddr is the control plane gRPC address the agent connects to, in host:port form.
	ControlPlaneAddr string `yaml:"control_plane_addr"`
}

// MongoDB configures the fleet database.
type MongoDB struct {
	// URI is the MongoDB connection URI (mongodb:// or mongodb+srv://).
	URI string `yaml:"uri"`
	// Database is the fleet database name.
	Database string `yaml:"database"`
}

// Liveness configures heartbeat-staleness detection for the device fleet.
type Liveness struct {
	// OfflineThreshold is the silence after which a device is marked offline.
	OfflineThreshold Duration `yaml:"offline_threshold"`
	// SweepInterval is how often the offline-staleness sweep runs.
	SweepInterval Duration `yaml:"sweep_interval"`
}

// Telemetry configures heartbeat ingestion.
type Telemetry struct {
	// BatchSize is the number of heartbeat events per batched telemetry write.
	BatchSize int `yaml:"batch_size"`
	// FlushInterval is the longest time a heartbeat waits for its write batch.
	FlushInterval Duration `yaml:"flush_interval"`
}

// Snapshots configures device-workflow state persistence to the fleet database.
type Snapshots struct {
	// Interval is the cadence of periodic device-workflow state snapshots.
	Interval Duration `yaml:"interval"`
}

// RabbitMQ configures the telemetry broker.
type RabbitMQ struct {
	// URL is the RabbitMQ connection URL (amqp:// or amqps://).
	URL string `yaml:"url"`
}

// Temporal configures the orchestration backend.
type Temporal struct {
	// Address is the Temporal frontend address, in host:port form.
	Address string `yaml:"address"`
	// Namespace is the Temporal namespace hosting the FleetOps workflows.
	Namespace string `yaml:"namespace"`
	// TaskQueue is the task queue the workers poll.
	TaskQueue string `yaml:"task_queue"`
}

// Observability configures metrics, tracing, and probe endpoints.
type Observability struct {
	// OTelEndpoint is the OTLP collector endpoint in host:port form; empty disables trace export.
	OTelEndpoint string `yaml:"otel_endpoint"`
	// MetricsAddr is the Prometheus metrics listen address, in host:port form.
	MetricsAddr string `yaml:"metrics_addr"`
	// HealthAddr is the liveness/readiness listen address, in host:port form.
	HealthAddr string `yaml:"health_addr"`
}

// Defaults returns the configuration a file with no fields would produce: the local-stack
// values documented in deploy/config.yaml.
func Defaults() Config {
	return Config{
		Simulation: Simulation{FleetSize: 100},
		GRPC:       GRPC{ListenAddr: ":9090", ControlPlaneAddr: "localhost:9090"},
		MongoDB:    MongoDB{URI: "mongodb://localhost:27017", Database: "fleetops"},
		Liveness: Liveness{
			OfflineThreshold: Duration{Duration: 30 * time.Second},
			SweepInterval:    Duration{Duration: 10 * time.Second},
		},
		Telemetry: Telemetry{
			BatchSize:     500,
			FlushInterval: Duration{Duration: time.Second},
		},
		Snapshots: Snapshots{Interval: Duration{Duration: time.Minute}},
		RabbitMQ:  RabbitMQ{URL: "amqp://guest:guest@localhost:5672/"},
		Temporal:  Temporal{Address: "localhost:7233", Namespace: "default", TaskQueue: "fleetops"},
		Observability: Observability{
			OTelEndpoint: "localhost:4317",
			MetricsAddr:  ":9091",
			HealthAddr:   ":8081",
		},
	}
}

// Load returns the configuration at path: the defaults overlaid with every value the file
// sets, then validated as a whole. An empty path selects the defaults without reading any
// file. A named path must exist and decode strictly — unknown keys and malformed YAML are
// errors naming the offending input.
func Load(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		return cfg, cfg.Validate()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := decode(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %s: %w", path, err)
	}
	return cfg, nil
}

// decode overlays a YAML document onto cfg, rejecting keys that are not part of the schema.
func decode(data []byte, cfg *Config) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
