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
	// Alerting configures the heartbeat alerting consumer.
	Alerting Alerting `yaml:"alerting"`
	// Rollout configures canary wave health gating.
	Rollout Rollout `yaml:"rollout"`
	// Temporal configures the orchestration backend.
	Temporal Temporal `yaml:"temporal"`
	// Observability configures metrics, tracing, and probe endpoints.
	Observability Observability `yaml:"observability"`
}

// Simulation configures the emulated device fleet.
type Simulation struct {
	// FleetSize is the number of simulated devices one agent emulator process runs.
	FleetSize int `yaml:"fleet_size"`
	// ApplyDelay is the simulated duration of one firmware application.
	ApplyDelay Duration `yaml:"apply_delay"`
	// ApplySuccessRate is the probability a simulated firmware application succeeds, in [0, 1].
	ApplySuccessRate float64 `yaml:"apply_success_rate"`
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
	// Prefetch is the number of unacknowledged deliveries one consumer holds in flight.
	Prefetch int `yaml:"prefetch"`
	// PublishBuffer is the number of events buffered for publication before load shedding.
	PublishBuffer int `yaml:"publish_buffer"`
	// MaxAttempts is the number of processing attempts before a delivery is dead-lettered.
	MaxAttempts int `yaml:"max_attempts"`
	// RetryBase is the backoff delay of the first retry attempt.
	RetryBase Duration `yaml:"retry_base"`
	// RetryMax is the upper bound on a retry attempt's backoff delay.
	RetryMax Duration `yaml:"retry_max"`
	// QueueDepthInterval is the cadence of queue-depth sampling.
	QueueDepthInterval Duration `yaml:"queue_depth_interval"`
}

// Alerting configures the heartbeat alerting consumer.
type Alerting struct {
	// HealthThreshold is the health score below which a degradation alert is recorded.
	HealthThreshold float64 `yaml:"health_threshold"`
}

// Rollout configures a canary rollout: the sequence of waves it drives, how wide a window a
// wave's health is measured over, what counts as a successful sample, and when the measurement
// is evidence enough to decide with. The per-sample threshold is deliberately separate from
// alerting.health_threshold even though their defaults agree — alert sensitivity and the
// promote/rollback boundary are different decisions, and one shared value would let a change to
// alerting silently move the gate.
type Rollout struct {
	// HealthWindow is the width of the sliding window a wave's health is evaluated over.
	HealthWindow Duration `yaml:"health_window"`
	// SampleHealthThreshold is the health score at or above which a heartbeat counts as a success.
	SampleHealthThreshold float64 `yaml:"sample_health_threshold"`
	// MinSuccessRatio is the success ratio at or above which a decided window is healthy.
	MinSuccessRatio float64 `yaml:"min_success_ratio"`
	// MinSamples is the number of samples a window must hold before its verdict is decided.
	MinSamples int `yaml:"min_samples"`
	// Waves is the canary sequence a rollout drives, in order.
	Waves []Wave `yaml:"waves"`
	// DecisionTimeout is the longest a wave may stay undecided before its gate treats it as
	// unhealthy.
	DecisionTimeout Duration `yaml:"decision_timeout"`
	// ResultTimeout is the longest a wave waits for a device's reported update result before
	// that device counts as unreported and the wave stops waiting on it.
	ResultTimeout Duration `yaml:"result_timeout"`
}

// Wave is one entry of the canary sequence: the share of the rollout's eligible pool the wave
// covers and whether it may start only after an operator approved it. Shares are cumulative, so
// a wave targets the devices its share adds beyond the shares of the waves before it.
type Wave struct {
	// Percent is the wave's cumulative share of the rollout's eligible pool, in (0, 100].
	Percent int `yaml:"percent"`
	// RequireApproval reports whether the wave waits for an operator's approval before it starts.
	RequireApproval bool `yaml:"require_approval"`
}

// Temporal configures the orchestration backend.
type Temporal struct {
	// Address is the Temporal frontend address, in host:port form.
	Address string `yaml:"address"`
	// Namespace is the Temporal namespace hosting the FleetOps workflows.
	Namespace string `yaml:"namespace"`
	// TaskQueue is the task queue the workers poll.
	TaskQueue string `yaml:"task_queue"`
	// DispatchTaskQueue is the task queue the control plane polls for device command dispatch.
	// It is separate from TaskQueue because a Temporal task is delivered to any poller of its
	// queue rather than to one that registered its type, so a process must never poll a queue
	// carrying task types it does not host.
	DispatchTaskQueue string `yaml:"dispatch_task_queue"`
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
		Simulation: Simulation{
			FleetSize:        100,
			ApplyDelay:       Duration{Duration: time.Second},
			ApplySuccessRate: 1.0,
		},
		GRPC:    GRPC{ListenAddr: ":9090", ControlPlaneAddr: "localhost:9090"},
		MongoDB: MongoDB{URI: "mongodb://localhost:27017", Database: "fleetops"},
		Liveness: Liveness{
			OfflineThreshold: Duration{Duration: 30 * time.Second},
			SweepInterval:    Duration{Duration: 10 * time.Second},
		},
		Telemetry: Telemetry{
			BatchSize:     500,
			FlushInterval: Duration{Duration: time.Second},
		},
		Snapshots: Snapshots{Interval: Duration{Duration: time.Minute}},
		RabbitMQ: RabbitMQ{
			URL:                "amqp://guest:guest@localhost:5672/",
			Prefetch:           32,
			PublishBuffer:      1024,
			MaxAttempts:        3,
			RetryBase:          Duration{Duration: 5 * time.Second},
			RetryMax:           Duration{Duration: time.Minute},
			QueueDepthInterval: Duration{Duration: 15 * time.Second},
		},
		Alerting: Alerting{HealthThreshold: 0.6},
		Rollout: Rollout{
			HealthWindow:          Duration{Duration: 5 * time.Minute},
			SampleHealthThreshold: 0.6,
			MinSuccessRatio:       0.95,
			MinSamples:            10,
			// The demo canary: two waves an operator watches without being asked, then the
			// two that commit real capacity and wait for approval.
			Waves: []Wave{
				{Percent: 1},
				{Percent: 5},
				{Percent: 25, RequireApproval: true},
				{Percent: 100, RequireApproval: true},
			},
			DecisionTimeout: Duration{Duration: 30 * time.Minute},
			// The same width as the health window: a wave that is judged on the samples its
			// window gathered has, by then, given its devices as long to report as it
			// measured them.
			ResultTimeout: Duration{Duration: 5 * time.Minute},
		},
		Temporal: Temporal{
			Address:           "localhost:7233",
			Namespace:         "default",
			TaskQueue:         "fleetops",
			DispatchTaskQueue: "fleetops-controlplane",
		},
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
