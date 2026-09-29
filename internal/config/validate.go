package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Validate checks every section of c — including sections the calling binary does not consume —
// and returns all violations joined, each error naming its field. Reasons are fixed strings and
// never echo the configured value, so a validation error can be logged without leaking
// credentials that a connection URI may carry.
func (c Config) Validate() error {
	var errs []error
	add := func(field, reason string) {
		errs = append(errs, fmt.Errorf("%s: %s", field, reason))
	}

	if c.Simulation.FleetSize <= 0 {
		add("simulation.fleet_size", fmt.Sprintf("must be positive, got %d", c.Simulation.FleetSize))
	}
	validateDuration(add, "simulation.apply_delay", c.Simulation.ApplyDelay)
	if !(c.Simulation.ApplySuccessRate >= 0 && c.Simulation.ApplySuccessRate <= 1) {
		// The closed-range check rejects NaN along with every out-of-range rate.
		add("simulation.apply_success_rate", "must be in [0, 1]")
	}
	validateHostPort(add, "grpc.listen_addr", c.GRPC.ListenAddr)
	validateHostPort(add, "grpc.control_plane_addr", c.GRPC.ControlPlaneAddr)
	validateScheme(add, "mongodb.uri", c.MongoDB.URI, "mongodb", "mongodb+srv")
	validateNonEmpty(add, "mongodb.database", c.MongoDB.Database)
	validateDuration(add, "liveness.offline_threshold", c.Liveness.OfflineThreshold)
	validateDuration(add, "liveness.sweep_interval", c.Liveness.SweepInterval)
	if c.Telemetry.BatchSize <= 0 {
		add("telemetry.batch_size", fmt.Sprintf("must be positive, got %d", c.Telemetry.BatchSize))
	}
	validateDuration(add, "telemetry.flush_interval", c.Telemetry.FlushInterval)
	validateDuration(add, "snapshots.interval", c.Snapshots.Interval)
	validateScheme(add, "rabbitmq.url", c.RabbitMQ.URL, "amqp", "amqps")
	if c.RabbitMQ.Prefetch <= 0 {
		add("rabbitmq.prefetch", fmt.Sprintf("must be positive, got %d", c.RabbitMQ.Prefetch))
	}
	if c.RabbitMQ.PublishBuffer <= 0 {
		add("rabbitmq.publish_buffer", fmt.Sprintf("must be positive, got %d", c.RabbitMQ.PublishBuffer))
	}
	if c.RabbitMQ.MaxAttempts <= 0 {
		add("rabbitmq.max_attempts", fmt.Sprintf("must be positive, got %d", c.RabbitMQ.MaxAttempts))
	}
	validateDuration(add, "rabbitmq.retry_base", c.RabbitMQ.RetryBase)
	validateDuration(add, "rabbitmq.retry_max", c.RabbitMQ.RetryMax)
	// A cap below the base would flatten the ladder; comparing only parsed values keeps a
	// malformed duration from reporting two violations for one field.
	if !c.RabbitMQ.RetryBase.invalid && !c.RabbitMQ.RetryMax.invalid &&
		c.RabbitMQ.RetryMax.Duration < c.RabbitMQ.RetryBase.Duration {
		add("rabbitmq.retry_max", "must not be below rabbitmq.retry_base")
	}
	validateDuration(add, "rabbitmq.queue_depth_interval", c.RabbitMQ.QueueDepthInterval)
	if !(c.Alerting.HealthThreshold >= 0 && c.Alerting.HealthThreshold <= 1) {
		// The closed-range check rejects NaN along with every out-of-range threshold.
		add("alerting.health_threshold", "must be in [0, 1]")
	}
	validateDuration(add, "rollout.health_window", c.Rollout.HealthWindow)
	if c.Rollout.MinSamples <= 0 {
		add("rollout.min_samples", fmt.Sprintf("must be positive, got %d", c.Rollout.MinSamples))
	}
	if !(c.Rollout.SampleHealthThreshold >= 0 && c.Rollout.SampleHealthThreshold <= 1) {
		// The closed-range check rejects NaN along with every out-of-range threshold.
		add("rollout.sample_health_threshold", "must be in [0, 1]")
	}
	if !(c.Rollout.MinSuccessRatio >= 0 && c.Rollout.MinSuccessRatio <= 1) {
		add("rollout.min_success_ratio", "must be in [0, 1]")
	}
	validateWaveSequence(add, "rollout.waves", c.Rollout.Waves)
	validateDuration(add, "rollout.decision_timeout", c.Rollout.DecisionTimeout)
	// A decision timeout below the window would decide a wave before its first measurement could
	// be taken. Comparing only parsed values keeps a malformed duration from reporting two
	// violations for one field.
	if !c.Rollout.HealthWindow.invalid && !c.Rollout.DecisionTimeout.invalid &&
		c.Rollout.DecisionTimeout.Duration < c.Rollout.HealthWindow.Duration {
		add("rollout.decision_timeout", "must not be below rollout.health_window")
	}
	validateHostPort(add, "temporal.address", c.Temporal.Address)
	validateNonEmpty(add, "temporal.namespace", c.Temporal.Namespace)
	validateNonEmpty(add, "temporal.task_queue", c.Temporal.TaskQueue)
	if c.Observability.OTelEndpoint != "" {
		validateHostPort(add, "observability.otel_endpoint", c.Observability.OTelEndpoint)
	}
	validateHostPort(add, "observability.metrics_addr", c.Observability.MetricsAddr)
	validateHostPort(add, "observability.health_addr", c.Observability.HealthAddr)

	return errors.Join(errs...)
}

// validateHostPort reports field unless value is a host:port pair with a numeric port.
func validateHostPort(add func(field, reason string), field, value string) {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		add(field, "must be a host:port address")
		return
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		add(field, "must be a host:port address with a numeric port")
	}
}

// validateScheme reports field unless value is a URI with one of the allowed schemes and a
// host. The reason is a fixed string because url.Parse errors echo the raw value.
func validateScheme(add func(field, reason string), field, value string, allowed ...string) {
	reject := func() {
		display := make([]string, len(allowed))
		for i, scheme := range allowed {
			display[i] = scheme + "://"
		}
		add(field, "must use scheme "+strings.Join(display, " or ")+" and name a host")
	}
	u, err := url.Parse(value)
	if err != nil {
		reject()
		return
	}
	for _, scheme := range allowed {
		if u.Scheme == scheme && u.Hostname() != "" {
			return
		}
	}
	reject()
}

// validateNonEmpty reports field if value is empty or only whitespace.
func validateNonEmpty(add func(field, reason string), field, value string) {
	if strings.TrimSpace(value) == "" {
		add(field, "must not be empty")
	}
}

// validateWaveSequence reports field unless waves is an ordered canary sequence: at least one
// entry, every share inside (0, 100], each share strictly above the one before it, and the last
// entry covering the whole pool so a completed rollout means the whole target group was reached.
// The first violation is the one reported: a broken sequence makes every later rule meaningless.
func validateWaveSequence(add func(field, reason string), field string, waves []Wave) {
	if len(waves) == 0 {
		add(field, "must not be empty")
		return
	}
	for i, wave := range waves {
		if wave.Percent <= 0 || wave.Percent > 100 {
			add(field, fmt.Sprintf("entry %d: percent must be in (0, 100], got %d", i, wave.Percent))
			return
		}
		if i > 0 && wave.Percent <= waves[i-1].Percent {
			add(field, fmt.Sprintf("entry %d: percent must be above the previous entry's %d",
				i, waves[i-1].Percent))
			return
		}
	}
	if last := waves[len(waves)-1].Percent; last != 100 {
		add(field, fmt.Sprintf("last entry: percent must be 100, got %d", last))
	}
}

// validateDuration reports field unless value is a parsed, positive duration. Reasons are
// fixed strings: an invalid duration never echoes the configured value.
func validateDuration(add func(field, reason string), field string, value Duration) {
	switch {
	case value.invalid:
		add(field, "must be a Go duration string")
	case value.Duration <= 0:
		add(field, "must be positive")
	}
}
