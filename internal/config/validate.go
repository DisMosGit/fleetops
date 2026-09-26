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
