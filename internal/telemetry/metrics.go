package telemetry

import "github.com/prometheus/client_golang/prometheus"

// metricsNamespace prefixes every metric this pipeline adds.
const metricsNamespace = "fleetops"

// Reasons an event could not be published, used as the reason label of Dropped.
const (
	// dropReasonUnreachable is a broker that could not be reached or a lost connection.
	dropReasonUnreachable = "unreachable"
	// dropReasonBufferFull is a full publish buffer shedding load.
	dropReasonBufferFull = "buffer_full"
	// dropReasonRejected is a broker-side rejection (a nack).
	dropReasonRejected = "rejected"
	// dropReasonUnroutable is a message no binding matched.
	dropReasonUnroutable = "unroutable"
	// dropReasonUnusable is an event that could not even be represented as a broker message.
	dropReasonUnusable = "unusable"
)

// Outcomes of one consumption, used as the outcome label of Consumed. Every delivery ends in
// exactly one of them.
const (
	// outcomeProcessed is a delivery whose side effect became durable.
	outcomeProcessed = "processed"
	// outcomeDuplicate is a delivery deduplication suppressed.
	outcomeDuplicate = "duplicate"
	// outcomeRetry is a delivery sent to the retry path.
	outcomeRetry = "retry"
	// outcomeDeadLetter is a delivery sent to the dead-letter path.
	outcomeDeadLetter = "dead_letter"
)

// Metrics is the telemetry pipeline's Prometheus source: how deep every queue is, how far
// behind each consumer is, and what happened to every event that entered the pipeline. It is
// registered on the registry the control plane already serves, so a scrape sees the pipeline
// alongside the metrics that were already there.
type Metrics struct {
	// QueueDepth reports the number of messages waiting on a queue, by queue name and kind
	// (work, retry, dead_letter).
	QueueDepth *prometheus.GaugeVec
	// Published counts events the broker confirmed, by event type.
	Published *prometheus.CounterVec
	// Dropped counts events that could not be published, by event type and reason.
	Dropped *prometheus.CounterVec
	// Consumed counts consumption outcomes by consumer and outcome.
	Consumed *prometheus.CounterVec

	registry prometheus.Registerer
}

// NewMetrics registers the pipeline collectors on reg. Registration is fatal on error: a
// duplicate registration is a wiring mistake, not a runtime condition.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "queue_depth",
			Help:      "Messages waiting on a FleetOps queue, by queue name and kind.",
		}, []string{"queue", "kind"}),
		Published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "events_published_total",
			Help:      "Events the broker confirmed as published, by event type.",
		}, []string{"event_type"}),
		Dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "events_dropped_total",
			Help:      "Events that could not be published, by event type and reason.",
		}, []string{"event_type", "reason"}),
		Consumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "events_consumed_total",
			Help:      "Consumption outcomes, by consumer and outcome.",
		}, []string{"consumer", "outcome"}),
		registry: reg,
	}
	reg.MustRegister(m.QueueDepth, m.Published, m.Dropped, m.Consumed)
	return m
}

// RegisterLag registers one consumer's lag gauge. Lag is read when the metrics endpoint is
// scraped rather than written on every event, so a consumer that falls behind while producing
// nothing of its own still reports a rising lag. Registration is fatal on error for the same
// reason as NewMetrics: two consumers sharing a name is a wiring mistake.
func (m *Metrics) RegisterLag(consumer string, lag func() float64) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Name:        "consumer_lag_events",
		Help:        "Events published after the last event the consumer processed.",
		ConstLabels: prometheus.Labels{"consumer": consumer},
	}, lag))
}
