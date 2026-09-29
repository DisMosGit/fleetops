package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// newTestMetrics returns pipeline metrics on their own registry, with the registry so a test can
// gather what the pipeline exposed.
func newTestMetrics() (*Metrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return NewMetrics(reg), reg
}

// lagFrom gathers a registry and returns the exposed lag of one consumer.
func lagFrom(t *testing.T, reg *prometheus.Registry, consumer string) float64 {
	t.Helper()
	for _, family := range gather(t, reg) {
		if family.GetName() != "fleetops_consumer_lag_events" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "consumer" && label.GetValue() == consumer {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("no fleetops_consumer_lag_events for consumer %s", consumer)
	return 0
}

// gather returns every metric family a registry exposes.
func gather(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	return families
}

// names returns the exposed metric family names.
func names(families []*dto.MetricFamily) []string {
	got := make([]string, 0, len(families))
	for _, family := range families {
		got = append(got, family.GetName())
	}
	return got
}

// labelValues returns the values one label takes across the metrics of a family.
func labelValues(families []*dto.MetricFamily, family, label string) []string {
	var got []string
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label {
					got = append(got, pair.GetValue())
				}
			}
		}
	}
	return got
}

// TestMetricsExposeThePipeline pins the pipeline's metric catalogue: every family is in the
// fleetops namespace, and the labels carry the queue kind, the drop reason, the consumer, and
// the outcome that make a scrape actionable.
func TestMetricsExposeThePipeline(t *testing.T) {
	t.Parallel()

	metrics, reg := newTestMetrics()
	metrics.QueueDepth.WithLabelValues(HeartbeatQueue, string(QueueKindWork)).Set(4)
	metrics.QueueDepth.WithLabelValues(HeartbeatQueue+".dlq", string(QueueKindDeadLetter)).Set(2)
	metrics.Published.WithLabelValues(HeartbeatEventType).Inc()
	metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonUnroutable).Inc()
	metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeProcessed).Inc()
	metrics.Consumed.WithLabelValues(HeartbeatQueue, outcomeDuplicate).Inc()
	metrics.RegisterLag(HeartbeatQueue, func() float64 { return 0 })

	families := gather(t, reg)
	want := []string{
		"fleetops_consumer_lag_events",
		"fleetops_events_consumed_total",
		"fleetops_events_dropped_total",
		"fleetops_events_published_total",
		"fleetops_queue_depth",
	}
	for _, name := range want {
		if !contains(names(families), name) {
			t.Errorf("metric %s missing from the exposition (have %v)", name, names(families))
		}
	}

	kinds := labelValues(families, "fleetops_queue_depth", "kind")
	for _, wantKind := range []string{string(QueueKindWork), string(QueueKindDeadLetter)} {
		if !contains(kinds, wantKind) {
			t.Errorf("queue depth kinds = %v, want %s among them", kinds, wantKind)
		}
	}
	if got := labelValues(families, "fleetops_events_dropped_total", "reason"); !contains(got, dropReasonUnroutable) {
		t.Errorf("drop reasons = %v, want %s among them", got, dropReasonUnroutable)
	}
	if got := labelValues(families, "fleetops_events_consumed_total", "consumer"); !contains(got, HeartbeatQueue) {
		t.Errorf("consumption consumers = %v, want %s among them", got, HeartbeatQueue)
	}
	outcomes := labelValues(families, "fleetops_events_consumed_total", "outcome")
	for _, wantOutcome := range []string{outcomeProcessed, outcomeDuplicate} {
		if !contains(outcomes, wantOutcome) {
			t.Errorf("consumption outcomes = %v, want %s among them", outcomes, wantOutcome)
		}
	}
}

// TestRegisterLagIsPerConsumerAndReadAtScrapeTime pins that lag is not a value someone has to
// remember to write: each consumer is reported under its own label and the number follows its
// producer between two scrapes.
func TestRegisterLagIsPerConsumerAndReadAtScrapeTime(t *testing.T) {
	t.Parallel()

	metrics, reg := newTestMetrics()
	var first, second float64
	metrics.RegisterLag("consumer-a", func() float64 { return first })
	metrics.RegisterLag("consumer-b", func() float64 { return second })

	first, second = 12, 0
	if got := lagFrom(t, reg, "consumer-a"); got != 12 {
		t.Errorf("consumer-a lag = %v, want 12", got)
	}
	if got := lagFrom(t, reg, "consumer-b"); got != 0 {
		t.Errorf("consumer-b lag = %v, want 0 — a caught-up consumer must not hide behind another", got)
	}

	second = 30
	if got := lagFrom(t, reg, "consumer-b"); got != 30 {
		t.Errorf("consumer-b lag = %v, want the value read at scrape time", got)
	}
	if got := lagFrom(t, reg, "consumer-a"); got != 12 {
		t.Errorf("consumer-a lag = %v, want its own value", got)
	}
}

// TestMetricReasonsAndOutcomesAreDistinct pins the label values the specs name, so a dashboard
// or an alert can rely on them.
func TestMetricReasonsAndOutcomesAreDistinct(t *testing.T) {
	t.Parallel()

	reasons := []string{
		dropReasonUnreachable,
		dropReasonBufferFull,
		dropReasonRejected,
		dropReasonUnroutable,
		dropReasonUnusable,
	}
	if got := len(unique(reasons)); got != len(reasons) {
		t.Errorf("drop reasons are not distinct: %v", reasons)
	}
	outcomes := []string{outcomeProcessed, outcomeDuplicate, outcomeRetry, outcomeDeadLetter}
	if got := len(unique(outcomes)); got != len(outcomes) {
		t.Errorf("consumption outcomes are not distinct: %v", outcomes)
	}
}

// contains reports whether values holds want.
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// unique returns the distinct values of a slice.
func unique(values []string) []string {
	seen := make(map[string]bool, len(values))
	var distinct []string
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		distinct = append(distinct, value)
	}
	return distinct
}
