package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
)

// newTestSampler returns a sampler of the heartbeat topology's queues with a discard logger and
// metrics of its own.
func newTestSampler(interval time.Duration) (*QueueSampler, *Metrics) {
	metrics, _ := newTestMetrics()
	topology := NewTopology(3, time.Second, time.Minute)
	return NewQueueSampler("amqp://guest:guest@localhost:5672/", topology, interval, metrics, slog.New(slog.DiscardHandler)), metrics
}

// TestSamplerReportsEveryQueueDepth pins that one sample records the depth of every queue of the
// layout, each under its own kind.
func TestSamplerReportsEveryQueueDepth(t *testing.T) {
	t.Parallel()

	sampler, metrics := newTestSampler(time.Minute)
	session := newDepthSession()
	depths := map[string]int{
		HeartbeatQueue:              3,
		HeartbeatQueue + ".retry.1": 1,
		HeartbeatQueue + ".retry.2": 0,
		HeartbeatQueue + ".dlq":     7,
		RolloutQueue:                2,
		RolloutQueue + ".retry.1":   0,
		RolloutQueue + ".retry.2":   0,
		RolloutQueue + ".dlq":       0,
	}
	for queue, depth := range depths {
		session.depths[queue] = depth
	}

	sampler.sample(context.Background(), session)

	for _, queue := range sampler.topology.Queues() {
		want := float64(depths[queue.Name])
		if got := testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(queue.Name, string(queue.Kind))); got != want {
			t.Errorf("queue %s depth = %v, want %v", queue.Name, got, want)
		}
	}
	if got := testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue+".dlq", string(QueueKindDeadLetter))); got != 7 {
		t.Errorf("dead-letter depth = %v, want the dead letters counted under kind dead_letter", got)
	}
}

// TestSamplerKeepsTheLastMeasuredDepthWhenTheBrokerFails pins the no-fabricated-zero rule: an
// unsampleable queue keeps its last real value and the failure is logged.
func TestSamplerKeepsTheLastMeasuredDepthWhenTheBrokerFails(t *testing.T) {
	t.Parallel()

	var logged strings.Builder
	metrics, _ := newTestMetrics()
	topology := NewTopology(3, time.Second, time.Minute)
	sampler := NewQueueSampler(
		"amqp://guest:guest@localhost:5672/",
		topology,
		time.Minute,
		metrics,
		slog.New(slog.NewJSONHandler(&logged, nil)),
	)

	healthy := newDepthSession()
	healthy.depths[HeartbeatQueue+".dlq"] = 5
	sampler.sample(context.Background(), healthy)
	if got := testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue+".dlq", string(QueueKindDeadLetter))); got != 5 {
		t.Fatalf("dead-letter depth = %v, want the measured 5", got)
	}

	// A broker that cannot be queried must not report zero: the last measured value stays.
	broken := &unreachableInspector{}
	sampler.sample(context.Background(), broken)

	if got := testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue+".dlq", string(QueueKindDeadLetter))); got != 5 {
		t.Errorf("dead-letter depth after a failed sample = %v, want the last measured 5", got)
	}
	if got := len(broken.attempted); got != len(topology.Queues()) {
		t.Errorf("attempted samples = %d, want every queue attempted (%d)", got, len(topology.Queues()))
	}
	entries := strings.Split(strings.TrimSpace(logged.String()), "\n")
	for _, entry := range entries {
		var record map[string]any
		if err := json.Unmarshal([]byte(entry), &record); err != nil {
			t.Fatalf("decode log line %q: %v", entry, err)
		}
		if record["level"] != "ERROR" || record["msg"] != "sample queue depth" {
			t.Errorf("log line = %v, want an ERROR naming the failed sample", record)
		}
	}
}

// TestSamplerServeSamplesOnConnectAndStopsOnLoss pins the loop: a starting process reports real
// depths immediately, and a lost session ends the loop for the supervisor to reconnect.
func TestSamplerServeSamplesOnConnectAndStopsOnLoss(t *testing.T) {
	t.Parallel()

	sampler, metrics := newTestSampler(time.Hour)
	session := newDepthSession()
	session.depths[HeartbeatQueue] = 6

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sampler.serve(ctx, session) }()

	deadline := time.Now().Add(5 * time.Second)
	for testutil.ToFloat64(metrics.QueueDepth.WithLabelValues(HeartbeatQueue, string(QueueKindWork))) != 6 {
		if time.Now().After(deadline) {
			t.Fatal("the sampler never reported the initial depth")
		}
		time.Sleep(time.Millisecond)
	}

	close(session.lost)
	select {
	case err := <-done:
		if err == nil {
			t.Error("serve() = nil, want the lost session reported")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve() did not return after the session was lost")
	}

	cancel()
}

// depthSession is a scripted Session that reports the depth of every queue and can lose its
// channel, so the sampler is unit-testable without a broker.
type depthSession struct {
	stubSession
	depths map[string]int
}

// newDepthSession returns a live depth-reporting session.
func newDepthSession() *depthSession {
	return &depthSession{stubSession: *newStubSession(), depths: make(map[string]int)}
}

// QueueDeclarePassive reports the depth scripted for a queue.
func (s *depthSession) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name, Messages: s.depths[name]}, nil
}

// TestSamplerStopsWhenContextIsDone pins the ctx-owned stop condition.
func TestSamplerStopsWhenContextIsDone(t *testing.T) {
	t.Parallel()

	sampler, _ := newTestSampler(time.Hour)
	session := newDepthSession()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.serve(ctx, session) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve() error = %v, want nil on shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve() did not return after ctx was cancelled")
	}
}

// errInspectorUnavailable is the failure an inspector reports when the broker cannot answer.
var errInspectorUnavailable = errors.New("broker unavailable")

// unreachableInspector is a broker that cannot answer a queue inspection.
type unreachableInspector struct {
	attempted []string
}

// QueueDeclarePassive fails without reporting a depth.
func (u *unreachableInspector) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	u.attempted = append(u.attempted, name)
	return amqp.Queue{}, errInspectorUnavailable
}
