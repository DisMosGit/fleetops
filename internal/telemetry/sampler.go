package telemetry

import (
	"context"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueInspector reports the state of a queue. *amqp.Channel and every Session satisfy it.
type QueueInspector interface {
	// QueueDeclarePassive inspects an existing queue without creating it.
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
}

// QueueSampler keeps the queue-depth metric current: it samples every queue of the topology —
// work, retry, and dead-letter queues — at the configured cadence, so a growing dead-letter
// backlog is visible without inspecting the broker by hand.
type QueueSampler struct {
	topology   Topology
	interval   time.Duration
	metrics    *Metrics
	log        *slog.Logger
	supervisor *Supervisor
}

// NewQueueSampler returns a sampler of every queue in topology, sampling at interval.
func NewQueueSampler(url string, topology Topology, interval time.Duration, metrics *Metrics, log *slog.Logger) *QueueSampler {
	return &QueueSampler{
		topology:   topology,
		interval:   interval,
		metrics:    metrics,
		log:        log,
		supervisor: NewSupervisor(url, topology, log),
	}
}

// Run samples until ctx is done, reconnecting through the supervisor when the connection or
// channel is lost.
func (s *QueueSampler) Run(ctx context.Context) error {
	return s.supervisor.Run(ctx, "queue-depth", s.serve)
}

// serve samples on one live session: once on connect, so a starting process reports real depths
// before its first interval elapses, and then at the configured cadence.
func (s *QueueSampler) serve(ctx context.Context, sess Session) error {
	s.sample(ctx, sess)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sess.Lost():
			return errSessionLost
		case <-ticker.C:
			s.sample(ctx, sess)
		}
	}
}

// sample records the depth of every queue of the topology. A queue that cannot be sampled is
// logged and skipped: reporting a depth nobody measured would read as an empty backlog, so the
// last measured value stays until the next successful sample.
func (s *QueueSampler) sample(ctx context.Context, inspector QueueInspector) {
	for _, queue := range s.topology.Queues() {
		if ctx.Err() != nil {
			return
		}
		inspected, err := inspector.QueueDeclarePassive(queue.Name, true, false, false, false, nil)
		if err != nil {
			s.log.Error("sample queue depth", "queue", queue.Name, "err", err)
			continue
		}
		s.metrics.QueueDepth.WithLabelValues(queue.Name, string(queue.Kind)).Set(float64(inspected.Messages))
	}
}
