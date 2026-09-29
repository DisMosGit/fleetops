package telemetry

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	amqp "github.com/rabbitmq/amqp091-go"
)

// recordingDeclarer is a hand-written declarer that records what a topology declaration asked
// the broker for, so the layout is asserted without a broker. failAt makes the nth call fail
// with err.
type recordingDeclarer struct {
	steps  []string
	failAt int
	err    error
	calls  int
}

// fail makes the nth declaration call fail.
func (r *recordingDeclarer) fail(n int, err error) *recordingDeclarer {
	r.failAt = n
	r.err = err
	return r
}

func (r *recordingDeclarer) record(step string) error {
	r.calls++
	r.steps = append(r.steps, step)
	if r.failAt == r.calls {
		return r.err
	}
	return nil
}

func (r *recordingDeclarer) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return r.record(fmt.Sprintf("exchange %s %s durable=%t autoDelete=%t internal=%t noWait=%t args=%v",
		name, kind, durable, autoDelete, internal, noWait, args))
}

func (r *recordingDeclarer) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	err := r.record(fmt.Sprintf("queue %s durable=%t autoDelete=%t exclusive=%t noWait=%t args=%v",
		name, durable, autoDelete, exclusive, noWait, args))
	return amqp.Queue{Name: name}, err
}

func (r *recordingDeclarer) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return r.record(fmt.Sprintf("bind %s <- %s key=%s noWait=%t args=%v", name, exchange, key, noWait, args))
}

// retrySteps returns the recorded retry-queue declarations and their TTLs.
func (r *recordingDeclarer) retryTTLs() map[string]string {
	ttls := make(map[string]string)
	for _, step := range r.steps {
		if !strings.HasPrefix(step, "queue ") || !strings.Contains(step, ".retry.") {
			continue
		}
		name := strings.Fields(step)[1]
		start := strings.Index(step, "x-message-ttl:")
		if start < 0 {
			ttls[name] = "missing"
			continue
		}
		rest := step[start+len("x-message-ttl:"):]
		ttls[name] = strings.TrimSuffix(strings.Fields(rest)[0], "]")
	}
	return ttls
}

// TestDeclareLayout pins the default layout: the three exchanges, both work queues with their
// dead-letter arguments, every binding, the per-attempt retry queues with their TTLs, and the
// dead-letter queues.
func TestDeclareLayout(t *testing.T) {
	t.Parallel()

	topology := NewTopology(3, 5*time.Second, time.Minute)
	recorder := &recordingDeclarer{}
	if err := topology.declare(recorder); err != nil {
		t.Fatalf("declare() error = %v", err)
	}

	want := []string{
		"exchange fleetops.events topic durable=true autoDelete=false internal=false noWait=false args=map[]",
		"exchange fleetops.retry direct durable=true autoDelete=false internal=false noWait=false args=map[]",
		"exchange fleetops.dead-letter direct durable=true autoDelete=false internal=false noWait=false args=map[]",

		"queue fleetops.heartbeat.alerting durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.dead-letter x-dead-letter-routing-key:fleetops.heartbeat.alerting.dlq]",
		"bind fleetops.heartbeat.alerting <- fleetops.events key=heartbeat.# noWait=false args=map[]",
		"bind fleetops.heartbeat.alerting <- fleetops.events key=retry.fleetops.heartbeat.alerting noWait=false args=map[]",
		"queue fleetops.heartbeat.alerting.retry.1 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.heartbeat.alerting x-message-ttl:5000]",
		"bind fleetops.heartbeat.alerting.retry.1 <- fleetops.retry key=fleetops.heartbeat.alerting.retry.1 noWait=false args=map[]",
		"queue fleetops.heartbeat.alerting.retry.2 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.heartbeat.alerting x-message-ttl:10000]",
		"bind fleetops.heartbeat.alerting.retry.2 <- fleetops.retry key=fleetops.heartbeat.alerting.retry.2 noWait=false args=map[]",
		"queue fleetops.heartbeat.alerting.dlq durable=true autoDelete=false exclusive=false noWait=false args=map[]",
		"bind fleetops.heartbeat.alerting.dlq <- fleetops.dead-letter key=fleetops.heartbeat.alerting.dlq noWait=false args=map[]",

		"queue fleetops.rollout.tasks durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.dead-letter x-dead-letter-routing-key:fleetops.rollout.tasks.dlq]",
		"bind fleetops.rollout.tasks <- fleetops.events key=rollout.task.# noWait=false args=map[]",
		"bind fleetops.rollout.tasks <- fleetops.events key=retry.fleetops.rollout.tasks noWait=false args=map[]",
		"queue fleetops.rollout.tasks.retry.1 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.rollout.tasks x-message-ttl:5000]",
		"bind fleetops.rollout.tasks.retry.1 <- fleetops.retry key=fleetops.rollout.tasks.retry.1 noWait=false args=map[]",
		"queue fleetops.rollout.tasks.retry.2 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.rollout.tasks x-message-ttl:10000]",
		"bind fleetops.rollout.tasks.retry.2 <- fleetops.retry key=fleetops.rollout.tasks.retry.2 noWait=false args=map[]",
		"queue fleetops.rollout.tasks.dlq durable=true autoDelete=false exclusive=false noWait=false args=map[]",
		"bind fleetops.rollout.tasks.dlq <- fleetops.dead-letter key=fleetops.rollout.tasks.dlq noWait=false args=map[]",

		// The notification queue is declared and bound with the rest of the layout, before any
		// consumer exists: a published notification has to be routable from the moment the
		// topology is declared.
		"queue fleetops.rollout.notifications durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.dead-letter x-dead-letter-routing-key:fleetops.rollout.notifications.dlq]",
		"bind fleetops.rollout.notifications <- fleetops.events key=rollout.notification.# noWait=false args=map[]",
		"bind fleetops.rollout.notifications <- fleetops.events key=retry.fleetops.rollout.notifications noWait=false args=map[]",
		"queue fleetops.rollout.notifications.retry.1 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.rollout.notifications x-message-ttl:5000]",
		"bind fleetops.rollout.notifications.retry.1 <- fleetops.retry key=fleetops.rollout.notifications.retry.1 noWait=false args=map[]",
		"queue fleetops.rollout.notifications.retry.2 durable=true autoDelete=false exclusive=false noWait=false " +
			"args=map[x-dead-letter-exchange:fleetops.events x-dead-letter-routing-key:retry.fleetops.rollout.notifications x-message-ttl:10000]",
		"bind fleetops.rollout.notifications.retry.2 <- fleetops.retry key=fleetops.rollout.notifications.retry.2 noWait=false args=map[]",
		"queue fleetops.rollout.notifications.dlq durable=true autoDelete=false exclusive=false noWait=false args=map[]",
		"bind fleetops.rollout.notifications.dlq <- fleetops.dead-letter key=fleetops.rollout.notifications.dlq noWait=false args=map[]",
	}
	if diff := cmp.Diff(want, recorder.steps); diff != "" {
		t.Errorf("declared layout mismatch (-want +got):\n%s", diff)
	}
}

// TestDeclareRetryLadder pins the backoff ladder per attempt count: retry queues exist for
// attempts 1..maxAttempts-1, and each queue's TTL is the documented delay.
func TestDeclareRetryLadder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxAttempts int
		wantTTLs    map[string]string
	}{
		{
			name:        "one attempt has no retry queue",
			maxAttempts: 1,
			wantTTLs:    map[string]string{},
		},
		{
			name:        "three attempts double from the base",
			maxAttempts: 3,
			wantTTLs: map[string]string{
				HeartbeatQueue + ".retry.1":           "5000",
				HeartbeatQueue + ".retry.2":           "10000",
				RolloutQueue + ".retry.1":             "5000",
				RolloutQueue + ".retry.2":             "10000",
				RolloutNotificationQueue + ".retry.1": "5000",
				RolloutNotificationQueue + ".retry.2": "10000",
			},
		},
		{
			name:        "five attempts keep doubling",
			maxAttempts: 5,
			wantTTLs: map[string]string{
				HeartbeatQueue + ".retry.1":           "5000",
				HeartbeatQueue + ".retry.2":           "10000",
				HeartbeatQueue + ".retry.3":           "20000",
				HeartbeatQueue + ".retry.4":           "40000",
				RolloutQueue + ".retry.1":             "5000",
				RolloutQueue + ".retry.2":             "10000",
				RolloutQueue + ".retry.3":             "20000",
				RolloutQueue + ".retry.4":             "40000",
				RolloutNotificationQueue + ".retry.1": "5000",
				RolloutNotificationQueue + ".retry.2": "10000",
				RolloutNotificationQueue + ".retry.3": "20000",
				RolloutNotificationQueue + ".retry.4": "40000",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topology := NewTopology(tc.maxAttempts, 5*time.Second, time.Minute)
			recorder := &recordingDeclarer{}
			if err := topology.declare(recorder); err != nil {
				t.Fatalf("declare() error = %v", err)
			}
			if diff := cmp.Diff(tc.wantTTLs, recorder.retryTTLs()); diff != "" {
				t.Errorf("retry ladder mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestRetryDelay pins the delay schedule and its cap.
func TestRetryDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     time.Duration
		max      time.Duration
		attempts []time.Duration // delays of attempts 1..n
	}{
		{
			name:     "unreachable cap keeps doubling",
			base:     5 * time.Second,
			max:      time.Hour,
			attempts: []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second},
		},
		{
			name:     "cap shortens the last attempts",
			base:     5 * time.Second,
			max:      12 * time.Second,
			attempts: []time.Duration{5 * time.Second, 10 * time.Second, 12 * time.Second, 12 * time.Second},
		},
		{
			name:     "cap equal to the base flattens the ladder",
			base:     3 * time.Second,
			max:      3 * time.Second,
			attempts: []time.Duration{3 * time.Second, 3 * time.Second},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topology := NewTopology(len(tc.attempts)+1, tc.base, tc.max)
			for i, want := range tc.attempts {
				attempt := i + 1
				if got := topology.RetryDelay(attempt); got != want {
					t.Errorf("RetryDelay(%d) = %s, want %s", attempt, got, want)
				}
			}
		})
	}
}

// TestTopologyNames pins the queue names, kinds, and routing keys the layout is addressed by.
func TestTopologyNames(t *testing.T) {
	t.Parallel()

	topology := NewTopology(3, 5*time.Second, time.Minute)

	wantQueues := []Queue{
		{Name: "fleetops.heartbeat.alerting", Kind: QueueKindWork},
		{Name: "fleetops.heartbeat.alerting.retry.1", Kind: QueueKindRetry},
		{Name: "fleetops.heartbeat.alerting.retry.2", Kind: QueueKindRetry},
		{Name: "fleetops.heartbeat.alerting.dlq", Kind: QueueKindDeadLetter},
		{Name: "fleetops.rollout.tasks", Kind: QueueKindWork},
		{Name: "fleetops.rollout.tasks.retry.1", Kind: QueueKindRetry},
		{Name: "fleetops.rollout.tasks.retry.2", Kind: QueueKindRetry},
		{Name: "fleetops.rollout.tasks.dlq", Kind: QueueKindDeadLetter},
		{Name: "fleetops.rollout.notifications", Kind: QueueKindWork},
		{Name: "fleetops.rollout.notifications.retry.1", Kind: QueueKindRetry},
		{Name: "fleetops.rollout.notifications.retry.2", Kind: QueueKindRetry},
		{Name: "fleetops.rollout.notifications.dlq", Kind: QueueKindDeadLetter},
	}
	if diff := cmp.Diff(wantQueues, topology.Queues()); diff != "" {
		t.Errorf("Queues() mismatch (-want +got):\n%s", diff)
	}

	heartbeat, err := topology.WorkQueue(HeartbeatQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", HeartbeatQueue, err)
	}
	if heartbeat.EventType != HeartbeatEventType {
		t.Errorf("heartbeat queue event type = %q, want %q", heartbeat.EventType, HeartbeatEventType)
	}
	if diff := cmp.Diff([]string{"heartbeat.#", "retry.fleetops.heartbeat.alerting"}, heartbeat.Keys); diff != "" {
		t.Errorf("heartbeat queue bindings mismatch (-want +got):\n%s", diff)
	}
	if _, err := topology.WorkQueue("fleetops.unknown"); err == nil {
		t.Error("WorkQueue(unknown) = nil error, want an error naming the queue")
	}

	if got := HeartbeatRoutingKey("eu-west", "v3"); got != "heartbeat.eu-west.v3" {
		t.Errorf("HeartbeatRoutingKey() = %q, want heartbeat.eu-west.v3", got)
	}
	if got := RolloutRoutingKey("start"); got != "rollout.task.start" {
		t.Errorf("RolloutRoutingKey() = %q, want rollout.task.start", got)
	}

	notifications, err := topology.WorkQueue(RolloutNotificationQueue)
	if err != nil {
		t.Fatalf("WorkQueue(%s) error = %v", RolloutNotificationQueue, err)
	}
	if notifications.EventType != RollbackEventType {
		t.Errorf("notification queue event type = %q, want %q",
			notifications.EventType, RollbackEventType)
	}
	wantBindings := []string{
		"rollout.notification.#",
		"retry." + RolloutNotificationQueue,
	}
	if diff := cmp.Diff(wantBindings, notifications.Keys); diff != "" {
		t.Errorf("notification queue bindings mismatch (-want +got):\n%s", diff)
	}
	// Two phases, two keys, both inside the notification family and inside the rollback kind, so
	// a consumer narrows to one phase, to every rollback announcement, or to every rollout
	// notification with a binding alone.
	wantKeys := map[RollbackPhase]string{
		RollbackStarted:   "rollout.notification.rollback.started",
		RollbackCompleted: "rollout.notification.rollback.completed",
	}
	for phase, want := range wantKeys {
		if got := RollbackRoutingKey(phase); got != want {
			t.Errorf("RollbackRoutingKey(%s) = %q, want %q", phase, got, want)
		}
		if !strings.HasPrefix(want, strings.TrimSuffix(notificationFamilyKey, "#")) {
			t.Errorf("rollback key %q is outside the notification family %q", want, notificationFamilyKey)
		}
		if !strings.HasPrefix(want, strings.TrimSuffix(rollbackNotificationsKey, "#")) {
			t.Errorf("rollback key %q is outside the rollback kind %q", want, rollbackNotificationsKey)
		}
	}
	if got := topology.RetryKey(HeartbeatQueue); got != "retry."+HeartbeatQueue {
		t.Errorf("RetryKey() = %q, want retry.%s", got, HeartbeatQueue)
	}
}

// TestDeclareNamesTheFailingElement proves a refused declaration reports which element was
// refused, so a conflicting broker element cannot hide behind a generic error.
func TestDeclareNamesTheFailingElement(t *testing.T) {
	t.Parallel()

	conflict := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg 'x-message-ttl'"}
	tests := []struct {
		name    string
		failAt  int
		wantErr string
	}{
		{name: "exchange", failAt: 2, wantErr: RetryExchange},
		{name: "work queue", failAt: 4, wantErr: HeartbeatQueue},
		{name: "binding", failAt: 5, wantErr: "bind queue " + HeartbeatQueue},
		{name: "retry queue", failAt: 7, wantErr: HeartbeatQueue + ".retry.1"},
		{name: "dead-letter queue", failAt: 11, wantErr: HeartbeatQueue + ".dlq"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := (&recordingDeclarer{}).fail(tc.failAt, conflict)
			err := NewTopology(3, 5*time.Second, time.Minute).declare(recorder)
			if err == nil {
				t.Fatal("declare() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("declare() error = %q, want it to name %q", err, tc.wantErr)
			}
			if !errors.Is(err, conflict) {
				t.Errorf("declare() error = %v, want it to wrap the broker error", err)
			}
		})
	}
}

// TestDeclareRefusesUnrepresentableDelay proves a retry delay the broker's millisecond TTL
// cannot express fails loudly instead of being truncated.
func TestDeclareRefusesUnrepresentableDelay(t *testing.T) {
	t.Parallel()

	topology := NewTopology(2, 100*24*time.Hour, 100*24*time.Hour)
	err := topology.declare(&recordingDeclarer{})
	if err == nil {
		t.Fatal("declare() = nil, want an error")
	}
	if !strings.Contains(err.Error(), strconv.Itoa(math.MaxInt32)) {
		t.Errorf("declare() error = %q, want it to name the millisecond limit", err)
	}
}
