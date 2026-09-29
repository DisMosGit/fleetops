package telemetry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

// callLog records the order in which two sinks were called.
type callLog struct {
	calls []string
}

// add appends one call.
func (l *callLog) add(entry string) { l.calls = append(l.calls, entry) }

// entries returns the recorded calls.
func (l *callLog) entries() []string { return append([]string(nil), l.calls...) }

// fakeIngest records heartbeats handed to the ingest half and can refuse them.
type fakeIngest struct {
	log *callLog
	err error
}

// Handle records the heartbeat and returns the scripted error.
func (f fakeIngest) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) error {
	f.log.add("ingest:" + hb.GetEventId())
	return f.err
}

// fakePublisher records heartbeats handed to the publication half.
type fakePublisher struct {
	log *callLog
}

// Handle records the heartbeat.
func (f fakePublisher) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) {
	f.log.add("publish:" + hb.GetEventId())
}

// TestFanoutStoresThenPublishes pins the order the two halves are called in: every published
// event has a durable counterpart.
func TestFanoutStoresThenPublishes(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	fanout := NewFanout(fakeIngest{log: log}, fakePublisher{log: log})
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}

	if err := fanout.Handle(context.Background(), heartbeat("ev-1", rec.ID, time.Now()), rec); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if got := strings.Join(log.entries(), ","); got != "ingest:ev-1,publish:ev-1" {
		t.Errorf("calls = %q, want the ingest write before the publication", got)
	}
}

// TestFanoutNeverPublishesARefusedHeartbeat pins that storage decides acceptance: a heartbeat
// the ingest path refuses is not published, and its error reaches the stream.
func TestFanoutNeverPublishesARefusedHeartbeat(t *testing.T) {
	t.Parallel()

	refusal := errors.New("ingest heartbeat: measurement time required")
	log := &callLog{}
	fanout := NewFanout(fakeIngest{log: log, err: refusal}, fakePublisher{log: log})
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}

	err := fanout.Handle(context.Background(), heartbeat("ev-1", rec.ID, time.Now()), rec)
	if !errors.Is(err, refusal) {
		t.Fatalf("Handle() error = %v, want the ingest refusal", err)
	}
	if got := strings.Join(log.entries(), ","); got != "ingest:ev-1" {
		t.Errorf("calls = %q, want no publication for a refused heartbeat", got)
	}
}

// TestFanoutNeverFailsAHeartbeatOnADroppedPublication pins the other half of the contract: the
// fan-out is derived, so shedding a publication never fails the heartbeat that storage accepted.
func TestFanoutNeverFailsAHeartbeatOnADroppedPublication(t *testing.T) {
	t.Parallel()

	metrics, _ := newTestMetrics()
	// A zero-capacity buffer sheds every event, standing in for a broker the publisher cannot
	// keep up with.
	publisher := newTestPublisher(0, metrics)
	log := &callLog{}
	fanout := NewFanout(fakeIngest{log: log}, publisher)
	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}

	if err := fanout.Handle(context.Background(), heartbeat("ev-1", rec.ID, time.Now()), rec); err != nil {
		t.Fatalf("Handle() error = %v, want the heartbeat accepted", err)
	}
	if got := strings.Join(log.entries(), ","); got != "ingest:ev-1" {
		t.Errorf("calls = %q, want the ingest write", got)
	}
	if got := testutil.ToFloat64(metrics.Dropped.WithLabelValues(HeartbeatEventType, dropReasonBufferFull)); got != 1 {
		t.Errorf("buffer-full drops = %v, want the shed publication counted", got)
	}
}
