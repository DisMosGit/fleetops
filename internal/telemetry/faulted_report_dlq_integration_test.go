//go:build integration

package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// The two ladders this test declares: the attempts one device's download is allowed, and the
// attempts the queue's consumer is given before an event is dead-lettered. Three attempts with a
// 200ms first backoff keep both observable without a long test.
const (
	faultedFetchBudget   = 3
	faultedQueueAttempts = 3
	faultedRetryBase     = 200 * time.Millisecond
	faultedRetryMax      = time.Second

	faultedDeviceID   = "dev-faulted"
	faultedFirmwareID = "fw-faulted"
)

// faultedReport is what one faulted device reported: the operator-safe detail its terminal result
// carried, and how many of its download attempts the budget paid for.
type faultedReport struct {
	detail    string
	downloads int
}

// TestFaultedReportReachesTheDeadLetterQueue verifies the two terminal behaviors the fault path
// exists to demonstrate, one after the other. A device whose download faults on every attempt of
// its budget reports exactly one terminal failed result carrying the operator-safe detail the
// fault produced — and nothing else: no fetch the fault did not take, no apply. The rollback
// announcement such a failure ends a rollout with is then handed to the heartbeat work queue,
// whose consumer is the fleet's real one and handles heartbeat events only, so an event that
// consumer cannot process walks that queue's retry ladder and lands on its dead-letter queue with
// its failure reason and the attempts made, instead of being dropped or requeued forever.
func TestFaultedReportReachesTheDeadLetterQueue(t *testing.T) {
	t.Parallel()

	report := driveFaultedUpdate(t)

	// The handler's store is a database of its own, so the test can assert the announcement it
	// refuses was refused before any write.
	alerts := mongotest.For(t)
	fixture := startConsumer(t, faultedQueueAttempts, faultedRetryBase, faultedRetryMax,
		NewAlerting(alerts.DB.Collection("device_alerts"), 0.6))
	ch := fixture.rabbit.Channel(t)
	dlq := fixture.topology.DeadLetterQueue(HeartbeatQueue)

	announcement := faultedRollback(report)
	purgeMeasuredQueues(t, fixture)
	start := time.Now()
	// The announcement is published into the heartbeat work queue's key space: an event family the
	// fleet's only running consumer does not handle. The notification queue the announcement
	// belongs to is declared without a consumer, so what the requirement's second half covers is
	// the terminal behavior of the queue that does have one.
	fixture.publisher.publishAnnouncement(t, announcement, HeartbeatRoutingKey("eu-west", "v3"))

	awaitOutcome(t, fixture, outcomeRetry, faultedQueueAttempts-1)
	awaitOutcome(t, fixture, outcomeDeadLetter, 1)
	elapsed := time.Since(start)
	// The first retry waits 200ms and the second 400ms: anything faster means the ladder did not
	// hold the event for its backoff.
	if minimum := 600 * time.Millisecond; elapsed < minimum {
		t.Errorf("retry ladder took %s, want at least %s", elapsed, minimum)
	}

	awaitDepth(t, fixture, ch, dlq, 1)
	dead := nextDelivery(t, fixture.rabbit, dlq)
	if got := dead.MessageId; got != announcement.EventID {
		t.Errorf("dead letter message id = %q, want %q", got, announcement.EventID)
	}
	if got := attemptOf(dead); got != faultedQueueAttempts {
		t.Errorf("dead letter attempt = %d, want the attempts made (%d)", got, faultedQueueAttempts)
	}
	reason, _ := dead.Headers[deadLetterReasonHeader].(string)
	if !strings.Contains(reason, "does not handle event type "+RollbackEventType) {
		t.Errorf("dead letter reason = %q, want the consumer's refusal", reason)
	}
	decoded, err := DecodeRollbackEvent(dead.Body)
	if err != nil {
		t.Fatalf("decode the dead-lettered announcement: %v", err)
	}
	if diff := cmp.Diff(announcement, decoded); diff != "" {
		t.Errorf("dead-lettered announcement (-want +got):\n%s", diff)
	}
	if decoded.Progress == nil || len(decoded.Progress.Steps) != 1 {
		t.Fatalf("dead-lettered announcement progress = %+v, want the one compensating step", decoded.Progress)
	}
	if got := decoded.Progress.Steps[0].Detail; got != report.detail {
		t.Errorf("dead-lettered step detail = %q, want the faulted report's detail %q", got, report.detail)
	}
	// The claim the consumer made before it could not apply the event stays unfinished, so a
	// redelivery would try again rather than skip it as processed.
	if record, ok := ledgerDoc(t, fixture, announcement.EventID); !ok || record.ProcessedAt != nil {
		t.Errorf("ledger record for the dead-lettered announcement = %+v (present %t), want an unfinished claim", record, ok)
	}
	if got := countDocuments(t, alerts, "device_alerts"); got != 0 {
		t.Errorf("alert documents = %d, want the refused announcement to have written none", got)
	}
}

// driveFaultedUpdate drives one update command through the real fault path: a seeded injector that
// stalls every download attempt, decorating a fetcher a faulted attempt must never reach, under
// the attempt budget a device is allowed.
func driveFaultedUpdate(t *testing.T) faultedReport {
	t.Helper()

	base := &unreachedFetcher{}
	fetcher, err := agent.NewFaultFetcher(base, agent.FaultConfig{
		Share:         1,
		Kinds:         []agent.FaultKind{agent.FaultStall},
		StallDuration: 20 * time.Millisecond,
	}, rand.NewSource(1))
	if err != nil {
		t.Fatalf("NewFaultFetcher() error = %v", err)
	}
	status := &recordedStatus{}
	results := &recordedResults{}
	applier := &unreachedApplier{}
	handler := agent.NewUpdateHandlerWithRetry(
		fetcher, applier, unreachedAdopter{},
		agent.NewUpdateReporter(status, results),
		agent.FetchRetry{Attempts: faultedFetchBudget, Max: faultedRetryMax},
		slog.New(slog.DiscardHandler),
	)
	cmd := &agentv1.Command{
		CommandId: "cmd-faulted",
		DeviceId:  faultedDeviceID,
		Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
			FirmwareId: faultedFirmwareID, Version: "2.0.0", Checksum: "sum",
		}},
	}
	if err := handler.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("Handle() error = %v, want the failed update reported rather than returned", err)
	}

	reports := results.requests()
	if len(reports) != 1 {
		t.Fatalf("terminal results = %d, want exactly one", len(reports))
	}
	if got := reports[0].GetOutcome(); got != agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED {
		t.Errorf("terminal outcome = %v, want %v", got, agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED)
	}
	detail := reports[0].GetDetail()
	if want := agent.ErrStall.Error(); detail != want {
		t.Errorf("terminal detail = %q, want the injected stall's detail %q", detail, want)
	}

	phases := status.requests()
	if len(phases) == 0 {
		t.Fatal("no update phase was reported")
	}
	if got := phases[len(phases)-1].GetPhase(); got != agentv1.UpdatePhase_UPDATE_PHASE_FAILED {
		t.Errorf("last reported phase = %v, want %v", got, agentv1.UpdatePhase_UPDATE_PHASE_FAILED)
	}
	downloads := 0
	for _, req := range phases {
		if req.GetPhase() == agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING {
			downloads++
		}
	}
	if downloads != faultedFetchBudget {
		t.Errorf("download-phase reports = %d, want the whole attempt budget (%d)", downloads, faultedFetchBudget)
	}
	if got := applier.callCount(); got != 0 {
		t.Errorf("apply calls = %d, want none after a download that never succeeded", got)
	}
	if got := base.callCount(); got != 0 {
		t.Errorf("decorated fetcher calls = %d, want none: every attempt faulted", got)
	}
	return faultedReport{detail: detail, downloads: downloads}
}

// faultedRollback returns the announcement one faulted device's failure ends a rollout with: the
// device is one the rollback could not restore, and the step that could not restore it carries the
// detail the device reported.
func faultedRollback(report faultedReport) RollbackEvent {
	event := NewRollbackEvent("rollout-faulted", RollbackCompleted, time.Now().UTC().Truncate(time.Millisecond))
	event.Rollout = RolloutAnnouncement{
		RolloutID:       "rollout-faulted",
		FirmwareID:      faultedFirmwareID,
		FirmwareVersion: "2.0.0",
		Region:          "eu-west",
		Model:           "v3",
		WaveID:          "wave-1",
		Outcome:         "wave failed",
		PlanSteps:       1,
		PlanDevices:     1,
	}
	event.Progress = &RollbackProgress{
		Failed:              1,
		UnrestoredDeviceIDs: []string{faultedDeviceID},
		Steps: []RollbackStepOutcome{{
			Kind: "downgrade", WaveID: "wave-1", Status: "failed", Devices: 1, Failed: 1,
			Detail: report.detail,
		}},
	}
	return event
}

// publishAnnouncement sends one rollback announcement the way a publisher bound to the events
// exchange would: persistently, under routingKey, with the mandatory flag set so an unroutable
// event fails the test instead of waiting for a message that went nowhere.
func (p *brokerPublisher) publishAnnouncement(t *testing.T, event RollbackEvent, routingKey string) {
	t.Helper()
	msg, err := event.Message(routingKey)
	if err != nil {
		t.Fatalf("build announcement %s: %v", event.EventID, err)
	}
	if err := p.ch.PublishWithContext(context.Background(), EventsExchange, routingKey, true, false, msg); err != nil {
		t.Fatalf("publish announcement %s: %v", event.EventID, err)
	}
	p.assertRouted(t, event.EventID)
}

// unreachedFetcher is the fetcher a fault injector decorates: a faulted attempt never reaches it,
// so every call it records is a defect the test asserts on.
type unreachedFetcher struct {
	mu    sync.Mutex
	calls int
}

// Fetch records the call it should never receive.
func (f *unreachedFetcher) Fetch(context.Context, string, string, string, func(int32)) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return "/tmp/never-reached", nil
}

// callCount returns how often the decorated fetcher was reached.
func (f *unreachedFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// unreachedApplier is the apply step of a device whose download never succeeded.
type unreachedApplier struct {
	mu    sync.Mutex
	calls int
}

// Apply records the call it should never receive.
func (a *unreachedApplier) Apply(context.Context, string, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return nil
}

// callCount returns how often the apply step was reached.
func (a *unreachedApplier) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// unreachedAdopter is the adoption step of a device whose update never applied. It fails loudly, so
// an adoption after a failed download cannot pass as a successful update.
type unreachedAdopter struct{}

// SetFirmware reports that no adoption was possible.
func (unreachedAdopter) SetFirmware(string, string) error {
	return errors.New("adopt: the update never applied")
}

// recordedStatus records the update phases a handler reported.
type recordedStatus struct {
	mu   sync.Mutex
	reqs []*agentv1.UpdateStatusRequest
}

// ReportUpdateStatus records one phase and accepts it.
func (s *recordedStatus) ReportUpdateStatus(_ context.Context, req *agentv1.UpdateStatusRequest) (*agentv1.UpdateStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, req)
	return &agentv1.UpdateStatusResponse{Accepted: true}, nil
}

// requests returns the recorded phases, oldest first.
func (s *recordedStatus) requests() []*agentv1.UpdateStatusRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.UpdateStatusRequest(nil), s.reqs...)
}

// recordedResults records the terminal results a handler reported. The embedded client is never
// called: this test drives the update path directly rather than over a stream.
type recordedResults struct {
	agentv1.AgentServiceClient
	mu      sync.Mutex
	results []*agentv1.ReportRequest
}

// Report records one terminal result and accepts it.
func (r *recordedResults) Report(_ context.Context, req *agentv1.ReportRequest, _ ...grpc.CallOption) (*agentv1.ReportResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, req)
	return &agentv1.ReportResponse{Accepted: true}, nil
}

// requests returns the recorded terminal results, oldest first.
func (r *recordedResults) requests() []*agentv1.ReportRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*agentv1.ReportRequest(nil), r.results...)
}
