package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// fakeStatusReporter is a hand-written StatusReporter double recording what it receives.
type fakeStatusReporter struct {
	mu     sync.Mutex
	reqs   []*agentv1.UpdateStatusRequest
	err    error
	reject bool
}

// ReportUpdateStatus records the report and returns the scripted outcome.
func (f *fakeStatusReporter) ReportUpdateStatus(
	_ context.Context,
	req *agentv1.UpdateStatusRequest,
) (*agentv1.UpdateStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.reqs = append(f.reqs, req)
	return &agentv1.UpdateStatusResponse{Accepted: !f.reject}, nil
}

// recorded returns the reports received so far.
func (f *fakeStatusReporter) recorded() []*agentv1.UpdateStatusRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*agentv1.UpdateStatusRequest(nil), f.reqs...)
}

// fakeResults is a hand-written AgentServiceClient double recording terminal reports.
type fakeResults struct {
	agentv1.AgentServiceClient
	mu      sync.Mutex
	reports []*agentv1.ReportRequest
	err     error
	reject  bool
}

// Report records the terminal result and returns the scripted outcome.
func (f *fakeResults) Report(
	_ context.Context,
	req *agentv1.ReportRequest,
	_ ...grpc.CallOption,
) (*agentv1.ReportResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.reports = append(f.reports, req)
	return &agentv1.ReportResponse{Accepted: !f.reject}, nil
}

// recorded returns the terminal results received so far.
func (f *fakeResults) recorded() []*agentv1.ReportRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*agentv1.ReportRequest(nil), f.reports...)
}

func TestUpdateReporterStatus(t *testing.T) {
	t.Parallel()

	t.Run("progress report carries the phase and progress", func(t *testing.T) {
		t.Parallel()
		status := &fakeStatusReporter{}
		reporter := NewUpdateReporter(status, &fakeResults{})

		err := reporter.Status(context.Background(), "dev-1", "fw-1",
			agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, 40, "")
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		got := status.recorded()
		if len(got) != 1 {
			t.Fatalf("reports = %d, want 1", len(got))
		}
		if got[0].GetDeviceId() != "dev-1" || got[0].GetFirmwareId() != "fw-1" ||
			got[0].GetPhase() != agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING ||
			got[0].GetProgressPercent() != 40 {
			t.Errorf("report = %+v, want the reported device, firmware, phase, and progress", got[0])
		}
	})

	t.Run("failure detail travels with the failed phase", func(t *testing.T) {
		t.Parallel()
		status := &fakeStatusReporter{}
		reporter := NewUpdateReporter(status, &fakeResults{})

		err := reporter.Status(context.Background(), "dev-1", "fw-1",
			agentv1.UpdatePhase_UPDATE_PHASE_FAILED, 0, "checksum mismatch")
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		if got := status.recorded()[0]; got.GetDetail() != "checksum mismatch" {
			t.Errorf("detail = %q, want the failure reason", got.GetDetail())
		}
	})

	t.Run("an unaccepted report surfaces as an error", func(t *testing.T) {
		t.Parallel()
		reporter := NewUpdateReporter(&fakeStatusReporter{reject: true}, &fakeResults{})
		err := reporter.Status(context.Background(), "dev-1", "fw-1",
			agentv1.UpdatePhase_UPDATE_PHASE_APPLYING, 0, "")
		if err == nil || !strings.Contains(err.Error(), "not accepted") {
			t.Errorf("Status() error = %v, want the unaccepted outcome", err)
		}
	})

	t.Run("an undelivered report surfaces as a wrapped error", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("stream lost")
		reporter := NewUpdateReporter(&fakeStatusReporter{err: boom}, &fakeResults{})
		if err := reporter.Status(context.Background(), "dev-1", "fw-1",
			agentv1.UpdatePhase_UPDATE_PHASE_APPLYING, 0, ""); !errors.Is(err, boom) {
			t.Errorf("Status() error = %v, want it wrapped", err)
		}
	})
}

func TestUpdateReporterConclude(t *testing.T) {
	t.Parallel()

	cmd := &agentv1.Command{
		CommandId: "cmd-1",
		DeviceId:  "dev-1",
		Kind:      &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{FirmwareId: "fw-1"}},
	}

	t.Run("terminal result carries the derived idempotency key", func(t *testing.T) {
		t.Parallel()
		results := &fakeResults{}
		reporter := NewUpdateReporter(&fakeStatusReporter{}, results)

		if err := reporter.Conclude(context.Background(), cmd,
			agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, ""); err != nil {
			t.Fatalf("Conclude() error = %v", err)
		}
		got := results.recorded()
		if len(got) != 1 {
			t.Fatalf("reports = %d, want 1", len(got))
		}
		if got[0].GetIdempotencyKey() != reportKey("cmd-1") ||
			got[0].GetCommandId() != "cmd-1" || got[0].GetDeviceId() != "dev-1" ||
			got[0].GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			t.Errorf("report = %+v, want the command's result under its derived key", got[0])
		}
	})

	t.Run("a redelivered result reuses the same key", func(t *testing.T) {
		t.Parallel()
		results := &fakeResults{}
		reporter := NewUpdateReporter(&fakeStatusReporter{}, results)

		for range 2 {
			if err := reporter.Conclude(context.Background(), cmd,
				agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED, "checksum mismatch"); err != nil {
				t.Fatalf("Conclude() error = %v", err)
			}
		}
		got := results.recorded()
		if len(got) != 2 {
			t.Fatalf("reports = %d, want 2", len(got))
		}
		if got[0].GetIdempotencyKey() != got[1].GetIdempotencyKey() {
			t.Errorf("idempotency keys = %q and %q, want one key per command result",
				got[0].GetIdempotencyKey(), got[1].GetIdempotencyKey())
		}
	})

	t.Run("unaccepted or undelivered results surface as errors", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("temporal unavailable")
		if err := NewUpdateReporter(&fakeStatusReporter{}, &fakeResults{reject: true}).
			Conclude(context.Background(), cmd, agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED, ""); err == nil {
			t.Error("Conclude() error = nil, want the unaccepted outcome")
		}
		if err := NewUpdateReporter(&fakeStatusReporter{}, &fakeResults{err: boom}).
			Conclude(context.Background(), cmd, agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED, ""); !errors.Is(err, boom) {
			t.Errorf("Conclude() error = %v, want it wrapped", err)
		}
	})
}

func TestClientReportUpdateStatus(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, &fakeService{}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		ack *agentv1.UpdateStatusResponse
		err error
	}
	got := make(chan result, 1)
	go func() {
		ack, err := c.ReportUpdateStatus(ctx, &agentv1.UpdateStatusRequest{
			DeviceId: "dev-1", FirmwareId: "fw-1",
			Phase: agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
		})
		got <- result{ack: ack, err: err}
	}()

	sent := <-c.reqs
	req := sent.GetUpdateStatus()
	if sent.CorrelationId == "" || req == nil || req.GetDeviceId() != "dev-1" {
		t.Fatalf("queued envelope = %+v, want a correlated update status request", sent)
	}
	c.deliver(sent.CorrelationId, &agentv1.ControlEnvelope{
		CorrelationId: sent.CorrelationId,
		Payload: &agentv1.ControlEnvelope_UpdateStatus{UpdateStatus: &agentv1.UpdateStatusResponse{
			Accepted: true,
		}},
	})

	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("ReportUpdateStatus() error = %v", res.err)
		}
		if !res.ack.GetAccepted() {
			t.Error("acknowledgment accepted = false, want true")
		}
	case <-ctx.Done():
		t.Fatal("report was not matched to its acknowledgment")
	}

	// A response carrying no acknowledgment is a protocol violation, not an acceptance.
	go func() {
		_, err := c.ReportUpdateStatus(ctx, &agentv1.UpdateStatusRequest{DeviceId: "dev-1"})
		got <- result{err: err}
	}()
	sent = <-c.reqs
	c.deliver(sent.CorrelationId, &agentv1.ControlEnvelope{
		CorrelationId: sent.CorrelationId,
		Payload:       &agentv1.ControlEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceResponse{}},
	})
	select {
	case res := <-got:
		if res.err == nil {
			t.Error("ReportUpdateStatus() error = nil, want the missing-acknowledgment error")
		}
	case <-ctx.Done():
		t.Fatal("report did not resolve")
	}

}
