package agentserver

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// newReportServer wires a Server with fakes for direct Report calls.
func newReportServer(signals DeviceSignaler) *Server {
	log := slog.New(slog.DiscardHandler)
	return NewServer(NewHub(&fakeSink{}, log), &fakeRegistry{}, signals, unavailableFirmware{}, log)
}

func TestReportValidation(t *testing.T) {
	t.Parallel()

	newRequest := func() *agentv1.ReportRequest {
		return &agentv1.ReportRequest{
			IdempotencyKey: "key-1",
			CommandId:      "cmd-1",
			DeviceId:       "device-0",
			Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		}
	}

	cases := []struct {
		name   string
		mutate func(*agentv1.ReportRequest)
		reason string
	}{
		{
			name:   "missing idempotency key",
			mutate: func(r *agentv1.ReportRequest) { r.IdempotencyKey = "" },
			reason: "idempotency key required",
		},
		{
			name:   "missing command id",
			mutate: func(r *agentv1.ReportRequest) { r.CommandId = "" },
			reason: "command id required",
		},
		{
			name:   "missing device id",
			mutate: func(r *agentv1.ReportRequest) { r.DeviceId = "" },
			reason: "device id required",
		},
		{
			name: "missing outcome",
			mutate: func(r *agentv1.ReportRequest) {
				r.Outcome = agentv1.CommandOutcome_COMMAND_OUTCOME_UNSPECIFIED
			},
			reason: "outcome required",
		},
		{
			name: "unsupported outcome",
			mutate: func(r *agentv1.ReportRequest) {
				r.Outcome = agentv1.CommandOutcome(99)
			},
			reason: "outcome not supported",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			signals := &fakeSignaler{}
			req := newRequest()
			tc.mutate(req)

			resp, err := newReportServer(signals).Report(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Report() error = %v, want InvalidArgument", err)
			}
			if status.Convert(err).Message() != tc.reason {
				t.Errorf("Report() message = %q, want %q", status.Convert(err).Message(), tc.reason)
			}
			if resp != nil {
				t.Errorf("Report() response = %+v, want nil on rejection", resp)
			}
			if got := signals.signaledResults(); len(got) != 0 {
				t.Errorf("workflow seam received %+v, want nothing for a rejected report", got)
			}
		})
	}
}

func TestReportForwardsCommandResult(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{}
	req := &agentv1.ReportRequest{
		IdempotencyKey: "key-1",
		CommandId:      "cmd-1",
		DeviceId:       "device-0",
		Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Detail:         "flash error",
	}
	resp, err := newReportServer(signals).Report(context.Background(), req)
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if !resp.GetAccepted() {
		t.Errorf("Report() response = %+v, want accepted", resp)
	}
	got := signals.signaledResults()
	if len(got) != 1 {
		t.Fatalf("workflow seam received %d results, want 1", len(got))
	}
	if got[0].GetIdempotencyKey() != "key-1" || got[0].GetCommandId() != "cmd-1" ||
		got[0].GetDeviceId() != "device-0" || got[0].GetOutcome() != req.GetOutcome() ||
		got[0].GetDetail() != "flash error" {
		t.Errorf("forwarded result = %+v, want the submitted report", got[0])
	}
}

func TestReportRepeatedIdempotencyKeyAccepted(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{}
	server := newReportServer(signals)
	req := &agentv1.ReportRequest{
		IdempotencyKey: "key-1",
		CommandId:      "cmd-1",
		DeviceId:       "device-0",
		Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
	}
	for range 2 {
		resp, err := server.Report(context.Background(), req)
		if err != nil {
			t.Fatalf("Report() error = %v", err)
		}
		if !resp.GetAccepted() {
			t.Errorf("Report() response = %+v, want a repeated report accepted", resp)
		}
	}
	// Both forwards reach the workflow; only the first can change device state — the
	// workflow deduplicates results by command id.
	if got := signals.signaledResults(); len(got) != 2 {
		t.Errorf("workflow seam received %d results, want 2", len(got))
	}
}

func TestReportSignalFailureNotAccepted(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{err: errors.New("temporal is down")}
	req := &agentv1.ReportRequest{
		IdempotencyKey: "key-1",
		CommandId:      "cmd-1",
		DeviceId:       "device-0",
		Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
	}
	resp, err := newReportServer(signals).Report(context.Background(), req)
	if status.Code(err) != codes.Internal {
		t.Errorf("Report() error = %v, want Internal", err)
	}
	if resp != nil {
		t.Errorf("Report() response = %+v, want nil when the result could not be recorded", resp)
	}
}
