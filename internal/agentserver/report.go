package agentserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// Report delivers the terminal result of one command to the device's workflow. A report
// repeating a seen idempotency key is accepted again with no second effect: the workflow
// deduplicates results by command id, so a retried report can never conclude a command
// twice.
func (s *Server) Report(
	ctx context.Context,
	req *agentv1.ReportRequest,
) (*agentv1.ReportResponse, error) {
	if err := validateReport(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.signals.SignalCommandResult(ctx, req); err != nil {
		// The workflow state is the destination of truth for this result: an unrecorded
		// report is not accepted, and the agent may retry it.
		s.log.Error("signal command result",
			"device_id", req.GetDeviceId(), "command_id", req.GetCommandId(), "err", err)
		return nil, status.Error(codes.Internal, "command result could not be recorded")
	}
	return &agentv1.ReportResponse{Accepted: true}, nil
}

// validateReport checks the fields a report cannot be acted on without. The messages are
// operator-safe by construction.
func validateReport(req *agentv1.ReportRequest) error {
	switch {
	case req.GetIdempotencyKey() == "":
		return errors.New("idempotency key required")
	case req.GetCommandId() == "":
		return errors.New("command id required")
	case req.GetDeviceId() == "":
		return errors.New("device id required")
	case req.GetOutcome() == agentv1.CommandOutcome_COMMAND_OUTCOME_UNSPECIFIED:
		return errors.New("outcome required")
	case req.GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED &&
		req.GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED:
		return errors.New("outcome not supported")
	default:
		return nil
	}
}
