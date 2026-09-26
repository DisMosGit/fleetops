package agentserver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// updateStatus accepts one firmware-update progress report from an enrolled device: accepted
// reports are forwarded to the device workflow, and every report is acknowledged on its
// correlation id. A report that is malformed or names a device not enrolled on this stream is
// answered with accepted: false instead of being forwarded, and never fails the stream —
// progress reporting is too fine-grained to end a live agent conversation over one bad message.
func (s *Server) updateStatus(
	ctx context.Context,
	sess *session,
	correlationID string,
	req *agentv1.UpdateStatusRequest,
) error {
	if correlationID == "" {
		// Without a correlation id there is no acknowledgment to return, so this is not a
		// status report at all — the same treatment a registration without one gets.
		return status.Error(codes.InvalidArgument, "update status without correlation id")
	}
	accepted := false
	if req.GetDeviceId() != "" && req.GetFirmwareId() != "" &&
		req.GetPhase() != agentv1.UpdatePhase_UPDATE_PHASE_UNSPECIFIED {
		if _, enrolled := s.hub.identity(req.GetDeviceId(), sess); enrolled {
			if err := s.signals.SignalUpdateStatus(ctx, req); err != nil {
				// Nothing was recorded, so nothing is accepted: the agent may report again.
				s.log.Error("signal update status",
					"device_id", req.GetDeviceId(),
					"firmware_id", req.GetFirmwareId(),
					"err", err)
			} else {
				accepted = true
			}
		}
	}
	return s.respond(ctx, sess, &agentv1.ControlEnvelope{
		CorrelationId: correlationID,
		Payload: &agentv1.ControlEnvelope_UpdateStatus{UpdateStatus: &agentv1.UpdateStatusResponse{
			Accepted: accepted,
		}},
	})
}
