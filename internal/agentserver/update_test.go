package agentserver

import (
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// reportStatus sends one update status envelope and returns the acknowledgment.
func reportStatus(
	t *testing.T,
	stream grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope],
	correlationID string,
	req *agentv1.UpdateStatusRequest,
) *agentv1.UpdateStatusResponse {
	t.Helper()
	env := &agentv1.AgentEnvelope{
		CorrelationId: correlationID,
		Payload:       &agentv1.AgentEnvelope_UpdateStatus{UpdateStatus: req},
	}
	if err := stream.Send(env); err != nil {
		t.Fatalf("send update status: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive update status response: %v", err)
	}
	if resp.CorrelationId != correlationID {
		t.Fatalf("response correlation id = %q, want %q", resp.CorrelationId, correlationID)
	}
	return resp.GetUpdateStatus()
}

// enrolledStream opens a stream with one registered device on it. The connection's teardown
// is the startServer cleanup's.
func enrolledStream(
	t *testing.T,
	signals *fakeSignaler,
) grpc.BidiStreamingClient[agentv1.AgentEnvelope, agentv1.ControlEnvelope] {
	t.Helper()
	conn := startServer(t, NewHub(&fakeSink{}, slog.New(slog.DiscardHandler)), &fakeRegistry{}, signals)
	stream := openStream(t, conn)
	if resp := registerDevice(t, stream, "reg-1", "dev-1", "oak-s3", "eu-west", "1.0.0"); !resp.Accepted {
		t.Fatalf("registration rejected: %s", resp.Reason)
	}
	return stream
}

func TestUpdateStatusAccepted(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{}
	stream := enrolledStream(t, signals)

	resp := reportStatus(t, stream, "upd-1", &agentv1.UpdateStatusRequest{
		DeviceId:        "dev-1",
		FirmwareId:      "fw-1",
		Phase:           agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
		ProgressPercent: 40,
		Detail:          "",
	})
	if !resp.GetAccepted() {
		t.Fatalf("acknowledgment accepted = false, want true")
	}
	updates := signals.signaledUpdates()
	if len(updates) != 1 {
		t.Fatalf("signaled updates = %d, want 1", len(updates))
	}
	if updates[0].GetDeviceId() != "dev-1" || updates[0].GetFirmwareId() != "fw-1" ||
		updates[0].GetPhase() != agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING ||
		updates[0].GetProgressPercent() != 40 {
		t.Errorf("signaled update = %+v, want the reported phase and progress", updates[0])
	}
}

func TestUpdateStatusRejectedWithoutFailingStream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  *agentv1.UpdateStatusRequest
	}{
		{
			name: "no device id",
			req: &agentv1.UpdateStatusRequest{
				FirmwareId: "fw-1", Phase: agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
			},
		},
		{
			name: "no firmware id",
			req: &agentv1.UpdateStatusRequest{
				DeviceId: "dev-1", Phase: agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
			},
		},
		{
			name: "no phase",
			req:  &agentv1.UpdateStatusRequest{DeviceId: "dev-1", FirmwareId: "fw-1"},
		},
		{
			name: "device not enrolled on this stream",
			req: &agentv1.UpdateStatusRequest{
				DeviceId: "dev-other", FirmwareId: "fw-1",
				Phase: agentv1.UpdatePhase_UPDATE_PHASE_FAILED, Detail: "checksum mismatch",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			signals := &fakeSignaler{}
			stream := enrolledStream(t, signals)

			resp := reportStatus(t, stream, "upd-1", tc.req)
			if resp.GetAccepted() {
				t.Error("acknowledgment accepted = true, want false")
			}
			if updates := signals.signaledUpdates(); len(updates) != 0 {
				t.Errorf("signaled updates = %d, want none", len(updates))
			}
			// The stream survives the rejection: the next report is still answered.
			alive := reportStatus(t, stream, "upd-2", &agentv1.UpdateStatusRequest{
				DeviceId: "dev-1", FirmwareId: "fw-1",
				Phase: agentv1.UpdatePhase_UPDATE_PHASE_COMPLETED, ProgressPercent: 100,
			})
			if !alive.GetAccepted() {
				t.Error("follow-up report rejected, want the stream still serving")
			}
		})
	}
}

func TestUpdateStatusSignalFailureIsNotAccepted(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{err: errors.New("temporal unavailable")}
	stream := enrolledStream(t, signals)

	resp := reportStatus(t, stream, "upd-1", &agentv1.UpdateStatusRequest{
		DeviceId: "dev-1", FirmwareId: "fw-1",
		Phase: agentv1.UpdatePhase_UPDATE_PHASE_APPLYING,
	})
	if resp.GetAccepted() {
		t.Error("acknowledgment accepted = true, want false (nothing was recorded)")
	}
}

func TestUpdateStatusWithoutCorrelationID(t *testing.T) {
	t.Parallel()

	signals := &fakeSignaler{}
	stream := enrolledStream(t, signals)

	err := stream.Send(&agentv1.AgentEnvelope{
		Payload: &agentv1.AgentEnvelope_UpdateStatus{UpdateStatus: &agentv1.UpdateStatusRequest{
			DeviceId: "dev-1", FirmwareId: "fw-1",
			Phase: agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
		}},
	})
	if err != nil {
		t.Fatalf("send update status: %v", err)
	}
	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("receive ended with %v, want a gRPC status error", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}
