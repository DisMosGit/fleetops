package agentv1_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// TestMessageRoundTrip proves every v1 envelope payload and report message survives the wire
// unchanged. v1 is frozen, so a round trip that loses or alters a field is a contract break,
// not a cosmetic diff.
func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "registration request envelope",
			msg: &agentv1.AgentEnvelope{
				CorrelationId: "corr-1",
				Payload: &agentv1.AgentEnvelope_RegisterDevice{
					RegisterDevice: &agentv1.RegisterDeviceRequest{
						DeviceId:  "dev-1",
						Model:     "thermostat",
						Region:    "eu-west",
						CurrentFw: "1.2.3",
					},
				},
			},
		},
		{
			name: "heartbeat envelope",
			msg: &agentv1.AgentEnvelope{
				Payload: &agentv1.AgentEnvelope_Heartbeat{
					Heartbeat: &agentv1.Heartbeat{
						EventId:   "evt-1",
						DeviceId:  "dev-1",
						CurrentFw: "1.2.3",
						Status:    "healthy",
						Ts:        timestamppb.New(time.Unix(1737500000, 0)),
						Cpu:       0.42,
						Mem:       0.17,
						Health:    0.99,
					},
				},
			},
		},
		{
			name: "firmware download request envelope",
			msg: &agentv1.AgentEnvelope{
				CorrelationId: "corr-2",
				Payload: &agentv1.AgentEnvelope_FirmwareDownload{
					FirmwareDownload: &agentv1.FirmwareDownloadRequest{
						DeviceId:   "dev-1",
						FirmwareId: "fw-9",
					},
				},
			},
		},
		{
			name: "update status request envelope",
			msg: &agentv1.AgentEnvelope{
				CorrelationId: "corr-3",
				Payload: &agentv1.AgentEnvelope_UpdateStatus{
					UpdateStatus: &agentv1.UpdateStatusRequest{
						DeviceId:        "dev-1",
						FirmwareId:      "fw-9",
						Phase:           agentv1.UpdatePhase_UPDATE_PHASE_FAILED,
						ProgressPercent: 37,
						Detail:          "checksum mismatch",
					},
				},
			},
		},
		{
			name: "registration response envelope",
			msg: &agentv1.ControlEnvelope{
				CorrelationId: "corr-1",
				Payload: &agentv1.ControlEnvelope_RegisterDevice{
					RegisterDevice: &agentv1.RegisterDeviceResponse{
						Accepted: true,
						DeviceId: "dev-1",
						Status:   "active",
						Reason:   "",
					},
				},
			},
		},
		{
			name: "firmware download chunk envelope",
			msg: &agentv1.ControlEnvelope{
				CorrelationId: "corr-2",
				Payload: &agentv1.ControlEnvelope_FirmwareDownload{
					FirmwareDownload: &agentv1.FirmwareDownloadResponse{
						FirmwareId: "fw-9",
						Version:    "2.0.0",
						Checksum:   "sha256:abc",
						Chunk:      []byte{0x00, 0x01, 0x02},
						Offset:     1024,
						Eof:        true,
					},
				},
			},
		},
		{
			name: "update status response envelope",
			msg: &agentv1.ControlEnvelope{
				CorrelationId: "corr-3",
				Payload: &agentv1.ControlEnvelope_UpdateStatus{
					UpdateStatus: &agentv1.UpdateStatusResponse{Accepted: true},
				},
			},
		},
		{
			name: "start update command envelope",
			msg: &agentv1.ControlEnvelope{
				Payload: &agentv1.ControlEnvelope_Command{
					Command: &agentv1.Command{
						CommandId: "cmd-1",
						DeviceId:  "dev-1",
						Kind: &agentv1.Command_StartUpdate{
							StartUpdate: &agentv1.StartUpdate{
								FirmwareId: "fw-9",
								Version:    "2.0.0",
								Checksum:   "sha256:abc",
							},
						},
					},
				},
			},
		},
		{
			name: "abort update command envelope",
			msg: &agentv1.ControlEnvelope{
				Payload: &agentv1.ControlEnvelope_Command{
					Command: &agentv1.Command{
						CommandId: "cmd-2",
						DeviceId:  "dev-1",
						Kind: &agentv1.Command_AbortUpdate{
							AbortUpdate: &agentv1.AbortUpdate{Reason: "wave health regressed"},
						},
					},
				},
			},
		},
		{
			name: "report request",
			msg: &agentv1.ReportRequest{
				IdempotencyKey: "idem-1",
				CommandId:      "cmd-1",
				DeviceId:       "dev-1",
				Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
			},
		},
		{
			name: "report request with failure detail",
			msg: &agentv1.ReportRequest{
				IdempotencyKey: "idem-2",
				CommandId:      "cmd-2",
				DeviceId:       "dev-1",
				Outcome:        agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
				Detail:         "flash write failed",
			},
		},
		{
			name: "report response",
			msg: &agentv1.ReportResponse{Accepted: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wire, err := proto.Marshal(tc.msg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			got := tc.msg.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(wire, got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			if diff := cmp.Diff(tc.msg, got, protocmp.Transform()); diff != "" {
				t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
