package protocodegen

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
)

// TestAgentAddingFieldKeepsOldClientsWorking backs the agent contract's "Adding a field keeps
// old clients working": a new field on an existing v1 message leaves a client generated before
// it working against the updated server, because the addition takes a fresh number and every
// frozen field keeps its number, name, and encoding.
func TestAgentAddingFieldKeepsOldClientsWorking(t *testing.T) {
	t.Parallel()

	assertAdditiveEvolution(t, evolutionCase{
		file:          (&agentv1.Heartbeat{}).ProtoReflect().Descriptor().ParentFile(),
		message:       "Heartbeat",
		baseline:      agentFrozen.messages["Heartbeat"],
		addedNumber:   9,
		addedIdentity: frozenIdentity{name: "region", kind: protoreflect.StringKind},
		addedValue:    "eu-west",
		values: map[protoreflect.FieldNumber]any{
			1: "evt-1",
			2: "dev-1",
			3: "1.2.3",
			4: "healthy",
			5: timestamppb.New(time.Unix(1737500000, 0)),
			6: 0.42,
			7: 0.17,
			8: 0.99,
		},
		decoded: func() proto.Message { return &agentv1.Heartbeat{} },
	})
}

// TestAgentAddingFieldAndRPCKeepsOldClientsWorking backs the agent contract's "Adding the field
// and RPC keeps old clients working": the firmware download evolution added
// FirmwareDownloadResponse.total_size and AgentService.DownloadFirmware, and a client generated
// before it keeps the Connect-exchange download it already makes working on the wire, because
// both additions left the frozen fields and RPCs untouched.
func TestAgentAddingFieldAndRPCKeepsOldClientsWorking(t *testing.T) {
	t.Parallel()

	file := (&agentv1.FirmwareDownloadResponse{}).ProtoReflect().Descriptor().ParentFile()

	// A client generated before the evolution speaks the response's first six fields and the
	// two RPCs the frozen stream already carried.
	before := map[protoreflect.FieldNumber]frozenIdentity{
		1: {name: "firmware_id", kind: protoreflect.StringKind},
		2: {name: "version", kind: protoreflect.StringKind},
		3: {name: "checksum", kind: protoreflect.StringKind},
		4: {name: "chunk", kind: protoreflect.BytesKind},
		5: {name: "offset", kind: protoreflect.Int64Kind},
		6: {name: "eof", kind: protoreflect.BoolKind},
	}

	assertAdditiveEvolution(t, evolutionCase{
		file:          file,
		message:       "FirmwareDownloadResponse",
		baseline:      before,
		addedNumber:   7,
		addedIdentity: frozenIdentity{name: "total_size", kind: protoreflect.Int64Kind},
		landed:        true,
		addedValue:    int64(4096),
		values: map[protoreflect.FieldNumber]any{
			1: "fw-9",
			2: "2.0.0",
			3: "sha256:abc",
			4: []byte{0x00, 0x01, 0x02},
			5: int64(1024),
			6: true,
		},
		decoded: func() proto.Message { return &agentv1.FirmwareDownloadResponse{} },
	})

	// The RPC half of the evolution: the calls a client generated before it already makes keep
	// their names and streaming shapes, the added call is the server stream, and it reuses the
	// frozen download messages rather than disturbing any message's fields.
	service := file.Services().ByName("AgentService")
	if service == nil {
		t.Fatal("fleetops.agent.v1 no longer declares AgentService")
	}
	shapes := make(map[string]string, service.Methods().Len())
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		shapes[string(method.Name())] = methodShape(method.IsStreamingClient(), method.IsStreamingServer())
	}
	want := map[string]string{
		"Connect":          "a bidirectional stream",
		"Report":           "unary",
		"DownloadFirmware": "a server stream",
	}
	if diff := cmp.Diff(want, shapes); diff != "" {
		t.Errorf("AgentService's RPCs changed shape (-want +got):\n%s", diff)
	}

	download := service.Methods().ByName("DownloadFirmware")
	if download == nil {
		t.Fatal("AgentService.DownloadFirmware is gone; the added RPC must keep the frozen exchange working")
	}
	if got, want := string(download.Input().FullName()), "fleetops.agent.v1.FirmwareDownloadRequest"; got != want {
		t.Errorf("DownloadFirmware takes %s, want the frozen %s", got, want)
	}
	if got, want := string(download.Output().FullName()), "fleetops.agent.v1.FirmwareDownloadResponse"; got != want {
		t.Errorf("DownloadFirmware returns %s, want the frozen %s", got, want)
	}
}

// TestRolloutAddingFieldKeepsOldClientsWorking backs the rollout health contract's "Adding a
// field keeps old clients working": a field added later to the wave health response leaves a
// client generated before it working against the updated server, because the addition takes a
// fresh number and the response keeps every frozen number, name, and kind.
func TestRolloutAddingFieldKeepsOldClientsWorking(t *testing.T) {
	t.Parallel()

	// The wave health response as it froze: the ten fields a client generated before a later
	// addition speaks. The package's TestFrozenFields pins the same numbers; this case is the
	// wire-level half, where the addition is applied and both sides must keep reading.
	before := map[protoreflect.FieldNumber]frozenIdentity{
		1:  {name: "rollout_id", kind: protoreflect.StringKind},
		2:  {name: "wave_id", kind: protoreflect.StringKind},
		3:  {name: "success_ratio", kind: protoreflect.DoubleKind},
		4:  {name: "sample_size", kind: protoreflect.Int64Kind},
		5:  {name: "verdict", kind: protoreflect.EnumKind, typeName: "fleetops.rollout.v1.WaveHealthVerdict"},
		6:  {name: "window_start", kind: protoreflect.MessageKind, typeName: "google.protobuf.Timestamp"},
		7:  {name: "window_end", kind: protoreflect.MessageKind, typeName: "google.protobuf.Timestamp"},
		8:  {name: "sample_health_threshold", kind: protoreflect.DoubleKind},
		9:  {name: "min_success_ratio", kind: protoreflect.DoubleKind},
		10: {name: "min_samples", kind: protoreflect.Int64Kind},
	}

	assertAdditiveEvolution(t, evolutionCase{
		file:          (&rolloutv1.GetWaveHealthResponse{}).ProtoReflect().Descriptor().ParentFile(),
		message:       "GetWaveHealthResponse",
		baseline:      before,
		addedNumber:   11,
		addedIdentity: frozenIdentity{name: "verdict_reason", kind: protoreflect.StringKind},
		addedValue:    "success ratio at or above the minimum",
		values: map[protoreflect.FieldNumber]any{
			1:  "roll-1",
			2:  "wave-1",
			3:  0.95,
			4:  int64(100),
			5:  protoreflect.EnumNumber(rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY),
			6:  timestamppb.New(time.Unix(1737500000, 0)),
			7:  timestamppb.New(time.Unix(1737500300, 0)),
			8:  0.6,
			9:  0.95,
			10: int64(10),
		},
		decoded: func() proto.Message { return &rolloutv1.GetWaveHealthResponse{} },
	})
}
