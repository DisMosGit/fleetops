package rolloutv1_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
)

// TestMessageRoundTrip proves the wave health request and response survive the wire unchanged.
// v1 is frozen, so a round trip that loses or alters a field is a contract break, not a
// cosmetic diff.
func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "wave health request",
			msg: &rolloutv1.GetWaveHealthRequest{
				RolloutId: "roll-1",
				WaveId:    "wave-1",
			},
		},
		{
			name: "decided wave health response",
			msg: &rolloutv1.GetWaveHealthResponse{
				RolloutId:             "roll-1",
				WaveId:                "wave-1",
				SuccessRatio:          0.95,
				SampleSize:            100,
				Verdict:               rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY,
				WindowStart:           timestamppb.New(time.Unix(1737500000, 0)),
				WindowEnd:             timestamppb.New(time.Unix(1737500300, 0)),
				SampleHealthThreshold: 0.6,
				MinSuccessRatio:       0.95,
				MinSamples:            10,
			},
		},
		{
			name: "empty window response",
			msg: &rolloutv1.GetWaveHealthResponse{
				RolloutId:             "roll-2",
				WaveId:                "wave-2",
				SuccessRatio:          0,
				SampleSize:            0,
				Verdict:               rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED,
				WindowStart:           timestamppb.New(time.Unix(1737500000, 0)),
				WindowEnd:             timestamppb.New(time.Unix(1737500000, 0)),
				SampleHealthThreshold: 0.6,
				MinSuccessRatio:       0.95,
				MinSamples:            10,
			},
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

// field is one field's frozen identity: the number is the wire contract, the name is what
// generated clients call it, and the kind is how it is encoded. Any change to the triple is
// a v1 violation, so the guard fails whether or not the tree still compiles.
type field struct {
	// Name is the field's name in the generated client.
	Name string
	// Kind is the field's wire encoding.
	Kind protoreflect.Kind
}

// fieldsOf maps a message's field numbers to their frozen identities.
func fieldsOf(desc protoreflect.MessageDescriptor) map[protoreflect.FieldNumber]field {
	out := make(map[protoreflect.FieldNumber]field, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		f := desc.Fields().Get(i)
		out[f.Number()] = field{Name: string(f.Name()), Kind: f.Kind()}
	}
	return out
}

// TestFrozenFields pins the field numbers, names, and kinds of this new v1 package. Renumbering,
// reusing, or retyping a field breaks every already-generated client, so it is rejected here
// even when the Go types happen to line up.
func TestFrozenFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		desc protoreflect.MessageDescriptor
		want map[protoreflect.FieldNumber]field
	}{
		{
			name: "GetWaveHealthRequest",
			desc: (&rolloutv1.GetWaveHealthRequest{}).ProtoReflect().Descriptor(),
			want: map[protoreflect.FieldNumber]field{
				1: {Name: "rollout_id", Kind: protoreflect.StringKind},
				2: {Name: "wave_id", Kind: protoreflect.StringKind},
			},
		},
		{
			name: "GetWaveHealthResponse",
			desc: (&rolloutv1.GetWaveHealthResponse{}).ProtoReflect().Descriptor(),
			want: map[protoreflect.FieldNumber]field{
				1:  {Name: "rollout_id", Kind: protoreflect.StringKind},
				2:  {Name: "wave_id", Kind: protoreflect.StringKind},
				3:  {Name: "success_ratio", Kind: protoreflect.DoubleKind},
				4:  {Name: "sample_size", Kind: protoreflect.Int64Kind},
				5:  {Name: "verdict", Kind: protoreflect.EnumKind},
				6:  {Name: "window_start", Kind: protoreflect.MessageKind},
				7:  {Name: "window_end", Kind: protoreflect.MessageKind},
				8:  {Name: "sample_health_threshold", Kind: protoreflect.DoubleKind},
				9:  {Name: "min_success_ratio", Kind: protoreflect.DoubleKind},
				10: {Name: "min_samples", Kind: protoreflect.Int64Kind},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tc.want, fieldsOf(tc.desc)); diff != "" {
				t.Errorf("frozen fields changed (-want +got):\n%s", diff)
			}
		})
	}
}

// TestFrozenVerdictValues pins the verdict enum's numbers: the unspecified zero and the three
// reported outcomes. The verdict set is closed — a served response never carries zero.
func TestFrozenVerdictValues(t *testing.T) {
	t.Parallel()

	desc := rolloutv1.WaveHealthVerdict(0).Descriptor()
	got := make(map[protoreflect.EnumNumber]string, desc.Values().Len())
	for i := 0; i < desc.Values().Len(); i++ {
		v := desc.Values().Get(i)
		got[v.Number()] = string(v.Name())
	}
	want := map[protoreflect.EnumNumber]string{
		0: "WAVE_HEALTH_VERDICT_UNSPECIFIED",
		1: "WAVE_HEALTH_VERDICT_UNDECIDED",
		2: "WAVE_HEALTH_VERDICT_HEALTHY",
		3: "WAVE_HEALTH_VERDICT_UNHEALTHY",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("frozen verdict values changed (-want +got):\n%s", diff)
	}
}

// TestAgentContractStaysUntouched pins the bounded-context split: an operator health query is
// not an agent exchange, so adding this package must leave fleetops.agent.v1 without a service,
// RPC, or message of its own for wave health.
func TestAgentContractStaysUntouched(t *testing.T) {
	t.Parallel()

	file := (&agentv1.ReportRequest{}).ProtoReflect().Descriptor().ParentFile()

	var services []string
	for i := 0; i < file.Services().Len(); i++ {
		services = append(services, string(file.Services().Get(i).Name()))
	}
	if diff := cmp.Diff([]string{"AgentService"}, services); diff != "" {
		t.Errorf("agent services changed (-want +got):\n%s", diff)
	}

	for i := 0; i < file.Messages().Len(); i++ {
		name := string(file.Messages().Get(i).Name())
		if len(name) >= len("WaveHealth") && name[:len("WaveHealth")] == "WaveHealth" {
			t.Errorf("fleetops.agent.v1 gained the wave health message %q, want the query to stay in fleetops.rollout.v1", name)
		}
	}
}
