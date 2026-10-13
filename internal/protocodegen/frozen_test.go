package protocodegen

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// frozenIdentity is one field's frozen wire identity: the number is the contract, the name is
// what generated clients call it, the kind is how it is encoded, and typeName ties a message or
// enum field to the type it carries. Any change to the four breaks clients generated before the
// change, so a proposal that makes one is a v1 violation.
type frozenIdentity struct {
	name     string
	kind     protoreflect.Kind
	typeName string
}

// frozenMethod is one RPC's frozen identity: its name and the streaming shape a client
// generated at the freeze already speaks. Adding an RPC is allowed; renaming or reshaping one
// is not.
type frozenMethod struct {
	name            string
	clientStreaming bool
	serverStreaming bool
}

// frozenContract is one v1 contract as it froze: the messages, enum values, and RPCs that
// existed then. Every later evolution must keep these intact and may only add beside them.
type frozenContract struct {
	name     string
	service  string
	methods  []frozenMethod
	messages map[string]map[protoreflect.FieldNumber]frozenIdentity
	enums    map[string]map[protoreflect.EnumNumber]string
}

// agentFrozen is fleetops.agent.v1 as it froze. No frozen field may move, change kind, or
// disappear and no addition may take a frozen number; no frozen enum value or RPC may be
// renamed, renumbered, or reshaped. None of the frozen fields is repeated, so a frozen number
// that became a list is a violation too.
var agentFrozen = frozenContract{
	name:    "fleetops.agent.v1",
	service: "AgentService",
	methods: []frozenMethod{
		{name: "Connect", clientStreaming: true, serverStreaming: true},
		{name: "Report"},
		{name: "DownloadFirmware", serverStreaming: true},
	},
	messages: map[string]map[protoreflect.FieldNumber]frozenIdentity{
		"AgentEnvelope": {
			1: {name: "correlation_id", kind: protoreflect.StringKind},
			2: {name: "register_device", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.RegisterDeviceRequest"},
			3: {name: "heartbeat", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.Heartbeat"},
			4: {name: "firmware_download", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.FirmwareDownloadRequest"},
			5: {name: "update_status", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.UpdateStatusRequest"},
		},
		"ControlEnvelope": {
			1: {name: "correlation_id", kind: protoreflect.StringKind},
			2: {name: "register_device", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.RegisterDeviceResponse"},
			3: {name: "firmware_download", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.FirmwareDownloadResponse"},
			4: {name: "update_status", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.UpdateStatusResponse"},
			5: {name: "command", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.Command"},
		},
		"RegisterDeviceRequest": {
			1: {name: "device_id", kind: protoreflect.StringKind},
			2: {name: "model", kind: protoreflect.StringKind},
			3: {name: "region", kind: protoreflect.StringKind},
			4: {name: "current_fw", kind: protoreflect.StringKind},
		},
		"RegisterDeviceResponse": {
			1: {name: "accepted", kind: protoreflect.BoolKind},
			2: {name: "device_id", kind: protoreflect.StringKind},
			3: {name: "status", kind: protoreflect.StringKind},
			4: {name: "reason", kind: protoreflect.StringKind},
		},
		"Heartbeat": {
			1: {name: "event_id", kind: protoreflect.StringKind},
			2: {name: "device_id", kind: protoreflect.StringKind},
			3: {name: "current_fw", kind: protoreflect.StringKind},
			4: {name: "status", kind: protoreflect.StringKind},
			5: {name: "ts", kind: protoreflect.MessageKind, typeName: "google.protobuf.Timestamp"},
			6: {name: "cpu", kind: protoreflect.DoubleKind},
			7: {name: "mem", kind: protoreflect.DoubleKind},
			8: {name: "health", kind: protoreflect.DoubleKind},
		},
		"FirmwareDownloadRequest": {
			1: {name: "device_id", kind: protoreflect.StringKind},
			2: {name: "firmware_id", kind: protoreflect.StringKind},
		},
		"FirmwareDownloadResponse": {
			1: {name: "firmware_id", kind: protoreflect.StringKind},
			2: {name: "version", kind: protoreflect.StringKind},
			3: {name: "checksum", kind: protoreflect.StringKind},
			4: {name: "chunk", kind: protoreflect.BytesKind},
			5: {name: "offset", kind: protoreflect.Int64Kind},
			6: {name: "eof", kind: protoreflect.BoolKind},
			7: {name: "total_size", kind: protoreflect.Int64Kind},
		},
		"UpdateStatusRequest": {
			1: {name: "device_id", kind: protoreflect.StringKind},
			2: {name: "firmware_id", kind: protoreflect.StringKind},
			3: {name: "phase", kind: protoreflect.EnumKind, typeName: "fleetops.agent.v1.UpdatePhase"},
			4: {name: "progress_percent", kind: protoreflect.Int32Kind},
			5: {name: "detail", kind: protoreflect.StringKind},
		},
		"UpdateStatusResponse": {
			1: {name: "accepted", kind: protoreflect.BoolKind},
		},
		"Command": {
			1: {name: "command_id", kind: protoreflect.StringKind},
			2: {name: "device_id", kind: protoreflect.StringKind},
			3: {name: "start_update", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.StartUpdate"},
			4: {name: "abort_update", kind: protoreflect.MessageKind, typeName: "fleetops.agent.v1.AbortUpdate"},
		},
		"StartUpdate": {
			1: {name: "firmware_id", kind: protoreflect.StringKind},
			2: {name: "version", kind: protoreflect.StringKind},
			3: {name: "checksum", kind: protoreflect.StringKind},
		},
		"AbortUpdate": {
			1: {name: "reason", kind: protoreflect.StringKind},
		},
		"ReportRequest": {
			1: {name: "idempotency_key", kind: protoreflect.StringKind},
			2: {name: "command_id", kind: protoreflect.StringKind},
			3: {name: "device_id", kind: protoreflect.StringKind},
			4: {name: "outcome", kind: protoreflect.EnumKind, typeName: "fleetops.agent.v1.CommandOutcome"},
			5: {name: "detail", kind: protoreflect.StringKind},
		},
		"ReportResponse": {
			1: {name: "accepted", kind: protoreflect.BoolKind},
		},
	},
	enums: map[string]map[protoreflect.EnumNumber]string{
		"UpdatePhase": {
			0: "UPDATE_PHASE_UNSPECIFIED",
			1: "UPDATE_PHASE_DOWNLOADING",
			2: "UPDATE_PHASE_APPLYING",
			3: "UPDATE_PHASE_REBOOTING",
			4: "UPDATE_PHASE_COMPLETED",
			5: "UPDATE_PHASE_FAILED",
			6: "UPDATE_PHASE_ROLLED_BACK",
		},
		"CommandOutcome": {
			0: "COMMAND_OUTCOME_UNSPECIFIED",
			1: "COMMAND_OUTCOME_SUCCEEDED",
			2: "COMMAND_OUTCOME_FAILED",
		},
	},
}

// sortedFieldNumbers returns a field-number-keyed map's keys in order, so a guard reports and
// a wire image builds in a stable order.
func sortedFieldNumbers[V any](m map[protoreflect.FieldNumber]V) []protoreflect.FieldNumber {
	out := make([]protoreflect.FieldNumber, 0, len(m))
	for number := range m {
		out = append(out, number)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// frozenViolations reports every way a proposed contract departs from the frozen baseline: a
// field that moved, was retyped, or disappeared, an addition that took a frozen number, an
// enum value that moved, or an RPC that was renamed or reshaped. It reports rather than fails
// so the same guard can run against the live contract and against a proposal.
func frozenViolations(proposal *descriptorpb.FileDescriptorProto, frozen frozenContract) []string {
	var violations []string

	for _, name := range sortedKeys(frozen.messages) {
		message := findMessage(proposal, name)
		if message == nil {
			violations = append(violations, fmt.Sprintf("%s: message %s is gone", frozen.name, name))
			continue
		}
		violations = append(violations, fieldViolations(frozen.name, message, frozen.messages[name])...)
	}

	for _, name := range sortedKeys(frozen.enums) {
		enum := findEnum(proposal, name)
		if enum == nil {
			violations = append(violations, fmt.Sprintf("%s: enum %s is gone", frozen.name, name))
			continue
		}
		violations = append(violations, enumViolations(frozen.name, enum, frozen.enums[name])...)
	}

	if frozen.service != "" {
		violations = append(violations, methodViolations(proposal, frozen)...)
	}

	sort.Strings(violations)
	return violations
}

// fieldViolations reports every frozen field of one message the proposal no longer carries at
// its frozen number with its frozen identity.
func fieldViolations(pkg string, message *descriptorpb.DescriptorProto, frozen map[protoreflect.FieldNumber]frozenIdentity) []string {
	byNumber := make(map[protoreflect.FieldNumber][]*descriptorpb.FieldDescriptorProto, len(message.GetField()))
	for _, field := range message.GetField() {
		byNumber[protoreflect.FieldNumber(field.GetNumber())] = append(byNumber[protoreflect.FieldNumber(field.GetNumber())], field)
	}

	var violations []string
	for _, number := range sortedFieldNumbers(frozen) {
		want := frozen[number]
		got := byNumber[number]
		switch {
		case len(got) == 0:
			violations = append(violations, fmt.Sprintf("%s: message %s no longer declares %s at field %d",
				pkg, message.GetName(), want.name, number))
		case len(got) > 1:
			violations = append(violations, fmt.Sprintf("%s: message %s field number %d is used by %s and %s",
				pkg, message.GetName(), number, got[0].GetName(), got[1].GetName()))
		default:
			violations = append(violations, identityViolations(pkg, message.GetName(), number, want, got[0])...)
		}
	}
	return violations
}

// identityViolations reports how one field's frozen identity changed.
func identityViolations(
	pkg, message string,
	number protoreflect.FieldNumber,
	want frozenIdentity,
	got *descriptorpb.FieldDescriptorProto,
) []string {
	switch {
	case got.GetName() != want.name:
		return []string{fmt.Sprintf("%s: message %s field %d is now %s, want %s",
			pkg, message, number, got.GetName(), want.name)}
	case protoreflect.Kind(got.GetType()) != want.kind:
		return []string{fmt.Sprintf("%s: message %s field %d (%s) is now encoded as %s, want %s",
			pkg, message, number, want.name, protoreflect.Kind(got.GetType()), want.kind)}
	case want.typeName != "" && strings.TrimPrefix(got.GetTypeName(), ".") != want.typeName:
		return []string{fmt.Sprintf("%s: message %s field %d (%s) now carries %s, want %s",
			pkg, message, number, want.name, strings.TrimPrefix(got.GetTypeName(), "."), want.typeName)}
	case got.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED:
		return []string{fmt.Sprintf("%s: message %s field %d (%s) is now repeated",
			pkg, message, number, want.name)}
	default:
		return nil
	}
}

// enumViolations reports every frozen enum value the proposal no longer carries at its frozen
// number under its frozen name.
func enumViolations(pkg string, enum *descriptorpb.EnumDescriptorProto, frozen map[protoreflect.EnumNumber]string) []string {
	byNumber := make(map[protoreflect.EnumNumber][]*descriptorpb.EnumValueDescriptorProto, len(enum.GetValue()))
	for _, value := range enum.GetValue() {
		byNumber[protoreflect.EnumNumber(value.GetNumber())] = append(byNumber[protoreflect.EnumNumber(value.GetNumber())], value)
	}

	var violations []string
	for _, number := range sortedEnumNumbers(frozen) {
		want := frozen[number]
		got := byNumber[number]
		switch {
		case len(got) == 0:
			violations = append(violations, fmt.Sprintf("%s: enum %s no longer declares %s at %d",
				pkg, enum.GetName(), want, number))
		case len(got) > 1:
			violations = append(violations, fmt.Sprintf("%s: enum %s number %d is used by %s and %s",
				pkg, enum.GetName(), number, got[0].GetName(), got[1].GetName()))
		case got[0].GetName() != want:
			violations = append(violations, fmt.Sprintf("%s: enum %s value %d is now %s, want %s",
				pkg, enum.GetName(), number, got[0].GetName(), want))
		}
	}
	return violations
}

// methodViolations reports every frozen RPC the proposal no longer carries with its frozen
// name and streaming shape.
func methodViolations(proposal *descriptorpb.FileDescriptorProto, frozen frozenContract) []string {
	var service *descriptorpb.ServiceDescriptorProto
	for _, candidate := range proposal.GetService() {
		if candidate.GetName() == frozen.service {
			service = candidate
			break
		}
	}
	if service == nil {
		return []string{fmt.Sprintf("%s: service %s is gone", frozen.name, frozen.service)}
	}

	byName := make(map[string]*descriptorpb.MethodDescriptorProto, len(service.GetMethod()))
	for _, method := range service.GetMethod() {
		byName[method.GetName()] = method
	}

	var violations []string
	for _, want := range frozen.methods {
		got, ok := byName[want.name]
		if !ok {
			violations = append(violations, fmt.Sprintf("%s: %s.%s is gone",
				frozen.name, frozen.service, want.name))
			continue
		}
		if got.GetClientStreaming() != want.clientStreaming || got.GetServerStreaming() != want.serverStreaming {
			violations = append(violations, fmt.Sprintf("%s: %s.%s is now %s, want %s",
				frozen.name, frozen.service, want.name,
				methodShape(got.GetClientStreaming(), got.GetServerStreaming()),
				methodShape(want.clientStreaming, want.serverStreaming)))
		}
	}
	return violations
}

// methodShape names a method's streaming shape for a failure message.
func methodShape(clientStreaming, serverStreaming bool) string {
	switch {
	case clientStreaming && serverStreaming:
		return "a bidirectional stream"
	case clientStreaming:
		return "a client stream"
	case serverStreaming:
		return "a server stream"
	default:
		return "unary"
	}
}

// sortedEnumNumbers returns an enum-value-keyed map's numbers in order.
func sortedEnumNumbers[V any](m map[protoreflect.EnumNumber]V) []protoreflect.EnumNumber {
	out := make([]protoreflect.EnumNumber, 0, len(m))
	for number := range m {
		out = append(out, number)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// findMessage returns a proposal's message by name, or nil.
func findMessage(proposal *descriptorpb.FileDescriptorProto, name string) *descriptorpb.DescriptorProto {
	for _, message := range proposal.GetMessageType() {
		if message.GetName() == name {
			return message
		}
	}
	return nil
}

// findEnum returns a proposal's enum by name, or nil.
func findEnum(proposal *descriptorpb.FileDescriptorProto, name string) *descriptorpb.EnumDescriptorProto {
	for _, enum := range proposal.GetEnumType() {
		if enum.GetName() == name {
			return enum
		}
	}
	return nil
}

// assertFrozen fails when the live contract no longer carries every frozen number, so a
// renumbering that lands in the tree is rejected by the test that backs the scenario.
func assertFrozen(t *testing.T, file protoreflect.FileDescriptor, frozen frozenContract) {
	t.Helper()

	if diff := cmp.Diff([]string(nil), frozenViolations(protodesc.ToFileDescriptorProto(file), frozen)); diff != "" {
		t.Errorf("%s no longer satisfies its frozen v1 baseline, so a client generated before the change breaks (-want +got):\n%s",
			frozen.name, diff)
	}
}
