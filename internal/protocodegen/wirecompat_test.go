package protocodegen

import (
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// evolutionCase is one frozen message plus the change that extends it: the field the change
// adds, a client's values for the frozen fields, and the value the updated contract sends in
// the added field. The fields a client generated before the change speaks are its baseline.
type evolutionCase struct {
	// file is the contract's live descriptor.
	file protoreflect.FileDescriptor
	// message is the message the change extends.
	message protoreflect.Name
	// baseline is the message as it existed before the addition: number -> frozen identity.
	baseline map[protoreflect.FieldNumber]frozenIdentity
	// addedNumber and addedIdentity are the field the change adds. The case is a synthetic
	// addition when the live contract does not carry the number yet, and the already-landed
	// evolution when it does; either way the number must be fresh.
	addedNumber   protoreflect.FieldNumber
	addedIdentity frozenIdentity
	// landed says the addition is already part of the live contract, so the case checks an
	// evolution as it shipped rather than applying one. A landed addition that is gone fails:
	// without it the case would degrade into a round trip of the unextended message.
	landed bool
	// addedValue is the value the updated contract sends in the added field. An old client
	// must keep reading every baseline field and must not read the addition as one of them.
	addedValue any
	// values is one value per baseline field: what a client generated before the change sends.
	values map[protoreflect.FieldNumber]any
	// decoded returns the generated message the updated server decodes a payload into.
	decoded func() proto.Message
}

// assertAdditiveEvolution produces the scenario's trigger — a frozen message that gained a
// field, and a client generated before the gain — and asserts its outcome on the wire in both
// directions: the old client's payload still decodes in the updated generated code, and the
// updated contract's payload still decodes in the old client's schema with the addition
// ignored. A message whose frozen fields moved fails before any wire comparison, so the two
// directions cannot pass by agreeing on the same moved number.
func assertAdditiveEvolution(t *testing.T, tc evolutionCase) {
	t.Helper()

	if _, frozen := tc.baseline[tc.addedNumber]; frozen {
		t.Fatalf("field %d is a frozen field of %s, so the case does not describe an addition", tc.addedNumber, tc.message)
	}
	assertFrozen(t, tc.file, frozenContract{
		name:     fmt.Sprintf("%s.%s", tc.file.Package(), tc.message),
		messages: map[string]map[protoreflect.FieldNumber]frozenIdentity{string(tc.message): tc.baseline},
	})

	// The changed contract: the live one with the addition in place. A synthetic addition is
	// appended the way a contract change would write it; an addition that already landed is
	// checked in place, and fails when it is gone.
	proposal := protodesc.ToFileDescriptorProto(tc.file)
	message := messageIn(t, proposal, string(tc.message))
	if _, ok := findFieldNumber(message, tc.addedNumber); !ok {
		if tc.landed {
			t.Fatalf("%s: the evolution added %s at field %d, but the live contract no longer carries it",
				tc.message, tc.addedIdentity.name, tc.addedNumber)
		}
		message.Field = append(message.Field, proto3Field(tc.addedIdentity.name, tc.addedNumber, tc.addedIdentity.kind))
	}
	updated := buildFile(t, proposal).Messages().ByName(tc.message)
	added := updated.Fields().ByNumber(tc.addedNumber)
	if added == nil {
		t.Fatalf("%s: the changed contract has no field %d, so the change under test is not there", tc.message, tc.addedNumber)
	}
	if added.Name() != protoreflect.Name(tc.addedIdentity.name) || added.Kind() != tc.addedIdentity.kind {
		t.Fatalf("%s field %d is %s (%s), want %s (%s)", tc.message, tc.addedNumber,
			added.Name(), added.Kind(), tc.addedIdentity.name, tc.addedIdentity.kind)
	}

	// The client generated before the change knows exactly the frozen fields, and nothing of
	// the addition.
	old := legacyMessage(t, proposal, tc)

	t.Run("an old client's payload still decodes in the updated server", func(t *testing.T) {
		t.Parallel()

		wire := marshalValues(t, dynamicpb.NewMessage(old), tc.values)
		server := tc.decoded()
		if err := proto.Unmarshal(wire, server); err != nil {
			t.Fatalf("the updated server refused an old client's payload: %v", err)
		}
		if unknown := server.ProtoReflect().GetUnknown(); len(unknown) > 0 {
			t.Errorf("the updated server read %d bytes of the old client's payload as unknown fields, so a frozen field moved", len(unknown))
		}
		if diff := cmp.Diff(tc.values, readValues(t, server.ProtoReflect(), tc.values), protocmp.Transform()); diff != "" {
			t.Errorf("the updated server read the old client's payload differently (-want +got):\n%s", diff)
		}
		// A synthetic addition is not in the generated message at all; a landed one is, and the
		// old client's payload must leave it at its default rather than filling it.
		if landed := server.ProtoReflect().Descriptor().Fields().ByNumber(tc.addedNumber); landed != nil {
			if got := server.ProtoReflect().Get(landed); !got.Equal(landed.Default()) {
				t.Errorf("the old client's payload carried the added field %d as %v; an old client cannot have sent it", tc.addedNumber, got)
			}
		}
	})

	t.Run("the updated server's payload still decodes in an old client", func(t *testing.T) {
		t.Parallel()

		sent := make(map[protoreflect.FieldNumber]any, len(tc.values)+1)
		for number, value := range tc.values {
			sent[number] = value
		}
		sent[tc.addedNumber] = tc.addedValue

		oldMessage := dynamicpb.NewMessage(old)
		if err := proto.Unmarshal(marshalValues(t, dynamicpb.NewMessage(updated), sent), oldMessage); err != nil {
			t.Fatalf("an old client refused the updated server's payload: %v", err)
		}
		if diff := cmp.Diff(tc.values, readValues(t, oldMessage, tc.values), protocmp.Transform()); diff != "" {
			t.Errorf("the old client read the updated payload differently (-want +got):\n%s", diff)
		}

		// The addition must be the only field the old client does not know: a frozen field
		// renumbered would surface here as a frozen number the old client no longer reads.
		want := []protowire.Number{protowire.Number(tc.addedNumber)}
		if diff := cmp.Diff(want, unknownNumbers(t, oldMessage.GetUnknown())); diff != "" {
			t.Errorf("the old client saw unknown fields other than the addition (-want +got):\n%s", diff)
		}
	})
}

// legacyMessage returns the schema a client generated before the addition speaks: the changed
// contract's fields restricted to the frozen numbers, so the client knows exactly the fields
// that existed when it was generated.
func legacyMessage(
	t *testing.T,
	proposal *descriptorpb.FileDescriptorProto,
	tc evolutionCase,
) protoreflect.MessageDescriptor {
	t.Helper()

	edited := proto.Clone(proposal).(*descriptorpb.FileDescriptorProto)
	editedMessage := messageIn(t, edited, string(tc.message))
	kept := make([]*descriptorpb.FieldDescriptorProto, 0, len(tc.baseline))
	for _, field := range editedMessage.GetField() {
		if _, frozen := tc.baseline[protoreflect.FieldNumber(field.GetNumber())]; frozen {
			kept = append(kept, field)
		}
	}
	editedMessage.Field = kept

	old := buildFile(t, edited).Messages().ByName(tc.message)
	if old == nil {
		t.Fatalf("the pre-change schema lost message %s", tc.message)
	}
	if field := old.Fields().ByNumber(tc.addedNumber); field != nil {
		t.Fatalf("the pre-change client's schema knows field %d (%s); the case does not describe an addition",
			tc.addedNumber, field.Name())
	}
	return old
}

// buildFile resolves an edited contract into the descriptors the proto runtime encodes and
// decodes with, so a proposal is exercised as a client and server would see it.
func buildFile(t *testing.T, proposal *descriptorpb.FileDescriptorProto) protoreflect.FileDescriptor {
	t.Helper()

	file, err := protodesc.NewFile(proposal, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("the proposal does not describe a valid contract: %v", err)
	}
	return file
}

// messageIn returns an editable proposal's message by name.
func messageIn(t *testing.T, proposal *descriptorpb.FileDescriptorProto, name string) *descriptorpb.DescriptorProto {
	t.Helper()

	message := findMessage(proposal, name)
	if message == nil {
		t.Fatalf("%s: no message %s in the proposal", proposal.GetName(), name)
	}
	return message
}

// fieldIn returns an editable message's field by number.
func fieldIn(t *testing.T, message *descriptorpb.DescriptorProto, number protoreflect.FieldNumber) *descriptorpb.FieldDescriptorProto {
	t.Helper()

	field, ok := findFieldNumber(message, number)
	if !ok {
		t.Fatalf("%s: no field %d", message.GetName(), number)
	}
	return field
}

// findFieldNumber returns an editable message's field by number and reports whether it exists.
func findFieldNumber(
	message *descriptorpb.DescriptorProto,
	number protoreflect.FieldNumber,
) (*descriptorpb.FieldDescriptorProto, bool) {
	for _, field := range message.GetField() {
		if field.GetNumber() == int32(number) {
			return field, true
		}
	}
	return nil, false
}

// proto3Field writes a singular proto3 field the way a contract change would declare it. The
// proto field type shares its numbering with the reflection kind, so the kind maps directly.
func proto3Field(name string, number protoreflect.FieldNumber, kind protoreflect.Kind) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(int32(number)),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   descriptorpb.FieldDescriptorProto_Type(kind).Enum(),
	}
}

// marshalValues encodes one value per field with the given schema, so a payload can be built
// as the client or server that owns that schema would put it on the wire.
func marshalValues(t *testing.T, message protoreflect.Message, values map[protoreflect.FieldNumber]any) []byte {
	t.Helper()

	for _, number := range sortedFieldNumbers(values) {
		field := message.Descriptor().Fields().ByNumber(number)
		if field == nil {
			t.Fatalf("%s: no field %d to send", message.Descriptor().FullName(), number)
		}
		message.Set(field, fieldValue(t, field, values[number]))
	}
	wire, err := proto.Marshal(message.Interface())
	if err != nil {
		t.Fatalf("marshal %s: %v", message.Descriptor().FullName(), err)
	}
	return wire
}

// fieldValue converts a table value into the wire value for a field, so the tables read as the
// values a client sends rather than as protobuf internals.
func fieldValue(t *testing.T, field protoreflect.FieldDescriptor, value any) protoreflect.Value {
	t.Helper()

	switch typed := value.(type) {
	case string:
		return protoreflect.ValueOfString(typed)
	case bool:
		return protoreflect.ValueOfBool(typed)
	case int64:
		return protoreflect.ValueOfInt64(typed)
	case float64:
		return protoreflect.ValueOfFloat64(typed)
	case []byte:
		return protoreflect.ValueOfBytes(typed)
	case protoreflect.EnumNumber:
		return protoreflect.ValueOfEnum(typed)
	case proto.Message:
		return protoreflect.ValueOfMessage(typed.ProtoReflect())
	default:
		t.Fatalf("%s: no wire value for %T", field.FullName(), value)
		return protoreflect.Value{}
	}
}

// readValues reads the table's fields back out of a decoded message, so both directions compare
// the same values regardless of which message implementation did the decoding.
func readValues(t *testing.T, message protoreflect.Message, want map[protoreflect.FieldNumber]any) map[protoreflect.FieldNumber]any {
	t.Helper()

	out := make(map[protoreflect.FieldNumber]any, len(want))
	for _, number := range sortedFieldNumbers(want) {
		field := message.Descriptor().Fields().ByNumber(number)
		if field == nil {
			t.Fatalf("%s: no field %d to read", message.Descriptor().FullName(), number)
		}
		out[number] = readField(message.Get(field), field)
	}
	return out
}

// readField turns a read-back field value into a form cmp.Diff compares across message
// implementations: a message field compares as the message itself, through protocmp.
func readField(value protoreflect.Value, field protoreflect.FieldDescriptor) any {
	switch field.Kind() {
	case protoreflect.StringKind:
		return value.String()
	case protoreflect.BoolKind:
		return value.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int()
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float()
	case protoreflect.BytesKind:
		return value.Bytes()
	case protoreflect.EnumKind:
		return value.Enum()
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return value.Message().Interface()
	default:
		return value.Interface()
	}
}

// unknownNumbers lists the field numbers a decode did not recognize, in order, so a test can
// assert exactly which fields an older schema saw as unknown.
func unknownNumbers(t *testing.T, raw []byte) []protowire.Number {
	t.Helper()

	var out []protowire.Number
	for len(raw) > 0 {
		number, _, consumed := protowire.ConsumeField(raw)
		if consumed < 0 {
			t.Fatalf("unknown fields are malformed: %v", protowire.ParseError(consumed))
		}
		out = append(out, number)
		raw = raw[consumed:]
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
