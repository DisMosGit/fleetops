package protocodegen

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// topLevelDeclRE matches a contract's top-level blocks. The contracts declare one block per
// message, enum, and service, so the next one is where the current block's text ends.
var topLevelDeclRE = regexp.MustCompile(`(?m)^(?:message|enum|service)\s`)

// messageBody returns the declared text of one top-level message, so a field check is anchored
// to the message that declares the field rather than to any same-named field elsewhere. A
// message nested inside another would be read as part of its parent; the generated-descriptor
// view of the guard rejects what that misses.
func messageBody(src, message string) (string, bool) {
	declaration := regexp.MustCompile(`(?m)^message\s+` + regexp.QuoteMeta(message) + `\s*\{`)
	loc := declaration.FindStringIndex(src)
	if loc == nil {
		return "", false
	}
	body := src[loc[1]:]
	if next := topLevelDeclRE.FindStringIndex(body); next != nil {
		body = body[:next[0]]
	}
	return body, true
}

// sourceFrozenViolations reports every frozen field the contract source no longer declares at
// its frozen number. The source is checked as well as the descriptors because a proposal is
// rejected when it is written: the stubs are build artifacts, and the scenario rejects a
// renumbering regardless of whether the tree still compiles.
func sourceFrozenViolations(src string, frozen frozenContract) []string {
	var violations []string
	for _, name := range sortedKeys(frozen.messages) {
		body, ok := messageBody(src, name)
		if !ok {
			violations = append(violations, fmt.Sprintf("%s: message %s is not declared", frozen.name, name))
			continue
		}
		for _, number := range sortedFieldNumbers(frozen.messages[name]) {
			want := frozen.messages[name][number]
			declaration := regexp.MustCompile(fmt.Sprintf(`\b%s\s*=\s*%d\s*;`, regexp.QuoteMeta(want.name), number))
			if !declaration.MatchString(body) {
				violations = append(violations, fmt.Sprintf("%s: message %s no longer declares %s at field %d",
					frozen.name, name, want.name, number))
			}
		}
	}
	sort.Strings(violations)
	return violations
}

// TestRenumberingIsRejected backs the agent contract's "Renumbering is rejected": a proposed
// change that reuses or reorders an existing field number is rejected as a v1 violation
// whether or not the current tree still compiles. The guard runs at both points a proposal can
// be seen — the contract source before regeneration, and the descriptors the generated stubs
// ship — and each proposal is rejected for the number it moved. It is the contract's freeze
// rule rather than a number-only check, so the table also covers the enum renumbering and RPC
// reshaping that break a generated client the same way. A clean contract and an addition at a
// fresh number both pass, so the rejection is about renumbering rather than about change.
func TestRenumberingIsRejected(t *testing.T) {
	t.Parallel()

	const contract = "api/proto/agent/v1/agent.proto"
	file := (&agentv1.Heartbeat{}).ProtoReflect().Descriptor().ParentFile()
	live := protodesc.ToFileDescriptorProto(file)
	src, err := os.ReadFile(filepath.Join(repoRoot, contract))
	if err != nil {
		t.Fatalf("read %s: %v", contract, err)
	}

	t.Run("the live contract keeps every frozen number", func(t *testing.T) {
		t.Parallel()

		assertFrozen(t, file, agentFrozen)

		if diff := cmp.Diff([]string(nil), sourceFrozenViolations(string(src), agentFrozen)); diff != "" {
			t.Errorf("%s no longer declares its frozen fields at their frozen numbers (-want +got):\n%s", contract, diff)
		}
	})

	// Each proposal applies what a contract change would do to an editable copy of the live
	// contract. A guard that rejected every change would pass a renumbering check without
	// backing the scenario, so every case also names the field number it wants reported.
	proposals := []struct {
		name   string
		mutate func(t *testing.T, proposal *descriptorpb.FileDescriptorProto)
		want   string
	}{
		{
			name: "a field moves to a fresh number",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				fieldIn(t, messageIn(t, proposal, "Heartbeat"), 8).Number = proto.Int32(9)
			},
			want: "Heartbeat no longer declares health at field 8",
		},
		{
			// Both fields are strings, so this proposal compiles and every generated client
			// silently reads version where checksum was written: the change the scenario calls
			// rejected regardless of whether the current code compiles.
			name: "two fields swap numbers",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				response := messageIn(t, proposal, "FirmwareDownloadResponse")
				version, checksum := fieldIn(t, response, 2), fieldIn(t, response, 3)
				version.Number, checksum.Number = proto.Int32(3), proto.Int32(2)
			},
			want: "FirmwareDownloadResponse field 2 is now checksum, want version",
		},
		{
			name: "an addition reuses a number a field gave up",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				heartbeat := messageIn(t, proposal, "Heartbeat")
				fieldIn(t, heartbeat, 8).Number = proto.Int32(9)
				heartbeat.Field = append(heartbeat.Field, proto3Field("health_bucket", 8, protoreflect.Int32Kind))
			},
			want: "Heartbeat field 8 is now health_bucket, want health",
		},
		{
			name: "two fields claim one number",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				heartbeat := messageIn(t, proposal, "Heartbeat")
				heartbeat.Field = append(heartbeat.Field, proto3Field("health_bucket", 8, protoreflect.Int32Kind))
			},
			want: "Heartbeat field number 8 is used by",
		},
		{
			name: "a field is retyped",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				fieldIn(t, messageIn(t, proposal, "Heartbeat"), 6).Type = descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum()
			},
			want: "Heartbeat field 6 (cpu) is now encoded as float, want double",
		},
		{
			name: "an enum value moves to a fresh number",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				for _, value := range findEnum(proposal, "UpdatePhase").GetValue() {
					if value.GetName() == "UPDATE_PHASE_FAILED" {
						value.Number = proto.Int32(7)
						return
					}
				}
				t.Fatal("UpdatePhase has no UPDATE_PHASE_FAILED to move")
			},
			want: "enum UpdatePhase no longer declares UPDATE_PHASE_FAILED at 5",
		},
		{
			name: "an RPC is reshaped",
			mutate: func(t *testing.T, proposal *descriptorpb.FileDescriptorProto) {
				for _, service := range proposal.GetService() {
					for _, method := range service.GetMethod() {
						if method.GetName() == "DownloadFirmware" {
							method.ServerStreaming = proto.Bool(false)
							return
						}
					}
				}
				t.Fatal("AgentService has no DownloadFirmware to reshape")
			},
			want: "AgentService.DownloadFirmware is now unary, want a server stream",
		},
	}

	for _, tc := range proposals {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			proposal := proto.Clone(live).(*descriptorpb.FileDescriptorProto)
			tc.mutate(t, proposal)
			if proto.Equal(proposal, live) {
				t.Fatal("the proposal changed nothing, so the check would pass without backing the scenario")
			}

			violations := frozenViolations(proposal, agentFrozen)
			if len(violations) == 0 {
				t.Fatal("the renumbering proposal was accepted; a v1 violation must be rejected")
			}
			if !slices.ContainsFunc(violations, func(violation string) bool { return strings.Contains(violation, tc.want) }) {
				t.Errorf("the rejection does not name %q, so it may reject something other than the renumbering:\n%s",
					tc.want, strings.Join(violations, "\n"))
			}
		})
	}

	t.Run("an addition at a fresh number is accepted", func(t *testing.T) {
		t.Parallel()

		proposal := proto.Clone(live).(*descriptorpb.FileDescriptorProto)
		message := messageIn(t, proposal, "Heartbeat")
		message.Field = append(message.Field, proto3Field("region", 9, protoreflect.StringKind))

		if diff := cmp.Diff([]string(nil), frozenViolations(proposal, agentFrozen)); diff != "" {
			t.Errorf("an addition at a fresh number was rejected, so the guard rejects change rather than renumbering (-want +got):\n%s", diff)
		}
	})

	t.Run("the contract source rejects a renumbering before regeneration", func(t *testing.T) {
		t.Parallel()

		// The proposal in its written form: the frozen declaration itself moves. The generated
		// stubs are untouched here, which is the point — the source view rejects the change the
		// moment it is written, not once it has been regenerated and compiles.
		renumbered := strings.Replace(string(src), "double health = 8;", "double health = 9;", 1)
		if renumbered == string(src) {
			t.Fatal("the frozen declaration of Heartbeat.health moved or changed shape; the case no longer identifies the behavior")
		}
		violations := sourceFrozenViolations(renumbered, agentFrozen)
		if !slices.ContainsFunc(violations, func(violation string) bool {
			return strings.Contains(violation, "Heartbeat no longer declares health at field 8")
		}) {
			t.Errorf("the renumbered contract source was accepted or misreported; want a violation naming Heartbeat field 8:\n%s",
				strings.Join(violations, "\n"))
		}

		// Control: the source view accepts the addition v1 allows, so the rejection above is
		// about the moved number rather than about editing the contract.
		added := strings.Replace(string(src), "double health = 8;", "double health = 8;\n  string region = 9;", 1)
		if diff := cmp.Diff([]string(nil), sourceFrozenViolations(added, agentFrozen)); diff != "" {
			t.Errorf("an addition at a fresh number was rejected (-want +got):\n%s", diff)
		}
	})
}
