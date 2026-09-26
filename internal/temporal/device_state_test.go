package temporal

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/converter"
)

func TestRecentIDs(t *testing.T) {
	t.Parallel()

	t.Run("zero value is usable", func(t *testing.T) {
		t.Parallel()
		var r recentIDs
		if r.has("a") {
			t.Fatal("empty window reported a key as seen")
		}
		r.push("a", 2)
		if !r.has("a") {
			t.Fatal("pushed key not reported as seen")
		}
	})

	t.Run("push keeps a key in place instead of duplicating", func(t *testing.T) {
		t.Parallel()
		var r recentIDs
		r.push("a", 3)
		r.push("b", 3)
		r.push("a", 3)
		if diff := cmp.Diff([]string{"a", "b"}, r.Keys); diff != "" {
			t.Errorf("keys mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("oldest keys evict at the limit", func(t *testing.T) {
		t.Parallel()
		var r recentIDs
		for _, k := range []string{"a", "b", "c", "d"} {
			r.push(k, 3)
		}
		if diff := cmp.Diff([]string{"b", "c", "d"}, r.Keys); diff != "" {
			t.Errorf("keys mismatch (-want +got):\n%s", diff)
		}
		if r.has("a") {
			t.Error("evicted key still reported as seen")
		}
	})

	t.Run("device rings keep their configured bounds", func(t *testing.T) {
		t.Parallel()
		var s deviceState
		for i := 0; i < maxRecentCommandIDs+10; i++ {
			s.applyCommandIssued(CommandIssuedSignal{CommandID: string(rune('a'+i%26)) + string(rune('0'+i/26))})
		}
		if got := len(s.RecentCommandIDs.Keys); got != maxRecentCommandIDs {
			t.Errorf("command ring holds %d keys, want %d", got, maxRecentCommandIDs)
		}
	})
}

func TestDeviceStateZeroValueIsValid(t *testing.T) {
	t.Parallel()
	// The zero state is the state of a device nothing is known about yet: applying signals
	// to it must work without construction.
	var s deviceState
	s.applyHeartbeat(HeartbeatSignal{EventID: "e1", CurrentFw: "fw-1", Timestamp: time.Unix(10, 0)})
	s.applyCommandIssued(CommandIssuedSignal{CommandID: "c1", Kind: CommandKindUpdate})
	s.applyCommandResult(CommandResultSignal{CommandID: "c1", Outcome: OutcomeSucceeded})
	s.applyConfigChanged(ConfigChangedSignal{Version: 1})
	if s.SignalsApplied != 4 {
		t.Errorf("SignalsApplied = %d, want 4", s.SignalsApplied)
	}
}

func TestDeviceStateApply(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	older := base.Add(-time.Second)
	newer := base.Add(time.Second)

	withState := func(setup func(*deviceState)) func(*deviceState) {
		return func(s *deviceState) {
			*s = newDeviceState("dev-1")
			setup(s)
		}
	}
	heard := withState(func(s *deviceState) {
		s.CurrentFw = "fw-1"
		s.LastHeartbeatAt = base
	})
	heardWithPending := withState(func(s *deviceState) {
		s.CurrentFw = "fw-1"
		s.LastHeartbeatAt = base
		s.applyCommandIssued(CommandIssuedSignal{
			CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-2", Version: "fw-2",
		})
	})

	heartbeat := HeartbeatSignal{EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: base}
	issued := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
	result := CommandResultSignal{DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded}
	config := ConfigChangedSignal{DeviceID: "dev-1", Version: 2, Snapshot: json.RawMessage(`{"a":1}`)}

	cases := []struct {
		name    string
		setup   func(*deviceState)
		apply   func(*deviceState)
		want    State
		applied int
	}{
		{
			name:  "heartbeat refreshes liveness",
			setup: newDeviceStateSetup(),
			apply: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-1", Timestamp: newer,
				})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: newer},
			applied: 1,
		},
		{
			name:  "heartbeat reports a firmware change",
			setup: heard,
			apply: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: newer,
				})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-2", LastHeartbeatAt: newer},
			applied: 1,
		},
		{
			name:  "stale heartbeat changes nothing",
			setup: heard,
			apply: func(s *deviceState) {
				s.applyHeartbeat(heartbeat) // equal timestamp
				s.applyHeartbeat(HeartbeatSignal{EventID: "evt-2", CurrentFw: "fw-9", Timestamp: older})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 2,
		},
		{
			name:  "duplicate heartbeat is dropped",
			setup: heard,
			apply: func(s *deviceState) {
				s.applyHeartbeat(heartbeat)
				s.applyHeartbeat(heartbeat)
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 2,
		},
		{
			name:  "heartbeat with empty firmware does not clobber the owned one",
			setup: heard,
			apply: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{EventID: "evt-2", Timestamp: newer})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: newer},
			applied: 1,
		},
		{
			name:  "issued command becomes pending",
			setup: newDeviceStateSetup(),
			apply: func(s *deviceState) { s.applyCommandIssued(issued) },
			want: State{DeviceID: "dev-1", Pending: &PendingCommand{
				Command: issued,
			}},
			applied: 1,
		},
		{
			name:  "newer command supersedes the pending one",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandIssued(CommandIssuedSignal{
					CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				})
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base, Pending: &PendingCommand{
				Command: CommandIssuedSignal{
					CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				},
			}},
			applied: 2,
		},
		{
			name:  "repeated issuance of the same command is a no-op",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandIssued(issued)
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base, Pending: &PendingCommand{
				Command: issued,
			}},
			applied: 2,
		},
		{
			name:  "matching result clears the pending command",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "boom",
				})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 2,
		},
		{
			name:    "success adopts the commanded firmware",
			setup:   heardWithPending,
			apply:   func(s *deviceState) { s.applyCommandResult(result) },
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-2", LastHeartbeatAt: base},
			applied: 2,
		},
		{
			name:  "duplicate command result is dropped",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandResult(result)
				s.applyCommandResult(result)
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-2", LastHeartbeatAt: base},
			applied: 3,
		},
		{
			name:    "result for a non-pending command changes nothing",
			setup:   heard,
			apply:   func(s *deviceState) { s.applyCommandResult(result) },
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 1,
		},
		{
			name:  "abort success leaves firmware unchanged",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandIssued(CommandIssuedSignal{
					CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				})
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-2", Outcome: OutcomeSucceeded,
				})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 3,
		},
		{
			name:  "newer configuration version is applied",
			setup: newDeviceStateSetup(),
			apply: func(s *deviceState) { s.applyConfigChanged(config) },
			want: State{DeviceID: "dev-1", Config: ConfigSnapshot{
				Version: 2, Data: json.RawMessage(`{"a":1}`),
			}},
			applied: 1,
		},
		{
			name:  "older or repeated configuration version is ignored",
			setup: newDeviceStateSetup(),
			apply: func(s *deviceState) {
				s.applyConfigChanged(config)
				s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 1, Snapshot: json.RawMessage(`{"b":2}`)})
				s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 2, Snapshot: json.RawMessage(`{"c":3}`)})
			},
			want: State{DeviceID: "dev-1", Config: ConfigSnapshot{
				Version: 2, Data: json.RawMessage(`{"a":1}`),
			}},
			applied: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newDeviceState("dev-1")
			tc.setup(&s)
			tc.apply(&s)
			if diff := cmp.Diff(tc.want, s.view()); diff != "" {
				t.Errorf("state view mismatch (-want +got):\n%s", diff)
			}
			if s.SignalsApplied != tc.applied {
				t.Errorf("SignalsApplied = %d, want %d", s.SignalsApplied, tc.applied)
			}
		})
	}
}

// newDeviceStateSetup returns the identity setup so table cases can share the fresh-state
// baseline without sharing the state itself.
func newDeviceStateSetup() func(*deviceState) {
	return func(s *deviceState) {
		*s = newDeviceState("dev-1")
	}
}

func TestDeviceStateValidate(t *testing.T) {
	t.Parallel()

	t.Run("current carry version passes", func(t *testing.T) {
		t.Parallel()
		if err := newDeviceState("dev-1").validate(); err != nil {
			t.Fatalf("validate() on fresh state: %v", err)
		}
	})

	t.Run("unknown carry version fails loudly", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1")
		s.CarryVersion = carryVersion + 1
		err := s.validate()
		if !errors.Is(err, ErrUnsupportedCarryVersion) {
			t.Fatalf("validate() error = %v, want ErrUnsupportedCarryVersion", err)
		}
	})

	t.Run("missing device id fails loudly", func(t *testing.T) {
		t.Parallel()
		if err := (deviceState{CarryVersion: carryVersion}).validate(); err == nil {
			t.Fatal("validate() on state without device id returned nil")
		}
	})
}

// TestCarryRoundTrip pins the continuation contract: the carried-over payload encodes and
// decodes losslessly, dedup memory included, and a state at an unknown carry version is
// refused on the way back in.
func TestCarryRoundTrip(t *testing.T) {
	t.Parallel()

	s := newDeviceState("dev-1")
	s.CurrentFw = "fw-2"
	s.LastHeartbeatAt = time.Unix(123456, 789).UTC()
	s.Pending = &PendingCommand{
		Command:    CommandIssuedSignal{CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate, FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum"},
		Dispatched: true,
	}
	s.Config = ConfigSnapshot{Version: 3, Data: json.RawMessage(`{"interval":"5s"}`)}
	s.RecentEventIDs.push("evt-1", maxRecentEventIDs)
	s.RecentCommandIDs.push("cmd-1", maxRecentCommandIDs)
	s.SignalsApplied = 7

	dc := converter.GetDefaultDataConverter()
	payloads, err := dc.ToPayloads(s)
	if err != nil {
		t.Fatalf("encode carry state: %v", err)
	}
	var decoded deviceState
	if err := dc.FromPayloads(payloads, &decoded); err != nil {
		t.Fatalf("decode carry state: %v", err)
	}
	if diff := cmp.Diff(s, decoded); diff != "" {
		t.Errorf("carry round trip lost state (-want +got):\n%s", diff)
	}
	if err := decoded.validate(); err != nil {
		t.Fatalf("validate() decoded state: %v", err)
	}

	decoded.CarryVersion = carryVersion + 1
	if err := decoded.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
		t.Errorf("validate() at carry version %d = %v, want ErrUnsupportedCarryVersion", decoded.CarryVersion, err)
	}
}
