package temporal

import (
	"encoding/json"
	"errors"
	"strings"
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
			*s = newDeviceState("dev-1", testSettings())
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
	// A restore commands the version the device ran before; a redeploy commands the one it
	// already runs. Both are ordinary update commands for the entity.
	restore := CommandIssuedSignal{
		CommandID: "cmd-restore", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-1", Version: "fw-1",
	}
	redeploy := CommandIssuedSignal{
		CommandID: "cmd-redeploy", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
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
			want: State{DeviceID: "dev-1", CurrentFw: "fw-2", PreviousFw: "fw-1", LastHeartbeatAt: newer},
			// A device that never reported a version carries none either: the version being
			// left was the empty one.
			applied: 1,
		},
		{
			name: "a report naming the current version moves neither version",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.CurrentFw, s.PreviousFw = "fw-2", "fw-1"
				s.LastHeartbeatAt = base
			},
			apply: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-2", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: newer,
				})
			},
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-2", PreviousFw: "fw-1", LastHeartbeatAt: newer},
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
			name:  "matching result clears the pending command and records the conclusion",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "boom",
				})
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{
					Command: issued, Outcome: OutcomeFailed, Detail: "boom",
				}},
			applied: 2,
		},
		{
			name:  "success adopts the commanded firmware and records the conclusion",
			setup: heardWithPending,
			apply: func(s *deviceState) { s.applyCommandResult(result) },
			want: State{DeviceID: "dev-1", CurrentFw: "fw-2", PreviousFw: "fw-1", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{Command: issued, Outcome: OutcomeSucceeded}},
			applied: 2,
		},
		{
			name:  "duplicate command result is dropped",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandResult(result)
				s.applyCommandResult(result)
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-2", PreviousFw: "fw-1", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{Command: issued, Outcome: OutcomeSucceeded}},
			applied: 3,
		},
		{
			name: "a restore moves the previous version too",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.CurrentFw, s.PreviousFw = "fw-2", "fw-1"
				s.LastHeartbeatAt = base
				s.applyCommandIssued(restore)
			},
			apply: func(s *deviceState) {
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: restore.CommandID, Outcome: OutcomeSucceeded,
				})
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-1", PreviousFw: "fw-2", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{Command: restore, Outcome: OutcomeSucceeded}},
			applied: 2,
		},
		{
			name: "a command for the version already running moves neither",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.CurrentFw, s.PreviousFw = "fw-2", "fw-1"
				s.LastHeartbeatAt = base
				s.applyCommandIssued(redeploy)
			},
			apply: func(s *deviceState) {
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: redeploy.CommandID, Outcome: OutcomeSucceeded,
				})
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-2", PreviousFw: "fw-1", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{Command: redeploy, Outcome: OutcomeSucceeded}},
			applied: 2,
		},
		{
			name:    "result for a non-pending command changes nothing",
			setup:   heard,
			apply:   func(s *deviceState) { s.applyCommandResult(result) },
			want:    State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base},
			applied: 1,
		},
		{
			name:  "successful abort leaves firmware unchanged and is recorded",
			setup: heardWithPending,
			apply: func(s *deviceState) {
				s.applyCommandIssued(CommandIssuedSignal{
					CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				})
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-2", Outcome: OutcomeSucceeded,
				})
			},
			want: State{DeviceID: "dev-1", CurrentFw: "fw-1", LastHeartbeatAt: base,
				LastCommand: &ConcludedCommand{
					Command: CommandIssuedSignal{
						CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
					},
					Outcome: OutcomeSucceeded,
				}},
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
			s := newDeviceState("dev-1", testSettings())
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

// testSettings returns valid entity settings for tests: anything positive works, but every
// test shares one value so comparisons read the same everywhere.
func testSettings() DeviceSettings {
	return DeviceSettings{
		SnapshotInterval:  time.Minute,
		OfflineThreshold:  30 * time.Second,
		DispatchTaskQueue: "fleetops-controlplane",
	}
}

// newDeviceStateSetup returns the identity setup so table cases can share the fresh-state
// baseline without sharing the state itself.
func newDeviceStateSetup() func(*deviceState) {
	return func(s *deviceState) {
		*s = newDeviceState("dev-1", testSettings())
	}
}

func TestDeviceStateValidate(t *testing.T) {
	t.Parallel()

	t.Run("current carry version passes", func(t *testing.T) {
		t.Parallel()
		if err := newDeviceState("dev-1", testSettings()).validate(); err != nil {
			t.Fatalf("validate() on fresh state: %v", err)
		}
	})

	t.Run("unknown carry version fails loudly", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
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

	t.Run("settings without positive snapshot interval fails loudly", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
		s.Settings.SnapshotInterval = 0
		if err := s.validate(); err == nil {
			t.Fatal("validate() on a zero snapshot interval returned nil")
		}
	})

	t.Run("settings without positive offline threshold fails loudly", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
		s.Settings.OfflineThreshold = -time.Second
		if err := s.validate(); err == nil {
			t.Fatal("validate() on a negative offline threshold returned nil")
		}
	})

	t.Run("settings without dispatch task queue fails loudly", func(t *testing.T) {
		t.Parallel()
		// An empty queue leaves the dispatch activity unschedulable rather than failing
		// anything, so it is refused at run start instead of silently stranding commands.
		s := newDeviceState("dev-1", testSettings())
		s.Settings.DispatchTaskQueue = ""
		err := s.validate()
		if err == nil {
			t.Fatal("validate() on an empty dispatch task queue returned nil")
		}
		if !strings.Contains(err.Error(), "dispatch task queue") {
			t.Errorf("validate() error = %q, want it to name the dispatch task queue", err)
		}
	})

	t.Run("carry version 1 payload is refused", func(t *testing.T) {
		t.Parallel()
		// A state as the previous build carried it: no identity, liveness, or settings
		// fields at all. It must fail loudly instead of running half-understood.
		const v1 = `{"carry_version":1,"device_id":"dev-1","current_fw":"fw-1"}`
		var s deviceState
		if err := json.Unmarshal([]byte(v1), &s); err != nil {
			t.Fatalf("decode v1 payload: %v", err)
		}
		if err := s.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
			t.Errorf("validate() on a v1 payload = %v, want ErrUnsupportedCarryVersion", err)
		}
	})

	t.Run("carry version 3 payload is refused", func(t *testing.T) {
		t.Parallel()
		// The schema before this change: complete in every field it knew, but with no way
		// to say what a device's last command concluded. Accepting it would leave every
		// in-flight device unable to answer the update wait, so it is refused loudly.
		const v3 = `{"carry_version":3,"device_id":"dev-1","current_fw":"fw-1",` +
			`"settings":{"snapshot_interval":60000000000,"offline_threshold":30000000000}}`
		var s deviceState
		if err := json.Unmarshal([]byte(v3), &s); err != nil {
			t.Fatalf("decode v3 payload: %v", err)
		}
		if s.LastCommand != nil {
			t.Errorf("a v3 payload decoded a last command: %+v", s.LastCommand)
		}
		if err := s.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
			t.Errorf("validate() on a v3 payload = %v, want ErrUnsupportedCarryVersion", err)
		}
	})

	t.Run("carry version 4 payload is refused", func(t *testing.T) {
		t.Parallel()
		// The schema before this change: complete in every field it knew, but with no record
		// of the firmware a device ran before its current one. Accepting it would leave a
		// rollback with nothing to restore its in-flight devices to, so it is refused loudly.
		const v4 = `{"carry_version":4,"device_id":"dev-1","current_fw":"fw-2",` +
			`"settings":{"snapshot_interval":60000000000,"offline_threshold":30000000000}}`
		var s deviceState
		if err := json.Unmarshal([]byte(v4), &s); err != nil {
			t.Fatalf("decode v4 payload: %v", err)
		}
		if s.PreviousFw != "" {
			t.Errorf("a v4 payload decoded a previous firmware version: %q", s.PreviousFw)
		}
		if err := s.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
			t.Errorf("validate() on a v4 payload = %v, want ErrUnsupportedCarryVersion", err)
		}
	})

	t.Run("carry version 5 payload is refused", func(t *testing.T) {
		t.Parallel()
		// The schema before this change: every field it knew, but settings that name only the
		// snapshot cadence and the offline threshold. Accepting it would schedule the device's
		// commands on an empty task queue, where nothing polls, so it is refused loudly.
		const v5 = `{"carry_version":5,"device_id":"dev-1","current_fw":"fw-2","previous_fw":"fw-1",` +
			`"settings":{"snapshot_interval":60000000000,"offline_threshold":30000000000}}`
		var s deviceState
		if err := json.Unmarshal([]byte(v5), &s); err != nil {
			t.Fatalf("decode v5 payload: %v", err)
		}
		if s.Settings.DispatchTaskQueue != "" {
			t.Errorf("a v5 payload decoded a dispatch task queue: %q", s.Settings.DispatchTaskQueue)
		}
		if err := s.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
			t.Errorf("validate() on a v5 payload = %v, want ErrUnsupportedCarryVersion", err)
		}
	})
}

// TestDeviceStateIdentityAdoption pins how the identity attributes are adopted: non-empty
// registration data in a heartbeat wins, empty values keep the owned ones, and a heartbeat
// that is not strictly newer changes nothing at all.
func TestDeviceStateIdentityAdoption(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	cases := []struct {
		name  string
		setup func(*deviceState)
		apply HeartbeatSignal
		want  State
		tr    transitions
	}{
		{
			name:  "registration data is adopted",
			setup: newDeviceStateSetup(),
			apply: HeartbeatSignal{
				EventID: "evt-1", DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
				Timestamp: base,
			},
			want: State{DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3", LastHeartbeatAt: base},
			tr:   transitions{Identity: true},
		},
		{
			name: "empty identity keeps the owned one",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.Region, s.Model = "eu-west", "oak-s3"
				s.LastHeartbeatAt = base
			},
			apply: HeartbeatSignal{EventID: "evt-2", DeviceID: "dev-1", Timestamp: base.Add(time.Second)},
			want: State{
				DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
				LastHeartbeatAt: base.Add(time.Second),
			},
			tr: transitions{},
		},
		{
			name: "unchanged identity is not a transition",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.Region, s.Model = "eu-west", "oak-s3"
				s.LastHeartbeatAt = base
			},
			apply: HeartbeatSignal{
				EventID: "evt-2", DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
				Timestamp: base.Add(time.Second),
			},
			want: State{
				DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3",
				LastHeartbeatAt: base.Add(time.Second),
			},
			tr: transitions{},
		},
		{
			name: "stale heartbeat changes nothing",
			setup: func(s *deviceState) {
				*s = newDeviceState("dev-1", testSettings())
				s.Region, s.Model = "eu-west", "oak-s3"
				s.LastHeartbeatAt = base
			},
			apply: HeartbeatSignal{
				EventID: "evt-2", DeviceID: "dev-1", Region: "us-east", Model: "oak-s9",
				Timestamp: base.Add(-time.Second),
			},
			want: State{
				DeviceID: "dev-1", Region: "eu-west", Model: "oak-s3", LastHeartbeatAt: base,
			},
			tr: transitions{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newDeviceState("dev-1", testSettings())
			tc.setup(&s)
			if diff := cmp.Diff(tc.tr, s.applyHeartbeat(tc.apply)); diff != "" {
				t.Errorf("transitions mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want, s.view()); diff != "" {
				t.Errorf("state view mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDeviceStateTransitions pins the transition report each apply produces, and the two
// scheduling questions the workflow asks of it: what snapshots immediately, what upserts
// search attributes.
func TestDeviceStateTransitions(t *testing.T) {
	t.Parallel()

	pending := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
	fresh := func(s *deviceState) {
		*s = newDeviceState("dev-1", testSettings())
		s.CurrentFw, s.LastHeartbeatAt = "fw-1", time.Unix(1000, 0)
	}
	withPending := func(s *deviceState) {
		fresh(s)
		s.applyCommandIssued(pending)
	}

	cases := []struct {
		name  string
		setup func(*deviceState)
		apply func(*deviceState) transitions
		want  transitions
	}{
		{
			name:  "firmware adoption",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				return s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: time.Unix(2000, 0),
				})
			},
			want: transitions{Firmware: true},
		},
		{
			name:  "timestamp-only heartbeat",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				return s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", Timestamp: time.Unix(2000, 0),
				})
			},
			want: transitions{},
		},
		{
			name:  "duplicate heartbeat",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: time.Unix(2000, 0),
				})
				return s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-2", Timestamp: time.Unix(2000, 0),
				})
			},
			want: transitions{},
		},
		{
			name:  "command set",
			setup: fresh,
			apply: func(s *deviceState) transitions { return s.applyCommandIssued(pending) },
			want:  transitions{Pending: true},
		},
		{
			name:  "command superseded",
			setup: withPending,
			apply: func(s *deviceState) transitions {
				return s.applyCommandIssued(CommandIssuedSignal{
					CommandID: "cmd-2", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				})
			},
			want: transitions{Pending: true},
		},
		{
			name:  "duplicate issuance",
			setup: withPending,
			apply: func(s *deviceState) transitions { return s.applyCommandIssued(pending) },
			want:  transitions{},
		},
		{
			name:  "command concluded with firmware adoption",
			setup: withPending,
			apply: func(s *deviceState) transitions {
				return s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
				})
			},
			want: transitions{Firmware: true, Pending: true},
		},
		{
			name:  "command concluded without firmware adoption",
			setup: withPending,
			apply: func(s *deviceState) transitions {
				return s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeFailed, Detail: "boom",
				})
			},
			want: transitions{Pending: true},
		},
		{
			name:  "result for a non-pending command",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				return s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
				})
			},
			want: transitions{},
		},
		{
			name:  "configuration applied",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				return s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 1})
			},
			want: transitions{Config: true},
		},
		{
			name:  "stale configuration version",
			setup: fresh,
			apply: func(s *deviceState) transitions {
				s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 2})
				return s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 1})
			},
			want: transitions{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newDeviceState("dev-1", testSettings())
			tc.setup(&s)
			if diff := cmp.Diff(tc.want, tc.apply(&s)); diff != "" {
				t.Errorf("transitions mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("scheduling question", func(t *testing.T) {
		t.Parallel()
		// Firmware, pending, and config changes snapshot immediately; identity adoption and
		// timestamp-only heartbeats wait for the periodic snapshot.
		for _, tr := range []transitions{{Firmware: true}, {Pending: true}, {Config: true}} {
			if !tr.needsSnapshot() {
				t.Errorf("%+v: needsSnapshot() = false, want true", tr)
			}
		}
		for _, tr := range []transitions{{}, {Identity: true}} {
			if tr.needsSnapshot() {
				t.Errorf("%+v: needsSnapshot() = true, want false", tr)
			}
		}
	})
}

// TestRefreshLiveness pins the derived liveness judgement: online within the configured
// threshold of the last heartbeat, offline past it or never heard from, and a reported flip
// only when the status actually changes.
func TestRefreshLiveness(t *testing.T) {
	t.Parallel()

	settings := testSettings()
	now := time.Unix(1000, 0)
	heard := now.Add(-settings.OfflineThreshold + time.Second)
	silent := now.Add(-settings.OfflineThreshold - time.Second)

	cases := []struct {
		name     string
		last     time.Time
		start    bool
		want     bool
		wantFlip bool
	}{
		{name: "never heard counts as offline", last: time.Time{}, start: false, want: false},
		{name: "recent heartbeat counts as online", last: heard, start: false, want: true, wantFlip: true},
		{name: "silence past the threshold counts as offline", last: silent, start: true, want: false, wantFlip: true},
		{name: "unchanged online reports no flip", last: heard, start: true, want: true},
		{name: "unchanged offline reports no flip", last: silent, start: false, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newDeviceState("dev-1", settings)
			s.LastHeartbeatAt = tc.last
			s.Online = tc.start
			if flip := s.refreshLiveness(now); flip != tc.wantFlip {
				t.Errorf("refreshLiveness() flip = %v, want %v", flip, tc.wantFlip)
			}
			if s.Online != tc.want {
				t.Errorf("Online = %v, want %v", s.Online, tc.want)
			}
		})
	}
}

// TestCarryRoundTrip pins the continuation contract: the carried-over payload encodes and
// decodes losslessly, dedup memory included, and a state at an unknown carry version is
// refused on the way back in.
func TestCarryRoundTrip(t *testing.T) {
	t.Parallel()

	s := newDeviceState("dev-1", testSettings())
	s.Region, s.Model = "eu-west", "oak-s3"
	s.CurrentFw = "fw-2"
	s.PreviousFw = "fw-1"
	s.Online = true
	s.LastHeartbeatAt = time.Unix(123456, 789).UTC()
	s.Pending = &PendingCommand{
		Command:    CommandIssuedSignal{CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate, FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum"},
		Dispatched: true,
	}
	s.LastCommand = &ConcludedCommand{
		Command: CommandIssuedSignal{CommandID: "cmd-0", DeviceID: "dev-1", Kind: CommandKindUpdate, FirmwareID: "fw-1", Version: "fw-1"},
		Outcome: OutcomeFailed,
		Detail:  "download digest mismatch",
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
	// The dispatch queue rides the settings, so a continuing run schedules its commands on the
	// same queue the chain started under rather than on an empty one.
	if decoded.Settings.DispatchTaskQueue != testSettings().DispatchTaskQueue {
		t.Errorf("carried dispatch task queue = %q, want %q",
			decoded.Settings.DispatchTaskQueue, testSettings().DispatchTaskQueue)
	}
	if err := decoded.validate(); err != nil {
		t.Fatalf("validate() decoded state: %v", err)
	}

	decoded.CarryVersion = carryVersion + 1
	if err := decoded.validate(); !errors.Is(err, ErrUnsupportedCarryVersion) {
		t.Errorf("validate() at carry version %d = %v, want ErrUnsupportedCarryVersion", decoded.CarryVersion, err)
	}
}
