package temporal

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// carryVersion is the schema version stamped on every carried-over state. A run refuses a
// state it cannot understand instead of guessing at a foreign shape.
const carryVersion = 1

// Bounds of the dedup memory carried across continuations. The rings are the fast path of
// signal idempotency; the structural guards in the apply methods are the backstop.
const (
	maxRecentEventIDs   = 512
	maxRecentCommandIDs = 64
)

// ErrUnsupportedCarryVersion reports a carry-over state written by a newer state schema than
// this build understands.
var ErrUnsupportedCarryVersion = errors.New("unsupported carry version")

// CommandKind names what an issued command does.
type CommandKind string

// The command kinds the workflow accepts and dispatches.
const (
	// CommandKindUpdate begins the download-and-apply cycle for a firmware version.
	CommandKindUpdate CommandKind = "update"
	// CommandKindAbort stops a running update at a safe point.
	CommandKindAbort CommandKind = "abort"
)

// CommandOutcome names the terminal state of one command.
type CommandOutcome string

// The terminal command outcomes a command result carries.
const (
	// OutcomeSucceeded means the command completed and the device reached the commanded state.
	OutcomeSucceeded CommandOutcome = "succeeded"
	// OutcomeFailed means the command did not complete.
	OutcomeFailed CommandOutcome = "failed"
)

// HeartbeatSignal is the heartbeat signal payload: one liveness sample keyed by the event id
// that redeliveries reuse. Timestamp is the device's sample time — identical on every
// redelivery, which is what makes a redelivered heartbeat a structural no-op.
type HeartbeatSignal struct {
	// EventID is the heartbeat's delivery key; a redelivery reuses it.
	EventID string `json:"event_id"`
	// DeviceID is the device the sample belongs to.
	DeviceID string `json:"device_id"`
	// CurrentFw is the firmware version the device reported running.
	CurrentFw string `json:"current_fw"`
	// Timestamp is the sample time the device reported.
	Timestamp time.Time `json:"timestamp"`
}

// CommandIssuedSignal is the command_issued signal payload: one command for a device. It is
// also the dispatch activity's request — what the workflow records as pending is exactly what
// it dispatches.
type CommandIssuedSignal struct {
	// CommandID is the command's delivery key and the id its result will reference.
	CommandID string `json:"command_id"`
	// DeviceID is the device the command targets.
	DeviceID string `json:"device_id"`
	// Kind names the commanded action.
	Kind CommandKind `json:"kind"`
	// FirmwareID is the firmware to fetch; set for CommandKindUpdate only.
	FirmwareID string `json:"firmware_id,omitempty"`
	// Version is the firmware version expected after the update; set for CommandKindUpdate only.
	Version string `json:"version,omitempty"`
	// Checksum the downloaded binary must match; set for CommandKindUpdate only.
	Checksum string `json:"checksum,omitempty"`
	// Reason is the operator-safe abort detail; set for CommandKindAbort only.
	Reason string `json:"reason,omitempty"`
}

// CommandResultSignal is the command_result signal payload: the terminal outcome of one
// command, keyed by the command id it concludes.
type CommandResultSignal struct {
	// DeviceID is the device that executed the command.
	DeviceID string `json:"device_id"`
	// CommandID is the result's delivery key and the command it concludes.
	CommandID string `json:"command_id"`
	// Outcome is the terminal outcome.
	Outcome CommandOutcome `json:"outcome"`
	// Detail is operator-safe failure detail; set on OutcomeFailed only.
	Detail string `json:"detail,omitempty"`
}

// ConfigChangedSignal is the config_changed signal payload: a complete configuration snapshot
// at a monotonically increasing version, keyed by that version.
type ConfigChangedSignal struct {
	// DeviceID is the device the configuration belongs to.
	DeviceID string `json:"device_id"`
	// Version is the snapshot's delivery key; only newer versions apply.
	Version int64 `json:"version"`
	// Snapshot is the complete configuration content.
	Snapshot json.RawMessage `json:"snapshot,omitempty"`
}

// PendingCommand is a command the workflow has accepted and not yet concluded, with its
// delivery state.
type PendingCommand struct {
	// Command is the command as issued; dispatch sends exactly this.
	Command CommandIssuedSignal `json:"command"`
	// Dispatched reports whether delivery to the agent has succeeded at least once.
	Dispatched bool `json:"dispatched"`
}

// ConfigSnapshot is a versioned device configuration snapshot.
type ConfigSnapshot struct {
	// Version is the snapshot's monotonically increasing version.
	Version int64 `json:"version"`
	// Data is the complete configuration content.
	Data json.RawMessage `json:"data,omitempty"`
}

// State is the authoritative device state a state query returns: the fields the device
// workflow owns.
type State struct {
	// DeviceID is the device this state belongs to.
	DeviceID string `json:"device_id"`
	// CurrentFw is the firmware version the device is considered to run.
	CurrentFw string `json:"current_fw"`
	// LastHeartbeatAt is the timestamp of the newest applied heartbeat.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// Pending is the outstanding command, or nil when none is outstanding.
	Pending *PendingCommand `json:"pending,omitempty"`
	// Config is the configuration snapshot and its version.
	Config ConfigSnapshot `json:"config"`
}

// deviceState is the workflow's only mutable data: the authoritative device state plus the
// dedup memory and the rollover counter. The exact same struct is the ContinueAsNew payload,
// so the carry version stamps it and validate guards every run's entry.
type deviceState struct {
	// CarryVersion stamps the state schema; see carryVersion.
	CarryVersion int `json:"carry_version"`
	// DeviceID is the device this state belongs to; the workflow's identity.
	DeviceID string `json:"device_id"`
	// CurrentFw is the firmware version the device is considered to run.
	CurrentFw string `json:"current_fw"`
	// LastHeartbeatAt is the timestamp of the newest applied heartbeat.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// Pending is the outstanding command, or nil when none is outstanding.
	Pending *PendingCommand `json:"pending,omitempty"`
	// Config is the configuration snapshot and its version.
	Config ConfigSnapshot `json:"config"`
	// RecentEventIDs remembers heartbeat delivery keys across continuations.
	RecentEventIDs recentIDs `json:"recent_event_ids"`
	// RecentCommandIDs remembers accepted command ids across continuations.
	RecentCommandIDs recentIDs `json:"recent_command_ids"`
	// SignalsApplied counts the signals this run has folded in, dropped duplicates included:
	// every delivered signal costs history events, so every delivered signal drives rollover.
	SignalsApplied int `json:"signals_applied"`
}

// recentIDs is a bounded FIFO of recently seen delivery keys — the fast path of signal
// dedup. It is state, not a cache: it rides the carry-over so dedup survives continuations.
type recentIDs struct {
	// Keys are the remembered delivery keys, oldest first.
	Keys []string `json:"keys"`
}

// has reports whether key is within the remembered window.
func (r *recentIDs) has(key string) bool {
	return slices.Contains(r.Keys, key)
}

// push remembers key, evicting the oldest keys beyond limit. A key already remembered keeps
// its place instead of moving to the back.
func (r *recentIDs) push(key string, limit int) {
	if r.has(key) {
		return
	}
	r.Keys = append(r.Keys, key)
	if len(r.Keys) > limit {
		r.Keys = r.Keys[len(r.Keys)-limit:]
	}
}

// newDeviceState returns the empty state of a device: no firmware known, never heard from,
// no pending command, no configuration.
func newDeviceState(deviceID string) deviceState {
	return deviceState{CarryVersion: carryVersion, DeviceID: deviceID}
}

// validate checks the carried-over state a run was started from. A state from a newer schema
// fails loudly instead of being half-understood.
func (s deviceState) validate() error {
	if s.CarryVersion != carryVersion {
		return fmt.Errorf("device state of %q at carry version %d (want %d): %w",
			s.DeviceID, s.CarryVersion, carryVersion, ErrUnsupportedCarryVersion)
	}
	if s.DeviceID == "" {
		return errors.New("device state without device id")
	}
	return nil
}

// view reports the authoritative device state — the four owned fields — without the dedup
// memory and rollover counter, which are bookkeeping and not device state.
func (s deviceState) view() State {
	out := State{
		DeviceID:        s.DeviceID,
		CurrentFw:       s.CurrentFw,
		LastHeartbeatAt: s.LastHeartbeatAt,
		Config:          s.Config,
	}
	if s.Pending != nil {
		pending := *s.Pending
		out.Pending = &pending
	}
	return out
}

// applyHeartbeat folds one heartbeat signal into the state: liveness moves to the signal's
// timestamp and the reported firmware is adopted. A heartbeat that is not strictly newer than
// the recorded one — its own redelivery, or any older sample — changes nothing, so a
// duplicate is a no-op even outside the dedup window.
func (s *deviceState) applyHeartbeat(h HeartbeatSignal) {
	s.SignalsApplied++
	if s.RecentEventIDs.has(h.EventID) {
		return
	}
	s.RecentEventIDs.push(h.EventID, maxRecentEventIDs)
	if !h.Timestamp.After(s.LastHeartbeatAt) {
		return
	}
	s.LastHeartbeatAt = h.Timestamp
	if h.CurrentFw != "" && h.CurrentFw != s.CurrentFw {
		s.CurrentFw = h.CurrentFw
	}
}

// applyCommandIssued folds one command_issued signal into the state: the command becomes the
// pending command, superseding any outstanding one. A command id already accepted changes
// nothing, so a redelivered issue cannot re-open a concluded command while its id is
// remembered.
func (s *deviceState) applyCommandIssued(c CommandIssuedSignal) {
	s.SignalsApplied++
	if s.RecentCommandIDs.has(c.CommandID) {
		return
	}
	s.RecentCommandIDs.push(c.CommandID, maxRecentCommandIDs)
	s.Pending = &PendingCommand{Command: c}
}

// applyCommandResult folds one command_result signal into the state: a result matching the
// pending command concludes it — adopting the commanded firmware on success — and any other
// result changes nothing, so a redelivered result can never conclude twice.
func (s *deviceState) applyCommandResult(r CommandResultSignal) {
	s.SignalsApplied++
	if s.Pending == nil || s.Pending.Command.CommandID != r.CommandID {
		return
	}
	if r.Outcome == OutcomeSucceeded && s.Pending.Command.Version != "" {
		s.CurrentFw = s.Pending.Command.Version
	}
	s.Pending = nil
}

// applyConfigChanged folds one config_changed signal into the state: the snapshot replaces
// the owned one only at a strictly newer version, so a duplicate or stale version changes
// nothing.
func (s *deviceState) applyConfigChanged(c ConfigChangedSignal) {
	s.SignalsApplied++
	if c.Version <= s.Config.Version {
		return
	}
	s.Config = ConfigSnapshot{Version: c.Version, Data: c.Snapshot}
}
