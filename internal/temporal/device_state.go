package temporal

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// carryVersion is the schema version stamped on every carried-over state. A run refuses a
// state it cannot understand instead of guessing at a foreign shape. Version 2 added the
// identity attributes, the liveness status, and the settings to the carried state; version 3
// added the update status; version 4 added the last concluded command; version 5 added the
// firmware version the device ran before its current one.
const carryVersion = 5

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

// UpdatePhase names where a device is in the firmware update lifecycle.
type UpdatePhase string

// The update phases an update status signal reports.
const (
	// PhaseDownloading is the phase where the firmware binary is downloaded and verified.
	PhaseDownloading UpdatePhase = "downloading"
	// PhaseApplying is the phase where the firmware is applied to the device.
	PhaseApplying UpdatePhase = "applying"
	// PhaseRebooting is the phase where the device reboots into the new firmware.
	PhaseRebooting UpdatePhase = "rebooting"
	// PhaseCompleted is the terminal phase: the update finished and the device runs the new
	// firmware.
	PhaseCompleted UpdatePhase = "completed"
	// PhaseFailed is the terminal failure phase; detail names why.
	PhaseFailed UpdatePhase = "failed"
	// PhaseRolledBack is the terminal phase: the device was rolled back to its previous
	// firmware.
	PhaseRolledBack UpdatePhase = "rolled_back"
)

// terminal reports whether the phase ends the update lifecycle — the phases worth persisting
// immediately instead of riding the periodic snapshot.
func (p UpdatePhase) terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseRolledBack
}

// HeartbeatSignal is the heartbeat signal payload: one liveness sample keyed by the event id
// that redeliveries reuse. Timestamp is the device's sample time — identical on every
// redelivery, which is what makes a redelivered heartbeat a structural no-op. Region and model
// ride the registration data the producer already holds, so the entity keeps its identity
// attributes current without a second entry point.
type HeartbeatSignal struct {
	// EventID is the heartbeat's delivery key; a redelivery reuses it.
	EventID string `json:"event_id"`
	// DeviceID is the device the sample belongs to.
	DeviceID string `json:"device_id"`
	// Region is the device's registered region; empty keeps the owned one.
	Region string `json:"region,omitempty"`
	// Model is the device's registered model; empty keeps the owned one.
	Model string `json:"model,omitempty"`
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

// UpdateStatusSignal is the update_status signal payload: one firmware-update progress report
// for a device. Progress reports carry no delivery key: they apply in arrival order, the
// latest wins, and none of them concludes a command — that stays with the command result.
type UpdateStatusSignal struct {
	// DeviceID is the device whose update is progressing.
	DeviceID string `json:"device_id"`
	// FirmwareID is the firmware being applied.
	FirmwareID string `json:"firmware_id"`
	// Phase is the phase the device reported reaching.
	Phase UpdatePhase `json:"phase"`
	// ProgressPercent is the completion of the current update, 0-100.
	ProgressPercent int32 `json:"progress_percent"`
	// Detail is operator-safe failure detail; set on PhaseFailed.
	Detail string `json:"detail,omitempty"`
}

// UpdateStatus is the latest firmware-update progress a device reported.
type UpdateStatus struct {
	// FirmwareID is the firmware being applied.
	FirmwareID string `json:"firmware_id"`
	// Phase is the phase the device reported reaching.
	Phase UpdatePhase `json:"phase"`
	// ProgressPercent is the completion of the current update, 0-100.
	ProgressPercent int32 `json:"progress_percent"`
	// Detail is operator-safe failure detail; set on PhaseFailed.
	Detail string `json:"detail,omitempty"`
}

// PendingCommand is a command the workflow has accepted and not yet concluded, with its
// delivery state.
type PendingCommand struct {
	// Command is the command as issued; dispatch sends exactly this.
	Command CommandIssuedSignal `json:"command"`
	// Dispatched reports whether delivery to the agent has succeeded at least once.
	Dispatched bool `json:"dispatched"`
}

// ConcludedCommand is a command the workflow has concluded, with the outcome it reported. A
// cleared pending command alone cannot say whether it succeeded or failed — a device that
// failed to reach the commanded version has no pending command either — so a caller that
// dispatched the command reads its result here.
type ConcludedCommand struct {
	// Command is the command as it was issued.
	Command CommandIssuedSignal `json:"command"`
	// Outcome is the terminal outcome the device reported.
	Outcome CommandOutcome `json:"outcome"`
	// Detail is operator-safe failure detail; set for OutcomeFailed only.
	Detail string `json:"detail,omitempty"`
}

// ConfigSnapshot is a versioned device configuration snapshot.
type ConfigSnapshot struct {
	// Version is the snapshot's monotonically increasing version.
	Version int64 `json:"version"`
	// Data is the complete configuration content.
	Data json.RawMessage `json:"data,omitempty"`
}

// DeviceSettings are the configured knobs the entity's own decisions depend on, seeded into
// the state at run-chain start and carried with it: the periodic snapshot cadence and the
// silence after which the device counts as offline. Configuration is deploy-time, so the
// values a run chain starts with stay its values for its life.
type DeviceSettings struct {
	// SnapshotInterval is the cadence of periodic state snapshots.
	SnapshotInterval time.Duration `json:"snapshot_interval"`
	// OfflineThreshold is the silence after which the device counts as offline.
	OfflineThreshold time.Duration `json:"offline_threshold"`
}

// State is the authoritative device state a state query returns: the fields the device
// workflow owns and carries.
type State struct {
	// DeviceID is the device this state belongs to.
	DeviceID string `json:"device_id"`
	// Region is the device's registered region, the identity attribute adopted from signals.
	Region string `json:"region,omitempty"`
	// Model is the device's registered model, the identity attribute adopted from signals.
	Model string `json:"model,omitempty"`
	// CurrentFw is the firmware version the device is considered to run.
	CurrentFw string `json:"current_fw"`
	// PreviousFw is the firmware version the device ran immediately before its current one, empty
	// while its firmware has never changed. It is what a rollback restores the device to.
	PreviousFw string `json:"previous_fw,omitempty"`
	// Online is the liveness status derived from heartbeat recency.
	Online bool `json:"online"`
	// LastHeartbeatAt is the timestamp of the newest applied heartbeat.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// Pending is the outstanding command, or nil when none is outstanding.
	Pending *PendingCommand `json:"pending,omitempty"`
	// LastCommand is the last command that concluded, or nil when none has — what a caller
	// waiting on a dispatched command reads its result from.
	LastCommand *ConcludedCommand `json:"last_command,omitempty"`
	// Update is the latest firmware-update progress the device reported, or nil when none
	// was reported.
	Update *UpdateStatus `json:"update_status,omitempty"`
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
	// Region is the device's registered region, adopted from the registration data signals
	// carry.
	Region string `json:"region,omitempty"`
	// Model is the device's registered model, adopted from the registration data signals
	// carry.
	Model string `json:"model,omitempty"`
	// CurrentFw is the firmware version the device is considered to run.
	CurrentFw string `json:"current_fw"`
	// PreviousFw is the firmware version the device ran immediately before its current one, empty
	// while its firmware has never changed.
	PreviousFw string `json:"previous_fw,omitempty"`
	// Online is the liveness status derived from heartbeat recency; see refreshLiveness.
	Online bool `json:"online"`
	// LastHeartbeatAt is the timestamp of the newest applied heartbeat.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	// Pending is the outstanding command, or nil when none is outstanding.
	Pending *PendingCommand `json:"pending,omitempty"`
	// LastCommand is the last command that concluded, or nil when none has.
	LastCommand *ConcludedCommand `json:"last_command,omitempty"`
	// Update is the latest firmware-update progress the device reported, or nil when none
	// was reported.
	Update *UpdateStatus `json:"update_status,omitempty"`
	// Config is the configuration snapshot and its version.
	Config ConfigSnapshot `json:"config"`
	// Settings are the configured knobs the entity's decisions depend on.
	Settings DeviceSettings `json:"settings"`
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
// no pending command, no configuration — decided under the given settings.
func newDeviceState(deviceID string, settings DeviceSettings) deviceState {
	return deviceState{
		CarryVersion: carryVersion,
		DeviceID:     deviceID,
		Settings:     settings,
	}
}

// transitions reports which meaningful changes one applied signal performed: identity or
// firmware adoption, a pending-command lifecycle change (set, superseded, or concluded), an
// update status reaching a terminal phase, or a configuration apply. It is the workflow's
// scheduling input: needsSnapshot names the changes worth persisting immediately, and the
// fields document what the state machine actually did.
type transitions struct {
	// Identity is set when the region or model was adopted.
	Identity bool
	// Firmware is set when the firmware version was adopted.
	Firmware bool
	// Pending is set when the pending command was set, superseded, or concluded.
	Pending bool
	// Update is set when an update status reached a terminal phase — the change worth
	// persisting immediately. Intermediate progress rides the periodic snapshot.
	Update bool
	// Config is set when a configuration snapshot was applied.
	Config bool
}

// needsSnapshot reports whether the change is one the workflow persists immediately instead of
// waiting for the periodic snapshot.
func (t transitions) needsSnapshot() bool {
	return t.Firmware || t.Pending || t.Update || t.Config
}

// validate checks the carried-over state a run was started from. A state from a newer schema
// fails loudly instead of being half-understood, and so does a state whose settings no
// decision could be made under.
func (s deviceState) validate() error {
	if s.CarryVersion != carryVersion {
		return fmt.Errorf("device state of %q at carry version %d (want %d): %w",
			s.DeviceID, s.CarryVersion, carryVersion, ErrUnsupportedCarryVersion)
	}
	if s.DeviceID == "" {
		return errors.New("device state without device id")
	}
	if s.Settings.SnapshotInterval <= 0 {
		return errors.New("device state without positive snapshot interval")
	}
	if s.Settings.OfflineThreshold <= 0 {
		return errors.New("device state without positive offline threshold")
	}
	return nil
}

// view reports the authoritative device state — the carried fields — without the dedup
// memory and rollover counter, which are bookkeeping and not device state.
func (s deviceState) view() State {
	out := State{
		DeviceID:        s.DeviceID,
		Region:          s.Region,
		Model:           s.Model,
		CurrentFw:       s.CurrentFw,
		PreviousFw:      s.PreviousFw,
		Online:          s.Online,
		LastHeartbeatAt: s.LastHeartbeatAt,
		Config:          s.Config,
	}
	if s.Pending != nil {
		pending := *s.Pending
		out.Pending = &pending
	}
	if s.LastCommand != nil {
		concluded := *s.LastCommand
		out.LastCommand = &concluded
	}
	if s.Update != nil {
		update := *s.Update
		out.Update = &update
	}
	return out
}

// refreshLiveness re-evaluates the derived liveness status at now: online while a heartbeat has
// arrived within the settings' offline threshold, offline once the recorded last heartbeat
// falls further behind, and never-heard counts as offline. It reports whether the status
// flipped, which the workflow treats as a meaningful transition.
func (s *deviceState) refreshLiveness(now time.Time) bool {
	online := now.Sub(s.LastHeartbeatAt) < s.Settings.OfflineThreshold
	if online == s.Online {
		return false
	}
	s.Online = online
	return true
}

// applyHeartbeat folds one heartbeat signal into the state: liveness moves to the signal's
// timestamp, the reported firmware is adopted, and the registration's identity attributes are
// adopted when present. A heartbeat that is not strictly newer than the recorded one — its own
// redelivery, or any older sample — changes nothing at all, so a duplicate is a no-op even
// outside the dedup window. The report names the meaningful changes among firmware and
// identity adoption.
func (s *deviceState) applyHeartbeat(h HeartbeatSignal) transitions {
	s.SignalsApplied++
	if s.RecentEventIDs.has(h.EventID) {
		return transitions{}
	}
	s.RecentEventIDs.push(h.EventID, maxRecentEventIDs)
	if !h.Timestamp.After(s.LastHeartbeatAt) {
		return transitions{}
	}
	s.LastHeartbeatAt = h.Timestamp
	var tr transitions
	if h.Region != "" && h.Region != s.Region {
		s.Region = h.Region
		tr.Identity = true
	}
	if h.Model != "" && h.Model != s.Model {
		s.Model = h.Model
		tr.Identity = true
	}
	if h.CurrentFw != "" && s.adoptFirmware(h.CurrentFw) {
		tr.Firmware = true
	}
	return tr
}

// adoptFirmware moves the device onto version and reports whether it moved. One rule maintains both
// firmware fields — the version being left becomes the previous one — so a report and a command
// conclusion that adopt the same version leave the same history behind, whichever lands first. A
// version the device already runs changes nothing, so a report that names it cannot make the device
// look as though it had been restored to itself.
func (s *deviceState) adoptFirmware(version string) bool {
	if version == "" || version == s.CurrentFw {
		return false
	}
	s.PreviousFw = s.CurrentFw
	s.CurrentFw = version
	return true
}

// applyCommandIssued folds one command_issued signal into the state: the command becomes the
// pending command, superseding any outstanding one. A command id already accepted changes
// nothing, so a redelivered issue cannot re-open a concluded command while its id is
// remembered.
func (s *deviceState) applyCommandIssued(c CommandIssuedSignal) transitions {
	s.SignalsApplied++
	if s.RecentCommandIDs.has(c.CommandID) {
		return transitions{}
	}
	s.RecentCommandIDs.push(c.CommandID, maxRecentCommandIDs)
	s.Pending = &PendingCommand{Command: c}
	return transitions{Pending: true}
}

// applyCommandResult folds one command_result signal into the state: a result matching the
// pending command concludes it — recording the command with the outcome it reported and, on
// success, adopting the commanded firmware — and any other result changes nothing, so a
// redelivered result can never conclude twice.
func (s *deviceState) applyCommandResult(r CommandResultSignal) transitions {
	s.SignalsApplied++
	if s.Pending == nil || s.Pending.Command.CommandID != r.CommandID {
		return transitions{}
	}
	var tr transitions
	if r.Outcome == OutcomeSucceeded {
		tr.Firmware = s.adoptFirmware(s.Pending.Command.Version)
	}
	s.LastCommand = &ConcludedCommand{
		Command: s.Pending.Command,
		Outcome: r.Outcome,
		Detail:  r.Detail,
	}
	s.Pending = nil
	tr.Pending = true
	return tr
}

// applyConfigChanged folds one config_changed signal into the state: the snapshot replaces
// the owned one only at a strictly newer version, so a duplicate or stale version changes
// nothing.
func (s *deviceState) applyConfigChanged(c ConfigChangedSignal) transitions {
	s.SignalsApplied++
	if c.Version <= s.Config.Version {
		return transitions{}
	}
	s.Config = ConfigSnapshot{Version: c.Version, Data: c.Snapshot}
	return transitions{Config: true}
}

// applyUpdateStatus folds one update_status signal into the state: the latest reported
// progress replaces the recorded one, so reports apply in arrival order and the newest one
// wins. An update status concludes nothing — the pending command stays pending until its
// command result arrives. Intermediate progress rides the periodic snapshot; only a terminal
// phase is a meaningful transition worth persisting immediately.
func (s *deviceState) applyUpdateStatus(u UpdateStatusSignal) transitions {
	s.SignalsApplied++
	s.Update = &UpdateStatus{
		FirmwareID:      u.FirmwareID,
		Phase:           u.Phase,
		ProgressPercent: u.ProgressPercent,
		Detail:          u.Detail,
	}
	return transitions{Update: u.Phase.terminal()}
}
