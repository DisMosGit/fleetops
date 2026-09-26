package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DisMosGit/fleetops/internal/devices"
)

// Snapshot is the snapshot activity's request: the device's state view at the time the
// workflow decided it. SnapshotAt is the workflow clock reading the decision was made at — a
// deterministic value, which is what makes retries of the same decision identical writes.
type Snapshot struct {
	State
	// SnapshotAt is the workflow time the state was decided at.
	SnapshotAt time.Time `json:"snapshot_at"`
}

// StateSnapshotter persists a snapshot of a device's state. *devices.SnapshotStore satisfies
// it — the interface lives here because this package is where the persistence is consumed.
type StateSnapshotter interface {
	// SaveSnapshot writes one snapshot of a device's state, converging on the newest.
	SaveSnapshot(ctx context.Context, snap devices.Snapshot) error
}

// NewSnapshotActivity returns the snapshot-device-state activity bound to store: it projects
// the state view onto the stored record and writes it. The write is at-least-once and keyed by
// the snapshot time, so a retry — or a snapshot overtaken by a newer one — changes nothing the
// store has not already converged on. The activity carries no retry policy of its own; its
// caller supplies one.
func NewSnapshotActivity(store StateSnapshotter) func(ctx context.Context, snap Snapshot) error {
	return func(ctx context.Context, snap Snapshot) error {
		record, err := snapshotRecord(snap)
		if err != nil {
			return err
		}
		if err := store.SaveSnapshot(ctx, record); err != nil {
			return fmt.Errorf("snapshot device state %s: %w", snap.DeviceID, err)
		}
		return nil
	}
}

// snapshotOf returns the snapshot of one device state, decided at the given workflow time.
func snapshotOf(s deviceState, at time.Time) Snapshot {
	return Snapshot{State: s.view(), SnapshotAt: at}
}

// snapshotRecord maps the workflow's state view onto the stored projection: the same fields,
// with the configuration content converted from its serialized form to what Mongo stores.
func snapshotRecord(snap Snapshot) (devices.Snapshot, error) {
	out := devices.Snapshot{
		DeviceID:        snap.DeviceID,
		Region:          snap.Region,
		Model:           snap.Model,
		CurrentFw:       snap.CurrentFw,
		Online:          snap.Online,
		LastHeartbeatAt: snap.LastHeartbeatAt,
		Config:          devices.SnapshotConfig{Version: snap.Config.Version},
		SnapshotAt:      snap.SnapshotAt,
	}
	if len(snap.Config.Data) > 0 {
		if err := json.Unmarshal(snap.Config.Data, &out.Config.Data); err != nil {
			return devices.Snapshot{}, fmt.Errorf("decode configuration snapshot of %s: %w",
				snap.DeviceID, err)
		}
	}
	if snap.Pending != nil {
		cmd := snap.Pending.Command
		out.Pending = &devices.SnapshotCommand{
			CommandID:  cmd.CommandID,
			DeviceID:   cmd.DeviceID,
			Kind:       string(cmd.Kind),
			FirmwareID: cmd.FirmwareID,
			Version:    cmd.Version,
			Checksum:   cmd.Checksum,
			Reason:     cmd.Reason,
			Dispatched: snap.Pending.Dispatched,
		}
	}
	if snap.Update != nil {
		out.Update = &devices.SnapshotUpdate{
			FirmwareID:      snap.Update.FirmwareID,
			Phase:           string(snap.Update.Phase),
			ProgressPercent: snap.Update.ProgressPercent,
			Detail:          snap.Update.Detail,
		}
	}
	return out, nil
}
