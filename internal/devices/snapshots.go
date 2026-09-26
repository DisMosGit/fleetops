package devices

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Snapshot is one device_state_snapshots document: the device workflow's state projected at
// the time the workflow decided it. SnapshotAt is the write-order guard — a snapshot never
// replaces a newer one.
type Snapshot struct {
	// DeviceID is the device identity; the _id of the snapshot document.
	DeviceID string `bson:"_id"`
	// Region is the device's region.
	Region string `bson:"region"`
	// Model is the device's model.
	Model string `bson:"model"`
	// CurrentFw is the firmware version the device runs.
	CurrentFw string `bson:"current_fw"`
	// Online is the device's liveness status.
	Online bool `bson:"online"`
	// LastHeartbeatAt is the timestamp of the newest applied heartbeat.
	LastHeartbeatAt time.Time `bson:"last_heartbeat"`
	// Pending is the outstanding command and its delivery state; nil while none is.
	Pending *SnapshotCommand `bson:"pending,omitempty"`
	// Update is the latest firmware-update progress the device reported; nil while none was.
	Update *SnapshotUpdate `bson:"update_status,omitempty"`
	// Config is the configuration snapshot and its version.
	Config SnapshotConfig `bson:"config"`
	// SnapshotAt is the time the workflow decided the state at.
	SnapshotAt time.Time `bson:"snapshot_at"`
}

// SnapshotCommand is the outstanding command as a snapshot records it: what was issued and
// whether delivery to the agent has succeeded at least once.
type SnapshotCommand struct {
	// CommandID is the command's identity and the id its result will reference.
	CommandID string `bson:"command_id"`
	// DeviceID is the device the command targets.
	DeviceID string `bson:"device_id"`
	// Kind names the commanded action.
	Kind string `bson:"kind"`
	// FirmwareID is the firmware to fetch; set for update commands only.
	FirmwareID string `bson:"firmware_id,omitempty"`
	// Version is the firmware version expected after the update; set for update commands only.
	Version string `bson:"version,omitempty"`
	// Checksum the downloaded binary must match; set for update commands only.
	Checksum string `bson:"checksum,omitempty"`
	// Reason is the operator-safe abort detail; set for abort commands only.
	Reason string `bson:"reason,omitempty"`
	// Dispatched reports whether delivery to the agent has succeeded at least once.
	Dispatched bool `bson:"dispatched"`
}

// SnapshotConfig is a configuration snapshot as a snapshot records it: complete content at a
// monotonically increasing version.
type SnapshotConfig struct {
	// Version is the snapshot's monotonically increasing version.
	Version int64 `bson:"version"`
	// Data is the complete configuration content — any JSON value the device was configured
	// with, stored as its natural document form.
	Data any `bson:"data,omitempty"`
}

// SnapshotUpdate is the latest firmware-update progress as a snapshot records it.
type SnapshotUpdate struct {
	// FirmwareID is the firmware being applied.
	FirmwareID string `bson:"firmware_id"`
	// Phase is the update phase the device reported reaching.
	Phase string `bson:"phase"`
	// ProgressPercent is the completion of the reported update, 0-100.
	ProgressPercent int32 `bson:"progress_percent"`
	// Detail is operator-safe failure detail; set on a failed phase.
	Detail string `bson:"detail,omitempty"`
}

// SnapshotStore is the device_state_snapshots collection: one projected document per device,
// replaced in place and only ever forward in time.
type SnapshotStore struct {
	coll *mongo.Collection
}

// NewSnapshotStore returns the state-snapshot store on the fleet database's
// device_state_snapshots collection.
func NewSnapshotStore(db *mongo.Database) *SnapshotStore {
	return &SnapshotStore{coll: db.Collection("device_state_snapshots")}
}

// SaveSnapshot projects one device state into its snapshot document. The write is monotone in
// the snapshot time: a snapshot only replaces a strictly older one, so a retried write is a
// no-op and an older snapshot landing after a newer one converges instead of regressing the
// projection. Exactly one document exists per device identity, ever.
func (s *SnapshotStore) SaveSnapshot(ctx context.Context, snap Snapshot) error {
	if snap.DeviceID == "" {
		return fmt.Errorf("save device snapshot: device id required")
	}
	filter := bson.D{
		{Key: "_id", Value: snap.DeviceID},
		{Key: "snapshot_at", Value: bson.D{{Key: "$lt", Value: snap.SnapshotAt}}},
	}
	_, err := s.coll.ReplaceOne(ctx, filter, snap, options.Replace().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// The document already carries this snapshot time or a newer one: the
			// projection has converged and this write has nothing left to do.
			return nil
		}
		return fmt.Errorf("save snapshot for %s: %w", snap.DeviceID, err)
	}
	return nil
}
