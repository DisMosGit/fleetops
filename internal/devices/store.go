package devices

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNoRecord reports a device the fleet database holds no document for.
var ErrNoRecord = errors.New("device record not found")

// FirmwareState is the outcome of reconciling one device's recorded firmware version against the
// version the device's workflow — the authority — holds.
type FirmwareState string

// The reconciliation outcomes of one device.
const (
	// FirmwareAgreed means the recorded version already matched, so nothing was written.
	FirmwareAgreed FirmwareState = "agreed"
	// FirmwareCorrected means the record held another version and now holds the reported one.
	FirmwareCorrected FirmwareState = "corrected"
	// FirmwareMissing means the fleet holds no record for the device, so nothing was compared
	// and nothing was created.
	FirmwareMissing FirmwareState = "missing"
)

// Store is the devices collection: registry upserts, coalesced state refreshes from heartbeat
// ingestion, the staleness flip the sweep runs, and the firmware reconciliation a rollback's
// inventory step corrects a record with.
type Store struct {
	coll *mongo.Collection
	// firmware is the same collection as the reconciliation reads and conditionally writes it.
	firmware firmwareRecords
}

// NewStore returns the device registry on the fleet database's devices collection.
func NewStore(db *mongo.Database) *Store {
	coll := db.Collection("devices")
	return &Store{coll: coll, firmware: &mongoFirmware{coll: coll}}
}

// Upsert registers or updates one device record: its identity fields, its presence status,
// and the last-seen time of the accepted registration. A device already known is updated in
// place — one devices document per identity, never a second one.
func (s *Store) Upsert(ctx context.Context, rec Record) error {
	if rec.ID == "" {
		return fmt.Errorf("upsert device: device id required")
	}
	filter := bson.D{{Key: "_id", Value: rec.ID}}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "model", Value: rec.Model},
		{Key: "region", Value: rec.Region},
		{Key: "current_fw", Value: rec.CurrentFw},
		{Key: "status", Value: rec.Status},
		{Key: "last_heartbeat", Value: rec.LastSeen},
	}}}
	if _, err := s.coll.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert device %s: %w", rec.ID, err)
	}
	return nil
}

// ApplyBatch applies device-state refreshes in one round trip. Refreshes of the same device
// coalesce to their newest: the last-seen time only moves forward ($max), and the reported
// firmware and status follow the newest refresh.
func (s *Store) ApplyBatch(ctx context.Context, updates []Update) error {
	batch := coalesce(updates)
	if len(batch) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(batch))
	for _, u := range batch {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: u.ID}}).
			SetUpdate(bson.D{
				{Key: "$max", Value: bson.D{{Key: "last_heartbeat", Value: u.LastSeen}}},
				{Key: "$set", Value: bson.D{
					{Key: "status", Value: u.Status},
					{Key: "current_fw", Value: u.CurrentFw},
				}},
			}))
	}
	if _, err := s.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("apply %d device updates: %w", len(models), err)
	}
	return nil
}

// ReconcileFirmware reconciles one device's recorded firmware version against the version the
// device's workflow holds: it reports that the record agreed, that it was wrong and now holds the
// reported version, or that the fleet holds no record for the device at all.
//
// The correction is conditional — it matches only a document whose current_fw differs — so a rerun
// of a reconciliation writes nothing, and a heartbeat that wrote a newer version concurrently is
// never regressed. Only the version the caller reports is ever written: nothing is derived,
// guessed, or cleared, which is why an empty version is refused rather than written.
func (s *Store) ReconcileFirmware(ctx context.Context, deviceID, version string) (FirmwareState, error) {
	if deviceID == "" || version == "" {
		return "", errors.New("reconcile device firmware: device id and version required")
	}
	recorded, err := s.firmware.currentFirmware(ctx, deviceID)
	if errors.Is(err, ErrNoRecord) {
		return FirmwareMissing, nil
	}
	if err != nil {
		return "", err
	}
	if recorded == version {
		return FirmwareAgreed, nil
	}
	corrected, err := s.firmware.correctFirmware(ctx, deviceID, version)
	if err != nil {
		return "", err
	}
	if !corrected {
		// A concurrent write moved the record to the version this reconciliation was about to
		// write: the record now holds what the device reports, which is agreement.
		return FirmwareAgreed, nil
	}
	return FirmwareCorrected, nil
}

// firmwareRecords is the slice of the devices collection the firmware reconciliation needs: the
// recorded firmware version of one device and a conditional correction of it. The Mongo-backed
// implementation is built by NewStore; tests hand-write a fake.
type firmwareRecords interface {
	// currentFirmware returns the firmware version recorded for one device, or an error wrapping
	// ErrNoRecord when the collection holds no document for it.
	currentFirmware(ctx context.Context, deviceID string) (string, error)
	// correctFirmware writes version to one device's record, matching only a record that does not
	// already hold it, and reports whether it wrote.
	correctFirmware(ctx context.Context, deviceID, version string) (bool, error)
}

// mongoFirmware is the devices collection as the reconciliation reads and writes it. Only the
// firmware field is touched: the device's statuses and heartbeat times belong to the writers that
// own them.
type mongoFirmware struct {
	coll *mongo.Collection
}

// currentFirmware reads one device's recorded firmware version.
func (m *mongoFirmware) currentFirmware(ctx context.Context, deviceID string) (string, error) {
	var doc struct {
		CurrentFw string `bson:"current_fw"`
	}
	if err := m.coll.FindOne(ctx, bson.D{{Key: "_id", Value: deviceID}}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", fmt.Errorf("read device %s firmware: %w", deviceID, ErrNoRecord)
		}
		return "", fmt.Errorf("read device %s firmware: %w", deviceID, err)
	}
	return doc.CurrentFw, nil
}

// correctFirmware writes version where the record disagrees with it, reporting whether it did.
func (m *mongoFirmware) correctFirmware(ctx context.Context, deviceID, version string) (bool, error) {
	filter := bson.D{
		{Key: "_id", Value: deviceID},
		{Key: "current_fw", Value: bson.D{{Key: "$ne", Value: version}}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "current_fw", Value: version}}}}
	res, err := m.coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, fmt.Errorf("correct device %s firmware to %s: %w", deviceID, version, err)
	}
	return res.MatchedCount > 0, nil
}

// MarkStale flips every online device last heard from before cutoff to offline and returns how
// many transitioned. The filter on status makes the count exact: a device already offline is
// never matched, and a device refreshed before the flip is not stale.
func (s *Store) MarkStale(ctx context.Context, cutoff time.Time) (int64, error) {
	filter := bson.D{
		{Key: "status", Value: StatusOnline},
		{Key: "last_heartbeat", Value: bson.D{{Key: "$lt", Value: cutoff}}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: StatusOffline}}}}
	res, err := s.coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("mark stale devices offline: %w", err)
	}
	return res.ModifiedCount, nil
}

// DeviceModels lists the distinct device models the registry knows — the models at least one
// registered device runs. It is the model catalog upload validation checks firmware targets
// against.
func (s *Store) DeviceModels(ctx context.Context) ([]string, error) {
	var models []string
	if err := s.coll.Distinct(ctx, "model", bson.D{}).Decode(&models); err != nil {
		return nil, fmt.Errorf("list device models: %w", err)
	}
	return models, nil
}

// coalesce reduces refreshes to one per device — the newest — preserving first-seen order so
// a batch writes each device at most once.
func coalesce(updates []Update) []Update {
	index := make(map[string]int, len(updates))
	out := make([]Update, 0, len(updates))
	for _, u := range updates {
		if i, ok := index[u.ID]; ok {
			if u.LastSeen.After(out[i].LastSeen) {
				out[i] = u
			}
			continue
		}
		index[u.ID] = len(out)
		out = append(out, u)
	}
	return out
}
