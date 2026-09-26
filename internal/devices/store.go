package devices

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Store is the devices collection: registry upserts, coalesced state refreshes from heartbeat
// ingestion, and the staleness flip the sweep runs.
type Store struct {
	coll *mongo.Collection
}

// NewStore returns the device registry on the fleet database's devices collection.
func NewStore(db *mongo.Database) *Store {
	return &Store{coll: db.Collection("devices")}
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
