package wavehealth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// waveDocument is the part of a wave document an evaluation needs: the rollout it belongs to, the
// devices it was dispatched to, and the time its health window opened.
type waveDocument struct {
	ID        string    `bson:"_id"`
	RolloutID string    `bson:"rollout_id"`
	DeviceIDs []string  `bson:"device_ids"`
	StartedAt time.Time `bson:"started_at"`
}

// sampleCounts is one aggregation result: the window's sample count and how many of those samples
// reported a health score at or above the threshold.
type sampleCounts struct {
	Total      int64 `bson:"total"`
	Successful int64 `bson:"successful"`
}

// Store is the fleet database as the aggregation consumes it: the rollouts and waves that say
// which devices a wave targets and since when, and the telemetry collection those devices'
// heartbeat samples live in. It satisfies both WaveSource and SampleStore. Every method is a
// read: the store never writes, so querying a wave's health cannot change it.
type Store struct {
	rollouts  *mongo.Collection
	waves     *mongo.Collection
	telemetry *mongo.Collection
}

// NewStore returns the wave health store over the fleet database's rollouts, waves, and telemetry
// collections.
func NewStore(db *mongo.Database) *Store {
	return &Store{
		rollouts:  db.Collection("rollouts"),
		waves:     db.Collection("waves"),
		telemetry: db.Collection("telemetry"),
	}
}

// Wave resolves one wave of one rollout into its membership and start time. The rollout is read
// first so an unknown rollout is a miss in its own right rather than an accident of a wave
// lookup, and the wave's own rollout_id is checked so a wave id belonging to a different rollout
// is a miss instead of an answer about the wrong wave. All three cases report ErrNotFound; every
// other failure is left for the caller to distinguish from a miss.
func (s *Store) Wave(ctx context.Context, rolloutID, waveID string) (Wave, error) {
	if err := s.requireRollout(ctx, rolloutID); err != nil {
		return Wave{}, err
	}

	var doc waveDocument
	// The wave id is unique, so this is one point read on the _id index.
	err := s.waves.FindOne(ctx, bson.D{{Key: "_id", Value: waveID}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Wave{}, fmt.Errorf("wave %s: %w", waveID, ErrNotFound)
	}
	if err != nil {
		return Wave{}, fmt.Errorf("read wave %s: %w", waveID, err)
	}
	if doc.RolloutID != rolloutID {
		return Wave{}, fmt.Errorf("wave %s of rollout %s: %w", waveID, rolloutID, ErrNotFound)
	}
	return Wave{DeviceIDs: doc.DeviceIDs, StartedAt: doc.StartedAt}, nil
}

// CountSamples counts a device set's heartbeat samples over a window in one aggregation round
// trip: a $match over the wave's devices and the measurement period, then a $group that sums the
// matched documents and the subset whose health met the success threshold. The existing
// {meta.device_id, ts} telemetry index serves the match, and only the two numbers leave the
// server rather than the samples themselves — a 100% wave over a large fleet is tens of thousands
// of documents per call.
func (s *Store) CountSamples(ctx context.Context, window SampleWindow) (int64, int64, error) {
	if len(window.DeviceIDs) == 0 {
		// No device can contribute a sample: answering locally avoids a round trip whose result
		// is zero by construction.
		return 0, 0, nil
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "meta.device_id", Value: bson.D{{Key: "$in", Value: window.DeviceIDs}}},
			{Key: "ts", Value: bson.D{
				{Key: "$gte", Value: window.From},
				{Key: "$lte", Value: window.To},
			}},
		}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "successful", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$gte", Value: bson.A{"$health", window.SuccessAtLeast}}},
				1,
				0,
			}}}}}},
		}}},
	}

	cursor, err := s.telemetry.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, 0, fmt.Errorf("aggregate telemetry samples: %w", err)
	}
	var counts []sampleCounts
	if err := cursor.All(ctx, &counts); err != nil {
		return 0, 0, fmt.Errorf("decode telemetry sample counts: %w", err)
	}
	// A window without samples produces no group document at all, which is an answer of zero
	// rather than a failure.
	if len(counts) == 0 {
		return 0, 0, nil
	}
	return counts[0].Total, counts[0].Successful, nil
}

// requireRollout reports ErrNotFound unless a rollout document with this id exists.
func (s *Store) requireRollout(ctx context.Context, rolloutID string) error {
	err := s.rollouts.FindOne(ctx, bson.D{{Key: "_id", Value: rolloutID}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("rollout %s: %w", rolloutID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("read rollout %s: %w", rolloutID, err)
	}
	return nil
}
