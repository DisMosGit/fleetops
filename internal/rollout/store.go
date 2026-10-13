package rollout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNotFound reports a rollout or wave record the fleet database does not hold.
var ErrNotFound = errors.New("rollout or wave not found")

// ResolveRequest names the wave to resolve, with the facts resolution needs that the pool cannot
// supply: the rollout it belongs to, the share it covers, the target selector whose pool it
// draws from, and the start time the workflow decided for it.
type ResolveRequest struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string
	// WaveID is the wave's derived identity.
	WaveID string
	// Percent is the wave's cumulative share of the eligible pool.
	Percent int
	// Region is the target selector's region.
	Region string
	// Model is the target selector's device model.
	Model string
	// StartedAt is when the wave started, as the workflow decided it.
	StartedAt time.Time
}

// WaveStateUpdate is a recorded wave's decided state: the status the wave reached, the success
// rate measured over its window once its health has been evaluated, and the devices whose updates
// failed or never reported once their outcomes have been collected.
type WaveStateUpdate struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string
	// WaveID is the wave to move.
	WaveID string
	// Status is the status the wave reached.
	Status WaveStatus
	// SuccessRate is the evaluated success ratio; zero until the wave has been evaluated.
	SuccessRate float64
	// FailedDeviceIDs are the devices that reported a failed update.
	FailedDeviceIDs []string
	// UnreportedDeviceIDs are the devices that never reported before the wave stopped waiting.
	UnreportedDeviceIDs []string
}

// PoolResolver resolves one selector's eligible devices in identity order. Store consumes it: the
// default reads the devices collection on every resolution, and the worker passes the view its
// change stream keeps current.
type PoolResolver interface {
	// Resolve returns the selector's eligible device ids, ordered by device identity.
	Resolve(ctx context.Context, sel Selector) ([]string, error)
}

// queryPoolResolver reads a selector's eligible devices from the devices collection. It is the
// rollout store's default, and it is what a store built without a view pays on every resolution.
type queryPoolResolver struct {
	devices *mongo.Collection
}

// Resolve implements PoolResolver with one ordered read of devices.
func (q queryPoolResolver) Resolve(ctx context.Context, sel Selector) ([]string, error) {
	return readPool(ctx, q.devices, sel)
}

// Store is the fleet database as a rollout consumes it: the eligible pools a selector's waves
// draw from, the waves that say which devices earlier waves already target, and the rollout and
// wave records the workflow drives. It has no workflow dependency — every method is a plain,
// idempotent write or read the workflow calls through an activity.
type Store struct {
	rollouts *mongo.Collection
	waves    *mongo.Collection
	pools    PoolResolver
}

// NewStore returns the rollout store over the fleet database's rollouts and waves collections,
// resolving eligible pools by querying devices on every resolution.
func NewStore(db *mongo.Database) *Store {
	return NewStoreWithPools(db, queryPoolResolver{devices: db.Collection("devices")})
}

// NewStoreWithPools returns the rollout store over the fleet database whose wave resolution reads
// eligible pools from the given resolver, which must be non-nil. The worker passes the
// change-stream-maintained view, so a wave resolves against a pool the fleet's own writes keep
// current rather than one re-queried on every resolution.
func NewStoreWithPools(db *mongo.Database, pools PoolResolver) *Store {
	return &Store{
		rollouts: db.Collection("rollouts"),
		waves:    db.Collection("waves"),
		pools:    pools,
	}
}

// RecordRollout ensures the rollout document exists and moves its status. The write is
// idempotent and terminal-status preserving: recording a status the document already holds
// changes nothing, and a document that has concluded is never moved again.
//
// A record carrying a rollback writes it too, on every write from the moment the rollout enters
// rollback: the steps' outcomes are replaced whole, so a step transition recorded twice converges
// on the same document instead of duplicating it. A nil rollback leaves a stored one alone, which
// is what every write before the rollback does.
func (s *Store) RecordRollout(ctx context.Context, rec RolloutRecord) error {
	if rec.ID == "" {
		return errors.New("record rollout: rollout id required")
	}
	fields := bson.D{
		{Key: "firmware_id", Value: rec.FirmwareID},
		{Key: "status", Value: rec.Status},
		{Key: "temporal_wf_id", Value: rec.WorkflowID},
		{Key: "region", Value: rec.Region},
		{Key: "model", Value: rec.Model},
	}
	if rec.Rollback != nil {
		fields = append(fields, bson.E{Key: "rollback", Value: rec.Rollback.document()})
	}

	live := bson.D{
		{Key: "_id", Value: rec.ID},
		{Key: "status", Value: bson.D{{Key: "$nin", Value: bson.A{
			RolloutRolledBack, RolloutCompleted, RolloutFailed,
		}}}},
	}
	res, err := s.rollouts.UpdateOne(ctx, live, bson.D{{Key: "$set", Value: fields}})
	if err != nil {
		return fmt.Errorf("record rollout %s: %w", rec.ID, err)
	}
	if res.MatchedCount > 0 {
		return nil
	}

	// Nothing live matched: the rollout either has no document yet or has concluded. The upsert
	// writes the first case; in the second it collides with the concluded document's own _id,
	// which is that terminal status standing as it is.
	_, err = s.rollouts.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: rec.ID}},
		bson.D{{Key: "$setOnInsert", Value: fields}},
		options.UpdateOne().SetUpsert(true))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("record rollout %s: %w", rec.ID, err)
	}
	return nil
}

// ResolveWave returns the wave's recorded membership and start time, resolving and recording them
// when it has no document yet. A wave already recorded is the answer: membership is resolved once
// and never re-derived, so a retried resolution — or a resolution that raced its own retry —
// converges on the document the first attempt wrote.
func (s *Store) ResolveWave(ctx context.Context, req ResolveRequest) (WaveRecord, error) {
	if req.RolloutID == "" || req.WaveID == "" {
		return WaveRecord{}, errors.New("resolve wave: rollout id and wave id required")
	}
	if recorded, err := s.wave(ctx, req.RolloutID, req.WaveID); err == nil {
		return recorded, nil
	} else if !errors.Is(err, ErrNotFound) {
		return WaveRecord{}, err
	}

	pool, err := s.eligiblePool(ctx, req.Region, req.Model)
	if err != nil {
		return WaveRecord{}, err
	}
	targeted, err := s.targetedDevices(ctx, req.RolloutID, req.WaveID)
	if err != nil {
		return WaveRecord{}, err
	}

	rec := WaveRecord{
		ID:        req.WaveID,
		RolloutID: req.RolloutID,
		Percent:   req.Percent,
		Status:    WaveDispatching,
		DeviceIDs: WaveTargets(pool, targeted, req.Percent),
		StartedAt: req.StartedAt,
		// A wave that has just been resolved has no device outcomes yet, and the fields are
		// written empty rather than left absent: a reader of the document does not have to
		// tell "no failures" apart from "not collected yet".
		FailedDeviceIDs:     []string{},
		UnreportedDeviceIDs: []string{},
	}
	if _, err := s.waves.InsertOne(ctx, rec); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// A retried resolution raced the attempt that recorded the wave: the recorded
			// document is the answer and this attempt's membership is discarded.
			return s.wave(ctx, req.RolloutID, req.WaveID)
		}
		return WaveRecord{}, fmt.Errorf("record wave %s: %w", req.WaveID, err)
	}
	return rec, nil
}

// RecordWaveState records a recorded wave's status, success rate, and collected device outcomes.
// A wave is resolved before it can move, so an unknown wave is an error rather than a write that
// would create a document without membership. Writing the same transition twice leaves the
// document unchanged.
func (s *Store) RecordWaveState(ctx context.Context, update WaveStateUpdate) error {
	if update.RolloutID == "" || update.WaveID == "" {
		return errors.New("record wave state: rollout id and wave id required")
	}
	filter := bson.D{
		{Key: "_id", Value: update.WaveID},
		{Key: "rollout_id", Value: update.RolloutID},
	}
	fields := bson.D{
		{Key: "status", Value: update.Status},
		{Key: "success_rate", Value: update.SuccessRate},
		// Always an array, never null: the outcome sets are written by every wave write, so a
		// wave that recorded no failures says so instead of leaving the field absent.
		{Key: "failed_device_ids", Value: nonNilIDs(update.FailedDeviceIDs)},
		{Key: "unreported_device_ids", Value: nonNilIDs(update.UnreportedDeviceIDs)},
	}
	res, err := s.waves.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: fields}})
	if err != nil {
		return fmt.Errorf("record wave %s state: %w", update.WaveID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("record wave %s of rollout %s state: %w",
			update.WaveID, update.RolloutID, ErrNotFound)
	}
	return nil
}

// nonNilIDs returns ids as an empty slice when it is nil, so a stored array field is written as
// an empty array rather than null.
func nonNilIDs(ids []string) []string {
	if ids == nil {
		return []string{}
	}
	return ids
}

// wave returns one recorded wave of one rollout. The rollout id is part of the lookup, so a wave
// id belonging to a different rollout is a miss instead of an answer about the wrong wave.
func (s *Store) wave(ctx context.Context, rolloutID, waveID string) (WaveRecord, error) {
	var rec WaveRecord
	filter := bson.D{
		{Key: "_id", Value: waveID},
		{Key: "rollout_id", Value: rolloutID},
	}
	if err := s.waves.FindOne(ctx, filter).Decode(&rec); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return WaveRecord{}, fmt.Errorf("wave %s of rollout %s: %w", waveID, rolloutID, ErrNotFound)
		}
		return WaveRecord{}, fmt.Errorf("read wave %s: %w", waveID, err)
	}
	return rec, nil
}

// eligiblePool returns the devices a selector names, in identity order, as the store's resolver
// holds them. A view-backed resolver answers from the maintained view; the default queries
// devices. Either way an empty pool is an empty answer rather than an error.
func (s *Store) eligiblePool(ctx context.Context, region, model string) ([]string, error) {
	return s.pools.Resolve(ctx, Selector{Region: region, Model: model})
}

// targetedDevices returns the devices the rollout's other waves already target, so a wave's own
// share excludes them. The wave being resolved is left out of its own exclusion set: it has no
// document yet, and a retried resolution returns before this is called.
func (s *Store) targetedDevices(ctx context.Context, rolloutID, exceptWaveID string) ([]string, error) {
	cursor, err := s.waves.Find(ctx,
		bson.D{
			{Key: "rollout_id", Value: rolloutID},
			{Key: "_id", Value: bson.D{{Key: "$ne", Value: exceptWaveID}}},
		},
		options.Find().SetProjection(bson.D{{Key: "device_ids", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("query waves of rollout %s: %w", rolloutID, err)
	}
	var docs []struct {
		DeviceIDs []string `bson:"device_ids"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("read waves of rollout %s: %w", rolloutID, err)
	}
	var targeted []string
	for _, doc := range docs {
		targeted = append(targeted, doc.DeviceIDs...)
	}
	return targeted, nil
}
