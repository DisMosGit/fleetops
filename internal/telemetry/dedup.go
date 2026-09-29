package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ClaimState reports what a consumer's claim attempt found in the processed-events ledger.
type ClaimState int

const (
	// ClaimNew is a claim this consumer just recorded: the side effect has not been applied yet.
	ClaimNew ClaimState = iota
	// ClaimUnfinished is a claim recorded earlier whose side effect never became durable — a
	// failed attempt, or a consumer that stopped mid-processing. It is work to resume, not a
	// duplicate.
	ClaimUnfinished
	// ClaimProcessed is a claim whose side effect is durable: the delivery is a duplicate.
	ClaimProcessed
)

// String names the claim state for logs.
func (s ClaimState) String() string {
	switch s {
	case ClaimNew:
		return "new"
	case ClaimUnfinished:
		return "unfinished"
	case ClaimProcessed:
		return "processed"
	default:
		return "unknown"
	}
}

// Ledger is the consumer dedup ledger: the `processed_events` collection, one document per
// (consumer, event id) pair. Its uniqueness constraint makes a second document for one pair
// impossible, so a redelivery is a counted no-op rather than a second side effect.
type Ledger struct {
	events *mongo.Collection
}

// NewLedger returns the ledger over the processed_events collection. *mongo.Collection
// satisfies the store contract with its plain insert, find-one, and update-one methods.
func NewLedger(events *mongo.Collection) *Ledger {
	return &Ledger{events: events}
}

// LedgerID returns the document id of one consumer's record of an event.
func LedgerID(consumer, eventID string) string {
	return consumer + ":" + eventID
}

// Claim records that the consumer is taking an event for processing and reports what it found: a
// fresh claim, an unfinished one to resume, or a finished one to treat as a duplicate. A claim
// conflict is a duplicate rather than a failure, so it is never surfaced as an error.
func (l *Ledger) Claim(ctx context.Context, consumer, eventID, deviceID string, claimedAt time.Time) (ClaimState, error) {
	claim := bson.D{
		{Key: "_id", Value: LedgerID(consumer, eventID)},
		{Key: "consumer", Value: consumer},
		{Key: "event_id", Value: eventID},
		{Key: "device_id", Value: deviceID},
		{Key: "claimed_at", Value: claimedAt},
	}
	// The retention window can remove a record between the refused insert and the read that
	// follows it; a second pass claims it again instead of failing on a record that is gone.
	const passes = 2
	for range passes {
		if _, err := l.events.InsertOne(ctx, claim); err == nil {
			return ClaimNew, nil
		} else if !mongo.IsDuplicateKeyError(err) {
			return ClaimNew, fmt.Errorf("claim event %s for %s: %w", eventID, consumer, err)
		}

		var record struct {
			ProcessedAt *time.Time `bson:"processed_at"`
		}
		err := l.events.FindOne(ctx, bson.D{{Key: "_id", Value: LedgerID(consumer, eventID)}}).Decode(&record)
		switch {
		case err == nil && record.ProcessedAt != nil:
			return ClaimProcessed, nil
		case err == nil:
			return ClaimUnfinished, nil
		case errors.Is(err, mongo.ErrNoDocuments):
			continue
		default:
			return ClaimNew, fmt.Errorf("read claim %s for %s: %w", eventID, consumer, err)
		}
	}
	return ClaimNew, fmt.Errorf("claim event %s for %s: refused as a duplicate twice", eventID, consumer)
}

// MarkProcessed records that the consumer's side effect for an event is durable. It fails when
// the claim itself is gone, because a completion without its claim would let a redelivery hide
// behind a missing record.
func (l *Ledger) MarkProcessed(ctx context.Context, consumer, eventID string, processedAt time.Time) error {
	result, err := l.events.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: LedgerID(consumer, eventID)}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "processed_at", Value: processedAt}}}},
	)
	if err != nil {
		return fmt.Errorf("mark event %s processed for %s: %w", eventID, consumer, err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("mark event %s processed for %s: no claim record", eventID, consumer)
	}
	return nil
}
