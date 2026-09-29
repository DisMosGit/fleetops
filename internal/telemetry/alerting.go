package telemetry

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// AlertStore is the part of the device_alerts collection the alerting handler needs: one upsert
// per degraded heartbeat. *mongo.Collection satisfies it.
type AlertStore interface {
	// UpdateOne applies one update to the document matching filter, inserting it when opts ask
	// for an upsert.
	UpdateOne(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
}

// Alerting is the heartbeat consumer's side effect: it records one durable alert document per
// device whose reported health fell below the configured threshold.
type Alerting struct {
	alerts    AlertStore
	threshold float64
}

// NewAlerting returns an alerting handler writing to the device_alerts collection, recording a
// degradation for every heartbeat below threshold.
func NewAlerting(alerts AlertStore, threshold float64) *Alerting {
	return &Alerting{alerts: alerts, threshold: threshold}
}

// Apply records one heartbeat's alert state. A heartbeat at or above the threshold writes
// nothing; a degraded one upserts the device's single alert document with a monotone window —
// the earliest first sighting, the lowest health seen, the newest sighting — so replaying the
// same event leaves the document unchanged and no counter can drift under at-least-once
// delivery. No raw heartbeat payload is stored.
func (a *Alerting) Apply(ctx context.Context, env Envelope) error {
	if env.EventType != HeartbeatEventType {
		return fmt.Errorf("alerting does not handle event type %s", env.EventType)
	}
	if env.Payload.Health >= a.threshold {
		return nil
	}
	observedAt := env.OccurredAt
	update := bson.D{
		{Key: "$min", Value: bson.D{
			{Key: "first_seen_at", Value: observedAt},
			{Key: "min_health", Value: env.Payload.Health},
		}},
		{Key: "$max", Value: bson.D{{Key: "last_seen_at", Value: observedAt}}},
		{Key: "$set", Value: bson.D{
			{Key: "device_id", Value: env.DeviceID},
			{Key: "region", Value: env.Region},
			{Key: "model", Value: env.Model},
			{Key: "threshold", Value: a.threshold},
		}},
	}
	_, err := a.alerts.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: env.DeviceID}},
		update,
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("record alert for device %s: %w", env.DeviceID, err)
	}
	return nil
}
