//go:build integration

package telemetry

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// alertDocument mirrors the device_alerts contract the integration tests assert on.
type alertDocument struct {
	DeviceID    string    `bson:"device_id"`
	Region      string    `bson:"region"`
	Model       string    `bson:"model"`
	Threshold   float64   `bson:"threshold"`
	MinHealth   float64   `bson:"min_health"`
	FirstSeenAt time.Time `bson:"first_seen_at"`
	LastSeenAt  time.Time `bson:"last_seen_at"`
}

// TestAlertingAgainstStore verifies the alerting side effect against the real schema and
// validator: one document per device with a widening, monotone window, nothing for a healthy
// heartbeat, and an identical document when the same event is applied again.
func TestAlertingAgainstStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	alerts := harness.DB.Collection("device_alerts")
	handler := NewAlerting(alerts, 0.6)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Millisecond)

	degraded := func(deviceID string, health float64, observedAt time.Time) Envelope {
		env := testEvent()
		env.EventID = "ev-" + deviceID + "-" + observedAt.Format("150405.000")
		env.DeviceID = deviceID
		env.OccurredAt = observedAt
		env.Payload.Health = health
		return env
	}
	read := func(t *testing.T, deviceID string) alertDocument {
		t.Helper()
		var got alertDocument
		if err := alerts.FindOne(ctx, bson.D{{Key: "_id", Value: deviceID}}).Decode(&got); err != nil {
			t.Fatalf("find alert for %s: %v", deviceID, err)
		}
		return got
	}
	count := func(t *testing.T, deviceID string) int64 {
		t.Helper()
		got, err := alerts.CountDocuments(ctx, bson.D{{Key: "_id", Value: deviceID}})
		if err != nil {
			t.Fatalf("count alerts for %s: %v", deviceID, err)
		}
		return got
	}

	t.Run("a degraded heartbeat opens one alert", func(t *testing.T) {
		event := degraded("dev-degraded", 0.4, base)
		if err := handler.Apply(ctx, event); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		got := read(t, "dev-degraded")
		if got.DeviceID != "dev-degraded" || got.Region != "eu-west" || got.Model != "v3" {
			t.Errorf("alert identity = %+v, want the device's registered identity", got)
		}
		if got.Threshold != 0.6 || got.MinHealth != 0.4 {
			t.Errorf("alert threshold/min_health = %v/%v, want 0.6/0.4", got.Threshold, got.MinHealth)
		}
		if !got.FirstSeenAt.Equal(base) || !got.LastSeenAt.Equal(base) {
			t.Errorf("alert window = %v..%v, want the first observation", got.FirstSeenAt, got.LastSeenAt)
		}
		if got := count(t, "dev-degraded"); got != 1 {
			t.Errorf("alert documents = %d, want exactly 1", got)
		}
	})

	t.Run("repeated degradation widens one alert", func(t *testing.T) {
		// A lower health later in time: the minimum falls, the window widens, and no second
		// document appears.
		if err := handler.Apply(ctx, degraded("dev-degraded", 0.2, base.Add(30*time.Second))); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		// An out-of-order older heartbeat: it may only move the window's start backwards.
		if err := handler.Apply(ctx, degraded("dev-degraded", 0.5, base.Add(-30*time.Second))); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		got := read(t, "dev-degraded")
		if !got.FirstSeenAt.Equal(base.Add(-30 * time.Second)) {
			t.Errorf("first_seen_at = %v, want the earliest observation %v", got.FirstSeenAt, base.Add(-30*time.Second))
		}
		if !got.LastSeenAt.Equal(base.Add(30 * time.Second)) {
			t.Errorf("last_seen_at = %v, want the newest observation %v", got.LastSeenAt, base.Add(30*time.Second))
		}
		if got.MinHealth != 0.2 {
			t.Errorf("min_health = %v, want the lowest observed 0.2", got.MinHealth)
		}
		if got := count(t, "dev-degraded"); got != 1 {
			t.Errorf("alert documents = %d, want the single alert refreshed in place", got)
		}
	})

	t.Run("replaying an event leaves the alert identical", func(t *testing.T) {
		before := read(t, "dev-degraded")
		if err := handler.Apply(ctx, degraded("dev-degraded", 0.2, base.Add(30*time.Second))); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		after := read(t, "dev-degraded")
		if after != before {
			t.Errorf("alert after replay = %+v, want it unchanged from %+v", after, before)
		}
	})

	t.Run("a healthy heartbeat writes nothing", func(t *testing.T) {
		event := degraded("dev-healthy", 0.9, base)
		if err := handler.Apply(ctx, event); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if got := count(t, "dev-healthy"); got != 0 {
			t.Errorf("alert documents for a healthy device = %d, want none", got)
		}
	})
}
