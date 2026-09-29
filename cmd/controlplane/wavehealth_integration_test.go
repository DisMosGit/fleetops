//go:build integration

package main

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// insertRolloutDoc stores a rollout document the wave health lookup resolves.
func insertRolloutDoc(t *testing.T, db *mongo.Database, id string) {
	t.Helper()
	_, err := db.Collection("rollouts").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: id},
		{Key: "firmware_id", Value: "fw-1"},
		{Key: "status", Value: "running"},
		{Key: "temporal_wf_id", Value: "wf-" + id},
		{Key: "region", Value: "eu-west"},
		{Key: "model", Value: "oak-s3"},
	})
	if err != nil {
		t.Fatalf("insert rollout %s: %v", id, err)
	}
}

// insertWaveDoc stores a wave document through the real validator.
func insertWaveDoc(t *testing.T, db *mongo.Database, id, rolloutID string, deviceIDs []string, startedAt time.Time) {
	t.Helper()
	members := bson.A{}
	for _, deviceID := range deviceIDs {
		members = append(members, deviceID)
	}
	_, err := db.Collection("waves").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: id},
		{Key: "rollout_id", Value: rolloutID},
		{Key: "percent", Value: 5},
		{Key: "status", Value: "running"},
		{Key: "success_rate", Value: 0.0},
		{Key: "device_ids", Value: members},
		{Key: "started_at", Value: startedAt},
	})
	if err != nil {
		t.Fatalf("insert wave %s: %v", id, err)
	}
}

// insertHeartbeat stores one heartbeat sample through the ingestion contract.
func insertHeartbeat(t *testing.T, db *mongo.Database, eventID, deviceID string, at time.Time, health float64) {
	t.Helper()
	_, err := db.Collection("telemetry").UpdateOne(context.Background(),
		bson.D{{Key: "_id", Value: eventID}},
		bson.D{{Key: "$setOnInsert", Value: bson.D{
			{Key: "_id", Value: eventID},
			{Key: "ts", Value: at},
			{Key: "meta", Value: bson.D{
				{Key: "device_id", Value: deviceID},
				{Key: "region", Value: "eu-west"},
				{Key: "model", Value: "oak-s3"},
			}},
			{Key: "cpu", Value: 0.2},
			{Key: "mem", Value: 0.3},
			{Key: "health", Value: health},
		}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("insert heartbeat %s: %v", eventID, err)
	}
}

// TestGetWaveHealthServesAHandInsertedWave drives the wiring run() builds for the operator health
// query: the service is constructed from a loaded configuration over the real fleet database and
// served on a real gRPC listener, and a hand-written wave answers through a generated client. Wave
// membership and start time have no producer until the rollout workflow lands, so hand-inserting
// them is exactly how the first query reaches this path.
func TestGetWaveHealthServesAHandInsertedWave(t *testing.T) {
	t.Parallel()

	db := mongotest.Start(t).DB
	log := slog.New(slog.DiscardHandler)

	cfg := config.Defaults()
	cfg.Rollout.HealthWindow = config.Duration{Duration: 5 * time.Minute}
	cfg.Rollout.SampleHealthThreshold = 0.6
	cfg.Rollout.MinSuccessRatio = 0.9
	cfg.Rollout.MinSamples = 5

	now := time.Now()
	insertRolloutDoc(t, db, "roll-hand")
	insertWaveDoc(t, db, "wave-hand", "roll-hand", []string{"dev-a", "dev-b"}, now.Add(-time.Hour))
	// Nine successful samples and one failing one: the ratio lands exactly on the configured
	// minimum, which is healthy.
	for i := 0; i < 9; i++ {
		insertHeartbeat(t, db, "hand-ok-"+string(rune('0'+i)), "dev-a", now.Add(-time.Duration(i)*time.Second), 0.95)
	}
	insertHeartbeat(t, db, "hand-bad-0", "dev-b", now.Add(-time.Minute), 0.1)
	// A wave of another rollout, and a device that is in no wave here.
	insertRolloutDoc(t, db, "roll-other")
	insertWaveDoc(t, db, "wave-other", "roll-other", []string{"dev-c"}, now.Add(-time.Hour))
	insertHeartbeat(t, db, "hand-outsider", "dev-outsider", now.Add(-time.Minute), 0.95)

	grpcServer := grpc.NewServer()
	rolloutv1.RegisterRolloutServiceServer(grpcServer, newWaveHealthService(cfg, db, log))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close connection: %v", err)
		}
	})
	client := rolloutv1.NewRolloutServiceClient(conn)
	ctx := context.Background()

	resp, err := client.GetWaveHealth(ctx, &rolloutv1.GetWaveHealthRequest{
		RolloutId: "roll-hand",
		WaveId:    "wave-hand",
	})
	if err != nil {
		t.Fatalf("GetWaveHealth() error = %v", err)
	}
	if resp.GetSuccessRatio() != 0.9 || resp.GetSampleSize() != 10 {
		t.Errorf("GetWaveHealth() = ratio %v over %d samples, want 0.9 over 10",
			resp.GetSuccessRatio(), resp.GetSampleSize())
	}
	if resp.GetVerdict() != rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY {
		t.Errorf("GetWaveHealth() verdict = %v, want healthy at the configured boundary", resp.GetVerdict())
	}
	if resp.GetRolloutId() != "roll-hand" || resp.GetWaveId() != "wave-hand" {
		t.Errorf("GetWaveHealth() ids = %q/%q, want the requested pair", resp.GetRolloutId(), resp.GetWaveId())
	}
	if resp.GetSampleHealthThreshold() != cfg.Rollout.SampleHealthThreshold ||
		resp.GetMinSuccessRatio() != cfg.Rollout.MinSuccessRatio ||
		resp.GetMinSamples() != int64(cfg.Rollout.MinSamples) {
		t.Errorf("GetWaveHealth() thresholds = %v/%v/%d, want the configured %v/%v/%d",
			resp.GetSampleHealthThreshold(), resp.GetMinSuccessRatio(), resp.GetMinSamples(),
			cfg.Rollout.SampleHealthThreshold, cfg.Rollout.MinSuccessRatio, cfg.Rollout.MinSamples)
	}
	start, end := resp.GetWindowStart().AsTime(), resp.GetWindowEnd().AsTime()
	if got := end.Sub(start); got != cfg.Rollout.HealthWindow.Duration {
		t.Errorf("GetWaveHealth() window = %v wide, want the configured %v", got, cfg.Rollout.HealthWindow.Duration)
	}
	if delta := time.Since(end); delta < 0 || delta > time.Minute {
		t.Errorf("GetWaveHealth() window end = %v, want the moment the call was served", end)
	}

	// The failure contract survives the wire: a miss is NOT_FOUND and a malformed request is
	// INVALID_ARGUMENT.
	if _, err := client.GetWaveHealth(ctx, &rolloutv1.GetWaveHealthRequest{
		RolloutId: "roll-hand",
		WaveId:    "wave-other",
	}); status.Code(err) != codes.NotFound {
		t.Errorf("GetWaveHealth(wave of another rollout) error = %v, want NOT_FOUND", err)
	}
	if _, err := client.GetWaveHealth(ctx, &rolloutv1.GetWaveHealthRequest{RolloutId: "roll-hand"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetWaveHealth(empty wave id) error = %v, want INVALID_ARGUMENT", err)
	}
}

// TestNewWaveHealthServiceRefusesAnUnknownRollout pins the wiring's not-found path against the
// real database: the service the control plane builds reports a miss as the sentinel the gRPC
// adapter maps onto NOT_FOUND, rather than as a storage failure.
func TestNewWaveHealthServiceRefusesAnUnknownRollout(t *testing.T) {
	t.Parallel()

	db := mongotest.Start(t).DB
	cfg := config.Defaults()
	service := newWaveHealthService(cfg, db, slog.New(slog.DiscardHandler))

	_, err := service.GetWaveHealth(context.Background(), &rolloutv1.GetWaveHealthRequest{
		RolloutId: "roll-missing",
		WaveId:    "wave-missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetWaveHealth() error = %v, want NOT_FOUND for an unknown rollout", err)
	}
}
