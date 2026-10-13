//go:build integration

package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"
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
//
// The configuration is deliberately not the default one: a two-minute window and a minimum of 25
// samples are what the served verdict has to follow. The wave carries failing samples older than
// the window, so a window wider than the configured one — or a hardcoded one — would evaluate a
// different ratio and answer unhealthy instead of healthy, and a second wave below the sample
// minimum proves the undecided verdict is the served one rather than a constant.
func TestGetWaveHealthServesAHandInsertedWave(t *testing.T) {
	t.Parallel()

	db := mongotest.For(t).DB
	log := slog.New(slog.DiscardHandler)

	cfg := config.Defaults()
	cfg.Rollout.HealthWindow = config.Duration{Duration: 2 * time.Minute}
	cfg.Rollout.SampleHealthThreshold = 0.6
	cfg.Rollout.MinSuccessRatio = 0.9
	cfg.Rollout.MinSamples = 25
	if cfg.Rollout.HealthWindow == config.Defaults().Rollout.HealthWindow ||
		cfg.Rollout.MinSamples == config.Defaults().Rollout.MinSamples {
		t.Fatal("the test's configuration equals the defaults, so a hardcoded window or minimum would pass")
	}

	now := time.Now()
	insertRolloutDoc(t, db, "roll-hand")
	insertWaveDoc(t, db, "wave-hand", "roll-hand", []string{"dev-a", "dev-b"}, now.Add(-time.Hour))
	// Twenty-three successful samples and two failing ones inside the window: the ratio is 0.92,
	// above the configured minimum, so the wave is healthy.
	for i := 0; i < 23; i++ {
		insertHeartbeat(t, db, "hand-ok-"+strconv.Itoa(i), "dev-a",
			now.Add(-time.Duration(i)*time.Second), 0.95)
	}
	for i := 0; i < 2; i++ {
		insertHeartbeat(t, db, "hand-bad-"+strconv.Itoa(i), "dev-b",
			now.Add(-time.Duration(i)*time.Second), 0.1)
	}
	// Ten failing samples older than the configured window. A five-minute window would count
	// them (23 of 35 = 0.66, unhealthy); the configured two-minute window must not.
	for i := 0; i < 10; i++ {
		insertHeartbeat(t, db, "hand-old-"+strconv.Itoa(i), "dev-b",
			now.Add(-3*time.Minute-time.Duration(i)*time.Second), 0.1)
	}
	// A second wave one sample short of the configured minimum: its verdict is undecided. Its
	// devices are its own, so the samples above cannot count for it — the evaluation counts the
	// wave's target devices, not the devices of the rollout.
	insertWaveDoc(t, db, "wave-few", "roll-hand", []string{"dev-few"}, now.Add(-time.Hour))
	for i := 0; i < cfg.Rollout.MinSamples-1; i++ {
		insertHeartbeat(t, db, "hand-few-"+strconv.Itoa(i), "dev-few",
			now.Add(-time.Duration(i)*time.Second), 0.95)
	}
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
	if resp.GetSuccessRatio() != 0.92 || resp.GetSampleSize() != 25 {
		t.Errorf("GetWaveHealth() = ratio %v over %d samples, want 0.92 over the window's 25: "+
			"the failing samples older than the configured window were counted",
			resp.GetSuccessRatio(), resp.GetSampleSize())
	}
	if resp.GetVerdict() != rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY {
		t.Errorf("GetWaveHealth() verdict = %v, want healthy above the configured ratio", resp.GetVerdict())
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

	// The sample minimum is the served one: a wave below it is undecided rather than healthy,
	// even though every sample it has is a success.
	few, err := client.GetWaveHealth(ctx, &rolloutv1.GetWaveHealthRequest{
		RolloutId: "roll-hand",
		WaveId:    "wave-few",
	})
	if err != nil {
		t.Fatalf("GetWaveHealth(below the sample minimum) error = %v", err)
	}
	if few.GetSampleSize() != int64(cfg.Rollout.MinSamples-1) {
		t.Errorf("GetWaveHealth(below the minimum) sample size = %d, want %d",
			few.GetSampleSize(), cfg.Rollout.MinSamples-1)
	}
	if few.GetVerdict() != rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED {
		t.Errorf("GetWaveHealth(below the minimum) verdict = %v, want undecided below the configured %d samples",
			few.GetVerdict(), cfg.Rollout.MinSamples)
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

	db := mongotest.For(t).DB
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
