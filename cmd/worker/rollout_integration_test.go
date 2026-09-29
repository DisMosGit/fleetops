//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// The smoke's timings are deliberately tiny: the health window is two seconds, so a rollout of
// two waves concludes in seconds and the assertions below stay honest about the real clock.
const (
	smokeHealthWindow  = 2 * time.Second
	smokeMinSamples    = 3
	smokeHeartbeatGap  = 300 * time.Millisecond
	smokePollInterval  = 150 * time.Millisecond
	smokePollTimeout   = 3 * time.Minute
	smokeTemporalStart = 60 * time.Second
)

// commandRecorder stands in for the device command seam: it records every update command a wave
// dispatches. The device side of the seam — the entity workflow that accepts a command and the
// control-plane activity that streams it to an agent — has its own tests; what this smoke drives
// for real is the rollout pipeline itself: the workflow, its six activities, the fleet database,
// the configuration, and the start path.
type commandRecorder struct {
	mu       sync.Mutex
	commands []temporal.CommandIssuedSignal
}

// SignalCommandIssued implements temporal.DeviceCommander.
func (r *commandRecorder) SignalCommandIssued(_ context.Context, cmd temporal.CommandIssuedSignal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, cmd)
	return nil
}

// recorded returns the commands the seam accepted, in delivery order.
func (r *commandRecorder) recorded() []temporal.CommandIssuedSignal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]temporal.CommandIssuedSignal(nil), r.commands...)
}

// TestRolloutSmoke drives rollouts the way the local stack does: a real MongoDB carrying the fleet
// schema, a real Temporal server, the worker's own registrations for the rollout workflow and its
// six activities, and the worker's configuration mapping. The agent emulator is absent — its
// heartbeats are written through the documented ingestion contract instead, exactly as the
// wave-health smoke does — and the device command seam is bound to a recorder in place of the
// device workflows.
//
// It covers the four things a rollout must do on a live stack: advance its waves in order, fill
// the wave documents in with membership, start time, status, and measured success rate, hold a
// wave that requires approval until an operator signals, and end a rollout as rolled back when a
// wave's health falls below the boundary.
func TestRolloutSmoke(t *testing.T) {
	db := mongotest.Start(t).DB
	address := startTemporalDevServer(t)

	cfg := config.Defaults()
	cfg.Temporal.Address = address
	cfg.Rollout.HealthWindow = config.Duration{Duration: smokeHealthWindow}
	cfg.Rollout.DecisionTimeout = config.Duration{Duration: 30 * time.Second}
	cfg.Rollout.MinSamples = smokeMinSamples
	cfg.Rollout.SampleHealthThreshold = 0.6
	cfg.Rollout.MinSuccessRatio = 0.95

	dispatcher := &commandRecorder{}
	tc := startSmokeWorker(t, cfg, db, dispatcher)
	ctx := context.Background()

	// The fleet: devices to canary in one region and two smaller fleets in others, all of the
	// model the firmware targets. The middle region is held back by an approval; the last one
	// reports failing heartbeats.
	canaryFleet := seedFleet(t, db, "eu-west", 4)
	gatedFleet := seedFleet(t, db, "us-east", 2)
	brokenFleet := seedFleet(t, db, "ap-south", 2)
	seedFirmware(t, db)

	stopHealthy := startHeartbeats(t, db, map[string][]string{
		"eu-west": canaryFleet,
		"us-east": gatedFleet,
	}, 0.95)
	stopBroken := startHeartbeats(t, db, map[string][]string{"ap-south": brokenFleet}, 0.1)
	t.Cleanup(stopHealthy)
	t.Cleanup(stopBroken)

	rollouts := db.Collection("rollouts")
	waves := db.Collection("waves")

	// 1. An un-gated canary sequence runs to completion, in order.
	canary := temporal.RolloutRequest{
		RolloutID: "ro-smoke-canary", FirmwareID: "fw-smoke", Region: "eu-west", Model: "oak-s3",
	}
	canarySettings := rolloutSettings(cfg.Rollout)
	canarySettings.Waves = []temporal.RolloutWave{{Percent: 25}, {Percent: 100}}
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, canarySettings).
		Start(ctx, canary); err != nil {
		t.Fatalf("start canary rollout: %v", err)
	}
	waitForRolloutStatus(t, rollouts, canary.RolloutID, rollout.RolloutCompleted)

	first := waitForWave(t, waves, rollout.WaveID(canary.RolloutID, 0, 25))
	last := waitForWave(t, waves, rollout.WaveID(canary.RolloutID, 1, 100))
	if len(first.DeviceIDs) != 1 || len(last.DeviceIDs) != 3 {
		t.Errorf("wave membership = %d and %d devices, want 1 and 3 of the four-device fleet",
			len(first.DeviceIDs), len(last.DeviceIDs))
	}
	for _, wave := range []rollout.WaveRecord{first, last} {
		if wave.Status != rollout.WaveHealthy {
			t.Errorf("wave %s status = %q, want healthy", wave.ID, wave.Status)
		}
		if wave.SuccessRate < cfg.Rollout.MinSuccessRatio {
			t.Errorf("wave %s success rate = %v, want at least %v",
				wave.ID, wave.SuccessRate, cfg.Rollout.MinSuccessRatio)
		}
		if wave.StartedAt.IsZero() {
			t.Errorf("wave %s has no start time, want the moment its window opened", wave.ID)
		}
	}
	if !first.StartedAt.Before(last.StartedAt) {
		t.Errorf("wave starts = %v then %v, want the sequence to advance in order",
			first.StartedAt, last.StartedAt)
	}
	assertDispatched(t, dispatcher, []rollout.WaveRecord{first, last})

	// 2. A wave that requires approval waits for the operator's signal.
	gated := temporal.RolloutRequest{
		RolloutID: "ro-smoke-gated", FirmwareID: "fw-smoke", Region: "us-east", Model: "oak-s3",
	}
	gatedSettings := rolloutSettings(cfg.Rollout)
	gatedSettings.Waves = []temporal.RolloutWave{{Percent: 100, RequireApproval: true}}
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, gatedSettings).
		Start(ctx, gated); err != nil {
		t.Fatalf("start gated rollout: %v", err)
	}
	waitForRolloutStatus(t, rollouts, gated.RolloutID, rollout.RolloutAwaitingApproval)
	if count := countWaves(t, waves, gated.RolloutID); count != 0 {
		t.Errorf("gated rollout has %d wave documents, want none before the approval", count)
	}
	if err := tc.SignalWorkflow(ctx, temporal.RolloutWorkflowID(gated.RolloutID), "",
		temporal.ApproveNextWaveSignalName, temporal.ApproveNextWaveSignal{}); err != nil {
		t.Fatalf("approve gated wave: %v", err)
	}
	waitForRolloutStatus(t, rollouts, gated.RolloutID, rollout.RolloutCompleted)
	if wave := waitForWave(t, waves, rollout.WaveID(gated.RolloutID, 0, 100)); wave.Status != rollout.WaveHealthy {
		t.Errorf("approved wave status = %q, want healthy", wave.Status)
	}

	// 3. A wave whose fleet reports failing health ends the rollout as rolled back.
	broken := temporal.RolloutRequest{
		RolloutID: "ro-smoke-broken", FirmwareID: "fw-smoke", Region: "ap-south", Model: "oak-s3",
	}
	brokenSettings := rolloutSettings(cfg.Rollout)
	brokenSettings.Waves = []temporal.RolloutWave{{Percent: 100}}
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, brokenSettings).
		Start(ctx, broken); err != nil {
		t.Fatalf("start broken rollout: %v", err)
	}
	record := waitForRolloutStatus(t, rollouts, broken.RolloutID, rollout.RolloutRolledBack)

	failed := waitForWave(t, waves, rollout.WaveID(broken.RolloutID, 0, 100))
	if failed.Status != rollout.WaveUnhealthy {
		t.Errorf("failing wave status = %q, want unhealthy", failed.Status)
	}
	if failed.SuccessRate >= cfg.Rollout.MinSuccessRatio {
		t.Errorf("failing wave success rate = %v, want the measured below-boundary ratio",
			failed.SuccessRate)
	}
	// The rollout document names the workflow execution driving it, which is the handle an
	// operator follows to the state query.
	if record.WorkflowID != temporal.RolloutWorkflowID(broken.RolloutID) {
		t.Errorf("rollout workflow id = %q, want %q",
			record.WorkflowID, temporal.RolloutWorkflowID(broken.RolloutID))
	}
}

// startTemporalDevServer starts a Temporal dev server on a free port and returns its address. The
// test is skipped when the Temporal CLI is not installed: the smoke needs a real frontend, and
// there is no in-process substitute for one.
func startTemporalDevServer(t *testing.T) string {
	t.Helper()
	binary, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI not installed: skipping the local-stack smoke")
	}
	address := freeAddress(t)

	cmd := exec.Command(binary, "server", "start-dev",
		"--headless",
		"--ip", "127.0.0.1",
		"--port", portOf(t, address),
		"--db-filename", t.TempDir()+"/temporal.db",
		"--log-level", "error",
	)
	// The server's own log is where a frontend complaint shows up, so a failing smoke reports
	// its tail.
	var serverLog bytes.Buffer
	cmd.Stdout, cmd.Stderr = &serverLog, &serverLog
	if err := cmd.Start(); err != nil {
		t.Fatalf("start temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("temporal dev server log:\n%s", serverLog.String())
		}
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Logf("kill temporal dev server: %v", err)
		}
		if err := cmd.Wait(); err != nil {
			t.Logf("temporal dev server exited: %v", err)
		}
	})

	deadline := time.Now().Add(smokeTemporalStart)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close probe connection: %v", closeErr)
			}
			return address
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("temporal dev server did not accept connections on %s within %v", address, smokeTemporalStart)
	return ""
}

// freeAddress reserves an ephemeral port and returns it as a dialable address. The reservation is
// released immediately, which is what keeps the test hermetic: nothing else binds that port.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return address
}

// portOf returns the port of a host:port address.
func portOf(t *testing.T, address string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %s: %v", address, err)
	}
	return port
}

// startSmokeWorker registers and runs the worker's own rollout registrations against the dev
// server, over the real fleet database and the configured policy. It returns a connected Temporal
// client and waits until the frontend answers, so a rollout started afterwards cannot race the
// server's own startup.
func startSmokeWorker(
	t *testing.T,
	cfg config.Config,
	db *mongo.Database,
	commands temporal.DeviceCommander,
) temporalclient.Client {
	t.Helper()
	ctx := context.Background()
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		t.Fatalf("connect to temporal: %v", err)
	}
	t.Cleanup(tc.Close)

	deadline := time.Now().Add(smokeTemporalStart)
	for {
		_, err := tc.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("temporal frontend did not answer within %v: %v", smokeTemporalStart, err)
		}
		time.Sleep(smokePollInterval)
	}

	w := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{})
	registerRollout(w, newRolloutDeps(db, commands, rolloutHealthSettings(cfg.Rollout)))

	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			slog.Error("smoke worker stopped", "err", err)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Log("smoke worker did not stop within 30s")
		}
	})
	return tc
}

// seedFleet registers n devices of the firmware's model in region and returns their identities.
func seedFleet(t *testing.T, db *mongo.Database, region string, n int) []string {
	t.Helper()
	ctx := context.Background()
	ids := make([]string, 0, n)
	for i := range n {
		id := fmt.Sprintf("smoke-%s-%02d", region, i)
		_, err := db.Collection("devices").InsertOne(ctx, bson.D{
			{Key: "_id", Value: id},
			{Key: "model", Value: "oak-s3"},
			{Key: "region", Value: region},
			{Key: "current_fw", Value: "1.0.0"},
			{Key: "status", Value: "online"},
			{Key: "last_heartbeat", Value: time.Now().UTC()},
		})
		if err != nil {
			t.Fatalf("seed device %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// seedFirmware stores the firmware document the rollouts deploy, through the real validator.
func seedFirmware(t *testing.T, db *mongo.Database) {
	t.Helper()
	_, err := db.Collection("firmware").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: "fw-smoke"},
		{Key: "version", Value: "2.0.0"},
		{Key: "models", Value: bson.A{"oak-s3"}},
		{Key: "checksum", Value: "sha256:smoke"},
		{Key: "size", Value: 1024},
		{Key: "created_at", Value: time.Now().UTC()},
		{Key: "gridfs_id", Value: "fwbin-smoke"},
	})
	if err != nil {
		t.Fatalf("seed firmware: %v", err)
	}
}

// startHeartbeats streams heartbeats for every fleet at the given health through the ingestion
// contract — the emulator's role, minus the stream — until the returned stop function is called.
// Samples keep arriving for as long as the rollouts run, so every wave's window holds evidence.
func startHeartbeats(
	t *testing.T,
	db *mongo.Database,
	fleets map[string][]string,
	health float64,
) func() {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		ticker := time.NewTicker(smokeHeartbeatGap)
		defer ticker.Stop()
		written := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				written++
				now := time.Now().UTC()
				for region, ids := range fleets {
					for _, id := range ids {
						eventID := fmt.Sprintf("smoke-%s-%d", id, written)
						_, err := db.Collection("telemetry").UpdateOne(ctx,
							bson.D{{Key: "_id", Value: eventID}},
							bson.D{{Key: "$setOnInsert", Value: bson.D{
								{Key: "_id", Value: eventID},
								{Key: "ts", Value: now},
								{Key: "meta", Value: bson.D{
									{Key: "device_id", Value: id},
									{Key: "region", Value: region},
									{Key: "model", Value: "oak-s3"},
								}},
								{Key: "cpu", Value: 0.2},
								{Key: "mem", Value: 0.3},
								{Key: "health", Value: health},
							}}},
							options.UpdateOne().SetUpsert(true),
						)
						if err != nil {
							t.Errorf("write heartbeat for %s: %v", id, err)
							return
						}
					}
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// waitForRolloutStatus polls the rollout document until it carries status, returning the record.
func waitForRolloutStatus(
	t *testing.T,
	coll *mongo.Collection,
	id string,
	status rollout.RolloutStatus,
) rollout.RolloutRecord {
	t.Helper()
	deadline := time.Now().Add(smokePollTimeout)
	var last rollout.RolloutRecord
	for time.Now().Before(deadline) {
		var rec rollout.RolloutRecord
		err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: id}}).Decode(&rec)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
			// The workflow has not written the document yet.
		case err != nil:
			t.Fatalf("read rollout %s: %v", id, err)
		default:
			last = rec
			if rec.Status == status {
				return rec
			}
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("rollout %s reached %q, want %q within %v", id, last.Status, status, smokePollTimeout)
	return rollout.RolloutRecord{}
}

// waitForWave polls until the wave document is written and decided, returning it.
func waitForWave(t *testing.T, coll *mongo.Collection, id string) rollout.WaveRecord {
	t.Helper()
	deadline := time.Now().Add(smokePollTimeout)
	for time.Now().Before(deadline) {
		var rec rollout.WaveRecord
		err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: id}}).Decode(&rec)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
		case err != nil:
			t.Fatalf("read wave %s: %v", id, err)
		default:
			switch rec.Status {
			case rollout.WaveHealthy, rollout.WaveUnhealthy, rollout.WaveFailed, rollout.WaveSkipped:
				return rec
			}
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("wave %s was not decided within %v", id, smokePollTimeout)
	return rollout.WaveRecord{}
}

// countWaves counts a rollout's recorded waves.
func countWaves(t *testing.T, coll *mongo.Collection, rolloutID string) int64 {
	t.Helper()
	count, err := coll.CountDocuments(context.Background(), bson.D{{Key: "rollout_id", Value: rolloutID}})
	if err != nil {
		t.Fatalf("count waves of %s: %v", rolloutID, err)
	}
	return count
}

// assertDispatched checks that every targeted device was commanded exactly once, carrying the
// firmware the rollout deploys, under the command id its wave and device derive.
func assertDispatched(t *testing.T, dispatcher *commandRecorder, waves []rollout.WaveRecord) {
	t.Helper()
	want := make(map[string]string)
	for _, wave := range waves {
		for _, device := range wave.DeviceIDs {
			want[temporal.CommandID(wave.ID, device)] = device
		}
	}

	commands := dispatcher.recorded()
	if len(commands) != len(want) {
		t.Fatalf("dispatched commands = %d, want one per targeted device (%d)", len(commands), len(want))
	}
	seen := make(map[string]bool, len(commands))
	for _, cmd := range commands {
		device, ok := want[cmd.CommandID]
		if !ok {
			t.Errorf("command %s targets no recorded membership", cmd.CommandID)
			continue
		}
		if seen[cmd.CommandID] {
			t.Errorf("command %s was dispatched twice", cmd.CommandID)
		}
		seen[cmd.CommandID] = true
		if cmd.DeviceID != device {
			t.Errorf("command %s targets %q, want %q", cmd.CommandID, cmd.DeviceID, device)
		}
		if cmd.Kind != temporal.CommandKindUpdate || cmd.FirmwareID != "fw-smoke" ||
			cmd.Version != "2.0.0" || cmd.Checksum != "sha256:smoke" {
			t.Errorf("command %s carries %q/%q/%q/%q, want the deployed firmware update",
				cmd.CommandID, cmd.Kind, cmd.FirmwareID, cmd.Version, cmd.Checksum)
		}
	}
}
