//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
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
	"github.com/DisMosGit/fleetops/internal/rolloutapi"
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
	// The update activity's observation interval is shrunk the same way the window is: the
	// smoke drives the real activity, and a device that never reports must be provable in
	// seconds rather than in the production interval's worth of polls.
	smokeUpdatePollInterval = 50 * time.Millisecond
	// smokeResultTimeout is how long a wave in the smoke waits for one device's reported result:
	// long enough for a device that reports late to be found, short enough that a device which
	// never reports is proven in seconds.
	smokeResultTimeout = 3 * time.Second
	// smokeBackendGrace is how long an operator request waits out a backend that is not answering,
	// long enough to outlast a stall of the smoke's single-node Temporal storage.
	smokeBackendGrace = 30 * time.Second
)

// commandRecorder stands in for the device command seam: it records every update command a wave
// dispatches and reports each commanded device as having concluded it successfully, which is the
// device world a healthy wave is measured over. The device side of the seam — the entity workflow
// that accepts a command and the control-plane activity that streams it to an agent — has its own
// tests; what this smoke drives for real is the rollout pipeline itself: the workflow, its six
// activities, the fleet database, the configuration, and the start path.
type commandRecorder struct {
	mu       sync.Mutex
	commands []temporal.CommandIssuedSignal
	// concluded records the outcome each device reported, keyed by command id.
	concluded map[string]temporal.ConcludedCommand
	// scripts is the device world the smoke drives: which devices fail, and which never report.
	scripts *deviceScripts
}

// deviceScripts scripts the devices the smoke drives: the ones that report a failed update with
// the detail they report, and the ones that never report at all.
type deviceScripts struct {
	// fail maps a device to the failure detail it reports.
	fail map[string]string
	// never marks a device that accepts its command and never reports a result.
	never map[string]bool
}

// script returns the recorder's device world, creating it on first use.
func (r *commandRecorder) script() *deviceScripts {
	if r.scripts == nil {
		r.scripts = &deviceScripts{}
	}
	if r.scripts.fail == nil {
		r.scripts.fail = map[string]string{}
	}
	if r.scripts.never == nil {
		r.scripts.never = map[string]bool{}
	}
	return r.scripts
}

// deliveredCommands returns the commands one device accepted, in delivery order.
func (r *commandRecorder) deliveredCommands(deviceID string) []temporal.CommandIssuedSignal {
	r.mu.Lock()
	defer r.mu.Unlock()
	var delivered []temporal.CommandIssuedSignal
	for _, cmd := range r.commands {
		if cmd.DeviceID == deviceID {
			delivered = append(delivered, cmd)
		}
	}
	return delivered
}

// SignalCommandIssued implements temporal.DeviceCommander. It records the command and decides the
// outcome the device will report: a success unless the device is scripted to fail or to stay
// silent.
func (r *commandRecorder) SignalCommandIssued(_ context.Context, cmd temporal.CommandIssuedSignal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, cmd)
	if r.scripts == nil {
		r.scripts = &deviceScripts{}
	}
	if r.scripts.never[cmd.DeviceID] {
		return nil
	}
	if r.concluded == nil {
		r.concluded = map[string]temporal.ConcludedCommand{}
	}
	outcome, detail := temporal.OutcomeSucceeded, ""
	if failed, ok := r.scripts.fail[cmd.DeviceID]; ok {
		outcome, detail = temporal.OutcomeFailed, failed
	}
	r.concluded[cmd.CommandID] = temporal.ConcludedCommand{
		Command: cmd, Outcome: outcome, Detail: detail,
	}
	return nil
}

// State implements temporal.DeviceStateReader: it reports the state of the device that accepted
// the command, which is how the update activity's wait for a reported result ends.
func (r *commandRecorder) State(_ context.Context, deviceID string) (temporal.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := temporal.State{DeviceID: deviceID, CurrentFw: "1.0.0"}
	// The newest command this device accepted is the one its state reports on.
	for i := len(r.commands) - 1; i >= 0; i-- {
		if r.commands[i].DeviceID != deviceID {
			continue
		}
		if concluded, ok := r.concluded[r.commands[i].CommandID]; ok {
			state.LastCommand = &concluded
			return state, nil
		}
		state.Pending = &temporal.PendingCommand{Command: r.commands[i], Dispatched: true}
		return state, nil
	}
	return temporal.State{}, fmt.Errorf("device %s: %w", deviceID, temporal.ErrDeviceNotFound)
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
	// The result timeout is shrunk the same way the window is: the smoke drives the real
	// per-device update activity, and a device that never reports has to be provable in seconds.
	cfg.Rollout.ResultTimeout = config.Duration{Duration: smokeResultTimeout}
	cfg.Rollout.MinSamples = smokeMinSamples
	cfg.Rollout.SampleHealthThreshold = 0.6
	cfg.Rollout.MinSuccessRatio = 0.95

	dispatcher := &commandRecorder{}
	tc := startSmokeWorker(t, cfg, db, dispatcher, dispatcher)
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
	canarySettings := smokeRolloutSettings(t, cfg)
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
	gatedSettings := smokeRolloutSettings(t, cfg)
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
	brokenSettings := smokeRolloutSettings(t, cfg)
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

	// 4. Every target device is commanded, and the wave records how each of them ended: one
	// succeeded, one reported a failure, and one never reported before the result timeout.
	outcomesFleet := seedFleet(t, db, "sa-east", 3)
	stopOutcomes := startHeartbeats(t, db, map[string][]string{"sa-east": outcomesFleet}, 0.95)
	t.Cleanup(stopOutcomes)
	scripts := dispatcher.script()
	scripts.fail[outcomesFleet[1]] = "flash write failed"
	scripts.never[outcomesFleet[2]] = true

	outcomes := temporal.RolloutRequest{
		RolloutID: "ro-smoke-outcomes", FirmwareID: "fw-smoke", Region: "sa-east", Model: "oak-s3",
	}
	outcomesSettings := smokeRolloutSettings(t, cfg)
	outcomesSettings.Waves = []temporal.RolloutWave{{Percent: 100}}
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, outcomesSettings).
		Start(ctx, outcomes); err != nil {
		t.Fatalf("start outcomes rollout: %v", err)
	}
	waitForRolloutStatus(t, rollouts, outcomes.RolloutID, rollout.RolloutCompleted)

	recorded := waitForWave(t, waves, rollout.WaveID(outcomes.RolloutID, 0, 100))
	if want := []string{outcomesFleet[1]}; !slices.Equal(recorded.FailedDeviceIDs, want) {
		t.Errorf("wave failed_device_ids = %v, want %v", recorded.FailedDeviceIDs, want)
	}
	if want := []string{outcomesFleet[2]}; !slices.Equal(recorded.UnreportedDeviceIDs, want) {
		t.Errorf("wave unreported_device_ids = %v, want %v", recorded.UnreportedDeviceIDs, want)
	}
	// The devices that did not take the update do not decide the wave: the health gate still
	// promoted it, which is what the configured ratio is for.
	if recorded.Status != rollout.WaveHealthy {
		t.Errorf("wave status = %q, want healthy: a device outcome is recorded, not gating",
			recorded.Status)
	}
	for _, deviceID := range outcomesFleet {
		assertCommandedUnderOneID(t, dispatcher, recorded.ID, deviceID)
	}

	// 5. The operator's HTTP surface, over the real client adapters: the same rollout's state,
	// and its approve, pause, and resume commands delivered as the workflow's own signals.
	operatorAPI := rolloutapi.NewHandler(
		temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, outcomesSettings),
		temporal.NewRolloutStates(tc),
		temporal.NewRolloutSignals(tc),
		smokeLogger(),
	)
	statePath := "/api/rollouts/" + outcomes.RolloutID
	if status, body := doRequest(t, operatorAPI, http.MethodGet, statePath); status != http.StatusOK {
		t.Errorf("GET %s = %d (body %s), want 200", statePath, status, body)
	} else {
		var view temporal.RolloutView
		if err := json.Unmarshal([]byte(body), &view); err != nil {
			t.Errorf("decode state body %s: %v", body, err)
		} else {
			if view.Status != rollout.RolloutCompleted {
				t.Errorf("state status = %q, want %q", view.Status, rollout.RolloutCompleted)
			}
			if len(view.Waves) != 1 || view.Waves[0].FailedCount != 1 ||
				view.Waves[0].UnreportedCount != 1 {
				t.Errorf("state waves = %+v, want 1 failed and 1 unreported device", view.Waves)
			}
		}
	}

	// A command on a rollout that already concluded is refused rather than accepted, and a
	// command on a rollout that does not exist is not found.
	if status, body := doRequest(t, operatorAPI, http.MethodPost, statePath+"/pause"); status != http.StatusConflict {
		t.Errorf("POST pause on a concluded rollout = %d (body %s), want 409", status, body)
	}
	missing := "/api/rollouts/ro-smoke-missing"
	if status, body := doRequest(t, operatorAPI, http.MethodPost, missing+"/pause"); status != http.StatusNotFound {
		t.Errorf("POST pause on an unknown rollout = %d (body %s), want 404", status, body)
	}

	// A pause on a running rollout is delivered and visible, and a resume continues it.
	live := temporal.RolloutRequest{
		RolloutID: "ro-smoke-http", FirmwareID: "fw-smoke", Region: "eu-west", Model: "oak-s3",
	}
	liveSettings := smokeRolloutSettings(t, cfg)
	liveSettings.Waves = []temporal.RolloutWave{{Percent: 25}, {Percent: 100, RequireApproval: true}}
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, liveSettings).
		Start(ctx, live); err != nil {
		t.Fatalf("start http rollout: %v", err)
	}
	livePath := "/api/rollouts/" + live.RolloutID
	waitForRolloutStatus(t, rollouts, live.RolloutID, rollout.RolloutAwaitingApproval)

	if status, body := doRequest(t, operatorAPI, http.MethodPost, livePath+"/pause"); status != http.StatusAccepted {
		t.Fatalf("POST pause = %d (body %s), want 202", status, body)
	}
	if status, body := doRequest(t, operatorAPI, http.MethodPost, livePath+"/pause"); status != http.StatusAccepted {
		t.Fatalf("POST repeated pause = %d (body %s), want 202", status, body)
	}
	waitForRolloutStatus(t, rollouts, live.RolloutID, rollout.RolloutPaused)

	if status, body := doRequest(t, operatorAPI, http.MethodPost, livePath+"/approve"); status != http.StatusAccepted {
		t.Fatalf("POST approve = %d (body %s), want 202", status, body)
	}
	if status, body := doRequest(t, operatorAPI, http.MethodPost, livePath+"/resume"); status != http.StatusAccepted {
		t.Fatalf("POST resume = %d (body %s), want 202", status, body)
	}
	// The banked approval carries the gated wave, so the rollout runs to completion after the
	// resume rather than holding for a second approval.
	waitForRolloutStatus(t, rollouts, live.RolloutID, rollout.RolloutCompleted)
	if wave := waitForWave(t, waves, rollout.WaveID(live.RolloutID, 1, 100)); wave.Status != rollout.WaveHealthy {
		t.Errorf("gated wave after the resume = %q, want healthy", wave.Status)
	}
}

// smokeLogger is the logger the operator surface is driven with: the smoke runs the surface as the
// test binary would in production, and a boundary log is the only place a backend failure's reason
// survives the handler's operator-safe response.
func smokeLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// doRequest runs one request against handler and returns its status and body, retrying while the
// handler answers 5xx.
//
// The 5xx the surface has is a backend the handler could not reach in time, and the smoke's backend
// is the Temporal dev server's single-node storage, which stalls under this fleet's load and on its
// own system workflows. A stalled read is not an answer about the operator surface, so the request
// is repeated rather than reported as one; a backend that is truly down exhausts the window and the
// last status is returned, which fails the assertion that follows.
func doRequest(t *testing.T, handler http.Handler, method, path string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(smokeBackendGrace)
	var status int
	var body string
	for {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		status, body = rec.Code, rec.Body.String()
		if status < http.StatusInternalServerError || !time.Now().Before(deadline) {
			return status, body
		}
		t.Logf("%s %s = %d (body %s), retrying: the backend did not answer", method, path, status, body)
		time.Sleep(smokePollInterval)
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
// smokeRolloutSettings builds the canary policy the smoke drives from the same configuration the
// control plane maps, so what the smoke exercises is what a real start path hands the workflow.
// The waves are replaced per scenario by the caller; this supplies the configured timing.
func smokeRolloutSettings(t *testing.T, cfg config.Config) temporal.RolloutSettings {
	t.Helper()
	settings, err := temporal.NewRolloutSettings(
		cfg.Rollout.HealthWindow.Duration,
		cfg.Rollout.DecisionTimeout.Duration,
		cfg.Rollout.ResultTimeout.Duration,
		smokeWaves(cfg.Rollout.Waves),
	)
	if err != nil {
		t.Fatalf("build rollout settings: %v", err)
	}
	return settings
}

// smokeWaves maps the configured sequence onto the workflow's own wave type.
func smokeWaves(waves []config.Wave) []temporal.RolloutWave {
	mapped := make([]temporal.RolloutWave, 0, len(waves))
	for _, wave := range waves {
		mapped = append(mapped, temporal.RolloutWave{
			Percent:         wave.Percent,
			RequireApproval: wave.RequireApproval,
		})
	}
	return mapped
}

func startSmokeWorker(
	t *testing.T,
	cfg config.Config,
	db *mongo.Database,
	commands temporal.DeviceCommander,
	devices temporal.DeviceStateReader,
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
	// The namespace bootstrap is part of startup, so the smoke runs it against a real frontend:
	// the attributes a workflow can upsert are the ones this registers.
	if err := bootstrapNamespace(ctx, tc.OperatorService(), cfg.Temporal.Namespace); err != nil {
		t.Fatalf("bootstrap namespace: %v", err)
	}

	deps := newRolloutDeps(db, commands, devices, rolloutHealthSettings(cfg.Rollout))
	deps.updateOptions = []temporal.UpdateDeviceOption{
		temporal.WithUpdatePollInterval(smokeUpdatePollInterval),
	}
	registerRollout(w, deps)

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

// assertDispatched checks that every targeted device was commanded, carrying the firmware the
// rollout deploys, under the command id its wave and device derive.
func assertDispatched(t *testing.T, dispatcher *commandRecorder, waves []rollout.WaveRecord) {
	t.Helper()
	want := make(map[string]string)
	for _, wave := range waves {
		for _, device := range wave.DeviceIDs {
			want[temporal.CommandID(wave.ID, device)] = device
		}
	}

	commands := dispatcher.recorded()
	if len(commands) < len(want) {
		t.Fatalf("dispatched commands = %d, want at least one per targeted device (%d)",
			len(commands), len(want))
	}
	seen := make(map[string]bool, len(commands))
	for _, cmd := range commands {
		device, ok := want[cmd.CommandID]
		if !ok {
			t.Errorf("command %s targets no recorded membership", cmd.CommandID)
			continue
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
	for commandID, device := range want {
		if !seen[commandID] {
			t.Errorf("device %s was never commanded for wave %s", device, commandID)
		}
	}
}

// assertCommandedUnderOneID checks that a device's update reached the device — at least once, since
// an attempt that dies while it waits is retried — always under the one command id its wave and
// device derive. Every delivery carries the same reference, which is what makes a retried attempt a
// no-op for a device that already accepted it, so a device that reports no result must still have
// seen exactly one command id.
func assertCommandedUnderOneID(
	t *testing.T,
	dispatcher *commandRecorder,
	waveID, deviceID string,
) {
	t.Helper()
	commands := dispatcher.deliveredCommands(deviceID)
	if len(commands) == 0 {
		t.Errorf("device %s was never commanded", deviceID)
		return
	}
	want := temporal.CommandID(waveID, deviceID)
	for _, cmd := range commands {
		if cmd.CommandID != want {
			t.Errorf("device %s was commanded with id %q, want %q", deviceID, cmd.CommandID, want)
		}
	}
}
