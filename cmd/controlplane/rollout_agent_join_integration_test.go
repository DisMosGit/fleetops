//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/temporaltest"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// The world the join test drives: one region of one model holding one device, running the firmware
// a rollout deploys, so the wave's target set, the command it builds, and the report that comes
// back are the production ones rather than a stub's.
const (
	// joinFirmwareID, joinFirmwareVersion, and joinFirmwareChecksum are what the wave must carry
	// to the agent: the identity, version, and digest the firmware registry holds.
	joinFirmwareID       = "fw-join"
	joinFirmwareVersion  = "9.9.9"
	joinFirmwareChecksum = "sha256:join-checksum"
	// joinFailureDetail is what the agent reports when it fails the update, so the outcome's
	// detail can be followed from the report to the device workflow.
	joinFailureDetail = "join test: flash write failed"
	// joinHealthWindow and joinMinSamples are the wave's gate: a two-second window over a sampler
	// that writes every couple of hundred milliseconds decides in seconds.
	joinHealthWindow = 2 * time.Second
	joinMinSamples   = 2
	// joinResultTimeout and joinDecisionTimeout size the wave's two policy windows. Both are sized
	// for the world rather than for a prompt device: the assertions below are about the report
	// travelling at all, so a loaded host must not be able to turn a report that arrives in a second
	// into a device the wave gave up on, or into a wave its own decision window ran out on.
	joinResultTimeout   = 30 * time.Second
	joinDecisionTimeout = time.Minute
	// joinGrace bounds every wait in the test, so a chain that never joined fails with what was
	// missing rather than hanging.
	joinGrace = 90 * time.Second
	// joinFleetPeriod is the cadence the test's one-device fleet heartbeats at. It is deliberately
	// far slower than the sampler below: every heartbeat reaches the device workflow as a signal, and
	// a fleet that outruns the entity it feeds only proves the entity's queue is finite.
	joinFleetPeriod = time.Second
	// joinHealthGap is the cadence the test's sampler writes healthy samples at.
	joinHealthGap = 200 * time.Millisecond
	// joinPollInterval is the pause between the test's polls.
	joinPollInterval = 50 * time.Millisecond
)

// TestRolloutReachesARealAgent is the join the rest of the suite stubs: both rollout suites bind the
// device command seam to an in-process recorder and every streaming suite ignores rollouts, so
// nothing connected the command a rollout builds to the agent that receives it.
//
// This test drives the whole chain over real dependencies: the rollout workflow resolves its wave
// against the real fleet database and signals the real device workflow; the device workflow
// schedules the real dispatch activity on the control-plane queue; that activity sends through a
// real hub onto a real agent stream; the agent reports the terminal result back through the real
// report RPC; the control plane signals the device workflow with it; and the wave records the
// outcome the device reported.
func TestRolloutReachesARealAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.DiscardHandler)

	server := temporaltest.For(t)
	db := mongotest.For(t).DB
	cfg := config.Defaults()
	cfg.Temporal.Address = server.Address
	cfg.Temporal.Namespace = server.Namespace

	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		t.Fatalf("connect to temporal: %v", err)
	}
	t.Cleanup(tc.Close)
	// The device workflow upserts search attributes, so the namespace carries them before any
	// workflow runs; without this the first update fails the way it would on an unbootstrapped
	// namespace.
	if err := temporal.EnsureSearchAttributes(ctx, tc.OperatorService(), cfg.Temporal.Namespace); err != nil {
		t.Fatalf("bootstrap the namespace's search attributes: %v", err)
	}

	// The one device the wave targets is a real emulator device, so the identity the agent
	// registers, the record the fleet database holds, and the selector the rollout resolves are
	// the same identity rather than three copies of a literal.
	ids, err := agent.NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen() error = %v", err)
	}
	fleet, err := agent.NewFleet(1, agent.FleetOptions{
		IDs:    ids,
		Source: agent.NewSimulation(rand.NewSource(1)),
		Period: joinFleetPeriod,
	})
	if err != nil {
		t.Fatalf("NewFleet() error = %v", err)
	}
	device := fleet.Identities()[0]

	seedJoinFirmware(t, db, device.Model)
	seedJoinDevice(t, db, device)
	stopHealth := startJoinHealth(t, db, device)
	t.Cleanup(stopHealth)

	// The control plane's half: the agent server, its hub, and the dispatch activity the device
	// workflow schedules onto the control-plane queue.
	sink := newJoinSink()
	hub := agentserver.NewHub(sink, log)
	signaler := temporal.NewSignaler(tc, cfg.Temporal.TaskQueue, temporal.DeviceSettings{
		SnapshotInterval:  time.Minute,
		OfflineThreshold:  time.Minute,
		DispatchTaskQueue: cfg.Temporal.DispatchTaskQueue,
	})
	grpcServer := grpc.NewServer(agentserver.ServerOptions(log)...)
	agentv1.RegisterAgentServiceServer(grpcServer,
		agentserver.NewServer(hub, devices.NewStore(db), signaler, firmware.NewStore(db), log))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for agents: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	// The agent's half: a real client on a real stream, with a handler that reports the terminal
	// result through the same RPC the emulator uses.
	conn, err := agent.Dial(listener.Addr().String())
	if err != nil {
		t.Fatalf("dial the control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close the agent connection: %v", err)
		}
	})
	raw := agentv1.NewAgentServiceClient(conn)
	relay := &agent.StatusRelay{}
	handler := &joinCommandHandler{
		reporter: agent.NewUpdateReporter(relay, raw),
		outcome:  agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED,
		detail:   joinFailureDetail,
	}
	client, err := agent.NewClient(raw, agent.Options{
		Devices:        fleet.Identities(),
		Handler:        handler,
		IDs:            ids,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Log:            log,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	relay.Bind(client)
	heartbeats := make(chan *agentv1.Heartbeat, 8)
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx, heartbeats) }()
	fleetDone := make(chan error, 1)
	go func() { fleetDone <- fleet.Run(ctx, heartbeats) }()

	// Both shapes of process in this one binary: the worker's registrations on the configured work
	// queue, and the control plane's single activity on its own queue.
	work := worker.New(tc, cfg.Temporal.TaskQueue, worker.Options{})
	registerJoinDevice(work, db)
	registerJoinRollout(work, tc, db, signaler)
	control := worker.New(tc, cfg.Temporal.DispatchTaskQueue, worker.Options{})
	control.RegisterActivityWithOptions(temporal.NewDispatchActivity(hub), activity.RegisterOptions{
		Name: temporal.DispatchActivityName,
	})
	startJoinWorker(t, work)
	startJoinWorker(t, control)

	// The command can only reach the agent once its stream has enrolled the device, so wait for a
	// heartbeat to arrive through the hub before the rollout can dispatch anything.
	sink.awaitHeartbeat(t, joinGrace)

	settings, err := temporal.NewRolloutSettings(
		joinHealthWindow, joinDecisionTimeout, joinResultTimeout,
		[]temporal.RolloutWave{{Percent: 100}},
	)
	if err != nil {
		t.Fatalf("build rollout settings: %v", err)
	}
	const rolloutID = "ro-join"
	if err := temporal.NewRolloutStarter(tc, cfg.Temporal.TaskQueue, settings).
		Start(ctx, temporal.RolloutRequest{
			RolloutID:  rolloutID,
			FirmwareID: joinFirmwareID,
			Region:     device.Region,
			Model:      device.Model,
		}); err != nil {
		t.Fatalf("start the rollout: %v", err)
	}

	// 1. The command reached the agent carrying the firmware the wave resolved.
	command := handler.awaitCommand(t, joinGrace)
	update := command.GetStartUpdate()
	if update == nil {
		t.Fatalf("the agent received %T, want a start-update command", command.GetKind())
	}
	if got := update.GetFirmwareId(); got != joinFirmwareID {
		t.Errorf("commanded firmware id = %q, want %q", got, joinFirmwareID)
	}
	if got := update.GetVersion(); got != joinFirmwareVersion {
		t.Errorf("commanded firmware version = %q, want %q", got, joinFirmwareVersion)
	}
	if got := update.GetChecksum(); got != joinFirmwareChecksum {
		t.Errorf("commanded checksum = %q, want the registry's %q", got, joinFirmwareChecksum)
	}
	if got := command.GetDeviceId(); got != device.ID {
		t.Errorf("commanded device = %q, want %q", got, device.ID)
	}

	// 2. The agent's report reached the device workflow, with the outcome and detail it reported.
	state := awaitJoinState(t, tc, device.ID, func(s temporal.State) bool {
		return s.LastCommand != nil && s.LastCommand.Command.CommandID == command.GetCommandId()
	})
	if got := state.LastCommand.Outcome; got != temporal.OutcomeFailed {
		t.Errorf("device workflow recorded outcome %q, want %q", got, temporal.OutcomeFailed)
	}
	if got := state.LastCommand.Detail; got != joinFailureDetail {
		t.Errorf("device workflow recorded detail %q, want the reported %q", got, joinFailureDetail)
	}

	// 3. The wave recorded that device's outcome, and the rollout concluded with it.
	wave := awaitJoinWave(t, db, rollout.WaveID(rolloutID, 0, 100))
	if want := []string{device.ID}; !slices.Equal(wave.FailedDeviceIDs, want) {
		t.Errorf("wave failed_device_ids = %v, want %v", wave.FailedDeviceIDs, want)
	}
	if len(wave.UnreportedDeviceIDs) != 0 {
		t.Errorf("wave unreported_device_ids = %v, want none: the device reported",
			wave.UnreportedDeviceIDs)
	}
	if want := []string{device.ID}; !slices.Equal(wave.DeviceIDs, want) {
		t.Errorf("wave membership = %v, want the one targeted device %v", wave.DeviceIDs, want)
	}
	// A device outcome is recorded, not gating: the health gate promoted the wave over a fleet
	// that reported healthy, which is what makes the failed device's own record the evidence that
	// the report travelled.
	if wave.Status != rollout.WaveHealthy {
		t.Errorf("wave status = %q, want %q", wave.Status, rollout.WaveHealthy)
	}
	awaitJoinRollout(t, db, rolloutID, rollout.RolloutCompleted)

	cancel()
	if err := <-clientDone; err != nil {
		t.Errorf("agent client Run() error = %v, want nil", err)
	}
	if err := <-fleetDone; err != nil {
		t.Errorf("agent fleet Run() error = %v, want nil", err)
	}
}

// joinRegistry is the registration surface this test needs from a Temporal worker: both
// workflows and activities. The control plane's own seam is deliberately activity-only, because
// the process hosts no workflow; the test's worker-shaped half hosts both, so it states the
// wider surface itself.
type joinRegistry interface {
	// RegisterWorkflowWithOptions registers a workflow function under an explicit name.
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	// RegisterActivityWithOptions registers an activity function under an explicit name.
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// joinSink is the hub's heartbeat sink: it records that the agent's stream is live and enrolled,
// which is the one fact the test needs before it can expect a command to be routable. Ingestion is
// not what this test joins, so it consumes nothing else.
type joinSink struct {
	mu   sync.Mutex
	seen int
	beat chan struct{}
	once sync.Once
}

// newJoinSink returns a sink whose beat channel closes on the first heartbeat it consumes.
func newJoinSink() *joinSink { return &joinSink{beat: make(chan struct{})} }

// Handle consumes one routed heartbeat.
func (s *joinSink) Handle(_ context.Context, _ *agentv1.Heartbeat, _ devices.Record) error {
	s.mu.Lock()
	s.seen++
	s.mu.Unlock()
	s.once.Do(func() { close(s.beat) })
	return nil
}

// awaitHeartbeat waits until one heartbeat has arrived through the hub, which is the evidence that
// the agent registered and its device is routable.
func (s *joinSink) awaitHeartbeat(t *testing.T, grace time.Duration) {
	t.Helper()
	select {
	case <-s.beat:
	case <-time.After(grace):
		t.Fatalf("no heartbeat reached the hub within %v: the agent never enrolled", grace)
	}
}

// joinCommandHandler is the agent side of the seam: it records the command it received and reports
// the outcome the test scripts through the real report RPC, exactly as the emulator's update
// handler does.
type joinCommandHandler struct {
	reporter *agent.UpdateReporter
	outcome  agentv1.CommandOutcome
	detail   string

	mu      sync.Mutex
	command *agentv1.Command
}

// Handle records one command and reports its terminal result.
func (h *joinCommandHandler) Handle(ctx context.Context, command *agentv1.Command) error {
	h.mu.Lock()
	h.command = command
	h.mu.Unlock()

	if err := h.reporter.Status(ctx, command.GetDeviceId(),
		command.GetStartUpdate().GetFirmwareId(),
		agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, 0, ""); err != nil {
		return fmt.Errorf("report the downloading phase: %w", err)
	}
	if err := h.reporter.Conclude(ctx, command, h.outcome, h.detail); err != nil {
		return fmt.Errorf("report the command's result: %w", err)
	}
	return nil
}

// awaitCommand waits until the agent has received a command.
func (h *joinCommandHandler) awaitCommand(t *testing.T, grace time.Duration) *agentv1.Command {
	t.Helper()
	deadline := time.Now().Add(grace)
	for {
		h.mu.Lock()
		command := h.command
		h.mu.Unlock()
		if command != nil {
			return command
		}
		if time.Now().After(deadline) {
			t.Fatalf("no command reached the agent within %v", grace)
		}
		time.Sleep(joinPollInterval)
	}
}

// registerJoinDevice registers the device entity and its snapshot activity, which is the workflow
// the rollout's per-device update activity signals.
func registerJoinDevice(w joinRegistry, db *mongo.Database) {
	w.RegisterWorkflowWithOptions(temporal.DeviceWorkflow, workflow.RegisterOptions{
		Name: temporal.DeviceWorkflowName,
	})
	w.RegisterActivityWithOptions(
		temporal.NewSnapshotActivity(devices.NewSnapshotStore(db)),
		activity.RegisterOptions{Name: temporal.SnapshotActivityName},
	)
}

// registerJoinRollout registers the rollout workflow and the activities it needs to reach the
// device: the fleet-database reads and writes, the per-device update that signals the device
// workflow and waits for its reported result, the rollback activities, and the health evaluation.
// The dispatch activity is deliberately absent: it belongs to the control-plane queue, where the
// other worker in this test registers it.
func registerJoinRollout(
	w joinRegistry,
	tc temporalclient.Client,
	db *mongo.Database,
	signaler *temporal.Signaler,
) {
	store := rollout.NewStore(db)
	samples := wavehealth.NewStore(db)
	registry := firmware.NewStore(db)
	records := devices.NewStore(db)
	states := temporal.NewDeviceStates(tc)

	w.RegisterWorkflowWithOptions(temporal.RolloutWorkflow, workflow.RegisterOptions{
		Name: temporal.RolloutWorkflowName,
	})
	activities := []struct {
		name string
		fn   any
	}{
		{temporal.LoadFirmwareActivityName, temporal.NewLoadFirmwareActivity(registry)},
		{temporal.ResolveWaveTargetsActivityName, temporal.NewResolveWaveActivity(store)},
		{temporal.RecordRolloutStateActivityName, temporal.NewRecordRolloutActivity(store)},
		{temporal.RecordWaveStateActivityName, temporal.NewRecordWaveActivity(store)},
		{temporal.UpdateDeviceActivityName, temporal.NewUpdateDeviceActivity(
			signaler, states, temporal.WithUpdatePollInterval(joinPollInterval),
		)},
		{temporal.DowngradeDeviceActivityName, temporal.NewDowngradeDeviceActivity(
			signaler, states, registry,
		)},
		{temporal.ReconcileInventoryActivityName, temporal.NewReconcileInventoryActivity(
			states, records,
		)},
		{temporal.AnnounceRollbackActivityName, temporal.NewAnnounceRollbackActivity(joinNotifier{})},
		{temporal.EvaluateWaveHealthActivityName, temporal.NewEvaluateWaveActivity(
			wavehealth.New(samples, samples, wavehealth.Settings{
				HealthWindow:          joinHealthWindow,
				SampleHealthThreshold: 0.6,
				MinSuccessRatio:       0.95,
				MinSamples:            joinMinSamples,
			}),
		)},
	}
	for _, a := range activities {
		w.RegisterActivityWithOptions(a.fn, activity.RegisterOptions{Name: a.name})
	}
}

// joinNotifier is the rollback announcement seam: this rollout is expected to conclude healthy, so
// an announcement is a failure the test would rather see than have published to a broker.
type joinNotifier struct{}

// Announce reports that no announcement was expected.
func (joinNotifier) Announce(context.Context, telemetry.RollbackEvent) error {
	return errors.New("join test: unexpected rollback announcement")
}

// startJoinWorker runs one worker until the test ends.
func startJoinWorker(t *testing.T, w worker.Worker) {
	t.Helper()
	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			t.Logf("join worker stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Log("a join worker did not stop within 30s")
		}
	})
}

// seedJoinFirmware stores the firmware document the wave resolves, through the real validator.
func seedJoinFirmware(t *testing.T, db *mongo.Database, model string) {
	t.Helper()
	_, err := db.Collection("firmware").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: joinFirmwareID},
		{Key: "version", Value: joinFirmwareVersion},
		{Key: "models", Value: bson.A{model}},
		{Key: "checksum", Value: joinFirmwareChecksum},
		{Key: "size", Value: 1024},
		{Key: "created_at", Value: time.Now().UTC()},
		{Key: "gridfs_id", Value: "fwbin-join"},
	})
	if err != nil {
		t.Fatalf("seed firmware %s: %v", joinFirmwareID, err)
	}
}

// seedJoinDevice registers the one device the wave targets.
func seedJoinDevice(t *testing.T, db *mongo.Database, device agent.Identity) {
	t.Helper()
	_, err := db.Collection("devices").InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: device.ID},
		{Key: "model", Value: device.Model},
		{Key: "region", Value: device.Region},
		{Key: "current_fw", Value: device.Firmware},
		{Key: "status", Value: "online"},
		{Key: "last_heartbeat", Value: time.Now().UTC()},
	})
	if err != nil {
		t.Fatalf("seed device %s: %v", device.ID, err)
	}
}

// startJoinHealth writes healthy samples for the targeted device through the ingestion contract's
// shape until the returned stop function is called, so the wave's gate has evidence to measure. The
// samples carry a sequence of their own, so each one is a new document rather than an upsert of the
// one before it.
func startJoinHealth(t *testing.T, db *mongo.Database, device agent.Identity) func() {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		ticker := time.NewTicker(joinHealthGap)
		defer ticker.Stop()
		written := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				written++
				now := time.Now().UTC()
				eventID := fmt.Sprintf("join-health-%d", written)
				_, err := db.Collection("telemetry").UpdateOne(ctx,
					bson.D{{Key: "_id", Value: eventID}},
					bson.D{{Key: "$setOnInsert", Value: bson.D{
						{Key: "_id", Value: eventID},
						{Key: "ts", Value: now},
						{Key: "meta", Value: bson.D{
							{Key: "device_id", Value: device.ID},
							{Key: "region", Value: device.Region},
							{Key: "model", Value: device.Model},
						}},
						{Key: "cpu", Value: 0.2},
						{Key: "mem", Value: 0.3},
						{Key: "health", Value: 0.95},
					}}},
					options.UpdateOne().SetUpsert(true),
				)
				if err != nil {
					t.Errorf("write a health sample for %s: %v", device.ID, err)
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

// awaitJoinState polls the device workflow's state query until it holds what the test waits for.
func awaitJoinState(
	t *testing.T,
	tc temporalclient.Client,
	deviceID string,
	held func(temporal.State) bool,
) temporal.State {
	t.Helper()
	states := temporal.NewDeviceStates(tc)
	deadline := time.Now().Add(joinGrace)
	var last temporal.State
	var lastErr error
	for time.Now().Before(deadline) {
		state, err := states.State(context.Background(), deviceID)
		switch {
		case err != nil:
			lastErr = err
		case held(state):
			return state
		default:
			last = state
		}
		time.Sleep(joinPollInterval)
	}
	t.Fatalf("device %s never reached the expected state: last state %+v, last error %v",
		deviceID, last, lastErr)
	return temporal.State{}
}

// awaitJoinWave polls until the wave is decided and returns it.
func awaitJoinWave(t *testing.T, db *mongo.Database, id string) rollout.WaveRecord {
	t.Helper()
	deadline := time.Now().Add(joinGrace)
	for time.Now().Before(deadline) {
		var rec rollout.WaveRecord
		err := db.Collection("waves").FindOne(context.Background(), bson.D{{Key: "_id", Value: id}}).Decode(&rec)
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
		time.Sleep(joinPollInterval)
	}
	t.Fatalf("wave %s was not decided within %v", id, joinGrace)
	return rollout.WaveRecord{}
}

// awaitJoinRollout polls until the rollout reaches status.
func awaitJoinRollout(t *testing.T, db *mongo.Database, id string, status rollout.RolloutStatus) {
	t.Helper()
	deadline := time.Now().Add(joinGrace)
	var last rollout.RolloutStatus
	for time.Now().Before(deadline) {
		var rec rollout.RolloutRecord
		err := db.Collection("rollouts").FindOne(context.Background(), bson.D{{Key: "_id", Value: id}}).Decode(&rec)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
		case err != nil:
			t.Fatalf("read rollout %s: %v", id, err)
		default:
			last = rec.Status
			if rec.Status == status {
				return
			}
		}
		time.Sleep(joinPollInterval)
	}
	t.Fatalf("rollout %s reached %q, want %q within %v", id, last, status, joinGrace)
}
