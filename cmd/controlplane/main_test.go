package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// fakeMarker is a hand-written StaleMarker double replaying scripted sweep results.
type fakeMarker struct {
	mu      sync.Mutex
	results []int64
	calls   chan struct{}
}

func newFakeMarker(results ...int64) *fakeMarker {
	return &fakeMarker{results: results, calls: make(chan struct{}, 64)}
}

func (f *fakeMarker) MarkStale(context.Context, time.Time) (int64, error) {
	f.mu.Lock()
	var result int64
	if len(f.results) > 0 {
		result = f.results[0]
		f.results = f.results[1:]
	}
	f.mu.Unlock()
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return result, nil
}

// waitCalls waits for n more sweep passes.
func waitCalls(t *testing.T, calls chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sweep pass %d of %d", i+1, n)
		}
	}
}

// scrape /metrics on the handler around reg.
func scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close scrape body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus exposition format", got)
	}
	return string(body)
}

func TestMetricsHandlerExposesOfflineTransitions(t *testing.T) {
	t.Parallel()

	reg, counter := newMetrics()
	counter.Add(1)

	body := scrape(t, metricsHandler(reg))
	if !strings.Contains(body, "fleetops_device_offline_transitions_total 1") {
		t.Errorf("scrape output = %q, want fleetops_device_offline_transitions_total with value 1", body)
	}
}

func TestOfflineTransitionsCountedOncePerTransition(t *testing.T) {
	t.Parallel()

	reg, counter := newMetrics()
	// Three transitions across three passes; the pass that finds nothing must not count.
	marker := newFakeMarker(3, 0, 2)
	sweeper := devices.NewSweeper(marker, time.Minute, 5*time.Millisecond,
		func(transitions int64) { counter.Add(float64(transitions)) },
		slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(ctx) }()

	waitCalls(t, marker.calls, 3)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("sweeper Run() error = %v, want nil", err)
	}

	if got := testutil.ToFloat64(counter); got != 5 {
		t.Errorf("fleetops_device_offline_transitions_total = %v, want 5", got)
	}
	body := scrape(t, metricsHandler(reg))
	if !strings.Contains(body, "fleetops_device_offline_transitions_total 5") {
		t.Errorf("scrape output = %q, want fleetops_device_offline_transitions_total with value 5", body)
	}
}

// fakeIngestSink is a hand-written HeartbeatIngest double recording what the fan-out stored,
// with a scripted refusal.
type fakeIngestSink struct {
	mu     sync.Mutex
	stored []string
	err    error
}

// Handle records the heartbeat's event id and returns the scripted error.
func (f *fakeIngestSink) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stored = append(f.stored, hb.GetEventId())
	return f.err
}

// events returns the recorded event ids.
func (f *fakeIngestSink) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stored...)
}

// fakeEventPublisher is a hand-written HeartbeatPublisher double recording what was queued for
// publication.
type fakeEventPublisher struct {
	mu        sync.Mutex
	published []string
}

// Handle records the heartbeat's event id.
func (f *fakeEventPublisher) Handle(_ context.Context, hb *agentv1.Heartbeat, _ devices.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, hb.GetEventId())
}

// events returns the recorded event ids.
func (f *fakeEventPublisher) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.published...)
}

// TestHeartbeatSinkFansOutToIngestThenPublisher pins the sink the agent hub is wired with: the
// ordered fan-out, so an accepted heartbeat is stored before it is published and a refused one
// is never published at all.
func TestHeartbeatSinkFansOutToIngestThenPublisher(t *testing.T) {
	t.Parallel()

	rec := devices.Record{ID: "dev-1", Region: "eu-west", Model: "v3"}
	hb := &agentv1.Heartbeat{EventId: "ev-1", DeviceId: rec.ID, Ts: timestamppb.Now()}

	ingest := &fakeIngestSink{}
	publisher := &fakeEventPublisher{}
	sink := heartbeatSink(ingest, publisher)
	if _, ok := sink.(*telemetry.Fanout); !ok {
		t.Fatalf("heartbeatSink() returned %T, want the ordered *telemetry.Fanout", sink)
	}
	if err := sink.Handle(context.Background(), hb, rec); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if got := ingest.events(); len(got) != 1 || got[0] != "ev-1" {
		t.Errorf("stored events = %v, want the accepted heartbeat", got)
	}
	if got := publisher.events(); len(got) != 1 || got[0] != "ev-1" {
		t.Errorf("published events = %v, want the accepted heartbeat", got)
	}

	refusal := errors.New("ingest heartbeat: measurement time required")
	ingest.err = refusal
	if err := sink.Handle(context.Background(), &agentv1.Heartbeat{EventId: "ev-2", DeviceId: rec.ID}, rec); !errors.Is(err, refusal) {
		t.Errorf("Handle() error = %v, want the ingest refusal", err)
	}
	if got := publisher.events(); len(got) != 1 {
		t.Errorf("published events = %v, want no publication for a refused heartbeat", got)
	}
}

// TestMetricsHandlerExposesTheEventPipeline pins that the pipeline's collectors are registered
// on the registry the control plane serves, before the endpoint starts serving: a scrape finds
// every family this change adds.
func TestMetricsHandlerExposesTheEventPipeline(t *testing.T) {
	t.Parallel()

	reg, _ := newMetrics()
	pipeline := telemetry.NewMetrics(reg)
	pipeline.QueueDepth.WithLabelValues(telemetry.HeartbeatQueue, string(telemetry.QueueKindWork)).Set(0)
	pipeline.Published.WithLabelValues(telemetry.HeartbeatEventType).Add(0)
	pipeline.Dropped.WithLabelValues(telemetry.HeartbeatEventType, "buffer_full").Add(0)
	pipeline.Consumed.WithLabelValues(telemetry.HeartbeatQueue, "processed").Add(0)
	pipeline.RegisterLag(telemetry.HeartbeatQueue, func() float64 { return 0 })

	body := scrape(t, metricsHandler(reg))
	want := []string{
		"fleetops_queue_depth",
		"fleetops_consumer_lag_events",
		"fleetops_events_published_total",
		"fleetops_events_dropped_total",
		"fleetops_events_consumed_total",
		"fleetops_device_offline_transitions_total",
	}
	for _, name := range want {
		if !strings.Contains(body, name) {
			t.Errorf("scrape output is missing %s:\n%s", name, body)
		}
	}
}

// TestRolloutSettings pins the configuration mapping the control plane's start path uses: the
// configured sequence and gate timing become the policy a rollout drives under, and a policy no
// rollout could sequence is refused here rather than inside a running rollout.
func TestRolloutSettings(t *testing.T) {
	t.Parallel()

	t.Run("the configured sequence and timing reach the policy", func(t *testing.T) {
		t.Parallel()

		cfg := config.Defaults()
		cfg.Rollout.HealthWindow = config.Duration{Duration: time.Minute}
		cfg.Rollout.DecisionTimeout = config.Duration{Duration: 5 * time.Minute}
		cfg.Rollout.ResultTimeout = config.Duration{Duration: 2 * time.Minute}
		cfg.Rollout.Waves = []config.Wave{
			{Percent: 10},
			{Percent: 100, RequireApproval: true},
		}

		got, err := rolloutSettings(cfg.Rollout)
		if err != nil {
			t.Fatalf("rolloutSettings: %v", err)
		}
		if got.HealthWindow != time.Minute || got.DecisionTimeout != 5*time.Minute ||
			got.ResultTimeout != 2*time.Minute {
			t.Errorf("windows = %v/%v/%v, want the configured 1m/5m/2m",
				got.HealthWindow, got.DecisionTimeout, got.ResultTimeout)
		}
		want := []temporal.RolloutWave{
			{Percent: 10},
			{Percent: 100, RequireApproval: true},
		}
		if diff := cmp.Diff(want, got.Waves); diff != "" {
			t.Errorf("waves mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a policy no rollout could sequence is refused", func(t *testing.T) {
		t.Parallel()

		cfg := config.Defaults()
		// A result wait longer than the wave's own decision timeout: the wave's verdict would
		// already be due while it still waited on devices.
		cfg.Rollout.ResultTimeout = config.Duration{Duration: cfg.Rollout.DecisionTimeout.Duration + time.Minute}

		if _, err := rolloutSettings(cfg.Rollout); err == nil {
			t.Fatal("rolloutSettings = nil error, want the policy refused")
		}
	})

	t.Run("the accepted start carries the settings to the workflow", func(t *testing.T) {
		t.Parallel()

		cfg := config.Defaults()
		cfg.Rollout.Waves = []config.Wave{{Percent: 100}}
		settings, err := rolloutSettings(cfg.Rollout)
		if err != nil {
			t.Fatalf("rolloutSettings: %v", err)
		}
		fc := &recordingStartClient{}
		req := temporal.RolloutRequest{
			RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
		}
		if err := temporal.NewRolloutStarter(fc, cfg.Temporal.TaskQueue, settings).
			Start(context.Background(), req); err != nil {
			t.Fatalf("start rollout: %v", err)
		}

		if len(fc.inputs) != 1 {
			t.Fatalf("started rollouts = %d, want 1", len(fc.inputs))
		}
		got := fc.inputs[0]
		if got.RolloutRequest != req {
			t.Errorf("start input request = %+v, want %+v", got.RolloutRequest, req)
		}
		if diff := cmp.Diff(settings, got.Settings); diff != "" {
			t.Errorf("start input settings mismatch (-want +got):\n%s", diff)
		}
	})
}

// fakeWorker records the activities registered on it. The embedded worker.Worker supplies the
// rest of the interface — it is nil, and no test calls through it.
type fakeWorker struct {
	worker.Worker
	activities map[string]any
}

func (w *fakeWorker) RegisterActivityWithOptions(a any, options activity.RegisterOptions) {
	w.activities[options.Name] = a
}

// fakeDispatcher is a hand-written temporal.CommandDispatcher double recording the commands the
// registered dispatch activity delivered.
type fakeDispatcher struct {
	mu       sync.Mutex
	commands []*agentv1.Command
}

// Send records one dispatched command.
func (d *fakeDispatcher) Send(_ context.Context, cmd *agentv1.Command) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.commands = append(d.commands, cmd)
	return nil
}

// sent returns the recorded commands.
func (d *fakeDispatcher) sent() []*agentv1.Command {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*agentv1.Command(nil), d.commands...)
}

// workQueueTaskTypes are the workflow and activity names the workers register on the work queue.
// The control plane must never register one of them: it does not poll the queue they are
// delivered to, and a process that registered them without polling for them would be pretending
// to a role it does not have.
func workQueueTaskTypes() []string {
	return []string{
		temporal.DeviceWorkflowName,
		temporal.SnapshotActivityName,
		temporal.RolloutWorkflowName,
		temporal.LoadFirmwareActivityName,
		temporal.ResolveWaveTargetsActivityName,
		temporal.RecordRolloutStateActivityName,
		temporal.RecordWaveStateActivityName,
		temporal.UpdateDeviceActivityName,
		temporal.DowngradeDeviceActivityName,
		temporal.ReconcileInventoryActivityName,
		temporal.AnnounceRollbackActivityName,
		temporal.EvaluateWaveHealthActivityName,
	}
}

// TestStartWorker pins the control plane's Temporal topology: the queue it polls and what it
// registers there. The polled queue is captured from the worker factory, so what is asserted is
// the queue string this process hands to worker.New, and the registration set is read back off the
// registry seam. Together they are the invariant the queue separation exists for: no poller of a
// queue may lack a task type that queue can deliver.
func TestStartWorker(t *testing.T) {
	t.Parallel()

	// Two distinct names, so the assertion says which configuration field the process reads
	// rather than agreeing with whichever one it happens to pick.
	cfg := config.Defaults()
	cfg.Temporal.TaskQueue = "work"
	cfg.Temporal.DispatchTaskQueue = "dispatch"

	hub := &fakeDispatcher{}
	built := &fakeWorker{activities: map[string]any{}}
	var polled string
	build := func(_ temporalclient.Client, queue string, options worker.Options) worker.Worker {
		polled = queue
		if options.WorkerStopTimeout <= 0 {
			t.Errorf("WorkerStopTimeout = %v, want a bounded stop so shutdown drains",
				options.WorkerStopTimeout)
		}
		return built
	}

	startWorker(build, nil, cfg, hub)

	if polled != cfg.Temporal.DispatchTaskQueue {
		t.Errorf("polled queue = %q, want the configured control-plane queue %q",
			polled, cfg.Temporal.DispatchTaskQueue)
	}
	if polled == cfg.Temporal.TaskQueue {
		t.Errorf("polled queue = work queue %q, want the control plane off it", polled)
	}

	// The registration set is exactly one activity: dispatch-command, under its explicit name
	// rather than a function-reflection name.
	if _, ok := built.activities[temporal.DispatchActivityName]; !ok {
		t.Errorf("activity %q is not registered", temporal.DispatchActivityName)
	}
	if len(built.activities) != 1 {
		t.Errorf("registered activities = %d, want only %q",
			len(built.activities), temporal.DispatchActivityName)
	}
	for _, name := range workQueueTaskTypes() {
		if _, ok := built.activities[name]; ok {
			t.Errorf("work-queue type %q is registered in the control plane, want it left to the workers",
				name)
		}
	}

	// The registered activity is bound to the hub it was registered with: a device command
	// reaches the agent connection this process holds.
	dispatch, ok := built.activities[temporal.DispatchActivityName].(func(context.Context, temporal.CommandIssuedSignal) error)
	if !ok {
		t.Fatalf("registered activity has type %T, want the dispatch activity",
			built.activities[temporal.DispatchActivityName])
	}
	if err := dispatch(context.Background(), temporal.CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: temporal.CommandKindUpdate,
		FirmwareID: "fw-2", Version: "2.0.0", Checksum: "sha256:0f1e2d",
	}); err != nil {
		t.Fatalf("run registered dispatch activity: %v", err)
	}
	sent := hub.sent()
	if len(sent) != 1 || sent[0].GetCommandId() != "cmd-1" || sent[0].GetDeviceId() != "dev-1" {
		t.Errorf("dispatched commands = %+v, want cmd-1 delivered to dev-1", sent)
	}
}

// recordingStartClient is a hand-written startClient double recording the inputs it was handed.
type recordingStartClient struct {
	inputs []temporal.RolloutInput
}

func (c *recordingStartClient) ExecuteWorkflow(
	_ context.Context, _ temporalclient.StartWorkflowOptions, _ any, args ...any,
) (temporalclient.WorkflowRun, error) {
	var input temporal.RolloutInput
	if len(args) > 0 {
		input, _ = args[0].(temporal.RolloutInput)
	}
	c.inputs = append(c.inputs, input)
	return nil, nil
}

// TestGatewayHandler pins the composition of the gateway listener: the firmware and rollout APIs
// answer on one mux under their own prefixes, neither shadows the other, and an unregistered path
// is the mux's own 404.
func TestGatewayHandler(t *testing.T) {
	t.Parallel()

	firmwareMux := http.NewServeMux()
	firmwareMux.HandleFunc("POST /api/firmwares", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte("firmware")); err != nil {
			t.Errorf("write firmware response: %v", err)
		}
	})
	rolloutMux := http.NewServeMux()
	rolloutMux.HandleFunc("POST /api/rollouts", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		if _, err := w.Write([]byte("rollout")); err != nil {
			t.Errorf("write rollout response: %v", err)
		}
	})
	rolloutMux.HandleFunc("GET /api/rollouts/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("state")); err != nil {
			t.Errorf("write state response: %v", err)
		}
	})

	gateway := gatewayHandler(firmwareMux, rolloutMux)
	tests := []struct {
		name         string
		method, path string
		wantCode     int
		wantBody     string
	}{
		{name: "a firmware upload reaches its own API", method: http.MethodPost,
			path: "/api/firmwares", wantCode: http.StatusCreated, wantBody: "firmware"},
		{name: "a rollout start reaches its own API", method: http.MethodPost,
			path: "/api/rollouts", wantCode: http.StatusAccepted, wantBody: "rollout"},
		{name: "a rollout read reaches its own API", method: http.MethodGet,
			path: "/api/rollouts/ro-1", wantCode: http.StatusOK, wantBody: "state"},
		{name: "an unregistered path is not found", method: http.MethodGet,
			path: "/api/devices", wantCode: http.StatusNotFound},
		{name: "a method mismatch is rejected", method: http.MethodGet,
			path: "/api/rollouts", wantCode: http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			gateway.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.wantCode {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.wantCode, rec.Body)
			}
			if tc.wantBody != "" && !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("body = %q, want it to contain %q", rec.Body, tc.wantBody)
			}
		})
	}
}

// TestNewRolloutAPI pins that the surface the control plane serves is built over the Temporal
// client it was handed: every route answers, and the backend refusing a read is the surface's own
// failure rather than a panic or a hang.
func TestNewRolloutAPI(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	handler, err := newRolloutAPI(cfg, &unreachableTemporalClient{}, testLogger())
	if err != nil {
		t.Fatalf("newRolloutAPI: %v", err)
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/rollouts/ro-1"},
		{http.MethodPost, "/api/rollouts/ro-1/pause"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			// The adapter's query fails: the surface answers a bounded, operator-safe failure
			// rather than echoing anything about the backend.
			if rec.Code != http.StatusInternalServerError {
				t.Errorf("status = %d, want %d (body %s)",
					rec.Code, http.StatusInternalServerError, rec.Body)
			}
		})
	}
}

// unreachableTemporalClient is a Temporal client double implementing exactly the three calls the
// rollout surface makes, each of them failing: it stands in for a frontend that is down.
type unreachableTemporalClient struct{}

func (c *unreachableTemporalClient) ExecuteWorkflow(
	context.Context, temporalclient.StartWorkflowOptions, any, ...any,
) (temporalclient.WorkflowRun, error) {
	return nil, errors.New("temporal frontend is unavailable")
}

func (c *unreachableTemporalClient) QueryWorkflow(
	context.Context, string, string, string, ...any,
) (converter.EncodedValue, error) {
	return nil, errors.New("temporal frontend is unavailable")
}

func (c *unreachableTemporalClient) SignalWorkflow(
	context.Context, string, string, string, any,
) error {
	return errors.New("temporal frontend is unavailable")
}

// testLogger discards its output: the API logs every rejection at the boundary, and that noise
// would drown the test output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
