package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// rolloutFakes stands in for every side effect the rollout's activities touch: the firmware
// registry, the fleet database's rollout and wave documents, the device command seam, and the
// wave-health evaluation. The real activity implementations are bound to it, so a workflow test
// exercises the whole path — activity mapping included — over a fake world.
//
// The fake mirrors the store's semantics that the workflow depends on: resolution is recorded once
// and returned as the answer afterwards, a wave must be resolved before its state can move, and a
// concluded rollout's status stands.
type rolloutFakes struct {
	mu sync.Mutex

	// settings is the policy the scripted evaluator windows against.
	settings RolloutSettings
	// firmware is the metadata registry, keyed by firmware id.
	firmware map[string]firmware.Record
	// pool is the eligible pool, in the order the store returns it.
	pool []string
	// rollouts and waves are the recorded documents, keyed by id.
	rollouts map[string]rollout.RolloutRecord
	waves    map[string]rollout.WaveRecord
	// device is the scriptable device world the update activities command devices through and
	// read their results from.
	device *deviceWorld
	// commands are the update commands the seam accepted, in delivery order.
	commands []CommandIssuedSignal
	// evaluations are the health evaluations the gate asked for, in order.
	evaluations []EvaluateWaveRequest
	// answers are the health measurements the evaluator returned, in order.
	answers []WaveHealth
	// health scripts the verdicts the evaluator returns, one per evaluation; the last one
	// repeats once the script is exhausted.
	health []WaveHealth
	// events is the ordered log of what the workflow did, for progression assertions.
	events []string
	// onWaveState, when set, runs after one wave write with the update it recorded. It is how a
	// test observes the rollout at a moment it chose: the state query evaluated inside the hook
	// sees exactly the state the rollout had reached then.
	onWaveState func(rollout.WaveStateUpdate)
	// onControlRecord, when set, runs synchronously after the rollout record of a control-signal
	// transition with the status it wrote. It is how a test observes the rollout at the exact
	// moment a signal was folded, rather than at a wall-clock instant the test guessed at.
	onControlRecord func(status rollout.RolloutStatus)

	// Failures the test scripts.
	firmwareErr   error
	resolveErr    error
	recordErr     error
	waveRecordErr error
	dispatchErr   error
	dispatchFails map[string]int
	healthFails   int
	// recordFails fails that many recording calls before they succeed, which is how a retried
	// write is simulated.
	recordFails int
	// resolveLag starts a wave's recorded window that far in the past, which is what a wave
	// resolved before a worker outage looks like once the rollout resumes.
	resolveLag time.Duration
}

// newRolloutFakes returns the fake world a rollout test runs under: one firmware in the registry,
// the given eligible pool, and the policy the evaluator clips its windows against. The devices in
// the pool start out concluding their updates successfully, which is the shape of a healthy wave;
// a test that needs another behaviour scripts it on the device world.
func newRolloutFakes(settings RolloutSettings, rec firmware.Record, pool ...string) *rolloutFakes {
	fakes := &rolloutFakes{
		settings:      settings,
		firmware:      map[string]firmware.Record{rec.ID: rec},
		pool:          pool,
		rollouts:      map[string]rollout.RolloutRecord{},
		waves:         map[string]rollout.WaveRecord{},
		device:        newDeviceWorld(),
		dispatchFails: map[string]int{},
	}
	fakes.device.succeed(pool...)
	return fakes
}

// devices returns the scriptable device world the update activities run against.
func (f *rolloutFakes) devices() *deviceWorld {
	return f.device
}

// scriptHealth queues the verdicts the evaluator returns, one per evaluation.
func (f *rolloutFakes) scriptHealth(health ...WaveHealth) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.health = append(f.health, health...)
}

// rolloutTestSettings is the demo canary the workflow tests drive: 1%, 5%, 25%, and 100% over a
// five-minute window with a thirty-minute decision timeout, none of them gated. Approval is a
// property of a sequence entry, so the gate tests script their own sequences.
func rolloutTestSettings() RolloutSettings {
	return RolloutSettings{
		HealthWindow:    5 * time.Minute,
		DecisionTimeout: 30 * time.Minute,
		ResultTimeout:   30 * time.Second,
		Waves: []RolloutWave{
			{Percent: 1},
			{Percent: 5},
			{Percent: 25},
			{Percent: 100},
		},
	}
}

// rolloutResultTimeout is the result timeout the workflow tests drive. It is short so a device
// that never reports is provable without the suite waiting out the production default, and longer
// than a few poll intervals so a device that reports late is still found.
const rolloutResultTimeout = 30 * time.Second

// rolloutSettingsWith returns the test policy with its sequence, window, and decision timeout
// replaced, for the gate tests that script their own timing.
func rolloutSettingsWith(window, timeout time.Duration, waves ...RolloutWave) RolloutSettings {
	return RolloutSettings{
		HealthWindow:    window,
		DecisionTimeout: timeout,
		ResultTimeout:   rolloutResultTimeout,
		Waves:           waves,
	}
}

// rolloutTestFirmware is the firmware the fake registry holds: version 2.0.0, targeting the
// oak-s3 model the test rollouts select.
func rolloutTestFirmware() firmware.Record {
	return firmware.Record{
		ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3"}, Checksum: "sha256:0f1e2d",
	}
}

// rolloutInputFor returns the start input of rollout ro-1 deploying fw-1 to the oak-s3 fleet in
// eu-west, driving settings.
func rolloutInputFor(settings RolloutSettings) RolloutInput {
	return RolloutInput{
		RolloutRequest: RolloutRequest{
			RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
		},
		Settings: settings,
	}
}

// deviceIDs returns n device ids in the order the store's pool query returns them.
func deviceIDs(n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, fmt.Sprintf("dev-%04d", i))
	}
	return ids
}

// healthyAt is a decided healthy verdict carrying the ratio it was decided on.
func healthyAt(ratio float64) WaveHealth {
	return WaveHealth{Verdict: wavehealth.VerdictHealthy, SuccessRatio: ratio, SampleSize: 100}
}

// unhealthyAt is a decided unhealthy verdict carrying the ratio it was decided on.
func unhealthyAt(ratio float64) WaveHealth {
	return WaveHealth{Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: ratio, SampleSize: 100}
}

// undecided is a verdict with too little evidence to decide on.
func undecided() WaveHealth {
	return WaveHealth{Verdict: wavehealth.VerdictUndecided, SampleSize: 2}
}

// Metadata implements FirmwareSource.
func (f *rolloutFakes) Metadata(_ context.Context, id string) (firmware.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.firmwareErr != nil {
		return firmware.Record{}, f.firmwareErr
	}
	rec, ok := f.firmware[id]
	if !ok {
		return firmware.Record{}, fmt.Errorf("firmware %s: %w", id, firmware.ErrNotFound)
	}
	return rec, nil
}

// ResolveWave implements TargetResolver: it records one wave document per wave id, resolving its
// membership the way the store does — the share's slice of the pool minus what earlier waves
// already target — and returns the recorded document on every later call.
func (f *rolloutFakes) ResolveWave(_ context.Context, req rollout.ResolveRequest) (rollout.WaveRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "resolve "+req.WaveID)
	if f.resolveErr != nil {
		return rollout.WaveRecord{}, f.resolveErr
	}
	if recorded, ok := f.waves[req.WaveID]; ok {
		return recorded, nil
	}
	var targeted []string
	for _, wave := range f.waves {
		targeted = append(targeted, wave.DeviceIDs...)
	}
	rec := rollout.WaveRecord{
		ID:        req.WaveID,
		RolloutID: req.RolloutID,
		Percent:   req.Percent,
		Status:    rollout.WaveDispatching,
		DeviceIDs: rollout.WaveTargets(f.pool, targeted, req.Percent),
		StartedAt: req.StartedAt.Add(-f.resolveLag),
		// The store writes both sets empty on insert, so a wave that has just been resolved
		// reports "no failures" rather than "not collected yet".
		FailedDeviceIDs:     []string{},
		UnreportedDeviceIDs: []string{},
	}
	f.waves[req.WaveID] = rec
	return rec, nil
}

// RecordRollout implements RolloutRecorder.
func (f *rolloutFakes) RecordRollout(_ context.Context, rec rollout.RolloutRecord) error {
	f.mu.Lock()
	f.events = append(f.events, "rollout "+string(rec.Status))
	if err := f.recordFailure(); err != nil {
		f.mu.Unlock()
		return err
	}
	if existing, ok := f.rollouts[rec.ID]; ok && existing.Status.Terminal() {
		// A terminal status is never left.
		f.mu.Unlock()
		return nil
	}
	f.rollouts[rec.ID] = rec
	hook := f.onControlRecord
	// The hook runs without the lock: it evaluates the workflow's state query, which reaches
	// back into this fake world.
	f.mu.Unlock()
	if hook != nil && rec.Status != rollout.RolloutRunning {
		hook(rec.Status)
	}
	return nil
}

// observeControlRecord registers a hook that runs after every non-running rollout write, which is
// what a control signal's transition writes.
func (f *rolloutFakes) observeControlRecord(hook func(rollout.RolloutStatus)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onControlRecord = hook
}

// RecordWaveState implements WaveRecorder.
func (f *rolloutFakes) RecordWaveState(_ context.Context, update rollout.WaveStateUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "wave "+update.WaveID+" "+string(update.Status))
	if err := f.recordFailure(); err != nil {
		return err
	}
	if f.waveRecordErr != nil {
		return f.waveRecordErr
	}
	rec, ok := f.waves[update.WaveID]
	if !ok {
		// A wave is resolved before it can move, so this is a workflow bug rather than a
		// transient failure.
		return fmt.Errorf("wave %s: %w", update.WaveID, rollout.ErrNotFound)
	}
	rec.Status = update.Status
	rec.SuccessRate = update.SuccessRate
	// Every wave write carries the outcome sets, exactly as the store writes them: a wave whose
	// devices all succeeded stores empty arrays rather than leaving the fields behind.
	rec.FailedDeviceIDs = update.FailedDeviceIDs
	rec.UnreportedDeviceIDs = update.UnreportedDeviceIDs
	f.waves[update.WaveID] = rec
	hook := f.onWaveState
	// The hook runs without the lock: it evaluates the workflow's state query, which reaches
	// back into this fake world.
	f.mu.Unlock()
	if hook != nil {
		hook(update)
	}
	f.mu.Lock()
	return nil
}

// observeWaveState registers a hook that runs after every wave write.
func (f *rolloutFakes) observeWaveState(hook func(rollout.WaveStateUpdate)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onWaveState = hook
}

// SignalCommandIssued implements DeviceCommander. A device with scripted failures left rejects
// its command that many times, which is how a partly delivered wave is simulated; otherwise the
// command reaches the scriptable device world, where the device's scripted conclusion lives.
func (f *rolloutFakes) SignalCommandIssued(ctx context.Context, cmd CommandIssuedSignal) error {
	f.mu.Lock()
	f.commands = append(f.commands, cmd)
	f.events = append(f.events, "command "+cmd.DeviceID)
	switch {
	case f.dispatchFails[cmd.DeviceID] > 0:
		f.dispatchFails[cmd.DeviceID]--
		f.mu.Unlock()
		return errors.New("device workflow is unreachable")
	case f.dispatchErr != nil:
		err := f.dispatchErr
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	return f.device.SignalCommandIssued(ctx, cmd)
}

// Evaluate implements HealthEvaluator: it answers with the next scripted verdict, clipped to the
// wave's recorded start the way the aggregator clips it, and it can fail a scripted number of
// calls before answering.
func (f *rolloutFakes) Evaluate(_ context.Context, query wavehealth.Query, at time.Time) (wavehealth.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evaluations = append(f.evaluations, EvaluateWaveRequest{
		RolloutID: query.RolloutID, WaveID: query.WaveID, At: at,
	})
	f.events = append(f.events, "evaluate "+query.WaveID)
	if f.healthFails > 0 {
		f.healthFails--
		return wavehealth.Result{}, errors.New("telemetry is unavailable")
	}
	if len(f.health) == 0 {
		return wavehealth.Result{}, errors.New("no scripted verdict")
	}
	scripted := f.health[min(len(f.evaluations), len(f.health))-1]

	start := at.Add(-f.settings.HealthWindow)
	if recorded, ok := f.waves[query.WaveID]; ok && recorded.StartedAt.After(start) {
		start = recorded.StartedAt
	}
	f.answers = append(f.answers, WaveHealth{
		Verdict:      scripted.Verdict,
		SuccessRatio: scripted.SuccessRatio,
		SampleSize:   scripted.SampleSize,
		WindowStart:  start,
		WindowEnd:    at,
	})
	return wavehealth.Result{
		RolloutID:    query.RolloutID,
		WaveID:       query.WaveID,
		SuccessRatio: scripted.SuccessRatio,
		SampleSize:   scripted.SampleSize,
		Verdict:      scripted.Verdict,
		WindowStart:  start,
		WindowEnd:    at,
		Settings: wavehealth.Settings{
			HealthWindow:    f.settings.HealthWindow,
			MinSamples:      1,
			MinSuccessRatio: 0.95,
		},
	}, nil
}

// recordFailure returns the failure a recording call should report: the sticky error when one is
// scripted, and the scripted number of transient failures before writes succeed.
func (f *rolloutFakes) recordFailure() error {
	if f.recordFails > 0 {
		f.recordFails--
		return errors.New("mongo is unavailable")
	}
	return f.recordErr
}

// recordedRollout returns the rollout document the workflow wrote, if it wrote one.
func (f *rolloutFakes) recordedRollout(id string) (rollout.RolloutRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.rollouts[id]
	return rec, ok
}

// recordedWaveEvents returns the wave-write events the workflow logged, in order, which is how a
// test counts the wave progress that mirrors no rollout attribute.
func (f *rolloutFakes) recordedWaveEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var events []string
	for _, event := range f.events {
		if strings.HasPrefix(event, "wave ") {
			events = append(events, event)
		}
	}
	return events
}

// rolloutStatuses returns every status the workflow recorded for the rollout, in order, which is
// how a test asserts that a control signal did or did not move the recorded picture.
func (f *rolloutFakes) rolloutStatuses() []rollout.RolloutStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	var statuses []rollout.RolloutStatus
	for _, event := range f.events {
		if after, ok := strings.CutPrefix(event, "rollout "); ok {
			statuses = append(statuses, rollout.RolloutStatus(after))
		}
	}
	return statuses
}

// recordedWave returns a wave document the workflow wrote, if it wrote one.
func (f *rolloutFakes) recordedWave(id string) (rollout.WaveRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.waves[id]
	return rec, ok
}

// recordedCommands returns the commands the seam accepted, ordered by command id. A wave's
// per-device updates run concurrently, so the order they arrive in is not deterministic; command
// ids are derived from the wave and the device, so ordering by them groups one wave's commands
// together in target order and makes an assertion about what was delivered comparable.
func (f *rolloutFakes) recordedCommands() []CommandIssuedSignal {
	f.mu.Lock()
	defer f.mu.Unlock()
	commands := append([]CommandIssuedSignal(nil), f.commands...)
	slices.SortFunc(commands, func(a, b CommandIssuedSignal) int {
		return strings.Compare(a.CommandID, b.CommandID)
	})
	return commands
}

// recordedEvaluations returns the health evaluations the gate asked for, in order.
func (f *rolloutFakes) recordedEvaluations() []EvaluateWaveRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]EvaluateWaveRequest(nil), f.evaluations...)
}

// recordedHealth returns the measurements the evaluator answered, in order.
func (f *rolloutFakes) recordedHealth() []WaveHealth {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]WaveHealth(nil), f.answers...)
}

// recordedEvents returns the ordered log of what the workflow did.
func (f *rolloutFakes) recordedEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

// rolloutTestTimeout bounds one workflow test run. It is generous on purpose: the update activity
// waits in wall-clock time even under the test environment, so a test that proves a device never
// reports spends real time in that wait, and the bound exists only to fail a hung test rather than
// to time one out mid-run.
const rolloutTestTimeout = 2 * time.Minute

// newRolloutEnv returns a test environment hosting RolloutWorkflow under its registered name with
// its six activities implemented by the real constructors over fakes, the per-device update among
// them driving the scriptable device world. The update activity's poll interval is shrunk so a
// device that reports late is found without the suite spending the production interval between
// observations.
func newRolloutEnv(fakes *rolloutFakes) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(discardLogger{})
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(rolloutTestTimeout)
	env.RegisterWorkflowWithOptions(RolloutWorkflow, workflow.RegisterOptions{Name: RolloutWorkflowName})
	env.RegisterActivityWithOptions(NewLoadFirmwareActivity(fakes),
		activity.RegisterOptions{Name: LoadFirmwareActivityName})
	env.RegisterActivityWithOptions(NewResolveWaveActivity(fakes),
		activity.RegisterOptions{Name: ResolveWaveTargetsActivityName})
	env.RegisterActivityWithOptions(NewRecordRolloutActivity(fakes),
		activity.RegisterOptions{Name: RecordRolloutStateActivityName})
	env.RegisterActivityWithOptions(NewRecordWaveActivity(fakes),
		activity.RegisterOptions{Name: RecordWaveStateActivityName})
	env.RegisterActivityWithOptions(
		NewUpdateDeviceActivity(fakes, fakes.devices(), WithUpdatePollInterval(10*time.Millisecond)),
		activity.RegisterOptions{Name: UpdateDeviceActivityName})
	env.RegisterActivityWithOptions(NewEvaluateWaveActivity(fakes),
		activity.RegisterOptions{Name: EvaluateWaveHealthActivityName})
	return env
}

// runRollout executes one rollout and returns the state its query reports when it concluded,
// failing the test when the workflow itself errored.
func runRollout(t *testing.T, env *testsuite.TestWorkflowEnvironment, in RolloutInput) RolloutView {
	t.Helper()
	env.ExecuteWorkflow(RolloutWorkflowName, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("rollout workflow error: %v", err)
	}
	return queryRollout(t, env)
}

// queryRollout evaluates the rollout's state query and returns its answer.
func queryRollout(t *testing.T, env *testsuite.TestWorkflowEnvironment) RolloutView {
	t.Helper()
	value, err := env.QueryWorkflow(GetRolloutStateQueryType)
	if err != nil {
		t.Fatalf("query %s: %v", GetRolloutStateQueryType, err)
	}
	var view RolloutView
	if err := value.Get(&view); err != nil {
		t.Fatalf("decode %s answer: %v", GetRolloutStateQueryType, err)
	}
	return view
}

// scheduledQuery is the answer one state query received while the workflow was still running.
type scheduledQuery struct {
	mu   sync.Mutex
	view RolloutView
	err  error
	done bool
}

// record stores the answer.
func (q *scheduledQuery) record(view RolloutView, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.view, q.err, q.done = view, err, true
}

// result returns the recorded answer, failing the test if the query never ran.
func (q *scheduledQuery) result(t *testing.T) RolloutView {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.done {
		t.Fatal("the scheduled state query never ran")
	}
	if q.err != nil {
		t.Fatalf("state query error: %v", q.err)
	}
	return q.view
}

// scheduleQuery asks the state query for its answer at the given workflow time, while the
// workflow is still running.
func scheduleQuery(env *testsuite.TestWorkflowEnvironment, at time.Duration) *scheduledQuery {
	query := &scheduledQuery{}
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(GetRolloutStateQueryType)
		if err != nil {
			query.record(RolloutView{}, err)
			return
		}
		var view RolloutView
		if err := value.Get(&view); err != nil {
			query.record(RolloutView{}, err)
			return
		}
		query.record(view, nil)
	}, at)
	return query
}
