//go:build integration

package rolloutapi

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/temporaltest"
	"github.com/DisMosGit/fleetops/internal/testexec"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// visibilityHealthWindow is the canary policy's health window in the visibility test: long enough
// that every run is still inside its first wave's window when the list queries execute, so every
// run's RolloutStatus attribute is "running" rather than a status a race decided.
const visibilityHealthWindow = 5 * time.Minute

// visibilityQueue is the task queue the test's worker polls.
const visibilityQueue = "rollout-visibility"

// visibilityPoll is how long the test waits between observations. The visibility store indexes a
// started run asynchronously, so every observation is a poll.
const visibilityPoll = 250 * time.Millisecond

// visibilityRun is one rollout the visibility test starts: the selector it targets and the
// firmware it deploys.
type visibilityRun struct {
	rolloutID  string
	firmwareID string
	version    string
	region     string
	model      string
}

// visibilityActivities implements the rollout activities the visibility runs call. The firmware
// metadata, the wave membership, and the per-device result carry the facts the scenario's
// conditions are about; the remaining activities stand in for side effects the scenario does not
// assert. A wave's update succeeds, so the run then waits out its health window — which is where
// every run stays while the queries execute.
type visibilityActivities struct {
	firmware map[string]temporal.Firmware
	devices  []string
}

// loadFirmware answers the firmware metadata of the run's firmware id.
func (a *visibilityActivities) loadFirmware(
	_ context.Context,
	req temporal.LoadFirmware,
) (temporal.Firmware, error) {
	fw, ok := a.firmware[req.FirmwareID]
	if !ok {
		return temporal.Firmware{}, fmt.Errorf("firmware %s is not held", req.FirmwareID)
	}
	return fw, nil
}

// resolveWave answers the wave with a membership of one device.
func (a *visibilityActivities) resolveWave(
	_ context.Context,
	req temporal.ResolveWaveRequest,
) (temporal.ResolvedWave, error) {
	return temporal.ResolvedWave{
		WaveID: req.WaveID, DeviceIDs: slices.Clone(a.devices), StartedAt: req.StartedAt,
	}, nil
}

// recordRollout stands in for the rollout document write.
func (a *visibilityActivities) recordRollout(context.Context, temporal.RecordRolloutRequest) error {
	return nil
}

// recordWave stands in for the wave document write.
func (a *visibilityActivities) recordWave(context.Context, temporal.RecordWaveRequest) error {
	return nil
}

// updateDevice answers the one target device's update as succeeded.
func (a *visibilityActivities) updateDevice(
	_ context.Context,
	req temporal.UpdateDeviceRequest,
) (temporal.DeviceUpdate, error) {
	return temporal.DeviceUpdate{DeviceID: req.DeviceID, Outcome: temporal.UpdateSucceeded}, nil
}

// evaluateWave answers a healthy measurement. The runs are still inside their health window while
// the test runs, so this is registered for a run that outlives it rather than called by one.
func (a *visibilityActivities) evaluateWave(
	context.Context,
	temporal.EvaluateWaveRequest,
) (temporal.WaveHealth, error) {
	return temporal.WaveHealth{Verdict: wavehealth.VerdictHealthy, SuccessRatio: 1}, nil
}

// startVisibilityWorker runs one worker until the test ends.
func startVisibilityWorker(t *testing.T, w worker.Worker) {
	t.Helper()

	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			t.Logf("visibility worker stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Log("the visibility worker did not stop within 30s")
		}
	})
}

// waitForRunningRollout waits until one started run reports the state its query's conditions are
// about: driving its wave, with the firmware version it deploys known. The state comes from the
// run's own query, so its firmware version is the one load-firmware answered.
func waitForRunningRollout(
	t *testing.T,
	ctx context.Context,
	states *temporal.RolloutStates,
	run visibilityRun,
	grace time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(grace)
	for {
		view, err := states.State(ctx, run.rolloutID)
		if err != nil {
			t.Fatalf("read rollout %s state: %v", run.rolloutID, err)
		}
		if view.Status == rollout.RolloutRunning && view.FirmwareVersion == run.version {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rollout %s reports %s/%q after %s, want %s/%s",
				run.rolloutID, view.Status, view.FirmwareVersion, grace,
				rollout.RolloutRunning, run.version)
		}
		time.Sleep(visibilityPoll)
	}
}

// searchAttributePropagating reports whether err is the frontend's transient refusal of a query
// naming a search attribute the namespace registry has not picked up yet. Registering an attribute
// writes the namespace's alias map, while the query validator reads a cached copy of it that is
// refreshed on system.namespaceCacheRefreshInterval — ten seconds by default — so a query issued
// immediately after registration is refused for up to that interval. It is a propagation delay
// rather than a failure, so the visibility waiters retry it; every other error still fails.
func searchAttributePropagating(err error) bool {
	return err != nil && strings.Contains(err.Error(), "is not a valid search attribute")
}

// visibleRolloutIDs asks the namespace's visibility store one list query — the same call the
// Temporal UI's list is built on — and returns the workflow ids it answers with, sorted so a
// comparison is deterministic. A query refused because its attribute is still propagating is
// returned as pending rather than as a failure, and the waiter decides when patience has run out.
func visibleRolloutIDs(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace, query string,
) (ids []string, pending error) {
	t.Helper()

	resp, err := client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     query,
	})
	if err != nil {
		if searchAttributePropagating(err) {
			return nil, err
		}
		t.Fatalf("list workflows with %q: %v", query, err)
	}
	ids = make([]string, 0, len(resp.GetExecutions()))
	for _, execution := range resp.GetExecutions() {
		ids = append(ids, execution.GetExecution().GetWorkflowId())
	}
	sort.Strings(ids)
	return ids, nil
}

// waitForVisible polls one list query until it answers with want, so a later exact comparison
// cannot pass because a run had not been indexed yet.
func waitForVisible(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace, query, want string,
	grace time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(grace)
	for {
		got, pending := visibleRolloutIDs(t, ctx, client, namespace, query)
		if slices.Contains(got, want) {
			return
		}
		if time.Now().After(deadline) {
			if pending != nil {
				t.Fatalf("query %q was still refused %s after it was issued: %v", query, grace, pending)
			}
			t.Fatalf("query %q did not answer with %s within %s: got %v", query, want, grace, got)
		}
		time.Sleep(visibilityPoll)
	}
}

// waitForSearchAttributes polls the namespace's registry until it carries the three rollout
// attributes, so the runs start only once the frontend behind the registration serves them: a run
// that upserts an attribute the frontend has not picked up would leave its run unindexed.
func waitForSearchAttributes(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace string,
	grace time.Duration,
) {
	t.Helper()

	want := []string{
		temporal.SearchAttrRolloutFirmware,
		temporal.SearchAttrRolloutRegion,
		temporal.SearchAttrRolloutStatus,
	}
	deadline := time.Now().Add(grace)
	for {
		resp, err := client.OperatorService().ListSearchAttributes(ctx,
			&operatorservice.ListSearchAttributesRequest{Namespace: namespace})
		if err != nil {
			t.Fatalf("list the search attributes of %s: %v", namespace, err)
		}
		have := resp.GetCustomAttributes()
		missing := make([]string, 0, len(want))
		for _, name := range want {
			if _, ok := have[name]; !ok {
				missing = append(missing, name)
			}
		}
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %s still lacks the search attributes %v after %s",
				namespace, missing, grace)
		}
		time.Sleep(visibilityPoll)
	}
}

// TestRolloutVisibilityFiltersRolloutRuns backs the rollout-search-attributes scenario "Filtering
// rollouts in the Temporal UI": an operator lists workflows with a query combining firmware
// version, region, and status, and exactly the rollout runs whose current state matches all three
// conditions are returned. It runs the real RolloutWorkflow on a real Temporal server — the
// visibility store, the search-attribute registry, and the list query are the server's own — over
// stand-in activities, then asks the namespace the way the UI does.
func TestRolloutVisibilityFiltersRolloutRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	grace := testexec.Get(t).Wait(60 * time.Second)

	server := temporaltest.For(t)
	client := server.Client
	// The namespace carries the rollout attributes before any run starts: a run cannot upsert an
	// attribute its namespace does not have, and the queries filter on them.
	if err := temporal.EnsureSearchAttributes(ctx, client.OperatorService(), server.Namespace); err != nil {
		t.Fatalf("register the namespace's rollout search attributes: %v", err)
	}
	waitForSearchAttributes(t, ctx, client, server.Namespace, grace)

	acts := &visibilityActivities{
		firmware: map[string]temporal.Firmware{
			"fw-a": {ID: "fw-a", Version: "2.0.0", Models: []string{"oak-s3"}, Checksum: "sha256:a"},
			"fw-b": {ID: "fw-b", Version: "1.0.0", Models: []string{"oak-s3"}, Checksum: "sha256:b"},
		},
		devices: []string{"device-1"},
	}
	w := worker.New(client, visibilityQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(temporal.RolloutWorkflow,
		workflow.RegisterOptions{Name: temporal.RolloutWorkflowName})
	w.RegisterActivityWithOptions(acts.loadFirmware,
		activity.RegisterOptions{Name: temporal.LoadFirmwareActivityName})
	w.RegisterActivityWithOptions(acts.resolveWave,
		activity.RegisterOptions{Name: temporal.ResolveWaveTargetsActivityName})
	w.RegisterActivityWithOptions(acts.recordRollout,
		activity.RegisterOptions{Name: temporal.RecordRolloutStateActivityName})
	w.RegisterActivityWithOptions(acts.recordWave,
		activity.RegisterOptions{Name: temporal.RecordWaveStateActivityName})
	w.RegisterActivityWithOptions(acts.updateDevice,
		activity.RegisterOptions{Name: temporal.UpdateDeviceActivityName})
	w.RegisterActivityWithOptions(acts.evaluateWave,
		activity.RegisterOptions{Name: temporal.EvaluateWaveHealthActivityName})
	startVisibilityWorker(t, w)

	settings, err := temporal.NewRolloutSettings(visibilityHealthWindow, 30*time.Minute,
		30*time.Second, []temporal.RolloutWave{{Percent: 100}})
	if err != nil {
		t.Fatalf("build the visibility runs' canary policy: %v", err)
	}
	starter := temporal.NewRolloutStarter(client, visibilityQueue, settings)

	// Three runs whose selectors differ in one condition each: the match, one deploying another
	// firmware, and one targeting another region. All three are driving their waves at the end
	// of the test, so only the conditions can separate them.
	match := visibilityRun{
		rolloutID: "ro-ui-match", firmwareID: "fw-a", version: "2.0.0",
		region: "eu-west", model: "oak-s3",
	}
	otherFirmware := visibilityRun{
		rolloutID: "ro-ui-other-firmware", firmwareID: "fw-b", version: "1.0.0",
		region: "eu-west", model: "oak-s3",
	}
	otherRegion := visibilityRun{
		rolloutID: "ro-ui-other-region", firmwareID: "fw-a", version: "2.0.0",
		region: "us-east", model: "oak-s3",
	}
	runs := []visibilityRun{match, otherFirmware, otherRegion}
	for _, run := range runs {
		err := starter.Start(ctx, temporal.RolloutRequest{
			RolloutID:  run.rolloutID,
			FirmwareID: run.firmwareID,
			Region:     run.region,
			Model:      run.model,
		})
		if err != nil {
			t.Fatalf("start rollout %s: %v", run.rolloutID, err)
		}
	}

	// Each run's own state query reports it driving its wave with its firmware version known.
	states := temporal.NewRolloutStates(client)
	for _, run := range runs {
		waitForRunningRollout(t, ctx, states, run, grace)
	}

	firmwareQuery := fmt.Sprintf("%s = %q", temporal.SearchAttrRolloutFirmware, match.version)
	regionQuery := fmt.Sprintf("%s = %q", temporal.SearchAttrRolloutRegion, match.region)
	statusQuery := fmt.Sprintf("%s = %q", temporal.SearchAttrRolloutStatus, rollout.RolloutRunning)
	combinedQuery := fmt.Sprintf("%s AND %s AND %s", firmwareQuery, regionQuery, statusQuery)

	// Every run is indexed with all three of its attributes before the exact comparisons below:
	// the run that differs by region is found by the matching firmware alone, the one that
	// differs by firmware by the matching region alone, and the match by the combined query.
	waitForVisible(t, ctx, client, server.Namespace, firmwareQuery,
		temporal.RolloutWorkflowID(otherRegion.rolloutID), grace)
	waitForVisible(t, ctx, client, server.Namespace, regionQuery,
		temporal.RolloutWorkflowID(otherFirmware.rolloutID), grace)
	waitForVisible(t, ctx, client, server.Namespace, combinedQuery,
		temporal.RolloutWorkflowID(match.rolloutID), grace)

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "firmware version alone",
			query: firmwareQuery,
			want: []string{
				temporal.RolloutWorkflowID(match.rolloutID),
				temporal.RolloutWorkflowID(otherRegion.rolloutID),
			},
		},
		{
			name:  "region alone",
			query: regionQuery,
			want: []string{
				temporal.RolloutWorkflowID(match.rolloutID),
				temporal.RolloutWorkflowID(otherFirmware.rolloutID),
			},
		},
		{
			name:  "status alone",
			query: statusQuery,
			want: []string{
				temporal.RolloutWorkflowID(match.rolloutID),
				temporal.RolloutWorkflowID(otherFirmware.rolloutID),
				temporal.RolloutWorkflowID(otherRegion.rolloutID),
			},
		},
		{
			name:  "firmware version, region, and status combined",
			query: combinedQuery,
			want:  []string{temporal.RolloutWorkflowID(match.rolloutID)},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			want := slices.Clone(testCase.want)
			sort.Strings(want)
			got, pending := visibleRolloutIDs(t, ctx, client, server.Namespace, testCase.query)
			if pending != nil {
				t.Fatalf("query %q was refused after the wait above: %v", testCase.query, pending)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("query %q returned (-want +got):\n%s", testCase.query, diff)
			}
		})
	}
}
