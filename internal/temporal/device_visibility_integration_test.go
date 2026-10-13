//go:build integration

package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/temporaltest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// deviceVisibilityQueue is the task queue the test's worker polls: the entity's workflow tasks and
// its snapshot activity. The settings name it as the dispatch queue too, because a device state is
// only valid with one — no command is issued, so no dispatch activity is ever scheduled.
const deviceVisibilityQueue = "device-visibility"

// The policy the test's runs start under. The snapshot interval outlives the test, so no periodic
// tick can change a state after the test has read it; the offline threshold is long enough that a
// fresh heartbeat keeps a device online for the whole run and short enough that a stale one marks
// its device offline immediately.
const (
	deviceVisibilitySnapshotInterval = time.Hour
	deviceVisibilityOfflineThreshold = 10 * time.Minute
	deviceVisibilityStaleHeartbeat   = 30 * time.Minute
)

// deviceVisibilityPoll is how long the test waits between observations. The visibility store
// indexes a started run and its attribute upserts asynchronously, so every observation is a poll.
const deviceVisibilityPoll = 250 * time.Millisecond

// deviceVisibilityRun is one device the test drives: the device id its workflow id derives from,
// and the four state values the scenario's conditions are about, which its run must end up
// carrying.
type deviceVisibilityRun struct {
	deviceID string
	region   string
	model    string
	firmware string
	online   bool
}

// deviceVisibilityActivities stands in for the snapshot activity. The scenario is about the
// attributes the workflow upserts, not the projection into the fleet database, so the write
// succeeds and asserts nothing.
type deviceVisibilityActivities struct{}

// snapshot stands in for the snapshot-device-state activity.
func (a *deviceVisibilityActivities) snapshot(context.Context, Snapshot) error {
	return nil
}

// startDeviceVisibilityWorker runs the test's worker until the test ends.
func startDeviceVisibilityWorker(t *testing.T, w worker.Worker) {
	t.Helper()

	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(stop); err != nil {
			t.Logf("device visibility worker stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Log("the device visibility worker did not stop within 30s")
		}
	})
}

// deviceVisibilityIDs returns the workflow ids of the given runs, sorted, so a comparison against
// a list query's answer is deterministic: the order the server answers in is not the test's.
func deviceVisibilityIDs(runs ...deviceVisibilityRun) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, DeviceWorkflowID(run.deviceID))
	}
	sort.Strings(ids)
	return ids
}

// waitForDeviceSearchAttributes polls the namespace's registry until it carries the four device
// attributes, so the runs start only once the registration landed: ListSearchAttributes reads the
// namespace's persisted configuration, and a run that upserts an attribute its namespace has not
// registered is left unindexed. The frontend's query path picks the registration up on its own
// cache's schedule, which the list-query waits below cover — see attributeNotCachedYet.
func waitForDeviceSearchAttributes(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace string,
	grace time.Duration,
) {
	t.Helper()

	want := []string{
		SearchAttrDeviceRegion,
		SearchAttrDeviceModel,
		SearchAttrDeviceFirmware,
		SearchAttrDeviceOnline,
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
		time.Sleep(deviceVisibilityPoll)
	}
}

// listDeviceWorkflowIDs asks the namespace's visibility store one list query — the same call the
// Temporal UI's list is built on — and returns the workflow ids it answers with, sorted so a
// comparison is deterministic.
func listDeviceWorkflowIDs(
	ctx context.Context,
	client temporalclient.Client,
	namespace, query string,
) ([]string, error) {
	resp, err := client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     query,
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(resp.GetExecutions()))
	for _, execution := range resp.GetExecutions() {
		ids = append(ids, execution.GetExecution().GetWorkflowId())
	}
	sort.Strings(ids)
	return ids, nil
}

// deviceVisibleWorkflowIDs is listDeviceWorkflowIDs for a caller that has already waited for the
// query to be served.
func deviceVisibleWorkflowIDs(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace, query string,
) []string {
	t.Helper()

	ids, err := listDeviceWorkflowIDs(ctx, client, namespace, query)
	if err != nil {
		t.Fatalf("list workflows with %q: %v", query, err)
	}
	return ids
}

// attributeNotCachedYet reports whether err is the frontend refusing a search-attribute name its
// namespace cache has not caught up with. With SQL visibility, registering a custom attribute
// writes a name-to-field alias onto the namespace config, and the frontend validating a list query
// reads the namespace through a cache it refreshes every system.namespaceCacheRefreshInterval
// (10 seconds by default). Until that refresh the query names an attribute the frontend's view of
// the namespace does not define, which is a wait rather than a failure — and the server's own text
// is what tells that apart from a genuinely malformed query.
func attributeNotCachedYet(err error) bool {
	var invalid *serviceerror.InvalidArgument
	return errors.As(err, &invalid) && strings.Contains(err.Error(), "is not a valid search attribute")
}

// waitForDeviceVisibility polls one list query until it answers with exactly want. The visibility
// store indexes a run's attributes asynchronously — and the frontend's view of the namespace's
// registered attributes lags registration — so the exact comparison after this must not be the
// first observation: settling on the exact set first means the comparison cannot fail because a
// run had not been indexed or an attribute was not yet queryable, and cannot pass because one the
// query should exclude was missing.
func waitForDeviceVisibility(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	namespace, query string,
	want []string,
	grace time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(grace)
	var lastErr error
	for {
		got, err := listDeviceWorkflowIDs(ctx, client, namespace, query)
		switch {
		case err == nil:
			if slices.Equal(got, want) {
				return
			}
			lastErr = nil
		case attributeNotCachedYet(err):
			lastErr = err
		default:
			t.Fatalf("list workflows with %q: %v", query, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("query %q answered %v (last error: %v) after %s, want %v",
				query, got, lastErr, grace, want)
		}
		time.Sleep(deviceVisibilityPoll)
	}
}

// signalDeviceVisibilityHeartbeat delivers the one heartbeat that puts a run into the state its
// conditions describe, then waits until the entity's own state query reports those four values.
// The upsert of the attributes is the same decision the state change is, so the state query
// answering is what says the trigger has landed; the sample time is what the entity derives
// liveness from, so an old one is how the offline run is made offline.
func signalDeviceVisibilityHeartbeat(
	t *testing.T,
	ctx context.Context,
	client temporalclient.Client,
	states *DeviceStates,
	run deviceVisibilityRun,
	sample time.Time,
	grace time.Duration,
) {
	t.Helper()

	signal := HeartbeatSignal{
		EventID:   "hb-" + run.deviceID,
		DeviceID:  run.deviceID,
		Region:    run.region,
		Model:     run.model,
		CurrentFw: run.firmware,
		Timestamp: sample,
	}
	if err := client.SignalWorkflow(ctx, DeviceWorkflowID(run.deviceID), "",
		HeartbeatSignalName, signal); err != nil {
		t.Fatalf("signal device %s: %v", run.deviceID, err)
	}

	deadline := time.Now().Add(grace)
	var last State
	var lastErr error
	for {
		state, err := states.State(ctx, run.deviceID)
		switch {
		case err != nil:
			// A query can arrive before the run's first workflow task has been decided; that
			// is a wait, not a state, so the read is retried until the deadline.
			lastErr = err
		case state.Region == run.region && state.Model == run.model &&
			state.CurrentFw == run.firmware && state.Online == run.online:
			return
		default:
			last, lastErr = state, nil
		}
		if time.Now().After(deadline) {
			t.Fatalf("device %s reports %+v (last read error: %v) after %s, want region %q, model %q, firmware %q, online %t",
				run.deviceID, last, lastErr, grace, run.region, run.model, run.firmware, run.online)
		}
		time.Sleep(deviceVisibilityPoll)
	}
}

// TestDeviceVisibilityFiltersDeviceRuns backs the device-search-attributes scenario "Filtering
// devices in the Temporal UI": an operator lists workflows with a query combining region, model,
// firmware version, and online status, and exactly the device runs whose current state matches all
// four conditions are returned. It runs real DeviceWorkflow entities on a real Temporal server —
// the visibility store, the search-attribute registry, and the list query are the server's own —
// over a stand-in snapshot activity, then asks the namespace the way the UI's list does. The
// package's existing integration test already boots the shared stack, so this adds no second one.
func TestDeviceVisibilityFiltersDeviceRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	grace := testexec.Get(t).Wait(60 * time.Second)

	server := temporaltest.For(t)
	client := server.Client
	// The namespace carries the device attributes before any run starts: a run cannot upsert an
	// attribute its namespace does not have, and the queries filter on them.
	if err := EnsureSearchAttributes(ctx, client.OperatorService(), server.Namespace); err != nil {
		t.Fatalf("register the namespace's device search attributes: %v", err)
	}
	waitForDeviceSearchAttributes(t, ctx, client, server.Namespace, grace)

	activities := &deviceVisibilityActivities{}
	w := worker.New(client, deviceVisibilityQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(DeviceWorkflow, workflow.RegisterOptions{Name: DeviceWorkflowName})
	w.RegisterActivityWithOptions(activities.snapshot,
		activity.RegisterOptions{Name: SnapshotActivityName})
	startDeviceVisibilityWorker(t, w)

	// Five runs: the match, and one that differs in exactly one of the four conditions. Each
	// condition alone therefore selects the match and three of the others, so a query that ignored
	// its condition would answer with extra runs, and one that over-applied it would lose a run.
	match := deviceVisibilityRun{
		deviceID: "device-ui-match", region: "eu-west", model: "sensor-2",
		firmware: "2.0.3", online: true,
	}
	otherRegion := deviceVisibilityRun{
		deviceID: "device-ui-other-region", region: "us-east", model: "sensor-2",
		firmware: "2.0.3", online: true,
	}
	otherModel := deviceVisibilityRun{
		deviceID: "device-ui-other-model", region: "eu-west", model: "oak-s3",
		firmware: "2.0.3", online: true,
	}
	otherFirmware := deviceVisibilityRun{
		deviceID: "device-ui-other-firmware", region: "eu-west", model: "sensor-2",
		firmware: "1.0.0", online: true,
	}
	offline := deviceVisibilityRun{
		deviceID: "device-ui-offline", region: "eu-west", model: "sensor-2",
		firmware: "2.0.3", online: false,
	}
	runs := []deviceVisibilityRun{match, otherRegion, otherModel, otherFirmware, offline}

	for _, run := range runs {
		_, err := client.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
			ID:        DeviceWorkflowID(run.deviceID),
			TaskQueue: deviceVisibilityQueue,
		}, DeviceWorkflowName, newDeviceState(run.deviceID, DeviceSettings{
			SnapshotInterval:  deviceVisibilitySnapshotInterval,
			OfflineThreshold:  deviceVisibilityOfflineThreshold,
			DispatchTaskQueue: deviceVisibilityQueue,
		}))
		if err != nil {
			t.Fatalf("start device %s: %v", run.deviceID, err)
		}
	}

	// Each run is driven into its state by the entity's own heartbeat signal — the upsert path the
	// attributes come from — and its state query is the proof the four values landed before any
	// list query runs. A stale sample is what makes the offline run offline: liveness is heartbeat
	// recency, so the entity has to derive it rather than be told it.
	states := NewDeviceStates(client)
	for _, run := range runs {
		sample := time.Now().UTC()
		if !run.online {
			sample = sample.Add(-deviceVisibilityStaleHeartbeat)
		}
		signalDeviceVisibilityHeartbeat(t, ctx, client, states, run, sample, grace)
	}

	// The four queries the UI issues for the conditions, and the combined one it issues when the
	// operator fills in every filter — the scenario's own query.
	regionQuery := fmt.Sprintf("%s = %q", SearchAttrDeviceRegion, match.region)
	modelQuery := fmt.Sprintf("%s = %q", SearchAttrDeviceModel, match.model)
	firmwareQuery := fmt.Sprintf("%s = %q", SearchAttrDeviceFirmware, match.firmware)
	onlineQuery := fmt.Sprintf("%s = true", SearchAttrDeviceOnline)
	combinedQuery := fmt.Sprintf("%s AND %s AND %s AND %s",
		regionQuery, modelQuery, firmwareQuery, onlineQuery)

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "region alone",
			query: regionQuery,
			want:  deviceVisibilityIDs(match, otherModel, otherFirmware, offline),
		},
		{
			name:  "model alone",
			query: modelQuery,
			want:  deviceVisibilityIDs(match, otherRegion, otherFirmware, offline),
		},
		{
			name:  "firmware version alone",
			query: firmwareQuery,
			want:  deviceVisibilityIDs(match, otherRegion, otherModel, offline),
		},
		{
			name:  "online status alone",
			query: onlineQuery,
			want:  deviceVisibilityIDs(match, otherRegion, otherModel, otherFirmware),
		},
		{
			name:  "region, model, firmware version, and online status combined",
			query: combinedQuery,
			want:  deviceVisibilityIDs(match),
		},
	}

	// Every run's indexing converges before the comparisons: each single-condition query settles on
	// exactly the four runs it should answer with — which also settles the excluded run's attribute
	// — and the combined query on the match alone. Nothing is compared until the frontend serves
	// the attributes and the store agrees with the state queries above.
	for _, testCase := range tests {
		waitForDeviceVisibility(t, ctx, client, server.Namespace, testCase.query, testCase.want, grace)
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := deviceVisibleWorkflowIDs(t, ctx, client, server.Namespace, testCase.query)
			if diff := cmp.Diff(testCase.want, got); diff != "" {
				t.Errorf("query %q returned (-want +got):\n%s", testCase.query, diff)
			}
		})
	}
}
