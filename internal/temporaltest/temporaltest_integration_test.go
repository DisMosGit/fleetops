//go:build integration

package temporaltest

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's Temporal server once, before the first test body, and releases it
// when the run ends.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m, SharedStack()))
}

// TestHarnessServesTheNamespace boots the harness twice, in sequence: the first boot proves the
// harness hands out a frontend that answers in the namespace its client is bound to, and the
// second — which starts only after the first subtest's cleanups ran — proves the teardown released
// the containers, the network, and the client rather than leaving them to collide with the next
// boot. It needs Docker.
func TestHarnessServesTheNamespace(t *testing.T) {
	t.Run("first boot", assertHarnessServesTheNamespace)
	t.Run("second boot after the first was torn down", assertHarnessServesTheNamespace)
}

// assertHarnessServesTheNamespace asserts one harness answers in the namespace the image's
// bootstrap created.
func assertHarnessServesTheNamespace(t *testing.T) {
	h := Start(t)
	ctx := context.Background()

	if h.Namespace != Namespace || h.Namespace == "" {
		t.Errorf("harness namespace = %q, want %q", h.Namespace, Namespace)
	}
	if h.Address == "" || h.Client == nil {
		t.Fatalf("harness = %+v, want a frontend address and a connected client", h)
	}
	if _, err := h.Client.WorkflowService().GetSystemInfo(ctx,
		&workflowservice.GetSystemInfoRequest{}); err != nil {
		t.Errorf("GetSystemInfo in namespace %s: %v", h.Namespace, err)
	}
	// GetSystemInfo is namespace-agnostic, so the namespace the client is bound to is asserted
	// through the namespace service itself: a client bound to a namespace that does not exist
	// would still answer the system call and fail on every workflow it started.
	described, err := h.Client.WorkflowService().DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: h.Namespace})
	if err != nil {
		t.Fatalf("DescribeNamespace(%s): %v", h.Namespace, err)
	}
	if got := described.GetNamespaceInfo().GetName(); got != Namespace {
		t.Errorf("describe namespace %s reports %q", Namespace, got)
	}
}

// TestWaitReadyFailsOnAServiceThatNeverAnswers drives the readiness helper with no containers at
// all: a closed port is a frontend that never answers, and the helper has to fail with a message
// that names the address and the budget rather than waiting for a call that will not return.
func TestWaitReadyFailsOnAServiceThatNeverAnswers(t *testing.T) {
	address := closedAddress(t)
	tests := []struct {
		name   string
		budget time.Duration
		want   string
	}{
		{
			name:   "a short budget is spent on attempts and then reported",
			budget: 300 * time.Millisecond,
			want:   "did not become ready",
		},
		{
			name:   "a budget that already elapsed returns immediately",
			budget: 0,
			want:   "already elapsed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := temporalclient.NewLazyClient(temporalclient.Options{
				HostPort:  address,
				Namespace: Namespace,
			})
			if err != nil {
				t.Fatalf("create client for %s: %v", address, err)
			}
			t.Cleanup(c.Close)

			started := time.Now()
			err = waitReady(context.Background(), c, Namespace, address, tc.budget)
			elapsed := time.Since(started)
			if err == nil {
				t.Fatalf("waitReady(%s, %v) = nil, want a failure", address, tc.budget)
			}
			if !strings.Contains(err.Error(), address) {
				t.Errorf("waitReady error = %q, want it to name %s", err, address)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("waitReady error = %q, want it to mention %q", err, tc.want)
			}
			// The budget is what bounds the wait: the call is never left to a transport
			// timeout, which is what makes a frontend that never answers a failed test rather
			// than a hung one.
			if limit := tc.budget + readinessAttempt; elapsed > limit {
				t.Errorf("waitReady took %v, want at most %v", elapsed, limit)
			}
		})
	}
}

// closedAddress returns a loopback address nothing listens on, as a dialable host:port.
func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a closed port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return address
}

// TestSharedNamespaceIsolation asserts what a namespace per test is for: each test gets a
// namespace of its own on the shared server, and an execution started in one test's namespace is
// not found from another's.
func TestSharedNamespaceIsolation(t *testing.T) {
	t.Parallel()
	const workflowID = "shared-namespace-isolation"

	var firstNamespace string
	t.Run("the first test starts an execution in a namespace of its own", func(t *testing.T) {
		h := Shared(t)
		firstNamespace = h.Namespace
		if h.Namespace == Namespace {
			t.Errorf("shared namespace = %q, want a namespace of this test's own", h.Namespace)
		}
		startProbe(t, h, workflowID)
		if _, err := h.Client.DescribeWorkflowExecution(context.Background(), workflowID, ""); err != nil {
			t.Fatalf("describe the workflow in namespace %s: %v", h.Namespace, err)
		}
	})

	t.Run("another test neither shares the namespace nor finds the execution", func(t *testing.T) {
		h := Shared(t)
		if h.Namespace == firstNamespace {
			t.Fatalf("both tests ran in namespace %q, want one namespace per test", h.Namespace)
		}
		_, err := h.Client.DescribeWorkflowExecution(context.Background(), workflowID, "")
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			t.Errorf("describing another namespace's execution: %v, want a not-found error", err)
		}
		// This namespace is usable in its own right, so the not-found above is isolation rather
		// than a client that cannot reach the server.
		startProbe(t, h, workflowID)
		if _, err := h.Client.DescribeWorkflowExecution(context.Background(), workflowID, ""); err != nil {
			t.Fatalf("describe this namespace's own workflow: %v", err)
		}
	})
}

// startProbe starts a workflow execution that no worker has to run: the point is that the execution
// exists in this namespace under id, which is what the isolation assertions compare.
func startProbe(t *testing.T, h *Harness, id string) {
	t.Helper()
	_, err := h.Client.ExecuteWorkflow(context.Background(), temporalclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: "namespace-isolation-probe",
	}, "NamespaceIsolationProbe")
	if err != nil {
		t.Fatalf("start the isolation probe %s in namespace %s: %v", id, h.Namespace, err)
	}
}
