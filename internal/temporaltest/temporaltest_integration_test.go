//go:build integration

package temporaltest

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"
)

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
