//go:build integration

package rabbittest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The shared stack is package-level state, which is what a broker shared by a package's test
// functions requires: Go gives a test no way to hand a value to the next test, so the one broker a
// package starts lives here and every test reaches it through Shared or For.
var (
	// sharedOnce guards the shared broker's single start.
	sharedOnce sync.Once
	// sharedStack is the started broker, valid once sharedErr is nil.
	sharedStack *Harness
	// sharedErr is why the broker could not start, kept so every test reports the same reason
	// rather than starting a broker of its own to fail differently.
	sharedErr error
)

// SharedStack is the broker an integration package wires into testexec.Main, so it is started once
// before the first test body and released when the run ends:
//
//	func TestMain(m *testing.M) { os.Exit(testexec.Main(m, rabbittest.SharedStack())) }
//
// A package that does not wire it still shares one broker: Shared starts it on first use.
func SharedStack() testexec.Stack {
	return testexec.Stack{Name: "rabbitmq", Start: func(ctx context.Context) (func(), error) {
		cfg, err := testexec.Resolve()
		if err != nil {
			return nil, err
		}
		h, err := startSharedBroker(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return func() { h.terminate() }, nil
	}}
}

// Shared returns the broker every test in the package publishes to and consumes from. Tests
// isolate themselves by purging the queues they use before they publish to them, which is what the
// harness's Purge is for; the queues themselves survive between tests, because a durable queue that
// vanished mid-run would be a different broker from the one the local stack runs.
func Shared(t *testing.T) *Harness {
	t.Helper()
	h, err := startSharedBroker(context.Background(), testexec.Get(t))
	if err != nil {
		t.Fatalf("shared rabbitmq stack: %v", err)
	}
	return h
}

// For returns the harness a test should use: the package's shared broker by default, or a broker
// of its own when the run asked for a private stack per test. It is what an integration test
// calls, so the run's configuration decides how the broker is obtained and the test's own
// assertions are the same either way.
func For(t *testing.T) *Harness {
	t.Helper()
	if !testexec.Get(t).SharesContainers() {
		return Start(t)
	}
	return Shared(t)
}

// startSharedBroker starts the package's broker once, returning the same harness — or the same
// failure — to every caller.
func startSharedBroker(ctx context.Context, cfg testexec.Config) (*Harness, error) {
	sharedOnce.Do(func() { sharedStack, sharedErr = bootShared(ctx, cfg) })
	return sharedStack, sharedErr
}

// bootShared reserves a host port and boots the shared broker on it.
func bootShared(ctx context.Context, cfg testexec.Config) (*Harness, error) {
	port, err := reserveHostPort()
	if err != nil {
		return nil, err
	}
	return boot(ctx, port, cfg)
}

// terminate removes the broker container, which is what the run's end does with the shared one.
func (h *Harness) terminate() {
	if h.container == nil {
		return
	}
	if err := h.container.Terminate(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "shared rabbitmq stack: terminate container: %v\n", err)
	}
}
