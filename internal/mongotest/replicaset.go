//go:build integration

package mongotest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The replica-set stack is package-level state of its own, for the same reason the standalone
// stack is: Go gives a test no way to hand a container to the next test, so the one replica-set
// stack a package starts lives here and every change-stream test reaches it through ReplicaSet.
var (
	// replicaSetOnce guards the replica-set stack's single start.
	replicaSetOnce sync.Once
	// replicaSetStack is the started replica-set stack, valid once replicaSetErr is nil.
	replicaSetStack *stack
	// replicaSetErr is why the replica-set stack could not start, kept so every test reports
	// the same reason rather than starting a container of its own to fail differently.
	replicaSetErr error
)

// replicaSetName is what the stack's single member is initiated as. Change streams are served by
// replica set members only, so a stack that must serve one is started as a one-member set.
const replicaSetName = "fleetops-rs"

// ReplicaSetStack is the replica-set stack an integration package wires into testexec.Main when
// its tests need change streams, so the container is started once before the first test body and
// released when the run ends:
//
//	func TestMain(m *testing.M) {
//		os.Exit(testexec.Main(m, mongotest.SharedStack(), mongotest.ReplicaSetStack()))
//	}
//
// It is separate from SharedStack because a change stream requires a replica set and most tests
// need only a standalone: the default stack stays as it is, and the replica-set container is
// started by the packages whose tests genuinely need one. A package that does not wire it still
// shares one stack: ReplicaSet starts it on first use.
func ReplicaSetStack() testexec.Stack {
	return testexec.Stack{Name: "mongodb-replicaset", Start: func(ctx context.Context) (func(), error) {
		cfg, err := testexec.Resolve()
		if err != nil {
			return nil, err
		}
		st, err := startReplicaSetStack(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return func() { st.close() }, nil
	}}
}

// ReplicaSet returns the package's replica-set stack with a database of this test's own, carrying
// the schema bootstrap and served by an initiated single-member replica set, so a test can open a
// change stream over the devices collection and watch it deliver an event. The container is
// started on first use and reused by every later test.
func ReplicaSet(t *testing.T) *Harness {
	t.Helper()
	st, err := startReplicaSetStack(context.Background(), testexec.Get(t))
	if err != nil {
		t.Fatalf("shared replica-set mongodb stack: %v", err)
	}
	return st.newHarness(t)
}

// startReplicaSetStack starts the package's replica-set stack once, returning the same stack — or
// the same failure — to every caller.
func startReplicaSetStack(ctx context.Context, cfg testexec.Config) (*stack, error) {
	replicaSetOnce.Do(func() {
		replicaSetStack, replicaSetErr = startStack(ctx, stackOptions{replSet: replicaSetName}, cfg)
	})
	return replicaSetStack, replicaSetErr
}

// replicaSetStartupBudget is the budget a replica-set stack needs to become usable, which is more
// than a standalone's: the container boots and accepts connections, the single member is initiated
// and elected primary, and the schema bootstrap runs against it. The port wait is bounded by the
// run's container-startup budget (startupTimeout, 60s, by default), the election by primaryBudget,
// and the bootstrap by primaryBudget's order of magnitude — the whole is what this documents, not
// a sum the harness enforces. Measured against the containers the suite starts: about 25s on an
// idle host and up to 90s on a loaded one.
const replicaSetStartupBudget = 2 * time.Minute
