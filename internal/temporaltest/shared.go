//go:build integration

package temporaltest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// The shared stack is package-level state, which is what a server shared by a package's test
// functions requires: Go gives a test no way to hand a value to the next test, so the one server a
// package starts lives here and every test reaches it through Shared or For.
var (
	// sharedOnce guards the server's single start.
	sharedOnce sync.Once
	// sharedStack is the started server, valid once sharedErr is nil.
	sharedStack *serverStack
	// sharedErr is why the server could not start, kept so every test reports the same reason
	// rather than starting a server of its own to fail differently.
	sharedErr error
	// namespaces numbers the namespaces the stack hands out, so no two tests share one.
	namespaces int
	// namespacesMu guards the counter, because a test may ask for its namespace in parallel with
	// another.
	namespacesMu sync.Mutex
)

// SharedStack is the server an integration package wires into testexec.Main, so it is started once
// before the first test body and released when the run ends:
//
//	func TestMain(m *testing.M) { os.Exit(testexec.Main(m, temporaltest.SharedStack())) }
//
// A package that does not wire it still shares one server: Shared starts it on first use.
func SharedStack() testexec.Stack {
	return testexec.Stack{Name: "temporal", Start: func(ctx context.Context) (func(), error) {
		cfg, err := testexec.Resolve()
		if err != nil {
			return nil, err
		}
		stack, err := startSharedStack(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return stack.close, nil
	}}
}

// Shared returns the package's server with a namespace of this test's own and a client bound to
// it. The server is started on first use and reused by every later test, so a package starts one
// server however many tests it has; the namespace is what keeps one test's workflow executions
// invisible to another.
func Shared(t *testing.T) *Harness {
	t.Helper()
	stack, err := startSharedStack(context.Background(), testexec.Get(t))
	if err != nil {
		t.Fatalf("shared temporal stack: %v", err)
	}
	return stack.newHarness(t)
}

// For returns the harness a test should use: a namespace of its own on the package's shared server
// by default, or a server of its own when the run asked for a private stack per test. It is what
// an integration test calls, so the run's configuration decides how the server is obtained and the
// test's own assertions are the same either way.
func For(t *testing.T) *Harness {
	t.Helper()
	if !testexec.Get(t).SharesContainers() {
		return Start(t)
	}
	return Shared(t)
}

// startSharedStack starts the package's server once, returning the same stack — or the same
// failure — to every caller.
func startSharedStack(ctx context.Context, cfg testexec.Config) (*serverStack, error) {
	sharedOnce.Do(func() { sharedStack, sharedErr = bootStack(ctx, cfg) })
	return sharedStack, sharedErr
}

// newHarness registers a namespace for one test and returns a harness whose client is bound to it.
// The namespace is named after the test, so a workflow a failed run left behind says where it came
// from.
func (s *serverStack) newHarness(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()
	name := nextNamespaceName(t.Name())
	if err := registerNamespace(ctx, s.client, name); err != nil {
		t.Fatalf("shared temporal stack: %v", err)
	}
	c, err := temporalclient.NewLazyClient(temporalclient.Options{
		HostPort:  s.address,
		Namespace: name,
	})
	if err != nil {
		t.Fatalf("create temporal client for namespace %s: %v", name, err)
	}
	t.Cleanup(c.Close)
	if err := waitReady(ctx, c, name, s.address, s.budget); err != nil {
		t.Fatalf("shared temporal stack: %v", err)
	}
	return &Harness{Address: s.address, Namespace: name, Client: c}
}

// nextNamespaceName returns a namespace name no other test of this run uses, carrying the test it
// belongs to.
func nextNamespaceName(testName string) string {
	namespacesMu.Lock()
	namespaces++
	n := namespaces
	namespacesMu.Unlock()

	suffix := sanitizeNamespaceName(testName)
	if suffix == "" {
		suffix = "test"
	}
	return fmt.Sprintf("fleetops-test-%d-%s", n, suffix)
}

// sanitizeNamespaceName reduces a test name to what a namespace name may carry, so a subtest's
// slashes and spaces do not end up in a name the server rejects.
func sanitizeNamespaceName(name string) string {
	mapped := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
	mapped = strings.Trim(mapped, "-")
	if len(mapped) > maxNamespaceSuffix {
		mapped = mapped[:maxNamespaceSuffix]
	}
	return mapped
}
