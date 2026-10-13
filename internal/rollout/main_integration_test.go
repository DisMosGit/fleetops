//go:build integration

package rollout

import (
	"os"
	"testing"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's MongoDB stacks once, before the first test body, and releases
// them when the run ends; each test gets a database of its own on each. The stores' tests use the
// standalone stack, and the eligible-device view's tests need the replica set, because MongoDB
// serves change streams only on one.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m,
		mongotest.SharedStack(),
		mongotest.ReplicaSetStack(),
	))
}
