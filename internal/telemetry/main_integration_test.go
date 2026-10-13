//go:build integration

package telemetry

import (
	"os"
	"testing"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's MongoDB and RabbitMQ stacks once, before the first test body, and
// releases them when the run ends; each test gets a database of its own, and isolates its queues
// by purging them.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m, mongotest.SharedStack(), rabbittest.SharedStack()))
}
