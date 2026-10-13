//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/rabbittest"
	"github.com/DisMosGit/fleetops/internal/temporaltest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's container stacks once, before the first test body, and releases
// them when the run ends. Every heavy test in this binary used to boot its own MongoDB, RabbitMQ,
// PostgreSQL, and Temporal containers — thirty-two container starts across the smoke and the eight
// restart scenarios. They now share one stack and isolate themselves by database and namespace,
// which is what makes the whole binary affordable on a small machine.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m,
		mongotest.SharedStack(),
		rabbittest.SharedStack(),
		temporaltest.SharedStack(),
	))
}
