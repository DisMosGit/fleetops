//go:build integration

package devices

import (
	"os"
	"testing"

	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestMain starts this package's MongoDB stack once, before the first test body, and releases it
// when the run ends; each test gets a database of its own on it.
func TestMain(m *testing.M) {
	os.Exit(testexec.Main(m, mongotest.SharedStack()))
}
