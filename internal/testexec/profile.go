// Package testexec resolves how much of the suite a test run executes and with what budgets.
//
// A run's configuration is a function of its environment alone: the helper reads the documented
// FLEETOPS_TEST_* variables and never inspects the host's memory, CPU count, or load. Two
// machines running the same commit with the same environment therefore run the same assertions,
// which is what keeps a failure reproducible somewhere else and keeps a misconfiguration visible
// instead of being absorbed by an automatic downgrade.
//
// The documented set is exactly the five variables below. With none of them set the run uses the
// values the suite used before this configuration existed; the weak profile — the one to reach
// for on a machine of about 4 GB of RAM and 4 CPUs — narrows workload sizes, probe budgets, and
// the container stack a package starts. Narrowing is by size and budget only: a test asserts the
// same behavior under every profile, and a test the run cannot afford is reported as skipped
// naming the variable that would afford it.
package testexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// The documented variable set. A variable outside it has no effect on a run.
const (
	// EnvProfile selects the run's profile: ProfileDefault or ProfileWeak.
	EnvProfile = "FLEETOPS_TEST_PROFILE"
	// EnvFleetSize caps how many simulated devices a test may run, as a positive integer.
	EnvFleetSize = "FLEETOPS_TEST_FLEET_SIZE"
	// EnvWaitMultiplier scales a test's own wait budgets, as a number greater than zero.
	EnvWaitMultiplier = "FLEETOPS_TEST_WAIT_MULTIPLIER"
	// EnvContainerStartup overrides every container-startup budget, as a positive duration.
	// Unset means each harness uses its own documented default.
	EnvContainerStartup = "FLEETOPS_TEST_CONTAINER_STARTUP"
	// EnvContainerUse selects how container-backed tests obtain their services:
	// ContainersShared or ContainersPerTest.
	EnvContainerUse = "FLEETOPS_TEST_CONTAINER_USE"
)

// The profiles a run may select.
const (
	// ProfileDefault is the profile a capable machine runs: today's workload sizes and budgets.
	ProfileDefault = "default"
	// ProfileWeak is the profile a small machine runs, documented for 4 GB of RAM and 4 CPUs.
	ProfileWeak = "weak"
)

// How container-backed tests obtain their services.
const (
	// ContainersShared shares one stack per test package, isolated per test. It is the default,
	// because a package's peak container count must not grow with its test count.
	ContainersShared = "shared"
	// ContainersPerTest gives every test its own stack. It is the escape hatch for a test that
	// needs a private service it may stop, restart, or exhaust.
	ContainersPerTest = "per-test"
)

// The numbers behind the two profiles. The weak profile's values are all at most the default
// profile's, which is what keeps a weak run from ever being heavier than an unconfigured one.
const (
	// defaultFleetSize is the largest fleet the suite simulates: the 1000-device fleet the
	// emulator's own tests already run.
	defaultFleetSize = 1000
	// defaultWaitMultiplier leaves every wait budget at the value its test documents.
	defaultWaitMultiplier = 1.0
	// weakFleetSize keeps a weak run's simulated fleets at 100 devices.
	weakFleetSize = 100
	// weakWaitMultiplier halves a weak run's wait budgets, because it narrows the work those
	// budgets have to cover by more than it narrows them: one shared stack per package and a
	// tenth of the fleet is the reason a small machine stops producing deadline failures.
	weakWaitMultiplier = 0.5
	// weakContainerStartup leaves 45s for a container to boot, against the 60s to 3m the
	// harnesses document individually.
	weakContainerStartup = 45 * time.Second
)

// ErrUndefinedProfile reports a profile name outside the documented set.
var ErrUndefinedProfile = errors.New("undefined test profile")

// Config is the resolved configuration of one test run.
type Config struct {
	// Profile is the profile the run selected.
	Profile string
	// MaxFleetSize is the largest simulated fleet the run affords.
	MaxFleetSize int
	// WaitMultiplier scales the wait budgets a test documents for itself.
	WaitMultiplier float64
	// ContainerStartup overrides every container-startup budget when non-zero. A zero value
	// means the run set no override and each harness uses its own documented default.
	ContainerStartup time.Duration
	// Containers is how container-backed tests obtain their services.
	Containers string
}

// Get returns the run's resolved configuration, failing t — before it does any of its own work —
// when the environment cannot be resolved. A test that needs the configuration calls this first.
func Get(t *testing.T) Config {
	t.Helper()
	cfg, err := Resolve()
	if err != nil {
		t.Fatalf("resolve the test run's configuration: %v", err)
	}
	return cfg
}

// Resolve returns the configuration the process environment describes. A value that cannot be
// used fails here, naming the variable and the value it rejected, rather than surfacing later as
// an unexplained wait or a test that quietly ran less than it says it does.
func Resolve() (Config, error) {
	return resolveFrom(os.LookupEnv)
}

// resolveFrom is Resolve over an arbitrary lookup, so the variables can be covered without
// touching the process environment.
func resolveFrom(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{
		Profile:        ProfileDefault,
		MaxFleetSize:   defaultFleetSize,
		WaitMultiplier: defaultWaitMultiplier,
		Containers:     ContainersShared,
	}

	profile := ProfileDefault
	if raw, ok := lookup(EnvProfile); ok {
		profile = raw
		switch raw {
		case ProfileDefault:
		case ProfileWeak:
			cfg = weakConfig()
		default:
			return Config{}, fmt.Errorf("%s: %w %q (defined: %s, %s)",
				EnvProfile, ErrUndefinedProfile, raw, ProfileDefault, ProfileWeak)
		}
	}
	cfg.Profile = profile

	if raw, ok := lookup(EnvFleetSize); ok {
		size, err := positiveInt(EnvFleetSize, raw)
		if err != nil {
			return Config{}, err
		}
		cfg.MaxFleetSize = size
	}
	if raw, ok := lookup(EnvWaitMultiplier); ok {
		multiplier, err := positiveFloat(EnvWaitMultiplier, raw)
		if err != nil {
			return Config{}, err
		}
		cfg.WaitMultiplier = multiplier
	}
	if raw, ok := lookup(EnvContainerStartup); ok {
		budget, err := positiveDuration(EnvContainerStartup, raw)
		if err != nil {
			return Config{}, err
		}
		cfg.ContainerStartup = budget
	}
	if raw, ok := lookup(EnvContainerUse); ok {
		switch raw {
		case ContainersShared, ContainersPerTest:
			cfg.Containers = raw
		default:
			return Config{}, fmt.Errorf("%s: rejected %q (defined: %s, %s)",
				EnvContainerUse, raw, ContainersShared, ContainersPerTest)
		}
	}
	return cfg, nil
}

// weakConfig is the weak profile's configuration, before any explicit variable overrides it.
func weakConfig() Config {
	return Config{
		Profile:          ProfileWeak,
		MaxFleetSize:     weakFleetSize,
		WaitMultiplier:   weakWaitMultiplier,
		ContainerStartup: weakContainerStartup,
		Containers:       ContainersShared,
	}
}

// Wait scales a wait budget the test documented for itself. A scaled budget never drops below a
// millisecond, so an extreme multiplier cannot turn a wait into a spin.
func (c Config) Wait(d time.Duration) time.Duration {
	scaled := time.Duration(float64(d) * c.WaitMultiplier)
	if scaled < time.Millisecond {
		return time.Millisecond
	}
	return scaled
}

// Startup returns the budget a container of the named service is given to become ready: the
// run's override when it set one, and the service's own documented fallback otherwise.
func (c Config) Startup(fallback time.Duration) time.Duration {
	if c.ContainerStartup > 0 {
		return c.ContainerStartup
	}
	return fallback
}

// SharesContainers reports whether container-backed tests share one stack per package. When it
// is false the run asked for a private stack per test.
func (c Config) SharesContainers() bool {
	return c.Containers == ContainersShared
}

// FleetSize returns the number of devices a test should simulate when it wants want: want
// itself, narrowed to what the run affords.
func (c Config) FleetSize(want int) int {
	if want > c.MaxFleetSize {
		return c.MaxFleetSize
	}
	return want
}

// RequireFleetSize skips t unless the run affords a fleet of want devices, naming what the run
// affords and the variable that would raise it. A test that cannot fit the run's budget says so
// rather than asserting against a fleet smaller than the behavior it claims to verify.
func (c Config) RequireFleetSize(t *testing.T, want int) {
	t.Helper()
	if c.MaxFleetSize < want {
		t.Skipf("skipping: this run affords a fleet of %d devices, and the test needs %d; set %s=%d",
			c.MaxFleetSize, want, EnvFleetSize, want)
	}
}

// positiveInt parses a variable that must hold a whole number greater than zero.
func positiveInt(name, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: rejected %q: not a whole number", name, raw)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: rejected %q: must be greater than zero", name, raw)
	}
	return value, nil
}

// positiveFloat parses a variable that must hold a number greater than zero.
func positiveFloat(name, raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: rejected %q: not a number", name, raw)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: rejected %q: must be greater than zero", name, raw)
	}
	return value, nil
}

// positiveDuration parses a variable that must hold a positive Go duration.
func positiveDuration(name, raw string) (time.Duration, error) {
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: rejected %q: not a duration (for example 90s)", name, raw)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: rejected %q: must be greater than zero", name, raw)
	}
	return value, nil
}

// Stack is a package-level dependency the tests of one package share: a container stack, or
// anything else that must exist before the package's first test body runs. An integration
// package wires the stacks it uses into Main.
type Stack struct {
	// Name is what a failure to start reports.
	Name string
	// Start opens the stack and returns what releases it. A start that fails returns the
	// reason the package cannot run.
	Start func(ctx context.Context) (release func(), err error)
}

// Main is the TestMain body of a package whose tests share stacks: it resolves the run's
// configuration and starts every stack before the first test body, so a misconfigured run or a
// stack that cannot boot fails the package once, with the reason, instead of failing each test
// separately. An integration package calls it as
//
//	func TestMain(m *testing.M) { os.Exit(testexec.Main(m, mongotest.SharedStack())) }
func Main(m *testing.M, stacks ...Stack) int {
	if _, err := Resolve(); err != nil {
		fmt.Fprintf(os.Stderr, "test configuration: %v\n", err)
		return 2
	}
	releases := make([]func(), 0, len(stacks))
	for _, stack := range stacks {
		release, err := stack.Start(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "test setup: %s could not start: %v\n", stack.Name, err)
			releaseAll(releases)
			return 1
		}
		releases = append(releases, release)
	}
	code := m.Run()
	releaseAll(releases)
	return code
}

// releaseAll releases every started stack, in reverse order of starting.
func releaseAll(releases []func()) {
	for i := len(releases) - 1; i >= 0; i-- {
		releases[i]()
	}
}
