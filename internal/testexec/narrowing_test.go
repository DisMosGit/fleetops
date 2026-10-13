package testexec

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// resolveEnv resolves the configuration an environment describes, failing the test when that
// environment cannot be resolved. It reads the table rather than the process environment, so the
// profiles below are selected exactly as a run started with them would be.
func resolveEnv(t *testing.T, env map[string]string) Config {
	t.Helper()
	cfg, err := resolveFrom(tableLookup(env))
	if err != nil {
		t.Fatalf("resolve %v: %v", env, err)
	}
	return cfg
}

// sizeAndBudgetOnly returns cfg with everything a profile may legitimately choose — the name it
// was selected by and the workload sizes and budgets it sets — cleared. What is left is the
// behavior a profile must not change, so a difference between two profiles seen through this
// function is a profile that changed something other than how much work a run does. Adding a field
// to Config therefore means deciding here whether it is a size or a budget: a knob that quietly
// weakened what a test asserts would appear as a diff instead of passing unnoticed.
func sizeAndBudgetOnly(cfg Config) Config {
	cfg.Profile = ""
	cfg.MaxFleetSize = 0
	cfg.WaitMultiplier = 0
	cfg.ContainerStartup = 0
	return cfg
}

// The workload the body below asks for. The fleet is above the weak profile's cap and below the
// default profile's, so one body run under both profiles is given two different workload sizes.
const (
	narrowBodyFleet = 400
	narrowWaveSize  = 50
	narrowBodyWait  = 2 * time.Second
)

// narrowOutcome is the observable behavior one run of the body asserts. It is what must not depend
// on the profile.
type narrowOutcome struct {
	// EveryDeviceTerminal is true when every device the run enrolled reached the terminal state.
	EveryDeviceTerminal bool
	// NoDeviceUpgradedTwice is true when no device was upgraded more than once.
	NoDeviceUpgradedTwice bool
	// WavesCoverTheFleet is true when no device was left out of the run's waves.
	WavesCoverTheFleet bool
}

// narrowRun is what one run of the body observed: the outcome the body asserted, and the workload
// and budgets the profile gave it. Only the second half may differ between profiles.
type narrowRun struct {
	asserted narrowOutcome
	// Fleet is how many devices the run afforded the body.
	Fleet int
	// Waves is how many waves that fleet took.
	Waves int
	// Wait is the budget the run afforded the body for one wave.
	Wait time.Duration
}

// runNarrowedBody is the body both profiles run, standing in for a test in the suite. It simulates
// a rollout over the fleet the run affords and asserts the same fixed outcome every time: every
// device it enrolled reached the terminal state, no device was upgraded twice, and the waves
// covered the fleet. Nothing in that assertion is derived from the profile — the profile decides
// how many devices the run has and how long the body may wait for a wave, never what the body
// asserts — so a profile that narrowed the body's assertions rather than its workload fails here.
func runNarrowedBody(t *testing.T, cfg Config) narrowRun {
	t.Helper()

	fleet := cfg.FleetSize(narrowBodyFleet)
	if fleet < 1 {
		t.Fatalf("the run affords %d devices: a profile that narrows the fleet to nothing cannot run this body", fleet)
	}

	upgrades := make([]int, fleet)
	waves := 0
	for first := 0; first < fleet; first += narrowWaveSize {
		last := min(first+narrowWaveSize, fleet)
		for device := first; device < last; device++ {
			upgrades[device]++
		}
		waves++
	}

	var terminal, twice, never int
	for _, count := range upgrades {
		switch {
		case count == 1:
			terminal++
		case count > 1:
			twice++
		default:
			never++
		}
	}
	if never != 0 {
		t.Errorf("the body's waves left %d of %d devices out", never, fleet)
	}

	return narrowRun{
		asserted: narrowOutcome{
			EveryDeviceTerminal:   terminal == fleet,
			NoDeviceUpgradedTwice: twice == 0,
			WavesCoverTheFleet:    never == 0,
		},
		Fleet: fleet,
		Waves: waves,
		Wait:  cfg.Wait(narrowBodyWait),
	}
}

// TestNarrowedRunAssertsTheSameBehavior backs the test-execution-profiles scenario "A narrowed run
// asserts the same behavior": one body runs under both profiles and reports the same observable
// outcome under each, while the workload size and the budgets it was given are narrowed. Both
// profiles are selected through the environment a run would be started with, and everything that
// is not a workload size or a budget is required to be identical — so a profile that reached its
// budget by changing what a test asserts, or by widening a knob, fails here.
func TestNarrowedRunAssertsTheSameBehavior(t *testing.T) {
	t.Parallel()

	base := resolveEnv(t, nil)
	weak := resolveEnv(t, map[string]string{EnvProfile: ProfileWeak})

	baseRun := runNarrowedBody(t, base)
	weakRun := runNarrowedBody(t, weak)

	// The body asserts the same fixed outcome under each profile.
	want := narrowOutcome{EveryDeviceTerminal: true, NoDeviceUpgradedTwice: true, WavesCoverTheFleet: true}
	runs := []struct {
		profile string
		run     narrowRun
	}{
		{profile: ProfileDefault, run: baseRun},
		{profile: ProfileWeak, run: weakRun},
	}
	for _, tc := range runs {
		if diff := cmp.Diff(want, tc.run.asserted); diff != "" {
			t.Errorf("the body's asserted outcome under the %s profile mismatch (-want +got):\n%s", tc.profile, diff)
		}
	}

	// And the same outcome is what the two runs have in common.
	if diff := cmp.Diff(baseRun.asserted, weakRun.asserted); diff != "" {
		t.Errorf("the same body asserted different behavior under the two profiles (-default +weak):\n%s", diff)
	}

	// The two runs were not the same run: a narrowed run means the weak profile gave the body less
	// work, which is what makes the shared outcome above an observation about narrowing rather
	// than about two identical runs.
	if weakRun.Fleet >= baseRun.Fleet || weakRun.Waves >= baseRun.Waves || weakRun.Wait >= baseRun.Wait {
		t.Errorf("the weak run was not narrowed: %d devices in %d waves with a %v wait, against %d devices in %d waves with a %v wait",
			weakRun.Fleet, weakRun.Waves, weakRun.Wait, baseRun.Fleet, baseRun.Waves, baseRun.Wait)
	}

	// Only workload sizes and budgets differ. Container use, and any other knob a profile could
	// set, is behavior and must be identical.
	if diff := cmp.Diff(sizeAndBudgetOnly(base), sizeAndBudgetOnly(weak)); diff != "" {
		t.Errorf("the profiles differ outside workload sizes and budgets (-default +weak):\n%s", diff)
	}

	// Every number the weak profile sets is at most the default's, so no configuration makes a run
	// heavier than an unconfigured one.
	if weak.MaxFleetSize > base.MaxFleetSize {
		t.Errorf("the weak profile affords %d devices, above the default profile's %d", weak.MaxFleetSize, base.MaxFleetSize)
	}
	if weak.WaitMultiplier > base.WaitMultiplier {
		t.Errorf("the weak profile's wait multiplier is %v, above the default profile's %v", weak.WaitMultiplier, base.WaitMultiplier)
	}
	if fallback := time.Hour; weak.Startup(fallback) > base.Startup(fallback) {
		t.Errorf("the weak profile gives a container %v to start, above the default's %v",
			weak.Startup(fallback), base.Startup(fallback))
	}
}

// TestWeakProfileFitsTheDocumentedBudget backs the test-execution-profiles scenario "The weak-host
// profile runs the suite within its budget" with the half a unit test can observe: every value the
// profile sets is strictly narrower than the default it replaces, and it keeps one shared container
// stack per package, so a package's peak container count is the number of stacks it wires into
// Main rather than its number of tests. Those two are what the documented target — a machine with
// at most 4 GB of RAM and 4 CPUs (the capability spec and CONTRIBUTING.md) — is bought with, and
// they are asserted here rather than assumed. The peak memory of a container-backed run is not
// observable from a unit test: it is recorded by the weak-profile integration run the change's
// task 8.2 performs, and nothing in this file stands in for that measurement.
func TestWeakProfileFitsTheDocumentedBudget(t *testing.T) {
	t.Parallel()

	weak := resolveEnv(t, map[string]string{EnvProfile: ProfileWeak})
	base := resolveEnv(t, nil)

	t.Run("every knob narrows relative to the default", func(t *testing.T) {
		t.Parallel()
		knobs := []struct {
			name       string
			weak, base float64
		}{
			{name: "simulated fleet size", weak: float64(weak.MaxFleetSize), base: float64(base.MaxFleetSize)},
			{name: "wait multiplier", weak: weak.WaitMultiplier, base: base.WaitMultiplier},
		}
		for _, knob := range knobs {
			t.Run(knob.name, func(t *testing.T) {
				t.Parallel()
				if knob.weak > knob.base {
					t.Errorf("the weak profile widens the %s: %v against the default %v", knob.name, knob.weak, knob.base)
				}
				if knob.weak == knob.base {
					t.Errorf("the weak profile leaves the %s at the default %v, so it narrows no work", knob.name, knob.base)
				}
			})
		}
	})

	// The startup budget narrows against each harness's own documented budget, not against the
	// default profile's "no override". CONTRIBUTING.md documents those budgets: 60s for MongoDB,
	// 90s for RabbitMQ, and 3m for Temporal.
	t.Run("the container startup budget is below every harness's budget", func(t *testing.T) {
		t.Parallel()
		harnesses := []struct {
			name     string
			fallback time.Duration
		}{
			{name: "mongodb", fallback: 60 * time.Second},
			{name: "rabbitmq", fallback: 90 * time.Second},
			{name: "temporal", fallback: 3 * time.Minute},
		}
		for _, harness := range harnesses {
			t.Run(harness.name, func(t *testing.T) {
				t.Parallel()
				if got := weak.Startup(harness.fallback); got >= harness.fallback {
					t.Errorf("the weak profile gives %s %v to start, not below its documented budget %v",
						harness.name, got, harness.fallback)
				}
			})
		}
	})

	t.Run("the peak container count is a package's stacks, not its tests", func(t *testing.T) {
		t.Parallel()
		const (
			// wiredStacks is the most container stacks one package wires into testexec.Main in
			// this repository: MongoDB, RabbitMQ, and the replica-set MongoDB the change-stream
			// tests ask for.
			wiredStacks = 3
			// tests is a package's test count: the number the peak container count must not grow
			// with.
			tests = 25
		)
		if !weak.SharesContainers() {
			t.Fatalf("the weak profile takes a private stack per test, so a %d-test package holds %d container stacks instead of the %d it wires into Main",
				tests, liveStacks(weak, wiredStacks, tests), wiredStacks)
		}
		if got := liveStacks(weak, wiredStacks, tests); got != wiredStacks {
			t.Errorf("a package with %d tests holds %d container stacks under the weak profile, want the %d it wires into Main",
				tests, got, wiredStacks)
		}
		// Without this, the bound above would hold for a helper that ignores its configuration.
		if perTest := liveStacks(Config{Containers: ContainersPerTest}, wiredStacks, tests); perTest <= wiredStacks {
			t.Errorf("a private stack per test still counts %d stacks for %d tests, so the bound proves nothing",
				perTest, tests)
		}
	})

	// Nothing a profile sets touches what a test asserts: the two profiles are identical once the
	// sizes and budgets they choose are cleared.
	t.Run("the profile moves no knob a test asserts with", func(t *testing.T) {
		t.Parallel()
		if diff := cmp.Diff(sizeAndBudgetOnly(base), sizeAndBudgetOnly(weak)); diff != "" {
			t.Errorf("the weak profile differs from the default outside workload sizes and budgets (-default +weak):\n%s", diff)
		}
	})
}

// liveStacks is the configuration-level bound on how many container stacks a package with tests
// tests holds live at once: one per stack the package wires into Main when the run shares them,
// and one per test when the run asked for a private stack. It bounds the container count the
// weak-host budget is about; the memory behind those stacks is what the recorded weak-profile run
// measures.
func liveStacks(cfg Config, wired, tests int) int {
	if cfg.SharesContainers() {
		return wired
	}
	return wired * tests
}
