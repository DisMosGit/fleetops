package testexec

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// tableLookup is a lookup over a map, so a case describes the environment it resolves without
// touching the process environment.
func tableLookup(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

// defaults is the configuration an environment with no variables set resolves to: today's
// workload sizes, today's probe budgets (no override, so each harness uses its own), and one
// shared container stack per package.
func defaults() Config {
	return Config{
		Profile:        ProfileDefault,
		MaxFleetSize:   defaultFleetSize,
		WaitMultiplier: defaultWaitMultiplier,
		Containers:     ContainersShared,
	}
}

func TestResolveFrom(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{
			name: "no variables set is today's run",
			env:  nil,
			want: defaults(),
		},
		{
			name: "the default profile selects today's values",
			env:  map[string]string{EnvProfile: ProfileDefault},
			want: defaults(),
		},
		{
			name: "the weak profile",
			env:  map[string]string{EnvProfile: ProfileWeak},
			want: Config{
				Profile:          ProfileWeak,
				MaxFleetSize:     weakFleetSize,
				WaitMultiplier:   weakWaitMultiplier,
				ContainerStartup: weakContainerStartup,
				Containers:       ContainersShared,
			},
		},
		{
			name: "the fleet size narrows the run",
			env:  map[string]string{EnvFleetSize: "25"},
			want: func() Config {
				cfg := defaults()
				cfg.MaxFleetSize = 25
				return cfg
			}(),
		},
		{
			name: "the wait multiplier scales the run's waits",
			env:  map[string]string{EnvWaitMultiplier: "3.5"},
			want: func() Config {
				cfg := defaults()
				cfg.WaitMultiplier = 3.5
				return cfg
			}(),
		},
		{
			name: "the container startup budget replaces every harness default",
			env:  map[string]string{EnvContainerStartup: "2m"},
			want: func() Config {
				cfg := defaults()
				cfg.ContainerStartup = 2 * time.Minute
				return cfg
			}(),
		},
		{
			name: "container use asks for a private stack per test",
			env:  map[string]string{EnvContainerUse: ContainersPerTest},
			want: func() Config {
				cfg := defaults()
				cfg.Containers = ContainersPerTest
				return cfg
			}(),
		},
		{
			name: "an explicit variable overrides the profile it names",
			env: map[string]string{
				EnvProfile:          ProfileWeak,
				EnvFleetSize:        "500",
				EnvWaitMultiplier:   "2",
				EnvContainerStartup: "90s",
				EnvContainerUse:     ContainersPerTest,
			},
			want: Config{
				Profile:          ProfileWeak,
				MaxFleetSize:     500,
				WaitMultiplier:   2,
				ContainerStartup: 90 * time.Second,
				Containers:       ContainersPerTest,
			},
		},
		{
			name: "an undocumented variable changes nothing",
			env: map[string]string{
				"FLEETOPS_TEST_FLEET":    "1",
				"FLEETOPS_TEST_PROFILES": ProfileWeak,
				"FLEETOPS_TEST":          "weak",
			},
			want: defaults(),
		},
		{
			name:    "an unknown profile name is refused",
			env:     map[string]string{EnvProfile: "tiny"},
			wantErr: EnvProfile,
		},
		{
			name:    "an empty profile name is refused",
			env:     map[string]string{EnvProfile: ""},
			wantErr: EnvProfile,
		},
		{
			name:    "a zero fleet size is refused",
			env:     map[string]string{EnvFleetSize: "0"},
			wantErr: EnvFleetSize,
		},
		{
			name:    "a negative fleet size is refused",
			env:     map[string]string{EnvFleetSize: "-10"},
			wantErr: EnvFleetSize,
		},
		{
			name:    "a non-numeric fleet size is refused",
			env:     map[string]string{EnvFleetSize: "many"},
			wantErr: EnvFleetSize,
		},
		{
			name:    "a zero wait multiplier is refused",
			env:     map[string]string{EnvWaitMultiplier: "0"},
			wantErr: EnvWaitMultiplier,
		},
		{
			name:    "a negative wait multiplier is refused",
			env:     map[string]string{EnvWaitMultiplier: "-1.5"},
			wantErr: EnvWaitMultiplier,
		},
		{
			name:    "a non-numeric wait multiplier is refused",
			env:     map[string]string{EnvWaitMultiplier: "twice"},
			wantErr: EnvWaitMultiplier,
		},
		{
			name:    "a zero container startup budget is refused",
			env:     map[string]string{EnvContainerStartup: "0s"},
			wantErr: EnvContainerStartup,
		},
		{
			name:    "a negative container startup budget is refused",
			env:     map[string]string{EnvContainerStartup: "-30s"},
			wantErr: EnvContainerStartup,
		},
		{
			name:    "a bare number is not a duration",
			env:     map[string]string{EnvContainerStartup: "90"},
			wantErr: EnvContainerStartup,
		},
		{
			name:    "an unknown container use is refused",
			env:     map[string]string{EnvContainerUse: "some"},
			wantErr: EnvContainerUse,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveFrom(tableLookup(tc.env))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("resolveFrom(%v) = %+v, want an error naming %s", tc.env, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolveFrom(%v) error = %q, want it to name %s", tc.env, err, tc.wantErr)
				}
				for _, raw := range tc.env {
					if raw != "" && !strings.Contains(err.Error(), raw) {
						t.Errorf("error %q does not name the rejected value %q", err, raw)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveFrom(%v): %v", tc.env, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("resolveFrom(%v) mismatch (-want +got):\n%s", tc.env, diff)
			}
		})
	}
}

// TestUndefinedProfileErrorIsWrapped keeps the sentinel usable by a caller that wants to tell an
// unknown profile from any other misconfiguration.
func TestUndefinedProfileErrorIsWrapped(t *testing.T) {
	t.Parallel()
	_, err := resolveFrom(tableLookup(map[string]string{EnvProfile: "tiny"}))
	if !errors.Is(err, ErrUndefinedProfile) {
		t.Fatalf("resolveFrom error = %v, want it to wrap ErrUndefinedProfile", err)
	}
}

// TestWeakProfileNarrowsEveryKnob asserts the property the profile exists for: no value it sets
// is above the default profile's, so configuring a weak run never makes it heavier than an
// unconfigured one.
func TestWeakProfileNarrowsEveryKnob(t *testing.T) {
	t.Parallel()
	weak, base := weakConfig(), defaults()
	if weak.MaxFleetSize > base.MaxFleetSize {
		t.Errorf("weak fleet size %d exceeds the default %d", weak.MaxFleetSize, base.MaxFleetSize)
	}
	if weak.WaitMultiplier > base.WaitMultiplier {
		t.Errorf("weak wait multiplier %v exceeds the default %v", weak.WaitMultiplier, base.WaitMultiplier)
	}
	if weak.ContainerStartup > 0 && base.ContainerStartup > 0 && weak.ContainerStartup > base.ContainerStartup {
		t.Errorf("weak startup budget %v exceeds the default %v", weak.ContainerStartup, base.ContainerStartup)
	}
}

// TestResolveIgnoresTheHostShape asserts that nothing in the resolution depends on how many CPUs
// the host has. GOMAXPROCS is the proxy available without a second machine: it is the runtime's
// own CPU-count knob, so a resolver that probed the host would be most likely to move with it.
func TestResolveIgnoresTheHostShape(t *testing.T) {
	baseline, err := Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	t.Setenv("GOMAXPROCS", "1")
	t.Setenv(EnvFleetSize, "250")
	again, err := Resolve()
	if err != nil {
		t.Fatalf("Resolve under a different host shape: %v", err)
	}
	want := baseline
	want.MaxFleetSize = 250
	if diff := cmp.Diff(want, again); diff != "" {
		t.Errorf("the same environment resolved differently (-want +got):\n%s", diff)
	}
}

// TestGetReportsAnUnusableEnvironment asserts the failure a test sees names the variable, which
// is what keeps a misconfiguration from surfacing as an unexplained timeout later. The failing
// test runs in a child process, because a failed subtest marks its parent failed too.
func TestGetReportsAnUnusableEnvironment(t *testing.T) {
	if os.Getenv("FLEETOPS_TESTEXEC_GET_BAD_ENV") == "1" {
		Get(t)
		return
	}
	t.Setenv("FLEETOPS_TESTEXEC_GET_BAD_ENV", "1")
	t.Setenv(EnvFleetSize, "not-a-number")
	code, out := runSelf(t)
	if code == 0 {
		t.Fatalf("Get returned from an unusable environment; output:\n%s", out)
	}
	for _, want := range []string{EnvFleetSize, "not-a-number"} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q does not name %q", out, want)
		}
	}
}

func TestWait(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		multiplier float64
		in         time.Duration
		want       time.Duration
	}{
		{name: "the default multiplier leaves the budget alone", multiplier: 1, in: 10 * time.Second, want: 10 * time.Second},
		{name: "a narrowing multiplier shortens the budget", multiplier: 0.5, in: 10 * time.Second, want: 5 * time.Second},
		{name: "a widening multiplier lengthens the budget", multiplier: 2, in: 10 * time.Second, want: 20 * time.Second},
		{name: "an extreme multiplier cannot turn a wait into a spin", multiplier: 0.000001, in: time.Second, want: time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{WaitMultiplier: tc.multiplier}
			if got := cfg.Wait(tc.in); got != tc.want {
				t.Errorf("Wait(%v) with multiplier %v = %v, want %v", tc.in, tc.multiplier, got, tc.want)
			}
		})
	}
}

func TestStartup(t *testing.T) {
	t.Parallel()
	fallback := 90 * time.Second
	tests := []struct {
		name string
		cfg  Config
		want time.Duration
	}{
		{name: "no override keeps the harness default", cfg: Config{}, want: fallback},
		{name: "an override replaces the harness default", cfg: Config{ContainerStartup: 45 * time.Second}, want: 45 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.Startup(fallback); got != tc.want {
				t.Errorf("Startup(%v) = %v, want %v", fallback, got, tc.want)
			}
		})
	}
}

func TestFleetSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		max  int
		want int
		got  int
	}{
		{name: "a test below the maximum runs its own size", max: 1000, want: 100, got: 100},
		{name: "a test at the maximum runs its own size", max: 1000, want: 1000, got: 1000},
		{name: "a test above the maximum is narrowed to it", max: 100, want: 1000, got: 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{MaxFleetSize: tc.max}
			if got := cfg.FleetSize(tc.want); got != tc.got {
				t.Errorf("FleetSize(%d) with maximum %d = %d, want %d", tc.want, tc.max, got, tc.got)
			}
		})
	}
}

func TestSharesContainers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		use  string
		want bool
	}{
		{name: "the shared default", use: ContainersShared, want: true},
		{name: "a private stack per test", use: ContainersPerTest, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Containers: tc.use}
			if got := cfg.SharesContainers(); got != tc.want {
				t.Errorf("SharesContainers() with %s = %v, want %v", tc.use, got, tc.want)
			}
		})
	}
}

// TestRequireFleetSize asserts both directions of the unaffordable-test rule: a run that cannot
// afford the fleet skips saying so, and a run that can afford it does not.
func TestRequireFleetSize(t *testing.T) {
	t.Parallel()
	t.Run("an unaffordable fleet skips instead of running smaller", func(t *testing.T) {
		t.Parallel()
		cfg := Config{MaxFleetSize: 10}
		cfg.RequireFleetSize(t, 100)
		t.Error("RequireFleetSize returned for a fleet the run cannot afford")
	})
	t.Run("an affordable fleet runs", func(t *testing.T) {
		t.Parallel()
		cfg := Config{MaxFleetSize: 1000}
		cfg.RequireFleetSize(t, 1000)
		if t.Skipped() {
			t.Error("RequireFleetSize skipped a fleet the run affords")
		}
	})
}

// TestGetHonoursTheProcessEnvironment is the one test that resolves the real environment: the
// documented variables reach a run through it.
func TestGetHonoursTheProcessEnvironment(t *testing.T) {
	t.Setenv(EnvProfile, ProfileWeak)
	t.Setenv(EnvFleetSize, "42")
	got := Get(t)
	want := Config{
		Profile:          ProfileWeak,
		MaxFleetSize:     42,
		WaitMultiplier:   weakWaitMultiplier,
		ContainerStartup: weakContainerStartup,
		Containers:       ContainersShared,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Get with a weak environment mismatch (-want +got):\n%s", diff)
	}
}

// TestResolveReportsTheVariableForEveryCase asserts the resolver's failure contract directly:
// every unusable value fails naming both the variable and the value.
func TestResolveReportsTheVariableForEveryCase(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, value string }{
		{EnvProfile, "micro"},
		{EnvFleetSize, "0"},
		{EnvFleetSize, "nope"},
		{EnvWaitMultiplier, "-0.5"},
		{EnvWaitMultiplier, "zero"},
		{EnvContainerStartup, "0"},
		{EnvContainerStartup, "soon"},
		{EnvContainerUse, "both"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			t.Parallel()
			_, err := resolveFrom(tableLookup(map[string]string{tc.name: tc.value}))
			if err == nil {
				t.Fatalf("%s=%q resolved without an error", tc.name, tc.value)
			}
			for _, want := range []string{tc.name, tc.value} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
		})
	}
}

// The child markers the two Main tests re-run this binary with.
const (
	childBadEnvironment = "FLEETOPS_TESTEXEC_MAIN_BAD_ENV"
	childBadStack       = "FLEETOPS_TESTEXEC_MAIN_BAD_STACK"
)

// TestMainFailsThePackageOnAnUnusableEnvironment asserts the package-level failure path: a
// misconfigured run is reported before any test body and with the variable named. Main is handed
// a nil *testing.M because both paths asserted here return before it would be used.
func TestMainFailsThePackageOnAnUnusableEnvironment(t *testing.T) {
	if os.Getenv(childBadEnvironment) == "1" {
		os.Exit(Main(nil))
	}
	t.Setenv(childBadEnvironment, "1")
	t.Setenv(EnvProfile, "micro")
	code, out := runSelf(t)
	if code != 2 {
		t.Errorf("Main with an unusable environment exited %d, want 2; output:\n%s", code, out)
	}
	if !strings.Contains(out, EnvProfile) || !strings.Contains(out, "micro") {
		t.Errorf("Main output %q does not name %s and the rejected value", out, EnvProfile)
	}
}

// TestMainFailsThePackageWhenAStackCannotStart asserts the other package-level failure path: a
// stack that cannot boot fails the run once, naming the stack.
func TestMainFailsThePackageWhenAStackCannotStart(t *testing.T) {
	if os.Getenv(childBadStack) == "1" {
		os.Exit(Main(nil, Stack{Name: "mongodb", Start: func(context.Context) (func(), error) {
			return nil, errors.New("docker is not reachable")
		}}))
	}
	t.Setenv(childBadStack, "1")
	code, out := runSelf(t)
	if code != 1 {
		t.Errorf("Main with a stack that cannot start exited %d, want 1; output:\n%s", code, out)
	}
	if !strings.Contains(out, "mongodb") || !strings.Contains(out, "docker is not reachable") {
		t.Errorf("Main output %q does not name the stack and its reason", out)
	}
}

// TestMainReleasesWhatItStarted asserts the other half of Main's contract: a stack that did
// start is released when a later stack fails, so a half-started run does not leak the containers
// it already booted. Main is handed a nil *testing.M because this path returns before it is used.
func TestMainReleasesWhatItStarted(t *testing.T) {
	if os.Getenv("FLEETOPS_TESTEXEC_MAIN_RELEASE") == "1" {
		os.Exit(Main(nil,
			Stack{Name: "first", Start: func(context.Context) (func(), error) {
				return func() { _, _ = os.Stdout.WriteString("released the first stack\n") }, nil
			}},
			Stack{Name: "second", Start: func(context.Context) (func(), error) {
				return nil, errors.New("docker is not reachable")
			}},
		))
	}
	t.Setenv("FLEETOPS_TESTEXEC_MAIN_RELEASE", "1")
	code, out := runSelf(t)
	if code != 1 {
		t.Fatalf("Main exited %d, want 1; output:\n%s", code, out)
	}
	if !strings.Contains(out, "released the first stack") {
		t.Errorf("Main output %q does not report the first stack's release", out)
	}
}

// TestReleaseAllReleasesInReverseOrder pins the order: the last stack started is the first one
// released, because a stack may depend on one started before it.
func TestReleaseAllReleasesInReverseOrder(t *testing.T) {
	t.Parallel()
	var released []string
	releaseAll([]func(){
		func() { released = append(released, "first") },
		func() { released = append(released, "second") },
		func() { released = append(released, "third") },
	})
	want := []string{"third", "second", "first"}
	if diff := cmp.Diff(want, released); diff != "" {
		t.Errorf("release order mismatch (-want +got):\n%s", diff)
	}
}

// runSelf re-runs this test binary with the child marker set, returning its exit code and the
// combined output, so Main's process-level behavior is observed rather than described.
func runSelf(t *testing.T) (int, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), string(out)
	}
	if err != nil {
		t.Fatalf("re-run %s: %v\n%s", os.Args[0], err, out)
	}
	return 0, string(out)
}
