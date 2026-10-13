package speccoverage

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mutationCase is one recorded spot check of the ledger's central claim. The scenario it backs
// says that a behavior a scenario describes, removed or reversed in the implementation, fails the
// test the ledger records as backing that scenario. The mapping is hand-maintained, and this is
// the one claim a parser cannot check, so the change records a per-scenario mutation check instead
// of an exhaustive harness: the case names the ledger row, the file holding the behavior, the
// reversal applied to it, and the test that must fail.
type mutationCase struct {
	capability string
	scenario   string
	// file is the repository-relative file holding the behavior.
	file string
	// what is the text carrying the behavior and after is its reversal.
	what  string
	after string
	// where is a marker that follows the behavior in the file, so the case reverses the last
	// occurrence of what before where rather than every occurrence in the file.
	where string
	// test is the test the ledger records as backing the scenario, testFile the file the ledger
	// records it in, and pkg the package to run.
	test     string
	testFile string
	pkg      string
}

// reversal applies the case's edit to src, failing when the anchor does not identify exactly one
// occurrence: a spot check whose behavior moved is a check that stopped checking anything.
func (c mutationCase) reversal(t *testing.T, src string) string {
	t.Helper()

	where := strings.Index(src, c.where)
	if where < 0 {
		t.Fatalf("%s: %q not found: the behavior the case reverses moved or was removed", c.file, c.where)
	}
	at := strings.LastIndex(src[:where], c.what)
	if at < 0 {
		t.Fatalf("%s: %q does not appear before %q: the case no longer identifies the behavior",
			c.file, c.what, c.where)
	}
	if strings.Count(src[:where], c.what) != 1 {
		t.Fatalf("%s: %q appears %d times before %q, want exactly one",
			c.file, c.what, strings.Count(src[:where], c.what), c.where)
	}
	return src[:at] + c.after + src[at+len(c.what):]
}

// TestRemovedBehaviorFailsItsBackingTest backs "A removed behavior fails its backing test": it
// reverses one recorded behavior in a build overlay — the working tree is never touched — and
// shows the test the ledger records for that scenario passes before the reversal and fails after
// it. The overlay is what makes this safe to run on the normal unit path: only the compilation of
// this one subprocess sees the reversed file.
func TestRemovedBehaviorFailsItsBackingTest(t *testing.T) {
	t.Parallel()

	cases := []mutationCase{{
		capability: "rollout-api",
		scenario:   "An unknown rollout is not found",
		file:       "internal/rolloutapi/handler.go",
		// The state handler answers 404 for a rollout whose execution does not exist; a 500
		// is the reversal — the endpoint would then read as broken rather than as missing.
		what:     "http.StatusNotFound",
		after:    "http.StatusInternalServerError",
		where:    `h.fail(w, r, "read rollout state", err)`,
		test:     "TestHandlerState",
		testFile: "internal/rolloutapi/handler_test.go",
		pkg:      "./internal/rolloutapi/",
	}, {
		capability: "test-execution-profiles",
		scenario:   "A narrowed run asserts the same behavior",
		file:       "internal/testexec/profile.go",
		// The weak profile's fleet cap is what makes a narrowed run narrower; raising it past
		// the default profile's fleet size is the reversal — the profile would narrow no work.
		what:     "weakFleetSize = 100",
		after:    "weakFleetSize = 2000",
		where:    "weakWaitMultiplier = 0.5",
		test:     "TestNarrowedRunAssertsTheSameBehavior",
		testFile: "internal/testexec/narrowing_test.go",
		pkg:      "./internal/testexec/",
	}}
	for _, tc := range cases {
		t.Run(tc.capability+"/"+tc.scenario, func(t *testing.T) {
			t.Parallel()

			recordedTest, recordedFile := recordedBacker(t, tc)
			if recordedTest != tc.test || recordedFile != tc.testFile {
				t.Fatalf("ledger records %s in %s for %q; the case reverses %s and runs %s from %s, "+
					"so the check would prove nothing",
					recordedTest, recordedFile, tc.scenario, tc.file, tc.test, tc.testFile)
			}

			src, err := os.ReadFile(filepath.Join(repoRoot, tc.file))
			if err != nil {
				t.Fatalf("read %s: %v", tc.file, err)
			}
			mutated := tc.reversal(t, string(src))
			if mutated == string(src) {
				t.Fatal("the reversal changed nothing")
			}

			dir := t.TempDir()
			mutatedPath := filepath.Join(dir, filepath.Base(tc.file))
			if err := os.WriteFile(mutatedPath, []byte(mutated), 0o600); err != nil {
				t.Fatalf("write the reversed file: %v", err)
			}
			originalPath, err := filepath.Abs(filepath.Join(repoRoot, tc.file))
			if err != nil {
				t.Fatalf("resolve %s: %v", tc.file, err)
			}
			overlayPath := filepath.Join(dir, "overlay.json")
			overlay, err := json.Marshal(map[string]any{
				"Replace": map[string]string{originalPath: mutatedPath},
			})
			if err != nil {
				t.Fatalf("encode the overlay: %v", err)
			}
			if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
				t.Fatalf("write the overlay: %v", err)
			}

			// The control run is what makes the reversal evidence: the recorded test passes
			// against the real behavior, and fails only once that behavior is reversed.
			base := []string{"test", "-count=1", "-run", "^" + tc.test + "$", tc.pkg}
			if out, err := goTest(t, base...); err != nil {
				t.Fatalf("the recorded test %s does not pass before the reversal, so it cannot back %q: %v\n%s",
					tc.test, tc.scenario, err, out)
			}
			out, err := goTest(t, append(base, "-overlay", overlayPath)...)
			if err == nil {
				t.Fatalf("the recorded test %s passed with %q reversed, so it does not back %q:\n%s",
					tc.test, tc.what, tc.scenario, out)
			}
			if !strings.Contains(out, tc.test) {
				t.Errorf("the post-reversal run failed without naming %s, so the failure may not be the test's:\n%s",
					tc.test, out)
			}
		})
	}
}

// recordedBacker returns the test and file the ledger records for the case's scenario, failing
// when the row is absent — a mutation case whose scenario left the ledger proves nothing.
func recordedBacker(t *testing.T, tc mutationCase) (test, file string) {
	t.Helper()

	led, err := loadLedger(os.DirFS(repoRoot), ledgerPath)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	for _, e := range led.entries {
		if e.capability == tc.capability && e.scenario == tc.scenario {
			return e.test, e.file
		}
	}
	t.Fatalf("the ledger records no entry for %s | %s", tc.capability, tc.scenario)
	return "", ""
}

// goTest runs one "go test" invocation from the repository root and returns its combined output.
func goTest(t *testing.T, args ...string) (string, error) {
	t.Helper()

	cmd := exec.Command("go", args...)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("go %s: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}
