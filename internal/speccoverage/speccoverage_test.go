package speccoverage

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
)

// allowUnexported returns the cmp option that lets comparisons include the
// package's unexported record types.
func allowUnexported() cmp.Option {
	return cmp.AllowUnexported(spec{}, scenario{}, problem{}, ledger{}, entry{}, exemption{})
}

// goFile builds an in-memory Go file declaring the named functions.
func goFile(names ...string) *fstest.MapFile {
	var b strings.Builder
	b.WriteString("package alpha\n\nimport \"testing\"\n\n")
	for _, name := range names {
		fmt.Fprintf(&b, "func %s(t *testing.T) {}\n\n", name)
	}
	return &fstest.MapFile{Data: []byte(b.String())}
}

// testSpec builds a one-requirement spec declaring the named scenarios.
func testSpec(capability string, names ...string) spec {
	sp := spec{capability: capability, requirements: []string{"Requirement one"}}
	for _, name := range names {
		sp.scenarios = append(sp.scenarios, scenario{
			capability:  capability,
			requirement: "Requirement one",
			name:        name,
		})
	}
	return sp
}

// TestCoverageLedger checks the repository's ledger against its capability
// specs. It fails, listing every gap, while a scenario has neither a backing
// test nor an exemption.
func TestCoverageLedger(t *testing.T) {
	t.Parallel()

	fsys := os.DirFS(repoRoot)
	specs, err := loadScenarios(fsys, specsDir, deltasDir)
	if err != nil {
		t.Fatalf("load scenarios: %v", err)
	}
	led, err := loadLedger(fsys, ledgerPath)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}

	problems := check(specs, led, fsys)
	if len(problems) == 0 {
		return
	}
	t.Errorf("scenario coverage is incomplete:\n\n%s", report(specs, led, problems))
}

// TestLoadScenariosFindsTheBaseline guards the spec walker and merger against
// silently finding nothing: the live tree must still hold at least the counts
// the ledger was written against, both in the baseline and in the effective set
// the baseline forms with the change's deltas.
func TestLoadScenariosFindsTheBaseline(t *testing.T) {
	t.Parallel()

	// Floors when the ledger was written. The specs only grow, so a count below
	// one of these means the walker or the parser lost scenarios.
	const (
		wantCapabilities   = 37
		wantRequirements   = 163
		wantScenarios      = 534
		wantDeltaScenarios = 594
		wantDeltaReqs      = 184
	)
	fsys := os.DirFS(repoRoot)
	baseline, err := loadSpecs(fsys, specsDir)
	if err != nil {
		t.Fatalf("loadSpecs: %v", err)
	}
	checkScenarioSet(t, baseline, wantCapabilities, wantRequirements, wantScenarios, "baseline")

	effective, err := loadScenarios(fsys, specsDir, deltasDir)
	if err != nil {
		t.Fatalf("loadScenarios: %v", err)
	}
	checkScenarioSet(t, effective, wantCapabilities+5, wantDeltaReqs, wantDeltaScenarios, "union")
	if len(effective) < len(baseline) {
		t.Errorf("the union has %d capabilities, fewer than the %d in the baseline",
			len(effective), len(baseline))
	}
	for _, want := range []string{"agent-emulator", "rollout-workflow", "temporal-worker"} {
		if !hasCapability(effective, want) {
			t.Errorf("capability %s was not found under %s", want, specsDir)
		}
	}
}

// checkScenarioSet asserts a parsed scenario set's structure and its floors.
func checkScenarioSet(t *testing.T, specs []spec, wantCapabilities, wantRequirements,
	wantScenarios int, label string) {
	t.Helper()

	if len(specs) < wantCapabilities {
		t.Errorf("%s: found %d capabilities, want at least %d", label, len(specs), wantCapabilities)
	}
	seenCaps := make(map[string]bool, len(specs))
	requirements, scenarios := 0, 0
	for _, sp := range specs {
		if sp.capability == "" || seenCaps[sp.capability] {
			t.Errorf("%s: capability %q is empty or declared twice", label, sp.capability)
		}
		seenCaps[sp.capability] = true
		requirements += len(sp.requirements)
		scenarios += len(sp.scenarios)
		if len(sp.scenarios) == 0 {
			t.Errorf("%s: capability %s declares no scenarios", label, sp.capability)
		}
		declared := make(map[string]bool, len(sp.requirements))
		for _, r := range sp.requirements {
			declared[r] = true
		}
		for _, sc := range sp.scenarios {
			if sc.capability != sp.capability {
				t.Errorf("%s: scenario %q belongs to %s, not %s",
					label, sc.name, sc.capability, sp.capability)
			}
			if !declared[sc.requirement] {
				t.Errorf("%s: scenario %q is under undeclared requirement %q",
					label, sc.name, sc.requirement)
			}
		}
	}
	if requirements < wantRequirements {
		t.Errorf("%s: found %d requirements, want at least %d", label, requirements, wantRequirements)
	}
	if scenarios < wantScenarios {
		t.Errorf("%s: found %d scenarios, want at least %d", label, scenarios, wantScenarios)
	}
}

// hasCapability reports whether the set holds a capability with that name.
func hasCapability(specs []spec, capability string) bool {
	for _, sp := range specs {
		if sp.capability == capability {
			return true
		}
	}
	return false
}

// TestParseSpec covers the spec parser's accepted shape and its rejections.
func TestParseSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		want    spec
		wantErr string
	}{
		{
			name: "requirements and scenarios in file order",
			src: "# One Specification\n\n## Requirements\n\n" +
				"### Requirement: First thing\n\n#### Scenario: A\n- **WHEN** x\n\n" +
				"### Requirement: Second thing\n\n#### Scenario: B\n- **WHEN** y\n",
			want: spec{
				capability:   "one",
				requirements: []string{"First thing", "Second thing"},
				scenarios: []scenario{
					{capability: "one", requirement: "First thing", name: "A"},
					{capability: "one", requirement: "Second thing", name: "B"},
				},
			},
		},
		{
			name:    "scenario before any requirement",
			src:     "#### Scenario: Orphan\n- **WHEN** x\n",
			wantErr: "is not under a requirement",
		},
		{
			name:    "scenario without a name",
			src:     "### Requirement: One\n\n#### Scenario:\n",
			wantErr: "scenario has no name",
		},
		{
			name:    "requirement without a name",
			src:     "### Requirement:\n\n#### Scenario: A\n",
			wantErr: "requirement has no name",
		},
		{
			name:    "no requirements",
			src:     "# Spec\n\nnothing here\n",
			wantErr: "declares no requirements",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseSpec("one", tc.src)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseSpec() = %+v, want error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseSpec() error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSpec() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, allowUnexported()); diff != "" {
				t.Errorf("parseSpec() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestLoadScenariosMergesDeltas covers the effective set: a capability declared
// by both the baseline and a delta keeps one copy of a scenario they share and
// gains the delta's own requirement and scenario, a delta-only capability is
// added, and a delta directory that does not exist is not an error.
func TestLoadScenariosMergesDeltas(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"specs/alpha/spec.md": {Data: []byte("### Requirement: One\n\n" +
			"#### Scenario: Shared\n- x\n\n#### Scenario: Baseline only\n- y\n")},
		"specs/zeta/spec.md": {Data: []byte("### Requirement: Zeta\n\n" +
			"#### Scenario: Zeta scenario\n- z\n")},
		"deltas/alpha/spec.md": {Data: []byte("### Requirement: One\n\n" +
			"#### Scenario: Shared\n- x\n\n### Requirement: Two\n\n" +
			"#### Scenario: Delta only\n- w\n")},
		"deltas/beta/spec.md": {Data: []byte("### Requirement: Beta\n\n" +
			"#### Scenario: Beta scenario\n- b\n")},
	}

	got, err := loadScenarios(fsys, "specs", "deltas")
	if err != nil {
		t.Fatalf("loadScenarios: %v", err)
	}
	want := []spec{
		{
			capability:   "alpha",
			requirements: []string{"One", "Two"},
			scenarios: []scenario{
				{capability: "alpha", requirement: "One", name: "Shared"},
				{capability: "alpha", requirement: "One", name: "Baseline only"},
				{capability: "alpha", requirement: "Two", name: "Delta only"},
			},
		},
		{
			capability:   "zeta",
			requirements: []string{"Zeta"},
			scenarios:    []scenario{{capability: "zeta", requirement: "Zeta", name: "Zeta scenario"}},
		},
		{
			capability:   "beta",
			requirements: []string{"Beta"},
			scenarios:    []scenario{{capability: "beta", requirement: "Beta", name: "Beta scenario"}},
		},
	}
	if diff := cmp.Diff(want, got, allowUnexported()); diff != "" {
		t.Errorf("loadScenarios() mismatch (-want +got):\n%s", diff)
	}

	absent, err := loadScenarios(fsys, "specs", "absent")
	if err != nil {
		t.Fatalf("loadScenarios with an absent delta directory: %v", err)
	}
	if len(absent) != 2 {
		t.Errorf("loadScenarios with an absent delta directory = %d capabilities, want 2",
			len(absent))
	}
}

// TestParseLedger covers the ledger parser's accepted shape and its rejections.
func TestParseLedger(t *testing.T) {
	t.Parallel()

	const valid = `# Coverage

The ledger records which test backs each capability scenario.

## Entries

| Capability | Scenario | Test | File |
| --- | --- | --- | --- |
| alpha | A | TestA | internal/alpha/alpha_test.go |

## Exemptions

| Capability | Scenario | Reason | What would verify it |
| --- | --- | --- | --- |
| beta | B | needs a running stack | a worker against a live server |
`

	tests := []struct {
		name    string
		src     string
		want    *ledger
		wantErr string
	}{
		{
			name: "valid ledger",
			src:  valid,
			want: &ledger{
				entries: []entry{{
					capability: "alpha",
					scenario:   "A",
					test:       "TestA",
					file:       "internal/alpha/alpha_test.go",
				}},
				exemptions: []exemption{{
					capability:  "beta",
					scenario:    "B",
					reason:      "needs a running stack",
					wouldVerify: "a worker against a live server",
				}},
			},
		},
		{
			name: "empty reason parses and is reported by check",
			src: "## Entries\n\n| Capability | Scenario | Test | File |\n| --- | --- | --- | --- |\n" +
				"\n## Exemptions\n\n| Capability | Scenario | Reason | What would verify it |\n" +
				"| --- | --- | --- | --- |\n| beta | B |  | a live server |\n",
			want: &ledger{exemptions: []exemption{{
				capability: "beta", scenario: "B", reason: "", wouldVerify: "a live server",
			}}},
		},
		{
			name:    "missing exemptions table",
			src:     "## Entries\n\n| Capability | Scenario | Test | File |\n| --- | --- | --- | --- |\n",
			wantErr: "missing the Exemptions table",
		},
		{
			name:    "missing entries table",
			src:     "## Exemptions\n\n| Capability | Scenario | Reason | What would verify it |\n",
			wantErr: "missing the Entries table",
		},
		{
			name: "row with the wrong number of columns",
			src: "## Entries\n\n| Capability | Scenario | Test | File |\n| --- | --- | --- | --- |\n" +
				"| alpha | A | TestA |\n\n## Exemptions\n\n" +
				"| Capability | Scenario | Reason | What would verify it |\n| --- | --- | --- | --- |\n",
			wantErr: "table row has 3 columns, want 4",
		},
		{
			name: "entry with an empty cell",
			src: "## Entries\n\n| Capability | Scenario | Test | File |\n| --- | --- | --- | --- |\n" +
				"| alpha | A | TestA |  |\n\n## Exemptions\n\n" +
				"| Capability | Scenario | Reason | What would verify it |\n| --- | --- | --- | --- |\n",
			wantErr: "entry has an empty file cell",
		},
		{
			name: "exemption with an empty scenario cell",
			src: "## Entries\n\n| Capability | Scenario | Test | File |\n| --- | --- | --- | --- |\n" +
				"\n## Exemptions\n\n| Capability | Scenario | Reason | What would verify it |\n" +
				"| --- | --- | --- | --- |\n| beta |  | a stack | a live server |\n",
			wantErr: "exemption has an empty scenario cell",
		},
		{
			name: "entry row before the table header",
			src: "## Entries\n\n| alpha | A | TestA | internal/alpha/alpha_test.go |\n\n" +
				"## Exemptions\n\n| Capability | Scenario | Reason | What would verify it |\n" +
				"| --- | --- | --- | --- |\n",
			wantErr: "entry row before the table header",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseLedger(tc.src)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseLedger() = %+v, want error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseLedger() error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLedger() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, allowUnexported()); diff != "" {
				t.Errorf("parseLedger() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestCheck covers every way the checker accepts or rejects a ledger.
func TestCheck(t *testing.T) {
	t.Parallel()

	alpha := testSpec("alpha", "A", "B", "C")
	file := "internal/alpha/alpha_test.go"
	files := fstest.MapFS{file: goFile("TestA", "helper")}

	tests := []struct {
		name  string
		specs []spec
		led   *ledger
		want  []problem
	}{
		{
			name:  "entry naming a test in the file passes",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "A", test: "TestA", file: file},
			}},
		},
		{
			name:  "exemption with a reason and verification passes",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{exemptions: []exemption{
				{capability: "alpha", scenario: "A", reason: "needs a stack",
					wouldVerify: "a worker against a live server"},
			}},
		},
		{
			name:  "scenario with neither an entry nor an exemption is unbacked",
			specs: []spec{testSpec("alpha", "A")},
			led:   &ledger{},
			want: []problem{
				{kind: kindUnbacked, capability: "alpha", scenario: "A"},
			},
		},
		{
			name:  "every unbacked scenario of every capability is reported",
			specs: []spec{alpha},
			led:   &ledger{},
			want: []problem{
				{kind: kindUnbacked, capability: "alpha", scenario: "A"},
				{kind: kindUnbacked, capability: "alpha", scenario: "B"},
				{kind: kindUnbacked, capability: "alpha", scenario: "C"},
			},
		},
		{
			name:  "entry naming a scenario the specs do not declare",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "Z", test: "TestA", file: file},
			}},
			want: []problem{
				{kind: kindUnbacked, capability: "alpha", scenario: "A"},
				{kind: kindUnknown, capability: "alpha", scenario: "Z",
					detail: "no such scenario in openspec/specs"},
			},
		},
		{
			name:  "exemption naming a capability the specs do not declare",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{exemptions: []exemption{
				{capability: "beta", scenario: "A", reason: "r", wouldVerify: "v"},
			}},
			want: []problem{
				{kind: kindUnbacked, capability: "alpha", scenario: "A"},
				{kind: kindUnknown, capability: "beta", scenario: "A",
					detail: "no such scenario in openspec/specs"},
			},
		},
		{
			name:  "entry whose test is not declared by the named file",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "A", test: "TestGone", file: file},
			}},
			want: []problem{
				{kind: kindUntested, capability: "alpha", scenario: "A",
					detail: file + " does not declare TestGone"},
			},
		},
		{
			name:  "entry naming a function that is not a test",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "A", test: "helper", file: file},
			}},
			want: []problem{
				{kind: kindUntested, capability: "alpha", scenario: "A",
					detail: "helper is not a test function"},
			},
		},
		{
			name:  "entry naming a file that does not exist",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "A", test: "TestA",
					file: "internal/alpha/missing_test.go"},
			}},
			want: []problem{
				{kind: kindUntested, capability: "alpha", scenario: "A",
					detail: "read internal/alpha/missing_test.go: open internal/alpha/missing_test.go: " +
						"file does not exist"},
			},
		},
		{
			name:  "exemption without a reason or a verification",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{exemptions: []exemption{
				{capability: "alpha", scenario: "A", reason: "", wouldVerify: ""},
			}},
			want: []problem{
				{kind: kindThinExemption, capability: "alpha", scenario: "A",
					detail: "missing reason and what would verify it"},
			},
		},
		{
			name:  "scenario recorded by two entries",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{entries: []entry{
				{capability: "alpha", scenario: "A", test: "TestA", file: file},
				{capability: "alpha", scenario: "A", test: "TestA", file: file},
			}},
			want: []problem{
				{kind: kindDuplicate, capability: "alpha", scenario: "A",
					detail: "recorded by more than one entry"},
			},
		},
		{
			name:  "scenario both entered and exempted",
			specs: []spec{testSpec("alpha", "A")},
			led: &ledger{
				entries: []entry{{capability: "alpha", scenario: "A", test: "TestA", file: file}},
				exemptions: []exemption{{capability: "alpha", scenario: "A", reason: "r",
					wouldVerify: "v"}},
			},
			want: []problem{
				{kind: kindDuplicate, capability: "alpha", scenario: "A",
					detail: "both an entry and an exemption"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := check(tc.specs, tc.led, files)
			if diff := cmp.Diff(tc.want, got, allowUnexported()); diff != "" {
				t.Errorf("check() problems (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReportListsEveryProblemKind checks the report groups problems under a
// heading and counts them, so the failure a reader sees is a readable list.
func TestReportListsEveryProblemKind(t *testing.T) {
	t.Parallel()

	specs := []spec{testSpec("alpha", "A", "B")}
	problems := []problem{
		{kind: kindUnbacked, capability: "alpha", scenario: "A"},
		{kind: kindUntested, capability: "alpha", scenario: "B",
			detail: "internal/alpha/alpha_test.go does not declare TestGone"},
	}
	got := report(specs, &ledger{}, problems)

	for _, want := range []string{
		"openspec/coverage.md covers 2 scenarios in 1 capabilities: 0 entries, 0 exemptions, 1 unbacked.",
		kindUnbacked.heading() + " (1):",
		"alpha: A",
		kindUntested.heading() + " (1):",
		"alpha: B (internal/alpha/alpha_test.go does not declare TestGone)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report() is missing %q:\n%s", want, got)
		}
	}
}
