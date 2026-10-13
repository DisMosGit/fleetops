// Package speccoverage checks the recorded scenario-to-test coverage ledger
// (openspec/coverage.md) against the capability specs under openspec/specs.
//
// The ledger is a hand-maintained markdown table that maps every capability
// scenario to the test that backs it, plus the scenarios that are exempt with
// the reason and what would verify them. TestCoverageLedger fails when a
// scenario has neither an entry nor an exemption, when the ledger names a
// scenario the specs do not declare, when an entry's test function is not in
// the file it names, and when an exemption names no reason or no verification.
package speccoverage

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"sort"
	"strings"
)

// Locations the checker reads, relative to the repository root. specsDir holds
// the capability baseline; deltasDir holds the active change's delta specs,
// whose scenarios are not in the baseline until the change is archived. The
// effective scenario set is the union of the two, because tasks add ledger rows
// for delta scenarios before archive.
const (
	specsDir   = "openspec/specs"
	deltasDir  = "openspec/changes/add-missing-feature-coverage/specs"
	ledgerPath = "openspec/coverage.md"
)

// repoRoot is the repository root relative to this package's directory, so the
// check runs against the working tree wherever "go test" is invoked from.
const repoRoot = "../.."

// spec is one parsed capability spec: the requirements it declares and the
// scenarios under them, in the order the file states them.
type spec struct {
	capability   string
	requirements []string
	scenarios    []scenario
}

// scenario is one scenario declared under a requirement in a capability spec.
type scenario struct {
	capability  string
	requirement string
	name        string
}

// entry is one row of the ledger's entries table.
type entry struct {
	capability string
	scenario   string
	test       string
	file       string
}

// exemption is one row of the ledger's exemptions table.
type exemption struct {
	capability  string
	scenario    string
	reason      string
	wouldVerify string
}

// ledger is the parsed contents of openspec/coverage.md.
type ledger struct {
	entries    []entry
	exemptions []exemption
}

// problemKind labels a class of coverage problem so the report can group it.
type problemKind int

const (
	// kindUnbacked marks a scenario with no entry and no exemption.
	kindUnbacked problemKind = iota
	// kindUnknown marks a ledger row naming a scenario no spec declares.
	kindUnknown
	// kindUntested marks an entry whose test function is not in the named file.
	kindUntested
	// kindThinExemption marks an exemption missing its reason or verification.
	kindThinExemption
	// kindDuplicate marks a scenario recorded more than once.
	kindDuplicate
)

// heading returns the report heading for a problem kind.
func (k problemKind) heading() string {
	switch k {
	case kindUnbacked:
		return "scenarios with no backing test and no exemption"
	case kindUnknown:
		return "ledger rows naming a scenario the specs do not declare"
	case kindUntested:
		return "entries whose test function is not in the file they name"
	case kindThinExemption:
		return "exemptions missing a reason or what would verify them"
	case kindDuplicate:
		return "scenarios recorded more than once"
	default:
		return "unknown problems"
	}
}

// problem is one disagreement between the specs and the ledger.
type problem struct {
	kind       problemKind
	capability string
	scenario   string
	detail     string
}

// String renders the problem as "capability: scenario" plus any detail.
func (p problem) String() string {
	if p.detail == "" {
		return fmt.Sprintf("%s: %s", p.capability, p.scenario)
	}
	return fmt.Sprintf("%s: %s (%s)", p.capability, p.scenario, p.detail)
}

// key identifies a scenario by capability and name.
func key(capability, name string) string {
	return capability + "\x00" + name
}

// loadSpecs reads every <capability>/spec.md under dir, in capability order.
// A directory that does not exist yields no specs rather than an error, so the
// checker keeps working once the change's deltas are archived into the baseline.
func loadSpecs(fsys fs.FS, dir string) ([]spec, error) {
	if _, err := fs.Stat(fsys, dir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", dir, err)
	}
	var dirs []string
	err := fs.WalkDir(fsys, dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "spec.md" {
			return nil
		}
		dirs = append(dirs, path.Dir(p))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", dir, err)
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("no capability specs found under %s", dir)
	}
	sort.Strings(dirs)

	specs := make([]spec, 0, len(dirs))
	for _, capabilityDir := range dirs {
		file := path.Join(capabilityDir, "spec.md")
		src, err := fs.ReadFile(fsys, file)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", file, err)
		}
		parsed, err := parseSpec(path.Base(capabilityDir), string(src))
		if err != nil {
			return nil, err
		}
		specs = append(specs, parsed)
	}
	return specs, nil
}

// loadScenarios reads the specs under each directory in turn and returns one
// spec per capability, deduplicated by scenario name so a scenario declared by
// both the baseline and a delta is counted once. Capabilities keep the order
// their directory is read in: baseline first, delta-only capabilities after.
func loadScenarios(fsys fs.FS, dirs ...string) ([]spec, error) {
	merged := make(map[string]*spec)
	var order []string
	for _, dir := range dirs {
		specs, err := loadSpecs(fsys, dir)
		if err != nil {
			return nil, err
		}
		for _, sp := range specs {
			existing, ok := merged[sp.capability]
			if !ok {
				copied := sp
				merged[sp.capability] = &copied
				order = append(order, sp.capability)
				continue
			}
			mergeSpec(existing, sp)
		}
	}
	out := make([]spec, 0, len(order))
	for _, capability := range order {
		out = append(out, *merged[capability])
	}
	return out, nil
}

// mergeSpec appends the requirements and scenarios delta declares that target
// does not already declare. A name declared twice is kept once.
func mergeSpec(target *spec, delta spec) {
	requirements := make(map[string]bool, len(target.requirements))
	for _, requirement := range target.requirements {
		requirements[requirement] = true
	}
	for _, requirement := range delta.requirements {
		if requirements[requirement] {
			continue
		}
		requirements[requirement] = true
		target.requirements = append(target.requirements, requirement)
	}

	scenarios := make(map[string]bool, len(target.scenarios))
	for _, sc := range target.scenarios {
		scenarios[sc.name] = true
	}
	for _, sc := range delta.scenarios {
		if scenarios[sc.name] {
			continue
		}
		scenarios[sc.name] = true
		target.scenarios = append(target.scenarios, sc)
	}
}

// parseSpec extracts the requirements and scenarios one capability spec declares.
func parseSpec(capability, src string) (spec, error) {
	out := spec{capability: capability}
	var requirement string
	for i, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "### Requirement:"):
			requirement = strings.TrimSpace(strings.TrimPrefix(line, "### Requirement:"))
			if requirement == "" {
				return spec{}, fmt.Errorf("%s line %d: requirement has no name", capability, i+1)
			}
			out.requirements = append(out.requirements, requirement)
		case strings.HasPrefix(line, "#### Scenario:"):
			name := strings.TrimSpace(strings.TrimPrefix(line, "#### Scenario:"))
			if name == "" {
				return spec{}, fmt.Errorf("%s line %d: scenario has no name", capability, i+1)
			}
			if requirement == "" {
				return spec{}, fmt.Errorf("%s line %d: scenario %q is not under a requirement",
					capability, i+1, name)
			}
			out.scenarios = append(out.scenarios, scenario{
				capability:  capability,
				requirement: requirement,
				name:        name,
			})
		}
	}
	if len(out.requirements) == 0 {
		return spec{}, fmt.Errorf("%s declares no requirements", capability)
	}
	return out, nil
}

// loadLedger reads and parses the coverage ledger at path.
func loadLedger(fsys fs.FS, path string) (*ledger, error) {
	src, err := fs.ReadFile(fsys, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	parsed, err := parseLedger(string(src))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return parsed, nil
}

// entryColumns returns the entries table's expected header.
func entryColumns() []string {
	return []string{"Capability", "Scenario", "Test", "File"}
}

// exemptionColumns returns the exemptions table's expected header.
func exemptionColumns() []string {
	return []string{"Capability", "Scenario", "Reason", "What would verify it"}
}

// parseLedger parses the ledger's entries and exemptions tables. It rejects a
// ledger whose tables are missing, whose rows do not match the four columns, or
// whose entries omit any of their four cells.
func parseLedger(src string) (*ledger, error) {
	out := &ledger{}
	var section string
	var entriesHeader, exemptionsHeader bool
	for i, raw := range strings.Split(src, "\n") {
		line := strings.TrimSpace(raw)
		lineNo := i + 1
		if name, ok := strings.CutPrefix(line, "## "); ok {
			switch strings.TrimSpace(name) {
			case "Entries":
				section = "entries"
			case "Exemptions":
				section = "exemptions"
			default:
				section = ""
			}
			continue
		}
		if section == "" || !strings.HasPrefix(line, "|") {
			continue
		}
		cells := tableRow(line)
		want := entryColumns()
		if section == "exemptions" {
			want = exemptionColumns()
		}
		if isSeparator(cells) {
			continue
		}
		if isHeader(cells, want) {
			if section == "entries" {
				entriesHeader = true
			} else {
				exemptionsHeader = true
			}
			continue
		}
		if len(cells) != len(want) {
			return nil, fmt.Errorf("line %d: table row has %d columns, want %d",
				lineNo, len(cells), len(want))
		}
		if section == "entries" {
			if !entriesHeader {
				return nil, fmt.Errorf("line %d: entry row before the table header", lineNo)
			}
			e, err := newEntry(cells, lineNo)
			if err != nil {
				return nil, err
			}
			out.entries = append(out.entries, e)
			continue
		}
		if !exemptionsHeader {
			return nil, fmt.Errorf("line %d: exemption row before the table header", lineNo)
		}
		x, err := newExemption(cells, lineNo)
		if err != nil {
			return nil, err
		}
		out.exemptions = append(out.exemptions, x)
	}
	if !entriesHeader {
		return nil, fmt.Errorf("missing the Entries table")
	}
	if !exemptionsHeader {
		return nil, fmt.Errorf("missing the Exemptions table")
	}
	return out, nil
}

// tableRow splits a markdown table row into its trimmed cells.
func tableRow(line string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line), "|"), "|")
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

// isSeparator reports whether cells form the "| --- | --- |" header separator.
func isSeparator(cells []string) bool {
	if len(cells) == 0 {
		return false
	}
	for _, cell := range cells {
		if len(cell) < 3 || strings.Trim(cell, "-:") != "" {
			return false
		}
	}
	return true
}

// isHeader reports whether cells are the expected table header.
func isHeader(cells, want []string) bool {
	if len(cells) != len(want) {
		return false
	}
	for i := range want {
		if !strings.EqualFold(cells[i], want[i]) {
			return false
		}
	}
	return true
}

// newEntry validates and builds an entry row.
func newEntry(cells []string, lineNo int) (entry, error) {
	e := entry{capability: cells[0], scenario: cells[1], test: cells[2], file: cells[3]}
	for i, cell := range cells {
		if cell == "" {
			return entry{}, fmt.Errorf("line %d: entry has an empty %s cell",
				lineNo, strings.ToLower(entryColumns()[i]))
		}
	}
	return e, nil
}

// newExemption validates and builds an exemption row. An empty reason or
// verification is reported by check, not rejected here, so the report names the
// scenario rather than the file failing to parse.
func newExemption(cells []string, lineNo int) (exemption, error) {
	x := exemption{
		capability:  cells[0],
		scenario:    cells[1],
		reason:      cells[2],
		wouldVerify: cells[3],
	}
	for i := 0; i < 2; i++ {
		if cells[i] == "" {
			return exemption{}, fmt.Errorf("line %d: exemption has an empty %s cell",
				lineNo, strings.ToLower(exemptionColumns()[i]))
		}
	}
	return x, nil
}

// check compares the declared scenarios with the ledger and returns every
// problem it finds, so the report can list them all rather than the first.
func check(specs []spec, led *ledger, fsys fs.FS) []problem {
	declared := make(map[string]bool)
	for _, sp := range specs {
		for _, sc := range sp.scenarios {
			declared[key(sc.capability, sc.name)] = true
		}
	}

	var problems []problem
	covered := make(map[string]bool)
	attempted := make(map[string]bool)
	funcs := make(map[string]map[string]bool)
	for _, e := range led.entries {
		k := key(e.capability, e.scenario)
		if !declared[k] {
			problems = append(problems, problem{kind: kindUnknown, capability: e.capability,
				scenario: e.scenario, detail: "no such scenario in " + specsDir})
			continue
		}
		if attempted[k] {
			problems = append(problems, problem{kind: kindDuplicate, capability: e.capability,
				scenario: e.scenario, detail: "recorded by more than one entry"})
			continue
		}
		attempted[k] = true
		if err := findTest(fsys, funcs, e.file, e.test); err != nil {
			problems = append(problems, problem{kind: kindUntested, capability: e.capability,
				scenario: e.scenario, detail: err.Error()})
			continue
		}
		covered[k] = true
	}

	exempted := make(map[string]bool)
	for _, x := range led.exemptions {
		k := key(x.capability, x.scenario)
		if !declared[k] {
			problems = append(problems, problem{kind: kindUnknown, capability: x.capability,
				scenario: x.scenario, detail: "no such scenario in " + specsDir})
			continue
		}
		if exempted[k] {
			problems = append(problems, problem{kind: kindDuplicate, capability: x.capability,
				scenario: x.scenario, detail: "exempted more than once"})
			continue
		}
		exempted[k] = true
		var missing []string
		if x.reason == "" {
			missing = append(missing, "reason")
		}
		if x.wouldVerify == "" {
			missing = append(missing, "what would verify it")
		}
		if len(missing) > 0 {
			problems = append(problems, problem{kind: kindThinExemption, capability: x.capability,
				scenario: x.scenario, detail: "missing " + strings.Join(missing, " and ")})
		}
	}

	for k := range attempted {
		if exempted[k] {
			parts := strings.SplitN(k, "\x00", 2)
			problems = append(problems, problem{kind: kindDuplicate, capability: parts[0],
				scenario: parts[1], detail: "both an entry and an exemption"})
		}
	}

	// A spec may declare the same scenario name under more than one requirement,
	// but the ledger keys a scenario by capability and name, so the report lists
	// each unbacked scenario once.
	reported := make(map[string]bool)
	for _, sp := range specs {
		for _, sc := range sp.scenarios {
			k := key(sc.capability, sc.name)
			if covered[k] || exempted[k] || attempted[k] || reported[k] {
				continue
			}
			reported[k] = true
			problems = append(problems, problem{kind: kindUnbacked,
				capability: sc.capability, scenario: sc.name})
		}
	}

	sort.SliceStable(problems, func(i, j int) bool {
		if problems[i].kind != problems[j].kind {
			return problems[i].kind < problems[j].kind
		}
		if problems[i].capability != problems[j].capability {
			return problems[i].capability < problems[j].capability
		}
		return problems[i].scenario < problems[j].scenario
	})
	return problems
}

// findTest reports whether file declares a test function called name, parsing
// each Go file once with go/parser and caching the functions it declares.
func findTest(fsys fs.FS, cache map[string]map[string]bool, file, name string) error {
	if !strings.HasPrefix(name, "Test") {
		return fmt.Errorf("%s is not a test function", name)
	}
	funcs, ok := cache[file]
	if !ok {
		src, err := fs.ReadFile(fsys, file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, src, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}
		funcs = make(map[string]bool)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			funcs[fn.Name.Name] = true
		}
		cache[file] = funcs
	}
	if !funcs[name] {
		return fmt.Errorf("%s does not declare %s", file, name)
	}
	return nil
}

// report renders the coverage state and every problem as a readable list:
// a one-line summary followed by one heading per non-empty problem kind.
func report(specs []spec, led *ledger, problems []problem) string {
	declared := make(map[string]bool)
	for _, sp := range specs {
		for _, sc := range sp.scenarios {
			declared[key(sc.capability, sc.name)] = true
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s covers %d scenarios in %d capabilities: %d entries, %d exemptions, "+
		"%d unbacked.", ledgerPath, len(declared), len(specs), len(led.entries),
		len(led.exemptions), countKind(problems, kindUnbacked))

	byKind := make(map[problemKind][]problem)
	var kinds []problemKind
	for _, p := range problems {
		if _, seen := byKind[p.kind]; !seen {
			kinds = append(kinds, p.kind)
		}
		byKind[p.kind] = append(byKind[p.kind], p)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })

	for _, kind := range kinds {
		group := byKind[kind]
		fmt.Fprintf(&b, "\n\n%s (%d):", kind.heading(), len(group))
		for _, p := range group {
			fmt.Fprintf(&b, "\n  %s", p)
		}
	}
	return b.String()
}

// countKind returns how many problems are of one kind.
func countKind(problems []problem, kind problemKind) int {
	n := 0
	for _, p := range problems {
		if p.kind == kind {
			n++
		}
	}
	return n
}
