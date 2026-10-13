package repolayout

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// makefileName is the build tool the capability requires at the root. The
// filename is spelled out rather than discovered so renaming it — which would
// break every documented invocation — fails the test that reads it.
const makefileName = "Makefile"

// makeAssign matches a variable assignment (VAR = value, VAR := value,
// VAR ?= value, VAR += value).
var makeAssign = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*(?::=|\?=|\+=|=)\s*(.*)$`)

// makeTarget is one Makefile target: the "## " help text that documents it and
// the recipes make runs for it, one entry per tab-indented line, already
// expanded and freed of the "@" echo suppressor and shell comments.
type makeTarget struct {
	name     string
	doc      string
	commands []string
}

// makefile is the parsed Makefile: its variable assignments and its targets,
// in file order.
type makefile struct {
	vars    map[string]string
	targets []makeTarget
}

// readMakefile parses the Makefile at the repository root.
func readMakefile(t *testing.T) makefile {
	t.Helper()
	src, err := os.ReadFile(path.Join(repoRoot, makefileName))
	if err != nil {
		t.Fatalf("read %s: %v", makefileName, err)
	}
	return parseMakefile(t, string(src))
}

// parseMakefile reads the variable assignments and targets a Makefile
// declares. It understands the subset the repository's Makefile uses —
// "name:" rules, tab-indented recipes, "## " help text, and VAR = assignments
// — which is enough to assert what each target runs. It is deliberately not a
// Make implementation: a construct it does not understand surfaces as a
// command that does not match the expected one, never as a silent pass.
func parseMakefile(t *testing.T, src string) makefile {
	t.Helper()
	m := makefile{vars: map[string]string{}}
	var current *makeTarget
	for i, raw := range strings.Split(src, "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "\t") {
			if current == nil {
				t.Fatalf("%s line %d: recipe line outside any target", makefileName, i+1)
			}
			if cmd := makeCommand(line); cmd != "" {
				current.commands = append(current.commands, cmd)
			}
			continue
		}
		// A target line ends at the first ":", which also splits off the
		// "## help" comment every documented target carries.
		if idx := strings.Index(line, ":"); idx > 0 && !strings.Contains(line[:idx], "=") {
			name := line[:idx]
			if !strings.ContainsAny(name, " \t$") {
				doc := ""
				if _, after, ok := strings.Cut(line[idx+1:], "## "); ok {
					doc = strings.TrimSpace(after)
				}
				m.targets = append(m.targets, makeTarget{name: name, doc: doc})
				current = &m.targets[len(m.targets)-1]
				continue
			}
		}
		// A file-level line ends the current recipe; anything that is not an
		// assignment (an include, a conditional) is left alone.
		current = nil
		if assignment := makeAssign.FindStringSubmatch(line); assignment != nil {
			m.vars[assignment[1]] = strings.TrimSpace(assignment[2])
		}
	}
	return m
}

// makeCommand turns one recipe line into the command make would run.
func makeCommand(line string) string {
	cmd := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "@"))
	// Whole-line comments inside a recipe are make comments, not commands.
	if cmd == "" || strings.HasPrefix(cmd, "#") {
		return ""
	}
	// A trailing comment is only stripped after whitespace, so a "#" inside an
	// argument (an awk program, a URL) survives.
	if idx := strings.Index(cmd, " #"); idx >= 0 {
		cmd = strings.TrimSpace(cmd[:idx])
	}
	return cmd
}

// mustTarget returns the named target or fails, listing the targets the
// Makefile does declare so the failure reads as a diff.
func (m makefile) mustTarget(t *testing.T, name string) makeTarget {
	t.Helper()
	for _, tgt := range m.targets {
		if tgt.name == name {
			return tgt
		}
	}
	names := make([]string, 0, len(m.targets))
	for _, tgt := range m.targets {
		names = append(names, tgt.name)
	}
	t.Fatalf("Makefile declares no %s target; it declares %v", name, names)
	return makeTarget{}
}

// recipe returns a target's commands with the Make variables the Makefile
// defines expanded, so the assertions read as the commands make would run.
func (m makefile) recipe(t *testing.T, name string) []string {
	t.Helper()
	tgt := m.mustTarget(t, name)
	if len(tgt.commands) == 0 {
		t.Fatalf("make %s has an empty recipe", name)
	}
	commands := make([]string, 0, len(tgt.commands))
	for _, cmd := range tgt.commands {
		expanded, err := m.expand(cmd)
		if err != nil {
			t.Fatalf("make %s runs %q, which the test cannot expand: %v", name, cmd, err)
		}
		// "$$1" is make's escape for a shell "$1": the command is asserted in
		// its runnable shell form.
		commands = append(commands, strings.ReplaceAll(expanded, "$$", "$"))
	}
	return commands
}

// expand substitutes the variables the Makefile defines. A reference to a
// variable the Makefile does not define is an error rather than an empty
// string, so a recipe that starts using an expansion this test does not
// understand fails loudly instead of quietly comparing against a command no
// one wrote. Make functions such as $(shell ...) are left for make to evaluate
// because their result depends on the machine, not on the Makefile's text.
func (m makefile) expand(cmd string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(cmd); {
		open := strings.IndexByte(cmd[i:], '$')
		if open < 0 {
			b.WriteString(cmd[i:])
			break
		}
		open += i
		b.WriteString(cmd[i:open])
		if open+1 >= len(cmd) || (cmd[open+1] != '(' && cmd[open+1] != '{') {
			b.WriteByte('$')
			i = open + 1
			continue
		}
		closeByte := byte(')')
		if cmd[open+1] == '{' {
			closeByte = '}'
		}
		end := matchingDelimiter(cmd, open+1, closeByte)
		if end < 0 {
			return "", fmt.Errorf("unbalanced %q", cmd[open:])
		}
		body := cmd[open+2 : end]
		if !makeVarName(body) {
			// A Make function or a computed name: make evaluates it, so leave
			// the reference as written.
			b.WriteString(cmd[open : end+1])
			i = end + 1
			continue
		}
		value, ok := m.vars[body]
		if !ok {
			return "", fmt.Errorf("undefined Make variable %q", body)
		}
		b.WriteString(value)
		i = end + 1
	}
	return b.String(), nil
}

// matchingDelimiter returns the index of the delimiter closing the opener at
// open, or -1 when the reference is unbalanced. Nesting is counted, so
// $(shell find ... $(NAME) -print) resolves to the right parenthesis.
func matchingDelimiter(s string, open int, closeByte byte) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case s[open]:
			depth++
		case closeByte:
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// makeVarName reports whether body is a plain Make variable name rather than a
// function call ("shell ...") or a computed name ("$(FOO)_BAR").
func makeVarName(body string) bool {
	if body == "" {
		return false
	}
	for i, r := range body {
		switch {
		case r == '_':
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// hasCommand reports whether the commands include the wanted one, compared
// after whitespace normalization.
func hasCommand(commands []string, want string) bool {
	return slices.ContainsFunc(commands, func(cmd string) bool {
		return normalizeCommand(cmd) == want
	})
}

// normalizeCommand collapses whitespace so a command written with different
// spacing compares equal.
func normalizeCommand(cmd string) string {
	return strings.Join(strings.Fields(cmd), " ")
}

// TestBuildToolTargetsExist backs the target list the "Build tool targets"
// requirement names: build, vet, lint, test, check, proto, up, and down all
// exist because contributors and CI call them by name.
func TestBuildToolTargetsExist(t *testing.T) {
	m := readMakefile(t)
	want := []string{"build", "vet", "lint", "test", "check", "proto", "up", "down"}
	for _, name := range want {
		if slices.ContainsFunc(m.targets, func(tgt makeTarget) bool { return tgt.name == name }) {
			continue
		}
		t.Errorf("Makefile declares no %s target; the capability requires at least %v", name, want)
	}
}

// TestCheckRunsTheDefinitionOfDoneInOrder backs the "Definition of Done via the
// build tool" scenario: make check runs the four Definition-of-Done commands —
// import/format cleanup, go vet, linter, race-enabled test suite — in that
// order, each as its own recipe line, so deleting, duplicating, or reordering
// one fails here rather than in CI.
func TestCheckRunsTheDefinitionOfDoneInOrder(t *testing.T) {
	m := readMakefile(t)
	commands := m.recipe(t, "check")

	want := []string{
		"goimports -w .",
		"go vet ./...",
		"golangci-lint run",
		"go test -race -count=1 ./...",
	}
	counts := map[string]int{}
	for _, cmd := range commands {
		counts[normalizeCommand(cmd)]++
	}
	for _, cmd := range want {
		if counts[cmd] != 1 {
			t.Errorf("make check runs %q %d times, want exactly 1: %q", cmd, counts[cmd], commands)
		}
	}

	// make runs each recipe line as its own shell command and stops at the
	// first non-zero exit, so the line order is what makes the definition of
	// done a fail-fast sequence; assert that order.
	order := make([]int, 0, len(want))
	for _, cmd := range want {
		order = append(order, slices.IndexFunc(commands, func(got string) bool {
			return normalizeCommand(got) == cmd
		}))
	}
	for i := 1; i < len(order); i++ {
		if order[i-1] < 0 || order[i] < 0 {
			continue
		}
		if order[i-1] >= order[i] {
			t.Fatalf("make check runs the definition of done out of order: indexes %v; commands are %q", order, commands)
		}
	}
}

// TestSingleTargetsRemainSinglePurpose backs the "Single targets stay
// single-purpose" scenario: make test, make vet, and make lint each run exactly
// their documented command and nothing else, so neither a duplicated command
// nor an extra step can hide in them.
func TestSingleTargetsRemainSinglePurpose(t *testing.T) {
	m := readMakefile(t)
	tests := []struct {
		target string
		want   string
		why    string
	}{
		{"vet", "go vet ./...", "the engineering rules' vet step"},
		{"lint", "golangci-lint run", "the repository's linter over the tree"},
		{"test", "go test -race -count=1 ./...", "the race-enabled suite, and nothing else"},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			commands := m.recipe(t, tc.target)
			if len(commands) != 1 {
				t.Fatalf("make %s runs %d commands, want exactly 1 (%s): %q",
					tc.target, len(commands), tc.why, commands)
			}
			if got := normalizeCommand(commands[0]); got != tc.want {
				t.Errorf("make %s runs %q, want %q", tc.target, got, tc.want)
			}
		})
	}
}

// TestRequiredTargetsRunTheirCommands backs the "Build tool targets"
// requirement's per-target contract — build compiles every binary, vet vets the
// tree, lint lints it, test runs the race-enabled suite, check runs the
// definition of done in order, proto regenerates the stubs, and up/down drive
// the deploy manifests — by asserting the command each target runs. The
// commands are written out here rather than read from the help text because for
// the "Build tool targets" requirement's targets the help text is the claim,
// and a claim checked against itself proves nothing.
func TestRequiredTargetsRunTheirCommands(t *testing.T) {
	m := readMakefile(t)
	tests := []struct {
		target string
		want   string
		why    string
	}{
		{"build", "go build -o bin/ ./cmd/...", "compiles all binaries"},
		{"vet", "go vet ./...", "vets the tree"},
		{"lint", "golangci-lint run", "lints the tree"},
		{"test", "go test -race -count=1 ./...", "runs the race-enabled suite"},
		{"proto", "goimports -w api/proto", "leaves the regenerated stubs goimports-clean"},
		{"up", "kubectl apply -f deploy/", "brings the local stack up from deploy/"},
		{"down", "kubectl delete -f deploy/", "tears the local stack down"},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			// up and down are asserted by TestStackLifecycleTargetsPointAtDeploy
			// with the failure handling they need; each check stays here because
			// the requirement names every target.
			commands := m.recipe(t, tc.target)
			if !hasCommand(commands, tc.want) {
				t.Errorf("make %s does not run %q, so it does not %s: %q",
					tc.target, tc.want, tc.why, commands)
			}
		})
	}
}

// TestStackLifecycleTargetsPointAtDeploy backs the manifest wiring the "Stack
// lifecycle targets" scenario names: make up and make down drive the tooling
// over deploy/, and a failure in that tooling fails the target. What the
// scenario also requires — that the deploy/ manifests exist and the stack comes
// up or tears down — is not backed here, because the manifests have not landed;
// that half is recorded as an exemption. These assertions still fail if the
// wiring is removed, stops naming deploy/, or starts swallowing the tooling's
// exit status.
func TestStackLifecycleTargetsPointAtDeploy(t *testing.T) {
	m := readMakefile(t)
	tests := []struct {
		target string
		want   string
		verb   string
	}{
		{"up", "kubectl apply -f deploy/", "bring up"},
		{"down", "kubectl delete -f deploy/", "tear down"},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			commands := m.recipe(t, tc.target)
			if !hasCommand(commands, tc.want) {
				t.Errorf("make %s does not %s the deploy/ manifests: it runs %q, want %q",
					tc.target, tc.verb, commands, tc.want)
			}
			// The help text names the command; a rewrite that stopped matching
			// it would leave the documentation lying.
			if doc := m.mustTarget(t, tc.target).doc; !strings.Contains(doc, tc.want) {
				t.Errorf("make %s documents %q, which does not name its command %q", tc.target, doc, tc.want)
			}
			for _, cmd := range commands {
				if strings.Contains(cmd, "|| true") || strings.HasPrefix(cmd, "-") {
					t.Errorf("make %s runs %q, which swallows the tooling's failure", tc.target, cmd)
				}
			}
		})
	}
}

// gitLsFiles lists the paths the index tracks under rel, one per line, as git
// reports them. It fails the test when git cannot answer, because an unreadable
// index would otherwise read as "nothing is tracked".
func gitLsFiles(t *testing.T, rel string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "--", rel)
	cmd.Dir = repoRoot
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files %s: %v", rel, err)
	}
	var files []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			files = append(files, line)
		}
	}
	return files
}

// parseGitmodules reads a .gitmodules file into one map per submodule, keyed by
// the submodule name from its [submodule "name"] header.
func parseGitmodules(t *testing.T, src string) map[string]map[string]string {
	t.Helper()
	header := regexp.MustCompile(`^\[submodule\s+"([^"]+)"\]$`)
	entries := map[string]map[string]string{}
	var current map[string]string
	for i, raw := range strings.Split(src, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if match := header.FindStringSubmatch(line); match != nil {
			current = map[string]string{}
			entries[match[1]] = current
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || current == nil {
			t.Fatalf(".gitmodules line %d: cannot parse %q", i+1, line)
		}
		current[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return entries
}

// submoduleNames lists the registered submodule names for a failure message.
func submoduleNames(entries map[string]map[string]string) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	return names
}

// frontendSource reports whether a filename is part of a React + Vite frontend
// rather than the placeholder note the parent repository carries.
func frontendSource(name string) bool {
	switch name {
	case "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "index.html",
		"vite.config.js", "vite.config.ts", "tsconfig.json", "node_modules":
		return true
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs", ".vue", ".svelte", ".css", ".scss":
		return true
	}
	return false
}
