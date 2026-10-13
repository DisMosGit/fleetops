package protocodegen

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// toolPins is the generator toolchain the Makefile pins. The pins are one
// configuration: the regeneration command installs exactly these versions, so
// a version moves in the Makefile's tool block and nowhere else.
type toolPins struct {
	protocGenGo     string
	protocGenGoGRPC string
	goimports       string
}

// pinRE matches one pin assignment in the Makefile's tool block.
var pinRE = regexp.MustCompile(`(?m)^(PROTOC_GEN_GO_VERSION|PROTOC_GEN_GO_GRPC_VERSION|GOIMPORTS_VERSION)\s*:?=\s*(\S+)\s*$`)

// pluginPinRE matches a pinned install of a generator plugin — the form that
// names a version outside the Makefile's tool block.
var pluginPinRE = regexp.MustCompile(`protoc-gen-go(-grpc)?@v?\d`)

// pluginVersionREs extract the generator version a committed stub records in
// its header, one expression per generator.
var (
	goPluginVersionRE   = regexp.MustCompile(`(?m)^//\s+protoc-gen-go\s+v(\S+)\s*$`)
	grpcPluginVersionRE = regexp.MustCompile(`(?m)^//\s+-?\s*protoc-gen-go-grpc\s+v(\S+)\s*$`)
)

// loadMakefilePins reads the Makefile's pinned generator versions and fails
// when a pin is missing or defined more than once: a second definition would
// make "the pinned toolchain" ambiguous.
func loadMakefilePins(t *testing.T, fsys fs.FS) toolPins {
	t.Helper()

	src, err := fs.ReadFile(fsys, "Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	defined := make(map[string][]string)
	for _, m := range pinRE.FindAllStringSubmatch(string(src), -1) {
		defined[m[1]] = append(defined[m[1]], m[2])
	}

	var pins toolPins
	for _, pin := range []struct {
		name string
		dst  *string
	}{
		{"PROTOC_GEN_GO_VERSION", &pins.protocGenGo},
		{"PROTOC_GEN_GO_GRPC_VERSION", &pins.protocGenGoGRPC},
		{"GOIMPORTS_VERSION", &pins.goimports},
	} {
		values := defined[pin.name]
		if len(values) != 1 {
			t.Fatalf("Makefile defines %s %d times, want exactly one pin: %v", pin.name, len(values), values)
		}
		*pin.dst = values[0]
	}
	return pins
}

// targetRecipe returns the recipe lines of the named Makefile target.
func targetRecipe(t *testing.T, makefile, target string) string {
	t.Helper()

	var recipe []string
	inTarget := false
	for _, line := range strings.Split(makefile, "\n") {
		if strings.HasPrefix(line, target+":") {
			inTarget = true
			continue
		}
		if !inTarget {
			continue
		}
		if strings.HasPrefix(line, "\t") {
			recipe = append(recipe, line)
			continue
		}
		if strings.TrimSpace(line) == "" {
			// A blank line may sit inside a recipe; only a following
			// non-recipe line ends it.
			continue
		}
		break
	}
	if len(recipe) == 0 {
		t.Fatalf("Makefile target %s has no recipe", target)
	}
	return strings.Join(recipe, "\n")
}

// assertProtoToolsReadsPins fails when the pinned install target does not
// reference a pin, which would leave that tool installed at whatever version
// happened to be published.
func assertProtoToolsReadsPins(t *testing.T, fsys fs.FS) {
	t.Helper()

	src, err := fs.ReadFile(fsys, "Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	recipe := targetRecipe(t, string(src), "proto-tools")
	for _, name := range []string{"PROTOC_GEN_GO_VERSION", "PROTOC_GEN_GO_GRPC_VERSION", "GOIMPORTS_VERSION"} {
		if !strings.Contains(recipe, "$("+name+")") {
			t.Errorf("Makefile proto-tools recipe does not read $(%s); the pinned install must use the pin", name)
		}
	}
}

// assertPinsOnlyInMakefile fails when a file other than the Makefile names a
// pinned install of a generator plugin: one configuration owns the pins, so a
// second install site cannot silently pick a version of its own.
func assertPinsOnlyInMakefile(t *testing.T, fsys fs.FS) {
	t.Helper()

	var offenders []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == ".git" || p == "node_modules" {
				return fs.SkipDir
			}
			return nil
		}
		if p == "Makefile" {
			return nil
		}
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		if pin := pluginPinRE.FindString(string(src)); pin != "" {
			offenders = append(offenders, fmt.Sprintf("%s pins %s", p, pin))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("generator plugin installs outside the Makefile, which owns the pins:\n%s", strings.Join(offenders, "\n"))
	}
}

// assertCommittedStubsMatchPins fails when the committed stubs were generated
// by a version other than the Makefile's pin: the committed artifacts and the
// pinned toolchain have drifted apart, and regenerating would dirty the tree.
func assertCommittedStubsMatchPins(t *testing.T, fsys fs.FS, pins toolPins) {
	t.Helper()

	const fix = "run 'make proto-tools' and then 'make proto' and commit the regenerated stubs"
	for _, p := range findStubs(t, fsys) {
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if strings.HasSuffix(p, "_grpc.pb.go") {
			assertVersion(t, p, firstSubmatch(string(src), grpcPluginVersionRE), pins.protocGenGoGRPC, fix)
			continue
		}
		assertVersion(t, p, firstSubmatch(string(src), goPluginVersionRE), pins.protocGenGo, fix)
	}
}

// assertBinariesDoNotDependOnPlugins fails when any shipped binary's dependency
// graph reaches a generator plugin, which is the runtime-dependency case the
// requirement forbids.
func assertBinariesDoNotDependOnPlugins(t *testing.T, plugins []string) {
	t.Helper()

	const tmpl = `{{.ImportPath}}{{range .Deps}} {{.}}{{end}}`
	cmd := exec.Command("go", "list", "-f", tmpl, modulePath+"/cmd/...")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s/cmd/...: %v: %s", modulePath, err, stderr.String())
	}

	var violations []string
	binaries := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		binaries++
		for _, dep := range fields[1:] {
			for _, plugin := range plugins {
				if dep == plugin {
					violations = append(violations, fields[0]+" depends on "+plugin)
				}
			}
		}
	}
	if binaries == 0 {
		t.Fatal("go list reported no binaries under cmd/...; the dependency check would pass vacuously")
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		t.Errorf("shipped binaries depend on generator plugins, which must stay build-time tooling:\n%s", strings.Join(violations, "\n"))
	}
}

// assertProductionTreeDoesNotImportPlugins scans the non-test Go files under
// cmd/ and internal/ — the module's production tree — for an import of a
// generator plugin. Test files are skipped: a plugin reached only from a test
// does not make the toolchain a runtime dependency.
func assertProductionTreeDoesNotImportPlugins(t *testing.T, fsys fs.FS, plugins []string) {
	t.Helper()

	var violations []string
	for _, root := range []string{"cmd", "internal"} {
		err := fs.WalkDir(fsys, root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			src, err := fs.ReadFile(fsys, p)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), p, src, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("parse %s: %w", p, err)
			}
			for _, spec := range file.Imports {
				imp := strings.Trim(spec.Path.Value, `"`)
				for _, plugin := range plugins {
					if imp == plugin {
						violations = append(violations, p+" imports "+plugin)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		t.Errorf("production packages import generator plugins, which are build-time tools:\n%s", strings.Join(violations, "\n"))
	}
}

// TestGeneratorPluginsAreBuildOnly backs "Tools are not runtime dependencies":
// the pinned plugins appear only as build-time tooling — no shipped binary's
// dependency graph reaches them, no production package imports them, the only
// pinned install of them is the Makefile's tool block the regeneration command
// reads, and the committed stubs record exactly the pinned versions. An
// `import _ ".../cmd/protoc-gen-go"` anywhere in the production tree, or a
// plugin pin outside the Makefile, fails here.
func TestGeneratorPluginsAreBuildOnly(t *testing.T) {
	t.Parallel()

	fsys := os.DirFS(repoRoot)
	plugins := []string{
		"google.golang.org/protobuf/cmd/protoc-gen-go",
		"google.golang.org/grpc/cmd/protoc-gen-go-grpc",
	}

	assertBinariesDoNotDependOnPlugins(t, plugins)
	assertProductionTreeDoesNotImportPlugins(t, fsys, plugins)

	pins := loadMakefilePins(t, fsys)
	assertProtoToolsReadsPins(t, fsys)
	assertPinsOnlyInMakefile(t, fsys)
	assertCommittedStubsMatchPins(t, fsys, pins)
}
