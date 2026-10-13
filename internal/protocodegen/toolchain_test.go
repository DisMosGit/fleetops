package protocodegen

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// toolchain is the resolved path of every tool `make proto` runs.
type toolchain struct {
	protoc          string
	protocGenGo     string
	protocGenGoGRPC string
	goimports       string
}

// protocHeaderRE extracts the protoc release a generated stub records.
var protocHeaderRE = regexp.MustCompile(`(?m)^//\s+-?\s*protoc\s+v(\S+)\s*$`)

// firstSubmatch returns the first capture group of re in src, or "".
func firstSubmatch(src string, re *regexp.Regexp) string {
	m := re.FindStringSubmatch(src)
	if m == nil {
		return ""
	}
	return m[1]
}

// normalizeVersion drops the leading "v" so pins and tool output compare.
func normalizeVersion(v string) string {
	return strings.TrimPrefix(strings.TrimSpace(v), "v")
}

// requireVersion stops the test when a tool is not the pinned version: the
// regeneration tests compare bytes, so a skewed generator must fail with the
// fix rather than a diff.
func requireVersion(t *testing.T, what, got, want, fix string) {
	t.Helper()

	if normalizeVersion(got) != normalizeVersion(want) {
		t.Fatalf("%s is %s, want %s; %s", what, got, want, fix)
	}
}

// assertVersion reports a version mismatch without stopping, for the pass that
// checks several committed artifacts.
func assertVersion(t *testing.T, what, got, want, fix string) {
	t.Helper()

	if normalizeVersion(got) != normalizeVersion(want) {
		t.Errorf("%s is %s, want %s; %s", what, got, want, fix)
	}
}

// protocNumber maps the version generated headers record (protoc-gen-go writes
// the protobuf library release, "v5.29.3") to the number protoc --version
// prints (the compiler release, "libprotoc 29.3"): one extra leading component.
func protocNumber(headerVersion string) string {
	v := strings.TrimPrefix(headerVersion, "v")
	if _, rest, ok := strings.Cut(v, "."); ok {
		return rest
	}
	return v
}

// requirePinnedToolchain resolves the tools `make proto` needs and fails unless
// they are the versions the Makefile pins (and, for protoc, the release the
// committed stubs record). A missing or skewed tool is a toolchain problem the
// reader fixes with 'make proto-tools'; skipping instead would leave the
// scenarios the regeneration tests back unverified.
func requirePinnedToolchain(t *testing.T, fsys fs.FS) toolchain {
	t.Helper()

	pins := loadMakefilePins(t, fsys)
	tc := toolchain{
		protoc: toolPath(t, "protoc",
			"install the protoc release that generated the committed stubs, with its include/ tree (see api/proto/README.md)"),
		protocGenGo:     toolPath(t, "protoc-gen-go", "run 'make proto-tools' to install the pinned generators"),
		protocGenGoGRPC: toolPath(t, "protoc-gen-go-grpc", "run 'make proto-tools' to install the pinned generators"),
		goimports:       toolPath(t, "goimports", "run 'make proto-tools' to install the pinned goimports"),
	}

	const fix = "run 'make proto-tools' to install the pinned versions"
	requireVersion(t, "protoc-gen-go", versionOutput(t, tc.protocGenGo), pins.protocGenGo, fix)
	requireVersion(t, "protoc-gen-go-grpc", versionOutput(t, tc.protocGenGoGRPC), pins.protocGenGoGRPC, fix)
	requireVersion(t, "goimports", goimportsModuleVersion(t, tc.goimports), pins.goimports, fix)
	requireVersion(t, "protoc", versionOutput(t, tc.protoc), protocNumber(committedProtocVersion(t, fsys)),
		"install the protoc release that generated the committed stubs")
	return tc
}

// committedProtocVersion returns the protoc release the committed stubs record
// in their header, so the byte comparison can refuse to run with a different
// compiler.
func committedProtocVersion(t *testing.T, fsys fs.FS) string {
	t.Helper()

	for _, p := range findStubs(t, fsys) {
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if v := firstSubmatch(string(src), protocHeaderRE); v != "" {
			return v
		}
	}
	t.Fatal("no committed stub records a protoc version")
	return ""
}

// versionOutput runs tool --version and returns the version token it prints.
func versionOutput(t *testing.T, tool string) string {
	t.Helper()

	out, err := exec.Command(tool, "--version").Output()
	if err != nil {
		t.Fatalf("%s --version: %v", tool, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		t.Fatalf("%s --version printed nothing", tool)
	}
	return fields[len(fields)-1]
}

// goimportsModuleVersion reads the golang.org/x/tools version a goimports
// binary was built from; goimports has no --version flag.
func goimportsModuleVersion(t *testing.T, tool string) string {
	t.Helper()

	out, err := exec.Command("go", "version", "-m", tool).Output()
	if err != nil {
		t.Fatalf("go version -m %s: %v", tool, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "mod" && fields[1] == "golang.org/x/tools" {
			return fields[2]
		}
	}
	t.Fatalf("go version -m %s: no golang.org/x/tools module line", tool)
	return ""
}

// absPath resolves p so a tool invoked by an absolute path stays found when a
// test replaces PATH.
func absPath(t *testing.T, p string) string {
	t.Helper()

	abs, err := filepath.Abs(p)
	if err != nil {
		t.Fatalf("resolve %s: %v", p, err)
	}
	return abs
}

// toolPath finds a tool on PATH or in GOPATH/bin, the two places the Makefile
// and api/proto/README.md install tooling into.
func toolPath(t *testing.T, name, hint string) string {
	t.Helper()

	if p, err := exec.LookPath(name); err == nil {
		return absPath(t, p)
	}
	bin := goBinDir(t)
	p := filepath.Join(bin, name)
	if info, err := os.Stat(p); err == nil && !info.IsDir() {
		return p
	}
	t.Fatalf("%s not found on PATH or in %s; %s", name, bin, hint)
	return ""
}

// goBinDir returns GOPATH/bin, where 'make proto-tools' installs the pinned
// generators.
func goBinDir(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("go", "env", "GOPATH").Output()
	if err != nil {
		t.Fatalf("go env GOPATH: %v", err)
	}
	return filepath.Join(strings.TrimSpace(string(out)), "bin")
}

// makePath resolves make absolutely: the missing-toolchain test hides tools by
// replacing PATH, and an invocation resolved through that PATH would hide make
// itself.
func makePath(t *testing.T) string {
	t.Helper()

	return toolPath(t, "make", "install make, the entry point for the documented regeneration command")
}

// goDir returns the directory of the go command, which the Makefile's
// "go env GOPATH" line and its $(GO) variable need while the missing-toolchain
// runs hide the protoc toolchain.
func goDir(t *testing.T) string {
	t.Helper()

	p, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go not found on PATH: %v", err)
	}
	return filepath.Dir(absPath(t, p))
}

// toolchainPath puts the directories of the resolved tools ahead of the current
// PATH, so make's recipes run exactly the tools the test version checked.
func toolchainPath(tc toolchain) string {
	var dirs []string
	seen := make(map[string]bool)
	for _, tool := range []string{tc.protoc, tc.protocGenGo, tc.protocGenGoGRPC, tc.goimports} {
		dir := filepath.Dir(tool)
		if seen[dir] {
			continue
		}
		seen[dir] = true
		dirs = append(dirs, dir)
	}
	return strings.Join(append(dirs, os.Getenv("PATH")), string(os.PathListSeparator))
}

// withPath returns the process environment with PATH replaced: a duplicate PATH
// entry would leave the child's lookup order unspecified.
func withPath(path string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			continue
		}
		out = append(out, kv)
	}
	return append(out, "PATH="+path)
}
