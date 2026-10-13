package repolayout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// repoRoot locates the repository root from this package's working directory,
// the same way internal/speccoverage does, so the tests assert the working tree
// wherever "go test" is invoked from.
const repoRoot = "../.."

// layoutDir is one top-level directory the repo-scaffold capability names as a
// sanctioned location for repository content.
type layoutDir struct {
	// dir is the path relative to the repository root.
	dir string
	// readme is the note the directory carries. Every sanctioned directory
	// holds one at this path (the spec names api/proto/, and web/ carries its
	// note in the frontend repository).
	readme string
	// delivered is true once the directory holds the content its stage
	// promises. A directory still waiting for its stage must carry a
	// placeholder note instead — see TestPlannedPathsAreMarkedNotImplemented.
	delivered bool
	// submodule marks the one entry committed as a git submodule: a clone that
	// does not initialize submodules has no directory for it at all, so its
	// content is checked only when the submodule is initialized.
	submodule bool
	// stage is the delivery stage the note beside the directory names, e.g.
	// "stage 4". It is what the note promises will fill the directory.
	stage string
}

// sanctionedLayout is the layout contract from openspec/specs/repo-scaffold:
// one directory per owner domain, each documented so a reader learns what
// belongs there and which stage fills it.
var sanctionedLayout = []layoutDir{
	{dir: "cmd", readme: "cmd/README.md", delivered: true, stage: "stage 1"},
	{dir: "internal", readme: "internal/README.md", delivered: true, stage: "stage 3"},
	{dir: "api/proto", readme: "api/proto/README.md", delivered: true, stage: "stage 1"},
	{dir: "deploy", readme: "deploy/README.md", delivered: true, stage: "stage 1"},
	{dir: "dashboards", readme: "dashboards/README.md", delivered: false, stage: "stage 4"},
	{dir: "web", readme: "web/README.md", delivered: false, submodule: true, stage: "stage 5"},
}

// readRepoFile reads a file under the repository root, failing the test rather
// than returning a zero value so a broken assertion is never read as a pass.
func readRepoFile(t *testing.T, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(path.Join(repoRoot, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return data
}

// readTree walks rel under the repository root and returns every file it holds
// as slash-separated paths relative to rel. Walking rather than checking a
// fixed list is deliberate: the assertions built on it have to see a file a
// contributor adds anywhere in the tree, not just where one is expected today.
func readTree(t *testing.T, rel string) []string {
	t.Helper()
	root := path.Join(repoRoot, rel)
	var files []string
	err := fs.WalkDir(os.DirFS(root), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// stagePattern matches a stage reference tolerantly: "stage 4", "stage-4", and
// "stages 1-5" all name their stage.
func stagePattern(stage string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)\bstages?[- ]` + regexp.QuoteMeta(strings.TrimPrefix(stage, "stage ")) + `\b`)
}

// submoduleContentMissing reports whether a sanctioned submodule's content is
// absent: the directory is not checked out, or it is the empty directory a
// clone that did not initialize submodules holds. The parent repository is then
// exactly as committed, and the note lives in the submodule rather than here —
// the "Usable clone without submodule content" scenario requires that state to
// keep working. TestFrontendSubmoduleRegistrationIsExplicit and
// TestWebIsPlaceholderOnly assert the registration and the placeholder instead.
func submoduleContentMissing(t *testing.T, want layoutDir) bool {
	t.Helper()
	if !want.submodule {
		return false
	}
	if _, err := os.Stat(path.Join(repoRoot, want.readme)); err != nil {
		t.Logf("%s carries no note because the submodule is not initialized; the registration "+
			"and the placeholder are asserted by the submodule tests", want.dir)
		return true
	}
	return false
}

// TestFreshCloneExposesTheLayout backs the "Fresh clone exposes the layout"
// scenario: every directory the capability names exists at the top level and is
// documented by a note that names the delivery stage responsible for its
// content, so a reader of a fresh clone learns what the directory is for.
//
// The directory set is asserted against the committed tree (git ls-tree HEAD)
// and the notes against the working tree, because "a contributor clones the
// repository" is a claim about the commit, not about whatever a contributor
// happens to have checked out. web/ is the one submodule: it is committed as a
// gitlink, so a clone that does not initialize submodules has an empty web/
// rather than a documented one, and its note is asserted only once the
// submodule is checked out.
func TestFreshCloneExposesTheLayout(t *testing.T) {
	committed := committedEntries(t)
	for _, want := range sanctionedLayout {
		t.Run(want.dir, func(t *testing.T) {
			if !slices.Contains(committed, want.dir) {
				t.Errorf("%s is not committed at HEAD, so a fresh clone does not expose it", want.dir)
			}
			if submoduleContentMissing(t, want) {
				return
			}
			info, err := os.Stat(path.Join(repoRoot, want.dir))
			if err != nil {
				t.Fatalf("sanctioned directory %s is missing: %v", want.dir, err)
			}
			if !info.IsDir() {
				t.Fatalf("%s is not a directory", want.dir)
			}
			if got := readTree(t, want.dir); len(got) == 0 {
				t.Fatalf("%s holds no content; the capability requires it to exist with at least its note", want.dir)
			}

			note := string(readRepoFile(t, want.readme))
			// Naming the directory keeps the note attached to the domain it
			// documents, so a note copied in from elsewhere does not read as
			// documentation of this one.
			if !strings.Contains(note, path.Base(want.dir)) {
				t.Errorf("%s does not name %s", want.readme, path.Base(want.dir))
			}
			// The stage reference is the part of the note the spec requires: a
			// reader must learn which delivery stage fills the directory. The
			// match is loose about spelling because only the promise matters.
			if !stagePattern(want.stage).MatchString(note) {
				t.Errorf("%s does not name %s, the delivery stage responsible for %s",
					want.readme, want.stage, want.dir)
			}
		})
	}
}

// committedEntries returns every directory the committed tree holds, as
// slash-separated paths with the trailing slash trimmed. It reads the commit
// rather than the working tree, so the layout assertion holds no matter what a
// contributor's checkout looks like.
func committedEntries(t *testing.T) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-tree", "-r", "-d", "--name-only", "HEAD")
	cmd.Dir = repoRoot
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-tree HEAD: %v", err)
	}
	var entries []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSuffix(strings.TrimSpace(line), "/"); line != "" {
			entries = append(entries, line)
		}
	}
	if len(entries) == 0 {
		t.Fatal("git ls-tree HEAD listed no entries: reading the commit failed, not the layout")
	}
	return entries
}

// TestNoCodeOutsideSanctionedDirectories backs the "No code outside the
// sanctioned directories" scenario: every Go file added to the repository lives
// in a thin cmd/ entrypoint or an internal/ package, with the generated code
// under api/proto/ as the one exempt location. The no utils/helpers/common rule
// is the same commitment stated by name, so it is asserted on the directory
// path of every Go file rather than on a list of places someone remembered.
func TestNoCodeOutsideSanctionedDirectories(t *testing.T) {
	// The walk starts at ".", which already covers the repository root, so a Go
	// file added outside every sanctioned prefix fails here.
	files := readTree(t, ".")
	if len(files) == 0 {
		t.Fatal("walked the repository root and found no files: the walk is wrong, not the layout")
	}

	sanctioned := []string{"cmd/", "internal/", "api/proto/"}
	goFiles := 0
	for _, file := range files {
		if path.Ext(file) != ".go" {
			continue
		}
		goFiles++
		if !slicesContainPrefix(sanctioned, file) {
			t.Errorf("Go file %s is outside the sanctioned directories %s", file, strings.Join(sanctioned, ", "))
		}
	}
	// A tree-wide walk that finds no Go file would pass vacuously; the
	// repository holds hundreds, so a zero here means the walk broke.
	if goFiles == 0 {
		t.Fatal("found no Go files at all: the walk is wrong, not the layout")
	}

	banned := []string{"utils", "helpers", "common"}
	for _, file := range files {
		if path.Ext(file) != ".go" {
			continue
		}
		for _, part := range strings.Split(path.Dir(file), "/") {
			for _, name := range banned {
				if part == name {
					t.Errorf("Go file %s lives in a %s directory; the capability names packages by what they provide",
						file, name)
				}
			}
		}
	}
}

// TestPlannedPathsAreMarkedNotImplemented backs the "Planned paths are not
// presented as working" scenario: a directory whose delivery stage has not
// landed says the code is not implemented yet and names the stage that will
// deliver it, so the placeholder cannot drift into looking like working
// content.
func TestPlannedPathsAreMarkedNotImplemented(t *testing.T) {
	for _, want := range sanctionedLayout {
		if want.delivered {
			continue
		}
		t.Run(want.dir, func(t *testing.T) {
			if submoduleContentMissing(t, want) {
				return
			}
			note := strings.ToLower(string(readRepoFile(t, want.readme)))
			if !strings.Contains(note, "not implemented") {
				t.Errorf("%s does not state that its code is not implemented yet", want.readme)
			}
			if !stagePattern(want.stage).MatchString(note) {
				t.Errorf("%s does not name %s, the stage that will deliver it", want.readme, want.stage)
			}
		})
	}
}

// TestNoBannedPackageNames backs the second half of the "No code outside the
// sanctioned directories" scenario: no utils, helpers, or common package
// exists. Asserting the package clause catches a package that is named for the
// leftover bucket even when it sits under internal/, which the path assertion
// alone would accept.
func TestNoBannedPackageNames(t *testing.T) {
	banned := map[string]bool{"utils": true, "helpers": true, "common": true}
	decl := regexp.MustCompile(`(?m)^package\s+([A-Za-z_][A-Za-z0-9_]*)`)
	checked := 0
	for _, file := range readTree(t, ".") {
		if path.Ext(file) != ".go" {
			continue
		}
		match := decl.FindSubmatch(readRepoFile(t, file))
		if match == nil {
			t.Errorf("%s declares no package clause", file)
			continue
		}
		checked++
		name := string(match[1])
		if banned[name] {
			t.Errorf("%s declares package %s; the capability forbids utils, helpers, and common packages", file, name)
		}
		// cmd/ holds thin entrypoints, so a non-main package there would mean
		// behavior moved out of an internal/ package and into the binary.
		if strings.HasPrefix(file, "cmd/") && name != "main" {
			t.Errorf("%s declares package %s, not main: cmd/ holds entrypoints only", file, name)
		}
	}
	if checked == 0 {
		t.Fatal("checked no Go files: the walk is wrong, not the layout")
	}
}

// TestBehaviorIsTestableBelowTheEntrypoint backs the "Behavior is testable
// below the entrypoint" scenario's observable part: a cmd/ entrypoint contains
// no logic beyond flags, wiring, and run. Parsing the AST rather than counting
// lines keeps the assertion about what the entrypoint declares: every function
// it defines is a main or an unexported helper, and no entrypoint imports a
// sibling entrypoint, which is how behavior would sneak out of a testable
// internal/ package. The behavior itself is backed by the internal/ packages'
// own tests, recorded in the coverage ledger.
func TestBehaviorIsTestableBelowTheEntrypoint(t *testing.T) {
	checked := 0
	for _, file := range readTree(t, "cmd") {
		if path.Ext(file) != ".go" || strings.HasSuffix(file, "_test.go") {
			continue
		}
		rel := path.Join("cmd", file)
		parsed, err := parser.ParseFile(token.NewFileSet(), rel, readRepoFile(t, rel), parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		checked++
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			if name == "main" || !ast.IsExported(name) {
				continue
			}
			t.Errorf("%s declares exported %s; an entrypoint holds flags, wiring, and run, "+
				"and behavior belongs in a testable internal/ package", rel, name)
		}
		for _, imp := range parsed.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: import path %s: %v", rel, imp.Path.Value, err)
			}
			if strings.HasPrefix(p, "github.com/DisMosGit/fleetops/cmd/") {
				t.Errorf("%s imports %s; entrypoints do not reach into one another", rel, p)
			}
		}
	}
	if checked == 0 {
		t.Fatal("checked no entrypoint files: the walk is wrong, not the layout")
	}
}

// slicesContainPrefix reports whether any prefix matches p.
func slicesContainPrefix(prefixes []string, p string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}
