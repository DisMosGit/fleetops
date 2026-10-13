package protocodegen

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// makeTarget is the documented, repeatable regeneration command: the Makefile
// target that regenerates both client and server stubs from every contract.
const makeTarget = "proto"

// treeDigest hashes every file under dir, keyed by its slash-separated path, so
// two trees compare byte for byte and a stray file shows up as a key.
func treeDigest(t *testing.T, dir string) map[string]string {
	t.Helper()

	out := make(map[string]string)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = fmt.Sprintf("%x", sha256.Sum256(data))
		return nil
	})
	if err != nil {
		t.Fatalf("digest %s: %v", dir, err)
	}
	if len(out) == 0 {
		t.Fatalf("digest %s: no files", dir)
	}
	return out
}

// workTree copies the named repository paths into a fresh temp directory in the
// repository's own layout and returns it. The regeneration tests run there, so
// protoc and 'goimports -w api/proto' rewrite the copy and the committed stubs
// stay exactly as the checkout left them.
func workTree(t *testing.T, fsys fs.FS, paths ...string) string {
	t.Helper()

	dir := t.TempDir()
	for _, p := range paths {
		copyPath(t, fsys, p, dir)
	}
	return dir
}

// copyPath copies one file or directory from fsys into dst, preserving the
// repository-relative layout.
func copyPath(t *testing.T, fsys fs.FS, src, dst string) {
	t.Helper()

	info, err := fs.Stat(fsys, src)
	if err != nil {
		t.Fatalf("stat %s: %v", src, err)
	}
	if !info.IsDir() {
		writeCopy(t, fsys, src, filepath.Join(dst, filepath.FromSlash(src)))
		return
	}
	err = fs.WalkDir(fsys, src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		target := filepath.Join(dst, filepath.FromSlash(p))
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		writeCopy(t, fsys, p, target)
		return nil
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
}

// writeCopy writes one file from fsys to target, creating its directory.
func writeCopy(t *testing.T, fsys fs.FS, src, target string) {
	t.Helper()

	data, err := fs.ReadFile(fsys, src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(target), err)
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", target, err)
	}
}

// runMakeProto runs the copied Makefile's proto target inside dir with the
// pinned toolchain on PATH and returns make's combined output. dir is a temp
// tree, so the run writes nothing into the working tree.
func runMakeProto(t *testing.T, dir string, tc toolchain) string {
	t.Helper()

	cmd := exec.Command(makePath(t), makeTarget)
	cmd.Dir = dir
	cmd.Env = withPath(toolchainPath(tc))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make %s in %s: %v\n%s", makeTarget, dir, err, out)
	}
	return string(out)
}

// removeStubs deletes the generated stubs in a temp tree before a regeneration
// run, so the run's exit zero and the byte comparison prove the command
// produces every stub itself instead of leaving stale committed copies in
// place.
func removeStubs(t *testing.T, dir string) {
	t.Helper()

	removed := 0
	err := filepath.WalkDir(filepath.Join(dir, "api", "proto"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".pb.go") {
			return nil
		}
		if err := os.Remove(p); err != nil {
			return err
		}
		removed++
		return nil
	})
	if err != nil {
		t.Fatalf("remove stubs in %s: %v", dir, err)
	}
	if removed == 0 {
		t.Fatalf("no stubs removed in %s; the run would only be comparing committed files with themselves", dir)
	}
}

// committedProtocHint names the compiler release the committed stubs record, so
// a byte mismatch caused by protoc version skew reads as the version problem it
// is rather than as a mysterious diff.
func committedProtocHint(t *testing.T, fsys fs.FS) string {
	t.Helper()

	return fmt.Sprintf("the committed stubs record protoc %s; install that release to reproduce them", committedProtocVersion(t, fsys))
}

// TestRegenerationReproducesCommittedStubs backs "Regenerating the full stub
// surface" and "Reproducible across machines": the repeatable command exits
// zero, writes client and server stubs for every service in every contract, and
// its output is byte-identical to the committed stubs — the committed bytes
// being what a contributor on another machine produced from the same contracts
// with the same pins. Dropping a generator from the command, or a generator
// that stops reproducing the committed output, fails here.
func TestRegenerationReproducesCommittedStubs(t *testing.T) {
	fsys := os.DirFS(repoRoot)
	tc := requirePinnedToolchain(t, fsys)
	dir := workTree(t, fsys, "Makefile", "api/proto")

	// Start without the committed stubs: only then does a zero exit with every
	// stub back in place prove the command produced the full surface, rather
	// than a stale committed copy staying put.
	removeStubs(t, dir)

	runMakeProto(t, dir, tc)

	want := treeDigest(t, filepath.Join(repoRoot, "api", "proto"))
	got := treeDigest(t, filepath.Join(dir, "api", "proto"))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("regenerated tree differs from the committed one, byte for byte (-committed +regenerated):\n%s\n%s",
			diff, committedProtocHint(t, fsys))
	}

	// "fresh client and server stubs for every service in every contract file":
	// assert each service's client and server symbols in the stub the run just
	// wrote, not only that some file changed.
	regenerated := os.DirFS(dir)
	for _, c := range loadContracts(t, fsys) {
		for _, s := range stubsFor(c) {
			if !strings.HasSuffix(s.path, "_grpc.pb.go") {
				continue
			}
			src, err := fs.ReadFile(regenerated, s.path)
			if err != nil {
				t.Errorf("%s: no gRPC stub regenerated for %s: %v", s.path, c.path, err)
				continue
			}
			for _, service := range c.services {
				for _, symbol := range []string{service + "Client", service + "Server"} {
					if !strings.Contains(string(src), symbol) {
						t.Errorf("%s: no %s after regeneration; every service in %s needs fresh client and server stubs", s.path, symbol, c.path)
					}
				}
			}
		}
	}
}

// TestRegenerationIsIdempotent backs "Regeneration is idempotent": the second
// run of the command, with no contract change in between, leaves the whole tree
// unchanged, so a regenerate-then-commit workflow produces no spurious diff.
func TestRegenerationIsIdempotent(t *testing.T) {
	fsys := os.DirFS(repoRoot)
	tc := requirePinnedToolchain(t, fsys)
	dir := workTree(t, fsys, "Makefile", "api/proto")

	runMakeProto(t, dir, tc)
	first := treeDigest(t, dir)

	runMakeProto(t, dir, tc)
	second := treeDigest(t, dir)

	if diff := cmp.Diff(first, second); diff != "" {
		t.Errorf("the second make %s changed the tree (-first +second):\n%s", makeTarget, diff)
	}
}

// TestHandEditsDoNotSurviveRegeneration backs "Hand edits do not survive":
// appending a hand edit to a committed stub and running the command restores
// the canonical generator output, so the contract stays the source of truth.
func TestHandEditsDoNotSurviveRegeneration(t *testing.T) {
	fsys := os.DirFS(repoRoot)
	tc := requirePinnedToolchain(t, fsys)
	dir := workTree(t, fsys, "Makefile", "api/proto")

	// Append rather than rewrite in place: a comment at the end is a legitimate
	// hand edit that survives gofmt and goimports, so only regeneration
	// restoring the file can remove it.
	const target = "api/proto/agent/v1/agent.pb.go"
	const marker = "\n// hand edit: regeneration must remove this\n"
	committed, err := fs.ReadFile(fsys, target)
	if err != nil {
		t.Fatalf("read %s: %v", target, err)
	}
	edited := filepath.Join(dir, filepath.FromSlash(target))
	if err := os.WriteFile(edited, append(append([]byte(nil), committed...), marker...), 0o644); err != nil {
		t.Fatalf("hand-edit %s: %v", edited, err)
	}

	runMakeProto(t, dir, tc)

	got, err := os.ReadFile(edited)
	if err != nil {
		t.Fatalf("read %s after regeneration: %v", target, err)
	}
	if strings.Contains(string(got), "hand edit") {
		t.Errorf("%s still carries the hand edit after make %s; the regeneration command is the only way generated files change", target, makeTarget)
	}
	if diff := cmp.Diff(string(committed), string(got)); diff != "" {
		t.Errorf("%s is not the canonical generator output after regeneration (-committed +regenerated):\n%s", target, diff)
	}
}
