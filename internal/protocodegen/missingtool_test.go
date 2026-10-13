package protocodegen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// cleanEnv is the environment for the missing-toolchain runs: PATH offers only
// the fake tools in bin plus the directories make and go need, and GOPATH is an
// empty directory so the Makefile's own "GOPATH/bin first" PATH line cannot
// reintroduce the real plugins the case hides.
func cleanEnv(t *testing.T, bin, goDir string) []string {
	t.Helper()

	return []string{
		"PATH=" + strings.Join([]string{bin, goDir, "/usr/bin", "/bin"}, string(os.PathListSeparator)),
		"GOPATH=" + t.TempDir(),
		"HOME=" + t.TempDir(),
	}
}

// writeStubTool writes a fake tool that exits zero, so a case can present the
// tools before the one it hides without running a real generator.
func writeStubTool(t *testing.T, bin, name string) {
	t.Helper()

	const script = "#!/bin/sh\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
}

// TestMissingToolchainFailsLoudly backs "Missing toolchain fails loudly": with
// protoc, a plugin, or goimports absent, `make proto` exits non-zero with a
// message naming the missing tool and how the toolchain is provided, and the
// failed run leaves no partially generated output behind.
func TestMissingToolchainFailsLoudly(t *testing.T) {
	fsys := os.DirFS(repoRoot)
	makeBin := makePath(t)
	goCmdDir := goDir(t)

	cases := []struct {
		name    string
		present []string
		want    []string
	}{
		{
			name: "protoc missing",
			want: []string{"protoc not found", "install protoc"},
		},
		{
			name:    "protoc-gen-go missing",
			present: []string{"protoc"},
			want:    []string{"protoc-gen-go not found", "make proto-tools"},
		},
		{
			name:    "protoc-gen-go-grpc missing",
			present: []string{"protoc", "protoc-gen-go"},
			want:    []string{"protoc-gen-go-grpc not found", "make proto-tools"},
		},
		{
			name:    "goimports missing",
			present: []string{"protoc", "protoc-gen-go", "protoc-gen-go-grpc"},
			want:    []string{"goimports not found", "make proto-tools"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := workTree(t, fsys, "Makefile", "api/proto")
			bin := t.TempDir()
			for _, tool := range tc.present {
				writeStubTool(t, bin, tool)
			}
			before := treeDigest(t, dir)

			cmd := exec.Command(makeBin, makeTarget)
			cmd.Dir = dir
			cmd.Env = cleanEnv(t, bin, goCmdDir)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("make %s succeeded with %s; want a non-zero exit naming the missing tool:\n%s", makeTarget, tc.name, out)
			}
			for _, want := range tc.want {
				if !strings.Contains(string(out), want) {
					t.Errorf("failure output does not contain %q; the reader must be told what is missing and how to provide it:\n%s", want, out)
				}
			}
			if diff := cmp.Diff(before, treeDigest(t, dir)); diff != "" {
				t.Errorf("the failed run changed the tree (-before +after); a failing command must not leave partial output:\n%s", diff)
			}
		})
	}
}
