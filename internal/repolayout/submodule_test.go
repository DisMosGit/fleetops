package repolayout

import (
	"os"
	"path"
	"strings"
	"testing"
)

// TestFrontendSubmoduleRegistrationIsExplicit backs the "Submodule
// registration is explicit" scenario's registration half: .gitmodules names the
// web/ path and the URL of the frontend repository, so removing or repointing
// the registration fails here.
func TestFrontendSubmoduleRegistrationIsExplicit(t *testing.T) {
	entries := parseGitmodules(t, string(readRepoFile(t, ".gitmodules")))
	entry, ok := entries["web"]
	if !ok {
		t.Fatalf(".gitmodules registers no web submodule; it declares %v", submoduleNames(entries))
	}
	if entry["path"] != "web" {
		t.Errorf(".gitmodules web submodule path is %q, want %q", entry["path"], "web")
	}
	// The URL is pinned exactly: repointing the submodule at another
	// repository is a change to the registration, not an implementation
	// detail, and the spec names this repository.
	const wantURL = "git@github.com:DisMosGit/fleetops-web.git"
	if entry["url"] != wantURL {
		t.Errorf(".gitmodules web submodule url is %q, want %q", entry["url"], wantURL)
	}
}

// TestFrontendSubmoduleNamesTheStackAndStage backs the "Submodule registration
// is explicit" scenario's placeholder half: the note in web/ names the frontend
// stack and the stage that delivers it. The note is absent exactly when the
// submodule is uninitialized, which is the state the "Usable clone without
// submodule content" scenario requires to keep working, so an uninitialized
// checkout is not a failure.
func TestFrontendSubmoduleNamesTheStackAndStage(t *testing.T) {
	if _, err := os.Stat(path.Join(repoRoot, "web/README.md")); err != nil {
		if os.IsNotExist(err) {
			t.Log("web/ holds no placeholder note because the submodule is not initialized; " +
				"the registration and the absence of vendored sources are asserted separately")
			return
		}
		t.Fatalf("stat web/README.md: %v", err)
	}
	note := string(readRepoFile(t, "web/README.md"))
	for _, want := range []string{"React", "Vite", "stage 5"} {
		if !strings.Contains(note, want) {
			t.Errorf("web/README.md does not name %q, so the placeholder does not say what will fill it", want)
		}
	}
}

// TestWebIsPlaceholderOnly backs the "Usable clone without submodule content"
// scenario: web/ is registered but holds no frontend sources in the parent
// repository, so a clone that has not initialized the submodule still builds,
// tests, and lints. Both the index and the working tree are checked, because
// they disagree exactly when a contributor's checkout has more than a fresh
// clone would. A checkout with no web/ directory at all is the uninitialized
// case and passes: the submodule is registered in .gitmodules, which the other
// assertion covers.
func TestWebIsPlaceholderOnly(t *testing.T) {
	// "web/ is registered": the guarantee is about a registered submodule, so the
	// registration is asserted rather than assumed — an untracked empty web/
	// would otherwise satisfy the placeholder checks on its own.
	entries := parseGitmodules(t, string(readRepoFile(t, ".gitmodules")))
	if _, ok := entries["web"]; !ok {
		t.Errorf(".gitmodules registers no web submodule, so web/ is not a registered placeholder; it declares %v",
			submoduleNames(entries))
	}

	// The parent repository must not track anything under web/ except the
	// submodule itself: a tracked file would arrive in a clone as working
	// frontend content the stage has not delivered. Reading the git index is
	// what makes this independent of whether the working tree has the
	// submodule initialized.
	tracked := gitLsFiles(t, "web")
	if len(tracked) != 1 || tracked[0] != "web" {
		t.Errorf("the parent repository tracks %v under web/, want exactly the submodule gitlink", tracked)
	}

	if _, err := os.Stat(path.Join(repoRoot, "web")); err != nil {
		t.Log("web/ is not checked out because the submodule is not initialized")
		return
	}

	// And the working tree must hold no frontend sources. The submodule's own
	// sources live in that repository; the parent side carries at most the
	// submodule marker and the placeholder note.
	for _, file := range readTree(t, "web") {
		switch name := path.Base(file); name {
		case ".git", "README.md":
			continue
		default:
			if frontendSource(name) {
				t.Errorf("web/%s is frontend content in the parent repository; web/ must stay placeholder-only", file)
			}
		}
	}
}
