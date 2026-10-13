package firmware

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// spoolEntries lists the spool's files by name.
func spoolEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool directory: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// writeAt writes a file with the given modification time.
func writeAt(t *testing.T, path string, content string, modTime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set %s modification time: %v", path, err)
	}
}

func TestSpool(t *testing.T) {
	t.Parallel()

	t.Run("a spooled body is the file the storage step reads", func(t *testing.T) {
		t.Parallel()

		spool := NewSpool(filepath.Join(t.TempDir(), "spool"))
		path, err := spool.Write(strings.NewReader("firmware bytes"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		t.Cleanup(func() {
			if err := spool.Remove(path); err != nil {
				t.Errorf("Remove() error = %v", err)
			}
		})

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read spooled file: %v", err)
		}
		if string(data) != "firmware bytes" {
			t.Errorf("spooled content = %q, want the request body", data)
		}
		if filepath.Dir(path) != spool.dir {
			t.Errorf("spooled path = %q, want it under %q", path, spool.dir)
		}
	})

	t.Run("two uploads spool to different files", func(t *testing.T) {
		t.Parallel()

		spool := NewSpool(filepath.Join(t.TempDir(), "spool"))
		first, err := spool.Write(strings.NewReader("one"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		second, err := spool.Write(strings.NewReader("two"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if first == second {
			t.Errorf("both uploads spooled to %q, want one file each", first)
		}
	})

	t.Run("removing a spooled file converges", func(t *testing.T) {
		t.Parallel()

		spool := NewSpool(filepath.Join(t.TempDir(), "spool"))
		path, err := spool.Write(strings.NewReader("one"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := spool.Remove(path); err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
		if err := spool.Remove(path); err != nil {
			t.Fatalf("Remove() on a removed file error = %v, want nil", err)
		}
	})

	t.Run("removing a file the spool did not write is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		foreign := filepath.Join(dir, "unrelated.txt")
		writeAt(t, foreign, "keep me", time.Now())

		spool := NewSpool(dir)
		if err := spool.Remove(foreign); err == nil {
			t.Fatal("Remove() error = nil, want a foreign path refused")
		}
		if _, err := os.Stat(foreign); err != nil {
			t.Errorf("foreign file = %v, want it left in place", err)
		}
	})

	t.Run("a failed write leaves nothing behind", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "spool")
		spool := NewSpool(dir)
		if _, err := spool.Write(failingReader{}); err == nil {
			t.Fatal("Write() error = nil, want the read failure")
		}
		if names := spoolEntries(t, dir); len(names) != 0 {
			t.Errorf("spool files = %v, want none after a failed write", names)
		}
	})

	t.Run("the sweep removes files older than the maximum age only", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		abandoned := filepath.Join(dir, spoolPrefix+"abandoned")
		fresh := filepath.Join(dir, spoolPrefix+"fresh")
		foreign := filepath.Join(dir, "unrelated.txt")
		writeAt(t, abandoned, "old", time.Now().Add(-2*time.Hour))
		writeAt(t, fresh, "new", time.Now())
		writeAt(t, foreign, "old", time.Now().Add(-2*time.Hour))

		removed, err := NewSpool(dir).Sweep(time.Hour)
		if err != nil {
			t.Fatalf("Sweep() error = %v", err)
		}
		if removed != 1 {
			t.Errorf("swept files = %d, want 1", removed)
		}
		if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("abandoned spool file = %v, want it swept", err)
		}
		for _, kept := range []string{fresh, foreign} {
			if _, err := os.Stat(kept); err != nil {
				t.Errorf("%s = %v, want it left in place", filepath.Base(kept), err)
			}
		}
	})

	t.Run("sweeping a directory that does not exist yet is not an error", func(t *testing.T) {
		t.Parallel()

		spool := NewSpool(filepath.Join(t.TempDir(), "never-used"))
		removed, err := spool.Sweep(time.Hour)
		if err != nil {
			t.Fatalf("Sweep() error = %v", err)
		}
		if removed != 0 {
			t.Errorf("swept files = %d, want 0", removed)
		}
	})

	t.Run("a spool directory is required", func(t *testing.T) {
		t.Parallel()

		if _, err := NewSpool("").Write(strings.NewReader("one")); err == nil {
			t.Error("Write() error = nil, want the missing directory reported")
		}
	})
}

// failingReader fails every read, standing in for a request body that breaks mid-upload.
type failingReader struct{}

// Read always fails.
func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("request body aborted")
}
