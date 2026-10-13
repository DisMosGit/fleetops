package firmware

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// spoolPrefix marks the files the spool writes. The sweep and the removal only touch files
// carrying it, so a spool directory an operator points at a shared location never has unrelated
// files removed from it.
const spoolPrefix = "upload-"

// Spool is the directory an upload's binary is written to between the request that received it
// and the workflow that stores it. An activity's input is serialized, so the binary reaches the
// storage step as a file path rather than as bytes, and a retried storage step re-reads the same
// file instead of a stream an earlier attempt already consumed. A file is removed once its upload
// concludes; the files a crash abandoned are removed by Sweep at startup.
type Spool struct {
	dir string
	// sequence numbers the files one process writes, so two uploads received in the same
	// nanosecond do not address the same file.
	sequence atomic.Uint64
}

// NewSpool returns the upload spool in dir. The directory is created on the first write, so a
// process that never receives an upload writes nothing anywhere.
func NewSpool(dir string) *Spool {
	return &Spool{dir: dir}
}

// Write streams r into a fresh file in the spool directory and returns its path. A write that
// fails part-way removes the file it started, so a failed spooling leaves nothing behind.
func (s *Spool) Write(r io.Reader) (string, error) {
	if strings.TrimSpace(s.dir) == "" {
		return "", errors.New("spool firmware upload: spool directory required")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return "", fmt.Errorf("spool firmware upload: %w", err)
	}
	name := fmt.Sprintf("%s%d-%d", spoolPrefix, os.Getpid(), s.sequence.Add(1))
	path := filepath.Join(s.dir, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("spool firmware upload: %w", err)
	}
	if _, err := io.Copy(file, r); err != nil {
		return "", errors.Join(
			fmt.Errorf("spool firmware upload: %w", err),
			s.Remove(path))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(
			fmt.Errorf("spool firmware upload: %w", err),
			s.Remove(path))
	}
	return path, nil
}

// Remove deletes one spooled file. A file that is already gone is not an error, so removing the
// spool of an upload that concluded converges when it is called twice, and a path the spool did
// not write is refused rather than deleted.
func (s *Spool) Remove(path string) error {
	if !strings.HasPrefix(filepath.Base(path), spoolPrefix) {
		return fmt.Errorf("remove spooled firmware: %s is not a spool file", path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove spooled firmware %s: %w", path, err)
	}
	return nil
}

// Sweep removes the spool files older than maxAge and returns how many it removed. It is the
// startup backstop for the files a crash abandoned: age is the criterion rather than presence,
// because a file another replica is still uploading through is not this replica's to delete.
func (s *Spool) Sweep(maxAge time.Duration) (int, error) {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("sweep spool %s: %w", s.dir, err)
	}
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), spoolPrefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, fmt.Errorf("sweep spool %s: %w", s.dir, err)
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := s.Remove(filepath.Join(s.dir, entry.Name())); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}
