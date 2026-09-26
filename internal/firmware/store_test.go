package firmware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// fakeMetadata is an in-memory metadata seam with a scriptable insert failure.
type fakeMetadata struct {
	docs      map[string]Record
	byVersion map[string]string
	insertErr error
	existsErr error
}

// newFakeMetadata returns an empty fake metadata collection.
func newFakeMetadata() *fakeMetadata {
	return &fakeMetadata{
		docs:      make(map[string]Record),
		byVersion: make(map[string]string),
	}
}

// insert writes one record, refusing a version the fake already holds.
func (f *fakeMetadata) insert(_ context.Context, rec Record) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	if _, ok := f.byVersion[rec.Version]; ok {
		return fmt.Errorf("insert firmware %s: %w", rec.Version, ErrVersionConflict)
	}
	f.docs[rec.ID] = rec
	f.byVersion[rec.Version] = rec.ID
	return nil
}

// find returns the record with the given id.
func (f *fakeMetadata) find(_ context.Context, id string) (Record, error) {
	rec, ok := f.docs[id]
	if !ok {
		return Record{}, fmt.Errorf("find firmware %s: %w", id, ErrNotFound)
	}
	return rec, nil
}

// versionExists reports whether the fake already holds version.
func (f *fakeMetadata) versionExists(_ context.Context, version string) (bool, error) {
	if f.existsErr != nil {
		return false, f.existsErr
	}
	_, ok := f.byVersion[version]
	return ok, nil
}

// fakeBinaries is an in-memory binaries seam with scriptable failures.
type fakeBinaries struct {
	stored    map[string][]byte
	putErr    error
	openErr   error
	removeErr error
	puts      int
	removed   []string
}

// newFakeBinaries returns an empty fake binary store.
func newFakeBinaries() *fakeBinaries {
	return &fakeBinaries{stored: make(map[string][]byte)}
}

// put stores the reader's bytes whole under a fresh reference.
func (f *fakeBinaries) put(_ context.Context, name string, r io.Reader) (string, int64, error) {
	f.puts++
	if f.putErr != nil {
		return "", 0, f.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	ref := fmt.Sprintf("ref-%d", f.puts)
	f.stored[ref] = data
	return ref, int64(len(data)), nil
}

// open returns a reader over the stored bytes.
func (f *fakeBinaries) open(_ context.Context, ref string) (io.ReadCloser, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	data, ok := f.stored[ref]
	if !ok {
		return nil, fmt.Errorf("open %s: not found", ref)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// remove deletes the stored bytes and remembers the reference.
func (f *fakeBinaries) remove(_ context.Context, ref string) error {
	f.removed = append(f.removed, ref)
	if f.removeErr != nil {
		return f.removeErr
	}
	delete(f.stored, ref)
	return nil
}

// checksumOf returns the lowercase hexadecimal SHA-256 digest of data.
func checksumOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// payload builds deterministic content of n bytes.
func payload(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func TestSave(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		content []byte
	}{
		{name: "small binary", content: payload(128)},
		{name: "multi-buffer binary", content: payload(1 << 20)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			meta := newFakeMetadata()
			bins := newFakeBinaries()
			store := &Store{meta: meta, binaries: bins}

			rec, err := store.Save(context.Background(), "2.0.0", []string{"oak-s3"}, bytes.NewReader(tc.content))
			if err != nil {
				t.Fatalf("Save() error = %v", err)
			}
			if rec.Checksum != checksumOf(tc.content) {
				t.Errorf("checksum = %q, want %q", rec.Checksum, checksumOf(tc.content))
			}
			if rec.Size != int64(len(tc.content)) {
				t.Errorf("size = %d, want %d", rec.Size, len(tc.content))
			}
			if rec.ID == "" || rec.GridFSID == "" || rec.CreatedAt.IsZero() {
				t.Errorf("record = %+v, want id, gridfs id, and creation time set", rec)
			}
			if got := bins.stored[rec.GridFSID]; !bytes.Equal(got, tc.content) {
				t.Errorf("stored binary differs from the uploaded content")
			}
			if stored := meta.docs[rec.ID]; stored.Checksum != rec.Checksum || len(stored.Models) != 1 {
				t.Errorf("stored metadata = %+v, want the returned record", stored)
			}
		})
	}

	t.Run("version is required", func(t *testing.T) {
		t.Parallel()
		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		_, err := store.Save(context.Background(), "", nil, strings.NewReader(""))
		if !errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("Save() error = %v, want ErrInvalidMetadata", err)
		}
	})

	t.Run("taken version stores nothing", func(t *testing.T) {
		t.Parallel()
		meta := newFakeMetadata()
		bins := newFakeBinaries()
		store := &Store{meta: meta, binaries: bins}
		if _, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("a")); err != nil {
			t.Fatalf("Save() first error = %v", err)
		}

		_, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("b"))
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Save() error = %v, want ErrVersionConflict", err)
		}
		if bins.puts != 1 {
			t.Errorf("binary writes = %d, want 1 (the conflicting upload must not be stored)", bins.puts)
		}
		if len(meta.docs) != 1 {
			t.Errorf("stored records = %d, want 1", len(meta.docs))
		}
	})

	t.Run("failed record write deletes the stored binary", func(t *testing.T) {
		t.Parallel()
		meta := newFakeMetadata()
		meta.insertErr = errors.New("write refused")
		bins := newFakeBinaries()
		store := &Store{meta: meta, binaries: bins}

		_, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("binary"))
		if err == nil || errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Save() error = %v, want the record write failure", err)
		}
		if len(bins.stored) != 0 {
			t.Errorf("stored binaries = %d, want 0 (orphan deleted)", len(bins.stored))
		}
		if len(bins.removed) != 1 {
			t.Errorf("removed binaries = %v, want exactly one", bins.removed)
		}
	})

	t.Run("racing duplicate keeps no binary", func(t *testing.T) {
		t.Parallel()
		meta := newFakeMetadata()
		// The unique version index is the race backstop: the insert meets it after the
		// pre-check has passed.
		meta.insertErr = fmt.Errorf("insert firmware 2.0.0: %w", ErrVersionConflict)
		bins := newFakeBinaries()
		store := &Store{meta: meta, binaries: bins}

		_, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("binary"))
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Save() error = %v, want ErrVersionConflict", err)
		}
		if len(bins.stored) != 0 {
			t.Errorf("stored binaries = %d, want 0 (orphan deleted)", len(bins.stored))
		}
	})

	t.Run("both failures surface", func(t *testing.T) {
		t.Parallel()
		meta := newFakeMetadata()
		meta.insertErr = errors.New("write refused")
		bins := newFakeBinaries()
		bins.removeErr = errors.New("delete refused")
		store := &Store{meta: meta, binaries: bins}

		_, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("binary"))
		if err == nil || !strings.Contains(err.Error(), "delete refused") {
			t.Fatalf("Save() error = %v, want it to carry the delete failure too", err)
		}
	})

	t.Run("binary write failure stores nothing", func(t *testing.T) {
		t.Parallel()
		meta := newFakeMetadata()
		bins := newFakeBinaries()
		bins.putErr = errors.New("storage full")
		store := &Store{meta: meta, binaries: bins}

		_, err := store.Save(context.Background(), "2.0.0", nil, strings.NewReader("binary"))
		if err == nil {
			t.Fatal("Save() error = nil, want the binary write failure")
		}
		if len(meta.docs) != 0 {
			t.Errorf("stored records = %d, want 0", len(meta.docs))
		}
	})
}

func TestOpen(t *testing.T) {
	t.Parallel()

	meta := newFakeMetadata()
	bins := newFakeBinaries()
	store := &Store{meta: meta, binaries: bins}
	content := payload(4096)
	rec, err := store.Save(context.Background(), "2.0.0", []string{"oak-s3"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	t.Run("stored binary comes back intact", func(t *testing.T) {
		t.Parallel()
		got, rc, err := store.Open(context.Background(), rec.ID)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer func() {
			if err := rc.Close(); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		}()
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if !bytes.Equal(data, content) {
			t.Error("Open() returned different bytes than were stored")
		}
		if got.Checksum != rec.Checksum || got.GridFSID != rec.GridFSID {
			t.Errorf("Open() record = %+v, want %+v", got, rec)
		}
	})

	t.Run("unknown firmware wraps ErrNotFound", func(t *testing.T) {
		t.Parallel()
		_, _, err := store.Open(context.Background(), "fw-unknown")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Open() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("empty id is refused", func(t *testing.T) {
		t.Parallel()
		_, _, err := store.Open(context.Background(), "")
		if err == nil {
			t.Fatal("Open() error = nil, want an error")
		}
	})

	t.Run("missing binary surfaces the storage failure", func(t *testing.T) {
		t.Parallel()
		orphan := &Store{meta: meta, binaries: newFakeBinaries()}
		_, _, err := orphan.Open(context.Background(), rec.ID)
		if err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("Open() error = %v, want the storage failure", err)
		}
	})
}
