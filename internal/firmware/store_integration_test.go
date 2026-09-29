//go:build integration

package firmware

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// failingMetadata refuses every insert but delegates reads to the real metadata collection,
// so a test can force the record write to fail after the binary is stored.
type failingMetadata struct {
	inner metadata
}

// insert always fails.
func (f failingMetadata) insert(context.Context, Record) error {
	return errors.New("write refused")
}

// find delegates to the real metadata collection.
func (f failingMetadata) find(ctx context.Context, id string) (Record, error) {
	return f.inner.find(ctx, id)
}

// findByVersion delegates to the real metadata collection.
func (f failingMetadata) findByVersion(ctx context.Context, version string) (Record, error) {
	return f.inner.findByVersion(ctx, version)
}

// versionExists delegates to the real metadata collection.
func (f failingMetadata) versionExists(ctx context.Context, version string) (bool, error) {
	return f.inner.versionExists(ctx, version)
}

func TestStore(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	files := harness.DB.Collection("fs.files")
	chunks := harness.DB.Collection("fs.chunks")
	firmwareDocs := harness.DB.Collection("firmware")

	countFiles := func(t *testing.T) int64 {
		t.Helper()
		n, err := files.CountDocuments(ctx, bson.D{})
		if err != nil {
			t.Fatalf("count GridFS files: %v", err)
		}
		return n
	}

	t.Run("multi-chunk binary round-trips through GridFS", func(t *testing.T) {
		content := payload(1 << 20) // several GridFS chunks
		rec, err := store.Save(ctx, "2.0.0", []string{"oak-s3", "birch-x1"}, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		if rec.Checksum != checksumOf(content) {
			t.Errorf("checksum = %q, want %q", rec.Checksum, checksumOf(content))
		}
		if rec.Size != int64(len(content)) {
			t.Errorf("size = %d, want %d", rec.Size, len(content))
		}

		spanned, err := chunks.CountDocuments(ctx, bson.D{{Key: "files_id", Value: rec.GridFSID}})
		if err != nil {
			t.Fatalf("count GridFS chunks: %v", err)
		}
		if spanned < 2 {
			t.Errorf("GridFS chunks = %d, want a multi-chunk upload", spanned)
		}

		got, rc, err := store.Open(ctx, rec.ID)
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
		if got.GridFSID != rec.GridFSID || got.Version != "2.0.0" {
			t.Errorf("Open() record = %+v, want the stored record", got)
		}

		var doc Record
		if err := firmwareDocs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.ID}}).Decode(&doc); err != nil {
			t.Fatalf("read firmware document: %v", err)
		}
		if len(doc.Models) != 2 || doc.CreatedAt.IsZero() || doc.Size != rec.Size {
			t.Errorf("firmware document = %+v, want models, size, and created_at recorded", doc)
		}
	})

	t.Run("duplicate version meets the unique index", func(t *testing.T) {
		meta := &mongoMetadata{coll: harness.DB.Collection("firmware")}
		rec := Record{
			ID: "fw-dup", Version: "1.5.0", Models: []string{"oak-s3"},
			Checksum: checksumOf([]byte("x")), Size: 1, GridFSID: "fwbin-x",
		}
		if err := meta.insert(ctx, rec); err != nil {
			t.Fatalf("insert() error = %v", err)
		}
		rec.ID = "fw-dup-race"
		if err := meta.insert(ctx, rec); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("insert() duplicate error = %v, want ErrVersionConflict", err)
		}
	})

	t.Run("failed record write leaves no orphan binary", func(t *testing.T) {
		before := countFiles(t)
		broken := &Store{
			meta:     failingMetadata{inner: &mongoMetadata{coll: harness.DB.Collection("firmware")}},
			binaries: &gridfsBinaries{bucket: harness.DB.GridFSBucket()},
		}
		if _, err := broken.Save(ctx, "3.0.0", []string{"oak-s3"}, bytes.NewReader(payload(4096))); err == nil {
			t.Fatal("Save() error = nil, want the record write failure")
		}
		if after := countFiles(t); after != before {
			t.Errorf("GridFS files = %d after the failed upload, want %d (orphan deleted)", after, before)
		}
	})

	t.Run("unknown firmware wraps ErrNotFound", func(t *testing.T) {
		_, _, err := store.Open(ctx, "fw-unknown")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Open() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("saved firmware resolves by version", func(t *testing.T) {
		got, err := store.MetadataByVersion(ctx, "2.0.0")
		if err != nil {
			t.Fatalf("MetadataByVersion() error = %v", err)
		}
		if got.ID == "" || got.Version != "2.0.0" || got.Checksum == "" || len(got.Models) == 0 {
			t.Errorf("MetadataByVersion() = %+v, want the saved record", got)
		}
		if _, err := store.MetadataByVersion(ctx, "9.9.9"); !errors.Is(err, ErrNotFound) {
			t.Errorf("MetadataByVersion() on an unclaimed version = %v, want ErrNotFound", err)
		}
	})

	t.Run("taken version stores nothing", func(t *testing.T) {
		before := countFiles(t)
		if _, err := store.Save(ctx, "2.0.0", []string{"oak-s3"}, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Save() error = %v, want ErrVersionConflict", err)
		}
		if after := countFiles(t); after != before {
			t.Errorf("GridFS files = %d after the refused upload, want %d", after, before)
		}
	})
}
