package firmware

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// spooledFile writes content to a file under a fresh temporary directory and returns its path.
func spooledFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upload-1")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write spooled binary: %v", err)
	}
	return path
}

func TestStoreBinary(t *testing.T) {
	t.Parallel()

	t.Run("streams the spooled binary and reports its checksum", func(t *testing.T) {
		t.Parallel()

		content := payload(1 << 20)
		bins := newFakeBinaries()
		store := &Store{meta: newFakeMetadata(), binaries: bins}

		stored, err := store.StoreBinary(context.Background(), "2.0.0", spooledFile(t, content))
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}
		if stored.Checksum != checksumOf(content) {
			t.Errorf("checksum = %q, want %q", stored.Checksum, checksumOf(content))
		}
		if stored.Size != int64(len(content)) {
			t.Errorf("size = %d, want %d", stored.Size, len(content))
		}
		if !bytes.Equal(bins.stored[stored.Ref], content) {
			t.Error("stored bytes differ from the spooled binary")
		}
		if diff := cmp.Diff([]string{"2.0.0"}, bins.names); diff != "" {
			t.Errorf("stored object names mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a retried step leaves one object per version", func(t *testing.T) {
		t.Parallel()

		bins := newFakeBinaries()
		store := &Store{meta: newFakeMetadata(), binaries: bins}
		path := spooledFile(t, []byte("firmware"))

		first, err := store.StoreBinary(context.Background(), "2.0.0", path)
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}
		second, err := store.StoreBinary(context.Background(), "2.0.0", path)
		if err != nil {
			t.Fatalf("StoreBinary() retry error = %v", err)
		}

		if diff := cmp.Diff(first, second); diff != "" {
			t.Errorf("retried storage mismatch (-first +second):\n%s", diff)
		}
		if len(bins.stored) != 1 {
			t.Errorf("stored objects = %d, want 1 for one version", len(bins.stored))
		}
	})

	t.Run("a second version is stored under its own address", func(t *testing.T) {
		t.Parallel()

		bins := newFakeBinaries()
		store := &Store{meta: newFakeMetadata(), binaries: bins}

		first, err := store.StoreBinary(context.Background(), "2.0.0", spooledFile(t, []byte("a")))
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}
		second, err := store.StoreBinary(context.Background(), "2.1.0", spooledFile(t, []byte("b")))
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}

		if first.Ref == second.Ref {
			t.Errorf("both versions stored at %q, want one address each", first.Ref)
		}
		if len(bins.stored) != 2 {
			t.Errorf("stored objects = %d, want 2", len(bins.stored))
		}
	})

	t.Run("version and binary path are required", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		for _, tc := range []struct {
			name    string
			version string
			path    string
		}{
			{name: "no version", version: "  ", path: spooledFile(t, []byte("a"))},
			{name: "no path", version: "2.0.0", path: ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if _, err := store.StoreBinary(context.Background(), tc.version, tc.path); !errors.Is(err, ErrInvalidMetadata) {
					t.Errorf("StoreBinary() error = %v, want ErrInvalidMetadata", err)
				}
			})
		}
	})

	t.Run("a missing spooled file is reported", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		_, err := store.StoreBinary(context.Background(), "2.0.0", filepath.Join(t.TempDir(), "gone"))
		if err == nil {
			t.Fatal("StoreBinary() error = nil, want the missing file reported")
		}
		if errors.Is(err, ErrNotFound) {
			t.Errorf("StoreBinary() error = %v, want a file error rather than ErrNotFound", err)
		}
	})

	t.Run("a storage failure is reported", func(t *testing.T) {
		t.Parallel()

		bins := newFakeBinaries()
		bins.putErr = errors.New("gridfs unavailable")
		store := &Store{meta: newFakeMetadata(), binaries: bins}

		if _, err := store.StoreBinary(context.Background(), "2.0.0", spooledFile(t, []byte("a"))); err == nil {
			t.Fatal("StoreBinary() error = nil, want the storage failure")
		}
	})
}

func TestUpsertMetadata(t *testing.T) {
	t.Parallel()

	accepted := UploadMetadata{
		Version: "2.0.0", Models: []string{"oak-s3"},
		WorkflowID: "firmware-upload-2.0.0", State: UploadAccepted,
	}

	t.Run("the first transition creates the upload's record", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}

		rec, err := store.UpsertMetadata(context.Background(), accepted)
		if err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}
		if rec.ID == "" || rec.State != UploadAccepted || rec.UploadWorkflowID != accepted.WorkflowID {
			t.Errorf("record = %+v, want an accepted record owned by its workflow", rec)
		}
		if rec.GridFSID != "" || rec.Checksum != "" || rec.Size != 0 {
			t.Errorf("record = %+v, want no binary facts before storage", rec)
		}
		if len(meta.docs) != 1 {
			t.Errorf("records = %d, want 1", len(meta.docs))
		}
	})

	t.Run("later transitions advance the same record", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		ctx := context.Background()

		claim, err := store.UpsertMetadata(ctx, accepted)
		if err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}
		storing := accepted
		storing.State = UploadStoring
		if _, err := store.UpsertMetadata(ctx, storing); err != nil {
			t.Fatalf("UpsertMetadata(storing) error = %v", err)
		}
		available := accepted
		available.State = UploadAvailable
		available.Binary = &StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7}
		rec, err := store.UpsertMetadata(ctx, available)
		if err != nil {
			t.Fatalf("UpsertMetadata(available) error = %v", err)
		}

		if rec.ID != claim.ID {
			t.Errorf("record id = %q, want the claim's %q", rec.ID, claim.ID)
		}
		want := Record{
			ID: claim.ID, Version: "2.0.0", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
			CreatedAt: claim.CreatedAt, GridFSID: "fwbin-1", State: UploadAvailable,
			UploadWorkflowID: accepted.WorkflowID,
		}
		if diff := cmp.Diff(want, rec); diff != "" {
			t.Errorf("record mismatch (-want +got):\n%s", diff)
		}
		if len(meta.docs) != 1 {
			t.Errorf("records = %d, want 1 after three transitions", len(meta.docs))
		}
	})

	t.Run("a retried transition leaves the record unchanged", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		ctx := context.Background()

		if _, err := store.UpsertMetadata(ctx, accepted); err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}
		first, err := store.UpsertMetadata(ctx, accepted)
		if err != nil {
			t.Fatalf("UpsertMetadata() retry error = %v", err)
		}
		second, err := store.UpsertMetadata(ctx, accepted)
		if err != nil {
			t.Fatalf("UpsertMetadata() second retry error = %v", err)
		}

		if diff := cmp.Diff(first, second); diff != "" {
			t.Errorf("retried record mismatch (-first +second):\n%s", diff)
		}
		if len(meta.docs) != 1 {
			t.Errorf("records = %d, want 1 after a retried transition", len(meta.docs))
		}
	})

	t.Run("a transition the record already passed does not regress it", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		ctx := context.Background()

		available := accepted
		available.State = UploadAvailable
		available.Binary = &StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7}
		stored, err := store.UpsertMetadata(ctx, available)
		if err != nil {
			t.Fatalf("UpsertMetadata(available) error = %v", err)
		}

		got, err := store.UpsertMetadata(ctx, accepted)
		if err != nil {
			t.Fatalf("UpsertMetadata(accepted) error = %v", err)
		}
		if diff := cmp.Diff(stored, got); diff != "" {
			t.Errorf("record mismatch (-stored +after late transition):\n%s", diff)
		}
	})

	t.Run("a version another upload owns is refused untouched", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		ctx := context.Background()

		other := accepted
		other.WorkflowID = "firmware-upload-other"
		owned, err := store.UpsertMetadata(ctx, other)
		if err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}

		if _, err := store.UpsertMetadata(ctx, accepted); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("UpsertMetadata() error = %v, want ErrVersionConflict", err)
		}
		if diff := cmp.Diff(owned, meta.docs[owned.ID]); diff != "" {
			t.Errorf("existing record changed (-before +after):\n%s", diff)
		}
	})

	t.Run("a record written before uploads carried a state is refused", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		legacy := Record{
			ID: "fw-legacy", Version: "2.0.0", Models: []string{"oak-s3"},
			Checksum: "sum", Size: 3, GridFSID: "fwbin-legacy",
		}
		if err := meta.insert(context.Background(), legacy); err != nil {
			t.Fatalf("insert() error = %v", err)
		}

		if _, err := store.UpsertMetadata(context.Background(), accepted); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("UpsertMetadata() error = %v, want ErrVersionConflict", err)
		}
		if diff := cmp.Diff(legacy, meta.docs[legacy.ID]); diff != "" {
			t.Errorf("legacy record changed (-before +after):\n%s", diff)
		}
	})

	t.Run("version, workflow, and state are required", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		for _, tc := range []struct {
			name string
			md   UploadMetadata
		}{
			{name: "no version", md: UploadMetadata{WorkflowID: "w", State: UploadAccepted}},
			{name: "no workflow", md: UploadMetadata{Version: "2.0.0", State: UploadAccepted}},
			{name: "unknown state", md: UploadMetadata{Version: "2.0.0", WorkflowID: "w", State: "halfway"}},
			{name: "no state", md: UploadMetadata{Version: "2.0.0", WorkflowID: "w"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if _, err := store.UpsertMetadata(context.Background(), tc.md); !errors.Is(err, ErrInvalidMetadata) {
					t.Errorf("UpsertMetadata() error = %v, want ErrInvalidMetadata", err)
				}
			})
		}
	})
}

func TestDiscardUpload(t *testing.T) {
	t.Parallel()

	t.Run("removes the upload's record and its binary", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		bins := newFakeBinaries()
		store := &Store{meta: meta, binaries: bins}
		ctx := context.Background()
		workflowID := "firmware-upload-2.0.0"

		if _, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: workflowID, State: UploadStoring,
		}); err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}
		stored, err := store.StoreBinary(ctx, "2.0.0", spooledFile(t, []byte("firmware")))
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}

		if err := store.DiscardUpload(ctx, "2.0.0", workflowID); err != nil {
			t.Fatalf("DiscardUpload() error = %v", err)
		}
		if len(meta.docs) != 0 {
			t.Errorf("records = %d, want none after the discard", len(meta.docs))
		}
		if _, ok := bins.stored[stored.Ref]; ok {
			t.Error("binary still stored after the discard")
		}
	})

	t.Run("a record another upload owns is left alone", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		bins := newFakeBinaries()
		store := &Store{meta: meta, binaries: bins}
		ctx := context.Background()

		owned, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: "firmware-upload-2.0.0", State: UploadAvailable,
			Binary: &StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7},
		})
		if err != nil {
			t.Fatalf("UpsertMetadata() error = %v", err)
		}

		if err := store.DiscardUpload(ctx, "2.0.0", "firmware-upload-someone-else"); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("DiscardUpload() error = %v, want ErrVersionConflict", err)
		}
		if diff := cmp.Diff(owned, meta.docs[owned.ID]); diff != "" {
			t.Errorf("record changed (-before +after):\n%s", diff)
		}
	})

	t.Run("discarding twice converges", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		ctx := context.Background()

		if err := store.DiscardUpload(ctx, "2.0.0", "firmware-upload-2.0.0"); err != nil {
			t.Fatalf("DiscardUpload() error = %v", err)
		}
		if err := store.DiscardUpload(ctx, "2.0.0", "firmware-upload-2.0.0"); err != nil {
			t.Fatalf("DiscardUpload() retry error = %v", err)
		}
	})

	t.Run("a version is required", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		if err := store.DiscardUpload(context.Background(), " ", "w"); !errors.Is(err, ErrInvalidMetadata) {
			t.Errorf("DiscardUpload() error = %v, want ErrInvalidMetadata", err)
		}
	})
}

func TestUploadState(t *testing.T) {
	t.Parallel()

	t.Run("an available record reports its firmware facts", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		rec := Record{
			ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
			GridFSID: "fwbin-1", State: UploadAvailable, UploadWorkflowID: "firmware-upload-2.0.0",
		}
		if err := meta.insert(context.Background(), rec); err != nil {
			t.Fatalf("insert() error = %v", err)
		}

		got, err := store.UploadState(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		want := UploadState{
			Version: "2.0.0", State: UploadAvailable, Concluded: true,
			FirmwareID: "fw-1", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a record without a state is unknown, not failed", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		if err := meta.insert(context.Background(), Record{
			ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
			GridFSID: "fwbin-1",
		}); err != nil {
			t.Fatalf("insert() error = %v", err)
		}

		got, err := store.UploadState(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		if got.State != UploadUnknown || got.Concluded || got.Failure != "" {
			t.Errorf("state = %+v, want an unknown, unconcluded state", got)
		}
	})

	t.Run("a failed record reports its reason", func(t *testing.T) {
		t.Parallel()

		meta := newFakeMetadata()
		store := &Store{meta: meta, binaries: newFakeBinaries()}
		if err := meta.insert(context.Background(), Record{
			ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3"},
			State: UploadFailed, FailureReason: "the stored binary could not be recorded",
		}); err != nil {
			t.Fatalf("insert() error = %v", err)
		}

		got, err := store.UploadState(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		if !got.Concluded || got.State != UploadFailed || got.Reason == "" {
			t.Errorf("state = %+v, want a concluded failure carrying its reason", got)
		}
	})

	t.Run("an unknown version wraps ErrNotFound", func(t *testing.T) {
		t.Parallel()

		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		if _, err := store.UploadState(context.Background(), "9.9.9"); !errors.Is(err, ErrNotFound) {
			t.Errorf("UploadState() error = %v, want ErrNotFound", err)
		}
	})
}

// TestUploadClaimMatchesTheFirmwareSchema pins the document a claim writes against the firmware
// collection's validator: the validator requires version, models, checksum, size, created_at, and
// gridfs_id, and an upload's first transition has to satisfy it before its binary exists. Empty
// strings and a zero size are still their declared types, which is what makes the claim writable
// without a migration and without inventing placeholder objects.
func TestUploadClaimMatchesTheFirmwareSchema(t *testing.T) {
	t.Parallel()

	claim, err := (&Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}).
		UpsertMetadata(context.Background(), UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: "firmware-upload-2.0.0", State: UploadAccepted,
		})
	if err != nil {
		t.Fatalf("UpsertMetadata() error = %v", err)
	}

	raw, err := bson.Marshal(claim)
	if err != nil {
		t.Fatalf("marshal the claim: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal the claim: %v", err)
	}

	// Every required field carries the type the validator declares for it.
	if got, ok := doc["version"].(string); !ok || got != "2.0.0" {
		t.Errorf("version = %#v, want the string 2.0.0", doc["version"])
	}
	models, ok := doc["models"].(bson.A)
	if !ok || len(models) != 1 {
		t.Errorf("models = %#v, want a non-empty array", doc["models"])
	}
	if _, ok := doc["checksum"].(string); !ok {
		t.Errorf("checksum = %#v, want a string before the binary is stored", doc["checksum"])
	}
	if _, ok := doc["gridfs_id"].(string); !ok {
		t.Errorf("gridfs_id = %#v, want a string before the binary is stored", doc["gridfs_id"])
	}
	if _, ok := doc["size"].(int64); !ok {
		t.Errorf("size = %#v, want a number before the binary is stored", doc["size"])
	}
	if _, ok := doc["created_at"].(bson.DateTime); !ok {
		t.Errorf("created_at = %#v, want a date", doc["created_at"])
	}
	// The state and its owner are the fields this change adds; the validator declares properties
	// without additionalProperties, so an added field is accepted.
	if got, ok := doc["state"].(string); !ok || got != UploadAccepted {
		t.Errorf("state = %#v, want the string %s", doc["state"], UploadAccepted)
	}
	if got, ok := doc["upload_workflow_id"].(string); !ok || got != "firmware-upload-2.0.0" {
		t.Errorf("upload_workflow_id = %#v, want the owning execution", doc["upload_workflow_id"])
	}
}
