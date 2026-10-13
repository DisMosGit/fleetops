//go:build integration

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

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// spooledBinary writes content to a file the storage step streams from and returns its path.
func spooledBinary(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upload-1")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write spooled binary: %v", err)
	}
	return path
}

// TestUploadStepsOnMongo pins the upload's steps against the real collections and their schema
// bootstrap: an upload's first transition is writable before its binary exists, storage is
// addressed by version so a retry leaves one object, a retried transition leaves one record, and
// a discarded upload leaves neither.
func TestUploadStepsOnMongo(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	files := harness.DB.Collection("fs.files")
	firmwareDocs := harness.DB.Collection("firmware")

	path := spooledBinary(t, []byte("firmware bytes"))
	workflowID := "firmware-upload-2.0.0"

	countFirmware := func(t *testing.T, version string) int64 {
		t.Helper()
		n, err := firmwareDocs.CountDocuments(ctx, bson.D{{Key: "version", Value: version}})
		if err != nil {
			t.Fatalf("count firmware documents: %v", err)
		}
		return n
	}
	countFiles := func(t *testing.T, ref string) int64 {
		t.Helper()
		n, err := files.CountDocuments(ctx, bson.D{{Key: "_id", Value: ref}})
		if err != nil {
			t.Fatalf("count GridFS files: %v", err)
		}
		return n
	}

	t.Run("the claim is written before the binary exists", func(t *testing.T) {
		rec, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: workflowID, State: UploadAccepted,
		})
		if err != nil {
			t.Fatalf("UpsertMetadata(accepted) error = %v", err)
		}
		if rec.State != UploadAccepted || rec.UploadWorkflowID != workflowID {
			t.Errorf("record = %+v, want an accepted record owned by the upload", rec)
		}
		if _, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: workflowID, State: UploadStoring,
		}); err != nil {
			t.Fatalf("UpsertMetadata(storing) error = %v", err)
		}

		stored, err := store.StoreBinary(ctx, "2.0.0", path)
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}
		retried, err := store.StoreBinary(ctx, "2.0.0", path)
		if err != nil {
			t.Fatalf("StoreBinary() retry error = %v", err)
		}
		if retried.Ref != stored.Ref || retried.Checksum != stored.Checksum || retried.Size != stored.Size {
			t.Errorf("retry = %+v, want the same object as %+v", retried, stored)
		}
		if got := countFiles(t, stored.Ref); got != 1 {
			t.Errorf("GridFS objects = %d, want 1 after a retried storage step", got)
		}

		available, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: workflowID, State: UploadAvailable, Binary: &stored,
		})
		if err != nil {
			t.Fatalf("UpsertMetadata(available) error = %v", err)
		}
		if available.State != UploadAvailable || available.Checksum != stored.Checksum {
			t.Errorf("record = %+v, want available with the stored checksum", available)
		}
		if _, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"oak-s3"},
			WorkflowID: workflowID, State: UploadAvailable, Binary: &stored,
		}); err != nil {
			t.Fatalf("UpsertMetadata(available) retry error = %v", err)
		}
		if got := countFirmware(t, "2.0.0"); got != 1 {
			t.Errorf("firmware records = %d, want 1 after a retried metadata write", got)
		}

		state, err := store.UploadState(ctx, "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		want := UploadState{
			Version: "2.0.0", State: UploadAvailable, Concluded: true,
			FirmwareID: available.ID, Models: []string{"oak-s3"},
			Checksum: stored.Checksum, Size: stored.Size,
		}
		if diff := cmp.Diff(want, state); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a version another upload owns is refused untouched", func(t *testing.T) {
		before, err := store.UploadState(ctx, "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}

		_, err = store.UpsertMetadata(ctx, UploadMetadata{
			Version: "2.0.0", Models: []string{"birch-x1"},
			WorkflowID: "firmware-upload-other", State: UploadAccepted,
		})
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("UpsertMetadata() error = %v, want ErrVersionConflict", err)
		}
		after, err := store.UploadState(ctx, "2.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		if diff := cmp.Diff(before, after); diff != "" {
			t.Errorf("record changed (-before +after):\n%s", diff)
		}
	})

	t.Run("discarding an unrecordable upload leaves neither record nor binary", func(t *testing.T) {
		stored, err := store.StoreBinary(ctx, "3.0.0", path)
		if err != nil {
			t.Fatalf("StoreBinary() error = %v", err)
		}
		if _, err := store.UpsertMetadata(ctx, UploadMetadata{
			Version: "3.0.0", Models: []string{"oak-s3"},
			WorkflowID: "firmware-upload-3.0.0", State: UploadStoring,
		}); err != nil {
			t.Fatalf("UpsertMetadata(storing) error = %v", err)
		}

		if err := store.DiscardUpload(ctx, "3.0.0", "firmware-upload-3.0.0"); err != nil {
			t.Fatalf("DiscardUpload() error = %v", err)
		}
		if got := countFirmware(t, "3.0.0"); got != 0 {
			t.Errorf("firmware records = %d, want none after the discard", got)
		}
		if got := countFiles(t, stored.Ref); got != 0 {
			t.Errorf("GridFS objects = %d, want none after the discard", got)
		}
		if err := store.DiscardUpload(ctx, "3.0.0", "firmware-upload-3.0.0"); err != nil {
			t.Errorf("DiscardUpload() retry error = %v, want it to converge", err)
		}
	})

	t.Run("a record written without a state reads as unknown", func(t *testing.T) {
		if _, err := store.Save(ctx, "4.0.0", []string{"oak-s3"}, bytes.NewReader([]byte("legacy"))); err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		state, err := store.UploadState(ctx, "4.0.0")
		if err != nil {
			t.Fatalf("UploadState() error = %v", err)
		}
		if state.State != UploadUnknown || state.Concluded || state.Reason != "" {
			t.Errorf("state = %+v, want an unknown, unconcluded record", state)
		}
	})
}
