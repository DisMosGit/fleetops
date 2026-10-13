//go:build integration

package firmware

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// metadataFields are the fields one firmware document may carry: the metadata this scenario names
// and nothing else. A payload smuggled into the document would have to arrive as a field this set
// does not name.
var metadataFields = map[string]bool{
	"_id": true, "version": true, "models": true, "checksum": true, "size": true,
	"created_at": true, "gridfs_id": true,
}

// TestMetadataNeverHoldsPayload backs the mongo-data-model scenario "Metadata never holds
// payloads": reading a firmware document exposes version, models, checksum, size, created_at, and
// gridfs_id, but no inline binary content.
func TestMetadataNeverHoldsPayload(t *testing.T) {
	t.Parallel()

	harness := mongotest.For(t)
	store := NewStore(harness.DB)
	ctx := context.Background()
	firmwareDocs := harness.DB.Collection("firmware")

	// A payload carrying a marker no metadata field could plausibly hold, and large enough that
	// an inlined binary would show up in the document's size as well as in its fields.
	marker := []byte("fleetops-inline-payload-marker")
	content := bytes.Repeat([]byte("firmware-binary-block"), 32*1024)
	copy(content[4096:], marker)

	rec, err := store.Save(ctx, "4.2.0", []string{"oak-s3", "birch-x1"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// The typed read is what the scenario says a reader is exposed to.
	var doc Record
	if err := firmwareDocs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.ID}}).Decode(&doc); err != nil {
		t.Fatalf("read the firmware document: %v", err)
	}
	if doc.Version != "4.2.0" {
		t.Errorf("the firmware document's version = %q, want %q", doc.Version, "4.2.0")
	}
	if want := []string{"oak-s3", "birch-x1"}; !slices.Equal(doc.Models, want) {
		t.Errorf("the firmware document's models = %v, want %v", doc.Models, want)
	}
	if doc.Checksum != checksumOf(content) {
		t.Errorf("the firmware document's checksum = %q, want %q", doc.Checksum, checksumOf(content))
	}
	if doc.Size != int64(len(content)) {
		t.Errorf("the firmware document's size = %d, want %d", doc.Size, len(content))
	}
	if doc.CreatedAt.IsZero() {
		t.Error("the firmware document carries no created_at")
	}
	if doc.GridFSID != rec.GridFSID {
		t.Errorf("the firmware document's gridfs_id = %q, want %q", doc.GridFSID, rec.GridFSID)
	}

	// The raw read is what the scenario forbids: a payload stored under a field the Record struct
	// does not name would still be visible in the document itself.
	var raw bson.M
	if err := firmwareDocs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.ID}}).Decode(&raw); err != nil {
		t.Fatalf("read the raw firmware document: %v", err)
	}
	for field := range raw {
		if !metadataFields[field] {
			t.Errorf("the firmware document carries the unexpected field %q", field)
		}
	}
	document, err := bson.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal the firmware document: %v", err)
	}
	if bytes.Contains(document, marker) {
		t.Error("the firmware document carries the payload's bytes inline")
	}
	if len(document) >= len(content)/100 {
		t.Errorf("the firmware document is %d bytes for a %d-byte binary, want metadata only",
			len(document), len(content))
	}

	// The payload is where the record says it is: the GridFS object gridfs_id references, holding
	// the whole binary.
	var stored struct {
		Length int64 `bson:"length"`
	}
	if err := harness.DB.Collection("fs.files").
		FindOne(ctx, bson.D{{Key: "_id", Value: doc.GridFSID}}).Decode(&stored); err != nil {
		t.Fatalf("read the GridFS object %s: %v", doc.GridFSID, err)
	}
	if stored.Length != int64(len(content)) {
		t.Errorf("the GridFS object holds %d bytes, want the uploaded %d", stored.Length, len(content))
	}
}
