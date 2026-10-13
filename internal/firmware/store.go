package firmware

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Record is one firmware document: the metadata of one uploaded firmware. The binary itself
// lives in GridFS and is referenced by GridFSID — the document never carries payload bytes. The
// record is also where its upload's state is recorded, so a reader can tell an upload still in
// progress from one that concluded without asking the workflow.
type Record struct {
	// ID is the firmware identity; the _id of the firmware document.
	ID string `bson:"_id" json:"id"`
	// Version is the firmware version, unique across the registry.
	Version string `bson:"version" json:"version"`
	// Models are the device models the firmware targets, as declared at upload.
	Models []string `bson:"models" json:"models"`
	// Checksum is the lowercase hexadecimal SHA-256 digest of the complete binary. It is empty
	// until the upload's binary has been stored.
	Checksum string `bson:"checksum" json:"checksum"`
	// Size is the stored binary's size in bytes; zero until the binary has been stored.
	Size int64 `bson:"size" json:"size"`
	// CreatedAt is the time the upload was recorded.
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	// GridFSID references the binary stored in GridFS; empty until the binary has been stored.
	GridFSID string `bson:"gridfs_id" json:"gridfs_id"`
	// State is the state the upload that wrote this record has reached. A record written before
	// uploads carried a state has none, which a reader reports as UploadUnknown rather than as
	// a failure.
	State string `bson:"state,omitempty" json:"state,omitempty"`
	// UploadWorkflowID is the id of the workflow execution that owns the upload.
	UploadWorkflowID string `bson:"upload_workflow_id,omitempty" json:"upload_workflow_id,omitempty"`
	// FailureReason is the operator-safe reason the upload concluded failed; empty otherwise.
	FailureReason string `bson:"failure_reason,omitempty" json:"failure_reason,omitempty"`
}

// metadata is the firmware metadata collection the store records into. The Mongo-backed
// implementation is built by NewStore; tests hand-write a fake.
type metadata interface {
	// insert writes one firmware document, failing with ErrVersionConflict when its
	// version is already taken.
	insert(ctx context.Context, rec Record) error
	// find returns the firmware document with the given id, or an error wrapping ErrNotFound.
	find(ctx context.Context, id string) (Record, error)
	// findByVersion returns the firmware document carrying version, or an error wrapping
	// ErrNotFound. Versions are unique, so the lookup has at most one answer.
	findByVersion(ctx context.Context, version string) (Record, error)
	// versionExists reports whether a document already carries version.
	versionExists(ctx context.Context, version string) (bool, error)
}

// metadataTransitions is the part of the metadata seam that carries an upload's record through
// its states: creating it on the upload's first transition, advancing it on later ones, and
// removing it when an upload cannot complete what it stored. It is a seam of its own rather than
// part of metadata, so a metadata implementation that only supports the registry's plain writes
// stays a valid seam — and a store whose metadata cannot record transitions fails loudly instead
// of dropping an upload's state. The Mongo-backed implementation supports both.
type metadataTransitions interface {
	// upsert writes one transition of an upload's record: it creates the record on the
	// upload's first transition and advances it on a later one, leaving the stored record
	// unchanged when the transition has already been recorded or when a later state has been.
	// A record the upload does not own — another upload's, or one written before uploads
	// carried a state — fails with ErrVersionConflict.
	upsert(ctx context.Context, rec Record) (Record, error)
	// removeClaim deletes the record an upload owns, so an upload that stored nothing usable
	// leaves no record behind. A record that is not this upload's is left alone.
	removeClaim(ctx context.Context, version, workflowID string) error
}

// binaries is the binary storage the store keeps firmware payloads in. put consumes r in
// bounded pieces without buffering it whole and returns the reference of the stored object;
// open streams exactly the stored bytes back; remove deletes the object, succeeding on one
// that is already gone so cleanup stays idempotent.
type binaries interface {
	// put streams r into the object addressed by ref, replacing whatever that address holds,
	// and returns the number of bytes stored.
	put(ctx context.Context, ref, name string, r io.Reader) (int64, error)
	// open returns a reader over the object addressed by ref.
	open(ctx context.Context, ref string) (io.ReadCloser, error)
	// remove deletes the object addressed by ref, succeeding on one that is already gone.
	remove(ctx context.Context, ref string) error
}

// Store is the firmware registry on the fleet database: metadata documents in the firmware
// collection and binaries in GridFS.
type Store struct {
	meta     metadata
	binaries binaries
}

// NewStore returns the firmware registry on the fleet database: metadata in the firmware
// collection, binaries in its GridFS bucket.
func NewStore(db *mongo.Database) *Store {
	return &Store{
		meta:     &mongoMetadata{coll: db.Collection("firmware")},
		binaries: &gridfsBinaries{bucket: db.GridFSBucket()},
	}
}

// Save stores one firmware binary and its metadata record: the checksum is computed as the
// binary streams into storage, and the whole binary is never held in memory. The version must
// be unclaimed — a taken version fails with ErrVersionConflict and stores nothing — and a
// binary whose record cannot be written is deleted again, so a rejected upload never leaves
// an orphaned binary behind.
func (s *Store) Save(ctx context.Context, version string, models []string, binary io.Reader) (Record, error) {
	if version == "" {
		return Record{}, fmt.Errorf("save firmware: %w: version required", ErrInvalidMetadata)
	}
	taken, err := s.meta.versionExists(ctx, version)
	if err != nil {
		return Record{}, fmt.Errorf("claim firmware version %s: %w", version, err)
	}
	if taken {
		return Record{}, fmt.Errorf("save firmware: %w: %s", ErrVersionConflict, version)
	}

	id, err := newID("fw")
	if err != nil {
		return Record{}, fmt.Errorf("firmware id: %w", err)
	}
	ref, err := newID("fwbin")
	if err != nil {
		return Record{}, fmt.Errorf("firmware binary reference: %w", err)
	}
	hash := sha256.New()
	size, err := s.binaries.put(ctx, ref, id, io.TeeReader(binary, hash))
	if err != nil {
		return Record{}, fmt.Errorf("store firmware binary %s: %w", version, err)
	}

	rec := Record{
		ID:        id,
		Version:   version,
		Models:    models,
		Checksum:  hex.EncodeToString(hash.Sum(nil)),
		Size:      size,
		CreatedAt: time.Now().UTC(),
		GridFSID:  ref,
	}
	if err := s.meta.insert(ctx, rec); err != nil {
		// The binary is worthless without its record: delete it instead of leaving an
		// orphan, and report both errors when even that fails.
		if rmErr := s.binaries.remove(ctx, ref); rmErr != nil {
			return Record{}, errors.Join(
				fmt.Errorf("record firmware %s: %w", version, err),
				fmt.Errorf("remove orphan binary of %s: %w", version, rmErr))
		}
		return Record{}, fmt.Errorf("record firmware %s: %w", version, err)
	}
	return rec, nil
}

// Metadata returns the metadata record of id without touching its stored binary — what a caller
// needs to decide whether a firmware may be deployed, and to name its version and checksum in the
// command that deploys it. An unknown id fails with an error wrapping ErrNotFound.
func (s *Store) Metadata(ctx context.Context, id string) (Record, error) {
	if id == "" {
		return Record{}, errors.New("read firmware metadata: firmware id required")
	}
	return s.meta.find(ctx, id)
}

// MetadataByVersion returns the metadata record carrying version, without touching its stored
// binary. It is how a caller that knows only a version — a device reporting the firmware it ran,
// or a rollback restoring a device to it — obtains the firmware's id, checksum, and target
// models. A version the registry does not hold fails with an error wrapping ErrNotFound, which
// is a different answer from a record with empty fields.
func (s *Store) MetadataByVersion(ctx context.Context, version string) (Record, error) {
	if version == "" {
		return Record{}, errors.New("read firmware metadata: firmware version required")
	}
	return s.meta.findByVersion(ctx, version)
}

// Open returns the firmware record of id and a reader over its stored binary. An unknown id
// fails with an error wrapping ErrNotFound.
func (s *Store) Open(ctx context.Context, id string) (Record, io.ReadCloser, error) {
	rec, err := s.Metadata(ctx, id)
	if err != nil {
		return Record{}, nil, err
	}
	rc, err := s.binaries.open(ctx, rec.GridFSID)
	if err != nil {
		return Record{}, nil, fmt.Errorf("open firmware binary %s: %w", id, err)
	}
	return rec, rc, nil
}

// newID mints a prefixed random identifier for a firmware record or a binary reference.
func newID(prefix string) (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(raw[:]), nil
}

// mongoMetadata is the firmware metadata collection.
type mongoMetadata struct {
	coll *mongo.Collection
}

// insert writes one firmware document; a duplicate version maps to ErrVersionConflict, the
// outcome of two uploads racing past the pre-check and meeting at the unique index.
func (m *mongoMetadata) insert(ctx context.Context, rec Record) error {
	if _, err := m.coll.InsertOne(ctx, rec); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("insert firmware %s: %w", rec.Version, ErrVersionConflict)
		}
		return fmt.Errorf("insert firmware %s: %w", rec.Version, err)
	}
	return nil
}

// find returns the firmware document with the given id.
func (m *mongoMetadata) find(ctx context.Context, id string) (Record, error) {
	var rec Record
	if err := m.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&rec); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Record{}, fmt.Errorf("find firmware %s: %w", id, ErrNotFound)
		}
		return Record{}, fmt.Errorf("find firmware %s: %w", id, err)
	}
	return rec, nil
}

// findByVersion returns the firmware document carrying version.
func (m *mongoMetadata) findByVersion(ctx context.Context, version string) (Record, error) {
	var rec Record
	if err := m.coll.FindOne(ctx, bson.D{{Key: "version", Value: version}}).Decode(&rec); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Record{}, fmt.Errorf("find firmware version %s: %w", version, ErrNotFound)
		}
		return Record{}, fmt.Errorf("find firmware version %s: %w", version, err)
	}
	return rec, nil
}

// versionExists reports whether a firmware document already claims version.
func (m *mongoMetadata) versionExists(ctx context.Context, version string) (bool, error) {
	err := m.coll.FindOne(ctx, bson.D{{Key: "version", Value: version}}).Err()
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("find firmware version %s: %w", version, err)
	default:
		return true, nil
	}
}

// upsert writes one transition of an upload's record. The version is unique, so the first
// transition of an upload inserts and every later one updates: a record that already stands at
// the transitioned state (or beyond it) is returned unchanged, which is what makes a retried
// write a no-op, and a record another upload owns is refused rather than overwritten.
func (m *mongoMetadata) upsert(ctx context.Context, rec Record) (Record, error) {
	current, err := m.findByVersion(ctx, rec.Version)
	switch {
	case errors.Is(err, ErrNotFound):
		if err := m.insert(ctx, rec); err != nil {
			return Record{}, err
		}
		return rec, nil
	case err != nil:
		return Record{}, err
	case current.UploadWorkflowID != rec.UploadWorkflowID:
		return Record{}, fmt.Errorf("upsert firmware %s: %w: %s",
			rec.Version, ErrVersionConflict, rec.Version)
	case stateRank(current.State) >= stateRank(rec.State):
		return current, nil
	}

	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "models", Value: rec.Models},
		{Key: "checksum", Value: rec.Checksum},
		{Key: "size", Value: rec.Size},
		{Key: "gridfs_id", Value: rec.GridFSID},
		{Key: "state", Value: rec.State},
		{Key: "upload_workflow_id", Value: rec.UploadWorkflowID},
		{Key: "failure_reason", Value: rec.FailureReason},
	}}}
	filter := bson.D{
		{Key: "version", Value: rec.Version},
		{Key: "upload_workflow_id", Value: rec.UploadWorkflowID},
	}
	if _, err := m.coll.UpdateOne(ctx, filter, update); err != nil {
		return Record{}, fmt.Errorf("advance firmware %s: %w", rec.Version, err)
	}
	// The record's id and creation time are the upload's from its first transition, so the
	// updated document is the current one with the transitioned fields applied.
	updated := current
	updated.Models = rec.Models
	updated.Checksum = rec.Checksum
	updated.Size = rec.Size
	updated.GridFSID = rec.GridFSID
	updated.State = rec.State
	updated.FailureReason = rec.FailureReason
	return updated, nil
}

// removeClaim deletes the record one upload owns. The filter matches the version and the owning
// workflow, so a record another upload wrote — including one that already became available — is
// never removed by an upload that stored nothing usable.
func (m *mongoMetadata) removeClaim(ctx context.Context, version, workflowID string) error {
	filter := bson.D{
		{Key: "version", Value: version},
		{Key: "upload_workflow_id", Value: workflowID},
		{Key: "state", Value: bson.D{{Key: "$in", Value: []string{UploadAccepted, UploadStoring}}}},
	}
	if _, err := m.coll.DeleteOne(ctx, filter); err != nil {
		return fmt.Errorf("remove upload %s: %w", version, err)
	}
	return nil
}

// gridfsBinaries keeps firmware binaries in the fleet database's GridFS bucket.
type gridfsBinaries struct {
	bucket *mongo.GridFSBucket
}

// put streams r into the GridFS object addressed by ref, returning the number of bytes stored.
// An object already stored at that address is replaced, so a retried storage step converges on
// one object rather than accumulating copies. A transfer that breaks aborts the upload, leaving
// the address holding whatever it held before.
func (g *gridfsBinaries) put(ctx context.Context, ref, name string, r io.Reader) (int64, error) {
	if err := g.remove(ctx, ref); err != nil {
		return 0, fmt.Errorf("replace stored binary %s: %w", ref, err)
	}
	stream, err := g.bucket.OpenUploadStreamWithID(ctx, ref, name)
	if err != nil {
		return 0, fmt.Errorf("open upload stream: %w", err)
	}
	size, err := io.Copy(stream, r)
	if err != nil {
		if abortErr := stream.Abort(); abortErr != nil {
			return 0, errors.Join(
				fmt.Errorf("write upload stream: %w", err),
				fmt.Errorf("abort upload stream: %w", abortErr))
		}
		return 0, fmt.Errorf("write upload stream: %w", err)
	}
	if err := stream.Close(); err != nil {
		return 0, fmt.Errorf("finalize upload stream: %w", err)
	}
	return size, nil
}

// open returns a reader over one stored binary.
func (g *gridfsBinaries) open(ctx context.Context, ref string) (io.ReadCloser, error) {
	return g.bucket.OpenDownloadStream(ctx, ref)
}

// remove deletes one stored binary; an object that is already gone is not an error, so a
// retried cleanup — and the replace a retried storage step performs — converges instead of
// failing on an address that holds nothing yet.
func (g *gridfsBinaries) remove(ctx context.Context, ref string) error {
	err := g.bucket.Delete(ctx, ref)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) && !errors.Is(err, mongo.ErrFileNotFound) {
		return err
	}
	return nil
}
