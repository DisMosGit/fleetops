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
// lives in GridFS and is referenced by GridFSID — the document never carries payload bytes.
type Record struct {
	// ID is the firmware identity; the _id of the firmware document.
	ID string `bson:"_id"`
	// Version is the firmware version, unique across the registry.
	Version string `bson:"version"`
	// Models are the device models the firmware targets, as declared at upload.
	Models []string `bson:"models"`
	// Checksum is the lowercase hexadecimal SHA-256 digest of the complete binary.
	Checksum string `bson:"checksum"`
	// Size is the stored binary's size in bytes.
	Size int64 `bson:"size"`
	// CreatedAt is the time the upload was recorded.
	CreatedAt time.Time `bson:"created_at"`
	// GridFSID references the binary stored in GridFS.
	GridFSID string `bson:"gridfs_id"`
}

// metadata is the firmware metadata collection the store records into. The Mongo-backed
// implementation is built by NewStore; tests hand-write a fake.
type metadata interface {
	// insert writes one firmware document, failing with ErrVersionConflict when its
	// version is already taken.
	insert(ctx context.Context, rec Record) error
	// find returns the firmware document with the given id, or an error wrapping ErrNotFound.
	find(ctx context.Context, id string) (Record, error)
	// versionExists reports whether a document already carries version.
	versionExists(ctx context.Context, version string) (bool, error)
}

// binaries is the binary storage the store keeps firmware payloads in. put consumes r in
// bounded pieces without buffering it whole and returns the reference of the stored object;
// open streams exactly the stored bytes back; remove deletes the object, succeeding on one
// that is already gone so cleanup stays idempotent.
type binaries interface {
	put(ctx context.Context, name string, r io.Reader) (ref string, size int64, err error)
	open(ctx context.Context, ref string) (io.ReadCloser, error)
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
	hash := sha256.New()
	ref, size, err := s.binaries.put(ctx, id, io.TeeReader(binary, hash))
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

// Open returns the firmware record of id and a reader over its stored binary. An unknown id
// fails with an error wrapping ErrNotFound.
func (s *Store) Open(ctx context.Context, id string) (Record, io.ReadCloser, error) {
	if id == "" {
		return Record{}, nil, errors.New("open firmware: firmware id required")
	}
	rec, err := s.meta.find(ctx, id)
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

// gridfsBinaries keeps firmware binaries in the fleet database's GridFS bucket.
type gridfsBinaries struct {
	bucket *mongo.GridFSBucket
}

// put streams r into one GridFS object under a fresh reference, returning the reference and
// the number of bytes stored. A transfer that breaks aborts the upload, leaving no object.
func (g *gridfsBinaries) put(ctx context.Context, name string, r io.Reader) (string, int64, error) {
	ref, err := newID("fwbin")
	if err != nil {
		return "", 0, err
	}
	stream, err := g.bucket.OpenUploadStreamWithID(ctx, ref, name)
	if err != nil {
		return "", 0, fmt.Errorf("open upload stream: %w", err)
	}
	size, err := io.Copy(stream, r)
	if err != nil {
		if abortErr := stream.Abort(); abortErr != nil {
			return "", 0, errors.Join(
				fmt.Errorf("write upload stream: %w", err),
				fmt.Errorf("abort upload stream: %w", abortErr))
		}
		return "", 0, fmt.Errorf("write upload stream: %w", err)
	}
	if err := stream.Close(); err != nil {
		return "", 0, fmt.Errorf("finalize upload stream: %w", err)
	}
	return ref, size, nil
}

// open returns a reader over one stored binary.
func (g *gridfsBinaries) open(ctx context.Context, ref string) (io.ReadCloser, error) {
	return g.bucket.OpenDownloadStream(ctx, ref)
}

// remove deletes one stored binary; an object that is already gone is not an error, so a
// retried cleanup converges instead of failing.
func (g *gridfsBinaries) remove(ctx context.Context, ref string) error {
	if err := g.bucket.Delete(ctx, ref); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	return nil
}
