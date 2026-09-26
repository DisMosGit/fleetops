package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// Download failure classes — the reasons a download can end in. Their messages are
// operator-safe by construction: the update report echoes them as its failure detail.
var (
	// ErrChecksumMismatch reports a transferred binary that does not verify: the digest of
	// the assembled bytes disagrees with the transfer checksum, or the transfer checksum
	// disagrees with the one the command carried.
	ErrChecksumMismatch = errors.New("checksum mismatch")
	// ErrTruncatedTransfer reports a transfer that ended short: without its end-of-transfer
	// marker, or before the size the transfer announced.
	ErrTruncatedTransfer = errors.New("truncated transfer")
	// ErrTransfer reports a transfer that failed on the wire or in storage.
	ErrTransfer = errors.New("firmware transfer failed")
)

// Downloader fetches firmware binaries over AgentService.DownloadFirmware to disk. Chunks
// stream into a partial file and are hashed as they land — the assembly is never held in
// memory — and only a verified transfer is renamed to its final name. Whatever the outcome,
// a failed fetch leaves no file of that firmware behind.
type Downloader struct {
	svc agentv1.AgentServiceClient
	dir string
}

// NewDownloader returns a downloader that stages firmware binaries under dir.
func NewDownloader(svc agentv1.AgentServiceClient, dir string) (*Downloader, error) {
	if svc == nil {
		return nil, errors.New("downloader service: required")
	}
	if dir == "" {
		return nil, errors.New("downloader directory: required")
	}
	return &Downloader{svc: svc, dir: dir}, nil
}

// Fetch downloads one firmware binary to disk and returns the path of the verified file. The
// transfer checksum must agree with expectedChecksum — the checksum the StartUpdate command
// carried — and with the digest computed over the assembled bytes; chunk offsets must cover
// the announced size contiguously. onProgress, when set, receives the download's completion
// percentage as the transfer advances. Every failure removes the partial and final files of
// this firmware before returning.
func (d *Downloader) Fetch(
	ctx context.Context,
	deviceID, firmwareID, expectedChecksum string,
	onProgress func(int32),
) (string, error) {
	if deviceID == "" || firmwareID == "" {
		return "", fmt.Errorf("fetch firmware: device id and firmware id required")
	}
	if strings.ContainsAny(firmwareID, `/\`) {
		return "", fmt.Errorf("fetch firmware %q: id must not name a path", firmwareID)
	}
	part := filepath.Join(d.dir, firmwareID+".part")
	final := filepath.Join(d.dir, firmwareID+".bin")

	path, err := d.receive(ctx, deviceID, firmwareID, expectedChecksum, part, final, onProgress)
	if err != nil {
		if cleanErr := removeFiles(part, final); cleanErr != nil {
			return "", errors.Join(err, cleanErr)
		}
		return "", err
	}
	return path, nil
}

// receive runs one transfer end to end: the metadata message first, then chunks at contiguous
// offsets into the partial file under an incremental hash, and the rename onto the final name
// only once the assembly verifies against the announced checksum and size. Its named error is
// what the deferred file cleanup joins its own failures onto.
func (d *Downloader) receive(
	ctx context.Context,
	deviceID, firmwareID, expectedChecksum, part, final string,
	onProgress func(int32),
) (path string, err error) {
	stream, err := d.svc.DownloadFirmware(ctx, &agentv1.FirmwareDownloadRequest{
		DeviceId:   deviceID,
		FirmwareId: firmwareID,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTransfer, err)
	}

	meta, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("%w: transfer opened without metadata", ErrTransfer)
	}
	if len(meta.GetChunk()) != 0 || meta.GetChecksum() == "" {
		return "", fmt.Errorf("%w: transfer does not open with metadata", ErrTransfer)
	}
	if meta.GetChecksum() != expectedChecksum {
		return "", fmt.Errorf("%w: transfer and command checksums disagree", ErrChecksumMismatch)
	}

	file, err := os.Create(part)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTransfer, err)
	}
	defer func() {
		// The success path closed the file already; what is left here is the failure
		// path, where a close error joins the failure that caused it.
		if cerr := file.Close(); cerr != nil && !errors.Is(cerr, fs.ErrClosed) {
			err = errors.Join(err, fmt.Errorf("close partial download: %w", cerr))
		}
	}()

	hash := sha256.New()
	sink := io.MultiWriter(file, hash)
	var offset int64
	reported := int32(-1)
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", fmt.Errorf("%w: stream ended without the end-of-transfer marker", ErrTruncatedTransfer)
			}
			return "", fmt.Errorf("%w: %v", ErrTransfer, err)
		}
		if msg.GetEof() {
			if offset != meta.GetTotalSize() || msg.GetOffset() != offset {
				return "", fmt.Errorf("%w: transfer ended at %d of %d bytes",
					ErrTruncatedTransfer, offset, meta.GetTotalSize())
			}
			break
		}
		if msg.GetOffset() != offset {
			return "", fmt.Errorf("%w: chunk offset %d does not continue %d",
				ErrTransfer, msg.GetOffset(), offset)
		}
		n, err := sink.Write(msg.GetChunk())
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrTransfer, err)
		}
		offset += int64(n)
		if onProgress != nil {
			if percent := percentOf(offset, meta.GetTotalSize()); percent != reported {
				onProgress(percent)
				reported = percent
			}
		}
	}
	if digest := hex.EncodeToString(hash.Sum(nil)); digest != meta.GetChecksum() {
		return "", fmt.Errorf("%w: assembled binary does not match the transfer checksum", ErrChecksumMismatch)
	}

	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrTransfer, err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrTransfer, err)
	}
	if err := os.Rename(part, final); err != nil {
		return "", fmt.Errorf("%w: %v", ErrTransfer, err)
	}
	if onProgress != nil && reported != 100 {
		onProgress(100)
	}
	return final, nil
}

// percentOf reports how much of the announced size has landed, at whole percents.
func percentOf(offset, total int64) int32 {
	if total <= 0 {
		return 0
	}
	return int32(offset * 100 / total)
}

// removeFiles deletes the given paths; a file that is already gone is not an error, so a
// retried cleanup converges instead of failing.
func removeFiles(paths ...string) error {
	var errs []error
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
