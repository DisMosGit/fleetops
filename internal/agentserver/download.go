package agentserver

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/firmware"
)

// downloadChunkSize bounds one chunk of a firmware download: the server's read buffer, the
// wire message, and the agent's write buffer are all one chunk, so a download costs one chunk
// of memory per side whatever the binary's size.
const downloadChunkSize = 64 << 10

// FirmwareReader opens stored firmware binaries for delivery to agents. *firmware.Store
// satisfies it — the interface lives here because this package is where delivery consumes it.
type FirmwareReader interface {
	// Open returns the firmware record of id and a reader over its stored binary. An unknown
	// id fails with an error wrapping firmware.ErrNotFound.
	Open(ctx context.Context, id string) (firmware.Record, io.ReadCloser, error)
}

// DownloadFirmware serves one firmware download over its server stream. The opening message
// carries the firmware metadata — version, checksum, total size — so the agent can verify what
// it assembles; the binary follows as chunks of at most one chunk size at contiguous offsets;
// the closing message marks the end of the transfer. The binary is streamed from storage in
// chunk-sized pieces and is never held in memory whole. An unknown firmware is a safe
// NOT_FOUND; a failure after streaming began ends the stream with a status error instead of a
// silently truncated transfer.
func (s *Server) DownloadFirmware(
	req *agentv1.FirmwareDownloadRequest,
	stream agentv1.AgentService_DownloadFirmwareServer,
) error {
	if req.GetDeviceId() == "" || req.GetFirmwareId() == "" {
		return status.Error(codes.InvalidArgument, "device id and firmware id required")
	}
	rec, body, err := s.firmware.Open(stream.Context(), req.GetFirmwareId())
	if err != nil {
		if errors.Is(err, firmware.ErrNotFound) {
			return status.Error(codes.NotFound, "firmware not found")
		}
		s.log.Error("open firmware", "firmware_id", req.GetFirmwareId(), "err", err)
		return status.Error(codes.Internal, "firmware could not be opened")
	}
	defer func() {
		if err := body.Close(); err != nil {
			s.log.Error("close firmware binary", "firmware_id", rec.ID, "err", err)
		}
	}()

	if err := stream.Send(&agentv1.FirmwareDownloadResponse{
		FirmwareId: rec.ID,
		Version:    rec.Version,
		Checksum:   rec.Checksum,
		TotalSize:  rec.Size,
	}); err != nil {
		return err
	}

	buf := make([]byte, downloadChunkSize)
	var offset int64
	for {
		n, readErr := io.ReadFull(body, buf)
		if n > 0 {
			if err := stream.Send(&agentv1.FirmwareDownloadResponse{
				FirmwareId: rec.ID,
				Chunk:      buf[:n],
				Offset:     offset,
			}); err != nil {
				return err
			}
			offset += int64(n)
		}
		if readErr == nil {
			continue
		}
		if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			s.log.Error("read firmware binary", "firmware_id", rec.ID, "err", readErr)
			return status.Error(codes.Internal, "firmware transfer failed")
		}
		break
	}
	return stream.Send(&agentv1.FirmwareDownloadResponse{
		FirmwareId: rec.ID,
		Offset:     offset,
		Eof:        true,
	})
}
