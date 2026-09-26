package agentserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/firmware"
)

// fakeFirmware is a hand-written FirmwareReader double serving one stored binary, with a
// scriptable open failure and a scriptable mid-transfer read failure.
type fakeFirmware struct {
	rec     firmware.Record
	data    []byte
	openErr error
	readErr error
}

// Open returns the fake's record and a reader over its bytes.
func (f *fakeFirmware) Open(_ context.Context, id string) (firmware.Record, io.ReadCloser, error) {
	if f.openErr != nil {
		return firmware.Record{}, nil, f.openErr
	}
	if id != f.rec.ID {
		return firmware.Record{}, nil, fmt.Errorf("open firmware %s: %w", id, firmware.ErrNotFound)
	}
	if f.readErr != nil {
		return f.rec, io.NopCloser(&failingReader{data: f.data, err: f.readErr}), nil
	}
	return f.rec, io.NopCloser(bytes.NewReader(f.data)), nil
}

// failingReader yields its data once and then fails, standing in for storage breaking
// mid-transfer.
type failingReader struct {
	data []byte
	err  error
}

// Read yields the remaining data, then the queued error.
func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// download pulls one firmware id and returns every message the stream delivered plus the
// error that ended it.
func download(
	ctx context.Context,
	t *testing.T,
	conn *grpc.ClientConn,
	deviceID, firmwareID string,
) ([]*agentv1.FirmwareDownloadResponse, error) {
	t.Helper()
	stream, err := agentv1.NewAgentServiceClient(conn).DownloadFirmware(ctx, &agentv1.FirmwareDownloadRequest{
		DeviceId:   deviceID,
		FirmwareId: firmwareID,
	})
	if err != nil {
		t.Fatalf("open download stream: %v", err)
	}
	var msgs []*agentv1.FirmwareDownloadResponse
	for {
		msg, err := stream.Recv()
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
}

// startDownloadServer serves one fake firmware store and returns a connection to it.
func startDownloadServer(t *testing.T, reader FirmwareReader) *grpc.ClientConn {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	return startServerWith(t, NewHub(&fakeSink{}, log), &fakeRegistry{}, &fakeSignaler{}, reader)
}

func TestDownloadFirmwareStream(t *testing.T) {
	t.Parallel()

	content := make([]byte, 3*downloadChunkSize+100)
	for i := range content {
		content[i] = byte(i % 251)
	}
	sum := sha256.Sum256(content)
	reader := &fakeFirmware{
		rec: firmware.Record{
			ID: "fw-1", Version: "2.0.0",
			Checksum: hex.EncodeToString(sum[:]),
			Size:     int64(len(content)),
		},
		data: content,
	}
	conn := startDownloadServer(t, reader)

	msgs, err := download(context.Background(), t, conn, "dev-1", "fw-1")
	if err != io.EOF {
		t.Fatalf("download ended with %v, want io.EOF (clean end of stream)", err)
	}
	if len(msgs) < 3 {
		t.Fatalf("messages = %d, want metadata, chunks, and an end marker", len(msgs))
	}

	meta := msgs[0]
	if meta.Version != "2.0.0" || meta.Checksum != reader.rec.Checksum || meta.TotalSize != int64(len(content)) {
		t.Errorf("metadata message = %+v, want version, checksum, and total size", meta)
	}
	if len(meta.Chunk) != 0 {
		t.Errorf("metadata chunk = %d bytes, want none", len(meta.Chunk))
	}

	var assembled []byte
	wantOffset := int64(0)
	for _, msg := range msgs[1 : len(msgs)-1] {
		if msg.Offset != wantOffset {
			t.Errorf("chunk offset = %d, want %d", msg.Offset, wantOffset)
		}
		if len(msg.Chunk) == 0 || len(msg.Chunk) > downloadChunkSize {
			t.Errorf("chunk size = %d, want 1..%d", len(msg.Chunk), downloadChunkSize)
		}
		wantOffset += int64(len(msg.Chunk))
		assembled = append(assembled, msg.Chunk...)
	}
	if !bytes.Equal(assembled, content) {
		t.Errorf("assembled %d bytes, want the %d stored bytes", len(assembled), len(content))
	}

	last := msgs[len(msgs)-1]
	if !last.GetEof() || len(last.Chunk) != 0 || last.Offset != int64(len(content)) {
		t.Errorf("end message = %+v, want the end marker at offset %d", last, len(content))
	}
}

func TestDownloadFirmwareEmptyBinary(t *testing.T) {
	t.Parallel()

	conn := startDownloadServer(t, &fakeFirmware{rec: firmware.Record{ID: "fw-1", Version: "1.0.0"}})

	msgs, err := download(context.Background(), t, conn, "dev-1", "fw-1")
	if err != io.EOF {
		t.Fatalf("download ended with %v, want io.EOF", err)
	}
	if len(msgs) != 2 || len(msgs[0].Chunk) != 0 || !msgs[1].GetEof() {
		t.Fatalf("messages = %+v, want metadata and end marker only", msgs)
	}
}

func TestDownloadFirmwareErrors(t *testing.T) {
	t.Parallel()

	reader := &fakeFirmware{
		rec:     firmware.Record{ID: "fw-1", Version: "1.0.0"},
		openErr: errors.New("mongo: connection refused at 10.0.0.5"),
	}
	conn := startDownloadServer(t, reader)
	unknown := startDownloadServer(t, &fakeFirmware{rec: firmware.Record{ID: "fw-1"}})

	cases := []struct {
		name     string
		conn     *grpc.ClientConn
		deviceID string
		fwID     string
		wantCode codes.Code
	}{
		{name: "unknown firmware", conn: unknown, deviceID: "dev-1", fwID: "fw-9", wantCode: codes.NotFound},
		{name: "missing device id", conn: unknown, deviceID: "", fwID: "fw-1", wantCode: codes.InvalidArgument},
		{name: "missing firmware id", conn: unknown, deviceID: "dev-1", fwID: "", wantCode: codes.InvalidArgument},
		{name: "storage failure", conn: conn, deviceID: "dev-1", fwID: "fw-1", wantCode: codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msgs, err := download(context.Background(), t, tc.conn, tc.deviceID, tc.fwID)
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("download ended with %v, want a gRPC status error", err)
			}
			if st.Code() != tc.wantCode {
				t.Errorf("status code = %v, want %v", st.Code(), tc.wantCode)
			}
			if len(msgs) != 0 {
				t.Errorf("messages = %d, want none before the failure", len(msgs))
			}
			// The message is operator-safe: contract-level reason only, never the wrapped
			// storage detail.
			for _, leak := range []string{"mongo", "10.0.0.5", "open firmware"} {
				if strings.Contains(st.Message(), leak) {
					t.Errorf("status message = %q, want no %q detail", st.Message(), leak)
				}
			}
		})
	}
}

func TestDownloadFirmwareMidStreamFailure(t *testing.T) {
	t.Parallel()

	content := make([]byte, downloadChunkSize+512)
	reader := &fakeFirmware{
		rec:     firmware.Record{ID: "fw-1", Version: "1.0.0", Size: int64(len(content) * 2)},
		data:    content,
		readErr: errors.New("gridfs read: connection reset"),
	}
	conn := startDownloadServer(t, reader)

	msgs, err := download(context.Background(), t, conn, "dev-1", "fw-1")
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("download ended with %v, want a gRPC status error (never a silent truncation)", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("status code = %v, want %v", st.Code(), codes.Internal)
	}
	if strings.Contains(st.Message(), "gridfs") {
		t.Errorf("status message = %q, want no storage detail", st.Message())
	}
	if len(msgs) == 0 || len(msgs[0].Chunk) != 0 {
		t.Errorf("messages = %+v, want the metadata message before the failure", msgs)
	}
	if msgs[len(msgs)-1].GetEof() {
		t.Error("last message carries the end marker, want the transfer to end before it")
	}
}
