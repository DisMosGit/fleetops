package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// fakeDownloadStream is a hand-written download-stream double delivering queued messages and
// then a scripted error (io.EOF for a clean end of stream).
type fakeDownloadStream struct {
	agentv1.AgentService_DownloadFirmwareClient
	msgs []*agentv1.FirmwareDownloadResponse
	err  error
}

// Recv delivers the next queued message, then the scripted error.
func (s *fakeDownloadStream) Recv() (*agentv1.FirmwareDownloadResponse, error) {
	if len(s.msgs) == 0 {
		return nil, s.err
	}
	msg := s.msgs[0]
	s.msgs = s.msgs[1:]
	return msg, nil
}

// downloadService returns a fake service handing the given stream to every download call, or
// failing the call with openErr first.
func downloadService(stream agentv1.AgentService_DownloadFirmwareClient, openErr error) *fakeService {
	return &fakeService{
		download: func(context.Context, *agentv1.FirmwareDownloadRequest) (agentv1.AgentService_DownloadFirmwareClient, error) {
			if openErr != nil {
				return nil, openErr
			}
			return stream, nil
		},
	}
}

// transfer builds the messages of one transfer of content under checksum, in chunks of at
// most chunkSize bytes.
func transfer(content []byte, checksum string, chunkSize int) []*agentv1.FirmwareDownloadResponse {
	msgs := []*agentv1.FirmwareDownloadResponse{{
		FirmwareId: "fw-1", Version: "2.0.0", Checksum: checksum, TotalSize: int64(len(content)),
	}}
	for offset := 0; offset < len(content); offset += chunkSize {
		end := min(offset+chunkSize, len(content))
		msgs = append(msgs, &agentv1.FirmwareDownloadResponse{
			FirmwareId: "fw-1", Chunk: content[offset:end], Offset: int64(offset),
		})
	}
	return append(msgs, &agentv1.FirmwareDownloadResponse{
		FirmwareId: "fw-1", Offset: int64(len(content)), Eof: true,
	})
}

// checksumOf returns the lowercase hexadecimal SHA-256 digest of data.
func checksumOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// content builds deterministic binary content of n bytes.
func content(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// dirFiles lists the file names left in a download directory.
func dirFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read download directory: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestFetchVerifiedDownload(t *testing.T) {
	t.Parallel()

	binary := content(3*1000 + 7)
	checksum := checksumOf(binary)
	stream := &fakeDownloadStream{msgs: transfer(binary, checksum, 1000), err: io.EOF}
	d, err := NewDownloader(downloadService(stream, nil), t.TempDir())
	if err != nil {
		t.Fatalf("NewDownloader: %v", err)
	}

	var progress []int32
	path, err := d.Fetch(context.Background(), "dev-1", "fw-1", checksum, func(percent int32) {
		progress = append(progress, percent)
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if path != filepath.Join(d.dir, "fw-1.bin") {
		t.Errorf("path = %q, want the final name under the download directory", path)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded binary: %v", err)
	}
	if !bytes.Equal(got, binary) {
		t.Error("downloaded bytes differ from the transferred content")
	}
	if names := dirFiles(t, d.dir); len(names) != 1 || names[0] != "fw-1.bin" {
		t.Errorf("download directory = %v, want only the verified binary", names)
	}

	if len(progress) == 0 {
		t.Fatal("no download progress was reported")
	}
	for i, percent := range progress {
		if percent < 0 || percent > 100 {
			t.Fatalf("progress[%d] = %d, want 0..100", i, percent)
		}
		if i > 0 && percent < progress[i-1] {
			t.Fatalf("progress went backwards: %v", progress)
		}
	}
	if progress[len(progress)-1] != 100 {
		t.Errorf("final progress = %d, want 100", progress[len(progress)-1])
	}
}

func TestFetchFailures(t *testing.T) {
	t.Parallel()

	binary := content(2500)
	verified := checksumOf(binary)
	other := checksumOf([]byte("something else"))

	complete := transfer(binary, verified, 1000)
	noMarker := complete[:len(complete)-1]
	gap := transfer(binary, verified, 1000)
	gap[2].Offset = 1500
	shortEOF := transfer(binary, verified, 1000)
	shortEOF[len(shortEOF)-1].Offset = 100
	chunkFirst := complete[1:]

	cases := []struct {
		name      string
		msgs      []*agentv1.FirmwareDownloadResponse
		expected  string // the checksum the command carried
		streamErr error
		openErr   error
		want      error
	}{
		{
			name:     "assembled bytes do not match the transfer checksum",
			msgs:     transfer(binary, other, 1000), // announces the checksum of other content
			expected: other,
			want:     ErrChecksumMismatch,
		},
		{
			name:     "transfer and command checksums disagree",
			msgs:     complete,
			expected: other,
			want:     ErrChecksumMismatch,
		},
		{
			name:      "stream ends without the end-of-transfer marker",
			msgs:      noMarker,
			expected:  verified,
			streamErr: io.EOF,
			want:      ErrTruncatedTransfer,
		},
		{
			name:     "end marker before the announced size",
			msgs:     shortEOF,
			expected: verified,
			want:     ErrTruncatedTransfer,
		},
		{
			name:     "chunk offset gap",
			msgs:     gap,
			expected: verified,
			want:     ErrTransfer,
		},
		{
			name:     "transfer opens with a chunk instead of metadata",
			msgs:     chunkFirst,
			expected: verified,
			want:     ErrTransfer,
		},
		{
			name:      "stream failure mid-transfer",
			msgs:      complete[:3],
			expected:  verified,
			streamErr: errors.New("stream reset"),
			want:      ErrTransfer,
		},
		{
			name:     "download call fails",
			expected: verified,
			openErr:  errors.New("connection refused"),
			want:     ErrTransfer,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			d, err := NewDownloader(downloadService(&fakeDownloadStream{msgs: tc.msgs, err: tc.streamErr}, tc.openErr), dir)
			if err != nil {
				t.Fatalf("NewDownloader: %v", err)
			}

			_, err = d.Fetch(context.Background(), "dev-1", "fw-1", tc.expected, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Fetch() error = %v, want %v", err, tc.want)
			}
			if names := dirFiles(t, dir); len(names) != 0 {
				t.Errorf("download directory = %v, want no file left behind", names)
			}
		})
	}
}

func TestFetchInputValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	d, err := NewDownloader(downloadService(&fakeDownloadStream{err: io.EOF}, nil), dir)
	if err != nil {
		t.Fatalf("NewDownloader: %v", err)
	}

	for _, tc := range []struct {
		name     string
		deviceID string
		fwID     string
	}{
		{name: "no device id", deviceID: "", fwID: "fw-1"},
		{name: "no firmware id", deviceID: "dev-1", fwID: ""},
		{name: "firmware id naming a path", deviceID: "dev-1", fwID: "../fw-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := d.Fetch(context.Background(), tc.deviceID, tc.fwID, "sum", nil); err == nil {
				t.Fatal("Fetch() error = nil, want an input error")
			}
		})
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("download directory = %v, want no file left behind", names)
	}
}

func TestNewDownloaderRequiresServiceAndDirectory(t *testing.T) {
	t.Parallel()

	if _, err := NewDownloader(nil, t.TempDir()); err == nil {
		t.Error("NewDownloader(nil, dir) error = nil, want an error")
	}
	if _, err := NewDownloader(&fakeService{}, ""); err == nil {
		t.Error("NewDownloader(svc, \"\") error = nil, want an error")
	}
}
