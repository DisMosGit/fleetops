package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// TestDeviceSettingsFromConfiguration pins the control plane's half of "Device workflow cadence
// comes from configuration": the configured snapshot interval and offline threshold are what the
// signaler seeds every new device workflow run chain with, not the defaults. The temporal package's
// scheduling tests prove those settings drive the snapshot cadence and the offline flip; this is the
// hop that puts the configured values in them, so hardcoding either value here fails here.
func TestDeviceSettingsFromConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		apply func(*config.Config)
		want  temporal.DeviceSettings
	}{
		{
			name: "configured cadence, threshold, and dispatch queue reach the settings",
			apply: func(cfg *config.Config) {
				cfg.Snapshots.Interval = config.Duration{Duration: 30 * time.Second}
				cfg.Liveness.OfflineThreshold = config.Duration{Duration: 2 * time.Minute}
				cfg.Temporal.DispatchTaskQueue = "fleetops-cp"
			},
			want: temporal.DeviceSettings{
				SnapshotInterval:  30 * time.Second,
				OfflineThreshold:  2 * time.Minute,
				DispatchTaskQueue: "fleetops-cp",
			},
		},
		{
			name:  "the documented defaults reach the settings when nothing is configured",
			apply: func(*config.Config) {},
			want: temporal.DeviceSettings{
				SnapshotInterval:  config.Defaults().Snapshots.Interval.Duration,
				OfflineThreshold:  config.Defaults().Liveness.OfflineThreshold.Duration,
				DispatchTaskQueue: config.Defaults().Temporal.DispatchTaskQueue,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.Defaults()
			tc.apply(&cfg)

			if diff := cmp.Diff(tc.want, deviceSettings(cfg)); diff != "" {
				t.Errorf("device settings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// failingCheck is a dependency probe that always fails: the readiness answer proves the checks the
// configured listener was built with reached its handler.
type failingCheck struct {
	name string
}

// Name implements health.Check.
func (c failingCheck) Name() string { return c.name }

// Check implements health.Check.
func (c failingCheck) Check(context.Context) error { return errors.New("dependency is unreachable") }

// TestProbeListenerFromConfiguration pins "Probe listener comes from configuration": the listener
// the control plane serves its probes on is the configured health address, and it answers there —
// liveness and readiness, over the configured dependency checks. The address is a free loopback
// address the test reserved rather than a fixed port, so a server built from any other (default)
// address cannot satisfy the test.
func TestProbeListenerFromConfiguration(t *testing.T) {
	t.Parallel()

	addr := freeLoopbackAddr(t)
	cfg := config.Defaults()
	cfg.Observability.HealthAddr = addr

	srv := probeServer(cfg, failingCheck{name: "mongodb"})
	if srv.Addr != addr {
		t.Fatalf("probe listener address = %q, want the configured %q", srv.Addr, addr)
	}
	if srv.Addr == config.Defaults().Observability.HealthAddr {
		t.Fatalf("probe listener address = %q, want the configured address, not the default", srv.Addr)
	}

	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("probe server stopped with %v, want a clean shutdown", err)
		}
	})

	live := getEventually(t, "http://"+addr+"/healthz")
	defer func() { _ = live.Body.Close() }()
	if live.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d, want %d on the configured address",
			live.StatusCode, http.StatusOK)
	}

	ready := getEventually(t, "http://"+addr+"/readyz")
	defer func() { _ = ready.Body.Close() }()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readiness status = %d, want %d for the failing dependency",
			ready.StatusCode, http.StatusServiceUnavailable)
	}
	var report struct {
		Status string `json:"status"`
		Checks map[string]struct {
			Status  string `json:"status"`
			Summary string `json:"summary"`
		} `json:"checks"`
	}
	if err := json.NewDecoder(ready.Body).Decode(&report); err != nil {
		t.Fatalf("decode readiness report: %v", err)
	}
	if report.Status != "not_ready" || report.Checks["mongodb"].Status != "error" {
		t.Errorf("readiness report = %+v, want the configured dependency check reported", report)
	}
}

// TestRejectedConfigurationStartsNoListener pins "Invalid configuration starts no listener" over
// the real entrypoint: the control plane binary is built and run against a configuration that fails
// validation, and the process exits non-zero with that validation error. The file names free
// listener addresses, so an entrypoint that ignored the rejection and served would have to report a
// listener (or a dependency failure) and would be found listening; the test asserts neither
// happened and that every configured address is still free after the process exited.
func TestRejectedConfigurationStartsNoListener(t *testing.T) {
	if testing.Short() {
		t.Skip("building the control plane binary is skipped in short mode")
	}
	t.Parallel()

	binary := buildControlPlane(t)
	grpcAddr := freeLoopbackAddr(t)
	metricsAddr := freeLoopbackAddr(t)
	healthAddr := freeLoopbackAddr(t)
	gatewayAddr := freeLoopbackAddr(t)

	configPath := filepath.Join(t.TempDir(), "rejected.yaml")
	writeFile(t, configPath, fmt.Sprintf(`
simulation:
  fleet_size: 0
grpc:
  listen_addr: %q
observability:
  metrics_addr: %q
  health_addr: %q
`, grpcAddr, metricsAddr, healthAddr))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary,
		"-config", configPath, "-http-addr", gatewayAddr).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the entrypoint did not exit on the rejected configuration: %v\n%s", ctx.Err(), out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("entrypoint error = %v, want a non-zero exit\n%s", err, out)
	}
	// The rejection names the offending field, which is what tells an operator what to fix — and
	// is the error only the configuration load produces.
	if !strings.Contains(string(out), "simulation.fleet_size") {
		t.Errorf("entrypoint output does not name the rejected field:\n%s", out)
	}
	// Nothing started: no listener was announced and no dependency was contacted on the way to a
	// listener, which is what a process that ignored the rejection would have done first.
	for _, announced := range []string{"listening", "connect to mongodb", "connect to temporal"} {
		if strings.Contains(string(out), announced) {
			t.Errorf("entrypoint output reports %q although the configuration was rejected:\n%s",
				announced, out)
		}
	}

	// Every address the rejected configuration named — the gRPC listener, the metrics listener,
	// the probe listener, and the gateway the flag names — is free: the process bound none of them.
	for _, addr := range []string{grpcAddr, metricsAddr, healthAddr, gatewayAddr} {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			t.Errorf("something listens on %s after the configuration was rejected", addr)
		}
	}
}

// buildControlPlane builds the control plane binary into the test's temporary directory and returns
// its path. The binary is the real entrypoint, so a test over it exercises the load-and-run path a
// deployment runs rather than a re-assembly of it.
func buildControlPlane(t *testing.T) string {
	t.Helper()

	binary := filepath.Join(t.TempDir(), "controlplane")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build control plane: %v\n%s", err, out)
	}
	return binary
}

// freeLoopbackAddr reserves a loopback address by asking the OS for one and releasing it, so a test
// configures an address nothing else holds and never a fixed port.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("release loopback address: %v", err)
	}
	return addr
}

// getEventually polls url until the listener answers or the budget runs out: the server binds in its
// own goroutine, so a request may race the bind.
func getEventually(t *testing.T, url string) *http.Response {
	t.Helper()

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			return resp
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET %s: %v", url, lastErr)
	return nil
}

// writeFile writes body to path, failing the test when it cannot.
func writeFile(t *testing.T, path, body string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
