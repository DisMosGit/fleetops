package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// scriptedSource replays a fixed sample sequence, one sample per call. It is safe for
// concurrent use: a fleet shares one source across its device goroutines.
type scriptedSource struct {
	mu      sync.Mutex
	samples []Sample
	next    int
}

func (s *scriptedSource) Next(string) Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	sample := s.samples[s.next%len(s.samples)]
	s.next++
	return sample
}

// newTestDevice returns a device with a scripted source and a frozen clock.
func newTestDevice(t *testing.T, samples ...Sample) *Device {
	t.Helper()
	ids, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	return &Device{
		Identity: Identity{ID: "device-7", Model: "oak-s3", Region: "eu-west", Firmware: "1.2.3"},
		Source:   &scriptedSource{samples: samples},
		IDs:      ids,
		Now:      func() time.Time { return time.Unix(1700000000, 0) },
	}
}

func TestDeviceRunEmitsOneHeartbeatPerTick(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sample Sample
	}{
		{
			name:   "healthy sample",
			sample: Sample{CPU: 0.4, Mem: 0.5, Health: 0.9, Status: StatusOnline},
		},
		{
			name:   "degraded sample",
			sample: Sample{CPU: 0.9, Mem: 0.8, Health: 0.3, Status: StatusDegraded},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dev := newTestDevice(t, tc.sample)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			ticks := make(chan time.Time)
			out := make(chan *agentv1.Heartbeat, 1)
			done := make(chan error, 1)
			go func() { done <- dev.Run(ctx, ticks, out) }()

			ticks <- time.Unix(1700000000, 0)
			got := <-out

			want := &agentv1.Heartbeat{
				EventId:   got.EventId,
				DeviceId:  "device-7",
				CurrentFw: "1.2.3",
				Status:    tc.sample.Status,
				Ts:        timestamppb.New(time.Unix(1700000000, 0)),
				Cpu:       tc.sample.CPU,
				Mem:       tc.sample.Mem,
				Health:    tc.sample.Health,
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("heartbeat mismatch (-want +got):\n%s", diff)
			}

			cancel()
			if err := <-done; err != nil {
				t.Errorf("Run returned %v, want nil", err)
			}
		})
	}
}

func TestDeviceRunEventIDsAreUnique(t *testing.T) {
	t.Parallel()

	dev := newTestDevice(t, Sample{CPU: 0.1, Mem: 0.1, Health: 1.0, Status: StatusOnline})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ticks := make(chan time.Time)
	out := make(chan *agentv1.Heartbeat, 2)
	go func() { _ = dev.Run(ctx, ticks, out) }()

	ticks <- time.Unix(1700000000, 0)
	ticks <- time.Unix(1700000005, 0)
	first, second := <-out, <-out
	if first.EventId == second.EventId {
		t.Errorf("distinct heartbeats must carry distinct event ids, both %q", first.EventId)
	}
}

func TestDeviceRunStopsWhileBackpressured(t *testing.T) {
	t.Parallel()

	dev := newTestDevice(t, Sample{CPU: 0.1, Mem: 0.1, Health: 1.0, Status: StatusOnline})
	ctx, cancel := context.WithCancel(context.Background())

	ticks := make(chan time.Time, 1)
	out := make(chan *agentv1.Heartbeat) // unbuffered: emission blocks without a reader
	done := make(chan error, 1)
	go func() { done <- dev.Run(ctx, ticks, out) }()

	ticks <- time.Unix(1700000000, 0) // device reaches the blocking send
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after ctx cancellation")
	}
}
