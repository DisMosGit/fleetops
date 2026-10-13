package agent

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"slices"
	"sync"
	"time"
)

// ErrStall reports a firmware download an injected stall ended before any data arrived. Its
// message is operator-safe by construction: the update report echoes it as its failure detail.
var ErrStall = errors.New("firmware download stalled")

// FaultKind names one kind of injected firmware-download fault.
type FaultKind string

// The fault kinds the injector can draw from: the two the update path's retry must survive.
const (
	// FaultStall is an attempt that stalls for the configured duration, then fails.
	FaultStall FaultKind = "stall"
	// FaultTransfer is an attempt that fails immediately with a transfer error.
	FaultTransfer FaultKind = "transfer"
)

// FaultKinds returns every fault kind the injector can draw, in the order a configuration may
// name them. The configuration validates against the same set, so a kind added here is
// rejected there until it is accepted too.
func FaultKinds() []FaultKind { return []FaultKind{FaultStall, FaultTransfer} }

// valid reports whether k is a kind the injector can draw.
func (k FaultKind) valid() bool { return slices.Contains(FaultKinds(), k) }

// FaultConfig configures the injector: how often an attempt faults, which kinds a faulted
// attempt draws from, and how long a stall lasts. Its zero value injects no fault at all, so a
// decorator left at the zero value behaves exactly like the fetcher it decorates.
type FaultConfig struct {
	// Share is the probability that one download attempt faults, in [0, 1].
	Share float64
	// Kinds are the kinds a faulted attempt draws from; no kinds injects nothing.
	Kinds []FaultKind
	// StallDuration is how long an injected stall waits before its attempt fails.
	StallDuration time.Duration
}

// injects reports whether the configuration can fault any attempt.
func (c FaultConfig) injects() bool { return c.Share > 0 && len(c.Kinds) > 0 }

// usesKind reports whether kind is one of the configured kinds.
func (c FaultConfig) usesKind(kind FaultKind) bool { return slices.Contains(c.Kinds, kind) }

// FaultFetcher decorates a FirmwareFetcher with seeded fault injection: every attempt draws,
// from the decorated device's own stream, whether it faults and which kind it draws. A stream
// is keyed by device and seeded from the source the emulator was built with plus the device id,
// so one device's draws never move another's and the same seed replays the same faults for the
// same attempt sequence. It is safe for concurrent use across the fleet's devices.
type FaultFetcher struct {
	base  FirmwareFetcher
	cfg   FaultConfig
	seed  int64
	sleep func(ctx context.Context, d time.Duration) error

	mu      sync.Mutex
	streams map[string]*rand.Rand
}

// NewFaultFetcher returns a fetcher decorating base with the configured faults, drawing each
// device's decisions from its own stream of src. A configuration that injects nothing is
// valid and delegates every attempt to base unchanged.
func NewFaultFetcher(
	base FirmwareFetcher,
	cfg FaultConfig,
	src rand.Source,
) (*FaultFetcher, error) {
	if base == nil {
		return nil, errors.New("fault fetcher base: required")
	}
	if src == nil {
		return nil, errors.New("fault fetcher source: required")
	}
	if !(cfg.Share >= 0 && cfg.Share <= 1) {
		return nil, errors.New("fault fetcher share: must be in [0, 1]")
	}
	if cfg.Share > 0 && len(cfg.Kinds) == 0 {
		return nil, errors.New("fault fetcher kinds: must name at least one kind when share is positive")
	}
	for _, kind := range cfg.Kinds {
		if !kind.valid() {
			return nil, fmt.Errorf("fault fetcher kind %q: must be a known fault kind", kind)
		}
	}
	if cfg.usesKind(FaultStall) && cfg.StallDuration <= 0 {
		return nil, errors.New("fault fetcher stall duration: must be positive for a stall")
	}
	return &FaultFetcher{
		base:    base,
		cfg:     cfg,
		seed:    src.Int63(),
		sleep:   sleepCtx,
		streams: make(map[string]*rand.Rand),
	}, nil
}

// Fetch draws this attempt's decision for the device: a stall waits out the configured
// duration and then fails, a transfer fault fails immediately, and every other draw delegates
// to the decorated fetcher. A stall ends with the context, so a shutdown never waits it out,
// and a faulted attempt never reaches the decorated fetcher on disk.
func (f *FaultFetcher) Fetch(
	ctx context.Context,
	deviceID, firmwareID, expectedChecksum string,
	onProgress func(int32),
) (string, error) {
	kind, faulting := f.decide(deviceID)
	if !faulting {
		return f.base.Fetch(ctx, deviceID, firmwareID, expectedChecksum, onProgress)
	}
	if kind == FaultStall {
		if err := f.sleep(ctx, f.cfg.StallDuration); err != nil {
			return "", fmt.Errorf("stalled download: %w", err)
		}
		return "", fmt.Errorf("%w after %s", ErrStall, f.cfg.StallDuration)
	}
	return "", fmt.Errorf("%w: injected fault", ErrTransfer)
}

// decide returns the fault kind this attempt draws for deviceID, or false when the attempt is
// not faulted. A configuration that injects nothing consumes no draw, so a device's stream
// starts where it would have without the decorator.
func (f *FaultFetcher) decide(deviceID string) (FaultKind, bool) {
	if !f.cfg.injects() {
		return "", false
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	r := f.stream(deviceID)
	if r.Float64() >= f.cfg.Share {
		return "", false
	}
	return f.cfg.Kinds[r.Intn(len(f.cfg.Kinds))], true
}

// stream returns deviceID's own draw stream, created from the injected seed on its first
// attempt. The caller holds the lock that guards the stream map.
func (f *FaultFetcher) stream(deviceID string) *rand.Rand {
	r, ok := f.streams[deviceID]
	if !ok {
		r = rand.New(rand.NewSource(f.seed + int64(crc32.ChecksumIEEE([]byte(deviceID)))))
		f.streams[deviceID] = r
	}
	return r
}

// retryableFetchError reports whether err is a download failure a retry can clear: a transfer
// fault or a stall. A checksum mismatch or a malformed transfer is deterministic — retrying it
// spends the attempt budget to produce the same failure — and a context error is the command's
// own end, so neither is retryable.
func retryableFetchError(err error) bool {
	return errors.Is(err, ErrTransfer) || errors.Is(err, ErrStall)
}
