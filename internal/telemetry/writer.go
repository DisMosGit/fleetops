package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/devices"
)

const (
	// queueBufferFactor sizes the bounded queue as a multiple of the batch size: a full
	// batch of work may wait while the next one accumulates, and not more.
	queueBufferFactor = 2
	// shutdownFlushTimeout bounds the final flush of a shutting-down writer.
	shutdownFlushTimeout = 5 * time.Second
	// initialRetryBackoff is the first pause between failed flush attempts.
	initialRetryBackoff = 100 * time.Millisecond
	// maxRetryBackoff caps the exponential growth of the flush retry pause.
	maxRetryBackoff = time.Second
	// errDuplicateKey is MongoDB's E11000 duplicate-key error code.
	errDuplicateKey = 11000
)

// EventStore is the part of the telemetry collection the writer needs: batched inserts whose
// per-row conflicts surface as write errors. *mongo.Collection satisfies it.
type EventStore interface {
	// InsertMany stores documents, reporting per-row conflicts in its error.
	InsertMany(ctx context.Context, documents any, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
}

// DeviceApplier is the part of the device registry the writer needs: coalesced device-state
// refreshes. *devices.Store satisfies it.
type DeviceApplier interface {
	// ApplyBatch applies device-state refreshes in one round trip.
	ApplyBatch(ctx context.Context, updates []devices.Update) error
}

// event is one telemetry document: measurement time, device meta, and the samples.
type event struct {
	ID     string    `bson:"_id"`
	TS     time.Time `bson:"ts"`
	Meta   meta      `bson:"meta"`
	CPU    float64   `bson:"cpu"`
	Mem    float64   `bson:"mem"`
	Health float64   `bson:"health"`
}

// meta is the time-series meta block: the device the sample belongs to and the identity
// fields recorded at registration.
type meta struct {
	DeviceID string `bson:"device_id"`
	Region   string `bson:"region"`
	Model    string `bson:"model"`
}

// pending is one accepted heartbeat on its way to storage: its telemetry document and the
// device-state refresh that keeps the device record current.
type pending struct {
	doc    event
	update devices.Update
}

// Writer persists heartbeat telemetry and the device-state refreshes that ride with it
// through one bounded, batched pipeline. Handle queues accepted heartbeats; Run flushes them
// in writes bounded by batch size and flush interval. The queue is bounded: a slow database
// blocks the accepting path — backpressure onto the agent stream — instead of growing memory
// or silently dropping accepted events.
type Writer struct {
	events        EventStore
	devices       DeviceApplier
	batchSize     int
	flushInterval time.Duration
	queue         chan pending
	log           *slog.Logger
}

// NewWriter returns an ingest writer storing heartbeats through events and refreshing device
// state through devices. batchSize and flushInterval must be positive.
func NewWriter(
	events EventStore,
	devices DeviceApplier,
	batchSize int,
	flushInterval time.Duration,
	log *slog.Logger,
) *Writer {
	return &Writer{
		events:        events,
		devices:       devices,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		queue:         make(chan pending, queueBufferFactor*batchSize),
		log:           log,
	}
}

// Handle queues one accepted heartbeat and its device-state refresh for the next batched
// write. It blocks while the pipeline is full and fails with ctx's error once the writer is
// shutting down; a heartbeat without a measurement time is refused so no document can carry a
// zero ts into the TTL index.
func (w *Writer) Handle(ctx context.Context, hb *agentv1.Heartbeat, rec devices.Record) error {
	if hb.GetTs() == nil {
		return errors.New("ingest heartbeat: measurement time required")
	}
	accepted := time.Now()
	p := pending{
		doc: event{
			ID: hb.GetEventId(),
			TS: hb.GetTs().AsTime(),
			Meta: meta{
				DeviceID: hb.GetDeviceId(),
				Region:   rec.Region,
				Model:    rec.Model,
			},
			CPU:    hb.GetCpu(),
			Mem:    hb.GetMem(),
			Health: hb.GetHealth(),
		},
		update: devices.Update{
			ID:        hb.GetDeviceId(),
			CurrentFw: hb.GetCurrentFw(),
			Status:    devices.StatusOnline,
			LastSeen:  accepted,
		},
	}
	select {
	case w.queue <- p:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queue heartbeat %s: %w", hb.GetEventId(), ctx.Err())
	}
}

// Run flushes batches until ctx is cancelled, then drains what the queue still holds and
// flushes it under a final timeout. A batch is retried with backoff until ctx is done, so
// returning non-nil means shutdown interrupted a write that was never accepted.
func (w *Writer) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	docs := make([]any, 0, w.batchSize)
	updates := make([]devices.Update, 0, w.batchSize)
	for {
		select {
		case <-ctx.Done():
			return w.drain(docs, updates)
		case p := <-w.queue:
			docs = append(docs, p.doc)
			updates = append(updates, p.update)
			if len(docs) < w.batchSize {
				continue
			}
		case <-ticker.C:
		}
		if err := w.flush(ctx, docs, updates); err != nil {
			// flush fails only once ctx is done: drain retries the same batch one last time.
			return w.drain(docs, updates)
		}
		docs, updates = docs[:0], updates[:0]
	}
}

// drain pulls whatever the queue still holds and flushes it under a bounded final context.
func (w *Writer) drain(docs []any, updates []devices.Update) error {
	for {
		select {
		case p := <-w.queue:
			docs = append(docs, p.doc)
			updates = append(updates, p.update)
		default:
			flushCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			defer cancel()
			return w.flush(flushCtx, docs, updates)
		}
	}
}

// flush writes one batch, retrying with backoff until it lands or ctx is done. Replays are
// safe: already-stored event ids make the insert a partial no-op and device updates are
// coalesced refreshes.
func (w *Writer) flush(ctx context.Context, docs []any, updates []devices.Update) error {
	if len(docs) == 0 && len(updates) == 0 {
		return nil
	}
	backoff := initialRetryBackoff
	for {
		err := w.writeOnce(ctx, docs, updates)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("flush heartbeat batch: %w", err)
		}
		w.log.Error("flush heartbeat batch failed, retrying",
			"events", len(docs), "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("flush heartbeat batch: %w", err)
		case <-time.After(backoff):
		}
		if backoff < maxRetryBackoff {
			backoff *= 2
		}
	}
}

// writeOnce performs one attempt at the batch: unordered event inserts (already-stored event
// ids are no-ops) followed by the coalesced device-state refreshes.
func (w *Writer) writeOnce(ctx context.Context, docs []any, updates []devices.Update) error {
	if len(docs) > 0 {
		_, err := w.events.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		if err != nil && !onlyRedeliveries(err) {
			return fmt.Errorf("insert telemetry events: %w", err)
		}
	}
	if len(updates) > 0 {
		if err := w.devices.ApplyBatch(ctx, updates); err != nil {
			return fmt.Errorf("update device state: %w", err)
		}
	}
	return nil
}

// onlyRedeliveries reports whether err is a write failure whose every write error is a
// duplicate-key conflict — the batch's new rows landed and only already-stored event ids were
// refused, which idempotent ingestion treats as a no-op. Anything else — a write concern
// error, a schema violation, a missing connection — is a real failure.
func onlyRedeliveries(err error) bool {
	var serverErr mongo.ServerError
	if !errors.As(err, &serverErr) {
		return false
	}
	codes := serverErr.ErrorCodes()
	if len(codes) == 0 {
		return false
	}
	for _, code := range codes {
		if code != errDuplicateKey {
			return false
		}
	}
	return true
}
