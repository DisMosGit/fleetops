package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// The change-stream operations the view reacts to. An event of any other kind — a namespace
// event without a document key, say — carries nothing a selector's membership depends on.
const (
	// operationInsert is a device that became present.
	operationInsert = "insert"
	// operationUpdate is a device whose document changed.
	operationUpdate = "update"
	// operationReplace is a device whose document was replaced whole.
	operationReplace = "replace"
	// operationDelete is a device that left the fleet.
	operationDelete = "delete"
)

// The watcher's reconnect policy. A lost stream is re-established with capped backoff, jittered
// so several worker replicas reconnecting after a shared outage do not retry in lockstep.
const (
	// watchRetryInitial is the delay before the first reconnection attempt.
	watchRetryInitial = 100 * time.Millisecond
	// watchRetryMax caps the delay between reconnection attempts.
	watchRetryMax = 5 * time.Second
	// watchRetryJitter is the fraction of a delay the jitter may add or subtract.
	watchRetryJitter = 0.2
	// watchAwaitTime bounds how long one read of the stream waits for an event before it
	// returns an empty batch. It is what a cancelled watcher notices its cancellation within,
	// and it keeps an idle stream to one read per interval.
	watchAwaitTime = time.Second
)

// Selector names the devices a wave draws its pool from: a region and a device model.
type Selector struct {
	// Region is the selector's region.
	Region string
	// Model is the selector's device model.
	Model string
}

// documentKey identifies the document a change event is about: its _id and nothing else.
type documentKey struct {
	// ID is the changed device's identity.
	ID string `bson:"_id"`
}

// changeEvent is one devices change-stream event as the view consumes it: what happened, which
// document it happened to, and the document as it stands after the change.
type changeEvent struct {
	// ID is the event's resume token.
	ID bson.Raw `bson:"_id"`
	// OperationType is the operation the event reports.
	OperationType string `bson:"operationType"`
	// FullDocument is the document after the change. A delete carries none, and neither does an
	// update the server's lookup could not resolve.
	FullDocument bson.Raw `bson:"fullDocument"`
	// DocumentKey identifies the changed document.
	DocumentKey documentKey `bson:"documentKey"`
}

// selector returns the selector the event's document matches, and whether the event carries a
// document at all. A delete carries none by definition, and the lookup behind an update returns
// none when the document was removed before the server resolved it.
func (e changeEvent) selector() (Selector, bool) {
	if len(e.FullDocument) == 0 {
		return Selector{}, false
	}
	var fields struct {
		Model  string `bson:"model"`
		Region string `bson:"region"`
	}
	if err := bson.Unmarshal(e.FullDocument, &fields); err != nil {
		return Selector{}, false
	}
	return Selector{Region: fields.Region, Model: fields.Model}, true
}

// PoolView is the eligible-device view: for every selector it has been asked about, the ids of
// the devices that selector names, ordered by identity. An entry is built lazily by one full read
// of devices and kept current by a change stream over that collection, which applies its events
// in memory and never queries devices — freshness comes from the fleet's own writes rather than
// from repeated reads. The collection stays the source of truth: dropping an entry costs one
// query, never information.
type PoolView struct {
	devices *mongo.Collection
	log     *slog.Logger

	// mu guards everything below. A build holds it across its query so no event is applied
	// while the entry is read: an event consumed but not reflected by the read would be lost
	// from the view. The query is one indexed read of a selector's devices.
	mu    sync.Mutex
	pools map[Selector][]string
	where map[string]Selector
	token bson.Raw
	// refreshes counts the entry write-backs the view has performed, which is what makes the
	// watcher's coalescing observable: one batch writes each affected selector's entry once.
	refreshes int
}

// NewPoolView returns the eligible-device view over the devices collection, reporting a lost or
// unusable change stream to log, which must be non-nil. The view holds nothing until its first
// resolution or change-stream event: it is built from the collection, which stays the source of
// truth.
func NewPoolView(devices *mongo.Collection, log *slog.Logger) *PoolView {
	return &PoolView{
		devices: devices,
		log:     log,
		pools:   make(map[Selector][]string),
		where:   make(map[string]Selector),
	}
}

// Resolve returns the selector's eligible device ids in identity order, building the selector's
// entry with one full read of devices when the view does not hold one yet. The result is the
// caller's to keep: changing it does not change the view.
func (v *PoolView) Resolve(ctx context.Context, sel Selector) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if ids, ok := v.pools[sel]; ok {
		return slices.Clone(ids), nil
	}

	ids, err := readPool(ctx, v.devices, sel)
	if err != nil {
		return nil, err
	}
	v.pools[sel] = ids
	for _, id := range ids {
		v.where[id] = sel
	}
	return slices.Clone(ids), nil
}

// Watch keeps the view current from the devices change stream until ctx ends, closing ready once
// a stream is established. It returns an error before that if no stream could be established: a
// caller that must not answer resolutions from a view nobody maintains treats that as a startup
// failure rather than as a feature that is quietly absent. After a stream has been established, a
// lost one is resumed from the view's position with capped backoff; a position the server will
// not serve flushes the view, which then rebuilds lazily from devices. Either way the cursor is
// released before Watch returns, including when its context ends.
func (v *PoolView) Watch(ctx context.Context, ready chan<- struct{}) error {
	delay := newBackoff(watchRetryInitial, watchRetryMax, watchRetryJitter,
		rand.New(rand.NewSource(time.Now().UnixNano())))
	established := false
	for {
		resumed := v.resumeToken()
		stream, err := v.open(ctx, resumed)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if len(resumed) > 0 {
				// The position was not served: forget it, and with it the view it described,
				// so the next attempt starts from now instead of failing the same way forever.
				v.log.Warn("devices change stream position unusable; rebuilding the eligible view",
					"err", err)
				v.flush()
				v.dropToken()
				continue
			}
			if !established {
				return fmt.Errorf("watch devices: %w", err)
			}
			v.log.Warn("devices change stream not established; retrying", "err", err)
			if waitErr := sleepCtx(ctx, delay.Delay()); waitErr != nil {
				return nil
			}
			continue
		}

		if len(resumed) == 0 {
			// A stream with no position to resume from starts at now, so an entry built before
			// it may predate a change it will never see: drop what the view holds and let each
			// selector rebuild from devices.
			v.flush()
		}
		if !established {
			established = true
			if ready != nil {
				close(ready)
			}
		}
		if len(resumed) > 0 {
			v.log.Info("devices change stream resumed")
		}
		delay.Reset()
		// A stream that was interrupted leaves its position in the view, so the next attempt
		// resumes from it and the changes made while it was down are not lost.
		readErr := v.consume(ctx, stream)
		if closeErr := stream.Close(context.Background()); closeErr != nil {
			readErr = errors.Join(readErr, fmt.Errorf("release devices change stream: %w", closeErr))
		}
		if ctx.Err() != nil {
			return nil
		}
		v.log.Warn("devices change stream lost; resuming from its position", "err", readErr)
		if waitErr := sleepCtx(ctx, delay.Delay()); waitErr != nil {
			return nil
		}
	}
}

// open establishes a change stream over devices. A non-empty token resumes the stream from the
// position it was taken at; an empty one starts at the stream's own beginning, which is the
// current end of the oplog. FullDocument: updateLookup is what lets an update be applied without
// reading the document the event names, and the await bound is what lets a cancelled watcher
// notice between reads.
func (v *PoolView) open(ctx context.Context, token bson.Raw) (*mongo.ChangeStream, error) {
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(watchAwaitTime)
	if len(token) > 0 {
		opts = opts.SetResumeAfter(token)
	}
	stream, err := v.devices.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("watch devices change stream: %w", err)
	}
	return stream, nil
}

// consume applies the events a stream holds, one drained batch at a time, until the stream fails,
// the server closes it, or ctx ends. The batch is what the stream already had: a burst of device
// changes therefore costs one refresh per affected selector rather than one per event.
//
// The reads are made with a context the caller's cancellation does not reach, and ctx is checked
// between them — one await interval apart — because the driver answers a cancelled read by
// replacing the cursor and forgetting the one it held: the cursor would stay open on the server,
// with nothing left to kill it. Checking between reads instead leaves the stream usable, so the
// watcher closes its cursor on the way out.
func (v *PoolView) consume(ctx context.Context, stream *mongo.ChangeStream) error {
	await := context.WithoutCancel(ctx)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var batch []changeEvent
		for stream.TryNext(await) {
			var evt changeEvent
			if err := stream.Decode(&evt); err != nil {
				return fmt.Errorf("decode devices change event: %w", err)
			}
			batch = append(batch, evt)
		}
		if len(batch) > 0 {
			v.fold(batch)
		}
		v.setToken(stream.ResumeToken())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := stream.Err(); err != nil {
			return fmt.Errorf("read devices change stream: %w", err)
		}
		if stream.ID() == 0 {
			return errors.New("devices change stream was closed by the server")
		}
	}
}

// fold applies one drained batch of events in memory, in the order the stream reported them.
// Every selector the batch affects is written back once, so a burst of changes to one selector is
// one refresh rather than one per event. Nothing here reads devices: an insert or a looked-up
// update carries the document it describes, and a delete carries the key it removed. An event
// whose document the stream could not return drops the entry the device was in, so that selector
// rebuilds from the collection instead of guessing at membership.
func (v *PoolView) fold(events []changeEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()

	working := make(map[Selector]map[string]struct{})
	dropped := make(map[Selector]struct{})
	work := func(sel Selector) map[string]struct{} {
		if set, ok := working[sel]; ok {
			return set
		}
		set := make(map[string]struct{}, len(v.pools[sel]))
		for _, id := range v.pools[sel] {
			set[id] = struct{}{}
		}
		working[sel] = set
		return set
	}

	for _, evt := range events {
		id := evt.DocumentKey.ID
		if id == "" {
			continue
		}
		previous, held := v.where[id]
		if held {
			// A model or region change is a move: the device leaves the entry it was in
			// before it enters the one it now belongs to.
			delete(work(previous), id)
			delete(v.where, id)
		}
		if evt.OperationType == operationDelete {
			continue
		}
		sel, ok := evt.selector()
		if !ok {
			if held {
				dropped[previous] = struct{}{}
			}
			continue
		}
		if _, built := v.pools[sel]; !built {
			// Only a built entry is maintained: an entry built later reads the collection,
			// which already holds everything this event did.
			continue
		}
		work(sel)[id] = struct{}{}
		v.where[id] = sel
	}

	for sel := range dropped {
		delete(working, sel)
		delete(v.pools, sel)
		for id, held := range v.where {
			if held == sel {
				delete(v.where, id)
			}
		}
	}
	for sel, set := range working {
		v.pools[sel] = orderedIDs(set)
		v.refreshes++
	}
}

// flush drops every built entry, so each selector rebuilds from devices at its next resolution.
// It is what an unusable position leaves behind: a view that cannot be kept current is not served.
// The resume token is kept, because it is what a later attempt picks the position up from.
func (v *PoolView) flush() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.pools = make(map[Selector][]string)
	v.where = make(map[string]Selector)
}

// resumeToken returns the position the view has reached, or nil when it holds none.
func (v *PoolView) resumeToken() bson.Raw {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.token
}

// setToken records the position the stream has reached, so a lost stream resumes after it rather
// than from a position whose changes the view already holds.
func (v *PoolView) setToken(token bson.Raw) {
	if len(token) == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.token = token
}

// dropToken forgets the position, so the next stream starts from now instead of from a position
// the server would not serve.
func (v *PoolView) dropToken() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.token = nil
}

// readPool lists the devices a selector names, ordered by identity. The order is what makes
// membership reproducible: the same pool and the same shares always resolve to the same target
// sets, so a re-run of a rollout's arithmetic cannot shuffle a wave's devices.
func readPool(ctx context.Context, devices *mongo.Collection, sel Selector) ([]string, error) {
	cursor, err := devices.Find(ctx,
		bson.D{{Key: "region", Value: sel.Region}, {Key: "model", Value: sel.Model}},
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("query devices in %s/%s: %w", sel.Region, sel.Model, err)
	}
	var docs []struct {
		ID string `bson:"_id"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("read devices in %s/%s: %w", sel.Region, sel.Model, err)
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids, nil
}

// orderedIDs returns a membership set as device ids in identity order.
func orderedIDs(set map[string]struct{}) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
