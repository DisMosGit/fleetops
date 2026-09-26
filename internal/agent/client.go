package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// CommandHandler receives commands the control plane dispatches to a device behind this agent.
// Firmware apply lands later and implements this seam; until then cmd/agent wires a logging
// handler.
type CommandHandler interface {
	// Handle consumes one command dispatched over the stream.
	Handle(ctx context.Context, cmd *agentv1.Command) error
}

// Options configures a Client. The zero value is not valid: Devices, Handler, and IDs must be
// set. Every knob has a documented default when zero.
type Options struct {
	// Devices are the identities registered on every stream before heartbeats flow.
	Devices []Identity
	// Handler receives commands dispatched by the control plane.
	Handler CommandHandler
	// IDs mints correlation ids for agent-initiated requests.
	IDs *IDGen
	// HeartbeatQueueBound bounds the outbound heartbeat queue; zero derives it from the
	// device count with a floor. Enqueueing blocks at the bound — never unbounded buffering.
	HeartbeatQueueBound int
	// RequestQueueBound bounds the outbound request queue; zero selects a small default.
	RequestQueueBound int
	// InitialBackoff is the first reconnect delay; zero selects 1s.
	InitialBackoff time.Duration
	// MaxBackoff caps the reconnect delay; zero selects 30s.
	MaxBackoff time.Duration
	// Jitter is the fraction of a reconnect delay jitter may add or subtract, in [0, 1];
	// zero selects 0.2.
	Jitter float64
	// Rand draws the backoff jitter; zero selects a time-seeded source.
	Rand *rand.Rand
	// Sleep waits between reconnect attempts; zero selects a ctx-aware timer.
	Sleep func(ctx context.Context, d time.Duration) error
	// Log receives stream-lifecycle logs; zero selects slog.Default().
	Log *slog.Logger
}

// Default queue and backoff values of a Client built with zero-valued Options fields.
const (
	defaultRequestQueueBound = 64
	minHeartbeatQueueBound   = 64
	defaultInitialBackoff    = time.Second
	defaultMaxBackoff        = 30 * time.Second
	defaultJitter            = 0.2
)

// pending is one in-flight agent request: its envelope, the response channel its waiter reads,
// and whether the current stream has taken it onto the wire.
type pending struct {
	env  *agentv1.AgentEnvelope
	resp chan *agentv1.ControlEnvelope
	sent bool
}

// Client maintains the agent's AgentService.Connect stream: one stream per emulator process
// multiplexing every simulated device. It registers all devices on every stream, sends queued
// heartbeats under backpressure, and reconnects with capped exponential backoff after failure
// without losing an accepted message.
type Client struct {
	svc      agentv1.AgentServiceClient
	devices  []Identity
	handler  CommandHandler
	ids      *IDGen
	log      *slog.Logger
	sleep    func(ctx context.Context, d time.Duration) error
	backoffs *backoff

	heartbeats chan *agentv1.AgentEnvelope // bounded outbound queue, gated on registration
	reqs       chan *agentv1.AgentEnvelope // bounded outbound queue, flows on every stream

	// held is the envelope whose stream send failed; the next session's writer retries it
	// before draining the queues (heartbeats still behind the registration gate). Owned by
	// the writer goroutines, which never overlap.
	held *agentv1.AgentEnvelope

	mu      sync.Mutex
	pending map[string]*pending
}

// NewClient returns a client for the given AgentService stub and options.
func NewClient(svc agentv1.AgentServiceClient, opts Options) (*Client, error) {
	if svc == nil {
		return nil, errors.New("client service: required")
	}
	if len(opts.Devices) == 0 {
		return nil, errors.New("client devices: at least one required")
	}
	if opts.Handler == nil {
		return nil, errors.New("client handler: required")
	}
	if opts.IDs == nil {
		return nil, errors.New("client ids: required")
	}

	hbBound := opts.HeartbeatQueueBound
	if hbBound <= 0 {
		hbBound = 2 * len(opts.Devices)
		if hbBound < minHeartbeatQueueBound {
			hbBound = minHeartbeatQueueBound
		}
	}
	reqBound := opts.RequestQueueBound
	if reqBound <= 0 {
		reqBound = defaultRequestQueueBound
	}
	initial := opts.InitialBackoff
	if initial <= 0 {
		initial = defaultInitialBackoff
	}
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}
	jitter := opts.Jitter
	if jitter == 0 {
		jitter = defaultJitter
	}
	r := opts.Rand
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	return &Client{
		svc:        svc,
		devices:    opts.Devices,
		handler:    opts.Handler,
		ids:        opts.IDs,
		log:        log,
		sleep:      sleep,
		backoffs:   newBackoff(initial, maxBackoff, jitter, r),
		heartbeats: make(chan *agentv1.AgentEnvelope, hbBound),
		reqs:       make(chan *agentv1.AgentEnvelope, reqBound),
		pending:    make(map[string]*pending),
	}, nil
}

// Run maintains the Connect stream until ctx is cancelled: heartbeats read from the heartbeats
// channel are delivered through the bounded outbound queue, and stream failures end in
// reconnect attempts with capped exponential backoff. Run returns nil on graceful shutdown and
// an error only when the client cannot continue (ctx cancelled while resending).
func (c *Client) Run(ctx context.Context, heartbeats <-chan *agentv1.Heartbeat) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.pump(gctx, heartbeats) })
	g.Go(func() error { return c.maintain(gctx) })
	return g.Wait()
}

// pump wraps emitted heartbeats into the outbound queue. It owns no state; its stop condition
// is ctx cancellation or a closed heartbeats channel.
func (c *Client) pump(ctx context.Context, heartbeats <-chan *agentv1.Heartbeat) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case hb, ok := <-heartbeats:
			if !ok {
				return nil
			}
			env := &agentv1.AgentEnvelope{
				Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: hb},
			}
			if err := c.enqueue(ctx, c.heartbeats, env); err != nil {
				return nil
			}
		}
	}
}

// maintain runs sessions back to back with backoff between failures until ctx is cancelled.
func (c *Client) maintain(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		established, err := c.runSession(ctx)
		if established {
			c.backoffs.Reset()
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.log.Warn("agent stream lost", "err", err)
		}
		if err := c.resendUnanswered(ctx); err != nil {
			return err
		}
		if err := c.sleep(ctx, c.backoffs.Delay()); err != nil {
			return nil
		}
	}
}

// request sends one agent request and waits for its correlated response. It spans stream
// reconnects: a request sent on a stream that dies unanswered is resent on the next stream
// under its original correlation id, and only ctx cancellation abandons the wait.
func (c *Client) request(ctx context.Context, env *agentv1.AgentEnvelope) (*agentv1.ControlEnvelope, error) {
	p := &pending{env: env, resp: make(chan *agentv1.ControlEnvelope, 1)}
	c.mu.Lock()
	c.pending[env.CorrelationId] = p
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, env.CorrelationId)
		c.mu.Unlock()
	}()

	if err := c.enqueue(ctx, c.reqs, env); err != nil {
		return nil, err
	}
	select {
	case resp := <-p.resp:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// resendUnanswered re-enqueues every request the dead stream took onto the wire but never
// answered, keeping its correlation id so the response still matches the waiting caller.
// Registrations are skipped: the session barrier registers every device afresh on every stream.
func (c *Client) resendUnanswered(ctx context.Context) error {
	c.mu.Lock()
	var stale []*pending
	for _, p := range c.pending {
		if p.sent && p.env.GetRegisterDevice() == nil {
			stale = append(stale, p)
		}
	}
	c.mu.Unlock()

	for _, p := range stale {
		c.mu.Lock()
		p.sent = false
		c.mu.Unlock()
		if err := c.enqueue(ctx, c.reqs, p.env); err != nil {
			return fmt.Errorf("resend request %s: %w", p.env.CorrelationId, err)
		}
	}
	return nil
}

// enqueue blocks until the envelope is queued or ctx is cancelled: at the queue bound the
// producer waits, which is the backpressure the specs require instead of unbounded buffering.
func (c *Client) enqueue(ctx context.Context, queue chan<- *agentv1.AgentEnvelope, env *agentv1.AgentEnvelope) error {
	select {
	case queue <- env:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// markSending records whether the stream has taken the request envelope onto the wire, so the
// reconnect cycle knows which unanswered requests need resending.
func (c *Client) markSending(correlationID string, sending bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.pending[correlationID]; ok {
		p.sent = sending
	}
}

// deliver hands a correlated response to the request waiting on it. A response with no waiter
// (already abandoned) is dropped: its exchange is re-issued by its owner.
func (c *Client) deliver(correlationID string, resp *agentv1.ControlEnvelope) {
	c.mu.Lock()
	p, ok := c.pending[correlationID]
	if ok {
		delete(c.pending, correlationID)
	}
	c.mu.Unlock()
	if !ok {
		c.log.Warn("control response without pending request", "correlation_id", correlationID)
		return
	}
	p.resp <- resp
}
