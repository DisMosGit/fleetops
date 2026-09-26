package agent

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// stream is the slice of the Connect stream the client uses. The generated
// AgentService_ConnectClient satisfies it, and a hand-written fake can too.
type stream interface {
	Send(*agentv1.AgentEnvelope) error
	Recv() (*agentv1.ControlEnvelope, error)
}

// runSession runs one Connect stream from open to failure. It returns established=true when
// the stream opened (the backoff resets), and nil error on graceful ctx cancellation.
func (c *Client) runSession(ctx context.Context) (bool, error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.svc.Connect(sctx)
	if err != nil {
		return false, fmt.Errorf("connect stream: %w", err)
	}

	// ready closes once every device is registered on this stream; until then the writer
	// holds heartbeats back so the server never sees a heartbeat for an unenrolled device.
	ready := make(chan struct{})

	g, gctx := errgroup.WithContext(sctx)
	g.Go(func() error {
		err := c.writeLoop(gctx, stream, ready)
		cancel() // tear the stream down so the reader's Recv unblocks
		return err
	})
	g.Go(func() error {
		err := c.readLoop(gctx, stream)
		cancel()
		return err
	})
	g.Go(func() error {
		if err := c.registerAll(gctx); err != nil {
			cancel()
			return err
		}
		close(ready)
		return nil
	})
	if err := g.Wait(); err != nil {
		return true, err
	}
	return true, nil
}

// writeLoop drains the outbound queues onto the stream. Requests flow on every stream; queued
// heartbeats — including one whose send failed on a dead stream — wait for ready (the
// registration barrier). An envelope is taken off a queue only to be retried on the next stream
// when its send fails, so no accepted message is lost.
func (c *Client) writeLoop(ctx context.Context, stream stream, ready <-chan struct{}) error {
	var heartbeatQueue <-chan *agentv1.AgentEnvelope // nil until the gate opens
	for {
		env := c.held
		switch {
		case env != nil && (heartbeatQueue != nil || env.GetHeartbeat() == nil):
			c.held = nil
		default:
			select {
			case env = <-c.reqs:
			case env = <-heartbeatQueue: // never fires while the gate is closed
			case <-ready:
				heartbeatQueue = c.heartbeats
				continue
			case <-ctx.Done():
				return nil
			}
		}

		if env.CorrelationId != "" {
			c.markSending(env.CorrelationId, true)
		}
		if err := stream.Send(env); err != nil {
			if env.CorrelationId != "" {
				c.markSending(env.CorrelationId, false)
			}
			c.held = env
			return fmt.Errorf("send envelope: %w", err)
		}
	}
}

// readLoop demuxes the control stream: commands go to the command handler, responses to the
// request waiting on their correlation id.
func (c *Client) readLoop(ctx context.Context, stream stream) error {
	for {
		env, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive control envelope: %w", err)
		}
		if cmd := env.GetCommand(); cmd != nil {
			if err := c.handler.Handle(ctx, cmd); err != nil {
				// A command handler failure must not tear the stream down: the command is
				// gone either way, and its result belongs on Report, not here.
				c.log.Error("handle command",
					"command_id", cmd.GetCommandId(), "device_id", cmd.GetDeviceId(), "err", err)
			}
			continue
		}
		if env.CorrelationId == "" {
			return errors.New("control response without correlation id")
		}
		c.deliver(env.CorrelationId, env)
	}
}

// registerAll registers every device on the current stream and waits for each acceptance
// before heartbeats may flow. Its ctx is the session's: a stream that dies mid-registration
// abandons the exchange, and the next session registers afresh.
func (c *Client) registerAll(ctx context.Context) error {
	for _, d := range c.devices {
		env := &agentv1.AgentEnvelope{
			CorrelationId: c.ids.Next("corr"),
			Payload: &agentv1.AgentEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceRequest{
				DeviceId:  d.ID,
				Model:     d.Model,
				Region:    d.Region,
				CurrentFw: d.Firmware,
			}},
		}
		resp, err := c.request(ctx, env)
		if err != nil {
			return fmt.Errorf("register device %s: %w", d.ID, err)
		}
		reg := resp.GetRegisterDevice()
		if !reg.GetAccepted() {
			return fmt.Errorf("register device %s: rejected: %s", d.ID, reg.GetReason())
		}
	}
	return nil
}
