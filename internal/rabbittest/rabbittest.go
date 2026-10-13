//go:build integration

// Package rabbittest boots a RabbitMQ test container carrying the real broker the telemetry
// pipeline runs against: every Start returns a ready connection URL whose host port survives a
// container restart, so integration tests can verify the topology, the retry ladder, the
// dead-letter path, and reconnection against RabbitMQ itself rather than a fake.
//
// A package's integration tests normally share one broker through Shared — wired into
// testexec.Main as SharedStack, so it is started once before the package's first test body — and
// isolate themselves by purging the queues they use. Start remains for a test that needs a broker
// of its own, and For picks between the two from the run's configuration.
package rabbittest

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

const (
	// image is the broker image; alpine keeps the pull and the boot small.
	image = "rabbitmq:3-alpine"
	// amqpPort is the container's AMQP 0-9-1 port.
	amqpPort = "5672/tcp"
	// startupTimeout bounds container start and AMQP readiness.
	startupTimeout = 90 * time.Second
	// dialRetryPause is the pause between AMQP readiness dials.
	dialRetryPause = 250 * time.Millisecond
	// settleTimeout bounds waiting for a queue to reach an expected depth.
	settleTimeout = 10 * time.Second
	// pollPause is the pause between queue-depth polls.
	pollPause = 20 * time.Millisecond
)

// Harness is a booted RabbitMQ broker reachable at URL.
type Harness struct {
	// URL is the AMQP URL of the broker, including its host port.
	URL string

	container testcontainers.Container
	// budget is how long the broker is given to accept a connection, from the run's
	// configuration.
	budget time.Duration
	// settle is how long a queue is given to reach an expected depth, from the run's
	// configuration.
	settle time.Duration
}

// Start boots a RabbitMQ container of its own and returns the harness once its AMQP port accepts a
// connection. The container is torn down when t ends. A test that does not need a broker of its own
// should call For, which shares the package's broker instead.
//
// The broker's host port is reserved by the harness instead of being allocated by Docker: a
// container that is stopped and started again keeps an explicit host port, which is what lets a
// test observe a supervised connection reconnecting to the same address.
func Start(t *testing.T) *Harness {
	t.Helper()
	return bootFor(t)
}

// bootFor starts a broker for one test, torn down when that test ends.
func bootFor(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()
	port := reservePort(t)

	h, err := boot(ctx, port, testexec.Get(t))
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() {
		if err := h.container.Terminate(context.Background()); err != nil {
			t.Logf("terminate rabbitmq container: %v", err)
		}
	})
	return h
}

// boot starts a broker container publishing AMQP on hostPort and returns it once it accepts a
// connection.
func boot(ctx context.Context, hostPort int, cfg testexec.Config) (*Harness, error) {
	rabbit, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{amqpPort},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort(amqpPort): {{HostPort: strconv.Itoa(hostPort)}},
				}
			},
			WaitingFor: wait.ForListeningPort(amqpPort).
				WithStartupTimeout(cfg.Startup(startupTimeout)),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start rabbitmq container: %w", err)
	}
	h, err := newHarness(ctx, rabbit, cfg)
	if err != nil {
		if termErr := rabbit.Terminate(ctx); termErr != nil {
			return nil, fmt.Errorf("%w (terminate broker: %v)", err, termErr)
		}
		return nil, err
	}
	return h, nil
}

// newHarness resolves a booted broker's URL and waits for it to accept a connection.
func newHarness(
	ctx context.Context,
	rabbit testcontainers.Container,
	cfg testexec.Config,
) (*Harness, error) {
	h := &Harness{
		container: rabbit,
		budget:    cfg.Startup(startupTimeout),
		settle:    cfg.Wait(settleTimeout),
	}
	url, err := h.resolveURL(ctx)
	if err != nil {
		return nil, err
	}
	h.URL = url
	if err := h.waitReady(ctx); err != nil {
		return nil, err
	}
	return h, nil
}

// resolveURL derives the broker's AMQP URL from its mapped host port.
func (h *Harness) resolveURL(ctx context.Context) (string, error) {
	host, err := h.container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("rabbitmq container host: %w", err)
	}
	port, err := h.container.MappedPort(ctx, amqpPort)
	if err != nil {
		return "", fmt.Errorf("rabbitmq container port: %w", err)
	}
	return fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port()), nil
}

// WaitReady blocks until the broker accepts an AMQP connection, so a test never races the
// broker's own startup beyond the container's listening port.
func (h *Harness) WaitReady(t *testing.T) {
	t.Helper()
	if err := h.waitReady(context.Background()); err != nil {
		t.Fatalf("%v", err)
	}
}

// waitReady polls the broker until it accepts a connection or the run's budget is spent.
func (h *Harness) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(h.budget)
	var lastErr error
	for {
		lastErr = dialReady(h.URL)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rabbitmq at %s never became ready: %w", h.URL, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("rabbitmq at %s: %w", h.URL, ctx.Err())
		case <-time.After(dialRetryPause):
		}
	}
}

// dialReady reports whether the broker at url accepts an AMQP connection.
func dialReady(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close readiness connection: %w", err)
	}
	return nil
}

// Channel opens a channel on a connection of its own, both closed when t ends. Passing the test
// that owns the channel keeps its cleanup — and any failure — in the right subtest.
func (h *Harness) Channel(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(h.URL)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close rabbitmq connection: %v", err)
		}
	})
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open rabbitmq channel: %v", err)
	}
	return ch
}

// Stop stops the broker container without removing it, so a test can observe an outage. The
// container keeps its host port, its durable queues, and their persistent messages.
func (h *Harness) Stop(t *testing.T) {
	t.Helper()
	timeout := 30 * time.Second
	if err := h.container.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stop rabbitmq container: %v", err)
	}
}

// Start starts a stopped broker container and returns once its AMQP port is ready again.
func (h *Harness) Start(t *testing.T) {
	t.Helper()
	if err := h.container.Start(context.Background()); err != nil {
		t.Fatalf("start rabbitmq container: %v", err)
	}
	h.WaitReady(t)
}

// Restart stops and starts the broker container and returns once its AMQP port is ready again.
// The explicit host port is kept, and durable queues and persistent messages survive it.
func (h *Harness) Restart(t *testing.T) {
	t.Helper()
	h.Stop(t)
	h.Start(t)
}

// Depth returns how many messages are waiting on a queue. A missing queue is a test failure
// rather than a broker error.
func (h *Harness) Depth(t *testing.T, ch *amqp.Channel, queue string, durable bool) int {
	t.Helper()
	inspected, err := ch.QueueDeclarePassive(queue, durable, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspect queue %s: %v", queue, err)
	}
	return inspected.Messages
}

// AwaitDepth waits until a queue holds want messages, so a test asserts on the settled state
// instead of sleeping.
func (h *Harness) AwaitDepth(t *testing.T, ch *amqp.Channel, queue string, durable bool, want int) {
	t.Helper()
	deadline := time.Now().Add(h.settle)
	var got int
	for {
		got = h.Depth(t, ch, queue, durable)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s depth = %d, want %d", queue, got, want)
		}
		time.Sleep(pollPause)
	}
}

// Purge removes every message waiting on a queue, so one test cannot see another's. A test on the
// shared broker purges the queues it is about to assert on before it publishes to them.
func (h *Harness) Purge(t *testing.T, ch *amqp.Channel, queue string) {
	t.Helper()
	if _, err := ch.QueuePurge(queue, false); err != nil {
		t.Fatalf("purge queue %s: %v", queue, err)
	}
}

// reservePort reserves and releases a loopback TCP port, returning the number a container should
// publish its AMQP port on.
func reservePort(t *testing.T) int {
	t.Helper()
	port, err := reserveHostPort()
	if err != nil {
		t.Fatalf("%v", err)
	}
	return port
}

// reserveHostPort is reservePort without a test to fail, for the shared broker started before any
// test body exists.
func reserveHostPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve a host port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, fmt.Errorf("release the reserved host port: %w", err)
	}
	return port, nil
}
