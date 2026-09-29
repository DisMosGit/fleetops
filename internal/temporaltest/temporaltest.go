//go:build integration

// Package temporaltest boots a Temporal server for integration tests: Start brings up a PostgreSQL
// container and a Temporal server (the temporalio/auto-setup image) on a private container network
// and returns a harness carrying the frontend address, the namespace its client is bound to, and
// that connected client.
//
// Readiness is a service call, never a port dial: the frontend's mapped port accepts TCP well
// before the frontend answers, and the image's bootstrap registers the default namespace after the
// server starts serving, so a dial would report ready while a workflow start still failed. Start
// therefore polls GetSystemInfo and DescribeNamespace until both answer or the startup budget is
// spent, and reports the address it was waiting on when they never do.
//
// Tests that use the harness need Docker; there is no in-process substitute for a real frontend.
// Both containers, the network, and the client are released when t ends.
package temporaltest

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"

	temporalclient "go.temporal.io/sdk/client"
)

const (
	// postgresImage is the database the server's schema lives in.
	postgresImage = "postgres:18-alpine"
	// temporalImage is the server image the local stack runs. Its entrypoint waits for the
	// database, sets the schema up, creates the default namespace, and only then serves.
	temporalImage = "temporalio/auto-setup:1.25.2"
	// frontendPort is the container's gRPC frontend port.
	frontendPort = "7233/tcp"
	// postgresPort is the database container's SQL port.
	postgresPort = "5432/tcp"
	// postgresAlias is the network alias the server resolves its database by.
	postgresAlias = "postgres"
	// user and password are the credentials both containers are configured with.
	user     = "temporal"
	password = "temporal"
	// database is the database the schema is created in; the postgres image creates it because
	// POSTGRES_DB names it.
	database = "temporal"
	// Namespace is the namespace the auto-setup image's bootstrap creates and the harness's client
	// uses. It is exported so a caller can name it without repeating the literal.
	Namespace = "default"
	// startupBudget bounds container start plus readiness. The image's schema setup runs before the
	// frontend serves and costs tens of seconds on a cold machine, so the budget is generous.
	startupBudget = 3 * time.Minute
	// readinessPoll is the pause between readiness attempts.
	readinessPoll = 250 * time.Millisecond
	// readinessAttempt bounds one readiness call, so a frontend that accepts connections without
	// answering is a failed attempt rather than a hung one.
	readinessAttempt = 5 * time.Second
	// logTailLines is how many lines of the server's own log are reported when it never becomes
	// ready: the reason a frontend did not start is in there.
	logTailLines = 40
)

// Harness is a booted Temporal server: the frontend a client dials, the namespace the server's
// bootstrap created, and a client already connected to both.
type Harness struct {
	// Address is the frontend's host:port, as a client dials it.
	Address string
	// Namespace is the namespace Client is bound to.
	Namespace string
	// Client is a connected client bound to Namespace. It is the same client a workflow starter,
	// a state reader, or a signal sender would be built over.
	Client temporalclient.Client
}

// Start boots PostgreSQL and a Temporal server on a private container network and returns the
// harness once the frontend answers and the namespace exists. Everything it starts is released
// through t.Cleanup.
func Start(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()

	// The network is created and removed first and last: both containers join it, so it must
	// outlive them.
	private, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create temporal harness network: %v", err)
	}
	t.Cleanup(func() {
		if err := private.Remove(context.Background()); err != nil {
			t.Logf("remove temporal harness network: %v", err)
		}
	})

	startPostgres(ctx, t, private.Name)
	server := startServer(ctx, t, private.Name)

	host, err := server.Host(ctx)
	if err != nil {
		t.Fatalf("temporal container host: %v", err)
	}
	port, err := server.MappedPort(ctx, frontendPort)
	if err != nil {
		t.Fatalf("temporal container port: %v", err)
	}
	address := net.JoinHostPort(host, port.Port())

	// A lazy client is deliberate: readiness is asserted below with a budget and an error that
	// names the address, where an eager Dial would fail with the frontend's own connection error
	// and no bound on how long it waits.
	c, err := temporalclient.NewLazyClient(temporalclient.Options{
		HostPort:  address,
		Namespace: Namespace,
	})
	if err != nil {
		t.Fatalf("create temporal client for %s: %v", address, err)
	}
	t.Cleanup(c.Close)

	if err := waitReady(ctx, c, Namespace, address, startupBudget); err != nil {
		dumpLogs(ctx, t, server)
		t.Fatalf("temporal harness: %v", err)
	}
	return &Harness{Address: address, Namespace: Namespace, Client: c}
}

// startPostgres boots the database the server's schema lives in, on the harness's network under
// the alias the server resolves it by.
func startPostgres(ctx context.Context, t *testing.T, networkName string) {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: postgresImage,
			Env: map[string]string{
				"POSTGRES_USER":     user,
				"POSTGRES_PASSWORD": password,
				"POSTGRES_DB":       database,
			},
			ExposedPorts:   []string{postgresPort},
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{networkName: {postgresAlias}},
			WaitingFor:     wait.ForListeningPort(postgresPort).WithStartupTimeout(startupBudget),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start temporal database container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate temporal database container: %v", err)
		}
	})
}

// startServer boots the Temporal server itself and returns its container. The database
// configuration is the image's own dialect switch plus the credentials above;
// DYNAMIC_CONFIG_FILE_PATH is deliberately not set, because this image resolves it against a file
// it does not ship and refuses to start when it cannot.
func startServer(ctx context.Context, t *testing.T, networkName string) testcontainers.Container {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: temporalImage,
			Env: map[string]string{
				"DB":             "postgres12",
				"DB_PORT":        "5432",
				"DBNAME":         database,
				"POSTGRES_USER":  user,
				"POSTGRES_PWD":   password,
				"POSTGRES_SEEDS": postgresAlias,
			},
			ExposedPorts: []string{frontendPort},
			Networks:     []string{networkName},
			WaitingFor:   wait.ForListeningPort(frontendPort).WithStartupTimeout(startupBudget),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start temporal server container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate temporal server container: %v", err)
		}
	})
	return container
}

// waitReady polls the frontend at address until it answers and namespace is registered, or budget
// runs out. It reports the address it was waiting on, so a harness that never came up names the
// service rather than the call that happened to fail first.
func waitReady(
	ctx context.Context,
	c temporalclient.Client,
	namespace, address string,
	budget time.Duration,
) error {
	if budget <= 0 {
		return fmt.Errorf("temporal frontend at %s: readiness budget %v already elapsed", address, budget)
	}
	deadline := time.Now().Add(budget)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, readinessAttempt)
		lastErr = probe(attemptCtx, c, namespace)
		cancel()
		if lastErr == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("temporal frontend at %s did not become ready within %v: %w",
				address, budget, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("temporal frontend at %s: %w", address, ctx.Err())
		case <-time.After(readinessPoll):
		}
	}
}

// probe makes one readiness check of everything a worker's own startup needs: the frontend
// answers, the namespace this harness's client is bound to is registered, and the operator service
// serves that namespace. The last check is not redundant: the image's bootstrap registers the
// namespace while the frontend is already serving, and a frontend that reports a namespace the
// operator service has not picked up yet would let a worker's startup bootstrap fail.
func probe(ctx context.Context, c temporalclient.Client, namespace string) error {
	if _, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}); err != nil {
		return fmt.Errorf("read system info: %w", err)
	}
	if _, err := c.WorkflowService().DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace}); err != nil {
		return fmt.Errorf("describe namespace %q: %w", namespace, err)
	}
	if _, err := c.OperatorService().ListSearchAttributes(ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: namespace}); err != nil {
		return fmt.Errorf("list search attributes on namespace %q: %w", namespace, err)
	}
	return nil
}

// dumpLogs reports the tail of the server container's own log, which is where the reason a
// frontend did not start survives.
func dumpLogs(ctx context.Context, t *testing.T, server testcontainers.Container) {
	t.Helper()
	logs, err := server.Logs(ctx)
	if err != nil {
		t.Logf("temporal server log unavailable: %v", err)
		return
	}
	defer func() {
		if err := logs.Close(); err != nil {
			t.Logf("close temporal server log: %v", err)
		}
	}()
	content, err := io.ReadAll(logs)
	if err != nil {
		t.Logf("read temporal server log: %v", err)
		return
	}
	lines := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
	if len(lines) > logTailLines {
		lines = lines[len(lines)-logTailLines:]
	}
	t.Logf("temporal server log (last %d lines):\n%s", len(lines), strings.Join(lines, "\n"))
}
