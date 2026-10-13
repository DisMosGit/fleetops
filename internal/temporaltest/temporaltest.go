//go:build integration

// Package temporaltest boots a Temporal server for integration tests: Start brings up a PostgreSQL
// container and a Temporal server (the temporalio/auto-setup image) on a private container network
// and returns a harness carrying the frontend address, the namespace its client is bound to, and
// that connected client.
//
// A package's integration tests normally share one server through Shared — wired into
// testexec.Main as SharedStack, so it is started once before the package's first test body — and
// isolate themselves with a namespace of their own created for each test, which is what keeps one
// test's workflow ids, histories, and search attributes invisible to another. Start remains for a
// test that needs a server of its own, and For picks between the two from the run's configuration.
//
// Readiness is a service call, never a port dial: the frontend's mapped port accepts TCP well
// before the frontend answers, and the image's bootstrap registers the default namespace after the
// server starts serving, so a dial would report ready while a workflow start still failed. The
// harness polls GetSystemInfo, DescribeNamespace, and the operator service until all three answer
// or the startup budget is spent, and reports the address it was waiting on when they never do.
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
	"google.golang.org/protobuf/types/known/durationpb"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/testexec"
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
	// Namespace is the namespace the auto-setup image's bootstrap creates and a harness started
	// by Start uses. It is exported so a caller can name it without repeating the literal.
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
	// namespaceRetention is how long a per-test namespace keeps its closed workflow histories. It
	// is deliberately long: the point of the namespace is isolation during a run, not retention.
	namespaceRetention = 24 * time.Hour
	// maxNamespaceSuffix bounds the test-name part of a namespace, well inside the server's limit.
	maxNamespaceSuffix = 60
)

// Harness is a booted Temporal server: the frontend a client dials, the namespace the harness's
// client is bound to, and a client already connected to both.
type Harness struct {
	// Address is the frontend's host:port, as a client dials it.
	Address string
	// Namespace is the namespace Client is bound to.
	Namespace string
	// Client is a connected client bound to Namespace. It is the same client a workflow starter,
	// a state reader, or a signal sender would be built over.
	Client temporalclient.Client
}

// Start boots PostgreSQL and a Temporal server on a private container network of its own and
// returns the harness once the frontend answers and the default namespace exists. Everything it
// starts is released through t.Cleanup. A test that does not need a server of its own should call
// For, which shares the package's server instead.
func Start(t *testing.T) *Harness {
	t.Helper()
	stack, err := bootStack(context.Background(), testexec.Get(t))
	if err != nil {
		t.Fatalf("temporal harness: %v", err)
	}
	t.Cleanup(stack.close)
	return &Harness{Address: stack.address, Namespace: Namespace, Client: stack.client}
}

// serverStack is one booted server: the containers, the network they share, the frontend address,
// and the client a harness over the stack is built from.
type serverStack struct {
	network  *testcontainers.DockerNetwork
	postgres testcontainers.Container
	server   testcontainers.Container
	address  string
	// client is bound to Namespace and is used to register each test's namespace and to prove the
	// server is ready before a harness is handed out.
	client temporalclient.Client
	// budget is how long the server is given to answer, from the run's configuration.
	budget time.Duration
}

// bootStack starts PostgreSQL and a Temporal server on a private container network and returns the
// stack once the frontend answers and the default namespace exists. The caller owns its lifetime.
func bootStack(ctx context.Context, cfg testexec.Config) (*serverStack, error) {
	budget := cfg.Startup(startupBudget)

	// The network is created and removed first and last: both containers join it, so it must
	// outlive them.
	private, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create temporal harness network: %w", err)
	}
	stack := &serverStack{network: private, budget: budget}

	stack.postgres, err = startPostgres(ctx, private.Name, budget)
	if err != nil {
		stack.close()
		return nil, err
	}
	stack.server, err = startServer(ctx, private.Name, budget)
	if err != nil {
		stack.close()
		return nil, err
	}

	host, err := stack.server.Host(ctx)
	if err != nil {
		stack.close()
		return nil, fmt.Errorf("temporal container host: %w", err)
	}
	port, err := stack.server.MappedPort(ctx, frontendPort)
	if err != nil {
		stack.close()
		return nil, fmt.Errorf("temporal container port: %w", err)
	}
	stack.address = net.JoinHostPort(host, port.Port())

	// A lazy client is deliberate: readiness is asserted below with a budget and an error that
	// names the address, where an eager Dial would fail with the frontend's own connection error
	// and no bound on how long it waits.
	stack.client, err = temporalclient.NewLazyClient(temporalclient.Options{
		HostPort:  stack.address,
		Namespace: Namespace,
	})
	if err != nil {
		stack.close()
		return nil, fmt.Errorf("create temporal client for %s: %w", stack.address, err)
	}
	if err := waitReady(ctx, stack.client, Namespace, stack.address, budget); err != nil {
		tail := serverLogTail(ctx, stack.server)
		stack.close()
		return nil, fmt.Errorf("%w\nserver log (last %d lines):\n%s", err, logTailLines, tail)
	}
	return stack, nil
}

// close releases the stack's client, its containers, and their network, in the order that leaves
// nothing orphaned: the client first, the containers next, the network they joined last.
func (s *serverStack) close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
	for _, container := range []testcontainers.Container{s.server, s.postgres} {
		if container == nil {
			continue
		}
		if err := container.Terminate(context.Background()); err != nil {
			fmt.Printf("temporal harness: terminate container: %v\n", err)
		}
	}
	if s.network != nil {
		if err := s.network.Remove(context.Background()); err != nil {
			fmt.Printf("temporal harness: remove network: %v\n", err)
		}
		s.network = nil
	}
}

// startPostgres boots the database the server's schema lives in, on the harness's network under
// the alias the server resolves it by.
func startPostgres(
	ctx context.Context,
	networkName string,
	budget time.Duration,
) (testcontainers.Container, error) {
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
			WaitingFor:     wait.ForListeningPort(postgresPort).WithStartupTimeout(budget),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start temporal database container: %w", err)
	}
	return container, nil
}

// startServer boots the Temporal server itself and returns its container. The database
// configuration is the image's own dialect switch plus the credentials above;
// DYNAMIC_CONFIG_FILE_PATH is deliberately not set, because this image resolves it against a file
// it does not ship and refuses to start when it cannot.
func startServer(
	ctx context.Context,
	networkName string,
	budget time.Duration,
) (testcontainers.Container, error) {
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
			WaitingFor:   wait.ForListeningPort(frontendPort).WithStartupTimeout(budget),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start temporal server container: %w", err)
	}
	return container, nil
}

// registerNamespace creates a namespace the server can serve, which is how a test gets workflow
// ids, histories, and search attributes of its own without a server of its own.
func registerNamespace(ctx context.Context, c temporalclient.Client, name string) error {
	_, err := c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        name,
		WorkflowExecutionRetentionPeriod: durationpb.New(namespaceRetention),
	})
	if err != nil {
		return fmt.Errorf("register namespace %s: %w", name, err)
	}
	return nil
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

// serverLogTail reads the tail of the server container's log, so a stack that never became ready
// reports why in the error that fails the package.
func serverLogTail(ctx context.Context, server testcontainers.Container) string {
	logs, err := server.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("server log unavailable: %v", err)
	}
	defer func() {
		if err := logs.Close(); err != nil {
			fmt.Printf("temporal harness: close server log: %v\n", err)
		}
	}()
	content, err := io.ReadAll(logs)
	if err != nil {
		return fmt.Sprintf("server log unreadable: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
	if len(lines) > logTailLines {
		lines = lines[len(lines)-logTailLines:]
	}
	return strings.Join(lines, "\n")
}
