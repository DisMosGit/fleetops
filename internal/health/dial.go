package health

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"
)

// dialTimeout bounds one connectivity dial so a probe can never hang.
const dialTimeout = 2 * time.Second

// DialCheck is a dependency connectivity check that verifies an endpoint accepts a TCP
// connection and closes it immediately: reachability without speaking the full application
// protocol, so no client-driver dependency is needed before its delivery stage.
type DialCheck struct {
	name    string
	address string
	timeout time.Duration
}

// NewDialCheck returns a DialCheck that dials address (host:port) under the dependency name.
func NewDialCheck(name, address string) *DialCheck {
	return &DialCheck{name: name, address: address, timeout: dialTimeout}
}

// Name returns the dependency name the readiness report uses.
func (c *DialCheck) Name() string { return c.name }

// Check dials the endpoint over TCP and closes the connection. Any error is wrapped with the
// dependency name and can only carry the host:port being dialed — never URI credentials.
func (c *DialCheck) Check(ctx context.Context) error {
	dialer := net.Dialer{Timeout: c.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return fmt.Errorf("%s unreachable: %w", c.name, err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("%s: close probe connection: %w", c.name, err)
	}
	return nil
}

// NewDependencyChecks returns the MongoDB, RabbitMQ, and Temporal connectivity checks for the
// given endpoints. Credentials in the connection URIs are dropped before dialing, so no check
// error can echo a secret.
func NewDependencyChecks(mongoURI, rabbitmqURL, temporalAddr string) ([]Check, error) {
	mongoAddr, err := dialAddr("mongodb", mongoURI, 27017)
	if err != nil {
		return nil, err
	}
	rabbitAddr, err := dialAddr("rabbitmq", rabbitmqURL, 5672)
	if err != nil {
		return nil, err
	}
	return []Check{
		NewDialCheck("mongodb", mongoAddr),
		NewDialCheck("rabbitmq", rabbitAddr),
		NewDialCheck("temporal", temporalAddr),
	}, nil
}

// dialAddr reduces a connection URI to the host:port a probe dials: userinfo is dropped and a
// missing port falls back to the protocol default. Errors are fixed strings because parse
// errors echo the raw URI, credentials included.
func dialAddr(name, raw string, defaultPort int) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%s: endpoint is not a valid URI", name)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("%s: endpoint has no host", name)
	}
	port := u.Port()
	if port == "" {
		port = strconv.Itoa(defaultPort)
	}
	return net.JoinHostPort(host, port), nil
}
