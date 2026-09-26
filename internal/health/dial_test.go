package health

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// listeningAddr returns an address whose TCP handshake completes: the kernel queues connects in
// the listener backlog without an Accept loop, which is all a dial probe observes.
func listeningAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})
	return ln.Addr().String()
}

// unusedPort returns an address nothing listens on: bind an ephemeral port and release it.
func unusedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

func TestDialCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address func(*testing.T) string
		timeout time.Duration
		wantErr bool
	}{
		{
			name:    "reachable endpoint passes",
			address: listeningAddr,
		},
		{
			name:    "refused connection fails",
			address: unusedPort,
			wantErr: true,
		},
		{
			// 192.0.2.1 (TEST-NET-1) never answers; with an unreachable or unroutable target
			// the dial must still come back inside its own timeout either way.
			name:    "stalled dial times out",
			address: func(*testing.T) string { return "192.0.2.1:81" },
			timeout: 100 * time.Millisecond,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			check := NewDialCheck("mongodb", tc.address(t))
			if tc.timeout != 0 {
				check.timeout = tc.timeout
			}

			start := time.Now()
			err := check.Check(context.Background())
			elapsed := time.Since(start)

			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Check() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Check() = nil, want an error")
			}
			for _, want := range []string{"mongodb", "unreachable"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Check() error = %q, want it to contain %q", err, want)
				}
			}
			if elapsed > time.Second {
				t.Errorf("Check() took %v, want it bounded by the dial timeout", elapsed)
			}
		})
	}
}

func TestNewDependencyChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mongoURI     string
		rabbitmqURL  string
		temporalAddr string
		wantAddrs    map[string]string
		wantErr      string
	}{
		{
			name:         "credentials are dropped",
			mongoURI:     "mongodb://operator:secret@db.infra:27017",
			rabbitmqURL:  "amqp://operator:secret@mq.infra:5672/",
			temporalAddr: "temporal.infra:7233",
			wantAddrs: map[string]string{
				"mongodb":  "db.infra:27017",
				"rabbitmq": "mq.infra:5672",
				"temporal": "temporal.infra:7233",
			},
		},
		{
			name:         "default ports fill in",
			mongoURI:     "mongodb://db.infra",
			rabbitmqURL:  "amqps://mq.infra",
			temporalAddr: "temporal.infra:7233",
			wantAddrs: map[string]string{
				"mongodb":  "db.infra:27017",
				"rabbitmq": "mq.infra:5672",
				"temporal": "temporal.infra:7233",
			},
		},
		{
			name:         "hostless uri fails without details",
			mongoURI:     "mongodb://",
			rabbitmqURL:  "amqp://mq.infra:5672/",
			temporalAddr: "temporal.infra:7233",
			wantErr:      "mongodb: endpoint has no host",
		},
		{
			name:         "unparseable uri fails without echoing it",
			mongoURI:     "mongodb://operator:secret@db.infra:notaport",
			rabbitmqURL:  "amqp://mq.infra:5672/",
			temporalAddr: "temporal.infra:7233",
			wantErr:      "mongodb: endpoint is not a valid URI",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, err := NewDependencyChecks(tc.mongoURI, tc.rabbitmqURL, tc.temporalAddr)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("NewDependencyChecks() = %v, want error containing %q", checks, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("NewDependencyChecks() error = %q, want it to contain %q", err, tc.wantErr)
				}
				for _, secret := range []string{"operator", "secret"} {
					if strings.Contains(err.Error(), secret) {
						t.Errorf("NewDependencyChecks() error = %q, want it not to echo %q", err, secret)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDependencyChecks() error = %v", err)
			}
			if len(checks) != len(tc.wantAddrs) {
				t.Fatalf("NewDependencyChecks() returned %d checks, want %d", len(checks), len(tc.wantAddrs))
			}
			for _, check := range checks {
				dial, ok := check.(*DialCheck)
				if !ok {
					t.Fatalf("check %T is not a *DialCheck", check)
				}
				want, ok := tc.wantAddrs[dial.name]
				if !ok {
					t.Errorf("unexpected check %q", dial.name)
					continue
				}
				if dial.address != want {
					t.Errorf("check %q dials %q, want %q", dial.name, dial.address, want)
				}
			}
		})
	}
}
