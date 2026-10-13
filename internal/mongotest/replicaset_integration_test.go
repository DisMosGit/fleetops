//go:build integration

package mongotest

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/DisMosGit/fleetops/internal/testexec"
)

// TestReplicaSetStack asserts what the change-stream tests depend on: a stack initiated as a
// single-member replica set, carrying the same schema bootstrap as the standalone stack, whose
// primary accepts a change stream over devices and delivers a device's change through it. It also
// records what the stack costs to become usable, which is the budget a package running it must
// allow.
func TestReplicaSetStack(t *testing.T) {
	started := time.Now()
	harness := ReplicaSet(t)
	// The stack is started on first use, so under -run TestReplicaSetStack this measures the
	// whole boot: container, initiation, election, and bootstrap.
	t.Logf("replica-set stack usable %v after first use (documented budget %v)",
		time.Since(started), replicaSetStartupBudget)
	if elapsed := time.Since(started); elapsed > replicaSetStartupBudget {
		t.Errorf("the stack took %v to become usable, want at most %v", elapsed, replicaSetStartupBudget)
	}

	ctx := context.Background()
	t.Run("the member is the initiated set's primary", func(t *testing.T) {
		var hello bson.M
		if err := harness.DB.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
			t.Fatalf("read the member's hello: %v", err)
		}
		if primary, _ := hello["isWritablePrimary"].(bool); !primary {
			t.Fatalf("the member reports isWritablePrimary = %v, want a primary", hello["isWritablePrimary"])
		}
		if name, _ := hello["setName"].(string); name != replicaSetName {
			t.Errorf("the member belongs to set %q, want %q", name, replicaSetName)
		}
	})

	t.Run("the stack carries the schema bootstrap", func(t *testing.T) {
		// The bootstrap ran against this database too, so a test writes through the real
		// validators rather than into an unvalidated collection.
		_, err := harness.DB.Collection("devices").
			InsertOne(ctx, bson.D{{Key: "_id", Value: "rs-incomplete"}})
		if err == nil {
			t.Error("the replica-set database accepted an incomplete device, want the devices validator")
		}
	})

	t.Run("a change stream over devices delivers a change", func(t *testing.T) {
		stream, err := harness.DB.Collection("devices").Watch(ctx, mongo.Pipeline{})
		if err != nil {
			t.Fatalf("watch devices: %v", err)
		}
		t.Cleanup(func() {
			if err := stream.Close(context.Background()); err != nil {
				t.Logf("close the change stream: %v", err)
			}
		})

		insertDevice(t, harness, "rs-watched")
		readCtx, cancel := context.WithTimeout(ctx, testexec.Get(t).Wait(streamReadBudget))
		defer cancel()
		if !stream.Next(readCtx) {
			t.Fatalf("the change stream delivered no event: %v", stream.Err())
		}
		var evt struct {
			OperationType string `bson:"operationType"`
			DocumentKey   struct {
				ID string `bson:"_id"`
			} `bson:"documentKey"`
		}
		if err := stream.Decode(&evt); err != nil {
			t.Fatalf("decode the change event: %v", err)
		}
		if evt.OperationType != "insert" || evt.DocumentKey.ID != "rs-watched" {
			t.Errorf("event = %s on %q, want an insert on rs-watched",
				evt.OperationType, evt.DocumentKey.ID)
		}
	})

	t.Run("a standalone refuses the change stream the worker needs", func(t *testing.T) {
		// The default shared stack is a standalone, and this is why the replica-set stack
		// exists: a worker that could not establish its stream must fail its startup, so the
		// environment that runs one has to serve change streams.
		standalone := Shared(t)
		stream, err := standalone.DB.Collection("devices").Watch(ctx, mongo.Pipeline{})
		if err == nil {
			if closeErr := stream.Close(context.Background()); closeErr != nil {
				t.Logf("close the standalone change stream: %v", closeErr)
			}
			t.Fatal("a standalone served a change stream, want the replica-set requirement to be real")
		}
		if !strings.Contains(err.Error(), "replica set") {
			t.Errorf("a standalone refused the change stream with %v, want it to name the replica set", err)
		}
	})
}

// streamReadBudget bounds waiting for one change-stream event, which is a delivery within the same
// replica set rather than a container boot.
const streamReadBudget = 10 * time.Second
