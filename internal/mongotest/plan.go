//go:build integration

package mongotest

import (
	"context"
	"slices"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Plan is what the server's query planner chose for one find: the indexes it scans, whether it
// scans the collection itself, and whether it sorts after reading. It is the witness an index
// assertion rests on — a lookup whose index the bootstrap no longer creates falls back to a
// collection scan, which is what fails the test rather than being invisible.
type Plan struct {
	// Indexes are the indexes the winning plan scans, outermost stage first. The identity lookup
	// a device resolution pays reports the `_id` index.
	Indexes []string
	// CollectionScan reports whether the winning plan scans every document of the collection.
	CollectionScan bool
	// InMemorySort reports whether the winning plan sorts after reading instead of reading in the
	// order an index already holds.
	InMemorySort bool
	// Stages are the stages the server reported, kept so a failure message names what the server
	// chose instead of only what the test wanted.
	Stages []string
}

// Uses reports whether the plan is served by the named index without scanning the collection.
func (p Plan) Uses(index string) bool {
	return !p.CollectionScan && slices.Contains(p.Indexes, index)
}

// ExplainFind returns the plan the server chose for one find over a collection of db, planning
// the query without running it. sort may be nil when the lookup is unordered.
func ExplainFind(t *testing.T, db *mongo.Database, collection string, filter, sort bson.D) Plan {
	t.Helper()
	find := bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: filter}}
	if len(sort) > 0 {
		find = append(find, bson.E{Key: "sort", Value: sort})
	}
	res := db.RunCommand(context.Background(), bson.D{
		{Key: "explain", Value: find},
		{Key: "verbosity", Value: "queryPlanner"},
	})
	var explained struct {
		QueryPlanner struct {
			WinningPlan planNode `bson:"winningPlan"`
		} `bson:"queryPlanner"`
	}
	if err := res.Decode(&explained); err != nil {
		t.Fatalf("explain find on %s: %v", collection, err)
	}
	var plan Plan
	explained.QueryPlanner.WinningPlan.walk(&plan)
	return plan
}

// planNode is one stage of the server's winning plan. A stage carries one input stage, or several
// when the plan merges the branches of an $or, so a plan is the tree the server walked to answer
// the query.
type planNode struct {
	Stage       string     `bson:"stage"`
	IndexName   string     `bson:"indexName"`
	InputStage  *planNode  `bson:"inputStage"`
	InputStages []planNode `bson:"inputStages"`
}

// walk records the stage and everything below it.
func (n planNode) walk(plan *Plan) {
	plan.Stages = append(plan.Stages, n.Stage)
	switch {
	case strings.Contains(n.Stage, "IXSCAN"):
		plan.Indexes = append(plan.Indexes, n.IndexName)
	case n.Stage == "IDHACK":
		// The server answers an exact `_id` lookup without naming the index it used; that lookup
		// is the unique `_id` index by definition.
		plan.Indexes = append(plan.Indexes, "_id_")
	case n.Stage == "COLLSCAN":
		plan.CollectionScan = true
	case n.Stage == "SORT":
		plan.InMemorySort = true
	}
	if n.InputStage != nil {
		n.InputStage.walk(plan)
	}
	for i := range n.InputStages {
		n.InputStages[i].walk(plan)
	}
}
