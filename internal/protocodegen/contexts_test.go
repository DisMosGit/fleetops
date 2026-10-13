package protocodegen

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestContextsAreIndependent backs "One package per bounded context": each
// context owns api/proto/<context>/v1/ and its own proto package, and no
// contract imports another context's contract. Adding a cross-context import
// fails here naming both the importer and the context it reached into.
func TestContextsAreIndependent(t *testing.T) {
	t.Parallel()

	fsys := os.DirFS(repoRoot)
	contracts := loadContracts(t, fsys)

	contexts := make(map[string]string) // context -> proto package
	for _, c := range contracts {
		context, ok := contractContext(c.path)
		if !ok {
			t.Errorf("%s: contract must live at api/proto/<context>/v1/<file>.proto", c.path)
			continue
		}
		if prev, seen := contexts[context]; seen && prev != c.protoPkg {
			t.Errorf("%s: context %s already declares proto package %q, not %q; one context owns one proto package", c.path, context, prev, c.protoPkg)
		}
		contexts[context] = c.protoPkg
	}
	if len(contexts) < 2 {
		t.Fatalf("found %d contract contexts, want the agent and rollout ones at least; independence needs two", len(contexts))
	}

	// No two bounded contexts may reuse one proto package.
	owner := make(map[string]string, len(contexts))
	for _, context := range sortedKeys(contexts) {
		if prev, ok := owner[contexts[context]]; ok {
			t.Errorf("contexts %s and %s share proto package %q; a new context gets its own package", prev, context, contexts[context])
			continue
		}
		owner[contexts[context]] = context
	}

	// No contract may import another context's contract.
	for _, c := range contracts {
		context, ok := contractContext(c.path)
		if !ok {
			continue
		}
		for _, imp := range c.imports {
			if from, foreign := foreignContractImport(context, imp); foreign {
				t.Errorf("%s: imports %q from context %s; a context's messages are its own and no other context imports them", c.path, imp, from)
			}
		}
	}
}

// TestForeignContractImportRejectsCrossContextImports exercises the detector
// against a synthetic cross-context import. The live contracts have none today,
// so without this control a detector that never rejects would let the scenario
// pass vacuously.
func TestForeignContractImportRejectsCrossContextImports(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		context string
		imp     string
		want    string
		foreign bool
	}{
		{name: "well-known type is shared", context: "agent", imp: "google/protobuf/timestamp.proto"},
		{name: "own context is allowed", context: "agent", imp: "agent/v1/common.proto"},
		{name: "another context is rejected", context: "agent", imp: "rollout/v1/rollout.proto", want: "rollout", foreign: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, foreign := foreignContractImport(tc.context, tc.imp)
			if foreign != tc.foreign {
				t.Fatalf("foreignContractImport(%q, %q) foreign = %v, want %v", tc.context, tc.imp, foreign, tc.foreign)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("foreignContractImport(%q, %q) context mismatch (-want +got):\n%s", tc.context, tc.imp, diff)
			}
		})
	}
}
