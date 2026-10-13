// Package repolayout holds the tests that back the structural commitments of
// the repo-scaffold capability: the top-level directory layout, the rule that
// Go code lives only under cmd/ or internal/ (plus generated code under
// api/proto/), the placeholder notes that mark paths a later delivery stage
// will fill, the Makefile targets contributors and CI rely on, and the
// placeholder-only frontend submodule.
//
// The package deliberately exports no library surface: it exists so those
// commitments are verified by a test that fails when one is removed or
// reversed, not so other packages can import it.
package repolayout
