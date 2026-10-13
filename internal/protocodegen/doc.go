// Package protocodegen holds the tests for the proto-codegen capability: the
// contract-to-Go-package layout under api/proto, the frozen v1 evolution rules
// the contracts live under, and the pinned generator toolchain behind the
// "make proto" regeneration command.
//
// The package is test-only — this file plus *_test.go files — and deliberately
// exports nothing, so it never grows into a library other packages import.
// Tests that need the generators copy the Makefile and api/proto into a temp
// directory and regenerate there, so the working tree stays untouched.
package protocodegen
