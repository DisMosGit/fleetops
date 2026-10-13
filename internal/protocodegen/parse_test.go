package protocodegen

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestParseContract guards the parser the layout checks rest on: a contract
// without go_package or without a proto package must be reported, not silently
// turned into an empty field that makes a layout assertion pass vacuously.
func TestParseContract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		src     string
		want    protoFile
		wantErr string
	}{
		{
			name: "full contract",
			src: `syntax = "proto3";
package fleetops.agent.v1;
option go_package = "github.com/DisMosGit/fleetops/api/proto/agent/v1;agentv1";
import "google/protobuf/timestamp.proto";
service AgentService {}
message Heartbeat {}
`,
			want: protoFile{
				protoPkg:  "fleetops.agent.v1",
				goPkg:     "github.com/DisMosGit/fleetops/api/proto/agent/v1",
				goPkgName: "agentv1",
				imports:   []string{"google/protobuf/timestamp.proto"},
				services:  []string{"AgentService"},
			},
		},
		{
			name: "go_package without an explicit package name",
			src: `package fleetops.rollout.v1;
option go_package = "github.com/DisMosGit/fleetops/api/proto/rollout/v1";
`,
			want: protoFile{
				protoPkg:  "fleetops.rollout.v1",
				goPkg:     "github.com/DisMosGit/fleetops/api/proto/rollout/v1",
				goPkgName: "v1",
			},
		},
		{
			name:    "no go_package",
			src:     "package fleetops.agent.v1;\n",
			wantErr: "go_package",
		},
		{
			name:    "no proto package",
			src:     "option go_package = \"example.com/x/v1\";\n",
			wantErr: "package",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const file = "api/proto/agent/v1/agent.proto"
			got, err := parseContract(file, []byte(tc.src))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseContract() error = %v, want one naming %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseContract() error = %v", err)
			}
			tc.want.path = file
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(protoFile{})); diff != "" {
				t.Errorf("parseContract() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
