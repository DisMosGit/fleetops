# cmd/ — binary entrypoints

One thin entrypoint package per FleetOps binary:

- `controlplane/` — control plane: gRPC agent server + HTTP/SSE gateway (stage 1)
- `worker/` — Temporal workers hosting the workflows and activities (stage 2)
- `agent/` — device-agent emulator running N simulated devices per process (stage 1)

**Not implemented yet.** Each `main` here is a skeleton — flag parsing, dependency wiring
placeholder, and a `run` call — and exits with an error until its delivery stage lands. All
behavior lives in `internal/` packages so it is testable without a running binary; entrypoints
only parse flags, construct dependencies, and start the run loop.
