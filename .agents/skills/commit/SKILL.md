---
name: commit
description: "Review all uncommitted changes and split them into separate logically grouped conventional commits (type(scope): summary), ordered from foundational to dependent, staging files or hunks explicitly. Use when the user asks to commit, to split uncommitted work into commits, or to write commit messages."
allowed-tools: Bash(git:*)
license: MIT
metadata:
  author: fleetops
  version: "1.0"
---

Turn the current uncommitted work into a sequence of self-contained conventional commits, ordered so every commit builds on the one before it.

**Input**: Optionally a subset of paths, a requested grouping, or a scope. Without one, the scope is everything uncommitted — staged, unstaged, and untracked.

**Steps**

1. **Survey the worktree**

   Run and read all of:
   - `git status --short` — staged, unstaged, untracked, submodules
   - `git diff --stat` and `git diff` — unstaged content
   - `git diff --cached` — already-staged content
   - `git log --oneline -15` — the message style this repo actually uses
   - `git submodule status` — a `web/` pointer bump is its own commit

   Read enough of every changed file to know *what it does*, not just which directory it sits in. Never group by filename alone: a single file often carries two concerns, and two files often carry one.

2. **Group into concerns**

   Each group is one concern that stands alone and reads coherently on its own. Name every group by the message you intend to write for it before touching the index.

   - A package's `*_test.go` rides with the code it tests. A separate `test(<scope>)` commit is for standalone test infrastructure only (`internal/mongotest`, `internal/rabbittest`).
   - Config settings ride with the feature that consumes them; a proto contract rides with its regenerated stubs.
   - Docs describing changed behavior ride with the change; substantial prose becomes its own `docs(<scope>)` commit.
   - OpenSpec artifacts follow repo precedent: the change plan first (`docs(openspec): add the <change> change plan`), task tick-offs last (`docs(openspec): mark <change> tasks complete`).
   - Formatting-only churn in files unrelated to a change is never mixed into it — leave it out, or ask (step 8).

3. **Order foundational to dependent**

   Dependencies point forward: no commit may need content that only lands in a later one. The repo's ladder:

   openspec plan → proto contract + stubs → config → storage (`mongo`) → domain packages (`telemetry`, `temporal`, `rollout`, `wavehealth`, `devices`, `firmware`, `agent`, `agentserver`) → `cmd` wiring → `deploy`/dashboards → task tick-offs.

   Not every change uses every rung; preserve the relative order of the rungs it does use.

4. **Write the messages**

   `type(scope): summary`, e.g. `feat(wavehealth): aggregate wave health over a sliding window`.

   - **Types**: `feat`, `fix`, `docs`, `test`, `chore`, `build` are in use here; `refactor`, `perf`, `ci` are allowed when none of those fit.
   - **Scopes**: pick from the ones in use — `cmd`, `config`, `proto`, `mongo`, `temporal`, `telemetry`, `agent`, `agentserver`, `devices`, `firmware`, `wavehealth`, `health`, `deploy`, `web`, `deps`, `agents`, `openspec`, `repo`. Fall back to the package directory name; do not invent a new scope without a reason. `cmd` for entrypoint wiring, `deps` for `go.mod`, `agents` for `AGENTS.md` and skills, `openspec` for change plans, `repo` for repo-wide chores.
   - Imperative mood, lowercase, no trailing period, subject at most 72 characters.
   - A body only when the *why* is non-obvious; wrap it at 72.
   - No `Co-Authored-By` or "Generated with" trailers unless the user asks for them — this repo's history has none.

5. **Stage explicitly**

   Stage one group at a time with explicit paths: `git add -- <path>…`.

   When one file's hunks belong to different groups, split them: `git add -p` when the session is interactive, otherwise build a filtered patch and `git apply --cached <patch>`. Confirm what is actually staged with `git diff --cached --stat` and `git status --short` before committing.

   Never `git add -A`, `git add .`, `git add -f`, or `git commit -a`. Committing is not editing: never modify tracked content to force a clean split. If a file genuinely mixes unrelated concerns, that is an ambiguity — ask (step 8).

6. **Run the safety gate before every commit**

   Read the full staged diff — not just the stat — then check:

   - **Secrets**: passwords, tokens, API keys, `-----BEGIN … PRIVATE KEY`, `AKIA[0-9A-Z]{16}`, credential-bearing `mongodb://` or `amqp://` URLs. Never stage `.env` or `configs/local*.yaml`, and never stage a credential to "fix it later". The fake credentials in `internal/config/validate_test.go` fixtures are intentional test data.
   - **Ignored and build output**: never force-add what `.gitignore` covers — `bin/`, `*.test`, coverage files, `startup.json`, `.nda/`, editor directories.
   - **Generated code**: `api/proto/**/*.pb.go` and `*_grpc.pb.go` are tracked and belong in the commit with their `.proto` change, but only as fresh `make proto` output — never hand-edited. Regenerate `mock_*.go`; never edit them by hand.
   - **Build**: for a Go change run `go build ./...` and the touched packages' tests (`go test -race -count=1 ./internal/<pkg>/...`); run `make check` when the user asks for the full Definition of Done. `make check` rewrites files via `goimports -w .` — re-check `git status` afterwards and put each resulting edit in the commit it belongs to, or ask. Never commit a tree the commit itself breaks.
   - **Hooks**: never `--no-verify`. If a hook fails, fix the cause and re-stage.

7. **Commit and confirm the sequence**

   Commit each group with its message (`git commit -m "<subject>"`, plus `-m "<body>"` when there is one), then re-run `git status --short` before starting the next group. Never `--amend` unless the user asks for it, and never amend a commit that may be pushed — check `git branch -r --contains <sha>` first.

8. **Stop and ask when a grouping is ambiguous**

   Ask — with the concrete candidates, the exact paths or hunks each would take, and a recommended option — when:

   - a file's hunks mix unrelated concerns and cannot be split without editing it;
   - a change plausibly belongs to two groups, or its tests/docs could accompany either of two commits;
   - formatting-only churn lands in files unrelated to the change;
   - a file's intent is genuinely unclear, or work is already partly committed so an amend would be needed;
   - credential-looking content appears in the diff — never rewrite history to remove an already-committed secret, stop and report it;
   - OpenSpec task tick-offs disagree with the code actually present;
   - the user's requested grouping conflicts with a dependency order.

   Otherwise proceed: the instruction is to ask on ambiguity, not to gate every commit.

9. **Verify and report**

   After the last commit, check the result:

   - `git log --oneline <original-head>..HEAD` — the commits, in order
   - `git status --short` — only intentionally-left changes remain

   Report each commit with its hash, message, and files/hunks; what was intentionally left uncommitted and why; anything the safety gate skipped; and any commit that did not come out self-contained.

**Guardrails**

- Never push, create or delete branches, rebase, reset, or drop stashes.
- Never rewrite history — not even to remove a secret; stop and report instead.
- Never commit unless committing is what was asked for.
- In plan mode, present the grouping plan for approval instead of committing.
- Grouping is a judgment call about the user's intent: when the diff does not settle it, ask rather than guess.
