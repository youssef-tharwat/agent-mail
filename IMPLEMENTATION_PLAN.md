# Implementation plan

Status: in progress, 2026-09-29. [ARCHITECTURE.md](ARCHITECTURE.md) is the
product and ownership contract. Local mail, work records, context, the plugin
manifest, explicit SSH exchange, and opt-in background sync are implemented. The early public build is published; real two-machine SSH release validation
remains open.

## Starting point

The Rust implementation has a SQLite-backed local inbox, idempotent send,
reply/resolve, withdrawal, Herdr pane binding, versioned work records, bounded
context, a periodic wake worker, an SSH relay with opt-in sync, and a macOS launchd
installer. Process-boundary tests use a fake Herdr socket. The full suite and
clippy pass. A real local Herdr smoke test passed: plugin link, setup/status
actions, binding, linked mail, context, resolution, work closure, and the safe
prompt hold. A two-machine SSH smoke test and release packaging remain open.
Herdr lacks a safe empty-draft signal, so automatic agent prompts are disabled
by default. Automatic SSH sync requires per-peer operator opt-in, and changing
the configured SSH target disables it. Manual `sync` remains available.

Build the smallest complete path first: local messages plus a work item and
one restart view. Keep the same binary and SQLite database. Add no broker, job
framework, workflow language, or task execution engine.

## 0. Establish the baseline

- Run formatting, the full Rust test suite, and clippy on the current tree.
  Fix failures before adding schema changes. Record the commands and results.
- Inspect the installed Herdr API for a reliable way to avoid prompting over
  a draft or dialog. If it cannot prove safe input, automatic prompt delivery
  must stay disabled; the inbox and operator status remain usable.
- Keep all test state isolated from a real Herdr session and user Mail state.

**Exit:** current local behavior passes its suite, and the prompt-safety path
is either demonstrated or explicitly held behind a disabled default.

## 1. Add the work register and one recovery view

Files: `migrations/`, `src/store.rs`, `src/main.rs`, focused store and CLI tests.

- Add versioned `work_items` and `work_changes` tables to the existing database.
  Store ID, group, scope, owner, state, next action, deadline, accepted revision,
  bounded evidence references, designated writer, version, and update time.
  Keep a short durable change history with actor and reason.
- Link messages to a work item by stable ID. Reject unknown or cross-group
  references. The link never changes work state implicitly.
- Add `work create`, `work show`, `work list`, and `work update`. Updates require
  the expected version and a reason; a stale update returns a conflict without
  overwriting the current record. The home writer owns decisions. Mail replies
  are evidence for a writer to inspect, not automatic acceptance.
- Add `context` as one bounded read of owned open work, next actions, pending
  message summaries, and remaining counts/cursors. Fetch bodies and full work
  records only by ID. Enforce byte and row limits in the returned output.
- Keep SQL static and checked with `sqlx::query!` / `query_as!`; execute short
  transactions and never call Herdr while a transaction is open.

**Exit:** after process restart, one `context` call reconstructs an agent's
owned work and unresolved mail. Concurrent version updates cannot silently
replace each other. Sending and resolving a linked message cannot accept a
work item. The existing mail tests still pass.

## 2. Complete the local Herdr plugin

Files: `herdr-plugin.toml`, `src/herdr.rs`, `src/service.rs`,
`src/supervision.rs`, `src/main.rs`, integration tests, short install guide.

- Add a manifest with setup/status actions and a one-shot startup hook. Linking
  the plugin alone must not enroll agents or start prompting them.
- Finish install and restart behavior for macOS. Add a Linux user-service or
  documented foreground supervisor path needed by remote Linux hosts.
- Verify the inbox binding against Herdr's current machine, pane, terminal,
  and native agent session before a prompt. Rebinding a replacement is explicit.
- Apply the prompt-safety decision from step 0. A verified idle state alone is
  insufficient if an unfinished draft or dialog can be overwritten. When safe
  delivery cannot be established, retain the request and expose the hold in
  `status`; never inject the message body.
- Persist wake reservations and the shared mailbox reminder budget. Keep one
  initial wake and at most two spaced reminders, with one overdue alert.

**Exit:** a named isolated real Herdr session demonstrates send, safe wake,
reply, resolution, restart, and bounded prompts. Draft/unknown-input cases
hold delivery. Stopping the worker leaves local CLI operations usable.

## 3. Add optional SSH delivery between machines

Files: new transport module, additive SQLite migration, CLI bridge/sync
commands, `src/service.rs`, isolated two-node integration tests.

- Assign each installation a stable machine ID. Configure one home machine per
  group and explicit peer SSH targets. Install the binary on each host; do not
  assume Herdr copies plugins or configuration to remotes.
- Persist an outgoing transfer before network I/O. Exchange bounded batches
  over `ssh <peer> agent-mail bridge export/exchange`; acknowledge only after the peer
  commits. Use origin machine and message IDs to make every import idempotent.
- Let the home relay between remote nodes. Keep the work register writable only
  at home; send each remote owner a read-only, timestamped work snapshot.
  Remote results travel as Mail, and home version checks reject stale work
  proposals instead of merging them silently.
- Show queued count, oldest age, last successful sync, and last error in
  `status` and `context`. During an outage, local sends stay queued and remote
  snapshots are clearly labeled stale.

**Exit:** prove both directions, remote-to-remote relay via home, duplicate and
lost acknowledgments, process crashes at commit boundaries, SSH disconnect and
reconnect, and repeated pane IDs on separate Herdr servers. No second logical
message appears; no queued message silently disappears.

## 4. Package and release-check

Files: `README.md`, install/uninstall instructions, example Herdr setup,
CI workflow, release notes, and the existing MIT license.

- Document the one-machine path first, then optional SSH. Explain what a local
  send receipt means, how to recover after compaction, and how to inspect
  overdue or stale work. Keep the agent-facing instruction short.
- Verify clean install, upgrade/migration, service restart, preserved state on
  uninstall, and a fresh machine with no state. Test Linux and macOS paths used
  by the advertised release.
- Run format, clippy, the full suite, a packaged-binary smoke test, and the
  real Herdr/SSH scenarios on the final revision. Record any unsupported
  platform or missing guarantee plainly.

**Exit:** the README's commands work from a clean checkout; all release gates
in [ARCHITECTURE.md](ARCHITECTURE.md) are met. Publication is a separate step.

## What stays outside this project

Herdr remains the source for agent inventory, lifecycle, and prompts. Mail
keeps only mailbox routing bindings. Fleet Campaign or another caller defines
review rules, acceptance gates, and authority to reassign work. Git and CI
hold the underlying evidence. V1 does not schedule agent tool calls, replay
agent reasoning, or automatically retry side effects.

## 5. Runtime-independent participants

Keep one binary, one durable store, and the existing Herdr plugin. Mail owns
stable addresses and the work register. Herdr supplies live session identity,
lifecycle observations, and wake prompts for its bindings.

- Add standalone setup, explicit registration, generated session credentials,
  and a credential-free participant listing.
- Store runtime bindings as a typed union, separate from mailbox identity.
  Preserve schema-5 identities, mail, work, routes, and reminder budgets.
- Require explicit replacement; check the binding generation in every actor
  transaction so stale sessions cannot keep reading or writing after rotation.
- Keep standalone availability unknown and recovery driven by `context`
  checkpoints. Do not add an agent launcher, heartbeat, broker, or public listener.
- Verify actual CLI flows without Herdr, mixed-runtime messaging, stale actor
  rejection, migration of populated state, and the full existing regression suite.

**Exit:** standalone participants exchange mail and recover work with Herdr
absent; existing Herdr behavior and delivery guarantees continue to pass.
