# User guide

[Back to the README](../README.md)

## Install

Download a prebuilt binary from [GitHub Releases](https://github.com/youssef-tharwat/agent-mail/releases/latest).
No Rust toolchain or Herdr installation is required.

| Platform | Archive target |
| --- | --- |
| macOS, Apple Silicon | `aarch64-apple-darwin` |
| macOS, Intel | `x86_64-apple-darwin` |
| Linux, x86-64 (glibc 2.35+) | `x86_64-unknown-linux-gnu` |
| Linux, ARM64 (glibc 2.35+) | `aarch64-unknown-linux-gnu` |

For example, on Apple Silicon:

```sh
version=0.3.0
target=aarch64-apple-darwin
archive="agent-mail-v${version}-${target}.tar.gz"
base="https://github.com/youssef-tharwat/agent-mail/releases/download/v${version}"
curl -fLO "$base/$archive"
curl -fLO "$base/$archive.sha256"
shasum -a 256 -c "$archive.sha256"
tar -xzf "$archive" agent-mail
mkdir -p "$HOME/.local/bin"
install -m 755 agent-mail "$HOME/.local/bin/agent-mail"
```

Add `~/.local/bin` to your `PATH`. On Linux, `sha256sum -c` also verifies the
checksum. macOS binaries are not Apple-notarized.

Or build from source with Rust 1.85+:

```sh
cargo install --git https://github.com/youssef-tharwat/agent-mail --tag v0.3.0 --locked
```

## Quick start

```sh
agent-mail setup --standalone --group project
agent-mail register --group project --name coordinator
agent-mail register --group project --name worker
```

Each registration returns a generated `session` credential. The operator or
launcher gives each agent its own credential through `AGENT_MAIL_SESSION`.
Set this in the worker's environment, using the worker's returned value:

```sh
export AGENT_MAIL_SESSION='<worker session>'
agent-mail context --group project
```

In the coordinator's environment, use the coordinator's credential:

```sh
export AGENT_MAIL_SESSION='<coordinator session>'
agent-mail work create --group project --id api-review --owner worker \
  --scope 'Review API changes' --next-action 'Review abc123'
agent-mail send --group project --to worker --key review-1 \
  --summary 'Review abc123' --work-id api-review
```

Both paths use the same send, inbox, resolve, context, and work commands.
`--session <credential>` also works; an explicit credential selects standalone
identity even inside Herdr. Invalid credentials fail without falling back to
another identity. Leave it unset for the normal Herdr pane binding.

`agent-mail participants --group project` lists addresses and runtime bindings
without exposing credentials. This is a registration view; it does not infer
whether a worker is alive. Without an adapter, standalone agents check `context` at checkpoints.
No background worker is needed for local mail or work operations. When run,
the worker reports pending/overdue standalone mail and unknown availability;
it cannot wake a standalone agent or send an operator notification without a
runtime integration.

To replace a standalone session explicitly:

```sh
agent-mail register --group project --name worker --replace
```

Supply the new credential to the replacement session. The old credential stops
working; the mailbox ID, pending mail, work ownership, and history survive.
The same `--replace` rule applies when switching between standalone and Herdr
bindings. Configure a Herdr socket with `setup --group project --socket PATH`
before using `bind`. A registered remote route cannot be converted this way.

`setup` uses a supplied or inherited Herdr socket when present; otherwise it
creates standalone state. `--standalone` explicitly ignores an inherited socket.
It never detaches an existing Herdr group silently. Use `--state-dir PATH` or
`AGENT_MAIL_STATE_DIR` for an isolated store; both runtimes otherwise share the
normal state location.

## Agent skill

The optional [Agent Mail skill](../skills/agent-mail/SKILL.md) teaches agents to
recover work and handle deliveries without loading whole conversations into
context. Herdr plugin installation does not install agent skills. Install it
separately with a skill manager:

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

For local development, link the skill from a stable checkout:

```sh
mkdir -p ~/.codex/skills
ln -s "$(pwd)/skills/agent-mail" ~/.codex/skills/agent-mail
```

Then ask a bound or registered agent to use `$agent-mail`, or let Codex select it for an
Agent Mail task. Other agent tools can read the same `SKILL.md` directly.

## Automatic recovery and change notifications (v0.3)

These commands require v0.3.0 or later.
Stop existing Mail workers and run `setup` to migrate a backed-up store to schema 9;
v0.2.0 cannot open the upgraded database.

Mail and work changes automatically publish recipient-scoped events in the same
SQLite transaction. The owner and designated writer receive work changes;
reassignment also notifies the previous owner. No separate notification send is
needed. Work changes participate in the existing bounded Herdr wake policy.

### Enable lifecycle hooks

Launch each client with its assigned `AGENT_MAIL_SESSION` (standalone) or its
verified Herdr pane binding, plus `AGENT_MAIL_GROUP` and, for a custom store,
`AGENT_MAIL_STATE_DIR`. The `agent-mail` binary must be on the client's `PATH`.

```sh
agent-mail hooks-config > /tmp/agent-mail-hooks.json
```

Merge the generated `hooks` entries into your client's existing configuration:

- **Codex:** `.codex/hooks.json` in the intended project, or `~/.codex/hooks.json`.
  Review and trust the new entries through `/hooks`; untrusted hooks are skipped.
- **Claude Code:** the `hooks` object in `.claude/settings.json` in the intended
  project. Follow the client's hook approval/setup flow.

Do not overwrite unrelated hooks. Configure only sessions assigned a Mail
identity; a missing/invalid identity fails visibly instead of borrowing another
participant's mailbox.

The common adapter reads lifecycle JSON on stdin and emits the documented hook
response. `SessionStart` restores state on startup/resume/compaction;
`UserPromptSubmit`, `PreToolUse`, and `PostToolUse` surface changes.
`PostCompact` invalidates cached emission state for clients that do not call
`SessionStart(compact)`, restoring context at the next supported boundary. `Stop` may request one
continuation for new obligations per recovery epoch, and never recursively
continues a stop-hook turn. No model call polls SQLite. Unchanged state emits no
context except up to two retries, spaced five minutes apart, after an emission
whose consumption cannot be confirmed. Reset always reconstructs current state.
Context text is capped at 6,000 UTF-8 bytes and contains summaries plus change IDs.

**Live check:** Codex 0.157 received assigned work through trusted hooks, recovered
changed work after manual compaction, and recovered changes after session resume,
without model tool calls. In that version, manual compaction needed the
`PostCompact` invalidation plus next-prompt fallback; immediate post-compaction
`SessionStart` injection was not observed. Claude Code is protocol-tested only.

**Limits:** each installed client must load and trust its hooks. A successful stdout write does not prove model
consumption: hook attempts never manufacture delivery receipts. `status` shows
unacknowledged notifications and emission attempts. Hooks need client lifecycle
activity; use the Codex queue adapter below for idle sessions. Herdr's optional wake
path retains its existing safety hold. Neither mode guarantees agent progress.

Hook contracts: [Codex](https://learn.chatgpt.com/docs/hooks),
[Claude Code](https://code.claude.com/docs/en/hooks).

### Wake an idle Codex session

For an existing persistent thread on a running local Codex app-server:

```sh
agent-mail attach-codex --group project --name worker \
  --socket /absolute/path/to/codex.sock --thread THREAD_UUID
agent-mail service run
```

Attachment is explicit permission to queue Mail updates to that thread. Use the
thread's actual UUID and its server's Unix socket; Mail verifies it exists and is
persistent. Start a dedicated server with `codex app-server --listen
unix:///absolute/path/to/codex.sock` if your client needs one, then connect your
client to that server. Mail does not start or resume Codex processes. Give the
client its participant credential as described above; attachment only configures
delivery, it does not set the client's environment. One thread can serve one Mail
participant per store.

The worker wakes an idle thread for current actionable obligations, using a summary capped at 6,000
UTF-8 bytes. Codex starts the turn; no agent poll or separate `context` call is
needed. Changes made while busy remain durable. Closed or reassigned work can steer an active
turn with an expected-turn precondition. Idle closure and passive receipt changes
remain available for recovery without starting courtesy turns. Install the lifecycle hooks too
for recovery during compaction and session resume. The queue adapter was tested
against Codex 0.157.0's experimental app-server API.

`pause --group project` holds delivery; `resume` restores it.
`detach-codex --group project --name worker` disables the endpoint.
Replacing a participant invalidates its attachment; attach the replacement
explicitly. `status` reports delivery cursors, binding validity, attempts and the
latest worker result. `resume --group project --rearm worker` resets an exhausted
retry budget.

A confirmed queue receipt advances the notification cursor, never resolves mail
or accepts work. An uncertain send gets at most three attempts per change batch,
spaced five minutes apart; a new change gets a fresh budget. Retry state survives
worker restarts. Codex does **not** deduplicate queue entries by client message ID,
so a lost response can cause a duplicate wake. Idempotent sends and decisions
protect business state. Queued input is not proof that the model acted on it.
Herdr's `prompt-mode --enable-unguarded` is unrelated to Codex attachment.

### Submit one work decision

The designated writer can update work and resolve a linked request atomically:

```json
{
  "key": "accept-api-v2",
  "version": 2,
  "reason": "Evidence verified",
  "patch": {
    "state": "accepted",
    "open": false,
    "accepted_revision": "abc123",
    "evidence": ["ci/run/42"]
  },
  "resolve_message": 12
}
```

```sh
agent-mail work decide --group project api-review --file decision.json
```

Use the current work version and your actual message ID. `resolve_message` is
optional; when supplied, the request must be in the writer's inbox and linked
to this work item. A failed step rolls back the decision, resolution, history,
and events. Retrying the same key and content returns the original result;
changed content is rejected. Workflow policy still determines acceptance.
Workers submit evidence using linked mail; they cannot accept their own work
unless they are its designated writer.

### Programmatic subscribers

`agent-mail events` returns a bounded page with a cursor and binding generation.
After an adapter confirms delivery of an individual event, it may call
`agent-mail ack EVENT_ID`. Acknowledgment never resolves mail or closes work.
Replacement sessions replay events independently of old binding receipts.
The built-in hooks deliberately track emission attempts separately from these
confirmed receipts. Detailed events and underlying obligations remain durable.

## Optional Herdr integration

Herdr supplies live agent identity, lifecycle observations, and wake hints.
Agent Mail keeps the durable participant registry, mail, and work records.

### Install the plugin

The Herdr plugin currently builds from source, requiring Rust 1.85+ and Herdr
0.9.1+. It installs the CLI into Cargo's bin directory:

```sh
herdr plugin install youssef-tharwat/agent-mail
# macOS: set up state and install the background worker
herdr plugin action invoke setup --plugin youssef-tharwat.agent-mail
# Linux: set up state; run the worker under your own supervisor
herdr plugin action invoke setup-linux --plugin youssef-tharwat.agent-mail
```

Setup does not bind agents. Run `herdr pane list`, then bind the intended native
sessions:

```sh
agent-mail bind --name coordinator --target YOUR_COORDINATOR_PANE
agent-mail bind --name worker --target YOUR_WORKER_PANE
```

For an existing named group, configure its socket with
`agent-mail setup --group project --socket "$HERDR_SOCKET_PATH"` and pass
`--group project` when binding. Leave `AGENT_MAIL_SESSION` unset inside bound
Herdr panes. The skill is installed separately using the command above.

### Local plugin development

```sh
cargo install --path . --locked
herdr plugin link . --enabled
```

On macOS, `agent-mail service install` installs a user launchd job; on Linux,
run `agent-mail service run` under your own process supervisor.
`service uninstall` preserves the database.

### Quiet prompts

Herdr's current socket API does not expose a reliable empty-draft check.
Automatic agent prompts are therefore **off by default**. Pending mail remains
in SQLite and `context`/`status`; the worker can raise one operator alert when
it becomes overdue. An operator who accepts the risk of overwriting an
unfinished agent draft may explicitly run:

```sh
agent-mail prompt-mode --group project --enable-unguarded
```

In that mode the worker sends only a short fixed inbox hint to a verified idle
agent, with at most one initial prompt and two reminders spaced five minutes
apart. It never inserts message bodies into prompts. `prompt-mode --disable`
returns to the safe default. `pause` and `resume` control a group's prompts.

## Upgrading

For an upgrade, stop the worker and other Mail commands, install the new binary,
rerun `setup` with the existing group and socket (or `--standalone`) to apply
migrations, then restart the worker. Schema 8 preserves existing mailbox IDs,
Herdr bindings, mail, work records, reminder budgets, and remote routes. Older
binaries cannot open the upgraded store; back it up before upgrading. On
macOS, use `service uninstall` before setup and `service install` afterward.

## Mail and work commands

With your assigned session credential (or inside a bound Herdr pane):

```sh
agent-mail send --group project --to worker --key review-1 \
  --summary 'Review revision abc123' --body-file request.txt
agent-mail context --group project
agent-mail inbox --group project 1
agent-mail resolve --group project 1 --note handled
```

Reuse a send key for a retry with identical content. A successful send means
the local SQLite transaction committed. Reading does not resolve mail. An
agent resolves its delivery explicitly and may include `--reply-key` and
`--reply-file` to publish a reply in the same transaction.

Create a work record from the designated home writer:

```sh
agent-mail work create --group project --id lane-a --scope 'Implement feature' \
  --owner worker --state active --next-action 'Inspect contract'
agent-mail send --group project --to worker --key lane-a-review \
  --summary 'Review the contract' --work-id lane-a
agent-mail work show --group project lane-a
agent-mail work update --group project lane-a --version 1 --reason 'Revision submitted' \
  --state review --next-action 'Check evidence'
```

Work updates require the current version and a reason. The home writer decides
state, ownership, and acceptance. Replies do not change work state.

## Optional remote machines

Each machine installs the binary and owns its own SQLite database. Configure
SSH aliases so the home can run `ssh ALIAS agent-mail bridge export` without
interactive input. Use the default state location on remote hosts; the bridge
reads the locator created by `setup`. There is no listening Mail port.

For a home host A and remote host B, run `setup` and `bind` for the local agents
on both hosts. Then use their `agent-mail machine-id` values:

```sh
# On B: point its group at A's authoritative work register.
agent-mail join --group project --home A_MACHINE_UUID
agent-mail route --group project --name coordinator --machine A_MACHINE_UUID

# On A: register B's logical inbox and an SSH alias for B.
agent-mail route --group project --name worker --machine B_MACHINE_UUID
agent-mail peer --machine B_MACHINE_UUID --ssh-target B_SSH_ALIAS
agent-mail sync --peer B_MACHINE_UUID
# Optional: authorize periodic sync by the home worker for this peer.
agent-mail auto-sync --peer B_MACHINE_UUID --enable
```

Run `sync` **on the home machine** after remote sends or whenever you want to
exchange queued events. Once enabled, the home worker also syncs the configured
peer about every 30 seconds. `auto-sync --peer B_MACHINE_UUID --disable` stops
future background transfers; an already running exchange may finish. Manual
`sync` still works. Adding the same SSH target
preserves the setting, while changing it disables automatic sync until the
operator opts in again. Automatic sync is off by default and requires a running
home worker. One installation must be the home for all its enrolled groups to
initiate sync.

The home forwards messages between remotes. Exchanged
events are committed before acknowledgment; a lost response causes replay,
which is deduplicated. While disconnected, local sends queue and local inboxes
remain readable. `status` shows the outbox count, oldest queued event, peer
sync time, auto-sync setting, and last error. Remote work snapshots show their
last transfer time as `synced_at` and remain
read-only; their version may be stale until another sync.

## Limits

- Named recipient fan-out is capped at 32. Message bodies are capped at 8 KiB;
  large evidence belongs in Git, CI, or artifacts referenced by path or ID.
- `context` returns at most five work summaries and five mail summaries per
  page within a 4 KiB budget. Cursors fetch further pages.
- Mail records obligations and decisions. It does not run tools, retry agent
  side effects, or decide whether a revision is correct.
- Bindings and standalone session credentials are workflow guards for agents
  sharing an OS account, not a
  security boundary against malicious same-user code.

## Development

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

License: MIT.

## Local event stream (v0.3)

Start the existing worker with `agent-mail service run`. In a participant's
configured environment:

```sh
agent-mail watch --group project
# Reconnect using the last event ID and the binding generation from Ready:
agent-mail watch --group project --after 42 --generation 1
```

The private `events.sock` uses versioned newline-delimited JSON. `ready` identifies
the participant and binding generation; `event` contains an ID, kind, subject,
and revision. It contains no message body or credential. Save the cursor only
after your consumer processes the event. A subscription never acknowledges Mail
or resolves work. Replacing the binding closes the old subscription; recover with
the new credential and cursor zero.

Database commits precede best-effort socket hints. Missed hints reconcile within
the worker's five-second interval. Writes succeed while the worker is stopped.
Replay uses batches of 32; at most 32 connections are admitted. An unread socket
is disconnected after a two-second blocked write and can resume from its cursor.
This stream is for programs; agents use runtime delivery and lifecycle recovery.

## Diagnose setup and attention (v0.3)

```sh
agent-mail doctor --group project --name worker
agent-mail status
```

`doctor` checks schema, selected identity, worker, authenticated stream, and the
configured runtime capability. Omit `--name` to check the caller's credential.
It emits structured checks (`pass`, `fail`, `warning`, `unknown`); exit 1 means at
least one failed check, exit 0 means none. Unknown hook trust requires a real
client check. Diagnostics never launch an agent, grant approval, or rotate identity.

`status.attention` lists open local work, explicit overdue deadlines, missing
standalone endpoints, unconfirmed attempts, and exhausted delivery budgets.
Results are bounded and expose `more` when truncated. Delivery is not progress:
accepted input leaves work open until its designated writer decides. No elapsed
period or workflow-state label invents a waiting state, deadline, or extra reminder.
