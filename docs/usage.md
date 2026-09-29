# User guide

[README](../README.md) · Agent Mail v0.4

## Install

With Homebrew:

```sh
brew install youssef-tharwat/tap/agent-mail
agent-mail --version
```

No Cargo or Rust compiler is required. Prebuilt binaries support macOS 14+ and Linux
(glibc 2.35+), on Apple Silicon/ARM64 and Intel/x86-64.

### Direct download

Download the archive and its checksum from [GitHub Releases](https://github.com/youssef-tharwat/agent-mail/releases/latest).
For Apple Silicon:

```sh
version=0.4.0
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

Add `~/.local/bin` to your PATH. Other targets: `x86_64-apple-darwin`,
`aarch64-unknown-linux-gnu`, `x86_64-unknown-linux-gnu`. Linux can verify with
`sha256sum -c`. macOS binaries are not Apple-notarized.

The checked-out repository also provides `sh scripts/install.sh`, which detects
your platform, verifies the published checksum and installs into `~/.local/bin`.
`AGENT_MAIL_INSTALL_DIR` overrides that destination. It installs the version pinned
by that checkout. npm distribution is not provided.

## Quick start

```sh
agent-mail init project
agent-mail participant add coordinator
agent-mail participant add worker
```

Each registration returns a credential in `session`. Give each participant its
own `AGENT_MAIL_SESSION`; keep credentials out of Git. Registration does not start
an agent or prove it is running. A second `participant add` never replaces an
existing credential. Use `participant replace NAME` explicitly when needed.

As the coordinator:

```sh
export AGENT_MAIL_SESSION='<coordinator credential>'
agent-mail task create api-review "Review API changes at abc123" --owner worker
```

As the worker:

```sh
export AGENT_MAIL_SESSION='<worker credential>'
agent-mail context
agent-mail mail send coordinator "Reviewed abc123; evidence: reviews/api.md" \
  --task api-review --key api-review-result-v1
```

The task's writer decides whether the result is acceptable. Creating or updating
a task publishes the relevant notifications atomically; no bookkeeping send is
needed. Without a runtime adapter these commands work manually, with no worker.

## Group and identity selection

Use `--group GROUP` anywhere in a command, or set `AGENT_MAIL_GROUP` once.
Otherwise Mail infers the group from a standalone credential or a verified Herdr
identity. An operator without a credential can use the sole configured group.
Multiple possible groups require an explicit selection. No last-used-group state
is stored, so concurrent terminals cannot change each other's selection.

A supplied invalid credential or mismatched group fails; Mail never falls back
to another identity. `--session` overrides `AGENT_MAIL_SESSION`. Herdr-bound agents
leave the standalone credential unset. Only operators issue or replace identities.

`--state-dir` overrides `AGENT_MAIL_STATE_DIR`; otherwise the saved/default store
is used. Use the same store for the CLI, client hooks and background worker.
`init GROUP` is explicit, local setup; an inherited Herdr socket does not select
a runtime. Repeating init preserves a previously configured group connection.

## Messages

```sh
agent-mail mail list
agent-mail mail show 12
agent-mail mail send reviewer "Review def456" --key review-def456 --task api-review
agent-mail mail reply 12 "Reviewed; see reviews/api.md"
agent-mail mail resolve 13 --note "Handled by the linked task decision"
agent-mail mail withdraw 14
```

- `send` creates a new logical request. Reuse its key for identical retries; use
  a new key for a changed or genuinely new request. Add recipients with `--to`.
- `reply` answers and resolves your delivery in one transaction. Its retry identity
  is derived from the original message; an identical retry creates no second
  answer. Changed replies conflict. Resolving first and replying later conflicts.
- `resolve` records an outcome without sending an answer. Reading does not resolve.
- `withdraw` is for the sender's request, not the recipient's disposition.

Use `--body-file PATH` or `--body-file -` for bounded input on send/reply; reply
accepts either inline text or a file, never both. Large evidence belongs in Git,
CI or artifacts. Message bodies are limited to 8 KiB, summaries to 240 UTF-8 bytes.

Requests have no business deadline unless `--due-in 15m` (or seconds/hours/days)
is supplied. Delivery still runs with bounded retries. A deadline reports overdue
work; it does not authorize tools, acceptance or automatic reassignment.

## Tasks

```sh
agent-mail task list
agent-mail task show api-review
agent-mail task history api-review
agent-mail task update api-review --version 1 \
  --next-action "Add retry coverage and resubmit" \
  --reason "Coverage missing" --resolve 12
```

Creation requires an ID, task description and owner. The description becomes the
scope and initial next action; override the initial step with `--next-action`.
State defaults to `open`. Deadlines and evidence are optional. A deadline is a UTC
RFC3339 timestamp, such as `--deadline 2026-10-01T12:00:00Z`.

Identical creation retries return the original creation result, even after later
updates. Changed creation content for the same ID conflicts. An old record without
creation provenance cannot be treated as an identical retry.

Only the designated writer on the group's home machine may update a task.
Use the version you actually observed. Updates derive retry identity from the
writer, task ID and expected version. Identical retries return the original result;
changed retries or stale versions fail. Reread and reconsider on a conflict.

### Atomic decisions

Simple changes use flags; complex changes use a typed JSON document:

```json
{
  "version": 2,
  "reason": "Reviewed corrected evidence",
  "patch": {
    "state": "accepted",
    "open": false,
    "accepted_revision": "def456",
    "evidence": ["ci/run/42"]
  },
  "resolve_message": 13
}
```

```sh
agent-mail task update api-review --file acceptance.json
# Or: agent-mail task update api-review --file - < acceptance.json
```

The file contains `version`, `reason`, `patch` and optional `resolve_message`.
Patch fields: `owner`, `state`, `open`, `next_action`, `deadline`,
`accepted_revision`, `evidence`. Omitted fields retain their values; use JSON null
to clear deadline or accepted revision, and `[]` to clear evidence. JSON deadlines
are Unix seconds; CLI deadline flags accept UTC timestamps. Unknown fields and
mixed file/flag updates are rejected.

A linked resolution must refer to a request in the writer's inbox for this task.
The update, resolution, history and events all commit or all roll back. State names
are workflow-defined; closing a task does not itself establish acceptance criteria.
Stored/API records retain the field names `work_id` and `work` for task associations
and recovery summaries; the public command is `task`.

## Automatic recovery and delivery

Run one worker for the store:

```sh
agent-mail service run
```

On macOS, `service install` installs a user launchd job; `service uninstall` removes
the job while preserving data. On Linux, use foreground run under your supervisor.
Mail does not launch or supervise the agents themselves.

### Claude Code

```sh
agent-mail runtime configure claude --output .claude/agent-mail-hooks.json
claude --settings .claude/agent-mail-hooks.json
```

Launch each Claude terminal with its assigned Mail credential/environment. The
normal Claude UI, tools and permissions remain in charge. Startup hooks register
Claude's native inbox automatically. Resume with the same environment and settings
file; compaction and lifecycle hooks recover current state.

The generated file contains no credentials. Generation is atomic and refuses to
overwrite different existing content. Do not also install the generic Mail hooks.
Client trust and inbound-message policy still apply; configuration is not proof
that hooks are trusted. Tested with Claude Code 2.1.284.

A socket write is only an attempt. A correlated native `UserPromptSubmit` hook
confirms admission and supplies fresh bounded context. It does not prove the model
finished a turn or accepted a result. Missing/refused delivery remains pending.
[Live acceptance evidence](native-inbox-acceptance.md).

### Codex

```sh
agent-mail runtime configure codex --output .codex/hooks.json
```

If that file already contains hooks, generate into a separate file and merge its
entries into the intended configuration. Review/trust hooks through Codex `/hooks`.
For idle wake, attach the participant to a persistent thread on an existing local
Codex app-server:

```sh
agent-mail runtime attach worker codex \
  --socket /absolute/path/to/codex.sock --thread THREAD_UUID
```

Use the actual socket and thread; Mail verifies the endpoint. It does not create
or resume the Codex session. Hooks supply reset/compaction recovery independently
of the queue. Tested with Codex 0.157.0's experimental app-server API.
[Codex acceptance evidence](local-codex-acceptance.md).

### Delivery controls

```sh
agent-mail runtime pause
agent-mail runtime resume
agent-mail runtime detach worker
agent-mail runtime enable worker
agent-mail runtime retry worker
```

Pause/resume affects group delivery, not mail writes. Retry resets only that
participant's delivery budget; it never resumes a paused group or retries tools.
Inspect and fix the endpoint before retrying. Attempts are limited to three per
change batch, five minutes apart, with budgets persisted across worker restarts.

Detach disables native attachment across startup hooks and session restarts.
Enable permits attachment again; resume Claude to register its inbox. Explicit
native attachment also enables delivery. Herdr group delivery uses pause/resume.

## Status and troubleshooting

```sh
agent-mail status
agent-mail status --check worker
```

Status reports stored coordination and delivery facts. `--check` probes setup and
runtime endpoints, exits nonzero on failed checks, and gives corrective actions.
Neither mode starts agents, repairs state, accepts work or answers permissions.
Unknown hook trust remains unknown. Delivery receipts and task progress are separate.

## Optional Herdr integration

```sh
herdr plugin install youssef-tharwat/agent-mail
```

The plugin downloads the checksum-verified release binary. No Cargo is required.
It installs into `~/.local/bin` (or `AGENT_MAIL_INSTALL_DIR`); put that directory on
PATH for hooks. The plugin keeps a local executable for its actions too.

```sh
agent-mail init project
agent-mail runtime herdr --socket "$HERDR_SOCKET_PATH"
agent-mail participant bind coordinator --herdr-pane COORDINATOR_PANE
agent-mail participant bind worker --herdr-pane WORKER_PANE
```

Herdr owns live sessions; Mail owns durable coordination. Herdr prompts default
to notification-only because its API cannot verify an empty draft. Operators may
explicitly choose `runtime herdr-policy unguarded`; `notify` restores the default.
Do not mistake registration or an idle pane for confirmed agent progress.

## Advanced integrations

`agent-mail adapter --help` exposes lifecycle hooks, event replay, streaming,
receipts, SSH transport and the optional client-owned Claude streaming bridge.
These are programmatic interfaces; agents must not acknowledge events themselves.
A stream cursor includes binding generation. Reading or acknowledging events never
resolves business obligations. ACP is not required.

`agent-mail remote --help` exposes machine identity, home assignment, peer routing
and explicit SSH sync. Automatic sync is opt-in per configured peer. Remote live
acceptance remains separate from local runtime validation; upgrade both ends to
v0.4 before exchanging messages without deadlines.

## Agent skill

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

This installs workflow instructions, not the binary. This installer uses Node/npm;
the CLI and Homebrew installation do not require Node. The bundled skill targets
v0.4 and must be upgraded alongside the CLI.

## Upgrading

v0.4 intentionally changes the command interface without old aliases. Regenerate
Mail hooks, update the skill, and update scripts to use `task`, `mail`, `participant`,
`runtime`, `status --check` and `adapter`. Stored tasks/messages are preserved.

Stop the worker and other Mail commands and back up your state directory. On macOS
run `service uninstall` before upgrading and `service install` afterward.

```sh
brew update
brew upgrade youssef-tharwat/tap/agent-mail
agent-mail init YOUR_EXISTING_GROUP
```

Init applies migrations through schema 12. Older binaries cannot open the upgraded
store. Restart the worker after migrating. Native Claude tokens stay in the private
database; status excludes them. Uninstalling a package does not erase Mail state.

## Development

With Rust 1.85+ and Git:

```sh
git clone https://github.com/youssef-tharwat/agent-mail.git
cd agent-mail
cargo build --locked
cargo run --locked -- --help
```

Install the checkout with `cargo install --path . --locked`. Before submitting:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
```

MIT licensed. Report issues with OS, Mail/runtime versions and reproduction steps;
omit credentials and private message contents.
