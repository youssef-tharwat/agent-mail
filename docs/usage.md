# User guide

[README](../README.md) · Agent Mail v0.8

## Install

The binary bundles the required Agent Mail skill.
With Homebrew:

```sh
brew install youssef-tharwat/tap/agent-mail
agent-mail --skill
agent-mail --version
```

No Cargo or Rust compiler is required. Prebuilt binaries support macOS 14+ and Linux
(glibc 2.35+), on Apple Silicon/ARM64 and Intel/x86-64.

Managed launch hooks supply the bundled skill at SessionStart, once per startup/resume and after compaction. Other integrations can load `agent-mail --skill`. No Node/npm is required.

### Direct download

Download the archive and its checksum from [GitHub Releases](https://github.com/youssef-tharwat/agent-mail/releases/latest).
For Apple Silicon:

```sh
version=0.8.0
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
```

In 0.8, the first `run` creates a missing registration. Concurrent
creation preserves one identity; existing credentials are never rotated implicitly.
Use `agent add NAME` when preparing assignments before launch. Registration stores
a private credential and does not prove liveness. `agent add` refuses an existing identity;
`agent replace NAME` explicitly rotates its credential while preserving tasks and
mail. Existing sessions then lose access; relaunch them with `run`.

Launch each client in its own terminal:

```sh
agent-mail run coordinator -- codex
agent-mail run worker -- claude
```

`run` supplies the stored identity, selected group, state directory and Mail binary
to the child process. There is no global current agent or credential to copy.
Retired agents and Herdr/remote bindings are rejected; existing standalone identities are reused. For multiple groups, use
`--group GROUP`. Client arguments pass through, including resume options:

```sh
agent-mail run worker -- claude --resume SESSION_ID
agent-mail run coordinator -- codex resume SESSION_ID
```

The launcher preserves terminal input/output, exit status and signals. Interactive
Codex launches supervise a private backend and terminal client; other commands
replace the launcher process. Managed native launches establish the Mail service automatically.
For one operator command, the same identity handling works with any executable:

```sh
agent-mail run coordinator -- agent-mail task create api-review "Review API" --owner worker
```

Custom commands receive identity but no runtime-specific hooks. Hooks are configured
for executables named `claude` or `codex` (including absolute paths).

Inside the coordinator’s session:

```sh
agent-mail task create api-review "Review API changes at abc123" --owner worker
```

Inside the worker’s session:

```sh
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
Patch fields: `owner`, `state`, `next_action`, `deadline`,
`accepted_revision`, `evidence`. Omitted fields retain their values; use JSON null
to clear deadline or accepted revision, and `[]` to clear evidence. JSON deadlines
are Unix seconds; CLI deadline flags accept UTC timestamps. Unknown fields and
mixed file/flag updates are rejected.

A linked resolution must refer to a request in the writer's inbox for this task.
The update, resolution, history and events all commit or all roll back. Task states are `open`, `ready`, `active`, `blocked`, `review`, `done`,
`accepted` and `cancelled`. The last three are terminal; actionability is derived
from state. The designated writer chooses transitions and may reopen a task by
setting an actionable state. `done` records completion; `accepted` records an
explicit acceptance decision. Workflow rules still define the evidence required.
Stored/API records retain the field names `work_id` and `work` for task associations
and recovery summaries; the public command is `task`.

## Automatic recovery and delivery

Managed `run NAME -- codex` and `run NAME -- claude` launches establish one local
worker and verify an authenticated stream before launching. For manual integrations, run one worker for the store:

```sh
agent-mail service run
```

On macOS, `service install` installs a user launchd job; `service uninstall` removes
the job while preserving data. On Linux, use foreground run under your supervisor.
`run` launches a client; the Mail service handles delivery only. Herdr continues
to launch and manage pane-bound agents.

### Claude Code

```sh
agent-mail run worker -- claude
```

Mail writes a credential-free hook plugin under its private state directory and
loads it with Claude’s additive `--plugin-dir` option. Existing settings, custom
hooks, tools and permissions remain controlled by Claude. Startup hooks register
its native inbox; resume and compaction hooks recover current state.

The launcher does not modify global or project client settings. Remove separately
installed Mail hooks before adopting `run` to avoid duplicate delivery. For a
manually managed launch, `runtime configure claude --output PATH` still generates
settings; use `claude --settings PATH` with the assigned identity environment.
Client trust and inbound-message policy still apply. Tested with Claude Code 2.1.285.

A socket write is only an attempt. A correlated native `UserPromptSubmit` hook
confirms admission and supplies fresh bounded context. It does not prove the model
finished a turn or accepted a result. Missing/refused delivery remains pending.
[Live acceptance evidence](native-inbox-acceptance.md).

### Codex

```sh
agent-mail run worker -- codex
```

Mail adds lifecycle hooks through launch-specific Codex configuration. Review and
trust those hooks in Codex’s native prompt or `/hooks`; untrusted hooks do not run.
The launcher never bypasses hook trust or sandbox permissions. For interactive sessions it starts a private local app-server, connects the native
UI over a Unix socket, and attaches the sole loaded thread automatically, with hooks confirming recovery. Both
processes receive only this agent's identity. The backend is reaped when the UI
exits. Headless/utility commands retain direct execution with `--no-daemon`.
Existing configuration and hook sources remain active. Tested with Codex 0.157.0.

`runtime configure codex --output PATH` remains available for manual integration.
Remove separately installed Mail hooks before switching to `run`.

Managed interactive launches discover and attach their sole loaded thread and
establish the shared worker automatically. To integrate an independently managed
Codex app-server, explicit attachment remains available:

```sh
agent-mail runtime attach worker codex \
  --socket /absolute/path/to/codex.sock --thread THREAD_UUID
```

Use the actual socket and thread; Mail verifies the endpoint. It does not create
or resume the Codex session. Hooks supply reset/compaction recovery independently
of the queue. Tested with Codex 0.157.0's experimental app-server API.
[Codex acceptance evidence](local-codex-acceptance.md).
[Managed launch and real engineering cycle](native-launch-acceptance.md).

### Delivery controls

```sh
agent-mail runtime pause
agent-mail runtime resume
agent-mail runtime detach worker
agent-mail runtime enable worker
agent-mail agent retry worker
```

Pause/resume affects group delivery, not mail writes. Retry resets only that
agent's delivery budget; it never resumes a paused group or retries tools.
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

Plain `status` shows a short readiness table. `status --json` returns the full
structured report, including retry timing, state directory and stored coordination.
`--all-groups` is the explicit installation overview; add `--json` for scripts.
`--check` probes setup and
runtime endpoints, exits nonzero on failed checks, and gives corrective actions.
Neither mode starts agents, repairs state, accepts work or answers permissions.
Unknown hook trust remains unknown. Delivery receipts and task progress are separate.

## Optional Herdr integration

```sh
herdr plugin install youssef-tharwat/agent-mail
herdr plugin enable youssef-tharwat.agent-mail
```

The plugin downloads the checksum-verified release binary. No Cargo is required.
It installs into `~/.local/bin` (or `AGENT_MAIL_INSTALL_DIR`); put that directory on
PATH for hooks. The plugin keeps a local executable for its actions too.

```sh
agent-mail init project
agent-mail runtime herdr --socket "$HERDR_SOCKET_PATH"
agent-mail agent bind coordinator --herdr-pane COORDINATOR_PANE
agent-mail agent bind worker --herdr-pane WORKER_PANE
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

The skill is required for agents using Mail, including Herdr. It is embedded in
the binary and supplied by recovery hooks at SessionStart. Normal managed launch
needs no separate skill installer. Print the exact installed instructions with:

```sh
agent-mail --skill
```

`--skill` prints the full guide; it does not register a discoverable skill. Use
[skills.sh](https://skills.sh/docs) to install the lightweight loader:

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

Select Codex and/or Claude Code in the installer. This command requires Node/npm;
the Agent Mail binary does not. No Mail database or group is needed. Start a new
client session if it does not discover the installation immediately.

The loader runs `agent-mail --skill`, obtaining instructions from the binary on
PATH. Updating the binary therefore updates the guide on its next load, without
copying documentation into every client. Already-loaded conversation context is
not replaced automatically. Managed launches prepend their binary directory to PATH;
for direct client launches, ensure PATH selects the intended version.

skills.sh owns the installed skill files. Update the loader itself through its
[CLI](https://github.com/vercel-labs/skills#skills-update):

```sh
npx skills update agent-mail -g
```

Existing installations containing a copied full guide need this update once to
receive the loader. Agent Mail does not overwrite installed skills during launch
or binary upgrades.

`status --check NAME` distinguishes `awaiting_hook`, `hook_observed`, and
`not_observed`. Evidence includes session, event and timestamp. A hook observation
proves adapter execution, not model consumption or ongoing process health. Endpoint
probing separately checks current delivery capability. Restarting a managed client
clears prior launch evidence; an old session cannot establish new readiness.

## Existing installations

Before migration, stop the delivery worker and client processes and back up the
state directory. Upgrade the binary, then run `agent-mail init GROUP`; it migrates
the store through schema 16. Older binaries cannot open the migrated store. Upgrade
both relay peers before syncing.

On macOS, `agent-mail service uninstall` stops the launchd worker and preserves its
database. Run `agent-mail service install` after migration. Managed launches load
the guide bundled with the current binary; skills.sh manages the discoverable loader.

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

The README animation is reproducible with `python3 scripts/render-demo.py` on
macOS with Pillow installed. It illustrates current CLI commands and labels its
output as state summaries.

### Upgrading task state storage

Schema 14 removes writable `open`, `--close` and `--reopen`. Use a terminal state
to close a task, or an actionable state to reopen it. Existing records, snapshots,
history and retry results are validated before migration. Unknown legacy states
or contradictions between state and `open` stop the migration atomically; resolve
them explicitly in a backed-up copy before upgrading. Mail does not guess their
business meaning. Upgrade relay peers together. Old update retry documents that
used `open` must not be replayed with different content; inspect the recorded
result and current version before issuing a new decision.

## Agent registration lifecycle (v0.7.0)

`agent list` and `agent show NAME` expose typed state and version.
`agent history NAME` returns the last 20 changes, newest first.

```sh
agent-mail agent show worker
agent-mail agent update worker --version 1 --state retired --reason "Work settled"
agent-mail agent update worker --version 2 --state registered --reason "New assignment"
agent-mail run worker -- claude
```

Use observed versions, not the example numbers. Identical retries return the original
result without applying it again; changed retries conflict. Binding changes also
advance the registration version. Runtime observations never retire agents.

Retirement rejects open tasks owned or written by the agent, open remote snapshots,
and pending incoming/outgoing requests. The task writer cannot be transferred:
settle its tasks through the workflow. Retired agents reject authentication, launches,
new mail, assignments and rebinding. Restore explicitly; standalone credentials rotate,
runtime attachments become invalid, and explicit delivery pause remains in effect.
History and coordination records are preserved. Manage remote registrations at home.

Schema 15 imports existing agents as registered/version 1 with an import history entry.
Run `init GROUP` for migration with the service stopped. Once the schema is current,
adding other groups does not require stopping delivery. Select campaigns explicitly
with `--group GROUP`; sharing a store does not mean sharing a group's inboxes.
Available starting with v0.7.0.

## Fleet-scoped status

`agent-mail --group GROUP status` shows that group's inboxes, attention, notification
budgets, endpoints, policies and service observations. Group inference follows the
current credential/Herdr identity, then the sole group; ambiguity is an error.
`agent-mail status --all-groups` is the explicit installation-wide operator view,
including peers and relay outbox counts. Do not combine it with `--group` or `--check`.
Group filters apply before bounded-query limits. A large fleet cannot hide another
fleet's pending work. Message IDs are installation-wide, but mailbox access remains
restricted to the authenticated registration in its group.

Binding a Herdr pane fails clearly when the plugin is disabled. `status --check NAME`
checks plugin enablement and group prompt policy separately from pane identity.
It never silently enables unguarded prompts or treats binding as delivery proof.


## Delivery verification (0.8)

Registration is an address. Transport acceptance is a dispatch receipt. A Claude
hook is runtime receipt. None alone marks automatic delivery ready.

When a task or message already wakes an agent, the worker includes the small
challenge in that same delivery. If there is no pending notification, it uses a
standalone probe when the client is idle. The receiving agent executes the supplied
`agent ack NONCE` command using its own identity. Only this exact acknowledgment,
bound to the current group, registration, launch and endpoint, plus connection
health checked within 30 seconds, yields `delivery.ready: true`. It never resolves
mail or changes task state. Local administrators are trusted; this is operational
evidence, not attestation of model understanding.

`status` includes delivery state, acknowledgment/receipt timestamps, attempt count,
next attempt and deadline. `status --check NAME` reports the same evidence with
connection diagnostics. There are at most three attempts spaced 60 seconds apart
within 180 seconds; restart does not reset the budget. A challenge attached to a
normal notification adds no separate wake. Busy clients are not interrupted by a
standalone probe. After diagnosing the cause, `agent retry NAME`
ensures the worker is connected and resets notification and verification budgets
atomically. It preserves identity, pause/prompt policy and business state. Never execute
an acknowledgment for another agent or obtain its challenge from storage.

Send/reply and task create/update responses include `delivery` per affected
recipient. These diagnostics cannot turn a committed write into an error. A
missing route means work is stored and awaiting delivery, not lost; do not resend
under a different key. Notifications and business obligations remain separate.

Herdr remains optional. Its notify-only policy cannot wake idle panes; use native
managed launches or explicitly opt into unguarded Herdr prompts. Verification
honors pause and policy settings. Custom commands and remote recipients do not
acquire a native wake route merely by registering.
