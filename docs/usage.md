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
agent-mail run coordinator -- agent-mail task create api-review "Review API" --owner worker --untracked
```

Custom commands receive identity but no runtime-specific hooks. Hooks are configured
for executables named `claude` or `codex` (including absolute paths).

The following assignment is an explicitly untracked compatibility example.
For contracted work use the [contracted task flow](#contracted-tasks-development-surface).
Inside the coordinator’s session:

```sh
agent-mail task create api-review "Review API changes at abc123" --owner worker --untracked
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

Agents use notifications rather than inbox/status polling loops. The worker owns
subscriptions and bounded retries. New actionable mail or task revisions start a
fresh Herdr wake budget automatically, with the existing five-minute cooldown.
Fetching visible records records retrieval for this binding and exact task versions;
it stops notification retries without resolving requests. Omitted page records and
later revisions remain eligible. Retrieval does not replace the explicit delivery
check acknowledgment. Diagnose and repair an exhausted route before `agent retry`;
normal new work requires no manual rearming. `status --check NAME` includes
`herdr_wake`: pending/attempted event IDs, effective attempts and next wake time.
An exhausted current generation fails this check with a repair action; fresh events
are not reported as exhausted because of an older generation's counters.

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

Upgrade the binary, then use Agent Mail normally. Commands, hooks, managed launches
and worker startup automatically upgrade an existing store to schema 18. Missing
stores still require `agent-mail init GROUP`. Help, the skill guide and read-only
`status --check NAME` diagnostics do not migrate state.

Upgrades serialize across the shared store, preserve a verified SQLite snapshot in
`STATE_DIR/backups`, and apply migrations in one transaction. The system stops the
store's delivery worker and restarts it using the upgraded executable, including
updating the executable copied by the macOS launchd installation. It preserves
registrations, pending mail, task history and explicit pause settings. A failed
migration leaves the original schema and records intact and restores the original
worker. Interrupted handoffs retain restart intent for the next invocation.

A watch interrupted for migration exits with an upgrade message. Resume it with
the new binary and the last handled cursor: `agent-mail watch --after CURSOR`.
Ordinary worker restarts still reconnect automatically. If an old command retains
a database handle, migration waits briefly and fails without changing the schema;
close that command and retry. A legacy worker that cannot be uniquely identified
requires stopping its supervisor once before retrying. External supervisors remain
operator-owned; automatic managed service replacement supports macOS launchd.

Older binaries cannot open a migrated store. Upgrade both relay peers before
syncing. Managed launches load the current binary's guide; skills.sh manages the
discoverable loader.

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
Existing stores migrate automatically on use. Once the schema is current,
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

Send/reply and legacy task create/update responses include `delivery` per affected
recipient. These diagnostics cannot turn a committed write into an error. A
missing route means work is stored and awaiting delivery, not lost; do not resend
under a different key. Notifications and business obligations remain separate.

Herdr remains optional. Its notify-only policy cannot wake idle panes; use native
managed launches or explicitly opt into unguarded Herdr prompts. Verification
honors pause and policy settings. Custom commands and remote recipients do not
acquire a native wake route merely by registering.

## Follow-through after delivery

Mail requests stay pending until reply/resolve/withdraw; task decisions still belong
to their writer. Delivery acknowledgment and record retrieval are separate from a
recorded outcome. Agent Mail now keeps an attention plan beside each local pending
request and task. Runtime readiness, retrieval, the next check, and escalations are
reported separately by `status --json` and `status --check NAME`.

New groups enable follow-through by default:

```sh
agent-mail init project
# Observation only, without automatic follow-up dispatch:
agent-mail init manual-project --no-follow-through

# Change only the settings supplied; omitted settings retain their saved values.
agent-mail --group project attention configure --interval 15m --max 1h
agent-mail --group project attention configure --observe
agent-mail --group project attention configure --enable
agent-mail --group project attention configure --notifier /absolute/path/to/notify --notifier-arg fleet
agent-mail --group project attention configure --clear-notifier
```

Plain `init` and upgrades preserve existing group policies. Use `init project
--follow-through` or `attention configure --enable` to enable an existing group.
Calling `attention configure` without flags reads the effective policy. Optional
`--file policy.json` imports a complete policy, and cannot be mixed with change flags:

```json
{"mode":"enabled","interval_seconds":900,"max_seconds":3600,"notifier":null}
```

Native Codex delivery correlates its client message ID with persisted user input
and completed-turn notifications. Trusted Claude/Codex hooks record the bounded
context they supply and reconcile it at Stop. Only versions offered to that turn
are considered. Later arrivals and superseded records are excluded; future
checkpoints retain their review time. An overdue unresolved hold goes directly
to its writer or sender for review, without prompting the worker to resume.
One unhandled completed turn creates corrective
attention; ignoring that offered correction in a later completed turn escalates
to the task writer or mail sender. Duplicate receipts are idempotent, including
after worker restart. A queue receipt or idle runtime state is never a completion.

Native retries retain the original payload and offered versions. A newer checkpoint
requires a separately correlated input or timer recovery. A new user input ends
the previous hook offer even when notification context is suppressed by cooldown.
This includes Claude delivery-verification prompts, which start an empty offer;
their receipt and explicit agent acknowledgment remain separate. A hook whose
output is discarded after a newer input or launch does not record source retrieval
or change its recovery deadline. Successful output receipts cover only the source
records and revisions included in that bounded response.
Managed hooks from an earlier launch or a different pinned session cannot change
current offers; authenticated hooks without managed-launch metadata remain supported.

Dependency changes prioritize reconciliation immediately through service hints.
The interval and maximum remain recovery bounds for missed lifecycle events,
unavailable runtimes, and overdue checkpoints. The timer path retains two reminder
opportunities before escalation. `max` must be at least four intervals and at most
seven days. Existing pause and Herdr prompt policies still apply. These times are
independent of business deadlines.

Native Codex subscriptions rejoin only already loaded threads and replay a bounded
recent history page after reconnect. Older clients without correlated input IDs,
Claude bridge sessions without trusted hooks, and Herdr sessions without lifecycle
hooks retain timer recovery. Failed/interrupted turns and missing history never
count as successful completion. No adapter answers approval requests.

### Record a next step when yielding

Fetch `task show ID` or `mail show ID` first. The response's `followup.version` is
the attention metadata version; a task also has its own top-level `version`.
For unfinished work, use a checkpoint file with UTC Unix seconds:

```json
{
  "version": 0,
  "next_step": "Review the remaining API evidence",
  "next_check_at": 1790800000,
  "waiting": null,
  "evidence": ["repo/path/report.md"]
}
```

```sh
agent-mail task checkpoint api --version 3 --key api-evidence-1 --file checkpoint.json
agent-mail mail checkpoint 42 --key request-42-review --file checkpoint.json
```

Replace the example time, IDs and both versions with current values. Times must be
in the future and within the returned escalation boundary. Identical key/input
retries are safe; changed retries and stale versions conflict. A task owner may
report intent, but only the writer changes task state, ownership, acceptance and
business deadlines. A checkpoint does not accept a task or resolve a request.
Ordinary final replies and task decisions need no extra checkpoint.

Optional `waiting` values:

```json
{"kind":"task","id":"dependency","states":["accepted"]}
```

```json
{"kind":"mail","id":57}
```

```json
{"kind":"external","responsible":"release owner","reason":"Awaiting rollout approval"}
```

A task dependency must be local and in the same group; cycles are rejected. A mail
wait names a request **you sent**. An external wait always needs a responsible
person or role and a next review time. When a condition changes, the agent fetches
current records and reassesses authority. Expiry of an approval hold does not
approve the action. An unchanged hold escalates for review instead of telling its
worker to resume implementation.

If a dependency becomes ready after the wait has escalated, the owner receives one
reassessment notification. The writer's unresolved escalation remains visible and
the original escalation boundary is preserved.

### Handle scheduled attention

```sh
agent-mail attention list
agent-mail attention list --after 123
agent-mail attention show 124
agent-mail attention checkpoint 124 --key review-extension-1 --file checkpoint.json
agent-mail attention history --task api
agent-mail attention history --mail 42
```

`attention show` fetches an occurrence addressed to your identity. Its `current`
field identifies superseded work, and its `followup` gives the current source and
metadata version. Mail occurrences include the original message and the addressed
delivery's current disposition in `mail`; task occurrences require `task show`
before acting. A sender reading escalated mail does not mark it retrieved for the
recipient or gain authority to resolve that delivery. `attention checkpoint` also
lets the request sender handle an escalation without borrowing the recipient's
identity. Only the task writer or request sender may set `extend_until`, together
with a nonempty `reason`, to authorize a later escalation boundary. Repeated reads,
acknowledgments, runtime activity, and identical reports do not extend the boundary.

A checkpoint or ordinary source outcome supersedes its scheduled occurrences.
Earlier reminders are also superseded when a later reminder/escalation is created.
An unchanged checkpoint does not supersede an occurrence or grant another interval.
Neither reading status nor inspecting history records retrieval for another agent.

### Operator alerts and compatibility

Escalation first targets the writer/sender. A self-escalation goes directly to the
operator; an unhandled escalation reaches that route after five minutes. Herdr
uses its operator notification surface. Standalone groups can configure `notifier`
with `attention configure --notifier /absolute/executable`, repeatable
`--notifier-arg VALUE`, or a policy-file array containing that executable and arguments. It is launched
without a shell, receives bounded group/attention JSON on stdin, has a five-second
timeout, and gets at most three attempts with five-minute cooldowns. An explicitly
configured notifier takes precedence over Herdr. Configure only a program you
intend Agent Mail to execute.

Without an operator route, status reports `unconfigured`; it does not claim a human
was notified. Alert acceptance is also separate from handling the underlying work.
Changing the configured notifier retries failed, unconfigured, or uncertain alerts
with a new bounded operator budget. It does not reset business delivery attempts.
`last_scan_age_seconds` exposes stale worker observations; a stopped worker relies
on process supervision to restart before it can send anything.

Status totals cover all active sources, even when its detail list is truncated.
`turn_receipts` distinguishes reserved offers from correlated completed turns;
these receipts never imply a business outcome.
Agent checks filter before pagination. Recovery responses stay within 4 KiB and
mark only returned records retrieved; `checkpoints_more` directs agents to fetch
full metadata through `task show` or `mail show`.

Schema 20 retains immutable native payloads. During the upgrade from schema 19,
open native offers without a reconstructible payload are abandoned; their history,
business records and retry budgets remain intact. New native input uses a distinct
client-ID namespace, and unfinished plans retain timer recovery.
Event subscriptions use protocol 2; an old subscriber gets an explicit
upgrade error. The existing automatic upgrade path drains the old worker and
backs up the database. Follow-up dispatch covers local tasks and local deliveries
on Herdr, managed Codex and managed Claude. Remote task snapshots and cross-machine
waits remain explicitly unsupported for follow-through; existing relay payloads
retain their original contract.

## Contracted tasks (development surface)

This source increment is awaiting composed remote validation. The installed
0.10.1 release used for campaign coordination does not provide these commands.
Managed target lifecycle and controlled text publication now have CLI consumers
of the runtime owner APIs. These additions await composed validation. Native
qualification is unavailable and enablement remains held. Task creation, a
publication receipt or a delivery check does not establish native qualification
or business acceptance.

Ordinary new `task create` requires a finite contract and explicit authority.
Incomplete inputs fail without falling back to a legacy task. `--untracked` is the
intentional compatibility escape for existing scripts and simple record keeping;
it creates no contract or execution accounting. Existing records are not adopted
on read. Existing observe and paused policies are preserved.

Finite tasks default to `open`. They can be examined and scheduled automatically
as soon as their authorization, dependencies, runtime capability, pause, budget
and active-attempt guards permit; no separate `ready` or `active` update is
required. The `open` state itself grants no execution authority, and untracked
legacy records gain none from their state. All eligibility checks, original
deadlines and execution accounting continue to apply.

Example input (replace the authorization reference with real prior consent):

```sh
agent-mail task create report 'Produce the review report' --owner worker \
  --key create-report-1 --reason 'Approved review assignment' \
  --criterion readable='Report covers the requested revision' \
  --allow 'read approved repository' --authorize approval/review-42 \
  --max-attempts 3 --max-elapsed 15m --allow-input-invalidation
agent-mail task inspect report
agent-mail task execution show report
```

The model creates the work, contract, graph, authority, finite execution accounting
and canonical receipt in one transaction. Readiness is independent of runtime
capability. `--held` stores an approval hold. `--completion-allowed` explicitly
permits completed outcomes; writer acceptance is the default. Exact integer cost
limits use `--max-cost N --cost-unit UNIT`; runtime support must enforce that unit.

Every contract requires `--allow-input-invalidation`: explicit consent to
invalidate dependent inputs when their basis changes. Creation, adoption and full
contract replacement reject its omission with `input_invalidation_consent_required`.
The CLI never adds this consent automatically.

Dependencies are ALL-of: repeat `--after TASK`. The default predicate is an accepted
outcome without a revision pin. Use `--after-outcome TASK=completed` or
`--after-revision TASK=REVISION` to change that declared predicate. A parent uses
`--parent ID --parent-version ID=VERSION`; children are required unless
`--optional-child` is explicit. `--inherit-authority` requires the declared parent
and the owner's delegation checks. Satisfied dependencies never grant new scope.

`task adopt ID --version N` takes the same contract, authority, graph, key and
reason flags, plus an explicit `--deliverable`. Adoption is atomic and preserves
old history. A contracted record cannot be adopted again to reset spending.

`task decide ID` (alias `task outcome`) uses `--version`, `--key`, and `--reason`.
It supports ordinary owner/state/next-action changes, `--clear-deadline`,
`--clear-evidence`, `--resolve MESSAGE_ID`, complete `--replace-contract`,
`--replace-requirements`, parent set/detach, explicit authorization replacement,
and `--clear-invalidation`. An authorization-only change requires explicit
`--approved-scope` units. A version conflict requires inspection and reconsideration;
the CLI never retries with an automatically refreshed version.

A successful typed outcome uses `--kind accepted|completed --candidate ID` and the
candidate's immutable revision. Negative outcomes use
`--kind cancelled|failed --revision REVISION`. `--withdraw-outcome` retains history.
Business cancellation does not assert that a running process or uncertain effects
have closed. Legacy `task update`, show/list/history wire shapes remain available;
the model rejects attempts to bypass a contracted decision through a legacy update.

### Candidates and exact inputs

Capture the exact phase inputs before producing the candidate:

```sh
agent-mail task inputs report --version VERSION --phase accept > report-inputs.json
agent-mail task candidate report --version VERSION --key candidate-1 \
  --revision artifact/review-1 --summary 'Review evidence assembled' \
  --inputs report-inputs.json --criterion-evidence readable=artifact/review-1
agent-mail task results report
```

Instead of `--inputs`, supply the original snapshot with explicit flags:
`--input-task-version`, `--input-epoch`, `--input-owner-generation`, `--input-phase`,
and repeated `--prerequisite-outcome TASK=ID`, `--required-child-outcome TASK=ID`,
and `--ancestor-authority TASK=DIGEST`. Omitted maps are empty, not inferred from
current state. File and flag snapshots cannot be mixed.

These are input examples, not passing transcripts. Ordinary business candidates
require the genuine task writer and an Accept snapshot. An exact policy-selected
reviewer may act only on its actual materialized decision task; that does not
grant review authority over an ordinary business task. An Execute snapshot cannot be relabeled
Accept. Preserve the snapshot used to produce the artifact; a fresh snapshot over
stale output is not revalidation. `task results --after CURSOR` pages immutable
history. A reported result, idle runtime, or accepted transport is not acceptance.

### Checkpoints, corrections and finite decisions

No JSON file is required to record attention intent:

```sh
agent-mail task checkpoint report --version VERSION --plan-version PLAN_VERSION \
  --key next-review-1 --next-step 'Inspect the review evidence' \
  --check-at 2026-10-02T12:00:00Z --evidence artifact/review-1
```

Use a future UTC timestamp appropriate to the actual task. `--plan-version` and
`--version` are distinct. Mail and attention checkpoints accept the same report
flags. A writer-only `--extend-until UTC --reason TEXT` changes attention timing;
it does not replenish task attempts, elapsed allowance or cost.

Original-source correction works without an addressed escalation:

```sh
agent-mail task followup show report --version VERSION
agent-mail task followup correct report --version VERSION \
  --plan-id PLAN_ID --plan-version PLAN_VERSION --key correction-1 \
  --reason 'Correct an obsolete review time' --next-step 'Review current evidence' \
  --check-at 2026-10-02T12:00:00Z --escalate-at 2026-10-02T12:15:00Z
```

Use `--plan-absent` only for an inspected missing plan. Supply every unresolved
`--case-version ID=VERSION`. Mail correction is `mail followup correct MESSAGE`
with explicit `--recipient NAME`, as original sender. It never resolves delivery.
`decision show CASE` shows original/effective times, authority and capability holds.
Advanced `decision correct` accepts a saved exact source guard; the owner's current
implementation limits this operation to an unmaterialized legacy case.

`decision policy POLICY` is an explicit separate operation, not an atomic option
on task creation. It requires original `--task ID --version N` or
`--mail ID --recipient NAME`, exact `--input-epoch`, `--candidate` and `--outcome`
where present, and finite contract, `--action`, deadline, authority-reference,
key and reason flags. `--policy-version` selects an existing policy revision;
omission deliberately selects initial adoption. A reviewer may recommend but
cannot acquire source-writer authority. `decision writer-fallback TASK` requires
observed task/case/policy versions and uses the owner's single authorized transition.
For a materialized strategy decision, `decision continue-strategy TASK` invokes
the actual atomic source continuation plus ordinary decision outcome. The
original policy must explicitly include `--action continue-strategy`. Supply the
observed decision `--version`, `--case-version`, `--policy-version`, original source
`--execution-version`, immutable `--candidate`, finite `--additional-segments` and
`--expires-at UTC`, plus `--reason`, `--key`, `--decision-key` and
`--continuation-key`. The outer key and ordinary decision key must differ.
`--kind accepted|completed` selects only the successful decision outcome; this
command permits no unrelated task edits. `Checked::Held` rolls back the whole
staged operation. Exact successful replay returns the historical receipt. This
narrow strategy operation does not provide arbitrary source finalization.

### Execution and progress

Record an actual admitted attempt's yield/result/failure without a JSON file:

```sh
agent-mail task execution report TASK --attempt ATTEMPT --fence FENCE \
  --dispatch-key DISPATCH_KEY --key REPORT_KEY --kind yield \
  --summary 'Partial output retained; continuation requested' --evidence artifact/partial
```

Substitute the complete original correlation. The authenticated runtime owner
returns its actual `Checked` result under `result`; Ready contains the append
record ID and Held contains responsible blocking causes. The CLI infers no latest
attempt and performs no outer current-capability preflight that would reject
historical replay. Report/yield does not close an attempt, release its slot, accept
business work, authorize a wait, or schedule a new segment. Physical capture and
actual predecessor closure remain runtime responsibilities. An attention checkpoint
is a separate intent report, not execution evidence. The native worker has a
separate combined Yield transaction using its private original admission basis;
the raw report command does not imitate it or substitute a latest checkpoint.
The native review interval is bounded to 1..3600 seconds; the 30-second acceptance
fixture is one explicit setting.

`task execution schedule ID --version N --execution-version E --key K
--reason TEXT --check-at UTC` changes only the due time within existing hard limits.
`task execution stop` additionally requires the exact attempt, fence and dispatch
key. Its receipt is stop intent, not proof of quiescence. `resolve-clock` requires
the observed discontinuity generation; no command here resets lifetime budgets.

`task progress policy` takes task/progress versions, a key/reason, `--max-segments`,
optional `--max-elapsed`, repeated `--milestone ID`, `--milestone-criterion ID=CRITERION`
and `--milestone-scope ID=UNIT`. `task progress judge` explicitly qualifies or rejects
an immutable `--report`, or revokes `--revoke-judgment`; it requires the task and
progress versions. A nonwriter also supplies the exact narrow grant and revision.
`task progress grant` records/revokes that original-writer consent.
`task progress history --after CURSOR` pages immutable policy and judgment records.
Judgments do not create source success or reset attempt/cost allowances.

### Managed targets and controlled text artifacts

These commands require the authenticated home-local actor. Configuration uses a
group-scoped name; subsequent commands require the exact returned target ID.
The input examples below require actual provisioned paths, policy references,
observed versions and original admitted correlation. They are not passing native
execution transcripts.

```sh
agent-mail runtime target configure review-target --key configure-1 \
  --reason 'Approved bounded review' --owner worker --client codex \
  --cwd /absolute/approved-input --profile staged-files \
  --artifact-root /absolute/protected-artifacts --configuration approved-policy
agent-mail runtime target show TARGET_ID
agent-mail runtime target disable TARGET_ID --generation GENERATION \
  --revision REVISION --key disable-1 --reason 'Stop new admissions'
agent-mail runtime target retire TARGET_ID --generation GENERATION \
  --revision REVISION --key retire-1 --reason 'All work has genuinely closed'
agent-mail runtime target enable TARGET_ID --generation GENERATION \
  --revision REVISION --key enable-1 --reason 'Request qualified admission' \
  --qualification QUALIFICATION_ID
```

`--client` accepts `codex|claude`; `--profile` accepts `read-only|staged-files`.
The configuration owner must be the authenticated actor. `--configuration` names
an existing protected policy; the command does not create policy, isolation or
qualification evidence. The staged artifact root must already meet the runtime's
protected-storage rules. Configuration creates a disabled target. Correction
requires both observed `--generation` and `--revision`; omit both only for initial
configuration. An exact keyed retry returns its historical receipt; inspect the
target to learn current enabled/revision/retired state.

Disable prevents new admission while retaining unresolved work. Retire requires
actual closure of attempts and effects. For an otherwise valid current target,
enable returns the owner's Held cause `managed_native_qualification_unavailable`.
A supplied qualification ID cannot enable it. There is no qualification collector
or qualification command in this increment. The existing `runtime configure
CLIENT --output PATH` hook generator and `runtime enable NAME` delivery command
have separate purposes.

The genuine task writer binds an exact task revision to its allowed destination:

```sh
agent-mail runtime artifact bind BINDING_ID --key binding-1 \
  --reason 'Approved review output' --task TASK --task-version VERSION \
  --target TARGET_ID --target-generation TARGET_GENERATION \
  --destination DESTINATION --scope 'write approved review' \
  --allowed-path review.txt
agent-mail runtime artifact show BINDING_ID
```

Repeat `--allowed-path` for the complete virtual path set. Correction additionally
requires the observed `--binding-revision`; binding identity and task stay fixed.
The runtime validates current contract, writer authority, target and destination.
A binding does not admit an attempt or establish runtime capability.

The original authenticated producer can publish actual bounded UTF-8 input:

```sh
agent-mail runtime artifact publish TASK --attempt ATTEMPT --fence FENCE \
  --dispatch-key DISPATCH_KEY --effect EFFECT --destination DESTINATION \
  --scope 'write approved review' --generation DESTINATION_GENERATION \
  --task-version VERSION --empty-destination --text-file review.txt=./review.txt
agent-mail runtime artifact receipt TASK --attempt ATTEMPT --fence FENCE \
  --dispatch-key DISPATCH_KEY --effect EFFECT
```

Use `--empty-destination` only when the original expected selection is empty.
Otherwise supply its exact `--previous-manifest DIGEST`. One is mandatory; the
CLI never reads a newer generation, task version, attempt or selection to refresh
a request. Keep the original effect and bytes for retries. Changed retry input
conflicts. `--generation` here is the destination generation, distinct from target
configuration generation. All guards still pass through the owner transaction.

Each repeated `--text-file VIRTUAL_PATH=INPUT_FILE` reads a real regular local file.
The CLI bounds reads before publication: at most 16 files, 4 KiB per file, 12 KiB
of total UTF-8 content, and an 8 KiB manifest. Virtual paths are canonical relative
paths, at most 256 bytes, with no duplicate or file/directory collision. The CLI
computes content digests and validates the manifest and content objects. The owner
alone opens protected artifact storage and commits selection and receipt; a
successful file read does not imply publication.

Configure, disable, retire, enable, bind and publish also accept `--file PATH`
(or `--file -` for stdin), mutually exclusive with mutation flags. The JSON envelope
is bounded to 128 KiB before parsing and must match the positional identity and
selected command. Configure/change/bind use their actual owner request DTOs.
Publish uses `{"request": PublicationRequest, "contents": ManagedArtifactContents}`;
its request must explicitly include `expected_manifest` (null for empty) and a
non-null `expected_task_version`, plus the same group/task identity as the command.
This is an advanced form of the same guarded operation.

Mutations emit schema-1 envelopes: configuration under `receipt`, binding under
`binding`, and lifecycle/publication under `result` with the owner's actual Ready
or Held value. Inspect the result; Held does not mean the requested enablement or
publication succeeded. The owner may still retain blocking causes and accounting.
Target show aliases capability inspection, preserving the owner's nested schema-2
view. Target/binding reads preserve `present:false` versus errors. Receipt reads
require the original producer binding and correlation, even after publication;
an ordinary writer or rebound owner cannot use them as a general review API.
A genuine unattended writer review consumer and its protected retained-content
access remain future work. Publication alone creates no business candidate or
accepted outcome.

### Honest status and publication boundaries

Human status displays policy and follow-up totals even with no registered agents.
JSON retains legacy fields and adds `schema_version` and `followup_policy`.
Notifier or Herdr route configuration is distinct from actual receipt and handling.

`task list --details [--after ID] [--limit 1..50]` reads one authenticated home-task
page visible to the current owner or writer. It includes terminal work and labels
legacy records untracked. `status --tasks` adds the same bounded page to human
status; add `--json` for full model, execution, cost and cause fields. These options
require the actual agent credential and cannot be combined with `--all-groups`.
The page returns `has_more` and `next_cursor`; follow that cursor rather than
assuming omitted work is absent. Model and per-task execution reads have separate
observation times and `atomic_snapshot:false`. Storage/authority/graph errors are
errors, not empty successful pages. Ordinary legacy `task list` remains unchanged.

`runtime capabilities TARGET_ID` uses the exact identity returned by registration,
not an agent alias. It reports absent targets as `present:false`, distinguishes
storage/authentication errors from absence, and returns the runtime owner's twelve
capability dimensions with actual optional witness times. A qualified profile at
observation time still grants no task admission. Delivery Ready, stored enablement
or a configuration value does not establish qualification. Staged native execution,
native session recovery and cost enforcement remain unsupported by the current
runtime implementation. Inspection does not enable, repair, renew or launch a target.

Inspect task/execution details for model holds, due times, inclusive ancestor
budgets and terminal cleanup. Unknown cost remains unknown; do not add parent and
child inclusive totals together. Delivery Ready does not qualify a managed profile.

The accepted future controlled-artifact protocol seals protected immutable bytes
before the SQL transaction. Publication occurs only when the transaction validates
current original phase/scope/action/input and admitted execution/destination guards,
CAS-selects the manifest and commits its receipt. Missing bytes retain history plus
uncertainty. Replay after rebind is historical. Arbitrary mutable exports and atomic
filesystem-pointer-plus-SQL claims are unsupported; unknown effects retain their
execution slot. This paragraph describes the protocol boundary, not a shipped
publication command.
