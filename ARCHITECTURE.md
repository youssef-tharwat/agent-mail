# Agent Mail architecture

Status: implementation in progress, 2026-09-29. Local mail and work records,
standalone participant registration,
the Herdr plugin manifest, and an SSH relay with explicit per-peer automatic
sync opt-in are implemented. Release validation remains open.

## Purpose

Agent Mail is a small, local-first coordination tool for coding agents. It keeps
messages and the current work position outside model context, so an agent can
restart, reconnect, or compact its context and still find what it owns and what
needs an answer. It is usable without Fleet Campaign. Agent-scoped CLI commands
accept either a verified Herdr binding or an explicitly registered standalone
session credential. Herdr remains an optional runtime integration.

Install one binary on each participating machine. There is no account, hosted
service, shared filesystem, Dolt database, or public network listener. A
single-machine setup needs only SQLite on local disk. For several machines,
existing SSH access carries bounded, retryable exchanges between local nodes.

The promise is durable state and visible outstanding obligations. Mail cannot
make an unavailable agent respond, decide whether submitted code is correct, or
execute a model's tools exactly once.

## Boundaries and responsibility

| Component | Owns |
| --- | --- |
| Runtime (Herdr integration today) | Live agent inventory, processes, sessions, machine connections, lifecycle observations, and wake prompts |
| Mail | Durable participant identities, runtime/routing bindings, delivery, request resolution, reminders, a small work register, and recovery views |
| Workflow using Mail | Meaning of work states, review criteria, who may accept or reassign work |
| Git and CI | Code revisions, artifacts, test results, and other underlying evidence |

Mail records decisions and evidence references. It never infers acceptance from
a reply, a green CI run, an idle pane, or a closed terminal. Fleet Campaign may
use Mail's work register, but its specific lane policy stays in Fleet.

## Shape

```text
agent CLI ── short SQLite transaction ── local mail.db
                                         ↑
                          local background worker
                          ├─ checks pending work
                          ├─ prompts via Herdr's local socket
                          └─ syncs SSH peers with operator opt-in

operator CLI ── explicit sync ── SSH peers

remote host: the same binary, its own mail.db, and optional runtime integration
```

One Rust executable supplies the CLI, local worker, and SSH stdio bridge. The
CLI reads and writes SQLite directly; message operations still work when the
worker is stopped. The worker is supervised by the host OS (launchd on macOS;
an equivalent user service or foreground process on Linux). Its only local
socket dependency is Herdr's existing socket for live state and short prompts.
The remote bridge uses SSH stdio; it does not open a TCP port.

Herdr bindings use its existing local socket. Standalone local operations need
no socket or running background worker. The participant listing describes
registrations; live availability is unknown until a runtime supplies evidence.

The local core uses SQLx for checked SQLite transactions and Tokio for the
worker's bounded I/O and timers. The three-reminder loop does not need a job
framework. SSH transport does not require NATS or another broker. These are
implementation choices, not a second execution engine.

This is pub/sub in the small: publishing a message durably creates one pending
delivery per named subscriber. When prompting is enabled, the worker emits a wake hint after commit. A
wake hint is never the message or its receipt. V1 has named recipients and
bounded fan-out, with no topic expressions or global broadcast.

## Durable model

A **group** is a namespace for mailboxes, messages, and optional work items.
It has a home machine. Mailbox names are unique only inside that group;
the routing identity also includes the machine. Herdr pane IDs and agent names
are machine-scoped and cannot serve as global Mail IDs. Herdr supplies live facts for its bound agents. Mail keeps the durable
participant registry but does not infer live processes or lifecycle from it.

A **message** has a stable ID, sender, recipient list, idempotency key, short
summary, bounded body, creation time, optional deadline, and optional work-item
reference. It is immutable after publication. Each recipient has an independent
delivery state: pending, resolved, or withdrawn. Reading does not resolve it.
The recipient resolves a request explicitly, optionally publishing a reply in
the same transaction. The sender may withdraw a superseded request; correction
is a new message referencing the old one. Ordinary progress belongs on the
work item and need not create mail.

A **work item** is a small current-state record: stable ID, scope, owner, state,
next action, optional deadline, accepted revision if any, evidence references,
linked message IDs, update time, and version. A group can have a root record
for its objective and acceptance contract. One designated writer at the home
node maintains each item. Updates require the expected version and a reason,
and keep a short change history.
Remote participants submit results or corrections through Mail; a reply does
not mutate the authoritative work item. The workflow decides whether to accept
a revision, reopen work, or reassign an owner.

An item may reference messages, and a message may reference an item. That link
keeps a request and its work context discoverable without turning every comment
into a task or every task into a message. The register is optional for groups
that need only mail.

SQLite is the source of truth on each machine. Use local disk, WAL,
`synchronous=FULL`, foreign keys, short transactions, versioned migrations,
and a bounded busy timeout. A successful local send means the local commit
succeeded. A failed commit never returns success. Missing or incompatible state
fails visibly instead of silently creating a fresh database. The implementation
uses compile-time checked SQLx queries and no dynamic SQL.

## Forgetful-agent contract (required next increment)

Status: v0.3 source implements transactional event subscriptions, generation-scoped
receipts, atomic writer decisions, bounded client hook output, and work-event
wake reconciliation. v0.2.0 lacks these features. Trusted Codex 0.157 prompt/resume recovery and the post-compaction fallback
have been exercised; the local Codex queue adapter supplies idle wake; broader client acceptance remains scoped below. Checkpoint polling is
an incomplete fallback, not the target reliability contract.

Fixed subscriptions are derived from recipients and work ownership. SQLite
triggers persist events in the mutation transaction, including relay imports;
no process-local callback can omit a committed change. The hook adapter tracks
emission attempts rather than claiming confirmed delivery. A reset restores
current obligations even after the previous emission budget was exhausted.
Programmatic subscribers acknowledge individual events only after confirmed
transport delivery. Receipt state is scoped to the binding generation.

Agents must not remember coordination bookkeeping. A domain operation commits
its state change and an event together in SQLite. Relevant subscriptions create
durable pending notifications for affected participants (owner and designated
writer, scoped by group and work item). Changing an assignment must notify both
previous and new owners. Retrying an operation or replaying an event must not
repeat its logical effect. Notification delivery is at least once, with stable
IDs and idempotent consumers; it is not exactly-once agent execution.

A small runtime adapter consumes those notifications and supplies bounded
context at supported session start, resume, post-compaction, and safe turn
boundaries. An idle agent needs a verified safe wake/queue mechanism; hooks alone
cannot awaken an agent when no client event occurs. No model call is used to
poll. The local worker reconciles committed events after restart, so an in-memory
socket hint is only a latency optimization, never the source of truth.

Delivery, presentation, and business resolution are different facts. A cursor
or successful context injection never resolves a request or accepts work.
Session recovery reconstructs unresolved obligations from current durable state,
even when an earlier session already received their notifications. Per-binding
notification acknowledgments cannot hide obligations from a replacement session.
Only acknowledge a delivery after the adapter confirms it; ambiguous outcomes
may repeat a bounded hint. Coalesce notifications and suppress unchanged context
within a session, but restore it after a reset. Failed adapters remain visibly
pending, with bounded retries and a visible stalled state, never silent loss.

The model or authorized operator still supplies semantic decisions: submit a
result, request changes, accept a revision, or reassign work. Provide one typed,
idempotent operation for each supported decision, so recording its evidence,
updating permitted state, resolving the associated obligation, and notifying
subscribers happen in one transaction. The active workflow supplies authority
and transition policy; Mail must not infer success from free-form text, tool exit,
an idle pane, or a stop hook. A decision not submitted remains visibly pending.

Before a session ends, a supported hook surfaces outstanding actionable
obligations. It must not force an endless continuation for blocked work, human
approval, or another participant's response. Clients without the necessary hooks
or safe wake mechanism expose a manual delivery mode explicitly; they cannot be
advertised as satisfying automatic progress or recovery guarantees.

Keep this local: one binary, SQLite events and subscription receipts, a bounded
worker, and small client adapters. No external broker or workflow language.

## Delivery and reconnect

For a group spanning machines, **the home node is the only authority** for the
mailbox routing directory and work register. Each remote node has a durable local
outbox and inbox. The home connects to opted-in peers periodically over SSH, or
to any configured peer on an explicit `sync` command, and exchanges
small batches in both directions. The bridge protocol uses stable origin
machine/message IDs and acknowledges a transfer only after the receiving
SQLite transaction commits. Replayed batches are deduplicated. A lost SSH
response may cause a resend, never a second logical message.

The home relays messages between remote nodes. A remote node may also keep a
read-only snapshot of work assigned to its local agents. Every snapshot carries
its last transfer time. A remote reply is a proposal or evidence, not
accepted work; the designated home writer applies any change with a version
check. There is no multi-writer merge for the register.

If SSH or the home is unavailable, local sends remain queued and local inboxes
remain readable. Other machines do not see those sends until sync succeeds.
An offline machine's last known state is labeled stale. Mail never silently
retargets a recipient or treats an unreachable host as an empty inbox. It does
not promise immediate cross-machine delivery or progress while the home is
offline. The operator can see unsynced count, oldest age, and last error.

## Participant identity, runtime bindings, and trust

A participant is a stable mailbox ID and group-scoped name. Its binding is one
of three typed forms: a Herdr session, a standalone session credential, or a
remote machine route. The binding is separate from work ownership and the
identity of messages already sent. A generated UUID credential identifies each
standalone registration; a mailbox name alone does not authorize a caller.
Credentials are returned only at registration and are excluded from participant
listings. The operator or launcher passes one participant's credential to its
agent through `AGENT_MAIL_SESSION` or `--session`.

Herdr remains the default when no standalone credential is supplied. Its adapter
checks the current socket, pane, terminal, and native agent session. An explicit
standalone credential is checked against that store and group, even inside
Herdr; failure never falls back to another identity. Neither path silently
registers a caller. Standalone availability stays unknown: registration is not
evidence of a running process, and there is no model heartbeat or new supervisor.

Replacing a session requires `register --replace` or `bind --replace`. This
preserves the mailbox ID and work ownership while advancing the binding version.
Reads and writes verify the version inside their database transaction; writes
serialize against replacement. An old actor snapshot cannot continue after a
replacement, even if a previous Herdr identity is later restored. A read already in progress may finish from its pre-replacement snapshot;
committed operations remain valid. Remote routes cannot be taken over by a
local registration. Existing schema-5 addresses migrate with their IDs, bindings,
messages, work records, and reminder budgets intact.

Standalone participants without a configured adapter remain pending with unknown
availability. An explicitly attached Codex endpoint binds a local Unix socket and
persistent thread UUID to the mailbox generation. The existing worker delivers
bounded recovery snapshots through Codex's queue when the thread is idle. A
durable cursor and bounded retry budget survive restarts; a successful queue
receipt acknowledges transport only. Binding replacement invalidates the
endpoint. The send holds the binding write lock within a five-second operation
timeout, excluding detach/rebind during delivery. Codex queue IDs are not
idempotency keys: uncertain delivery can duplicate a wake. Mail never starts,
resumes, or supervises a Codex runtime. Herdr participants retain fresh identity checks,
idle-state checks, and the existing bounded wake policy. A last-moment pane
replacement can still receive a generic wake hint because Herdr's state read
and prompt are separate operations; the hint contains no message or work body.

These checks guard against accidental identity mistakes among processes sharing
an OS account. They are not adversarial isolation: same-user processes can read
the database and registration credentials. SSH authenticates machine connections.
The designated home writer and active workflow still govern work acceptance.

Each participating host installs Mail explicitly. Herdr plugin installation and
its existing setup/status actions remain supported. An installation can contain
Herdr and standalone participants in the same group.

## Quiet liveness and compact context

The worker periodically reconciles durable pending rows with live Herdr state.
It batches wake-ups per participant, sends only a fixed prompt to check Mail,
and never inserts sender prose, evidence, or message bodies into a prompt.
Automatic prompts are disabled by default because Herdr does not expose a
reliable empty-draft check. If an operator enables unguarded prompts, the
worker verifies an idle, interactive agent. Otherwise the request stays pending
and the reason is visible. A working or blocked agent
is left to check at its next checkpoint. No model call is spent on polling.

One initial wake-up and at most two reminders share a mailbox budget, spaced
at least five minutes apart by default. After the budget or deadline, the
operator gets one alert; the unresolved item remains visible. Budgets and alert
state survive restarts, new arrivals do not reset them, and sleep does not
replay missed ticks in a burst. Mail reports stalled or overdue work; it never
automatically reassigns a lane or accepts a result.

`context` is the restart view. It returns the caller's group, owned open work,
next actions, unresolved inbox summaries, and stale-peer warnings within a
fixed byte and row budget. It reports a continuation cursor when truncated.
`inbox <id>` and `work show <id>` fetch detail on demand. Message bodies have
a size cap; large evidence stays in Git, CI, or artifacts referenced by ID or
path. Prompts contain only a short instruction and count or IDs. This bounds
automatic context use without hiding outstanding work.

## Minimal interface

```text
agent-mail setup                 initialize state; optionally configure Herdr
agent-mail register              create or explicitly replace a standalone binding
agent-mail participants          list addresses and bindings; no credentials
agent-mail bind                  attach an inbox to a verified Herdr session
agent-mail send                  publish to named recipients, with a retry key
agent-mail inbox [id]            list summaries or fetch one message
agent-mail resolve <id>          close a delivery; optionally reply atomically
agent-mail context               compact resume view
agent-mail work show/update      inspect or version-update the small register
agent-mail status                pending, overdue, binding, worker and sync health
```

The Herdr plugin adds setup/status actions and a startup hook that reconnects
the existing worker after restore. It does not enroll agents merely by being
linked. V1 includes no dashboard, workflow scheduler, public API, broker,
model heartbeat, automatic reassignment, general dependency graph,
deterministic replay of agent reasoning, or automatic retry of agent tool
calls. A workflow that needs those execution guarantees needs a workflow
engine rather than an expanded Mail worker.

## Required proof before release

The project must demonstrate: restart and crash recovery; idempotent send and
resolve; bounded context and reminders; safe prompt behavior around drafts and
pane replacement; and one-machine operation while the worker is down. The
remote path must demonstrate two-way delivery, duplicate or lost SSH transfer
acknowledgments, disconnect and reconnect, repeated pane IDs on two machines,
and visibly stale snapshots. These are release gates, not current claims.

The Rust implementation now includes local mail and work records, bounded
context, a plugin manifest, a macOS worker, and an explicit SSH bridge. Linux
uses a foreground worker under an external supervisor. The safe empty-draft
prompt gate is unavailable, so prompting remains off by default. Automatic
SSH sync is off until an operator opts in for a configured peer; changing that
peer's target revokes the opt-in. The protocol has disposable two- and
three-node tests, including a process-boundary SSH command test. A real local
Herdr smoke test passed; a two-machine SSH smoke test and
release checks are still required. It should not be published as a complete
solution until those paths pass.

## References and design lessons

- [Herdr plugins](https://herdr.dev/docs/plugins/) and
  [connecting machines](https://herdr.dev/docs/connecting-machines/) define the
  host boundaries and machine-scoped identities.
- [Beads agent coordination](https://github.com/gastownhall/beads/blob/main/docs/multi-agent/coordination.md)
  demonstrates durable work items, ready-work queries, and atomic claims. Mail
  borrows the small-state and recovery ideas without its Dolt work graph.
- [aweb mail for Beads](https://aweb.ai/docs/beads-mail/) demonstrates that
  cross-machine agent mail already exists. Mail's intended difference is a
  zero-account local installation with optional SSH, a compact shared register,
  and explicit unresolved-work supervision. This is a product hypothesis to
  validate, not a claim of unique underlying technology.
