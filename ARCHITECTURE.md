# Agent Mail architecture

Status: implementation in progress, 2026-09-28. Local mail and work records,
the Herdr plugin manifest, and an SSH relay with explicit per-peer automatic
sync opt-in are implemented. Release validation remains open.

## Purpose

Agent Mail is a small, local-first coordination tool for coding agents. It keeps
messages and the current work position outside model context, so an agent can
restart, reconnect, or compact its context and still find what it owns and what
needs an answer. It is usable without Fleet Campaign. Agent-scoped CLI commands
currently require a Herdr binding; operator and database commands do not.

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
| Herdr | Agent inventory, processes, sessions, machine connections, live state, and prompts |
| Mail | Mailbox addresses and routing bindings, delivery, request resolution, reminders, a small work register, and recovery views |
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

remote host: the same binary, its own mail.db, and its own Herdr server
```

One Rust executable supplies the CLI, local worker, and SSH stdio bridge. The
CLI reads and writes SQLite directly; message operations still work when the
worker is stopped. The worker is supervised by the host OS (launchd on macOS;
an equivalent user service or foreground process on Linux). Its only local
socket dependency is Herdr's existing socket for live state and short prompts.
The remote bridge uses SSH stdio; it does not open a TCP port.

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
are machine-scoped and cannot serve as global Mail IDs. Herdr remains the
source for which agents actually exist and what they are doing; Mail does not
maintain a second agent inventory.

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

## Agent binding and trust

A Mail address is a logical inbox, not another Herdr agent record. Its binding
records machine, server/session, pane, terminal, and native agent incarnation
when available.
Before an automatic prompt, the worker checks fresh Herdr state and verifies
that the intended agent is still there. An agent CLI call also checks its
current binding. Missing or ambiguous identity holds prompting and appears in
status; rebinding is explicit. A last-moment pane replacement can still receive
the generic wake hint because Herdr's state read and prompt are separate
operations. The hint carries no message body or work data.

Herdr plugins run as local processes. Agents sharing one OS account can read
or alter that account's files, so Mail's actor checks are workflow guards, not
a security boundary against malicious same-user code. SSH authenticates the
machine connection; it does not turn a model's claim about its role into proof.
Coordinator-only acceptance is enforced by the workflow and the designated
home writer. Stronger adversarial isolation would require separate OS
principals and is outside v1.

Herdr does not install local plugins on remote machines. Each participating
host must install Mail explicitly. Herdr is currently required for agent-scoped
send, inbox, resolve, context, and work commands.

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
agent-mail setup                 initialize local state and worker
agent-mail bind                  attach a logical inbox to this agent
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
