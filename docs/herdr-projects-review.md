# Herdr Projects: state and communication review

Reviewed 2026-09-28 at commit
[`15bbfa4f12d5cdca123a0cc5b348e70f286a67b5`](https://github.com/eliasstravik/herdr-projects/tree/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5).
This is a source inspection, not a runtime reliability test or a benchmark.

## How it works

### Separate live state from workflow state

Thread records persist `starting`, `open`, `failed`, or `resolved` in TOML.
The UI group is derived from that record, Herdr lifecycle, reports, PR facts,
and progress self-reports: Working, Waiting on you, Ready for review, Landing,
Idle, or Resolved. One precedence function owns that derivation. In particular,
a working harness takes precedence over a new report for review readiness;
an idle harness alone does not prove task completion. See
[`thread.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/thread.rs#L565).

Progress reports contain socket, pane, terminal and native session identifiers.
Transient activity expires after five minutes. A reported wait remains meaningful
until resumed work or a new user prompt invalidates it; it is not simply an
expiring heartbeat. Hook reminders are throttled to about once a minute and
exclude subagents. See
[`progress.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/progress.rs#L25).

### Poll, compare, persist changes

A singleton ticker reconciles state every 15 seconds, with a faster two-second
check while initial briefs await delivery. It caches session listings within a
tick and performs cheaper state checks before slower copies and launches. It
persists previous observations and emits inbox items for selected transitions.
This path uses periodic Herdr reads, not a durable socket pub/sub log. See
[`ticker.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/ticker.rs#L229).

### Reports upstream; prompts downstream

Workers write a report containing results, proposed next actions and optional
lessons. The coordinator reads reports and sends follow-up prompts. The worker
brief explicitly excludes direct worker-to-worker communication. It is a
coordinator workflow rather than a general peer mailbox. See
[`THREAD.md`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/skill/THREAD.md).

Inbox items are Markdown files with TOML metadata, allocated under a project
lock and written through an atomic-write helper. There are two distinct markers:

- **Seen:** `context` showed the item; its ID enters a persisted seen set.
- **Handled:** `inbox done` moves the file into `inbox/done/`.

Reading is not handling. See
[`inbox.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/inbox.rs#L121).

### Carefully gate nudges

The coordinator must remain idle/done with an unchanged state sequence for at
least 60 seconds. Its input box must be observed empty across at least ten
seconds; a draft or an unrecognized screen postpones the nudge. These are
observational safeguards, not an atomic guarantee against a last-moment keystroke.
See [`coordinator.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/coordinator.rs#L142)
and [`steps.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/steps.rs#L313).

Nudges coalesce events for at most five named subjects. They use fixed event
phrases instead of arbitrary report or GitHub text. The ticker persists a hash
of the unseen IDs announced. There is no timed re-nudge for the same set.
Consequently, a seen-but-unhandled item remains in the inbox but no longer
drives this automatic wake-up path. It will still appear in later context reads.
This is a deliberate low-noise policy, not a claim that the item was lost.

Initial brief delivery has a separate confirmation mechanism: Herdr is asked to
observe `working` or `blocked` after submission. An ambiguous response is not
blindly treated as proof that nothing was typed. See
[`herdr.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/herdr.rs#L451).

### Context cost

Polling and screen checks are ordinary code and consume no model tokens by
themselves. Waking an agent and supplying context do consume tokens. The context
digest includes the memory index, tasks, all open threads, and all unhandled
inbox summaries; that path has no fixed total response budget. Its cost can grow
with project state. No measured token comparison was performed. See
[`coordinator.rs`](https://github.com/eliasstravik/herdr-projects/blob/15bbfa4f12d5cdca123a0cc5b348e70f286a67b5/src/coordinator.rs#L524).

## Decisions for Agent Mail

Keep a small coordination store: SQLite inboxes, versioned work records,
explicit recipients, `send`, `inbox`, `resolve`, a bounded `context` view, and a
supervised reconciliation loop using Herdr's socket.
The following rules matter for the Fleet Campaign use case:

1. A lifecycle transition, successful prompt request, or inbox read never
   resolves a delivery. Keep pending work, bounded reminders and overdue status
   until explicit handling. No separate persisted `seen` state is needed in v1.
2. Bind to the actual agent incarnation. Reused pane IDs and names are
   insufficient. Agent Mail already requires terminal and native session identity.
3. Coalesce wake-ups and fetch bounded summaries; load a body only by request.
   Avoid repeatedly injecting the campaign register and full inbox into context.
4. An idle/interactive flag does not establish that an input box is empty.
   The draft wake-up implementation currently checks those flags only. Safe
   automatic prompting needs a release gate for drafts and unknown screens;
   do not claim the current implementation satisfies it. Keep uncertain delivery
   visible without inserting text. Prefer a host-provided input guard when
   available; any screen adapter must be narrow, tested and fail closed.
5. Persist the operational work register beside messages so context resets can
   recover it through one command. The plugin stores generic records; Fleet
   defines their workflow meaning. PR monitoring, worktree lifecycle, progress
   estimation, shared memory and task scheduling remain outside Mail.

## Fleet Campaign integration boundary

| Fact | Authority |
| --- | --- |
| Agent lifecycle and runtime identity | Herdr |
| Message pending, resolved or withdrawn | Agent Mail |
| Persisted lane ownership, accepted revision, evidence pointers and next action | Work ledger in the coordination plugin; Fleet coordinator is its writer |
| Allowed lane transitions, gate interpretation and rulings | Fleet Campaign protocol and coordinator |

A useful Fleet handoff contains the lane, expected revision or artifact identity,
the decision requested, and evidence paths. Use a stable send key for retries.
The recipient validates the current register and evidence, records its disposition,
then resolves the delivery; use an atomic reply when an answer is required.
Resolve means the communication was handled, not that the lane passed review.

An obsolete request can remain durably pending. The recipient must check its
revision and authority before acting, and the sender can withdraw superseded
requests. Mail must not infer validity from age or turn overdue messages into
permission to act.

Mail makes unhandled communication recoverable and visible. It cannot guarantee
that an agent makes progress, detect every silent lane that never publishes a
handoff, or repair a workflow register. Fleet's checkpoint and restart-recovery
rules remain necessary. Bounded reminders deliberately end in a visible need for
attention rather than an unlimited model loop.
