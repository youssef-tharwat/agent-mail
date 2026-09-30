# Durable follow-through

Status: implemented in schema 18. Automated validation and remaining rollout work
are recorded in [FOLLOW_THROUGH_ACCEPTANCE.md](FOLLOW_THROUGH_ACCEPTANCE.md).

## Outcome

A reviewer can submit a result after the coordinator ends its turn. Agent Mail
wakes the coordinator, exposes the current request and task, and keeps the decision
visible until the coordinator records an outcome or an explicit waiting condition.
This works across worker restarts without a user message to restart coordination.

The invariant is: every pending obligation has a responsible participant and a
scheduled next check, an explicit waiting condition, or a visible escalation.
The service owns remembering when to revisit it. Delivery, retrieval, reported
progress, and business completion remain separate evidence.

## Current foundation and observed gaps

Inspection baseline: HEAD `378f58b`, with existing uncommitted automatic-upgrade
work in the checkout. Implementation integrates with that work; the
graph describes committed code, so verify affected working-tree files directly.

| Existing behavior | Implementation to reuse |
|---|---|
| Durable requests and explicit reply/resolve | `src/store.rs`, `deliveries` and `messages` |
| Writer-authorized, versioned task decisions and atomic linked-mail resolution | `src/work.rs` |
| Transactional events, retrieval receipts, binding-scoped delivery budgets | `src/events.rs`, migrations 0007–0017 |
| Worker singleton, event wakeups, five-second recovery scan | `src/service.rs`, `src/stream.rs`, `src/supervision.rs` |
| Native and Herdr wake adapters | `src/native.rs`, `src/claude_inbox.rs`, `src/herdr.rs` |
| Bounded recovery, watch cursors, request waits | `src/recovery.rs`, `src/watch.rs`, `src/hooks.rs` |
| Delivery diagnostics and deadline attention | `src/doctor.rs`, `src/status.rs`, `src/attention.rs` |

The live campaign inspection found two different failures:

- The coordinator had six pending messages and zero wake attempts. Herdr reported
  `done`, while the delivery worker's latest scan reported `busy`. Inspection of the
  live payload found omitted `interactive_ready` metadata on manually started clients;
  the previous decoder defaulted that to false. The revised eligibility check accepts
  idle/done without launch metadata, retains explicit false and launch-pending holds,
  and reports the exact ineligibility reason. Regression fixtures cover both cases.
- The learning lane acknowledged a delivery challenge and ignored three batches
  naming the same pending mail and task revision. Herdr's compact notification formatter emits
  JSON without the retrieval instruction used by the longer native format.

Retrieval already stops transport retries without resolving mail. Follow-through
after retrieval needs its own schedule. Current attention reports depend largely
on business deadlines and delivery budgets; neither describes a decision that was
read and then abandoned without a deadline.

## Design decisions

### 1. Preserve domain authority

Mail deliveries and tasks remain the business records. An attention plan is metadata
attached to one of those records, not another assignment or completion lifecycle.
It cannot accept work, resolve mail, change an owner, or authorize external actions.

- A mail recipient may record a checkpoint for their own pending delivery.
- A task's current owner or writer may record a checkpoint for the observed task
  revision. An owner's report does not change writer-controlled task fields.
- Only the writer can change task state, ownership, scope, acceptance, or business
  deadline. Existing reply/resolve and task-update authorization stays authoritative.
- Task revision changes invalidate the previous owner's checkpoint. Reconcile the
  new revision; do not carry an old waiting condition onto a changed assignment.
- Closing a task does not implicitly resolve linked mail. An explicit task update
  with linked resolution still commits both changes atomically.

### 2. Add small, versioned attention metadata

Introduce `src/followup.rs` and the next available migration, currently 0018.
Keep storage normalized and scoped to a group and a concrete source record:

- Source: task ID plus observed task revision, or message plus recipient delivery.
  Use foreign keys and a constraint requiring exactly one source type.
- Checkpoint: author, idempotency key, metadata version, reported next step,
  optional evidence references, and creation time.
- Schedule: `next_check_at`, optional typed waiting condition, and an escalation
  boundary. A checkpoint is either active or waiting; completion comes from the
  source record. These values describe an agent's report, not verified progress.
- Operational history: checkpoint changes, due occurrences, reminder reservations,
  and escalation routing/results. Keep attempts and receipts separate from the
  canonical checkpoint so restart cannot reset a budget.

Use a current-plan row plus an append-only history, with uniqueness for the source,
checkpoint version, and due occurrence. Default plans are created transactionally
with new obligations. Recovery scans repair missing derived scheduling entries
from source records and canonical plans; they preserve attempt and escalation history.

Commands proposed for this release:

- `task checkpoint ID --version TASK_VERSION --key KEY --file PATH`
- `mail checkpoint ID --key KEY --file PATH`

The input includes the observed metadata version, a concrete next step, the next
check time, and an optional waiting condition. New plans start at metadata version
zero. Identical retries return the same result; changed retries or concurrent
updates conflict. Validate the current registration, source state, owner, task
revision, and metadata version in one transaction. Return refreshed task/mail
details on a stale-plan error so the agent can reconsider.

Checkpoints appear in existing `task show`, `mail show`, context, and history
responses. Ordinary final outcomes continue to use reply, resolve, and task update.
Only require a checkpoint when an agent yields with unfinished work or a waiting
condition; do not add per-tool heartbeats or repeated bookkeeping for settled work.
Add a recipient-scoped `attention show ID` for fetching a due occurrence and its
source references. Administrative status reads never acknowledge another agent's
retrieval or handle an occurrence on their behalf.

### 3. Make waiting explicit and bounded

Support these waiting conditions initially:

| Condition | Reactivation |
|---|---|
| Time | Next check becomes due |
| Task dependency | A specified task reaches one of the explicitly selected states |
| Reply/decision | A specified outgoing request receives a reply or is settled |
| Human approval or external blocker | Explicit update, with a responsible person/role and review time |

Validate accessible same-group references. Reject self-dependencies and dependency
cycles. Evaluate the condition's current value when saving it, then subscribe and
recheck transactionally so a concurrent change cannot be missed. A condition being
satisfied wakes the agent to reassess; it never grants permission to implement,
deploy, accept, or resume a task whose writer still records a hold.

Owners can send a result with existing Mail commands, then checkpoint the task as
waiting on that outgoing request. Fetch replies when it settles and inspect the
current task again. Do not infer this relationship from message prose or merely
from a shared task ID. In a later convenience operation, sending the result and
recording the checkpoint can be atomic; correctness cannot depend on that shortcut.

Waiting always includes a review time. An unchanged hold causes attention for the
writer or operator at that time, not repeated instructions for the worker to resume.
Business deadlines remain separate from these scheduling times.

Initial, configurable policy defaults for newly enabled groups:

- Unplanned retrieved work: first follow-up after 15 minutes.
- One further follow-up after another 15 minutes if no disposition/checkpoint exists.
- Escalation at 45 minutes if those reminders remain unhandled, with a maximum
  unattended interval of 60 minutes if the runtime stays busy or unavailable and
  the reminders cannot be delivered.
- Explicit waits use their recorded review time. Human holds remain in force after
  that time; expiry requests reassessment from the responsible authority.

These are initial operating defaults to measure during acceptance, not universal
response-time promises. Preserve the existing transport retry policy separately.
If the original notification was never retrieved and its transport budget is
exhausted, escalate that failure immediately; do not create a follow-up event to
obtain three more delivery attempts. The 15/30/45-minute sequence applies to
retrieved work that lacks a disposition or valid checkpoint.
Reading, delivery acknowledgments, runtime activity, and repeat checkpoints with
identical content do not reset the escalation boundary. Owner-authored extensions
are capped by group policy; a writer/operator can explicitly extend the boundary
with an audited reason. Store reported progress as reported evidence; software
cannot determine whether an arbitrary prose update is substantively meaningful.
Carry the original outstanding-work age across ordinary task revisions; revising
a task or changing metadata must not silently grant a fresh escalation interval.

### 4. Reconcile through the existing worker

Add `followup::reconcile(store, now)` to the event-driven worker and its recovery
scan. Reuse the existing singleton and shutdown/upgrade behavior.

For each bounded page of due sources:

1. Re-read the authoritative source and checkpoint. Supersede obsolete occurrences
   if the source was settled, reassigned, or revised.
2. Evaluate the waiting condition and time. Keep an unsatisfied hold visible.
3. Determine whether the next action is a reminder, dependency recheck, or escalation.
4. In one transaction, reserve the occurrence and publish its durable attention event.
5. Let the existing adapter deliver it under the current identity and prompt policy.

Introduce an `attention_due` event with a distinct occurrence identity and revision;
do not fabricate a new task revision or ordinary mail request to trigger a wake.
Update the event constraints, wake views, serializers, `Changes`, stream protocol,
and recovery paths together. Notifications gain a bounded `followups` collection.
Old stream clients must either negotiate a supported version or fail clearly and
reconnect with an upgraded client; they must not silently lose the new event kind.

Fetching an occurrence records retrieval of that exact occurrence. It stops that
occurrence's transport retries, but the underlying follow-up remains outstanding
until a valid checkpoint or domain outcome is recorded. The next scheduled reminder
is a bounded policy step, not a fresh unlimited retry budget.

Add explicit record-retrieval observations for every runtime. The current Herdr
event receipt is not a universal retrieval signal: native queue acceptance and
Claude hook receipt also need to remain separate from authenticated record reads.
Only records and occurrence revisions actually returned in a response are marked
retrieved. Hidden pages, later revisions, and operator inspection remain unreceived.

Reserve before I/O; dispatch only still-current occurrences. Recheck the source and
binding immediately before sending. Delivery can still race with resolution, so the
recipient must fetch current state and safely ignore superseded work. A crash after
reservation leaves a recoverable attempt with a timeout; never strand it as permanently
in flight. The contract permits duplicate delivery and requires idempotent handling.
It does not retry external work such as deployments merely because a reply was lost.

Paginate scans fairly across groups and participants. Coalesce notifications for one
recipient, preserve cooldowns, and persist occurrence budgets. A missing event hint,
process restart, clock jump, or truncated status page must not make a due source
disappear. Persist UTC due times; use monotonic time only for process-local sleeps.

### 5. Separate delivery eligibility and follow-through health

Replace ambiguous Herdr `busy` results with the specific reason: active turn,
approval UI, launch pending, missing readiness capability, identity mismatch, or
unavailable endpoint. Unknown capability data must be diagnosed explicitly.
Use the same eligibility result in dispatch and diagnostics. Preserve the existing
operator-selected Herdr prompt policy and client interaction safeguards.

Every notification includes an action instruction: fetch current records, act within
the assignment, or record a checkpoint/blocker. Reserve space for the instruction
before packing IDs. If the transport budget cannot fit both the action and the
delivery challenge, use an explicit bounded recovery command or a separate probe;
never silently drop the handling instruction. Show where to fetch omitted pages.

Status should expose these independently, with timestamps and source IDs:

- Transport and current eligibility.
- Retrieved records/occurrences, without claiming comprehension.
- Outstanding next action and responsible participant.
- Waiting condition and next review time.
- Overdue/escalated action and escalation delivery status.

### 6. Route escalation independently

Escalate a task to its writer, and a mail request to its sender. Coalesce when these
are the same participant. If that recipient is also the stalled agent, unavailable,
or already waiting on the same escalation, go to the operator surface.

Persist one escalation per source/checkpoint generation. Use a distinct system
attention event, not a new business request that recursively creates more overdue
requests. A checkpoint may record that the escalation was handled, but merely reading
the alert does not erase the outstanding obligation.

Use existing Herdr operator notifications where available, plus durable CLI status
and service diagnostics. For standalone use, expose an explicit operator notifier
configuration and report when no push route exists. Do not claim that a terminal
status entry actively alerted a human. A stopped service cannot dispatch alerts;
supervision must restart it, and stale scan timestamps must be visible externally.

## Implementation sequence

Each step includes its focused validation before proceeding.

| Step | Work and primary files | Exit criterion |
|---|---|---|
| 1. Repair wake eligibility | Capture actual Herdr `agent.list`/`agent.get` payloads and worker/binary identity. Trace `src/herdr.rs`, `src/service.rs`, `src/verification.rs`, `src/doctor.rs`. | The coordinator mismatch is explained and a regression reproduces its cause. Eligible idle/done clients wake; blocked or unready clients give a precise reason. |
| 2. Make wakes actionable | `src/events.rs`, `src/watch.rs`, `src/hooks.rs`, adapter format tests, bundled guide. | Herdr and native wakes retain explicit retrieval/handling instructions at payload limits. A challenge acknowledgment alone leaves handling outstanding. |
| 3. Persist checkpoints | New migration and `src/followup.rs`; `build.rs`, `src/store.rs`, `src/work.rs`, `src/cli.rs`, `src/main.rs`, `src/lib.rs`, `src/states.rs`, `src/recovery.rs`. | Ownership, versioning, exact retries, holds, history, and atomic settlement survive restart. Owners can report progress without gaining writer authority. |
| 4. Schedule reconciliation | `src/followup.rs`, `src/service.rs`, event/wake views, `src/events.rs`, `src/stream.rs`, native/Herdr adapters. | Events and recovery scans produce the same bounded due actions; no lost or unlimited reminders across crashes. |
| 5. Escalate and explain | `src/attention.rs`, `src/status.rs`, `src/doctor.rs`, operator notification routes and configuration. | A stalled coordinator reaches operator attention independently; status identifies the next responsible actor and distinguishes every stage. |
| 6. Integrate and roll out | Migration/upgrade coverage; protocol compatibility; README, usage, agent guide, acceptance record. | Failure matrix passes, then an isolated real-client handoff completes across a worker restart without user prompting. |

Existing upgrade edits overlap steps 1, 3, 4, and 5. Use their migration lock, backup,
stream shutdown, and worker replacement paths rather than adding another upgrade
mechanism. Migration numbering and source positions must be rechecked at implementation.

## Migration, compatibility, and rollout

- Add the schema to both runtime migrations and `build.rs`'s compile-time schema;
  verify foreign keys and triggers after any event-table rebuild.
- Preserve existing task/mail states, source versions, receipts, idempotency history,
  retry exhaustion, registrations, and pauses. Backfill attention metadata with
  provenance and a grace interval; never mark historical work retrieved or handled.
- Legacy blocked/review records without a structured condition need writer attention.
  Do not invent their dependency or resume implementation because a timer expired.
- Deploy diagnostics first. Enable follow-up dispatch for an isolated group, inspect
  its due items, then enable it for the intended campaign with persisted rate limits.
  Old groups begin in observation mode to avoid a migration-driven prompt burst.
- Initial dispatch scope is locally authoritative tasks and local mail deliveries
  across Herdr, managed Codex, and managed Claude. Keep relay payloads compatible:
  attention metadata is not silently appended to strict `WorkItem` snapshots.
  Remote task snapshots and cross-machine waits report follow-up capability as
  unsupported until a separately versioned relay extension exists; ordinary mail
  and task relay behavior continues. Report this limit in status and release notes.
- Roll back behavior by disabling follow-up dispatch while retaining records and
  diagnostics. Do not run an old incompatible binary against the migrated store or
  restore a backup over newer business writes.

## Verification and acceptance

Use deterministic time and disposable stores. Extend existing integration fixtures
in `tests/reliability.rs`, `tests/local_stream.rs`, `tests/async_watch.rs`,
`tests/cli_domain.rs`, `tests/typed_states.rs`, `tests/migration.rs`,
`tests/automatic_upgrade.rs`, `tests/codex_wake.rs`, `tests/claude_wake.rs`,
`tests/claude_inbox.rs`, and `tests/relay.rs`. Add a focused follow-up integration
suite for the new domain and scheduler contracts.

| Scenario | Required result |
|---|---|
| Coordinator ends its turn before reviewer reply | Reply triggers a new turn and remains pending until an explicit decision/checkpoint. |
| Agent acknowledges a challenge but never fetches | Retrieval remains outstanding; bounded reminders lead to escalation. |
| Agent fetches and then stops | Transport retries stop; scheduled follow-through and escalation remain active. |
| Agent reports an explicit hold | Worker receives no instruction to resume early; dependency changes or review time cause reassessment. |
| Reply arrives before/during wait registration | Current-state check and subscription observe it without a lost wake. |
| Task changes, resolves, cancels, or reassigns during dispatch | Old occurrence becomes harmless; new owner/revision retains its own attention. |
| Concurrent checkpoint updates or lost CLI response | One authoritative metadata version; exact retry does not duplicate the change. |
| Crash before/after reservation, dispatch, receipt, or checkpoint | Recovery preserves the pending source and bounded attempt history. |
| Busy/blocked/unavailable runtime | No unsafe input; time still advances toward visible escalation. |
| Coordinator and worker wait on each other | Cycle is rejected or surfaced to an independent operator route. |
| Repeated identical progress reports | Escalation boundary does not drift indefinitely. |
| Rebind, retire/restore, multiple groups, mail fanout | No cross-identity receipts or schedules; each recipient delivery remains independent. |
| Burst, pagination, service upgrade, clock movement | Fair bounded recovery; omitted records and due times remain discoverable. |
| Old protocol peer or remote snapshot | Explicit capability result; no silent loss and no changed relay authority. |

After focused tests pass, run the repository's CI checks once on the final change:
`cargo fmt --all --check`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`, and
`cargo test --locked --all-features`. CI covers Linux and macOS.

Finally, use a disposable group with real clients: send a review result after the
coordinator finishes, restart the delivery worker at a recorded point, and observe
retrieval plus an authorized decision or checkpoint without manual prompting.
Also exercise an ignored notification and a legitimate approval hold. Keep simulated
protocol results separate from real-client evidence. Record reminder volume, time
to first retrieval, time to disposition, false reminders, and escalation delivery.

The implementation retains observation mode by default. Automated checks use
disposable state; enabling a live campaign and collecting real-client acceptance
evidence are rollout steps, not implied by a passing protocol fixture.
