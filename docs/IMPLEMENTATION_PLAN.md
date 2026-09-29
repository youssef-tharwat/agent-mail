# Implementation plan

Approved scope: 2026-09-29. Status: planned; implementation is not complete.

Build on the tested local Agent Mail core: one Rust binary, SQLite, and the
existing background worker. Add immediate event delivery, principled wake routing,
attention reporting, setup diagnostics, and live-tested Claude support. Evaluate
ACP for the runtime boundary while retaining useful native adapters.

## Baseline and unfinished work

- Committed baseline: `1ae5c4d`; Codex wake implementation: `320b5df`.
- Schema 8, local Codex queue delivery, lifecycle hooks, Herdr integration,
  atomic decisions, and durable events are implemented in source. Published
  binaries remain v0.2.0 at plan creation.
- [Live acceptance](local-codex-acceptance.md): two Codex agents completed the
  workflow in eight turns; four performed substantive actions. Thirty-seven
  automated tests and Linux/macOS CI passed for that implementation.
- Claude hooks have protocol fixtures, but no live Claude acceptance witness.
- Uncommitted schema-9, attention, work-policy, and hook edits were started before
  the design discussion. They are draft work, not the approved implementation.
  Review them against this plan before reuse; in particular, do not adopt their
  proposed manual follow-up field or inferred waiting rules without justification.
- [Implementation history](implementation-history.md) preserves earlier work and
  witnesses. [Architecture](ARCHITECTURE.md) remains the ownership reference;
  the approved additions below supersede its earlier polling-only delivery scope.

## Ownership and invariants

| Component | Responsibility |
| --- | --- |
| Mail core | Durable identities, messages, work records, events, authorization, idempotency, and attention facts |
| Mail worker | Reconcile committed events, stream updates, coalesce changes, and reserve bounded delivery attempts |
| Runtime adapter | Negotiate capabilities, verify the bound session, translate notifications into safe runtime operations, and report receipts/unknowns |
| Herdr, Codex, or the user's ACP client/launcher | Own agent processes, sessions, approvals, and runtime lifecycle |
| Workflow and designated writer | Decide responsibility, review criteria, reassignment, and acceptance |

Keep these facts distinct: persisted operation, streamed event, accepted runtime
input, actionable obligation, and resolved work. None implies the next.

Every meaningful change remains durable, including passive updates. Receiving or
classifying an event must not manufacture a runtime receipt or resolve work.
After reset, recover current obligations regardless of old notification receipts.
No arbitrary workflow-state strings, elapsed time alone, or model prose may imply
completion, waiting, or permission to run tools again.

No broker, hosted service, extra database, workflow language, or agent supervisor.
Keep remote SSH validation outside this local increment. Do not modify real user
stores, hook configuration, or sessions during tests.

## 1. Reconcile the draft and define the contracts

- [ ] Inventory the uncommitted draft; retain only changes justified by this plan.
  Keep unrelated edits intact. Establish a passing baseline before integration.
- [ ] Define small typed event, delivery-result, capability, and attention models.
  Separate event origin and binding generation from workflow ownership: sharing
  a mailbox name does not prove the current session already saw a change.
- [ ] Record adapter capabilities explicitly: safe idle input, session attachment,
  lifecycle recovery, runtime status, and receipt strength. Unsupported or unknown
  capabilities must remain visible.
- [ ] Specify a versioned local stream envelope with event ID, recipient scope,
  binding generation, kind, subject, and revision. Keep bodies out of routine hints.
- [ ] Keep application SQL static and compile-time checked. Use additive migrations
  that preserve existing data and retry-key semantics.

**Exit:** contracts and migration strategy reviewed against restart, replacement,
ambiguous delivery, and compatibility with existing clients.

## 2. Add the local event stream

Primary areas: event/store modules, `src/service.rs`, a small local transport
module, and a CLI subscription entry point such as `agent-mail watch`.

- [ ] Use a private Unix socket owned by the existing worker. Use bounded,
  newline-delimited JSON frames and an explicit protocol version. Authenticate
  and scope subscriptions; never print credentials in frames intended for logs.
- [ ] Commit domain state and its event before emitting a best-effort worker hint.
  Cover local mutations and imported events. CLI success depends on the database
  commit, not worker availability or successful socket delivery.
- [ ] Subscribe from a cursor, replay committed events in order, then follow live
  changes without a gap between replay and subscription. Reconnect from durable
  state; detect binding replacement and terminate the old subscription.
- [ ] Use bounded batches and queues. A slow subscriber must not block commits
  or grow memory without limit; disconnect it with a resumable outcome.
- [ ] Keep reconciliation for a crash between commit and hint, missed hints,
  worker restart, and disconnected adapters. Hints optimize latency only.
- [ ] Keep stream delivery in ordinary code. Only the wake policy may start a
  model turn or inject a bounded context summary.

**Exit:** immediate local delivery before the reconciliation interval in a
controlled test; complete replay after restart; no loss across subscription races;
slow consumers and an absent worker do not impair successful CLI writes.

## 3. Route wakes by actionable obligations

Primary areas: events, delivery worker, adapters, and lifecycle hook handling.

- [ ] Retain all subscriptions and event history. Classify changes separately
  as needing a turn or available on the next recovery view.
- [ ] Suppress an echo only when evidence ties the originating action to the
  current bound session. Do not suppress a replacement session's recovery.
- [ ] Coalesce changes while busy and re-evaluate current state before delivery.
  Preserve new requests, corrections, and responsibility transfers.
- [ ] Handle closure and reassignment as possible cancellation obligations.
  An actively working former owner must learn to stop; an idle participant with
  no remaining action need not get a courtesy model turn. Use verified runtime
  capabilities and lifecycle boundaries; expose uncertainty instead of guessing.
- [ ] Keep passive-event classification separate from transport acknowledgment.
  Prevent Stop hooks from recreating turns that wake routing deliberately avoided.

**Exit:** the same live workflow uses fewer than eight turns without omitting a
required action. Tests cover active-owner cancellation, reassignment, replacement,
reset recovery, duplicate events, and passive updates remaining recoverable.

## 4. Add attention reporting before more reminders

Primary areas: `status`, bounded recovery output where relevant, and worker
observations. Keep structured delivery facts separate from work facts.

- [ ] Report unresolved obligations, explicit expired deadlines, delivery failures,
  exhausted attempts, missing/stale endpoints, and unavailable runtime capabilities.
- [ ] Describe evidence precisely: “deadline passed” or “delivery uncertain,” not
  “agent stuck” merely because a record has not changed. With no deadline, retain
  the open obligation without inventing a timeout.
- [ ] Preserve bounded transport retries. Do not add automatic work retries or
  reminders solely because a notification was accepted without a later decision.
- [ ] Enable any deadline follow-up only where the workflow has explicit current
  responsibility and waiting conditions. If those facts are unavailable, create
  an operator attention item instead. Do not require a separate agent-maintained
  flag just to keep notification bookkeeping correct.
- [ ] Persist any enabled follow-up budget; recheck eligibility before sending.
  Waiting, blocked, resolved, or reassigned work must not trigger repeated turns.

**Exit:** a client accepts input but makes no decision; Mail preserves the open
obligation and surfaces its expired deadline without false completion or an
endless prompt loop. Ambiguous delivery and exhausted budgets remain distinguishable.

## 5. Add ACP integration and first-class Claude support

Primary areas: runtime adapter boundary, Claude hook integration, compatibility
fixtures, and isolated live tests. The Mail event stream and ACP serve distinct
boundaries: durable coordination and runtime interaction respectively.

- [ ] Evaluate the existing [Claude ACP adapter](https://github.com/agentclientprotocol/claude-agent-acp)
  before writing a Claude transport. Pin the tested protocol/adapter versions and
  verify capabilities, identity, prompt acceptance, progress, cancellation,
  disconnect behavior, resume, and normal permission handling.
- [ ] Prove session ownership. Determine whether the adapter can attach to the
  user's existing session or requires an ACP-managed session. Standard local ACP
  commonly uses a client-launched subprocess; do not silently create a second
  agent or claim arbitrary terminal attachment.
- [ ] Implement the smallest supported ACP path through the user's explicitly
  chosen client/launcher. Keep process ownership there. If existing-session
  attachment is unavailable, document ACP-managed sessions as the supported mode
  and preserve the native Herdr path for existing panes where its safety holds apply.
- [ ] Share Mail routing, retry rules, and recovery across adapters. Keep native
  Codex queue integration where it supplies capabilities unavailable through ACP.
  Do not convert tool-output streams into automatic work acceptance.
- [ ] Run Claude hook configuration through its actual trust/permission flow.
  Verify startup, resume, compaction recovery, safe boundaries, and idle input in
  the supported mode. A protocol fixture is not live compatibility evidence.

**Exit:** a supported local Claude session completes the real handoff workflow,
including idle delivery and reset recovery. ACP behavior and session limitations
are documented; no approvals are bypassed and no existing user session is taken over.
If ACP fails a required capability, record the concrete failure and evaluate the
supported Claude SDK path before claiming support.

## 6. Add one setup diagnostic command

Proposed interface: `agent-mail doctor --group GROUP [--name PARTICIPANT]`.

- [ ] Check database/schema, selected identity, binding generation, worker,
  local stream socket, configured runtime endpoint, and target session state.
- [ ] Report tested client/adapter compatibility and negotiated capabilities.
  A reachable socket alone is not a working delivery integration.
- [ ] Distinguish pass, failure, warning, and unknown. Report hook trust as unknown
  without client evidence; a config file or historical invocation is insufficient.
- [ ] Give one concrete next action for each failed check. Handle missing initial
  setup and stale credentials without exposing secrets or modifying configuration.
- [ ] Return structured output and documented exit status. Never prompt an agent,
  start a runtime, rotate credentials, or approve hooks merely to run diagnostics.

**Exit:** fixtures cover missing state, stale binding, absent worker, dead socket,
unsupported capability, incompatible client, and unknown trust. Live Claude and
Codex setups receive accurate diagnostics and usable remedies.

## 7. Acceptance, documentation, and release

- [ ] Run isolated real Codex/Codex, Claude/Claude, and mixed Claude/Codex handoffs:
  assignment → submission → correction → resubmission → acceptance.
- [ ] Exercise reset/compaction and Mail worker restart during the flow. Use
  deterministic fault tests for commit/hint crashes, lost receipts, duplicates,
  subscriber reconnect, backpressure, stale sessions, and an agent ignoring input.
- [ ] Compare with the eight-turn baseline: substantive and notification-only
  turns, added context bytes, delivery latency, and model usage where observable.
  Do not equate payload bytes with total token cost or require every workflow to
  fit a predetermined turn count.
- [ ] Validate migrations from the published schema and schema 8, then run:
  `cargo fmt --all --check`,
  `cargo clippy --locked --all-targets --all-features -- -D warnings`, and
  `cargo test --locked --all-features`. Require Linux/macOS CI to pass.
- [ ] Update architecture, user guide, bundled skill, compatibility table, and
  acceptance evidence under `docs/`. Keep the README brief. Clearly distinguish
  installed release behavior from source-only features.
- [ ] Package and smoke-test supported release binaries after the final gates.
  Publish source and binaries with matching instructions; retain explicit limits
  for untested clients and out-of-scope remote behavior.

**Completion:** the local stream recovers reliably, wake reduction preserves
obligations, attention reporting makes no unsupported progress claims, diagnostics
work before setup, and Claude/Codex interoperability has a real acceptance witness.
