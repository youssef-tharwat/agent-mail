# Communication intent and durable attention

The agreed direction is to make each model interruption correspond to an
outstanding reason for its recipient to act or reconsider work. Information,
requests, outcomes, and transport receipts have different effects. This document
defines the implemented contract and its acceptance checks. Isolated regression
coverage verifies the durable and adapter boundaries; live model ingestion and
campaign rollout remain separate operational acceptance checks.

## Behavior before this change

`Publish` in [store.rs](../src/store.rs) previously had no communication-intent field. Every
delivery starts pending, and the follow-up trigger creates a plan for local
pending deliveries. `resolve_tx` publishes a reply as another ordinary message.
Consequently, routine broadcasts and final answers can create additional requests
to resolve.

The full stream in [stream.rs](../src/stream.rs) replays coordination events
independently of transport receipts. [watch.rs](../src/watch.rs) groups them into
mail, task, and attention changes. This is useful audit behavior. Passing its
output directly to a model also exposes passive updates: mail-change events
already have `wake=0`, but the stream includes them.

[followup.rs](../src/followup.rs) already persists checkpoints, prerequisites,
attention occurrences, deadlines, and bounded escalation. Native delivery,
Herdr delivery, and recovery hooks previously selected and emitted events
independently. The shared projection and reservation now build on these mechanisms and preserve the
continuation boundaries in [Minimal continuation](MINIMAL_CONTINUATION.md).

## Communication meaning

| Intent | Effect | Observation and completion |
|---|---|---|
| Request | Create a recipient obligation | Retrieval records observation. An authorized resolve, reply, or withdrawal changes the disposition. |
| Notice | Preserve information for authorized readers | No answer, business deadline, or follow-up is owed. It remains available in history and recovery context. |
| Response | Provide an outcome to a referenced request | The requester has new information to inspect. The response creates no reciprocal obligation to reply or resolve. |
| Receipt | Record transport delivery or observation | Update receipt state without completing a request or task. |
| Invalidation | Report cancellation, reassignment, or revocation | Recheck the current owner and source revision; route valid stop-work attention with priority. |

Request, notice, and response are explicit message intents. Receipts and
invalidations retain their existing domain and transport representations.
Response publication validates the referenced request and the responder's
authority. A response may reject or question an outcome; receiving it never
implies agreement or task acceptance. If a response asks for fresh work, that
work must be an explicit request.

Stored messages retain request semantics, including previously published replies.
Migration must not infer intent from text, existing `reply_to` links, or age, and
must not silently settle pending deliveries. Default `mail send` remains a
request. New notices require explicit intent. New final replies can use response
intent once every participating route supports it.

An unchanged retry retains its original canonical identity. Adding a default
intent preserves retries of stored requests. A changed intent under the same
send key is a conflict, not an opportunity to reinterpret the old message.

## Outstanding reasons for attention

Every new message also has [typed context](MAIL_CONTEXT.md): a task and observed
version, or an explicit conversation. Replies inherit the exact parent context.
Context does not itself create a response obligation or supply task authority.
Schema 26 preserves historical canonical records and recovers conversation roots;
new unscoped sends and old wire message variants are rejected. Upgrade all relay
hops and synchronize capabilities before new contextual mail is sent.

The existing task and request records remain authoritative. Attention is their
recipient-scoped projection, evaluated with current observations, persisted
waiting conditions, deadlines, and delivery policy.

An attention reason identifies its recipient and binding generation, source,
source revision or attention occurrence, reason, and stage. Its identifier stays
stable while that reason remains outstanding. Repeating a scan or restarting a
watch does not create a new reason. A later source revision, newly satisfied
condition, or due follow-up can legitimately create another reason.

Initial reasons include an unread request, an unobserved response, an assignment
change, a satisfied prerequisite, a due reminder, a required authority review,
and valid stop-work attention. Notices and routine receipt updates do not create
these reasons by themselves.

Source mutations provide durable facts and hints to reconciliation. Reconciliation
also runs after restart and at persisted deadlines; missed hints cannot erase
attention. Recheck source revision, ownership, pause policy, approval holds, and
prerequisites before delivering a reason. A transient readiness signal that is
no longer valid must not authorize stale action; its history remains inspectable.

Retrieval suppresses repeated unread hints for the records actually returned.
The business obligation persists until its explicit disposition. A checkpoint
reports the next step and schedules reassessment within the existing hard
boundary. It does not grant approval, satisfy evidence requirements, or reset
an exhausted transport budget.

## Persisted waiting conditions

The coordinator declares the condition whose outcome matters. For example, a
required broadcast can be one fan-out request with a condition that all selected
recipients resolve it. Individual resolutions update progress quietly; reaching
the declared condition produces one reason to reassess the dependent work.

Support explicit first-reply, any-settled, and all-settled predicates rather than
changing the meaning of an existing wait. The current `WaitFor::Mail` qualifies
on a reply or when all deliveries settle. This has the explicit name
`first_reply_or_all_settled`, and remains the default for an omitted predicate.
A delivery receipt alone cannot satisfy a condition
that requires a business disposition or a substantive answer.

Task prerequisites already carry qualifying states and, where required, an
accepted revision. External conditions such as a PR merge need an adapter that
persists the observed fact, source identity, and relevant revision. Shell output
or a renewed monitor's initial reading is not an authoritative business outcome.
An unsupported external wait remains visibly waiting with its responsible role
and deadline; it must not appear to have automatic supervision.

## Delivery ownership and receipts

Model-facing delivery consumes the same attention projection and policy across
Herdr, native clients, recovery hooks, and any model-facing watch. The complete
audit stream remains independent so multiple observers can replay history.

For a participant's current runtime binding, one durable delivery reservation
owns a particular attention batch across competing delivery paths. Ownership is
bound to the runtime generation and recovers through bounded expiry after failure.
Restarting the same consumer cannot reprint or resend an unconfirmed lease. Merely
opening a watch does not acquire ownership or disable the worker. A watch used
for model delivery needs an explicit integration contract for confirmed ingestion.

Track delivery attempt, runtime receipt, record retrieval, and business
disposition separately. Writing a socket, printing a batch, queue acceptance, or
advancing a stream cursor does not confirm model ingestion. A receipt applies
only to the confirmed batch and matching generation. It does not acknowledge
omitted records or authorize decisions.

Coalesce compatible reasons into bounded batches, with fair selection and a
maximum dispatch delay. While an agent is active, ordinary attention can wait
for a supported delivery boundary. Valid cancellation and revocation retain
priority. Existing pause, observe mode, and approval holds remain enforced.
Transport retries stay bounded and preserve uncertainty. Failure becomes visible
to the responsible authority or operator without generating recursive mail
requests.

Herdr reserves one source to fit its terminal prompt bound; other adapters reserve
up to five. Fetching the listed source receipts that exact observation. Hooks
restore startup/reset context and use shared reservations for ordinary attention.
Stop hooks never create continuations; the service owns persisted reassessment
deadlines in both enabled and observation modes. The old independent hook budget
and Stop fallback are removed, and migration refreshes the guide once for existing
sessions.

## Implementation

1. **Explicit message intent.** Add an additive migration, validation, and public
   message representation. Preserve stored requests and canonical retries. Add
   notice publication and response handling, update pending and follow-up
   projections, and make CLI help and bundled instructions describe the intended
   effects. Keep audit replay complete.
2. **Waiting predicates and attention projection.** Extend existing checkpoints
   with explicit mail predicates and implement one projection of current reasons.
   Expose both a bounded attention snapshot and changes to that projection.
   Ensure snapshots recover outstanding reasons without replaying all passive
   history.
3. **Shared delivery ownership.** Make all model-delivery adapters use the same
   reservations, generation checks, and receipt contract. Preserve protocol
   recovery and cancellation priority. Provide an explicit model-facing watch
   integration rather than interpreting arbitrary stdout as receipt evidence.
4. **External facts and operational use.** The GitHub adapter persists observed PR
   identity, state, head, merge commit, query time, and errors. A declared wait
   requires a confirmed merge of its exact expected full head. The service refreshes
   a fair bounded page independently of delivery and caches observations for two
   minutes. Status identifies unsupported waits as manually supervised. Existing
   coordinator sessions can adopt these declared waits when loading the new guide.

Relay intent and receipt capabilities must be negotiated before routing new
semantics to a peer. A peer that cannot preserve notice or response meaning must
reject that traffic visibly; silently treating it as a request is unacceptable.
Ordinary request traffic keeps its existing wire representation. New intents use
an explicit typed event after capability negotiation. Migration clears queued
prompts without batch identity and current sources recover through the shared
projection; there is no unscoped receipt fallback.

[names.rs](../src/names.rs) validates group and participant names and closes the
delivery-consumer vocabulary. [notification.rs](../src/notification.rs) implements
bounded current snapshots, shared leases, exact receipts, source revalidation,
and per-reason retry budgets. Exhausted reasons cannot block newer eligible work
or acquire fresh retries because unrelated events arrived. Migration carries
already consumed budgets into this ledger once. The old raw delivery renderer is
removed; audit replay retains its independent purpose.

Source files affected include [store.rs](../src/store.rs),
[states.rs](../src/states.rs), [relay.rs](../src/relay.rs),
[followup.rs](../src/followup.rs), [events.rs](../src/events.rs),
[watch.rs](../src/watch.rs), [stream.rs](../src/stream.rs),
[service.rs](../src/service.rs), [native.rs](../src/native.rs),
[claude_inbox.rs](../src/claude_inbox.rs), [hooks.rs](../src/hooks.rs),
the CLI and recovery views, migrations, and bundled agent instructions.

## Acceptance checks

- A notice creates no response obligation, follow-up, or deadline escalation;
  authorized readers can still recover it.
- A new final response does not create an acknowledgement request. Stored replies
  and requests retain their recorded disposition and retry identity.
- An agent's own confirmed command result causes no redundant interruption.
  An external update satisfying a declared dependency does produce attention.
- Eight routine receipts remain quiet. Eight required business dispositions
  satisfy an explicit all-settled condition once; an earlier first reply cannot
  satisfy that condition.
- Repeated scans and re-armed watches do not create new attention. A later task
  revision remains independently visible.
- Two delivery paths racing for the same reason share a reservation. Lost or
  ambiguous receipts retain bounded recovery; no exactly-once ingestion claim
  is made.
- Restart, disconnect, clock changes, missed hints, and truncated batches preserve
  outstanding attention. Returning one page does not receipt the next page.
- A replacement binding cannot inherit its predecessor's delivery proof. Stale
  owners cannot act or suppress delivery to the current owner.
- Blocked and review work remains held. Due review reaches its decision owner;
  stop-work attention has priority over ordinary information.
- Unsupported peer or external-condition capabilities fail visibly without
  weakening semantics or leaving a false success record.

Run existing migration, relay, follow-up, watch, reliability, native inbox, and
delivery-verification coverage alongside focused negative controls. The live
acceptance must exercise competing paths, restart, an actual model consuming a
bounded batch, preserved holds, and a coordinator receiving a failed-delivery
escalation. Fixture tests alone cannot establish those live-runtime claims.

Measure model interruptions per newly observed attention reason, duplicate
delivery attempts, required-attention latency, recovery after disconnects, and
escalation latency. A quieter session is useful only while required attention
continues to arrive or visibly escalates.
