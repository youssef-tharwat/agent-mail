# Improvements from local testing

These improvements are approved and specified in the active
[implementation plan](IMPLEMENTATION_PLAN.md). They are not completed features.
Keep the same CLI, SQLite store, and small background worker.

Evidence: [local Codex acceptance](local-codex-acceptance.md).

## 1. Reduce unnecessary wake turns

**Observed:** the final test used eight model turns for four substantive actions:
submit, request correction, resubmit, and accept. The remaining turns handled
notifications without changing the work. Mail text was small (623–1,026 bytes per
wake), but each wake still pays for a model turn and its existing context.

Make delivery aware of whether a turn is needed. Keep every change durable, but
avoid a new turn solely to echo an action that the same session just committed
or announce closure when that recipient has nothing else to do. Include those
changes in its next recovery view. Continue coalescing changes that arrive while
an agent is busy. Reassignment, new requests, and corrections must still wake
the responsible participant.

Validate against the same two-agent flow: fewer notification-only turns with no
missed obligations, including after a restart. Do not remove durable subscriptions
to save turns.

## 2. Distinguish delivered notifications from work that needs attention

**Observed:** a queue response timed out even though Codex received the update.
Durable retry handling worked. The resulting delay for a newer event was fixed.

**Remaining design gap:** a successful queue receipt proves transport acceptance,
not that an agent acted. A caught-up delivery cursor can coexist with unresolved
work. The successful live run does not establish recovery from an agent ignoring
an assignment.

Report delivery and work progress separately. Surface expired deadlines,
exhausted attempts, and missing endpoints as attention items. Attention reporting
is the default. Add a bounded deadline follow-up only when responsibility and
waiting conditions are explicit; otherwise report the obligation to the operator.
Do not add an agent-maintained bookkeeping flag or infer waiting from arbitrary
workflow-state names.
Never infer acceptance or automatically retry tool side effects.

Test a client that accepts a notification but makes no progress. It should leave
an explicit attention item, without an endless wake loop or false completion.

## 3. Make setup self-checking

**Observed:** live setup required a participant credential, the correct Codex
socket and thread UUID, a running Mail worker, and separately trusted hooks.
Codex 0.157 also needed next-boundary recovery after PostCompact invalidation.

Add one diagnostic command that checks the current binding, endpoint reachability,
thread state, Mail worker, and known client compatibility. Show one concrete fix
for each failed check. Report hook trust as unknown unless the client provides
proof; configuration on disk is not proof of activation. Keep installation and
attachment explicit, preserving unrelated client settings.

## Scope

The approved scope also includes a resumable local event stream, evaluation of
ACP at the runtime boundary, and live Claude support alongside Codex. The active
implementation plan defines their order and acceptance gates. No new broker,
hosted service, workflow language, or agent supervisor is needed. Remote SSH
validation remains separate work.
