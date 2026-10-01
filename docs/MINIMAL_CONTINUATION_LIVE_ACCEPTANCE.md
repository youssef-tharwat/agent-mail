# Minimal continuation live acceptance — 2026-10-01

Tested the reduced implementation with Codex 0.159.3 and its configured default
model, `gpt-6.1-sol`, on macOS. The tests used private Mail databases, dedicated
Codex app-server Unix sockets, and disposable workspaces. Global hook and trust
configuration, existing user groups, and the installed release were unchanged.
The rollout was limited to the isolated `minimal-live` group in these stores.

## Continuation with no new input

Two runs used the service to deliver the original task, with no manually started
model turn. The worker fetched the task, wrote partial progress, recorded a
75-second checkpoint for the remaining action, sent a status report, and ended
its turn. The remaining output did not exist when that turn ended.

One run kept the service running. The other stopped it cleanly after the partial
turn and started a new worker process. Each persisted deadline produced an
`attention_due` event and a new model turn. The agent fetched the current records,
wrote the remaining output, and reported completion. There were no incoming user
messages, follow-up prompts, polling loops, or product lifecycle subscriptions.
The task remained active at version 1 until the writer independently verified
the evidence and closed it at version 2.

| Scenario | Persisted checkpoint, UTC | Attention event, UTC | Result |
| --- | --- | --- | --- |
| Service restart | 2026-10-01 21:15:50 | 2026-10-01 21:15:50 | Remaining output written by resumed agent |
| Continuous service | 2026-10-01 21:17:43 | 2026-10-01 21:17:43 | Remaining output written by resumed agent |

The harness kept an observation connection to collect runtime events. That
connection neither scheduled model turns nor sent follow-up input. The product
service used its existing deadline and queue paths.

An initial exploratory run was excluded from acceptance: its manually started
turn raced the first service notification, leaving that notification queued for
the next turn. It showed resumption but did not prove deadline-driven continuation.
The two accepted runs removed manual `turn/start` calls and required a persisted
deadline event before accepting the result.

## Approval hold and named coordinator

A real worker fetched a blocked task, recorded an active checkpoint, and ended
its turn without implementing or sending the coordinator a message. The service
was restarted. When the checkpoint became due, it created exactly one escalation
addressed to the task writer, named `coordinator`, and no worker reminder.

The coordinator model fetched the attention and task, verified the blocked state,
and wrote a receipt containing the attention ID and task ID. The task remained
blocked. Its checkpoint did not grant approval, and no implementation output was
created. This proves model consumption of the escalation as well as transport
acceptance.

## Failure and rollout boundaries

A second task was assigned to an isolated registered participant without a runtime
endpoint. The service retained it through the 240-second hard boundary and
escalated it to the same coordinator. The coordinator fetched the escalation and
source task and wrote an explicit receipt; the task remained active.

This live failure is unavailable delivery, rather than three lost queue responses.
Bounded retry exhaustion, lost receipts, concurrent scans, fresh-event cooldown,
and uncertainty surviving restart were separately rechecked in the 35 targeted
native/follow-up tests. Native queue submission remains at-least-once; no
exactly-once model execution or external-effect guarantee is claimed.

After collecting evidence, the isolated group was paused and all test service
and app-server processes were stopped. No broader rollout, merge, or release was
performed. A production service still needs its existing process supervision.

## Review and validation

The primary agent reviewed design and correctness in two sequential passes;
these were not independent reviews. The inspected scope included native/Herdr
wakes, lock and reservation lifetimes, hook/inbox retrieval, follow-up escalation,
approval and dependency holds, verification, status and retained migrations.
Two documentation nits were fixed: obsolete immediate-turn wording in CLI help
and an incomplete description of wake-lock ownership. No additional actionable
code defect was found in that scope.

The attempted graph refresh failed while sealing a Markdown input. Review used
current source and the frozen Git diff; no graph all-clear is claimed.

- Full repository tests passed on the implementation commit, with one existing
  ignored test.
- Final targeted suite: 35 tests passed.
- Final strict Clippy, formatting, build and diff whitespace checks passed.

Sanitized runtime traces, scenario results, harness sources, and the review ledger
are retained under the ignored `.state/minimal-continuation-review/` directory.
Raw credentials and Mail databases remain in private disposable fixture directories
and are excluded from commits and PR content.
