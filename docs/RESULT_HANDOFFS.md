# Task results and shared-session delivery

The reliability contract is a durable writer obligation. Submitting a result
does not accept work. Paused delivery, a stopped worker, session replacement or
uncertain command output cannot erase the submitted result or its pending
disposition.

## Submit and decide

On the task's home store, its current owner submits JSON with `task report`:

```json
{
  "version": 3,
  "key": "api-result-abc123",
  "summary": "Revision ready; review and acceptance required",
  "revision": "abc123",
  "evidence": ["ci/run/42", "github:owner/repo@abc123:reviews/result.md"],
  "body": "Checks passed. No outstanding findings."
}
```

```sh
agent-mail task report api --file result.json
agent-mail task reports api
agent-mail task result RESULT_ID
```

One SQLite transaction validates the current identity, exact task version and
owner, saves immutable result metadata, and publishes a mandatory contextual
request to the writer. Existing message triggers create durable attention and
follow-up supervision in that transaction. There is no separate result queue.
Reported revisions share acceptance's 128-byte limit. New results on terminal,
reassigned or superseded tasks are rejected. Unknown
fields, including a quiet `intent`, are rejected before effects are committed.

Retries use the original key and identical content. They return the same logical
result even after a decision or writer transfer. Changed retries conflict. A
failed delivery diagnosis never changes a successful commit into a failed write;
the CLI returns `persisted`, `disposition` and separate recipient `delivery`
diagnostics. Retry the same input after uncertain output instead of inventing a
new key.

The returned `message` identifies the writer's current inbox obligation. The
writer reads the result and current task, verifies evidence, then applies its
authorized decision with `task update ... --resolve MESSAGE_ID`. That task
decision and request resolution commit together. Fetching a result receipts only
the addressed notification; the obligation remains pending. Task closure without
resolution does not silently settle it.
An owner cannot withdraw a typed result's current decision request; corrections
remain explicit reports for the writer to assess.

Metadata is group-visible; result bodies are available to the submitter and
current task writer. Submission explicitly grants an authorized successor writer
access to that result. A writer transfer forwards its pending decision request
transactionally, preserves immutable evidence and the original escalation
boundary, and withdraws the superseded request. Ordinary private mail is not
forwarded. An already-published task escalation remains available to the successor,
including a writer returning after previously reading that escalation. Read the
current follow-up version after transfer before checkpointing. Pending results
hold owner reminders for their current assignment;
the service continues supervision to the writer. A later scope/owner revision
does not grant an old result permission to revive work.

Typed submission currently requires an authoritative local task record. Remote
snapshot holders retain the contextual-mail path; the operation explicitly
rejects a cached snapshot as proof of current ownership.

## One session, several groups or projects

A group retains its identities, tasks, mailboxes, authority and receipts. An exact
Herdr session endpoint consists of its socket, pane, terminal, agent kind and
native session identity. Addresses already explicitly bound to that same
endpoint can share delivery consent and appear in one attention view:

```sh
agent-mail --group neola-main runtime herdr-policy unguarded --agent coordinator
agent-mail attention session
agent-mail attention session --after-group neola-intake
```

The named consent command is an operator operation and verifies the current live
target. Without `--agent`, it authenticates the calling agent. Consent is shared
only across exact endpoint matches. New groups bound to that same session use
its existing choice; another pane, another socket and a replacement native
session do not inherit it. Group pauses and per-binding delivery opt-outs remain
effective. Legacy group policies migrate conservatively: conflicting choices
become notification-only, and future sessions receive no group default consent.

The combined view discovers only the current session's registered bindings. Each
item retains its group and binding generation. It reports pause and consent
status, records no receipt, and supplies an explicit continuation cursor when
its byte or group budget omits entries. Recovery also identifies additional
bindings. Ordinary writes still require explicit group authority; a standalone
credential remains confined to its own registration.

Herdr wake instructions contain the exact `--group` fetch command. All addresses
of one endpoint share an OS wake lock, including verification prompts, while
attention reservations and receipts remain scoped to their individual binding
generations. Long source/group identifiers cannot prevent an actionable hint:
when its verification challenge cannot fit the same prompt, verification is
deferred without consuming a challenge attempt.
Each scan selects the oldest eligible attention across the endpoint's groups,
with stop-work priority. A busy group or an exhausted/cooling reason cannot take
another group's dispatch slot.

## Acceptance

Regression coverage verifies atomic rollback, concurrent identical retries,
restart recovery, immutable evidence, writer transfers and preserved supervision
boundaries, quiet-result rejection, current-owner/version checks, group isolation,
four-group session coverage, independent pauses/opt-outs, replacement-session
consent and conservative migration. Existing runtime and migration suites remain
required alongside these tests.

Live model ingestion and an installed multi-group campaign are separate
acceptance evidence. The source change does not install a binary, migrate the
operator's live store, alter live consent or decide pending Neola work.
