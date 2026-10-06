# Mail context

Every new message identifies the work or discussion it concerns. The group selects
the participants; context selects a task revision or a durable conversation. Intent
separately determines whether a request, quiet notice, or final response is sent.

## Task observations

Read the task and supply the version you observed:

```sh
agent-mail task show api-review
agent-mail mail send coordinator "Review result; decision needed" \
  --task api-review --version 1 --key api-review-result
```

The stored context is `{"kind":"task","id":"api-review","version":1}`.
The task must exist in the current group and the positive version must already be
known locally, through authoritative work or a synchronized snapshot. An older
observation remains valid evidence; it does not assert that the task is current.
A future or absent task version is rejected before any mail is persisted.
Context never assigns work, authorizes a decision, accepts a result, or settles a
request. Retrieved qualifying task requests retain the shared scheduling behavior.

## Conversations and inherited replies

Begin an explicit discussion without inventing a task:

```sh
agent-mail mail send reviewer "Discuss the API boundary" \
  --new-conversation --key api-boundary
```

The result includes `context.kind=conversation` and a stable UUID. An identical
retry returns the same message and conversation, including after restart. Continue
with `--conversation UUID`, or inherit a visible message's context:

```sh
agent-mail mail send coordinator "Interim finding; review continues" \
  --reply-to 12 --key finding-12
agent-mail mail reply 12 "Final review result"
agent-mail mail conversation UUID --after 0
```

`mail reply` sends a final response and resolves the original delivery atomically.
`mail send --reply-to` creates an independent communication and leaves the original
request pending. Both preserve the exact parent context, including the task version
the original sender observed. A reply cannot independently replace that context.
Portable parent UUIDs remain available even when a private parent is absent from
the recipient's store; this reference grants no access to that parent's contents.

Conversation membership does not grant access to every participant's messages.
The sender can continue a thread only after authoring or receiving mail in it,
and can invite another recipient through an authorized send. Conversation history
contains up to six authored or addressed summaries per page, with a cursor. It
exposes neither other recipients' summaries nor bodies, and does not acknowledge
delivery or mark the source body retrieved. Existing inbox visibility still governs
`mail show` and business dispositions. Context UUIDs are scoped to their group.

## Storage and upgrades

Schema 26 adds immutable, mandatory message context and enforces inherited context
for replies with a locally known parent. New API publications use a closed
`ContextSource` enum; absent, conflicting, unknown, or untyped input is rejected.
The send CLI requires one context selector; task context also requires `--version`.

Historical mail did not record observed task versions. The migration assigns
conversation roots from existing reply references and stable message UUIDs,
including queued relay messages. It preserves existing task associations, private
recipients, request dispositions, canonical retry records, receipts and budgets.
It does not invent a historical task observation or silently complete old mail.
Invalid or cyclic histories abort the migration, with the original verified backup
retained. Active sessions reload the updated guide on their next input hook.

Relay peers advertise `mail_context_v1`. Every new message uses a contextual wire
event that older relays reject, so context cannot be silently discarded. Upgrade
and synchronize every relay hop before sending new mail. Existing queued mail is
converted during migration and retains its envelope and message identities.
There is no runtime fallback accepting new unscoped publications or old mail events.
