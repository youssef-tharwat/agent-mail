---
name: agent-mail
description: >-
  Use Agent Mail for durable handoffs in a configured Mail group: resume assignments after a context reset, handle pending requests and replies, and record task decisions. Apply when coordinating agents through Mail, including Claude Code, Codex, Herdr and Fleet workflows, even when the user describes a forgotten assignment or stalled handoff without naming Mail. Requires Agent Mail v0.4 or later.
---

# Agent Mail

Mail owns durable tasks and messages. The runtime owns live agents and permissions.
The workflow decides reviews and acceptance. Use `agent-mail COMMAND --help` for
the installed syntax. v0.4 uses `task`, `mail`, `participant` and `runtime` groups.

## Identity and recovery

Use the participant credential assigned by the operator in `AGENT_MAIL_SESSION`,
or a verified Herdr binding with that variable unset. Never borrow credentials,
register yourself, rotate identity or guess another group to bypass an error.
Group selection uses `--group`, then `AGENT_MAIL_GROUP`, then verified identity.
A supplied invalid credential fails; it never falls back to another runtime.

Use injected recovery context directly. Do not call `context` just to repeat it.
If recovery is not configured, run `agent-mail context` at startup/reset or after
a wake hint; report the missing integration to the operator. Do not poll each turn.
Fetch only needed details: `task show ID` or `mail show ID`. Follow a returned
cursor when omitted items matter. Recovery JSON uses `work` for task summaries.

## Requests and answers

- New request: `agent-mail mail send RECIPIENT "SUMMARY" --key STABLE_KEY`.
  Add `--task ID` for a task association and `--body-file PATH` for details.
  A new logical request needs a new key; an identical retry reuses its key.
- Final answer: `agent-mail mail reply ID "ANSWER"`. This replies and resolves
  atomically, with automatic retry identity. Changed retries conflict. Use a new
  send for interim discussion that must leave the original request open.
- Outcome without an answer: `agent-mail mail resolve ID --note "OUTCOME"`.
  Reading is not resolution. Leave requests pending while action is still owed.
- Sender cancellation: `agent-mail mail withdraw ID`. It does not accept a task.

Use `--body-file -` for bounded stdin. Large evidence stays in Git, CI or artifacts;
send references. Deadlines are optional (`--due-in 15m`), not permission to retry
agent tools or make acceptance decisions.

## Task decisions

The designated writer creates assignments with
`agent-mail task create ID "TASK" --owner NAME`. The task text supplies the initial
next action unless overridden. Creation publishes notifications automatically.
Do not send duplicate bookkeeping mail.

Only the writer changes a task, using the observed version and an explicit reason:

```sh
agent-mail task update ID --version VERSION --reason "REASON" \
  --next-action "NEXT ACTION" --resolve MESSAGE_ID
```

`--resolve` is optional and must identify a request in the writer's inbox linked
to that task. The update and resolution commit together. For complex changes use
`task update ID --file PATH` (or `--file -`) with:

```json
{"version":2,"reason":"Evidence verified","patch":{"state":"accepted","open":false,"accepted_revision":"abc123","evidence":["ci/run/42"]},"resolve_message":12}
```

Versions, revisions and message IDs above are illustrative. Acceptance follows
the active workflow, never an idle agent, reply or delivery receipt. Non-writers
submit results through Mail. Omitted patch fields remain unchanged; JSON null
clears an optional deadline/revision and `[]` clears evidence. Do not mix file
input with mutation flags.

Task creation/update retries are identified automatically from durable task
identity and expected version. Retry identical content after an uncertain result;
on conflict, read current state and reconsider. Never silently substitute a newer
version to force an old decision through.

## Delivery belongs to integrations

Hooks and runtime adapters own event receipts and recovery. Never call `adapter ack`
on their behalf, reset a retry budget, attach/detach endpoints or launch runtimes
to repair a stalled handoff. Report `agent-mail status --check NAME` to the operator.
A confirmed delivery does not mean the model completed the work. Deadline and
transport warnings describe attention needed, not authority to take action.
