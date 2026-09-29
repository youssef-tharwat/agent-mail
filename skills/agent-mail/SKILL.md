---
name: agent-mail
description: >-
  Use Agent Mail for durable handoffs in a configured Mail group with Herdr or standalone participants: resume assigned work after a context reset, handle pending requests and replies, and maintain linked work records. Apply when the task describes these needs even without naming Agent Mail.
---

# Agent Mail

Agent Mail stores messages and small work records outside agent context. Mail owns durable participant identities, pending deliveries, and work state. Herdr supplies live sessions and wake hints for Herdr bindings; standalone participants use operator-issued session credentials. Use `agent-mail <command> --help` for the installed CLI syntax.

## Identity

Use the assigned group and identity. A Herdr pane uses its verified binding with `AGENT_MAIL_SESSION` unset. A standalone agent receives its own `AGENT_MAIL_SESSION` from the operator or launcher; `--session` is an explicit override. Never borrow another participant's credential. An invalid credential fails rather than falling back to Herdr. If it expires through explicit replacement, ask the operator for the new registration. `participants` lists bindings, not live availability.

## Automatic recovery when configured

On v0.3 or later with trusted lifecycle hooks or a configured Codex queue adapter, use the supplied Agent Mail
recovery context directly. Do not run another `context` merely to repeat it.
Fetch details by ID when the supplied view is insufficient. Hook delivery and
`ack` receipts never mean a request was handled; acknowledgment is the runtime
adapter's responsibility. Do not acknowledge events on behalf of an adapter.
If no recovery adapter is configured, use the manual recovery path below and tell the
operator that automatic recovery is not configured.

## Resume with bounded context

- Use the task's group name; `default` is appropriate only when that is the configured group. Run `agent-mail context --group <group>` when starting or resuming Mail-backed work, after a context reset, or after a Mail wake hint. Do not poll it every turn.
- The command gives short work and inbox summaries. Follow a returned cursor only when more entries are relevant. Fetch a full message with `agent-mail inbox --group <group> <message-id>` or a work record with `agent-mail work show --group <group> <work-id>` when needed.
- If the CLI reports that this agent is unbound or the group is missing, tell the operator which binding or setup is needed. Do not register, rebind, or rotate your own identity, and do not invent a group.

## Send and resolve

- Send a new request with `agent-mail send --group <group> --to <recipient> --key <stable-key> --summary <short-summary>`. Add `--work-id <id>` when it concerns a work record, and `--body-file <path>` only when detail is needed. Keep large evidence in Git, CI, or artifacts and send references.
- A send key identifies one logical message. Reuse it only to retry identical content; use a new key for a changed request.
- Reading a message does not resolve it. After handling your delivery, run `agent-mail resolve --group <group> <message-id> --note <outcome>`. If an answer is owed, add `--reply-key <stable-key> --reply-file <path>` so the reply and resolution commit together. Leave unresolved requests visible while they still need action.

## Maintain work deliberately

- On v0.3, the designated writer uses `work decide <id> --file <json>` for an authorized decision. Supply a stable `key`, current `version`, `reason`, and `patch`; include `resolve_message` when the decision handles a linked request in your inbox. This commits the state, resolution, history, and notifications together. Reuse the key only for identical retries. Work changes notify subscribers automatically; do not send duplicate bookkeeping messages.
- On v0.2 or for initial creation, the designated home writer creates or updates work records. Inspect the current version first, then update with `agent-mail work update --group <group> <work-id> --version <version> --reason <reason>` and the intended fields. On a version conflict, reread the record and reconsider the change.
- Other participants send results or correction requests through Mail. A reply, an idle agent, or a passing test does not itself accept work or change its owner or state. Follow the active workflow's acceptance rules.

Only the operator attaches or detaches Codex wake endpoints. Do not rearm a delivery budget yourself or poll while waiting for the other agent. A queued update may be repeated after a lost transport response; use stable operation keys.

Herdr prompts remain off by default. Trusted client hooks supply recovery automatically; manual checkpoints are the fallback when hooks are unavailable.

If delivery appears broken, report `agent-mail doctor --group <group> --name <participant>` to the operator. Do not launch an agent, rotate identity, or rearm retries to repair it yourself. Deadline and delivery warnings are attention facts, not permission to retry work. `watch` is for programmatic consumers; do not use it to keep a model turn waiting.
