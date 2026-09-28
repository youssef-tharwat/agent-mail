---
name: agent-mail
description: >-
  Use Agent Mail for durable handoffs in a configured Herdr group: resume assigned work after a context reset, handle pending requests and replies, and maintain linked work records. Apply when the task describes these needs even without naming Agent Mail.
---

# Agent Mail

Agent Mail stores messages and small work records outside agent context. Herdr owns live agent identity and sessions; Mail owns pending deliveries and work state. Use `agent-mail <command> --help` for the installed CLI syntax.

## Resume with bounded context

- Use the task's group name; `default` is appropriate only when that is the configured group. Run `agent-mail context --group <group>` when starting or resuming Mail-backed work, after a context reset, or after a Mail wake hint. Do not poll it every turn.
- The command gives short work and inbox summaries. Follow a returned cursor only when more entries are relevant. Fetch a full message with `agent-mail inbox --group <group> <message-id>` or a work record with `agent-mail work show --group <group> <work-id>` when needed.
- If the CLI reports that this agent is unbound or the group is missing, tell the operator which binding or setup is needed. Do not bind yourself or invent a group.

## Send and resolve

- Send a new request with `agent-mail send --group <group> --to <recipient> --key <stable-key> --summary <short-summary>`. Add `--work-id <id>` when it concerns a work record, and `--body-file <path>` only when detail is needed. Keep large evidence in Git, CI, or artifacts and send references.
- A send key identifies one logical message. Reuse it only to retry identical content; use a new key for a changed request.
- Reading a message does not resolve it. After handling your delivery, run `agent-mail resolve --group <group> <message-id> --note <outcome>`. If an answer is owed, add `--reply-key <stable-key> --reply-file <path>` so the reply and resolution commit together. Leave unresolved requests visible while they still need action.

## Maintain work deliberately

- The designated home writer creates or updates work records. Inspect the current version first, then update with `agent-mail work update --group <group> <work-id> --version <version> --reason <reason>` and the intended fields. On a version conflict, reread the record and reconsider the change.
- Other participants send results or correction requests through Mail. A reply, an idle agent, or a passing test does not itself accept work or change its owner or state. Follow the active workflow's acceptance rules.

Automatic prompts are off by default; use the bounded context command at real checkpoints.
