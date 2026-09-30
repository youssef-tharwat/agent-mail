---
name: agent-mail
description: >-
  Use Agent Mail to coordinate durable tasks and messages: recover assignments after context resets, assign work, report blockers or results, request reviews, and record corrections or acceptance. Apply to Mail-based handoffs in Codex, Claude Code, Herdr or Fleet, including forgotten assignments and stalled requests. Also use when asked to set up coordination for a new project or fleet, follow changes, wait for replies, diagnose delivery, or retire and restore registrations. Workflow and runtime integrations are optional; this skill teaches the tool's operating flows.
---

# Agent Mail

Load the operating instructions from the installed binary:

```sh
agent-mail --skill
```

Follow that output for setup, tasks, mail, identity, delivery and repair. It matches
the installed version; this discoverable skill deliberately carries no copied CLI
reference that could become stale after an upgrade. If the current session already
contains the guide injected by Agent Mail's startup/reset hook, use it directly.

If the command is missing, report that Agent Mail must be installed and on PATH.
Do not substitute remembered commands or fetch instructions for another version.
Installing or updating the binary takes effect on the next invocation that loads
these instructions; it does not rewrite an active conversation's existing context.
