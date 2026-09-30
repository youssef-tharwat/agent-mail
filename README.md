# Agent Mail

**Local, durable coordination for coding agents.**

Keep tasks, ownership, next actions, evidence and unanswered requests across
context resets and restarts. Agent Mail stores them in SQLite and restores the
relevant context when an agent resumes.

One native CLI. No account or hosted service. Works with Claude Code and Codex;
Herdr and Fleet Campaign are optional.

![Assign a task, recover after a reset, send a result, and accept it.](assets/agent-mail-demo.gif)

## Install

```sh
brew install youssef-tharwat/tap/agent-mail
```

Prebuilt binaries support macOS and Linux on ARM64 and x86-64. No Cargo or Rust
compiler required. [Other installation options](docs/usage.md#install).

## Agent skill

Agents need the [operating skill](skills/agent-mail/SKILL.md) to use Mail correctly.
It teaches assignment, blockers, review, correction, acceptance and safe retries.

**Managed Codex/Claude launches:** `agent-mail run NAME -- codex` (or `claude`)
supplies the bundled, version-matched instructions at startup and after context
resets. Routine updates do not repeat them. No separate skill installation or
Node/npm is needed for this flow.

**Discoverable skill:** to let your client discover and invoke `$agent-mail`,
install it with the skills CLI (requires Node/npm):

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

Select your agent clients in the installer, then start a new agent session.
Keep separately installed skills updated alongside the binary.

**Inspect the bundled instructions:**

```sh
agent-mail --skill
```

This prints the skill; it does **not** install or register it with your client.

## Agent lifecycle

Agents have durable `registered` / `retired` states, versions and history:

```sh
agent-mail agent show worker
agent-mail agent update worker --version VERSION --state retired --reason "Finished"
agent-mail agent history worker
```

Retirement requires owned/written tasks and incoming/outgoing mail to be settled.
Restore with `--state registered` and the current version, then launch again.
Old credentials and runtime attachments stay invalid. Registration state is
separate from whether the client is running.

## Quick start

Create a group, register agents, and start the delivery worker:

```sh
agent-mail init project
agent-mail agent add coordinator
agent-mail agent add worker
agent-mail service run
```

Leave the worker running. Launch each agent in a separate terminal:

```sh
agent-mail run coordinator -- codex
agent-mail run worker -- claude
```

Mail supplies identity and configures recovery hooks. Review native hook trust
when prompted; existing agent permissions still apply. Interactive Codex gets a
private local backend with automatic attachment. Claude uses its native inbox.

### Assign, report, accept

Inside the coordinator's session:

```sh
agent-mail task create api-review "Review API changes at abc123" --owner worker
```

The worker receives the assignment through the integration. After doing the work:

```sh
agent-mail mail send coordinator "Reviewed abc123; evidence: reviews/api.md" \
  --task api-review --key api-review-result-v1
```

The coordinator checks the result and evidence, then reads the current task and
pending mail with `task show api-review` and `mail list`. Using the observed
`VERSION` and incoming `MESSAGE_ID`, it can accept and resolve together:

```sh
agent-mail task update api-review --version VERSION --reason "Evidence verified" \
  --state accepted --accepted-revision abc123 --evidence reviews/api.md \
  --resolve MESSAGE_ID
```

The task creator is its **writer**; only that identity can change it. The **owner**
does the work and reports results. Changes publish notifications automatically.
Your workflow decides what evidence is sufficient for acceptance.

## Task lifecycle

Tasks use `open`, `ready`, `active`, `blocked`, `review`, `done`, `accepted`, or
`cancelled`. The last three close the task automatically—there is no separate
close flag. `done` records completion; `accepted` records the writer's acceptance.

Updates require the version you observed and a reason. A linked message can be
resolved in the same transaction. Reading a message, delivering a notification,
or ending an agent turn never completes a task.

Tasks store evidence references. General resource attachments are not implemented.

## Recovery and delivery

Use injected context directly. If your integration has not supplied it, run
`agent-mail context`. Fetch `task show ID` or `mail show ID` only for needed details.

The delivery worker uses durable events and bounded retries. It survives restarts
without relying on the agent to remember delivery bookkeeping. The agent or
operator still makes business decisions explicitly.

```sh
agent-mail status
agent-mail status --check worker
```

Diagnostics distinguish missing setup, observed hooks and delivery problems.
Hook execution is evidence of integration activity, not proof of model consumption.
Manual CLI use works without the delivery worker; automatic idle wake needs it.
[Runtime setup and controls](docs/usage.md#automatic-recovery-and-delivery).

## Herdr plugin (optional)

```sh
herdr plugin install youssef-tharwat/agent-mail
```

The plugin downloads a verified binary. Herdr owns agent sessions and pane identity;
Mail owns durable coordination. Load the output of `agent-mail --skill` at startup
or [install the discoverable skill](#agent-skill). [Herdr setup](docs/usage.md#optional-herdr-integration).

## Upgrading to v0.6

Stop clients and the delivery worker, back up your store, then follow the
[upgrade guide](docs/usage.md#upgrading). Schema 14 introduces typed task states
and rejects ambiguous legacy states. Writable `open`, `--close` and `--reopen`
have been removed. The bundled skill matches the installed binary.

## Documentation and contributing

- [User guide](docs/usage.md): commands, integrations and troubleshooting.
- [Agent operating skill](skills/agent-mail/SKILL.md): usage flows and decisions.
- [Architecture](docs/ARCHITECTURE.md): ownership and delivery guarantees.
- [Live validation and limitations](docs/native-launch-acceptance.md).
- [Development](docs/usage.md#development) · [Implementation plan](docs/IMPLEMENTATION_PLAN.md).
- [Issues](https://github.com/youssef-tharwat/agent-mail/issues): bugs and proposals.

Maintained by [Youssef Tharwat](https://github.com/youssef-tharwat). [MIT](LICENSE).

## Multiple fleets

Each group has its own agents, tasks and inboxes. Groups share the local database
and delivery service. Adding a group on the current schema does not stop delivery.

```sh
agent-mail init recall
agent-mail --group recall agent add coordinator
agent-mail --group recall status
agent-mail status --all-groups  # installation-wide operator view
```

Commands infer the group from the current identity or sole group. Ambiguous
selection fails rather than choosing another fleet.
