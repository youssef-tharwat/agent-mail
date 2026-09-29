# Agent Mail

**Durable tasks and messages for coding agents, on your machine.**

Keep assignments, next actions, evidence and unanswered requests across context
resets and restarts. Agent Mail stores coordination state in local SQLite and
supplies current context through Claude Code, Codex or Herdr integrations.

One native CLI. No account or hosted service. Herdr and Fleet Campaign are optional.

![Agent Mail: assign a task, recover after a reset, send a result, and accept it.](assets/agent-mail-demo.gif)

## Install

Install both the CLI and the required agent skill:

```sh
brew install youssef-tharwat/tap/agent-mail
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

Prebuilt macOS and Linux binaries for ARM64 and x86-64. **No Cargo or Rust compiler
required.** The skill installer uses Node/npm; select every agent client that
will use Mail. The [skill](skills/agent-mail/SKILL.md) supplies the handoff and
decision rules agents need. [Installation details](docs/usage.md#install).

## Quick start

Create a group and two participants:

```sh
agent-mail init project
agent-mail participant add coordinator
agent-mail participant add worker
```

Each registration returns a `session` credential. Give each participant its own
`AGENT_MAIL_SESSION`. Keep credentials out of version control.

As the coordinator, assign a task:

```sh
export AGENT_MAIL_SESSION='<coordinator credential>'
agent-mail task create api-review "Review API changes at abc123" --owner worker
```

As the worker, recover the assignment and send a result:

```sh
export AGENT_MAIL_SESSION='<worker credential>'
agent-mail context
agent-mail mail send coordinator "Reviewed abc123; evidence: reviews/api.md" \
  --task api-review --key api-review-result-v1
```

The group is inferred from the credential. Task changes notify the relevant
participants automatically. The writer decides whether to accept the result;
receiving a message never marks a task complete.
[Replies and task decisions](docs/usage.md#atomic-decisions).

## Connect your agents

For automatic recovery and idle wake, configure a runtime and run the Mail worker:

```sh
agent-mail service run
```

| Runtime | Setup |
| --- | --- |
| Claude Code | [Normal terminal with native inbox hooks](docs/usage.md#claude-code) |
| Codex | [Lifecycle hooks and app-server attachment](docs/usage.md#codex) |
| Herdr | [Optional plugin and verified pane binding](docs/usage.md#optional-herdr-integration) |

Mail keeps durable tasks, messages and bounded delivery retries. Your runtime owns
agent execution and permissions; your workflow owns review and acceptance rules.
Manual CLI use needs no running service or agent runtime.

```sh
agent-mail status
agent-mail status --check worker
```

## Herdr plugin (optional)

```sh
herdr plugin install youssef-tharwat/agent-mail
```

The plugin downloads a verified release binary; it does not require Cargo.
Install the required agent skill above for the clients running inside Herdr too.
[Setup and binding](docs/usage.md#optional-herdr-integration).

## Documentation and help

- [User guide](docs/usage.md): commands, runtime setup, delivery controls and upgrades.
- [Architecture](docs/ARCHITECTURE.md): ownership and delivery guarantees.
- [CLI design](docs/CLI_REDESIGN.md): defaults and workflow decisions.
- [Live runtime validation](docs/native-inbox-acceptance.md).
- [Issues](https://github.com/youssef-tharwat/agent-mail/issues): bugs and feature requests.

v0.4 changes command names and hook entry points. Existing users should follow
[the upgrade guide](docs/usage.md#upgrading) before updating their store.

## Contributing

See [development setup and checks](docs/usage.md#development). Keep changes focused
and add regression coverage for behavior changes. Discuss larger changes in an
issue first. [Implementation plan](docs/IMPLEMENTATION_PLAN.md).

Maintained by [Youssef Tharwat](https://github.com/youssef-tharwat).
Licensed under [MIT](LICENSE).
