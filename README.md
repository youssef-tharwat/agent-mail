# Agent Mail

**Durable tasks and messages for coding agents, on your machine.**

Keep assignments, next actions, evidence and unanswered requests across context
resets and restarts. Agent Mail stores coordination state in local SQLite and
supplies current context through Claude Code, Codex or Herdr integrations.

One native CLI. No account or hosted service. Herdr and Fleet Campaign are optional.

![Agent Mail: assign a task, recover after a reset, send a result, and accept it.](assets/agent-mail-demo.gif)

## Install

Install the CLI (the required agent skill is bundled):

```sh
brew install youssef-tharwat/tap/agent-mail
agent-mail --skill
```

Prebuilt macOS and Linux binaries for ARM64 and x86-64. **No Cargo or Rust compiler
required.** No Node/npm needed. Startup hooks supply the version-matched
[skill](skills/agent-mail/SKILL.md); `--skill` prints it for other integrations.
[Installation details](docs/usage.md#install).

## Quick start

Create a group and two agents:

```sh
agent-mail init project
agent-mail agent add coordinator
agent-mail agent add worker
```

Launch each agent in its own terminal:

```sh
agent-mail run coordinator -- codex
agent-mail run worker -- claude
```

Mail supplies identity and recovery hooks automatically. Review the generated
Codex hooks when prompted. The required skill teaches agents the commands below.

Inside the coordinator’s session, assign a task:

```sh
agent-mail task create api-review "Review API changes at abc123" --owner worker
```

Inside the worker’s session, recover the assignment and send a result:

```sh
agent-mail context
agent-mail mail send coordinator "Reviewed abc123; evidence: reviews/api.md" \
  --task api-review --key api-review-result-v1
```

The group is inferred from the credential. Task changes notify the relevant
agents automatically. The writer decides whether to accept the result;
receiving a message never marks a task complete.
[Replies and task decisions](docs/usage.md#atomic-decisions).

## Connect your agents

Run the delivery worker in another terminal for automatic idle wake:

```sh
agent-mail service run
```

| Runtime | Setup |
| --- | --- |
| Claude Code | [Normal terminal with native inbox hooks](docs/usage.md#claude-code) |
| Codex | [Managed local socket and lifecycle hooks](docs/usage.md#codex) |
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
For Herdr clients, install the discoverable skill or load `agent-mail --skill`
at session startup; the instructions remain required.
[Setup and binding](docs/usage.md#optional-herdr-integration).

## Documentation and help

- [User guide](docs/usage.md): commands, runtime setup, delivery controls and upgrades.
- [Architecture](docs/ARCHITECTURE.md): ownership and delivery guarantees.
- [CLI design](docs/CLI_REDESIGN.md): defaults and workflow decisions.
- [Live runtime validation](docs/native-inbox-acceptance.md).
- [Issues](https://github.com/youssef-tharwat/agent-mail/issues): bugs and feature requests.

v0.6 bundles the operating skill and derives task closure from typed states.
Existing users should follow
[the upgrade guide](docs/usage.md#upgrading) before updating their store.

## Contributing

See [development setup and checks](docs/usage.md#development). Keep changes focused
and add regression coverage for behavior changes. Discuss larger changes in an
issue first. [Implementation plan](docs/IMPLEMENTATION_PLAN.md).

Maintained by [Youssef Tharwat](https://github.com/youssef-tharwat).
Licensed under [MIT](LICENSE).
