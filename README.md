# Agent Mail

Durable messages and work records for coding agents. Keep assignments, next
actions, evidence, and pending requests across context resets and restarts.

One local CLI and SQLite database. No account or hosted service. Herdr is optional.

![Agent Mail CLI demo](assets/agent-mail-demo.gif)

## Install

Build the latest source with Rust 1.85+:

```sh
cargo install --git https://github.com/youssef-tharwat/agent-mail --locked
```

[Prebuilt binaries](https://github.com/youssef-tharwat/agent-mail/releases/latest)
are available for macOS and Linux.
[Binary installation and upgrades](docs/usage.md#install).

## Quick start

Create a group and register two agents:

```sh
agent-mail setup --standalone --group project
agent-mail register --group project --name coordinator
agent-mail register --group project --name worker
```

Each registration returns a `session` credential. Give each agent its own value
as `AGENT_MAIL_SESSION`.

In the coordinator's environment, assign work:

```sh
export AGENT_MAIL_SESSION='<coordinator session>'
agent-mail work create --group project --id api-review --owner worker \
  --scope 'Review API changes' --next-action 'Review revision abc123'
```

In the worker's environment, recover the assignment and submit a result:

```sh
export AGENT_MAIL_SESSION='<worker session>'
agent-mail context --group project
agent-mail send --group project --to coordinator --key api-review-result \
  --summary 'Reviewed abc123; evidence at reviews/api.md' --work-id api-review
```

Use a stable send key when retrying. Reading mail does not resolve it. The work
record's writer decides whether to accept the result.
[Commands and decisions](docs/usage.md#submit-one-work-decision).

## Automatic recovery

Lifecycle hooks supply current work after resets and when state changes. The
optional Codex adapter wakes idle sessions with a bounded summary. Agents do not
need to poll. Idle wake requires a running Mail worker and an attached Codex session.

[Set up hooks and Codex wake](docs/usage.md#automatic-recovery-and-change-notifications-v03).
Tested with two live Codex agents through submission, correction, and acceptance.
[Results and limits](docs/local-codex-acceptance.md).

Use `agent-mail doctor --group project --name worker` to check setup.
`agent-mail status` reports unresolved work and delivery issues.

## Agent skill

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

The [skill](skills/agent-mail/SKILL.md) teaches agents the Mail workflow.
It is installed separately from the CLI or Herdr plugin.

## Herdr integration

```sh
herdr plugin install youssef-tharwat/agent-mail
```

Herdr provides live sessions and lifecycle information. Agent Mail stores durable
mail and work state. [Setup and binding](docs/usage.md#optional-herdr-integration).

## Documentation

- [User guide](docs/usage.md)
- [Architecture and ownership](docs/ARCHITECTURE.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Improvements from testing](docs/next-steps.md)

MIT licensed.
