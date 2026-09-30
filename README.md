# Agent Mail

**Durable tasks and messages for coding agents, on your machine.**

Keep assignments, decisions, evidence and unanswered requests across context resets.
Agent Mail stores coordination in SQLite, restores relevant context and wakes connected
agents when work arrives. Works with **Claude Code and Codex**. No account or hosted service.

![Assign a task, recover after a reset, send a result, and accept it.](assets/agent-mail-demo.gif)

## Install

```sh
brew install youssef-tharwat/tap/agent-mail
```

Prebuilt binaries for macOS and Linux, ARM64 and x86-64. No Rust compiler needed.
[Other installation options](docs/usage.md#install).

## Start

Create one isolated group for this project or fleet. This adds a group to the
existing local store; it does not join or change another group's agents, tasks or
inbox. Use the same group name for its agents:

```sh
agent-mail init my-project
```

Launch each agent in its own terminal:

```sh
agent-mail --group my-project run coordinator -- codex
agent-mail --group my-project run worker -- claude
```

`run` creates a missing identity, starts the shared delivery worker when needed,
and supplies the agent skill and recovery hooks. Existing identities are preserved;
retired agents require explicit restoration. Review the client's normal hook trust prompts.

## Coordinate

Ask the coordinator to assign work, or run inside its session:

```sh
agent-mail task create api-review "Review the API changes" --owner worker
```

The worker receives the assignment, does the work, and reports to the coordinator:

```sh
agent-mail mail send coordinator "Review complete; evidence: reviews/api.md" \
  --task api-review --key api-review-result
```

The task's creator is its **writer** and records decisions. Its **owner** does the
work and reports results. Task changes publish notifications automatically. Reading or delivering a
message never accepts a task.

Follow changes without polling, then fetch only the changed records:

```sh
agent-mail watch
agent-mail mail show 42
agent-mail task show api-review
```

`watch` starts from the current position and prints a resumable cursor with each
small grouped batch. Save the latest cursor after handling its batch and resume
with `agent-mail watch --after CURSOR`. `mail wait ID` returns on the first reply, when all recipients resolve without
a reply, or at the request deadline. `--timeout 5m` sets an earlier limit. Waiting
never resolves the request or changes its task.
[Assignment, review and acceptance](docs/agent-guide.md#assignment--result--decision).

## Check delivery

```sh
agent-mail status
```

Example:

```text
Group: project
Delivery worker: running

AGENT        DELIVERY
coordinator  Ready
worker       Verifying · retry in 42s
```

**Ready** requires an explicit agent acknowledgment of a delivery check and a
healthy current connection. A saved message or successful socket write is not enough.
Unavailable recipients keep their pending work. After fixing a delivery problem:

```sh
agent-mail status --check worker
agent-mail agent retry worker
```

Retries are bounded. They never change identity, clear a pause, or complete work.
Use `status --json` for full structured evidence. [Delivery details](docs/usage.md#delivery-verification-08).

## Commands

| Category | Commands | Purpose |
|---|---|---|
| Start | `init`, `run` | Create a group and launch agents. |
| Coordinate | `context`, `task`, `mail`, `watch` | Recover assignments, manage tasks, exchange requests and follow changes. |
| Manage | `status`, `agent` | Check delivery, retry, or manage registrations. |
| Integrations | `runtime`, `service` | Configure manual integrations and worker supervision. |

Use `agent-mail COMMAND --help` for options. Task/mail operations return JSON.
Groups have separate agents, tasks and inboxes; select one with `--group NAME` when
needed. They share one local store and worker. `status --all-groups` shows the operator overview.

## Agent skill

The [operating guide](docs/agent-guide.md) teaches recovery, ownership, blockers,
reviews, acceptance and safe retries. Managed launches supply it automatically.

Install with [skills.sh](https://skills.sh/docs):

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

The installed skill loads `agent-mail --skill`, so its operating instructions follow
the binary on PATH when next invoked. skills.sh manages installation and loader
updates. [Installation and update details](docs/usage.md#agent-skill).

## Herdr (optional)

```sh
herdr plugin install youssef-tharwat/agent-mail
```

Herdr is optional: Mail works with standalone managed launches. Binding an existing
Herdr pane requires Herdr's agent integration to report its native session identity;
the Agent Mail Herdr plugin only handles delivery. [Herdr setup and prompt policy](docs/usage.md#optional-herdr-integration).
Fleet Campaign is also optional; your workflow defines review and acceptance rules.

## More

[User guide](docs/usage.md) · [Architecture](docs/ARCHITECTURE.md) ·
[Implementation plan](docs/IMPLEMENTATION_PLAN.md) · [Development](docs/usage.md#development) ·
[Issues](https://github.com/youssef-tharwat/agent-mail/issues)

Maintained by [Youssef Tharwat](https://github.com/youssef-tharwat). [MIT](LICENSE).
