# Agent Mail

Local coordination for **Codex and Claude Code**. Assign tasks, exchange messages,
and recover unfinished work after a context reset.

Agent Mail keeps tasks, decisions and pending requests in SQLite. Connected agents
receive small notifications and fetch details when needed. No account or hosted service.

![Assign work, recover context, report results, and record a decision.](assets/agent-mail-demo.gif)

## Install

Install the CLI and its agent skill:

```sh
brew install youssef-tharwat/tap/agent-mail
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

Prebuilt binaries support macOS and Linux on ARM64 and x86-64. No Rust compiler
needed. [Other installation options](docs/usage.md#install).

## Start a project

Create a group for your project or fleet:

```sh
agent-mail init my-project
```

Launch each agent in its own terminal, using that same group:

```sh
# Terminal 1
agent-mail --group my-project run coordinator -- codex

# Terminal 2
agent-mail --group my-project run worker -- claude
```

`run` registers the agent, supplies the skill and recovery hooks, and starts the
local delivery worker when needed. Follow the client's normal trust prompts.
Groups keep their agents, tasks and inboxes separate while sharing the local store.

## Assign work and get a result

Ask the coordinator to use Agent Mail, or run this inside its session:

```sh
agent-mail task create api-review "Review the API changes" --owner worker
```

The worker receives the assignment and reports its result:

```sh
agent-mail mail send coordinator "API review complete" \
  --task api-review --key api-review-result --body-file reviews/api.md
```

The coordinator records the next action or decision on the task. The creator owns
those decisions; the assigned worker does the work. Receiving a message never
marks a task complete. [Full assignment and review flow](docs/agent-guide.md#assignment--result--decision).

## Recover, follow, or wait

```sh
agent-mail context                  # Current assignments and pending requests
agent-mail watch                    # Stream changes; fetch details as needed
agent-mail mail wait 42 --timeout 5m # Wait for a request's reply or settlement
```

`watch` emits compact batches with a resume cursor. Continue with
`agent-mail watch --after CURSOR`. `mail wait` uses the event stream; it does not
poll or resolve the request. [Streaming and waiting](docs/agent-guide.md#follow-changes-and-wait-for-a-reply).

## Check delivery

```sh
agent-mail status
```

```text
coordinator  Ready
worker       Verifying · retry in 42s
```

**Ready** means the agent acknowledged a delivery check and its connection is
healthy. If delivery is unavailable, work stays pending. Use
`agent-mail status --check worker` to diagnose the cause, then
`agent-mail agent retry worker` after fixing it. [Delivery and repair](docs/usage.md#delivery-verification-08).

## Commands

| Purpose | Commands |
|---|---|
| Set up | `init`, `run` |
| Coordinate | `context`, `task`, `mail`, `watch` |
| Inspect and repair | `status`, `agent` |
| Configure integrations | `runtime`, `service` |

Use `agent-mail COMMAND --help` for options. Task and mail commands return JSON.
Use `--group NAME` outside a managed session when group selection is needed.

## Herdr integration

Herdr is optional. To coordinate agents already running in Herdr panes, install
its delivery plugin and follow the [Herdr setup guide](docs/usage.md#optional-herdr-integration):

```sh
herdr plugin install youssef-tharwat/agent-mail
```

Pane binding also requires Herdr's agent integration to report the native session
identity. The standalone launches above work without Herdr. Your workflow defines
review and acceptance rules; Fleet Campaign is optional too.

## Documentation

[Agent operating guide](docs/agent-guide.md) · [User guide](docs/usage.md) ·
[Architecture](docs/ARCHITECTURE.md) · [Development](docs/usage.md#development) ·
[Issues](https://github.com/youssef-tharwat/agent-mail/issues)

Update with `brew upgrade youssef-tharwat/tap/agent-mail`. The skill loads the
installed binary's instructions on its next invocation. [Skill updates](docs/usage.md#agent-skill).

Maintained by [Youssef Tharwat](https://github.com/youssef-tharwat). [MIT](LICENSE).
