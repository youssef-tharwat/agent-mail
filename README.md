# Agent Mail

Agent Mail keeps short agent messages and small work records in SQLite. After a
context reset, an agent runs one `context` command to find its current work,
next action, pending mail, and sync backlog. Message bodies and full work
records are fetched by ID. It is a general Herdr plugin; Fleet Campaign is one
possible workflow using it.

![Agent Mail: send a request, recover after a context reset, then reply and resolve](assets/agent-mail-demo.gif)

*Illustrated flow: send, recover, resolve.*

This is an early public build. The local flow is tested; a real two-machine
SSH smoke test remains open. The [architecture](ARCHITECTURE.md) defines ownership
and guarantees; the [implementation plan](IMPLEMENTATION_PLAN.md) records
remaining release checks.

## Agent skill

The optional [Agent Mail skill](skills/agent-mail/SKILL.md) teaches agents to
recover work and handle deliveries without loading whole conversations into
context. Herdr plugin installation does not install agent skills. Install it
separately with a skill manager:

```sh
npx skills add youssef-tharwat/agent-mail --skill agent-mail -g
```

For local development, link the skill from a stable checkout:

```sh
mkdir -p ~/.codex/skills
ln -s "$(pwd)/skills/agent-mail" ~/.codex/skills/agent-mail
```

Then ask a bound agent to use `$agent-mail`, or let Codex select it for an
Agent Mail task. Other agent tools can read the same `SKILL.md` directly.

## Install from GitHub

Requires Rust 1.85+ and Herdr 0.9.1+. Herdr builds the plugin and installs the
`agent-mail` CLI into Cargo's bin directory:

```sh
herdr plugin install youssef-tharwat/agent-mail
herdr plugin action invoke setup --plugin youssef-tharwat.agent-mail
```

The setup action creates the `default` group and installs the macOS worker (on
Linux, run `agent-mail service run` under your own supervisor). It does not bind
agents. Run `herdr pane list`, then bind each intended native agent session with
`agent-mail bind --name NAME --target PANE_ID`. The agent skill is installed
separately as described above.

## Build and link locally

Requires Rust 1.85+ and Herdr 0.9.1+. On each host:

```sh
cargo install --path . --locked
herdr plugin link . --enabled
agent-mail setup --group project --socket "$HERDR_SOCKET_PATH"
agent-mail bind --group project --name coordinator --target YOUR_COORDINATOR_PANE
agent-mail bind --group project --name worker --target YOUR_WORKER_PANE
agent-mail status
```

Run `setup` from a Herdr environment with `HERDR_SOCKET_PATH`, or pass an
absolute socket path. On macOS, `agent-mail service install` installs a user
launchd job; on Linux run `agent-mail service run` under your own process
supervisor. `setup`
does not bind or prompt agents. The plugin manifest also exposes setup and
status actions. `service uninstall` preserves the database.

For an upgrade, stop the worker, install the new binary, rerun `setup` with the
existing group and socket to apply migrations, then restart the worker. On
macOS, use `service uninstall` before setup and `service install` afterward.

Inside a bound Herdr agent pane:

```sh
agent-mail send --group project --to worker --key review-1 \
  --summary 'Review revision abc123' --body-file request.txt
agent-mail context --group project
agent-mail inbox --group project 1
agent-mail resolve --group project 1 --note handled
```

Reuse a send key for a retry with identical content. A successful send means
the local SQLite transaction committed. Reading does not resolve mail. An
agent resolves its delivery explicitly and may include `--reply-key` and
`--reply-file` to publish a reply in the same transaction.

Create a work record from the designated home writer:

```sh
agent-mail work create --group project --id lane-a --scope 'Implement feature' \
  --owner worker --state active --next-action 'Inspect contract'
agent-mail send --group project --to worker --key lane-a-review \
  --summary 'Review the contract' --work-id lane-a
agent-mail work show --group project lane-a
agent-mail work update --group project lane-a --version 1 --reason 'Revision submitted' \
  --state review --next-action 'Check evidence'
```

Work updates require the current version and a reason. The home writer decides
state, ownership, and acceptance. Replies do not change work state.

## Quiet prompts

Herdr's current socket API does not expose a reliable empty-draft check.
Automatic agent prompts are therefore **off by default**. Pending mail remains
in SQLite and `context`/`status`; the worker can raise one operator alert when
it becomes overdue. An operator who accepts the risk of overwriting an
unfinished agent draft may explicitly run:

```sh
agent-mail prompt-mode --group project --enable-unguarded
```

In that mode the worker sends only a short fixed inbox hint to a verified idle
agent, with at most one initial prompt and two reminders spaced five minutes
apart. It never inserts message bodies into prompts. `prompt-mode --disable`
returns to the safe default. `pause` and `resume` control a group's prompts.

## Optional remote machines

Each machine installs the binary and owns its own SQLite database. Configure
SSH aliases so the home can run `ssh ALIAS agent-mail bridge export` without
interactive input. Use the default state location on remote hosts; the bridge
reads the locator created by `setup`. There is no listening Mail port.

For a home host A and remote host B, run `setup` and `bind` for the local agents
on both hosts. Then use their `agent-mail machine-id` values:

```sh
# On B: point its group at A's authoritative work register.
agent-mail join --group project --home A_MACHINE_UUID
agent-mail route --group project --name coordinator --machine A_MACHINE_UUID

# On A: register B's logical inbox and an SSH alias for B.
agent-mail route --group project --name worker --machine B_MACHINE_UUID
agent-mail peer --machine B_MACHINE_UUID --ssh-target B_SSH_ALIAS
agent-mail sync --peer B_MACHINE_UUID
# Optional: authorize periodic sync by the home worker for this peer.
agent-mail auto-sync --peer B_MACHINE_UUID --enable
```

Run `sync` **on the home machine** after remote sends or whenever you want to
exchange queued events. Once enabled, the home worker also syncs the configured
peer about every 30 seconds. `auto-sync --peer B_MACHINE_UUID --disable` stops
future background transfers; an already running exchange may finish. Manual
`sync` still works. Adding the same SSH target
preserves the setting, while changing it disables automatic sync until the
operator opts in again. Automatic sync is off by default and requires a running
home worker. One installation must be the home for all its enrolled groups to
initiate sync.

The home forwards messages between remotes. Exchanged
events are committed before acknowledgment; a lost response causes replay,
which is deduplicated. While disconnected, local sends queue and local inboxes
remain readable. `status` shows the outbox count, oldest queued event, peer
sync time, auto-sync setting, and last error. Remote work snapshots show their
last transfer time as `synced_at` and remain
read-only; their version may be stale until another sync.

## Limits

- Named recipient fan-out is capped at 32. Message bodies are capped at 8 KiB;
  large evidence belongs in Git, CI, or artifacts referenced by path or ID.
- `context` returns at most five work summaries and five mail summaries per
  page within a 4 KiB budget. Cursors fetch further pages.
- Mail records obligations and decisions. It does not run tools, retry agent
  side effects, or decide whether a revision is correct.
- Binding checks are workflow guards for agents sharing an OS account, not a
  security boundary against malicious same-user code.

## Development

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

License: MIT.
