# Agent Mail

Agent Mail keeps short agent messages and small work records in SQLite. After a
context reset, an agent runs one `context` command to find its current work,
next action, pending mail, and sync backlog. Message bodies and full work
records are fetched by ID. It is a standalone CLI for coding agents: no account,
server, or agent runtime required. Herdr is an optional integration.
Fleet Campaign is one possible workflow using it.

![Agent Mail CLI: send a request, recover work and pending mail, then resolve the delivery](assets/agent-mail-demo.gif)

CLI demo with isolated test data. [Terminal recording](assets/agent-mail-demo.cast).

This is an early public build. The local flow is tested; a real two-machine
SSH smoke test remains open. The [architecture](ARCHITECTURE.md) defines ownership
and guarantees; the [implementation plan](IMPLEMENTATION_PLAN.md) records
remaining release checks.

## Install

Download a prebuilt binary from [GitHub Releases](https://github.com/youssef-tharwat/agent-mail/releases/latest).
No Rust toolchain or Herdr installation is required.

| Platform | Archive target |
| --- | --- |
| macOS, Apple Silicon | `aarch64-apple-darwin` |
| macOS, Intel | `x86_64-apple-darwin` |
| Linux, x86-64 (glibc 2.35+) | `x86_64-unknown-linux-gnu` |
| Linux, ARM64 (glibc 2.35+) | `aarch64-unknown-linux-gnu` |

For example, on Apple Silicon:

```sh
version=0.2.0
target=aarch64-apple-darwin
archive="agent-mail-v${version}-${target}.tar.gz"
base="https://github.com/youssef-tharwat/agent-mail/releases/download/v${version}"
curl -fLO "$base/$archive"
curl -fLO "$base/$archive.sha256"
shasum -a 256 -c "$archive.sha256"
tar -xzf "$archive" agent-mail
mkdir -p "$HOME/.local/bin"
install -m 755 agent-mail "$HOME/.local/bin/agent-mail"
```

Add `~/.local/bin` to your `PATH`. On Linux, `sha256sum -c` also verifies the
checksum. macOS binaries are not Apple-notarized.

Or build from source with Rust 1.85+:

```sh
cargo install --git https://github.com/youssef-tharwat/agent-mail --tag v0.2.0 --locked
```

## Quick start

```sh
agent-mail setup --standalone --group project
agent-mail register --group project --name coordinator
agent-mail register --group project --name worker
```

Each registration returns a generated `session` credential. The operator or
launcher gives each agent its own credential through `AGENT_MAIL_SESSION`.
Set this in the worker's environment, using the worker's returned value:

```sh
export AGENT_MAIL_SESSION='<worker session>'
agent-mail context --group project
```

In the coordinator's environment, use the coordinator's credential:

```sh
export AGENT_MAIL_SESSION='<coordinator session>'
agent-mail work create --group project --id api-review --owner worker \
  --scope 'Review API changes' --next-action 'Review abc123'
agent-mail send --group project --to worker --key review-1 \
  --summary 'Review abc123' --work-id api-review
```

Both paths use the same send, inbox, resolve, context, and work commands.
`--session <credential>` also works; an explicit credential selects standalone
identity even inside Herdr. Invalid credentials fail without falling back to
another identity. Leave it unset for the normal Herdr pane binding.

`agent-mail participants --group project` lists addresses and runtime bindings
without exposing credentials. This is a registration view; it does not infer
whether a worker is alive. Standalone agents check `context` at checkpoints.
No background worker is needed for local mail or work operations. When run,
the worker reports pending/overdue standalone mail and unknown availability;
it cannot wake a standalone agent or send an operator notification without a
runtime integration.

To replace a standalone session explicitly:

```sh
agent-mail register --group project --name worker --replace
```

Supply the new credential to the replacement session. The old credential stops
working; the mailbox ID, pending mail, work ownership, and history survive.
The same `--replace` rule applies when switching between standalone and Herdr
bindings. Configure a Herdr socket with `setup --group project --socket PATH`
before using `bind`. A registered remote route cannot be converted this way.

`setup` uses a supplied or inherited Herdr socket when present; otherwise it
creates standalone state. `--standalone` explicitly ignores an inherited socket.
It never detaches an existing Herdr group silently. Use `--state-dir PATH` or
`AGENT_MAIL_STATE_DIR` for an isolated store; both runtimes otherwise share the
normal state location.

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

Then ask a bound or registered agent to use `$agent-mail`, or let Codex select it for an
Agent Mail task. Other agent tools can read the same `SKILL.md` directly.

## Optional Herdr integration

Herdr supplies live agent identity, lifecycle observations, and wake hints.
Agent Mail keeps the durable participant registry, mail, and work records.

### Install the plugin

The Herdr plugin currently builds from source, requiring Rust 1.85+ and Herdr
0.9.1+. It installs the CLI into Cargo's bin directory:

```sh
herdr plugin install youssef-tharwat/agent-mail
# macOS: set up state and install the background worker
herdr plugin action invoke setup --plugin youssef-tharwat.agent-mail
# Linux: set up state; run the worker under your own supervisor
herdr plugin action invoke setup-linux --plugin youssef-tharwat.agent-mail
```

Setup does not bind agents. Run `herdr pane list`, then bind the intended native
sessions:

```sh
agent-mail bind --name coordinator --target YOUR_COORDINATOR_PANE
agent-mail bind --name worker --target YOUR_WORKER_PANE
```

For an existing named group, configure its socket with
`agent-mail setup --group project --socket "$HERDR_SOCKET_PATH"` and pass
`--group project` when binding. Leave `AGENT_MAIL_SESSION` unset inside bound
Herdr panes. The skill is installed separately using the command above.

### Local plugin development

```sh
cargo install --path . --locked
herdr plugin link . --enabled
```

On macOS, `agent-mail service install` installs a user launchd job; on Linux,
run `agent-mail service run` under your own process supervisor.
`service uninstall` preserves the database.

### Quiet prompts

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

## Upgrading

For an upgrade, stop the worker and other Mail commands, install the new binary,
rerun `setup` with the existing group and socket (or `--standalone`) to apply
migrations, then restart the worker. Schema 6 preserves existing mailbox IDs,
Herdr bindings, mail, work records, reminder budgets, and remote routes. Older
binaries cannot open the upgraded store; back it up before upgrading. On
macOS, use `service uninstall` before setup and `service install` afterward.

## Mail and work commands

With your assigned session credential (or inside a bound Herdr pane):

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
- Bindings and standalone session credentials are workflow guards for agents
  sharing an OS account, not a
  security boundary against malicious same-user code.

## Development

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

License: MIT.
