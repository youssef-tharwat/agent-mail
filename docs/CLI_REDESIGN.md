# CLI workflow redesign

Status: approved for implementation and release, 2026-09-29. The public noun is
`task` (the original review used `work`). Validation results are recorded in the
implementation plan before publication.

The user explicitly permits breaking CLI changes: do not retain old spellings,
hidden compatibility aliases or duplicate execution paths. Preserve stored mail,
work, identities and history through tested migrations.

## What the walkthrough found

Inspected the current command definitions, identity checks, work mutation paths,
operator guide and agent skill. Exercised the CLI with a disposable database;
no real agents or runtimes were changed.

- The main help exposes more than 30 commands, mixing agent work, operator setup,
  transport protocols and remote-machine configuration.
- `AGENT_MAIL_GROUP=project` is honored by hooks but ignored by `context`. With a
  valid agent credential, the latter reports an unknown session in the
  wrong group. This is a context-selection problem, not an authentication failure.
- Retrying identical `task create` fails with a raw SQLite uniqueness error.
- `task update` and `work decide` reach the same underlying mutation code, but
  only `decide` provides keyed retries and atomic linked-message resolution.
- `resolve` combines three different intentions: completion, reply and sender
  withdrawal. Even a short reply requires a temporary file and two reply flags.
- Setup can select Herdr merely because its socket environment is inherited.
- A number of work subcommands and options have no help descriptions.

The target is fewer concepts and fewer decisions per workflow, rather than a
smaller count achieved by putting unrelated operations behind flags.

## 1. Public command surface

```text
agent-mail init GROUP       create or verify a local coordination group
agent-mail context          recover my work and pending requests
agent-mail mail …           send, read, reply, resolve or withdraw requests
agent-mail task …           create, inspect or update work records
agent-mail agent …    operator: manage durable Mail identities
agent-mail runtime …        operator: configure delivery and recovery
agent-mail service …        operator: run or supervise the local worker
agent-mail status           inspect coordination and delivery health
```

Advanced sections document `remote …` and `adapter …`. They are discoverable
from help but do not compete with ordinary operations in the main command list.
No TUI, interactive wizard, output-format redesign or agent launcher is needed.
Bare `agent-mail` prints concise help and one setup example without mutating state.

### Full disposition of existing commands

| Current command | Proposed command / behavior | Decision |
| --- | --- | --- |
| `setup` | `init GROUP` | Local by default; runtime configuration is separate. |
| `register` | `agent add NAME` | Positional name; identity issued explicitly. |
| `register --replace` | `agent replace NAME` | Make credential invalidation a distinct operation. |
| `agents` | `agent list` | Durable registration view, not live-agent inventory. |
| `bind` | `agent bind NAME --herdr-pane PANE` | Keep verified binding explicit. |
| `context` | `context` | Keep the bounded recovery view. |
| `send` | `mail send RECIPIENT SUMMARY` | Keep stable key; additional recipients via repeated `--to`. |
| `inbox` | `mail list` | Pending request summaries only. |
| `inbox ID` | `mail show ID` | Distinguish detail retrieval from listing. |
| `resolve ID` | `mail resolve ID --note OUTCOME` | Resolve only; require the actual outcome. |
| `resolve --reply-*` | `mail reply ID TEXT` | Reply and resolve together in one transaction. |
| `resolve --withdraw` | `mail withdraw ID` | Separate sender authority and cancellation intent. |
| `task create` | `task create ID TASK --owner NAME` | Positional identity/task; safe creation retries. |
| `work show/list/history` | Same under `work` | Distinct read needs; add useful help. |
| `task update` + `work decide` | `task update ID` | One validated, idempotent atomic mutation; flags or JSON input. |
| `status` + `doctor` | `status` / `status --check [NAME]` | Shared report; optional active diagnostics. |
| `hooks-config` | `runtime configure claude\|codex --output PATH` | Explicit runtime; no implementation-specific `--claude-inbox` flag. |
| `attach-codex` | `runtime attach NAME codex --socket PATH --thread UUID` | Endpoint metadata stays explicit where it cannot be discovered reliably. |
| `attach-claude` | `runtime attach NAME claude-stream --socket PATH --session UUID` | Advanced stream client only; normal Claude attaches via hooks. |
| `detach-codex/claude` | `runtime detach NAME` | One endpoint operation; report what was detached. |
| `pause/resume` | `runtime pause/resume` | Group delivery controls, independent of the worker process. |
| `resume --rearm NAME` | `runtime retry NAME` | Reset a delivery budget only; never resumes a paused group. |
| `prompt-mode` | `runtime herdr-policy notify\|unguarded` | Keep unsafe prompt choice explicit and runtime-specific. |
| `service run/install/uninstall` | Same under `service` | One worker lifecycle; no new supervisor. |
| `machine-id/join/peer/auto-sync/route/sync` | Corresponding `remote …` operations | Relocate existing functionality; defer remote redesign. |
| `events/watch/ack` | `adapter events/watch/ack` | Programmatic consumers, not model bookkeeping. |
| `hook/claude-hook/restore/bridge/claude-bridge` | Corresponding `adapter …` entry points | Integrations generate/use these; operators rarely call them. |

Do not merge `context`, `mail list` and `work list`: they answer “what must I
resume?”, “which requests are pending?” and “what work exists?”. Make `context`
the default recovery instruction so agents do not call all three.

Do merge status and diagnostics at the command boundary, but retain the behavior
boundary: ordinary status reads state; `--check` also probes runtime endpoints.
Neither launches an agent, repairs configuration, resets retries or sends a prompt.
Plain status can succeed while reporting unhealthy coordination; `--check` exits
nonzero for failed checks. Unknown hook trust is reported as unknown.

## 2. Defaults and authority

### Group selection

Resolve the store first using the existing explicit state-dir/environment/locator
rules. Then use this group precedence consistently for every relevant command:

1. Explicit global `--group`, valid before or after subcommands.
2. `AGENT_MAIL_GROUP`.
3. The unique group associated with the authenticated standalone credential or
   verified Herdr identity in that store.
4. For operator commands without a supplied identity, the sole configured group.
5. Otherwise fail with candidate group names and a concrete selection example.

Validate an explicitly supplied credential even if there is only one group.
Invalid credentials or identity/group mismatches never fall back to an operator
or another runtime identity. Multiple matches are ambiguous, not “pick first”.
An explicit missing group fails. Read commands never create `default` or a database.
`init GROUP` requires a name and ignores ambient Herdr sockets. No persistent
“last used group”, repository configuration file or machine-wide active agent
is needed. Agent environments remain independent, including concurrent sessions.

### Participant identity

Keep one credential per standalone agent, injected by its operator/launcher.
Infer the sender from that credential; never add a convenient `--as worker` that
impersonates a agent. Group inference does not issue credentials.

Repeated `agent add NAME` reports that it already exists and does not
return or rotate its secret. Explicit replacement remains an operator action.
The runtime registry and Mail agent registry continue to have separate owners.

### Creation and mutation defaults

- `task create ID TASK --owner NAME`: require the owner. Set initial next action
  to TASK unless explicitly overridden; set scope to TASK, state to `open`, and
  leave deadlines/evidence/accepted revision absent. A distinct scope or first
  step can still be supplied. This default applies only on creation.
- Do not default a message recipient or infer an active work ID. Multiple open
  obligations are normal. Use optional `--work ID` for an explicit association.
- Accept short bodies/replies inline and `--body-file PATH` or `--body-file -`
  for larger input. Reject conflicting sources; bound stdin just like file input.
- Present durations as `--due-in 15m` and deadlines as documented UTC timestamps,
  rather than raw seconds. New messages have no business deadline unless supplied.
  This does not remove bounded initial delivery/retry or invent deadline follow-up
  turns; review legacy reminder eligibility before changing deadline storage.
- Page sizes, recovery caps and transport retry intervals remain internal defaults.
  Pagination cursors remain available when a response says there is more.
- Keep `--version` and `--reason` on work changes. Never silently fetch the latest
  version and overwrite a decision based on stale context.
- Business state names stay workflow-defined. Do not add built-in `accept`,
  `review`, `approve` or Fleet-specific transitions merely to shorten commands.

## 3. One work mutation, with automatic retry identity

Merge `work decide` into `task update`. Both a small flag-based update and a JSON
update must construct the same typed operation and use the same transaction.
The optional linked-message resolution stays in that transaction.

Work creation already has a stable operation identity: group + work ID. Persist
its canonical initial request and result. An identical retry returns that original
result without reapplying defaults, notifying again or overwriting later edits.
Changed creation content for the same ID produces a clear conflict. Legacy records
without creation provenance must not be guessed equivalent to a new request.

For updates, actor + work ID + expected version identifies the attempted transition.
Persist canonical requested fields and the result under that identity. Identical
retries return the saved result, including after newer versions exist. Changed
payloads under the same committed identity conflict. Failed transactions consume
no retry identity. Validate current actor authority before returning saved results.

This replaces an agent-invented update key with existing domain identity. It needs
a migration and store-level tests; it is not merely a Clap default. There must be
no separate unsafe flag-based update path after the merge.

A final reply has a natural identity too: actor + original message ID. It resolves
one recipient delivery once. Persist the canonical reply and result under that
identity; an identical retry returns the original result, changed content conflicts.
Remove the reply key from the public interface. Additional discussion while a
request stays open uses a separate send; `reply` explicitly means the final answer
and resolution. Resolving without a reply and later trying to reply must fail
clearly rather than silently discarding the new answer.

New outbound requests lack that natural identity: two identical requests may be
intentional. Keep a caller-supplied `--key` only on `mail send`. Neither random
keys on every attempt nor content hashes can make an uncertain retry safe. A
future typed application operation may derive its own key, but this CLI should
not pretend to know intent.

For complex updates, support `task update ID --file decision.json` and `--file -`.
The file contains expected version, reason, patch and optional linked message.
Reject mixing file input with mutation flags. Use JSON null/empty arrays for
clearing optional fields, rather than adding a clear flag for every field.
Common flags cover owner, state, next action, close/reopen and evidence; detailed
help documents the complete JSON schema. No generic string-based `--set` language.

## 4. Walkthrough of the proposed interface

### First use

```sh
brew install youssef-tharwat/tap/agent-mail
agent-mail init project
agent-mail agent add coordinator
agent-mail agent add worker
```

The operator uses `agent-mail run NAME -- CLIENT`; the launcher supplies identity.
When only this group exists, no repeated group flag is needed. Registration does
not start agents; `run` starts the selected client. Neither installs a service or
changes client permissions.

### Assignment and result

```sh
# Coordinator environment: its own AGENT_MAIL_SESSION is already set.
agent-mail task create api-review "Review API changes at abc123" --owner worker

# Worker receives the assignment through configured runtime integration.
# Manual recovery is available if needed:
agent-mail context
agent-mail mail send coordinator "Reviewed abc123; evidence: reviews/api.md" \
  --task api-review --key api-review-result-v1
```

Work creation publishes the assignment automatically. No duplicate assignment
message is required. Existing work state supplies what recovery needs.

### Correction, resubmission and acceptance

For a standalone question, `mail reply 12 "Please include retry coverage"`
answers and resolves it atomically. For a work decision, the writer
instead combines the state change and request resolution:

```sh
agent-mail task update api-review --version 1 \
  --next-action "Add retry coverage and resubmit" \
  --reason "Coverage missing" --resolve 12
```

The worker receives the changed next action automatically and sends the corrected
result with a new logical send key. The writer then records the authorized
acceptance with one update, using the version actually observed:

```json
{
  "version": 2,
  "reason": "Reviewed corrected evidence",
  "patch": {
    "state": "accepted",
    "open": false,
    "accepted_revision": "def456",
    "evidence": ["ci/run/42"]
  },
  "resolve_message": 13
}
```

```sh
agent-mail task update api-review --file acceptance.json
```

The IDs/revisions are illustrative. A reply does not itself decide work; a work
update does not resolve unrelated messages. Workflow policy supplies acceptance.

### Runtime setup and recovery

```sh
# Run once in the project; writes only the explicitly named settings file.
agent-mail runtime configure claude --output .claude/agent-mail-hooks.json

# Each agent launches normally in its assigned identity environment.
claude --settings .claude/agent-mail-hooks.json

# One Mail worker for the store, in another terminal:
agent-mail service run
```

Configure writes atomically, creates only the requested parent directory, and
refuses to overwrite a different existing file. Identical generation is a no-op.
It reports the native launch/trust step; it does not change global settings or
claim trust. Claude startup hooks register its endpoint automatically. Codex hook
configuration is equally simple, but attaching its app-server endpoint still needs
verified thread/socket metadata. Do not fake discovery or add a new agent launcher
for visual symmetry. Keep the same coordination guarantees across both runtimes.

Detach must be durable: `runtime detach NAME` disables automatic reattachment
until an explicit runtime attach/enable action. A later startup hook may recover
context but must not silently undo detach. Represent this intent in Mail state;
do not require the agent to remember to remove its hooks. Participant replacement
invalidates the old binding and delivery configuration as usual.

Resume/compaction recover obligations automatically. Agents do not call context
again when the supplied recovery view is sufficient, acknowledge events, or reset
retry budgets. Normal idle waiting requires no model polling.

### Troubleshooting

```sh
agent-mail status
agent-mail status --check worker
agent-mail runtime retry worker     # explicit after correcting the delivery issue
```

Report unresolved work, delivery failures and runtime observations separately.
Retry resets delivery attempts only; it does not redo tools or decide work.
Missing setup and ambiguous identities receive concrete fixes without printing
credentials, raw SQL, stack traces or suggested authority bypasses.

## 5. Installation and documentation

Prepare a personal `youssef-tharwat/homebrew-tap` formula selecting the four release
archives, with pinned SHA-256 hashes. Reuse release binaries; do not make consumers
build Rust. Document `brew upgrade youssef-tharwat/tap/agent-mail`, direct downloads
and contributor-only Cargo installation. Maintain one release version across
Cargo, the Herdr manifest, binaries and tap.

The user authorized publication after this CLI pass is validated. npm remains deferred. The Herdr installer must use the same verified release binary.

README should show purpose, install, one assignment/result example, runtime setup
links, help and contribution links. Keep this design and advanced commands in docs.
Update the bundled skill and generated hooks to the new names in the same change;
no compatibility aliases are required. Existing manual hook configurations need
explicit regeneration. Database migration must remain lossless.

## 6. Implementation order and evidence required

1. **Context and hierarchy:** global group resolver, new command tree, useful help,
   positional arguments, stable JSON output. Remove obsolete forms.
2. **Domain simplification:** merge work mutations; add creation/update retry
   identity and durable detach intent; split reply/resolve/withdraw; bounded stdin.
   Audit deadline/reminder behavior before making missing deadlines the default.
3. **Setup:** deterministic runtime config generation, matching skill/docs, status
   diagnostics, Homebrew formula and upgrade instructions.
4. **Acceptance:** run the workflows below, then revisit release publication.

Required checks:

- Single group works without flags; two groups plus a credential select correctly;
  ambiguous operator selection fails; an invalid credential never falls back.
- Explicit group/environment precedence is identical for hooks and CLI commands.
- Repeat creation/update after a simulated lost response: one mutation, one history
  entry/event set; changed replay conflicts; later edits are never overwritten.
- Work updates and linked resolution either both commit or both roll back.
- Repeating the same reply produces one response; changed replies conflict;
  resolve-then-reply fails explicitly. Reply/withdraw permissions remain distinct.
- Malformed or oversized stdin cannot partially mutate state; no-deadline
  messages still get bounded initial delivery.
- Detach survives runtime restart and hooks; explicit enable restores delivery.
- Fresh setup, normal resume, compaction and correction/acceptance work with Claude
  and Codex. Missing endpoints and exhausted retries give actionable diagnostics.
- A fresh Homebrew installation uses a verified release binary with no Rust
  toolchain; upgrades preserve state; all supported platform assets exist.
- Main help exposes common workflows; each public command has an example and
  meaningful option descriptions; shell/agent JSON consumers get no progress noise.

This is a command/workflow redesign. It does not add a hosted service, workflow
engine, runtime inventory, credential manager, custom terminal client or ACP layer.
