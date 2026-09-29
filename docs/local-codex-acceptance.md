# Local Codex acceptance — 2026-09-29

## Scope

Agent Mail 0.3 source, schema 8; installed Codex 0.157.0 app-server on macOS.
Two real Codex threads on a dedicated local Unix socket. Separate standalone
Mail credentials and a disposable database/workspace. Existing user Mail data,
Herdr sessions, and hook configuration were untouched.

The operator created the threads, attached their Mail endpoints, and assigned
one work record. Each thread received its role and command wrapper at creation.
The worker could submit evidence; the reviewer owned register decisions. No
handoff prompts were manually sent. The queue adapter supplied each state update.

## Observed workflow

1. Assignment woke the worker and reviewer. Reviewer waited for evidence.
2. Worker wrote `initial result` and sent `submission1`.
3. Reviewer inspected the file, atomically resolved the submission and requested
   a correction, producing work version 2.
4. Worker appended `regression passed` and sent `submission2`.
5. Reviewer verified the file and atomically accepted version 3, resolving the
   second submission and recording an evidence pointer and accepted revision.
6. Final notifications were delivered; both native threads returned to idle.
   Further scanning reported both notification cursors caught up.

Every scan used a newly started `agent-mail service run --once` process. Agents
used neither `context` nor notification polling. Final database facts:

- Two messages, both deliveries resolved.
- Two keyed writer decisions: correction and acceptance.
- Work version 3, `open=false`, state `accepted`.
- Twelve coordination events; both recipients' cursors reached their latest event.
- Both wake attempt counters returned to zero.

## Cost and failure behavior

The final run completed in 17 worker scans and eight model turns, including
notification-only turns. Mail payload sizes were 723, 723, 904, 784, 841, 1026,
736, and 623 UTF-8 bytes: 6,360 bytes total. This measures added Mail text, not
model tokens, base instructions, tool output, or total inference cost.

An earlier run saw a queue-response timeout although Codex received the update.
The durable attempt remained visible. That exposed a delay for newer changes
behind an older retry cooldown; the implementation and regression test now allow
a fresh change through immediately. The final repeat had no queue timeouts.

A separate real API probe demonstrated that repeating `clientUserMessageId`
creates duplicate queued submissions. The adapter therefore claims bounded
at-least-once attempts, not exactly-once wake or execution. Business send/decision
keys remain idempotent. A queue receipt never accepts work.

## Automated gates

- `cargo fmt --all --check`
- `cargo clippy --locked --all-targets --all-features -- -D warnings`
- `cargo test --locked --all-features`: 37 tests passed.
- Targeted final regression: `cargo test --locked --test codex_wake` passed.

Transport tests cover busy-thread holding, pause, persisted delivery cursors,
worker restart, repeated attachment, binding replacement, lost receipts, retry
exhaustion/rearm, new changes during cooldown, and detachment. Existing Herdr,
standalone, migration, hook, coordination, and relay tests still pass.

## Compatibility boundary

Codex's queue API is experimental. The adapter requires a running app-server and
a loaded persistent thread; Mail does not own runtime startup or resume. Trusted
lifecycle hooks remain necessary for recovery inside a client lifecycle. Prior
live testing verified Codex 0.157 prompt/resume recovery and PostCompact
invalidation followed by next-boundary recovery. Immediate mid-turn reinjection
is not promised. Claude hooks are protocol-tested only; there is no Claude idle
wake adapter in this release. SSH acceptance is outside this local test.
