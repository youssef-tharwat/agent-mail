# Follow-through implementation evidence

This records the earlier rollout. For the reduced continuation implementation and
its current executable acceptance evidence, see [Minimal continuation](MINIMAL_CONTINUATION.md).

Implementation and live acceptance date: 2026-09-30. Baseline: `378f58b` plus the
checkout's existing automatic-upgrade changes. The authorized live rollout migrated
an existing campaign from schema 17 to 18 with a verified backup and worker handoff,
then enabled the default 900/3600-second follow-through policy. Private campaign
records and backup locations are kept outside this repository.

## Delivered

- Schema 18 adds source-bound attention plans, versioned checkpoints, history,
  due occurrences, and persisted operator alert budgets. Tasks and mail retain
  their existing authority and completion rules.
- The service reconciles bounded pages, preserves escalation age through task
  revisions, and routes unanswered work to its writer/sender and then the operator.
- Explicit waits use dependency checks and review times. A late dependency gets
  one owner notification while its existing escalation remains unresolved.
- CLI checkpoint/attention commands, source details, recovery, status, diagnostics,
  and the bundled agent guide expose the full handling cycle.
- Herdr idle/done clients with omitted launch metadata are eligible. Explicit
  unready, active, blocked, unknown, and launch-pending states retain their holds.
- Compact wake payloads retain retrieval and handling instructions. Event streams
  use protocol 2 and reject old subscribers explicitly; strict relay snapshots
  retain their existing shape.

## Automated evidence

Automated validation uses disposable stores, deterministic scheduler times, real
CLI child processes, and simulated Herdr/native transports. The separate live
exercise below used actual Codex clients. Neither establishes that a model will
correctly act on every prompt.

| Coverage | Evidence |
|---|---|
| Retrieve, ignore, remind twice, escalate, settle | `tests/followup.rs` |
| Sender source access, no false recipient receipt, exact self-retrieval | `tests/followup.rs` |
| Exhausted unread delivery never gains a new owner budget | `tests/followup.rs` |
| Checkpoint authority, versions, exact retries, unchanged reports | `tests/followup.rs` |
| Explicit hold, dependency cycles, reply-before-wait, late dependency | `tests/followup.rs` |
| Restart, revision/reassignment, bounded scans and accurate totals | `tests/followup.rs` |
| Failed/uncertain operator attempts, cooldown, route repair, self-escalation | `tests/followup.rs` |
| Observation, group pause, payload bounds and visible-only retrieval | `tests/followup.rs`, `tests/reliability.rs` |
| Manually started done client, explicit unready/blocked client | `tests/reliability.rs` |
| Native Codex/Claude delivery, receipts, cancellation, identity isolation | Existing native adapter suites |
| Schema upgrades, foreign keys, worker replacement and watch cursors | Migration, automatic-upgrade, stream and async-watch suites |
| Relay compatibility | `tests/relay.rs` |

Commands used on macOS:

```sh
cargo fmt --all --check
RUSTC_WRAPPER= cargo clippy --locked --all-targets --all-features -- -D warnings
RUSTC_WRAPPER= cargo test --locked --all-features
```

Result for v0.10.1: formatting and strict Clippy passed; **139 tests passed**,
including 21 follow-through tests and one documentation test. `git diff --check`
passed. The unpublished v0.10.0 candidate exposed two Linux fixture races: the
worker-crash test matched an outdated probe prefix, and the success notifier
exited without consuming its payload. The corrected fixtures preserve their
original reservation and delivery assertions.

`RUSTC_WRAPPER` is cleared because the local sccache wrapper could not operate in
the sandbox. Socket fixtures ran with sandbox escalation. Release CI repeats checks
on macOS and Linux; local validation alone is not evidence for those runners.

## Live acceptance

Two actual Codex clients were launched through `agent-mail run` using the v0.10.0
candidate in a disposable
directory and group, using workspace sandboxing and normal on-request approvals.
New hook trust was declined; each client loaded the candidate binary's guide during
bootstrap, and the managed launcher established its native endpoint independently.

With delivery paused, the coordinator created task `handoff` for the reviewer and
ended its turn. The isolated worker was terminated and restarted, then delivery
resumed. Without another manual prompt, the reviewer fetched the task and sent its
result; the idle coordinator woke, fetched the result, accepted task revision 2,
and atomically resolved the linked message. The accepted revision was
`acceptance-v010`. Acceptance occurred about 63 seconds after the restart snapshot.
Both clients acknowledged their delivery challenges on the first attempt.

For an approval-hold exercise, the reviewer received a blocked task and recorded
an external-wait checkpoint with a 60-second review time. The due condition
escalated to the coordinator. The coordinator fetched the occurrence and recorded
an audited writer extension, preserving the external wait. Task state remained
`blocked` at business version 1; checkpoint history separately records the reviewer
and writer reports. The reviewer did not resume implementation.

For failure injection, the coordinator deliberately ignored a separate task owned
by itself. It received the original notification, but did not fetch or checkpoint
the source. At the configured 240-second boundary, the service persisted a
self-escalation and invoked the independent operator capture executable once.
The capture contained the expected group and attention ID, and its state was
`accepted`. The task remained open at version 1 with no retrieval timestamp.
This proves notifier execution and durable escalation, not that a human read it.

An additional real Herdr campaign upgraded through the installed candidate. Its
coordinator, previously idle with zero wake attempts and expired verification,
received a notification, fetched current records, and became delivery-verified
after the diagnosed readiness issue was repaired and its expired check was rearmed
once. Existing work decisions remained with their original writers.

## Rollout

All groups start in observation mode. Build/install the changed binary, inspect
`status --json`, and use the policy example in [usage.md](usage.md#follow-through-after-delivery)
to enable an isolated group before enabling the intended campaign.

The defaults are initial operating values. Broader workload measurements of false
reminders and time to disposition should guide future policy tuning.

Remote snapshots and cross-machine waits report follow-through as unsupported.
Turning policy back to `observe` stops follow-up dispatch while retaining history;
it does not downgrade the schema or roll back business records.
