# Minimal continuation

Unfinished tasks remain pending until an explicit task decision settles them.
A status reply, delivery receipt, or turn ending cannot complete work. The existing
service reconciles durable follow-up deadlines, dispatches bounded reminders, and
escalates unresolved work to the named task writer (or request sender for mail).

## Scope cut

The Foundation and Core branches remain intact as reference. This replacement is
built on `main`, without merging either PR. It removes runtime turn subscriptions,
input/offer correlation, offer persistence, completion replay, and turn receipt
status. Task contracts, execution graphs, managed runtime creation, artifact
custody, cost accounting, and decision supervision from the proposed stack are
not imported. Existing released task, record, artifact, and coordination features
remain compatible.

Migration history is unchanged. The old turn-offer tables are unused; removing
previously shipped migrations would break upgrades. There are no new migrations.

## Continuation behavior

- Existing task retrieval schedules a follow-up; a checkpoint can record a concrete
  next action and check time within the existing escalation boundary.
- The service checks persisted deadlines even when no new messages arrive. Restart
  recovery uses the same schedule and bounded periodic scan.
- Two unattended reminder opportunities lead to escalation to the task writer.
  Exhausted delivery attempts or the hard deadline also escalate.
- Blocked and review tasks go to their decision owner without worker reminders,
  including when an active checkpoint or satisfied dependency exists. A checkpoint
  never grants approval or changes the task state.
- A per-participant OS lock serializes external wakes and verification prompts.
  It is released automatically on cancellation or process exit. Durable reservations
  are written before delivery; fresh events cannot bypass an unconfirmed native
  attempt's retry delay.
- Native status exposes `delivery_unconfirmed` and a diagnostic when a reservation
  has no confirmed receipt. That evidence survives restart. Worker observations
  provide current transport errors and timeouts. Queue acceptance remains separate
  from execution and task completion.

An existing worker must be running for scheduled dispatch; persisted deadlines
survive downtime and are reconciled when it starts. Group pause, observe mode,
and delivery controls remain respected.

## Executable acceptance evidence

Run:

```sh
RUSTC_WRAPPER= cargo test --locked --test codex_wake --test followup
```

`partial_progress_resumes_after_turn_end_and_worker_restart_without_new_messages`
uses the production service executable and a local Codex protocol fixture. It
accepts the initial task notification, records partial progress and a next action,
ends the turn, closes the store, and starts a new service process. Without a new
incoming message or lifecycle subscription, that process delivers the due
continuation to the same runtime. The task remains active at its original revision.

`turn_boundaries_leave_task_deadlines_pending_and_restart_recovers_them` also
records a status reply, checks successful/failed/repeated turn endings, reopens the
store, and verifies reminders and eventual escalation to the named writer. Duplicate
scans do not duplicate attention.

`duplicate_and_fresh_wakes_preserve_uncertainty_and_escalate_to_coordinator` runs
concurrent scans, loses queue receipts, introduces fresh work during the reserved
retry delay, and reopens the store. It verifies one initial wake, persistent
uncertainty, bounded retries, and escalation of both tasks to the coordinator.

`holds_with_active_checkpoints_escalate_to_writer_without_worker_reminders`
verifies that blocked and review states survive due checkpoints and escalation.
Existing pause, dependency, notification, migration, and runtime tests provide
additional regression coverage.

These tests prove service scheduling, persistence, bounded dispatch, and routing
against a runtime fixture. They do not claim that a live model completes the
remaining task or that a human reads an escalation.

Validation on this change: `cargo fmt --all --check`,
`RUSTC_WRAPPER= cargo clippy --locked --all-targets --all-features -- -D warnings`,
and `RUSTC_WRAPPER= cargo test --locked --all-features` passed on macOS. The full
suite retains one previously ignored test.

Live Codex acceptance subsequently passed, including restart, preserved approval
holds, and actual coordinator receipt. See [Live acceptance](MINIMAL_CONTINUATION_LIVE_ACCEPTANCE.md).
