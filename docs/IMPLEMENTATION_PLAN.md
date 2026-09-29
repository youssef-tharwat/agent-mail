# Implementation plan

## Scope decision — 2026-09-29

Ship the local coordination improvements in v0.3.0. ACP and standalone Claude
idle delivery are deferred at the user's request. Herdr stays optional.

One Rust binary, SQLite, and the existing worker. Mail owns durable identities,
mail, work records, events and delivery bookkeeping. Runtime integrations own
session interaction; the designated work writer owns decisions and acceptance.
An accepted notification never proves progress or completes work.

## Implemented and verified locally

- [x] Reconcile the draft: remove inferred waiting states and manual follow-up
  flags. No automatic deadline reminders or workflow engine.
- [x] Add schema 9 with separate wake/cancellation intent and a scan cursor.
  Passive classification never manufactures a delivery receipt.
- [x] Commit events before best-effort socket hints. Database writes succeed
  without a worker; missed hints reconcile from SQLite.
- [x] Add `watch`: private Unix socket, versioned NDJSON, scoped identity and
  binding generation, replay by cursor, bounded batches and slow-client timeout.
- [x] Wake Codex only for actionable obligations; steer active cancellation with
  an expected-turn precondition. Idle closure remains available for recovery.
- [x] Report observable attention facts: open work, explicit expired deadlines,
  missing endpoints, unconfirmed attempts and exhausted delivery budgets.
- [x] Add `doctor`: database, identity, worker, authenticated stream, native Codex
  capability/session probe and Herdr binding. Hook trust remains unknown without
  client evidence. Diagnostics do not start or resume agents.
- [x] Test replay/restart, missed hints, subscription races, binding replacement,
  recipient isolation, actual socket backpressure, cancellation and reassignment.
- [x] Verify upgrades from published schema 6 and schema 8, retaining retry state.
- [x] Run a real Codex/Codex assignment → submission → correction → resubmission
  → acceptance flow: **4 turns versus 8**, both requests resolved, accepted
  version 3. Injected payload: 3,793 bytes versus 6,360. These are not token counts.
  See [acceptance evidence](local-codex-acceptance.md).
- [x] Retain the earlier live Codex hook resume and compaction witness with its
  documented next-boundary fallback. Do not claim immediate compaction injection.
- [x] Remove ACP-only schema variants, runtime placeholders and stdio dependency.
  Retain the shared bounded recovery payload used by the native adapter.

## Release gates

- [x] Final formatting, Clippy (`--all-targets --all-features -- -D warnings`),
  and tests (`--locked --all-features`) on the narrowed implementation.
- [x] Update user guide, concise README, bundled skill and release notes.
- [x] Commit and require Linux/macOS CI to pass.
- [x] Build and smoke-test macOS ARM64/x86-64 and Linux ARM64/x86-64 archives;
  publish binaries and source with matching v0.3.0 instructions.

Release: [v0.3.0](https://github.com/youssef-tharwat/agent-mail/releases/tag/v0.3.0),
source commit `c1222ce`. [Linux/macOS CI](https://github.com/youssef-tharwat/agent-mail/actions/runs/36573011737)
and [four-platform release checks](https://github.com/youssef-tharwat/agent-mail/actions/runs/36573236160)
passed. All 46 tests passed. The downloaded Apple Silicon archive additionally
passed checksum verification, version reporting, fresh setup, status, and CLI help
checks in disposable local state. No user installation or database was replaced.

## Deferred

ACP integration, Claude/Claude and mixed-client handoffs, and remote SSH acceptance
are outside this release. Claude hook fixtures are not a live compatibility claim.
Standalone Claude idle delivery is unsupported.

The official Claude ACP adapter 0.84.0 completed a standalone ACP v1 session and
prompt probe. Codex ACP 2.0.0 was identified but has no Mail integration witness.
Neither finding justifies adding a second runtime layer now. A future ACP proposal
must show a concrete need, explicit session ownership, negotiated capabilities,
permission forwarding, recovery behavior and an intentional credential boundary.
No ACP launcher or automatic credential export ships in v0.3.


## Native Claude parity — next release, ACP independent

- [x] Share delivery policy, SQLite retry budgets, bounded context and diagnostics
  across native Codex and Claude transports.
- [x] Add an operator-owned Claude streaming bridge with bounded private socket
  requests, session checks and runtime lifecycle receipts. Forward approvals.
- [x] Preserve existing Codex request contracts and the `status.codex` view.
- [x] Add schema 10 without rewriting published migrations; preserve Codex state.
- [x] Run a live Claude/Claude submission, correction and acceptance flow.
- [x] Finish live mixed-runtime, resume and compaction acceptance.
- [x] Pass local formatting, strict Clippy and all tests (58 plus one doctest).
  Concurrent maintenance changes are preserved.
- [x] Version source and the plugin manifest as 0.4.0.
- [ ] Publish after integrated review and CI; binaries are still v0.3.0.

ACP and remote SSH acceptance remain deferred. The schema 10 streaming bridge
is complemented by native terminal inbox integration in schema 11 below.


## Simplify Claude setup with its native inbox (schema 11)

- [x] Probe the official per-session inbox in an isolated runtime. It accepts
  native user frames but returns no delivery acknowledgment.
- [x] Generate an opt-in Claude hook configuration that registers the exported
  endpoint automatically; preserve native UI and permission ownership.
- [x] Correlate automatic UserPromptSubmit receipts with opaque pending markers;
  inject fresh state there, never count a socket write as confirmed delivery.
- [x] Preserve bounded retries across same-session resume and reject stale or
  replaced identities and socket files. Exclude runtime tokens from diagnostics.
- [x] Verify a live Claude/Claude correction and acceptance flow, resume and compact.
- [x] Finish ordinary-terminal, mixed-runtime and refusal acceptance checks.
- [x] Pass formatting, full regression tests (61 plus one doctest) and strict
  Clippy on the final diff.

Keep the streaming bridge as an advanced integration. Do not build a terminal
client, approval UI, model-driven acknowledgment loop or ACP dependency.


## Release hold: CLI workflow redesign

The user paused v0.4.0 publication to simplify installation, defaults and the
command model. No backward CLI compatibility is required; stored data must survive.
The proposed keep/merge/automate/remove mapping and workflow acceptance criteria
are in [CLI redesign](CLI_REDESIGN.md). This is a design proposal, not implemented
behavior. Do not publish the release or tap until this pass is complete and the
release hold is lifted.


## Approved CLI and distribution pass

The user approved implementing the redesign and publishing afterward. Public
assignments are named `task`, not `work` or `job`. This approval lifts the earlier
release hold after validation. Schema 12 preserves data while adding task creation
provenance, durable runtime delivery policy and optional business deadlines.

- [x] Implement group inference and the grouped task/mail/runtime CLI without aliases.
- [x] Merge task mutations and derive retry identities; split reply/resolve/withdraw.
- [x] Persist detach across hooks and runtime restarts.
- [x] Generate runtime settings safely; align the README, user guide and skill.
- [x] Prepare binary installer, Herdr binary consumption and personal Homebrew tap.
- [x] Complete regression and live acceptance on the final interface: 66 tests
  and one doctest, strict Clippy, mixed Claude/Codex correction and acceptance,
  then Claude resume and compaction recovery.
- [ ] Publish v0.4.0 binaries and verify Homebrew/direct installation.
