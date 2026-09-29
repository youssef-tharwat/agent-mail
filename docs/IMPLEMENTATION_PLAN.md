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
- [ ] Commit and require Linux/macOS CI to pass.
- [ ] Build and smoke-test macOS ARM64/x86-64 and Linux ARM64/x86-64 archives;
  publish binaries and source with matching v0.3.0 instructions.

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
