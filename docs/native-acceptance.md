# Native runtime acceptance

The next release shares Mail delivery policy between native Codex and Claude.
ACP is not involved. Tests use disposable projects, identities and databases.

## Observed

- Claude 2.1.284 advertises `msg_lifecycle_v1`. Native queued lifecycle events
  match submitted UUIDs; model results alone do not establish idle state.
- A native `priority: now` cancellation interrupted an active Claude response
  and produced the requested STOPPED response. This starts a new response,
  whereas Codex steers its existing turn.
- Claude/Claude completed initial submission, correction, resubmission and
  acceptance at work version 3, with two linked requests. The fixture also
  resumed the worker session. Seven model results include two startup READY
  turns and one resumed READY turn: four business turns.
- A fresh Mail worker process ran each reconciliation, exercising worker restart.
- The first mixed-runtime test caught a missing required Codex
  `clientUserMessageId`; restored it and strengthened the protocol fixture.

- The corrected mixed Claude worker / Codex reviewer flow completed at version 3,
  with both linked submissions resolved. It used four business turns: two Claude
  and two Codex, plus Claude startup, resume, compaction and recovery checks.
- Claude native resume preserved identity and supplied `SessionStart:resume`
  recovery. Manual `/compact` emitted a real `compact_boundary` and successful
  `SessionStart:compact` hook output. The following response reported task version
  3 and no open assignment without a tool call. This verifies this Claude path;
  Codex retains its documented next-boundary compaction fallback.
- The complete regression suite passed: 58 tests plus one documentation test, including native receipts,
  durable retries, identity replacement, active cancellation, stream backpressure,
  and populated upgrades from schema 9. This includes existing test coverage.

Local private fixtures: `am-native-claude-6t2i98w7` and
`am-native-mixed-mmxuufio`. Raw traces and participant credentials are not published.
The initial mixed runs failed because of a missing required Codex request field
and then a READY-only fixture instruction; neither is counted as a passing run.

## Remaining release gates

Formatting, strict Clippy and the integrated regression suite passed locally.
Source is versioned 0.4.0. CI and binary publication remain pending; the checkout
also contains concurrent maintenance changes, which are preserved.
Do not treat these observations as proof that arbitrary Claude terminal sessions
can be attached, or that all client versions support the same compaction boundary.
