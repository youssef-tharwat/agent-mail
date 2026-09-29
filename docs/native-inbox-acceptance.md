# Native Claude inbox acceptance

Tested locally on 2026-09-29 with Claude Code 2.1.284 and the schema 11 source.
This documents source validation; it does not announce a published binary.

## Live checks

| Check | Observed result |
| --- | --- |
| Ordinary interactive Claude terminal | Mail woke the normal terminal; the assistant replied `INTERACTIVE_RECEIVED`; the automatic hook advanced the delivery cursor. No custom terminal client. |
| Claude worker and Claude reviewer | Submission, correction, resubmission and acceptance completed at work version 3; both linked requests resolved. |
| Claude worker and Codex reviewer | The same correction and acceptance flow reached version 3; both requests resolved. An interrupted Claude response recovered on resume. |
| Resume and compaction | Startup reattached the resumed session; after real compaction, both live flows recovered work version 3. |
| Active cancellation | Native priority-now input interrupted a response early and produced the requested cancellation response. |
| Inbound refusal | With `crossSessionInbound: refuse`, the inbox input produced neither a response nor a matching receipt hook. |

Fixtures used isolated Mail stores and per-launch Claude settings. No production
Mail state or global Claude hook settings were changed. The streaming test driver
was only an acceptance harness, not a new shipped runtime client.

The interactive terminal harness timed out because ANSI cursor movement split its
expected response text. The exact assistant response was independently verified
in that fixture's session transcript, alongside its SQLite delivery receipt.
The mixed-runtime harness resumed a worker mid-response; that run demonstrates
recovery, not a clean turn-count or token-efficiency comparison.

## Automated regression coverage

Tests establish that socket writes alone do not acknowledge events; unrelated,
duplicate and wrong-session hooks cannot forge receipts; cancellation uses native
priority-now input; lost hooks exhaust a bounded retry budget; same-session resume
preserves that budget; identity rotation invalidates old credentials; replaced
socket files are rejected; and authenticated exit works after socket removal.
Migration coverage includes schema 10 upgrades with existing cursors and attempts.

## Protocol and limits

Claude exports its private inbox endpoint and token to lifecycle hooks. Mail sends
native JSON input through that socket. The socket returns no acknowledgment, and
Claude does not preserve the submitted input UUID as the hook prompt ID. Mail
therefore correlates the exact submitted opaque marker with its pending record.
The matching hook supplies fresh state and records admission, never acceptance of
work or proof the model completed a turn. Recovery remains based on durable state.

Activity is observed through lifecycle hooks. Disabled or broken hooks leave
unconfirmed delivery visible and retries bounded. Setup requires a Claude version
that exports the documented messaging environment; older versions fail visibly.
These checks cover the local flow, not remote SSH acceptance.

Sources: [Claude cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging)
and [lifecycle hooks](https://code.claude.com/docs/en/hooks).


## Final CLI validation

The v0.4 command redesign was exercised with a fresh isolated Claude worker and
Codex reviewer. Using `task create/update` and `mail send`, they completed initial
submission, correction, resubmission and acceptance at task version 3. Both linked
requests resolved. The Claude worker then resumed and compacted; recovery reported
the accepted task at version 3 with no remaining open assignment. No fixture
permission requests were denied. The test used native delivery and generated
`adapter claude-hook` settings, not manual context polling by either agent.
