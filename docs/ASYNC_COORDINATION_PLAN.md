# Async coordination

Reuse the local durable event log and Unix socket stream. No broker, hosted service or new business states.

- `watch [--after CURSOR]`: start at the current event position; emit an initial scoped cursor and bounded JSON-line batches of changed mail/task IDs. Resume from the last handled cursor. Automatically reconnect after worker restarts; reject changed identities or cursors from another agent/store.
- `mail wait ID [--timeout 5m]`: sender-only observation of an outgoing request. Return on the first reply, when all recipients settle without a reply, or the earlier business deadline/timeout. Report reply IDs and unresolved recipients. Subscribe before rechecking; events drive reads. Never resolve requests or complete tasks.
- Runtime notifications: group at most five changed records under `new_mail`, `mail_updates` and `tasks`; include an overflow flag. Keep startup/reset context recovery separate. Native runtimes and Herdr use the same grouped JSON fields, within their transport limits.

Verification: replay and reconnect, cursor isolation and replacement identity, bounded grouping, resolution/reply/withdraw/deadline races, fanout and unchanged task/mail business state. Update README and bundled skill with supported flows.
