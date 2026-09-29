# Agent Mail v0.3.0

Local coordination that survives context resets and worker restarts.

- Durable change notifications and atomic `work decide` operations.
- Bounded lifecycle recovery and native Codex idle delivery.
- Resumable `watch` stream over a private Unix socket; SQLite replay handles
  missed hints and reconnects without agent polling.
- Actionable wake routing and active cancellation. The isolated Codex handoff
  completed in four turns instead of eight, with both submissions resolved.
- `doctor` setup checks and `status.attention` for unresolved work, explicit
  deadlines and delivery problems.

## Upgrade

Stop Mail workers and other commands, back up the state directory, install the
new binary, then run `agent-mail setup` for the existing store to migrate to
schema 9. Restart the worker. Older binaries cannot open the upgraded schema.
Keep the backup if rollback is needed. Do not run old and new workers together.

Native delivery was live-tested on Codex 0.157.0. Claude hook support has protocol
fixtures only; standalone Claude idle delivery and ACP are deferred. Herdr remains
optional. See the user guide for setup and the acceptance report for test limits.
