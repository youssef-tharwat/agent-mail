-- Native Claude inbox metadata. Tokens never appear in status output.
CREATE TABLE claude_inboxes (
 recipient INTEGER PRIMARY KEY REFERENCES runtime_wakes(recipient) ON DELETE CASCADE,
 token TEXT NOT NULL,
 socket_identity TEXT NOT NULL,
 activity TEXT NOT NULL CHECK(activity IN ('idle','active','ended')),
 pending_id TEXT,
 pending_event INTEGER NOT NULL DEFAULT 0
);
PRAGMA user_version=11;
