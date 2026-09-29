CREATE TABLE codex_wakes (
 recipient INTEGER PRIMARY KEY REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 socket TEXT NOT NULL,
 thread TEXT NOT NULL,
 delivered INTEGER NOT NULL DEFAULT 0,
 attempted INTEGER NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt INTEGER NOT NULL DEFAULT 0,
 UNIQUE(socket, thread)
);
PRAGMA user_version=8;
