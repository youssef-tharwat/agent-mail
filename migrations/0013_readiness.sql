CREATE TABLE runtime_readiness (
    recipient INTEGER PRIMARY KEY REFERENCES mailboxes(id) ON DELETE CASCADE,
    binding_version INTEGER NOT NULL,
    launch TEXT NOT NULL,
    runtime TEXT NOT NULL,
    client_session TEXT,
    last_hook TEXT,
    observed_at INTEGER
);
PRAGMA user_version = 13;
