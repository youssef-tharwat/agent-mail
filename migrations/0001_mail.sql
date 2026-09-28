CREATE TABLE groups (
    name TEXT PRIMARY KEY NOT NULL,
    socket TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1))
);

CREATE TABLE mailboxes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL REFERENCES groups(name),
    name TEXT NOT NULL,
    pane TEXT NOT NULL,
    terminal TEXT NOT NULL,
    agent TEXT NOT NULL,
    session_kind TEXT NOT NULL,
    session_value TEXT NOT NULL,
    cwd TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_wake INTEGER NOT NULL DEFAULT 0,
    alerted INTEGER NOT NULL DEFAULT 0,
    UNIQUE(group_name, name),
    UNIQUE(group_name, pane)
);

CREATE TABLE messages (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    sender INTEGER NOT NULL REFERENCES mailboxes(id),
    dedup_key TEXT NOT NULL,
    canonical TEXT NOT NULL,
    summary TEXT NOT NULL,
    body TEXT NOT NULL,
    created INTEGER NOT NULL,
    due INTEGER NOT NULL,
    reply_to INTEGER REFERENCES messages(id),
    UNIQUE(sender, dedup_key)
);

CREATE TABLE deliveries (
    message INTEGER NOT NULL REFERENCES messages(id),
    recipient INTEGER NOT NULL REFERENCES mailboxes(id),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'resolved', 'withdrawn')),
    resolution TEXT,
    reply_id INTEGER REFERENCES messages(id),
    PRIMARY KEY(message, recipient)
);

CREATE INDEX pending_inbox ON deliveries(recipient, state, message);
CREATE INDEX message_due ON messages(due);
PRAGMA user_version = 1;
