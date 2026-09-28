CREATE TABLE node (id TEXT PRIMARY KEY NOT NULL);
ALTER TABLE groups ADD COLUMN home_machine TEXT NOT NULL DEFAULT '';
ALTER TABLE mailboxes ADD COLUMN remote_machine TEXT;
ALTER TABLE messages ADD COLUMN global_id TEXT;
CREATE UNIQUE INDEX message_global_id ON messages(global_id) WHERE global_id IS NOT NULL;

CREATE TABLE peers (
    machine_id TEXT PRIMARY KEY NOT NULL,
    ssh_target TEXT NOT NULL
);

CREATE TABLE outbox (
    event_id TEXT PRIMARY KEY NOT NULL,
    dest_machine TEXT NOT NULL,
    payload TEXT NOT NULL,
    created INTEGER NOT NULL
);
CREATE INDEX outbox_by_destination ON outbox(dest_machine, created);

CREATE TABLE seen_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    received INTEGER NOT NULL
);

CREATE TABLE work_snapshots (
    group_name TEXT NOT NULL,
    work_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    home_version INTEGER NOT NULL,
    synced_at INTEGER NOT NULL,
    PRIMARY KEY(group_name, work_id)
);

PRAGMA user_version = 4;
