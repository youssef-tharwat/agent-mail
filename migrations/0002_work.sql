CREATE TABLE work_items (
    group_name TEXT NOT NULL REFERENCES groups(name),
    id TEXT NOT NULL,
    scope TEXT NOT NULL,
    owner TEXT NOT NULL,
    writer TEXT NOT NULL,
    state TEXT NOT NULL,
    open INTEGER NOT NULL DEFAULT 1 CHECK (open IN (0, 1)),
    next_action TEXT NOT NULL,
    deadline INTEGER,
    accepted_revision TEXT,
    evidence TEXT NOT NULL DEFAULT '[]',
    version INTEGER NOT NULL DEFAULT 1,
    updated INTEGER NOT NULL,
    PRIMARY KEY(group_name, id),
    FOREIGN KEY(group_name, owner) REFERENCES mailboxes(group_name, name),
    FOREIGN KEY(group_name, writer) REFERENCES mailboxes(group_name, name)
);

CREATE INDEX work_by_owner ON work_items(group_name, owner, id);

CREATE TABLE work_changes (
    group_name TEXT NOT NULL,
    work_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    changed INTEGER NOT NULL,
    PRIMARY KEY(group_name, work_id, version),
    FOREIGN KEY(group_name, work_id) REFERENCES work_items(group_name, id)
);

ALTER TABLE messages ADD COLUMN work_id TEXT;
CREATE INDEX message_by_work ON messages(work_id);
PRAGMA user_version = 2;
