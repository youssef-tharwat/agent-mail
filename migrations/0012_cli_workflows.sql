-- Preserve legacy due for published-schema compatibility; deadline is authoritative.
ALTER TABLE messages ADD COLUMN deadline INTEGER;
UPDATE messages SET deadline=due;
CREATE TABLE work_creations (
 group_name TEXT NOT NULL,
 work_id TEXT NOT NULL,
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 canonical TEXT NOT NULL,
 result TEXT NOT NULL,
 PRIMARY KEY(group_name,work_id),
 FOREIGN KEY(group_name,work_id) REFERENCES work_items(group_name,id)
);
CREATE TABLE runtime_policy (
 recipient INTEGER PRIMARY KEY REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 enabled INTEGER NOT NULL CHECK(enabled IN (0,1))
);
PRAGMA user_version=12;
