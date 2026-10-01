CREATE TABLE task_relations (
 group_name TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('parent','related','dependency')),
 review_round TEXT NOT NULL DEFAULT '', source_revision TEXT NOT NULL DEFAULT '',
 active INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL,
 actor TEXT NOT NULL, reason TEXT NOT NULL, updated INTEGER NOT NULL,
 PRIMARY KEY(group_name,source,target,kind,review_round,source_revision),
 FOREIGN KEY(group_name,source) REFERENCES work_items(group_name,id),
 FOREIGN KEY(group_name,target) REFERENCES work_items(group_name,id)
);
CREATE TABLE task_relation_history (
 id INTEGER PRIMARY KEY, group_name TEXT NOT NULL, source TEXT NOT NULL,
 snapshot TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, changed INTEGER NOT NULL
);
CREATE TABLE task_transfers (
 group_name TEXT NOT NULL, work_id TEXT NOT NULL, expected INTEGER NOT NULL,
 actor TEXT NOT NULL, canonical TEXT NOT NULL, result TEXT NOT NULL,
 old_writer TEXT NOT NULL, new_writer TEXT NOT NULL, operator INTEGER NOT NULL,
 changed INTEGER NOT NULL, PRIMARY KEY(group_name,work_id,expected)
);

CREATE TABLE task_relation_retries (
 group_name TEXT NOT NULL, source TEXT NOT NULL, expected INTEGER NOT NULL,
 actor INTEGER NOT NULL, binding_version INTEGER NOT NULL,
 canonical TEXT NOT NULL, result TEXT NOT NULL,
 PRIMARY KEY(group_name,source,expected)
);
CREATE TABLE task_relation_snapshots (
 group_name TEXT NOT NULL, source TEXT NOT NULL, version INTEGER NOT NULL,
 snapshot TEXT NOT NULL, synced_at INTEGER NOT NULL,
 PRIMARY KEY(group_name,source)
);
PRAGMA user_version=22;
-- Aggregate prerequisite changes receive the same bounded scheduler hint as single waits.
CREATE TRIGGER followup_dependency_tasks AFTER UPDATE ON work_items BEGIN
 UPDATE followups SET scanned=0 WHERE group_name=NEW.group_name
 AND json_extract(checkpoint,'$.waiting.kind')='tasks'
 AND EXISTS(SELECT 1 FROM json_each(checkpoint,'$.waiting.tasks') WHERE json_extract(value,'$.id')=NEW.id);
END;
