-- Setup holds the exclusive schema lock and disables FK enforcement on the
-- migration connection while rebuilding this referenced table. Validate all
-- references below before the migration can commit.
CREATE TABLE mailboxes_next (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL REFERENCES groups(name),
    name TEXT NOT NULL,
    binding TEXT NOT NULL CHECK (json_valid(binding)),
    binding_version INTEGER NOT NULL DEFAULT 1 CHECK (binding_version > 0),
    pane TEXT GENERATED ALWAYS AS (CASE WHEN json_extract(binding, '$.runtime') = 'herdr' THEN json_extract(binding, '$.pane') END) VIRTUAL,
    remote_machine TEXT GENERATED ALWAYS AS (CASE WHEN json_extract(binding, '$.runtime') = 'remote' THEN json_extract(binding, '$.machine') END) VIRTUAL,
    standalone_session TEXT GENERATED ALWAYS AS (CASE WHEN json_extract(binding, '$.runtime') = 'standalone' THEN json_extract(binding, '$.session') END) VIRTUAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_wake INTEGER NOT NULL DEFAULT 0,
    alerted INTEGER NOT NULL DEFAULT 0,
    CHECK (json_extract(binding, '$.runtime') IN ('herdr', 'standalone', 'remote')),
    UNIQUE(group_name, name),
    UNIQUE(group_name, pane),
    UNIQUE(standalone_session)
);
INSERT INTO mailboxes_next(id, group_name, name, binding, attempts, next_wake, alerted)
SELECT id, group_name, name,
    CASE WHEN remote_machine IS NOT NULL
        THEN json_object('runtime', 'remote', 'machine', remote_machine)
        ELSE json_object('runtime', 'herdr', 'pane', pane, 'terminal', terminal,
            'agent', agent, 'session_kind', session_kind, 'session_value', session_value, 'cwd', cwd)
    END,
    attempts, next_wake, alerted
FROM mailboxes;
DROP TABLE mailboxes;
ALTER TABLE mailboxes_next RENAME TO mailboxes;

CREATE TABLE migration_integrity_check (violations INTEGER NOT NULL CHECK (violations = 0));
INSERT INTO migration_integrity_check SELECT COUNT(*) FROM pragma_foreign_key_check;
DROP TABLE migration_integrity_check;
PRAGMA user_version = 6;
