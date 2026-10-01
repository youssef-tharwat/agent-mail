-- Scheduling state only: original Runtime evidence remains with its owner.
CREATE TEMP TABLE reclamation_fairness_version_guard(version INTEGER CHECK(version=31));
INSERT INTO reclamation_fairness_version_guard SELECT user_version FROM pragma_user_version;
DROP TABLE reclamation_fairness_version_guard;

CREATE TABLE execution_reconcile_groups (
    group_name TEXT NOT NULL PRIMARY KEY REFERENCES groups(name),
    next_class TEXT NOT NULL DEFAULT 'held' CHECK(next_class IN ('held','reclamation')),
    reclamation_after TEXT NOT NULL DEFAULT '' COLLATE BINARY
        CHECK(typeof(reclamation_after)='text' AND length(CAST(reclamation_after AS BLOB))<=128)
);
CREATE TRIGGER execution_reconcile_group_identity BEFORE UPDATE ON execution_reconcile_groups
WHEN OLD.group_name<>NEW.group_name
BEGIN SELECT RAISE(ABORT,'immutable reconciliation group'); END;
CREATE TRIGGER execution_reconcile_group_retained BEFORE DELETE ON execution_reconcile_groups
BEGIN SELECT RAISE(ABORT,'reconciliation preference retained'); END;

CREATE TEMP TABLE reclamation_fairness_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO reclamation_fairness_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE reclamation_fairness_fk_guard;
PRAGMA user_version=32;
