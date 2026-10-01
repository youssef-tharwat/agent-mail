-- Independent service jobs reserve visits separately from owner-page completion.
-- These cursors carry scheduling responsibility, never execution permission.
CREATE TABLE execution_driver_cursors (
    job TEXT PRIMARY KEY CHECK(job IN ('repair','claim','reconcile','supervise')),
    last_group TEXT NOT NULL DEFAULT '',
    attempted_at INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    completed_at INTEGER,
    completed INTEGER NOT NULL DEFAULT 0 CHECK(completed >= 0),
    last_error TEXT CHECK(last_error IS NULL OR length(last_error) <= 2048)
);
INSERT INTO execution_driver_cursors(job,last_group)
SELECT 'repair',last_group FROM execution_controller WHERE id=1;
INSERT INTO execution_driver_cursors(job) VALUES('claim'),('reconcile'),('supervise');
-- At most one immutable basis per original attempt; legacy attempts stay absent.
CREATE UNIQUE INDEX execution_claim_progress_basis
ON execution_events(attempt) WHERE kind='progress_claim_basis';
CREATE TEMP TABLE execution_driver_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO execution_driver_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE execution_driver_fk_guard;
PRAGMA user_version=26;
