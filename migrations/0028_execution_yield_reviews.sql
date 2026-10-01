-- One authenticated immutable review fact per original execution attempt.
-- Historical raw Yield reports are not backfilled into scheduling authority.
CREATE UNIQUE INDEX execution_original_yield_review
ON execution_events(attempt) WHERE kind='yield_review';
CREATE TEMP TABLE execution_yield_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO execution_yield_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE execution_yield_fk_guard;
PRAGMA user_version=28;
