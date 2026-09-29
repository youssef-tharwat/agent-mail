ALTER TABLE codex_wakes RENAME TO runtime_wakes;
ALTER TABLE runtime_wakes ADD COLUMN runtime TEXT NOT NULL DEFAULT 'codex' CHECK(runtime IN ('codex','claude'));
PRAGMA user_version=10;
