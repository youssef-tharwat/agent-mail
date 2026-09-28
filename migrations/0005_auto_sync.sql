ALTER TABLE peers ADD COLUMN last_sync INTEGER;
ALTER TABLE peers ADD COLUMN last_error TEXT;
ALTER TABLE peers ADD COLUMN auto_sync INTEGER NOT NULL DEFAULT 0 CHECK (auto_sync IN (0, 1));
PRAGMA user_version = 5;
