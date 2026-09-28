ALTER TABLE groups ADD COLUMN auto_prompt INTEGER NOT NULL DEFAULT 0 CHECK (auto_prompt IN (0, 1));
PRAGMA user_version = 3;
