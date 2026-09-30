-- Native retries must send the same input and retain the original plan snapshot.
ALTER TABLE turn_offers ADD COLUMN native_payload TEXT
 CHECK(native_payload IS NULL OR (runtime='codex' AND length(CAST(native_payload AS BLOB))<=6000));
ALTER TABLE turn_offers ADD COLUMN native_nonce TEXT;

-- The old payload was not retained. Preserve its history but never attribute a
-- delayed completion to the old mutable snapshot. New input uses a new ID namespace.
UPDATE turn_offers SET state='abandoned'
 WHERE runtime='codex' AND state='offered' AND native_payload IS NULL;
PRAGMA user_version=20;
