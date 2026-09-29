-- Refuse ambiguous legacy states atomically instead of guessing intent.
CREATE TEMP TABLE validate_task_states (
 invalid INTEGER NOT NULL CONSTRAINT task_state_requires_explicit_migration CHECK(invalid=0)
);
INSERT INTO validate_task_states SELECT COUNT(*) FROM work_items
WHERE state NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled')
 OR open <> (state NOT IN ('done','accepted','cancelled'));
INSERT INTO validate_task_states SELECT COUNT(*) FROM work_changes WHERE (json_extract(snapshot,'$.state') IS NULL OR json_extract(snapshot,'$.state') NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled') OR (json_extract(snapshot,'$.open') IS NOT NULL AND json_extract(snapshot,'$.open') <> (json_extract(snapshot,'$.state') NOT IN ('done','accepted','cancelled'))));
INSERT INTO validate_task_states SELECT COUNT(*) FROM work_snapshots WHERE (json_extract(snapshot,'$.state') IS NULL OR json_extract(snapshot,'$.state') NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled') OR (json_extract(snapshot,'$.open') IS NOT NULL AND json_extract(snapshot,'$.open') <> (json_extract(snapshot,'$.state') NOT IN ('done','accepted','cancelled'))));
INSERT INTO validate_task_states SELECT COUNT(*) FROM work_decisions WHERE (json_extract(result,'$.state') IS NULL OR json_extract(result,'$.state') NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled') OR (json_extract(result,'$.open') IS NOT NULL AND json_extract(result,'$.open') <> (json_extract(result,'$.state') NOT IN ('done','accepted','cancelled'))));
INSERT INTO validate_task_states SELECT COUNT(*) FROM work_creations WHERE (json_extract(result,'$.state') IS NULL OR json_extract(result,'$.state') NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled') OR (json_extract(result,'$.open') IS NOT NULL AND json_extract(result,'$.open') <> (json_extract(result,'$.state') NOT IN ('done','accepted','cancelled'))));
INSERT INTO validate_task_states SELECT COUNT(*) FROM outbox WHERE json_extract(payload,'$.event.kind')='work_snapshot' AND (json_extract(payload,'$.event.data.state') IS NULL OR json_extract(payload,'$.event.data.state') NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled') OR (json_extract(payload,'$.event.data.open') IS NOT NULL AND json_extract(payload,'$.event.data.open') <> (json_extract(payload,'$.event.data.state') NOT IN ('done','accepted','cancelled'))));
-- Remove only the obsolete projection after every snapshot passes validation.
UPDATE work_changes SET snapshot=json_remove(snapshot,'$.open');
UPDATE work_snapshots SET snapshot=json_remove(snapshot,'$.open');
UPDATE work_decisions SET result=json_remove(result,'$.open');
UPDATE work_creations SET result=json_remove(result,'$.open');
UPDATE outbox SET payload=json_remove(payload,'$.event.data.open') WHERE json_extract(payload,'$.event.kind')='work_snapshot';
DROP TABLE validate_task_states;
CREATE TRIGGER typed_task_insert BEFORE INSERT ON work_items
WHEN NEW.state NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled')
 OR NEW.open <> (NEW.state NOT IN ('done','accepted','cancelled'))
BEGIN SELECT RAISE(ABORT,'invalid task lifecycle or derived open flag'); END;
CREATE TRIGGER typed_task_update BEFORE UPDATE OF state,open ON work_items
WHEN NEW.state NOT IN ('open','ready','active','blocked','review','done','accepted','cancelled')
 OR NEW.open <> (NEW.state NOT IN ('done','accepted','cancelled'))
BEGIN SELECT RAISE(ABORT,'invalid task lifecycle or derived open flag'); END;
PRAGMA user_version=14;
