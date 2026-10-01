-- Original physical custody only. No native qualification or business authority.
CREATE TABLE runtime_capture_intents (
 attempt TEXT PRIMARY KEY REFERENCES runtime_segments(attempt),
 correlation TEXT NOT NULL CHECK(json_valid(correlation)),
 node TEXT NOT NULL,
 boot_id TEXT NOT NULL,
 object_key TEXT NOT NULL UNIQUE,
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes=27262976),
 created INTEGER NOT NULL,
 containment_creation TEXT CHECK(containment_creation IS NULL OR json_valid(containment_creation)),
 reclaim_checked INTEGER NOT NULL DEFAULT 0 CHECK(reclaim_checked>=0),
 exposed INTEGER NOT NULL DEFAULT 0 CHECK(exposed IN (0,1))
);
CREATE TRIGGER runtime_capture_capacity BEFORE INSERT ON runtime_capture_intents
WHEN NEW.reserved_bytes+(SELECT coalesce(sum(reserved_bytes),0) FROM runtime_capture_intents)>268435456
BEGIN SELECT RAISE(ABORT,'runtime capture capacity exhausted'); END;
CREATE TRIGGER runtime_capture_intent_identity BEFORE UPDATE ON runtime_capture_intents
WHEN OLD.attempt<>NEW.attempt OR OLD.correlation<>NEW.correlation
 OR OLD.node<>NEW.node OR OLD.boot_id<>NEW.boot_id
 OR OLD.object_key<>NEW.object_key OR OLD.reserved_bytes<>NEW.reserved_bytes
 OR OLD.created<>NEW.created OR OLD.exposed>NEW.exposed OR OLD.reclaim_checked>NEW.reclaim_checked
 OR (OLD.containment_creation IS NOT NULL AND OLD.containment_creation IS NOT NEW.containment_creation)
BEGIN SELECT RAISE(ABORT,'immutable capture intent'); END;
CREATE TRIGGER runtime_capture_intent_no_delete BEFORE DELETE ON runtime_capture_intents
BEGIN SELECT RAISE(ABORT,'capture reservation is permanent'); END;
CREATE TABLE runtime_capture_objects (
 attempt TEXT NOT NULL REFERENCES runtime_capture_intents(attempt),
 kind TEXT NOT NULL CHECK(kind IN ('launch','custody','effects','stdout','stderr','journal','scratch')),
 name TEXT NOT NULL UNIQUE,
 identity TEXT NOT NULL CHECK(json_valid(identity)),
 PRIMARY KEY(attempt,kind)
);
CREATE TRIGGER runtime_capture_object_no_update BEFORE UPDATE ON runtime_capture_objects
BEGIN SELECT RAISE(ABORT,'immutable capture object'); END;
CREATE TRIGGER runtime_capture_object_no_delete BEFORE DELETE ON runtime_capture_objects
BEGIN SELECT RAISE(ABORT,'capture object retained'); END;
CREATE TABLE runtime_capture_processes (
 attempt TEXT NOT NULL REFERENCES runtime_capture_intents(attempt),
 role TEXT NOT NULL CHECK(role IN ('custodian','contained')),
 identity TEXT NOT NULL CHECK(json_valid(identity)),
 created INTEGER NOT NULL,
 PRIMARY KEY(attempt,role)
);
CREATE TRIGGER runtime_capture_process_no_update BEFORE UPDATE ON runtime_capture_processes
BEGIN SELECT RAISE(ABORT,'immutable capture process'); END;
CREATE TRIGGER runtime_capture_process_no_delete BEFORE DELETE ON runtime_capture_processes
BEGIN SELECT RAISE(ABORT,'capture process retained'); END;
CREATE TABLE runtime_capture_failures (
 attempt TEXT PRIMARY KEY REFERENCES runtime_capture_intents(attempt),
 disposition TEXT NOT NULL CHECK(disposition IN ('failed','invalid')),
 detail TEXT NOT NULL CHECK(length(CAST(detail AS BLOB))<=4096),
 created INTEGER NOT NULL
);
CREATE TRIGGER runtime_capture_failure_no_update BEFORE UPDATE ON runtime_capture_failures
BEGIN SELECT RAISE(ABORT,'immutable original capture failure'); END;
CREATE TRIGGER runtime_capture_failure_no_delete BEFORE DELETE ON runtime_capture_failures
BEGIN SELECT RAISE(ABORT,'capture failure retained'); END;
CREATE TABLE runtime_capture_seals (
 attempt TEXT PRIMARY KEY REFERENCES runtime_capture_intents(attempt),
 id TEXT NOT NULL UNIQUE,
 disposition TEXT NOT NULL CHECK(disposition IN ('complete','failed','invalid','interrupted')),
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 created INTEGER NOT NULL
);
CREATE TRIGGER runtime_capture_seal_no_update BEFORE UPDATE ON runtime_capture_seals
BEGIN SELECT RAISE(ABORT,'immutable capture seal'); END;
CREATE TRIGGER runtime_capture_seal_no_delete BEFORE DELETE ON runtime_capture_seals
BEGIN SELECT RAISE(ABORT,'capture seal retained'); END;
CREATE TABLE runtime_capture_reclamations (
 attempt TEXT PRIMARY KEY REFERENCES runtime_capture_intents(attempt),
 closure TEXT NOT NULL REFERENCES runtime_closures(id),
 containment TEXT NOT NULL CHECK(json_valid(containment)),
 created INTEGER NOT NULL
);
CREATE TRIGGER runtime_capture_reclamation_no_update BEFORE UPDATE ON runtime_capture_reclamations
BEGIN SELECT RAISE(ABORT,'immutable original reclamation'); END;
CREATE TRIGGER runtime_capture_reclamation_no_delete BEFORE DELETE ON runtime_capture_reclamations
BEGIN SELECT RAISE(ABORT,'reclamation history retained'); END;
CREATE TABLE runtime_capture_fk_check(violations INTEGER CHECK(violations=0));
INSERT INTO runtime_capture_fk_check SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE runtime_capture_fk_check;
PRAGMA user_version=31;
