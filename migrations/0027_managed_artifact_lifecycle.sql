-- Public lifecycle and retained text-artifact facts. Qualification and physical
-- spool custody are deliberately separate; these rows grant no native capability.
CREATE TABLE runtime_target_lifecycle (
 target TEXT PRIMARY KEY REFERENCES runtime_targets(id),
 revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>0),
 retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1))
);
INSERT INTO runtime_target_lifecycle(target,revision) SELECT id,1 FROM runtime_targets;
CREATE TRIGGER runtime_lifecycle_create AFTER INSERT ON runtime_targets
BEGIN INSERT INTO runtime_target_lifecycle(target,revision) VALUES(NEW.id,1); END;
CREATE TRIGGER runtime_retired_target BEFORE UPDATE ON runtime_targets
WHEN EXISTS(SELECT 1 FROM runtime_target_lifecycle WHERE target=OLD.id AND retired=1)
 AND (NEW.enabled<>0 OR NEW.current_generation<>OLD.current_generation)
BEGIN SELECT RAISE(ABORT,'retired runtime target'); END;
CREATE TRIGGER runtime_lifecycle_advance AFTER UPDATE ON runtime_targets
WHEN OLD.current_generation<>NEW.current_generation OR OLD.enabled<>NEW.enabled
BEGIN UPDATE runtime_target_lifecycle SET revision=revision+1 WHERE target=NEW.id; END;
CREATE TRIGGER runtime_lifecycle_identity BEFORE UPDATE ON runtime_target_lifecycle
WHEN OLD.target<>NEW.target OR NEW.revision<OLD.revision OR NEW.retired<OLD.retired
 OR (NEW.retired=1 AND EXISTS(SELECT 1 FROM runtime_targets WHERE id=NEW.target AND enabled<>0))
BEGIN SELECT RAISE(ABORT,'invalid runtime lifecycle transition'); END;
CREATE TRIGGER runtime_lifecycle_no_delete BEFORE DELETE ON runtime_target_lifecycle
BEGIN SELECT RAISE(ABORT,'runtime lifecycle history cannot be deleted'); END;

CREATE TABLE runtime_lifecycle_receipts (
 id TEXT PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 actor_binding INTEGER NOT NULL CHECK(actor_binding>0),
 retry_key TEXT NOT NULL,
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request)),
 target TEXT NOT NULL REFERENCES runtime_targets(id),
 receipt TEXT NOT NULL CHECK(json_valid(receipt)),
 created INTEGER NOT NULL,
 UNIQUE(group_name,actor,retry_key)
);
CREATE TRIGGER runtime_lifecycle_receipt_no_update BEFORE UPDATE ON runtime_lifecycle_receipts
BEGIN SELECT RAISE(ABORT,'immutable runtime lifecycle receipt'); END;
CREATE TRIGGER runtime_lifecycle_receipt_no_delete BEFORE DELETE ON runtime_lifecycle_receipts
BEGIN SELECT RAISE(ABORT,'runtime lifecycle receipt cannot be deleted'); END;

CREATE TABLE runtime_artifact_bindings (
 identity TEXT PRIMARY KEY,
 group_name TEXT NOT NULL,
 id TEXT NOT NULL CHECK(length(id) BETWEEN 1 AND 128),
 task TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0),
 UNIQUE(group_name,id), UNIQUE(identity,group_name,task),
 FOREIGN KEY(group_name,task) REFERENCES work_items(group_name,id),
 FOREIGN KEY(identity,revision) REFERENCES runtime_artifact_binding_versions(identity,revision)
 DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE runtime_artifact_binding_versions (
 identity TEXT NOT NULL REFERENCES runtime_artifact_bindings(identity),
 revision INTEGER NOT NULL CHECK(revision>0),
 target TEXT NOT NULL,
 target_generation INTEGER NOT NULL,
 destination TEXT NOT NULL REFERENCES runtime_destinations(id),
 scope_unit TEXT NOT NULL,
 allowed_paths TEXT NOT NULL CHECK(json_valid(allowed_paths) AND length(CAST(allowed_paths AS BLOB))<=8192),
 authority TEXT NOT NULL CHECK(json_valid(authority) AND length(CAST(authority AS BLOB))<=262144),
 created INTEGER NOT NULL,
 PRIMARY KEY(identity,revision),
 FOREIGN KEY(target,target_generation) REFERENCES runtime_target_versions(target,generation)
);
CREATE TRIGGER runtime_binding_identity BEFORE UPDATE ON runtime_artifact_bindings
WHEN OLD.identity<>NEW.identity OR OLD.group_name<>NEW.group_name OR OLD.id<>NEW.id
 OR OLD.task<>NEW.task OR NEW.revision<>OLD.revision+1
BEGIN SELECT RAISE(ABORT,'immutable runtime artifact binding identity'); END;
CREATE TRIGGER runtime_binding_no_delete BEFORE DELETE ON runtime_artifact_bindings
BEGIN SELECT RAISE(ABORT,'runtime artifact binding history cannot be deleted'); END;
CREATE TRIGGER runtime_binding_version_no_update BEFORE UPDATE ON runtime_artifact_binding_versions
BEGIN SELECT RAISE(ABORT,'immutable runtime artifact binding revision'); END;
CREATE TRIGGER runtime_binding_version_no_delete BEFORE DELETE ON runtime_artifact_binding_versions
BEGIN SELECT RAISE(ABORT,'runtime artifact binding history cannot be deleted'); END;

CREATE TABLE runtime_destination_owners (
 destination TEXT PRIMARY KEY REFERENCES runtime_destinations(id),
 group_name TEXT NOT NULL,
 task TEXT NOT NULL,
 FOREIGN KEY(destination,group_name) REFERENCES runtime_destinations(id,group_name),
 FOREIGN KEY(group_name,task) REFERENCES work_items(group_name,id)
);
CREATE TRIGGER runtime_destination_owner_no_update BEFORE UPDATE ON runtime_destination_owners
BEGIN SELECT RAISE(ABORT,'immutable runtime destination owner'); END;
CREATE TRIGGER runtime_destination_owner_no_delete BEFORE DELETE ON runtime_destination_owners
BEGIN SELECT RAISE(ABORT,'runtime destination owner cannot be deleted'); END;

CREATE TABLE runtime_artifact_admissions (
 attempt TEXT PRIMARY KEY REFERENCES runtime_segments(attempt),
 binding TEXT NOT NULL,
 binding_revision INTEGER NOT NULL,
 destination_generation INTEGER NOT NULL CHECK(destination_generation>0),
 destination_manifest TEXT CHECK(destination_manifest IS NULL OR length(destination_manifest)=64),
 task_version INTEGER NOT NULL CHECK(task_version>0),
 followup_version INTEGER NOT NULL CHECK(followup_version>=0),
 basis TEXT NOT NULL CHECK(json_valid(basis) AND length(CAST(basis AS BLOB))<=262144),
 created INTEGER NOT NULL,
 FOREIGN KEY(binding,binding_revision) REFERENCES runtime_artifact_binding_versions(identity,revision)
);
CREATE TRIGGER runtime_artifact_admission_no_update BEFORE UPDATE ON runtime_artifact_admissions
BEGIN SELECT RAISE(ABORT,'immutable runtime artifact admission'); END;
CREATE TRIGGER runtime_artifact_admission_no_delete BEFORE DELETE ON runtime_artifact_admissions
BEGIN SELECT RAISE(ABORT,'runtime artifact admission cannot be deleted'); END;

-- Permanent conservative reservation before any artifact write. Repeated digests
-- do not replenish capacity. No garbage collection or caller-selected capacity.
CREATE TABLE runtime_artifact_reservations (
 id TEXT PRIMARY KEY,
 attempt TEXT NOT NULL,
 effect TEXT NOT NULL,
 bytes INTEGER NOT NULL CHECK(bytes BETWEEN 1 AND 4194304),
 created INTEGER NOT NULL,
 FOREIGN KEY(attempt,effect) REFERENCES runtime_effects(attempt,effect)
);
CREATE TRIGGER runtime_artifact_reservation_capacity BEFORE INSERT ON runtime_artifact_reservations
WHEN NEW.bytes+(SELECT coalesce(sum(bytes),0) FROM runtime_artifact_reservations)>268435456
BEGIN SELECT RAISE(ABORT,'managed artifact installation capacity exhausted'); END;
CREATE TRIGGER runtime_artifact_reservation_no_update BEFORE UPDATE ON runtime_artifact_reservations
BEGIN SELECT RAISE(ABORT,'immutable runtime artifact reservation'); END;
CREATE TRIGGER runtime_artifact_reservation_no_delete BEFORE DELETE ON runtime_artifact_reservations
BEGIN SELECT RAISE(ABORT,'runtime artifact reservation cannot be deleted'); END;

CREATE TABLE runtime_binding_receipts (
 id TEXT PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 actor_binding INTEGER NOT NULL CHECK(actor_binding>0),
 retry_key TEXT NOT NULL,
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request)),
 binding TEXT NOT NULL REFERENCES runtime_artifact_bindings(identity),
 receipt TEXT NOT NULL CHECK(json_valid(receipt)),
 created INTEGER NOT NULL,
 UNIQUE(group_name,actor,retry_key)
);
CREATE TRIGGER runtime_binding_receipt_no_update BEFORE UPDATE ON runtime_binding_receipts
BEGIN SELECT RAISE(ABORT,'immutable runtime binding receipt'); END;
CREATE TRIGGER runtime_binding_receipt_no_delete BEFORE DELETE ON runtime_binding_receipts
BEGIN SELECT RAISE(ABORT,'runtime binding receipt cannot be deleted'); END;

-- One accepted combined operation. The scheduler owns review facts in schema28;
-- this immutable whole receipt only supplies exact historical wrapper replay.
CREATE TABLE runtime_yield_receipts (
 attempt TEXT NOT NULL REFERENCES runtime_segments(attempt),
 retry_key TEXT NOT NULL,
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 actor_binding INTEGER NOT NULL CHECK(actor_binding>0),
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request) AND length(CAST(canonical_request AS BLOB))<=65536),
 receipt TEXT NOT NULL CHECK(json_valid(receipt) AND length(CAST(receipt AS BLOB))<=262144),
 created INTEGER NOT NULL,
 PRIMARY KEY(attempt,retry_key)
);
CREATE TRIGGER runtime_yield_receipt_no_update BEFORE UPDATE ON runtime_yield_receipts
BEGIN SELECT RAISE(ABORT,'immutable runtime yield receipt'); END;
CREATE TRIGGER runtime_yield_receipt_no_delete BEFORE DELETE ON runtime_yield_receipts
BEGIN SELECT RAISE(ABORT,'runtime yield receipt cannot be deleted'); END;
CREATE TABLE runtime_lifecycle_fk_check(violations INTEGER CHECK(violations=0));
INSERT INTO runtime_lifecycle_fk_check SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE runtime_lifecycle_fk_check;
PRAGMA user_version=27;
