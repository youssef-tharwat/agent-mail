-- Runtime physical facts. Scheduler22 alone owns attempts, budgets and slots.
-- The exact22 preview keys are pinned in runtime-adapters/scheduler-api-reply-v1.txt.
CREATE TABLE runtime_targets (
 id TEXT PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 name TEXT NOT NULL,
 current_generation INTEGER NOT NULL CHECK(current_generation>0),
 enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)),
 UNIQUE(group_name,name), UNIQUE(id,group_name),
 FOREIGN KEY(id,current_generation) REFERENCES runtime_target_versions(target,generation)
 DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER runtime_target_identity BEFORE UPDATE ON runtime_targets
WHEN OLD.id<>NEW.id OR OLD.group_name<>NEW.group_name OR OLD.name<>NEW.name
 OR NEW.current_generation<OLD.current_generation
BEGIN SELECT RAISE(ABORT,'immutable runtime target identity'); END;
CREATE TRIGGER runtime_target_no_delete BEFORE DELETE ON runtime_targets
BEGIN SELECT RAISE(ABORT,'runtime target history cannot be deleted'); END;
CREATE TABLE runtime_target_versions (
 target TEXT NOT NULL REFERENCES runtime_targets(id),
 generation INTEGER NOT NULL CHECK(generation>0),
 owner TEXT NOT NULL, owner_binding INTEGER NOT NULL CHECK(owner_binding>0),
 client TEXT NOT NULL CHECK(client IN ('codex','claude')),
 profile TEXT NOT NULL CHECK(profile IN ('read_only','staged_files')),
 concurrency_key TEXT NOT NULL,
 specification TEXT NOT NULL CHECK(json_valid(specification)),
 policy_digest TEXT NOT NULL CHECK(length(policy_digest)=64),
 executable_digest TEXT NOT NULL CHECK(length(executable_digest)=64),
 created INTEGER NOT NULL,
 PRIMARY KEY(target,generation)
);
CREATE TRIGGER runtime_target_version_no_update BEFORE UPDATE ON runtime_target_versions
BEGIN SELECT RAISE(ABORT,'immutable runtime target generation'); END;
CREATE TRIGGER runtime_target_version_no_delete BEFORE DELETE ON runtime_target_versions
BEGIN SELECT RAISE(ABORT,'runtime target history cannot be deleted'); END;
CREATE TABLE runtime_capability_witnesses (
 id TEXT PRIMARY KEY,
 target TEXT NOT NULL, generation INTEGER NOT NULL,
 capability TEXT NOT NULL,
 observed INTEGER NOT NULL, valid_until INTEGER NOT NULL CHECK(valid_until>observed),
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 FOREIGN KEY(target,generation) REFERENCES runtime_target_versions(target,generation)
);
CREATE TRIGGER runtime_witness_no_update BEFORE UPDATE ON runtime_capability_witnesses
BEGIN SELECT RAISE(ABORT,'immutable runtime witness'); END;
CREATE TRIGGER runtime_witness_no_delete BEFORE DELETE ON runtime_capability_witnesses
BEGIN SELECT RAISE(ABORT,'runtime witness history cannot be deleted'); END;

CREATE TABLE runtime_segments (
 attempt TEXT PRIMARY KEY,
 group_name TEXT NOT NULL, task TEXT NOT NULL,
 fence INTEGER NOT NULL CHECK(fence>0), dispatch_key TEXT NOT NULL UNIQUE,
 target TEXT NOT NULL, target_generation INTEGER NOT NULL,
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request)),
 journal_key TEXT NOT NULL UNIQUE,
 containment TEXT CHECK(containment IS NULL OR json_valid(containment)),
 launch_committed INTEGER NOT NULL DEFAULT 0 CHECK(launch_committed IN (0,1)),
 tombstoned INTEGER NOT NULL DEFAULT 0 CHECK(tombstoned IN (0,1)),
 state TEXT NOT NULL CHECK(state IN ('prepared','starting','running','stopping','uncertain','quiescent')),
 native_session TEXT,
 created INTEGER NOT NULL,
 UNIQUE(attempt,group_name,task),
 FOREIGN KEY(attempt,group_name,task) REFERENCES execution_attempts(id,group_name,task),
 FOREIGN KEY(target,group_name) REFERENCES runtime_targets(id,group_name),
 FOREIGN KEY(target,target_generation) REFERENCES runtime_target_versions(target,generation)
);
CREATE TRIGGER runtime_segment_correlation BEFORE INSERT ON runtime_segments
WHEN NOT EXISTS(SELECT 1 FROM execution_attempts a WHERE a.id=NEW.attempt
 AND a.group_name=NEW.group_name AND a.task=NEW.task
 AND a.fence=NEW.fence AND a.dispatch_key=NEW.dispatch_key)
BEGIN SELECT RAISE(ABORT,'runtime attempt correlation mismatch'); END;
CREATE TRIGGER runtime_segment_identity BEFORE UPDATE ON runtime_segments
WHEN OLD.attempt<>NEW.attempt OR OLD.group_name<>NEW.group_name OR OLD.task<>NEW.task
 OR OLD.fence<>NEW.fence OR OLD.dispatch_key<>NEW.dispatch_key
 OR OLD.target<>NEW.target OR OLD.target_generation<>NEW.target_generation
 OR OLD.canonical_request<>NEW.canonical_request OR OLD.journal_key<>NEW.journal_key
 OR OLD.created<>NEW.created OR OLD.launch_committed>NEW.launch_committed
 OR OLD.tombstoned>NEW.tombstoned
 OR (OLD.containment IS NOT NULL AND OLD.containment IS NOT NEW.containment)
 OR (OLD.native_session IS NOT NULL AND OLD.native_session IS NOT NEW.native_session)
 OR (OLD.state='quiescent' AND NEW.state<>'quiescent')
BEGIN SELECT RAISE(ABORT,'immutable runtime segment identity or tombstone'); END;
CREATE TRIGGER runtime_segment_no_delete BEFORE DELETE ON runtime_segments
BEGIN SELECT RAISE(ABORT,'runtime dispatch history cannot be deleted'); END;

CREATE TABLE runtime_destinations (
 id TEXT PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 target TEXT NOT NULL,
 target_generation INTEGER NOT NULL,
 generation INTEGER NOT NULL DEFAULT 1 CHECK(generation>0),
 manifest TEXT CHECK(manifest IS NULL OR length(manifest)=64),
 UNIQUE(id,group_name),
 FOREIGN KEY(target,group_name) REFERENCES runtime_targets(id,group_name),
 FOREIGN KEY(target,target_generation) REFERENCES runtime_target_versions(target,generation)
);
CREATE TABLE runtime_effects (
 attempt TEXT NOT NULL REFERENCES runtime_segments(attempt), effect TEXT NOT NULL,
 group_name TEXT NOT NULL, task TEXT NOT NULL,
 destination TEXT NOT NULL,
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request)),
 manifest TEXT NOT NULL CHECK(length(manifest)=64),
 manifest_bytes INTEGER NOT NULL CHECK(manifest_bytes BETWEEN 0 AND 65536),
 scope_unit TEXT NOT NULL,
 expected_generation INTEGER NOT NULL CHECK(expected_generation>0),
 expected_manifest TEXT CHECK(expected_manifest IS NULL OR length(expected_manifest)=64),
 expected_task_version INTEGER CHECK(expected_task_version IS NULL OR expected_task_version>0),
 state TEXT NOT NULL CHECK(state IN ('pinned','sealed','published','abandoned')),
 seal TEXT CHECK(seal IS NULL OR json_valid(seal)),
 retention_pin INTEGER NOT NULL DEFAULT 1 CHECK(retention_pin=1),
 created INTEGER NOT NULL,
 PRIMARY KEY(attempt,effect),
 FOREIGN KEY(attempt,group_name,task) REFERENCES runtime_segments(attempt,group_name,task),
 FOREIGN KEY(destination,group_name) REFERENCES runtime_destinations(id,group_name)
);
CREATE TABLE runtime_effect_sets (
 attempt TEXT PRIMARY KEY REFERENCES runtime_segments(attempt),
 id TEXT NOT NULL UNIQUE,
 effects TEXT NOT NULL CHECK(json_valid(effects)),
 created INTEGER NOT NULL,
 UNIQUE(id,attempt)
);
CREATE TRIGGER runtime_effect_set_no_update BEFORE UPDATE ON runtime_effect_sets
BEGIN SELECT RAISE(ABORT,'immutable sealed runtime effect set'); END;
CREATE TRIGGER runtime_effect_set_no_delete BEFORE DELETE ON runtime_effect_sets
BEGIN SELECT RAISE(ABORT,'sealed runtime effect set cannot be deleted'); END;
CREATE TRIGGER runtime_effect_set_closed BEFORE INSERT ON runtime_effects
WHEN EXISTS(SELECT 1 FROM runtime_effect_sets WHERE attempt=NEW.attempt)
BEGIN SELECT RAISE(ABORT,'runtime effect set is already sealed'); END;
CREATE TRIGGER runtime_effect_identity BEFORE UPDATE ON runtime_effects
WHEN OLD.attempt<>NEW.attempt OR OLD.effect<>NEW.effect OR OLD.group_name<>NEW.group_name
 OR OLD.task<>NEW.task OR OLD.destination<>NEW.destination
 OR OLD.canonical_request<>NEW.canonical_request OR OLD.manifest<>NEW.manifest
 OR OLD.manifest_bytes<>NEW.manifest_bytes OR OLD.scope_unit<>NEW.scope_unit
 OR OLD.expected_generation<>NEW.expected_generation OR OLD.expected_manifest IS NOT NEW.expected_manifest
 OR OLD.expected_task_version IS NOT NEW.expected_task_version OR OLD.created<>NEW.created
 OR (OLD.seal IS NOT NULL AND OLD.seal IS NOT NEW.seal)
 OR (OLD.state='sealed' AND NEW.state='pinned')
 OR (OLD.state IN ('published','abandoned') AND OLD.state<>NEW.state)
BEGIN SELECT RAISE(ABORT,'immutable runtime effect intent or disposition'); END;
CREATE TRIGGER runtime_effect_no_delete BEFORE DELETE ON runtime_effects
BEGIN SELECT RAISE(ABORT,'runtime effect pins cannot be deleted'); END;
CREATE TABLE runtime_receipts (
 id TEXT PRIMARY KEY,
 attempt TEXT NOT NULL, effect TEXT NOT NULL,
 canonical_request TEXT NOT NULL CHECK(json_valid(canonical_request)),
 disposition TEXT NOT NULL CHECK(disposition IN ('published','abandoned')),
 observation TEXT NOT NULL CHECK(json_valid(observation)),
 created INTEGER NOT NULL,
 UNIQUE(attempt,effect),
 FOREIGN KEY(attempt,effect) REFERENCES runtime_effects(attempt,effect)
);
CREATE TRIGGER runtime_receipt_no_update BEFORE UPDATE ON runtime_receipts
BEGIN SELECT RAISE(ABORT,'immutable runtime effect receipt'); END;
CREATE TRIGGER runtime_receipt_no_delete BEFORE DELETE ON runtime_receipts
BEGIN SELECT RAISE(ABORT,'runtime effect receipt cannot be deleted'); END;
CREATE TABLE runtime_observations (
 id TEXT PRIMARY KEY, attempt TEXT NOT NULL REFERENCES runtime_segments(attempt),
 sequence INTEGER NOT NULL CHECK(sequence>0),
 observed INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('active','unknown','exit_observed')),
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 UNIQUE(attempt,sequence)
);
CREATE TRIGGER runtime_observation_no_update BEFORE UPDATE ON runtime_observations
BEGIN SELECT RAISE(ABORT,'immutable runtime observation'); END;
CREATE TRIGGER runtime_observation_no_delete BEFORE DELETE ON runtime_observations
BEGIN SELECT RAISE(ABORT,'runtime observation cannot be deleted'); END;
CREATE TABLE runtime_closures (
 id TEXT PRIMARY KEY, attempt TEXT NOT NULL UNIQUE REFERENCES runtime_segments(attempt),
 effect_set TEXT NOT NULL,
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 costs TEXT NOT NULL CHECK(json_valid(costs)),
 created INTEGER NOT NULL,
 FOREIGN KEY(effect_set,attempt) REFERENCES runtime_effect_sets(id,attempt)
);
CREATE TRIGGER runtime_closure_no_update BEFORE UPDATE ON runtime_closures
BEGIN SELECT RAISE(ABORT,'immutable runtime closure'); END;
CREATE TRIGGER runtime_closure_no_delete BEFORE DELETE ON runtime_closures
BEGIN SELECT RAISE(ABORT,'runtime closure cannot be deleted'); END;
CREATE TABLE runtime_fk_check(violations INTEGER CHECK(violations=0));
INSERT INTO runtime_fk_check SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE runtime_fk_check;
PRAGMA user_version=23;
