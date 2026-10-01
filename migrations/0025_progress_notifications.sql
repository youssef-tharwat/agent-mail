-- Apply only after the actual owner migrations 21--24, with the legacy sender
-- stopped by the central integration. This migration never edits their sources.
CREATE TEMP TABLE progress_predecessor(version INTEGER CHECK(version=24));
INSERT INTO progress_predecessor SELECT user_version FROM pragma_user_version;
DROP TABLE progress_predecessor;

CREATE TABLE progress_records (
 id INTEGER PRIMARY KEY,
 group_name TEXT NOT NULL,
 task TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('policy','qualified','rejected','revoked')),
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 actor_binding INTEGER NOT NULL,
 request_key TEXT NOT NULL,
 canonical TEXT NOT NULL CHECK(json_valid(canonical)),
 payload TEXT NOT NULL CHECK(json_valid(payload)),
 created INTEGER NOT NULL,
 UNIQUE(actor,request_key),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task)
);
CREATE INDEX progress_history ON progress_records(group_name,task,id);
CREATE TRIGGER progress_record_no_update BEFORE UPDATE ON progress_records
BEGIN SELECT RAISE(ABORT,'progress records are immutable'); END;
CREATE TRIGGER progress_record_no_delete BEFORE DELETE ON progress_records
BEGIN SELECT RAISE(ABORT,'progress records are immutable'); END;
CREATE TABLE task_progress (
 group_name TEXT NOT NULL,
 task TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0),
 policy_record INTEGER NOT NULL REFERENCES progress_records(id),
 PRIMARY KEY(group_name,task),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task)
);
CREATE TABLE progress_milestones (
 group_name TEXT NOT NULL,
 task TEXT NOT NULL,
 milestone TEXT NOT NULL,
 definition TEXT NOT NULL CHECK(json_valid(definition)),
 PRIMARY KEY(group_name,task,milestone),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task)
);
CREATE TRIGGER progress_milestone_no_update BEFORE UPDATE ON progress_milestones
BEGIN SELECT RAISE(ABORT,'milestone identities retain their original meaning'); END;
CREATE TRIGGER progress_milestone_no_delete BEFORE DELETE ON progress_milestones
BEGIN SELECT RAISE(ABORT,'milestone identities retain history'); END;
-- Derived selection only; report/evidence bytes and corrections stay immutable.
CREATE TABLE progress_current (
 group_name TEXT NOT NULL,
 task TEXT NOT NULL,
 milestone TEXT NOT NULL,
 input_epoch INTEGER NOT NULL,
 judgment INTEGER NOT NULL REFERENCES progress_records(id),
 PRIMARY KEY(group_name,task,milestone,input_epoch),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task)
);

CREATE TABLE operator_notice_routes (
 group_name TEXT PRIMARY KEY REFERENCES groups(name),
 generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 route TEXT NOT NULL CHECK(json_valid(route)),
 informational INTEGER NOT NULL DEFAULT 0 CHECK(informational IN (0,1)),
 changed INTEGER NOT NULL
);
INSERT INTO operator_notice_routes(group_name,route,changed)
 SELECT g.name,json_object('notifier',json(p.notifier),'socket',g.socket),p.updated
 FROM groups g JOIN followup_policy p ON p.group_name=g.name;

-- A cause account is independent of task revisions, plan IDs and aliases.
CREATE TABLE operator_notice_accounts (
 id INTEGER PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 source_key TEXT NOT NULL,
 episode TEXT NOT NULL,
 UNIQUE(group_name,source_key,episode)
);
CREATE TABLE operator_notice_spending (
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 generation INTEGER NOT NULL,
 exposures INTEGER NOT NULL DEFAULT 0 CHECK(exposures>=0),
 next_at INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(account,generation)
);
CREATE TABLE operator_notices (
 id INTEGER PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 source_kind TEXT NOT NULL CHECK(source_kind IN ('attention_occurrence','operator_obligation')),
 source_id INTEGER NOT NULL,
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 revision INTEGER NOT NULL CHECK(revision>0),
 source_snapshot TEXT NOT NULL CHECK(json_valid(source_snapshot)),
 responsible TEXT NOT NULL,
 unresolved INTEGER NOT NULL CHECK(unresolved IN (0,1)),
 due_at INTEGER NOT NULL,
 first_dirty INTEGER NOT NULL,
 accepted_revision INTEGER NOT NULL DEFAULT 0 CHECK(accepted_revision>=0),
 accepted_generation INTEGER CHECK(accepted_generation>=0),
 state TEXT NOT NULL CHECK(state IN ('pending','reserved','exposed','accepted','failed','uncertain','unconfigured','retired')),
 batch TEXT,
 UNIQUE(group_name,source_kind,source_id)
);
CREATE INDEX operator_notice_due ON operator_notices(group_name,due_at,id);
CREATE TABLE operator_notice_batches (
 id TEXT PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 generation INTEGER NOT NULL,
 route TEXT NOT NULL CHECK(json_valid(route)),
 payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=8192 AND json_valid(payload)),
 owner TEXT NOT NULL,
 lease_until INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('reserved','exposed','accepted','failed','uncertain','cancelled')),
 exposed_at INTEGER,
 finished_at INTEGER,
 detail TEXT
);
CREATE TABLE operator_notice_batch_items (
 batch TEXT NOT NULL REFERENCES operator_notice_batches(id),
 notice INTEGER NOT NULL REFERENCES operator_notices(id),
 revision INTEGER NOT NULL,
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 source_snapshot TEXT NOT NULL CHECK(json_valid(source_snapshot)),
 PRIMARY KEY(batch,notice)
);
CREATE TRIGGER operator_batch_frozen BEFORE UPDATE OF id,group_name,generation,route,payload,owner,lease_until ON operator_notice_batches
BEGIN SELECT RAISE(ABORT,'operator batch identity and bytes are immutable'); END;
CREATE TRIGGER operator_batch_no_delete BEFORE DELETE ON operator_notice_batches
BEGIN SELECT RAISE(ABORT,'operator batches retain history'); END;
CREATE TRIGGER operator_item_no_update BEFORE UPDATE ON operator_notice_batch_items
BEGIN SELECT RAISE(ABORT,'operator batch membership is immutable'); END;
CREATE TRIGGER operator_item_no_delete BEFORE DELETE ON operator_notice_batch_items
BEGIN SELECT RAISE(ABORT,'operator batch membership retains history'); END;
CREATE TABLE operator_notice_events (
 id INTEGER PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 batch TEXT REFERENCES operator_notice_batches(id),
 kind TEXT NOT NULL,
 payload TEXT NOT NULL CHECK(json_valid(payload)),
 created INTEGER NOT NULL
);
CREATE TRIGGER operator_event_no_update BEFORE UPDATE ON operator_notice_events
BEGIN SELECT RAISE(ABORT,'operator events are immutable'); END;
CREATE TRIGGER operator_event_no_delete BEFORE DELETE ON operator_notice_events
BEGIN SELECT RAISE(ABORT,'operator events retain history'); END;
CREATE TABLE operator_notice_projection (
 group_name TEXT PRIMARY KEY REFERENCES groups(name),
 attention_after INTEGER NOT NULL DEFAULT 0,
 obligation_after INTEGER NOT NULL DEFAULT 0
);
-- Fair group selection is separate from per-source projection/spending.
CREATE TABLE operator_notice_dispatch_cursor (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 last_group TEXT NOT NULL
);
INSERT INTO operator_notice_dispatch_cursor VALUES(1,'');
CREATE TABLE operator_notice_repairs (
 group_name TEXT NOT NULL REFERENCES groups(name),
 key TEXT NOT NULL,
 canonical TEXT NOT NULL CHECK(json_valid(canonical)),
 generation INTEGER NOT NULL,
 PRIMARY KEY(group_name,key)
);

-- Preserve raw legacy facts, including aliases no longer in active_attention.
-- Historical route generation and occurrence retrieval binding were not stored.
-- NULL means unknown; the current route is merely the migration-time route.
CREATE TABLE operator_notice_legacy (
 occurrence INTEGER PRIMARY KEY REFERENCES attention_occurrences(id),
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 attempts INTEGER NOT NULL,
 state TEXT NOT NULL,
 next_at INTEGER NOT NULL,
 detail TEXT,
 occurrence_retrieved_at INTEGER,
 occurrence_retrieved_binding INTEGER,
 provenance TEXT NOT NULL CHECK(json_valid(provenance))
);
INSERT OR IGNORE INTO operator_notice_accounts(group_name,source_key,episode)
 SELECT f.group_name,CASE WHEN f.task IS NOT NULL THEN json_array('task',f.task)
 ELSE json_array('delivery',f.message,f.recipient) END,'obligation'
 FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE o.stage=3;
INSERT INTO operator_notice_legacy
 SELECT o.id,a.id,o.operator_attempts,o.operator_state,o.operator_next,o.operator_detail,
 o.retrieved_at,NULL,json_object('followup',f.id,'plan_version',o.plan_version,
 'occurrence_created',o.created,'operator_after',o.operator_after,'recipient',o.recipient,
 'original_authority',f.authority,'followup_task_version_at_migration',f.task_version,
 'followup_retrieved_at_at_migration',f.retrieved_at,
 'followup_retrieved_binding_at_migration',f.retrieved_binding,
 'historical_route_generation',NULL)
 FROM attention_occurrences o JOIN followups f ON f.id=o.followup
 JOIN operator_notice_accounts a ON a.group_name=f.group_name AND a.episode='obligation'
 AND a.source_key=CASE WHEN f.task IS NOT NULL THEN json_array('task',f.task)
 ELSE json_array('delivery',f.message,f.recipient) END WHERE o.stage=3;
-- Sum every recorded spent exposure, even aliases exceeding today's cap.
INSERT INTO operator_notice_spending(account,generation,exposures,next_at)
 SELECT account,0,sum(attempts),max(next_at) FROM operator_notice_legacy GROUP BY account;
CREATE TRIGGER operator_legacy_no_update BEFORE UPDATE ON operator_notice_legacy
BEGIN SELECT RAISE(ABORT,'legacy notice evidence is immutable'); END;
CREATE TRIGGER operator_legacy_no_delete BEFORE DELETE ON operator_notice_legacy
BEGIN SELECT RAISE(ABORT,'legacy notice evidence retains history'); END;
PRAGMA user_version=25;
