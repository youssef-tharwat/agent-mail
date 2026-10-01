-- Additive recovery state. Execution22 and runtime23 must already be installed.
CREATE TEMP TABLE decision_schema_guard(version INTEGER CHECK(version = 23));
INSERT INTO decision_schema_guard SELECT user_version FROM pragma_user_version;
DROP TABLE decision_schema_guard;

CREATE TABLE decision_cases (
    id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL REFERENCES groups(name),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('task','delivery')),
    task TEXT,
    message INTEGER,
    recipient INTEGER,
    source_key TEXT NOT NULL,
    episode TEXT NOT NULL,
    authority TEXT NOT NULL,
    original_source TEXT NOT NULL CHECK(json_valid(original_source)),
    current_source TEXT NOT NULL CHECK(json_valid(current_source)),
    execution_source TEXT CHECK(execution_source IS NULL OR json_valid(execution_source)),
    execution_original_guard TEXT CHECK(execution_original_guard IS NULL OR json_valid(execution_original_guard)),
    execution_guard TEXT CHECK(execution_guard IS NULL OR json_valid(execution_guard)),
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    opened INTEGER NOT NULL,
    original_due INTEGER NOT NULL,
    review_at INTEGER NOT NULL,
    hard_due INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('held','decision_pending','operator_required','handled','superseded')),
    capability_hold TEXT,
    decision_task TEXT,
    policy_ref TEXT CHECK(policy_ref IS NULL OR json_valid(policy_ref)),
    reviewer_advanced INTEGER NOT NULL DEFAULT 0 CHECK(reviewer_advanced IN (0,1)),
    requires_reassessment INTEGER NOT NULL DEFAULT 0 CHECK(requires_reassessment IN (0,1)),
    reassessment_event INTEGER REFERENCES task_model_events(id),
    last_scan INTEGER NOT NULL DEFAULT 0,
    UNIQUE(group_name,id),
    UNIQUE(group_name,source_key,episode),
    UNIQUE(group_name,decision_task),
    FOREIGN KEY(group_name,task) REFERENCES work_items(group_name,id),
    FOREIGN KEY(message,recipient) REFERENCES deliveries(message,recipient),
    FOREIGN KEY(group_name,authority) REFERENCES mailboxes(group_name,name),
    FOREIGN KEY(group_name,decision_task) REFERENCES task_models(group_name,task),
    CHECK((source_kind='task' AND task IS NOT NULL AND message IS NULL AND recipient IS NULL)
       OR (source_kind='delivery' AND task IS NULL AND message IS NOT NULL AND recipient IS NOT NULL)),
    CHECK(review_at <= hard_due),
    CHECK((execution_source IS NULL) = (execution_guard IS NULL)),
    CHECK((execution_source IS NULL) = (execution_original_guard IS NULL))
);
CREATE INDEX decision_due ON decision_cases(group_name,state,last_scan,id);
CREATE TRIGGER immutable_decision_identity BEFORE UPDATE ON decision_cases
WHEN NEW.group_name<>OLD.group_name OR NEW.source_kind<>OLD.source_kind
  OR NEW.task IS NOT OLD.task OR NEW.message IS NOT OLD.message OR NEW.recipient IS NOT OLD.recipient
  OR NEW.source_key<>OLD.source_key OR NEW.episode<>OLD.episode OR NEW.authority<>OLD.authority
  OR NEW.original_source<>OLD.original_source OR NEW.opened<>OLD.opened OR NEW.original_due<>OLD.original_due
  OR NEW.execution_source IS NOT OLD.execution_source OR NEW.execution_original_guard IS NOT OLD.execution_original_guard
  OR (OLD.decision_task IS NOT NULL AND NEW.decision_task IS NOT OLD.decision_task)
  OR NEW.reviewer_advanced<OLD.reviewer_advanced
BEGIN SELECT RAISE(ABORT, 'decision identity and original evidence are immutable'); END;

-- Source semantics belong here; complete graph projection belongs to task_graph.
CREATE TABLE decision_blockers (
    group_name TEXT NOT NULL,
    case_id INTEGER NOT NULL,
    case_version INTEGER NOT NULL CHECK(case_version > 0),
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 31),
    selector TEXT NOT NULL CHECK(json_valid(selector)),
    waiting TEXT CHECK(waiting IS NULL OR json_valid(waiting)),
    responsible TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence TEXT NOT NULL CHECK(json_valid(evidence)),
    PRIMARY KEY(group_name,case_id,ordinal),
    FOREIGN KEY(group_name,case_id) REFERENCES decision_cases(group_name,id)
);

CREATE TABLE decision_audit (
    id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL REFERENCES groups(name),
    case_id INTEGER,
    actor INTEGER REFERENCES mailboxes(id),
    key TEXT NOT NULL,
    operation TEXT NOT NULL,
    canonical TEXT NOT NULL CHECK(json_valid(canonical)),
    result TEXT NOT NULL CHECK(json_valid(result)),
    created INTEGER NOT NULL,
    FOREIGN KEY(group_name,case_id) REFERENCES decision_cases(group_name,id)
);
CREATE UNIQUE INDEX decision_actor_key ON decision_audit(actor,key) WHERE actor IS NOT NULL;
CREATE INDEX decision_case_history ON decision_audit(group_name,case_id,id);
CREATE TRIGGER immutable_decision_audit_update BEFORE UPDATE ON decision_audit
BEGIN SELECT RAISE(ABORT, 'decision audit is immutable'); END;
CREATE TRIGGER immutable_decision_audit_delete BEFORE DELETE ON decision_audit
BEGIN SELECT RAISE(ABORT, 'decision audit is immutable'); END;

-- Business responsibility is independent of notifier transport. Schema25 owns
-- notices, routes, leases, transmission counts and receipts. No forward FK.
CREATE TABLE operator_obligations (
    id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL,
    case_id INTEGER NOT NULL,
    authority TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    opened INTEGER NOT NULL,
    hard_due INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','escalated','handled','superseded')),
    reason TEXT NOT NULL,
    evidence TEXT NOT NULL CHECK(json_valid(evidence)),
    last_scan INTEGER NOT NULL DEFAULT 0,
    UNIQUE(group_name,id),
    UNIQUE(group_name,case_id),
    FOREIGN KEY(group_name,case_id) REFERENCES decision_cases(group_name,id),
    FOREIGN KEY(group_name,authority) REFERENCES mailboxes(group_name,name)
);
CREATE INDEX operator_due ON operator_obligations(group_name,state,last_scan,id);

CREATE TABLE decision_supervision (
    group_name TEXT PRIMARY KEY REFERENCES groups(name),
    generation INTEGER NOT NULL DEFAULT 1 CHECK(generation > 0),
    heartbeat INTEGER,
    source_cursor INTEGER NOT NULL DEFAULT 0 CHECK(source_cursor >= 0),
    execution_cursor TEXT NOT NULL DEFAULT '',
    execution_passes INTEGER NOT NULL DEFAULT 0 CHECK(execution_passes >= 0),
    case_cursor INTEGER NOT NULL DEFAULT 0 CHECK(case_cursor >= 0),
    missing_task_cursor TEXT NOT NULL DEFAULT '',
    missing_mail_cursor INTEGER NOT NULL DEFAULT 0 CHECK(missing_mail_cursor >= 0),
    missing_recipient_cursor INTEGER NOT NULL DEFAULT 0 CHECK(missing_recipient_cursor >= 0),
    source_passes INTEGER NOT NULL DEFAULT 0 CHECK(source_passes >= 0),
    case_passes INTEGER NOT NULL DEFAULT 0 CHECK(case_passes >= 0),
    missing_task_passes INTEGER NOT NULL DEFAULT 0 CHECK(missing_task_passes >= 0),
    missing_mail_passes INTEGER NOT NULL DEFAULT 0 CHECK(missing_mail_passes >= 0),
    completed_scans INTEGER NOT NULL DEFAULT 0 CHECK(completed_scans >= 0),
    last_full_scan INTEGER,
    unresolved INTEGER NOT NULL DEFAULT 0 CHECK(unresolved >= 0),
    capability_hold TEXT NOT NULL DEFAULT 'model_materialization_scheduler_ack_and_shared_notifier_unavailable'
);
INSERT INTO decision_supervision(group_name) SELECT name FROM groups;
CREATE TRIGGER decision_supervision_group AFTER INSERT ON groups BEGIN
    INSERT INTO decision_supervision(group_name) VALUES(NEW.name);
END;

CREATE TEMP TABLE decision_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO decision_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE decision_fk_guard;
PRAGMA user_version=24;
