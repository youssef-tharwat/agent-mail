-- Local contracts deliberately leave the legacy task and relay wire unchanged.
CREATE TABLE task_models (
    group_name TEXT NOT NULL,
    task TEXT NOT NULL,
    contract TEXT NOT NULL CHECK (json_valid(contract)),
    authorization TEXT NOT NULL CHECK (json_valid(authorization)),
    input_epoch INTEGER NOT NULL CHECK (input_epoch > 0),
    parent TEXT,
    parent_predicate TEXT CHECK (parent_predicate IS NULL OR json_valid(parent_predicate)),
    current_outcome TEXT,
    current_candidate TEXT,
    invalidation_causes TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(invalidation_causes)),
    PRIMARY KEY (group_name, task),
    FOREIGN KEY (group_name, task) REFERENCES work_items(group_name, id),
    FOREIGN KEY (group_name, parent) REFERENCES task_models(group_name, task),
    CHECK ((parent IS NULL) = (parent_predicate IS NULL)),
    CHECK (parent IS NULL OR parent <> task)
);
CREATE INDEX task_children ON task_models(group_name, parent);

CREATE TABLE task_requirements (
    group_name TEXT NOT NULL,
    consumer TEXT NOT NULL,
    prerequisite TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted','completed','cancelled','failed')),
    revision TEXT,
    PRIMARY KEY (group_name, consumer, prerequisite),
    FOREIGN KEY (group_name, consumer) REFERENCES task_models(group_name, task),
    FOREIGN KEY (group_name, prerequisite) REFERENCES task_models(group_name, task),
    CHECK (consumer <> prerequisite)
);
CREATE INDEX task_dependents ON task_requirements(group_name, prerequisite);

CREATE TABLE task_results (
    sequence INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    group_name TEXT NOT NULL,
    task TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('candidate','accepted','completed','cancelled','failed')),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created INTEGER NOT NULL,
    UNIQUE (group_name, task, id),
    FOREIGN KEY (group_name, task) REFERENCES task_models(group_name, task)
);
CREATE INDEX task_result_history ON task_results(group_name, task, created, id);
CREATE TRIGGER immutable_task_result_update BEFORE UPDATE ON task_results
BEGIN SELECT RAISE(ABORT, 'task results are immutable'); END;
CREATE TRIGGER immutable_task_result_delete BEFORE DELETE ON task_results
BEGIN SELECT RAISE(ABORT, 'task results are immutable'); END;

CREATE TABLE task_model_events (
    id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL,
    task TEXT NOT NULL,
    task_version INTEGER NOT NULL CHECK (task_version > 0),
    input_epoch INTEGER NOT NULL CHECK (input_epoch > 0),
    operation TEXT NOT NULL,
    origin TEXT NOT NULL CHECK (origin IN ('writer','policy_reviewer','system_invalidation','system_materialization')),
    actor TEXT NOT NULL,
    root_task TEXT NOT NULL,
    reason TEXT NOT NULL,
    snapshot TEXT NOT NULL CHECK (json_valid(snapshot)),
    previous_snapshot TEXT CHECK (previous_snapshot IS NULL OR json_valid(previous_snapshot)),
    created INTEGER NOT NULL,
    UNIQUE (group_name, task, operation),
    FOREIGN KEY (group_name, task) REFERENCES task_models(group_name, task)
);
CREATE INDEX task_model_event_cursor ON task_model_events(group_name, id);
CREATE TRIGGER immutable_task_event_update BEFORE UPDATE ON task_model_events
BEGIN SELECT RAISE(ABORT, 'task model events are immutable'); END;
CREATE TRIGGER immutable_task_event_delete BEFORE DELETE ON task_model_events
BEGIN SELECT RAISE(ABORT, 'task model events are immutable'); END;

CREATE TABLE task_decisions (
    actor INTEGER NOT NULL REFERENCES mailboxes(id),
    key TEXT NOT NULL,
    canonical TEXT NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY (actor, key)
);

-- Projection ownership is explicit. Other domain owners must supply validated
-- projections in the same transaction as their sources before integration.
CREATE TABLE task_blocking_edges (
    group_name TEXT NOT NULL,
    owner TEXT NOT NULL CHECK (owner IN ('model','scheduler','recovery','followup')),
    source TEXT NOT NULL,
    source_version INTEGER CHECK (source_version IS NULL OR source_version > 0),
    consumer TEXT NOT NULL CHECK (json_valid(consumer)),
    prerequisite TEXT NOT NULL CHECK (json_valid(prerequisite)),
    kind TEXT NOT NULL,
    PRIMARY KEY (group_name, owner, source, consumer, prerequisite, kind),
    FOREIGN KEY (group_name) REFERENCES groups(name)
);

-- Source policies also cover explicitly adopted legacy work and original Mail
-- deliveries. The original source writer/sender remains their immutable issuer.
CREATE TABLE task_decision_policies (
    group_name TEXT NOT NULL REFERENCES groups(name),
    id TEXT NOT NULL,
    source TEXT NOT NULL,
    issuer INTEGER NOT NULL REFERENCES mailboxes(id),
    writer TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    revoked INTEGER NOT NULL CHECK (revoked IN (0,1)),
    PRIMARY KEY (group_name, id),
    UNIQUE (group_name, source)
);
CREATE TABLE task_decision_policy_history (
    group_name TEXT NOT NULL,
    id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    source_guard TEXT NOT NULL CHECK (json_valid(source_guard)),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    issuer INTEGER NOT NULL REFERENCES mailboxes(id),
    reason TEXT NOT NULL,
    created INTEGER NOT NULL,
    PRIMARY KEY (group_name, id, revision),
    FOREIGN KEY (group_name, id) REFERENCES task_decision_policies(group_name, id)
);
CREATE TRIGGER immutable_decision_policy_history_update BEFORE UPDATE ON task_decision_policy_history
BEGIN SELECT RAISE(ABORT, 'decision policy history is immutable'); END;
CREATE TRIGGER immutable_decision_policy_history_delete BEFORE DELETE ON task_decision_policy_history
BEGIN SELECT RAISE(ABORT, 'decision policy history is immutable'); END;

-- Narrow grants for already contracted work.
CREATE TABLE task_grants (
    group_name TEXT NOT NULL,
    task TEXT NOT NULL,
    id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    kind TEXT NOT NULL CHECK (kind IN ('decision','progress_judge')),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    revoked INTEGER NOT NULL CHECK (revoked IN (0,1)),
    PRIMARY KEY (group_name, task, id),
    FOREIGN KEY (group_name, task) REFERENCES task_models(group_name, task)
);
CREATE TABLE task_grant_history (
    group_name TEXT NOT NULL,
    task TEXT NOT NULL,
    id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created INTEGER NOT NULL,
    PRIMARY KEY (group_name, task, id, revision),
    FOREIGN KEY (group_name, task, id) REFERENCES task_grants(group_name, task, id)
);
CREATE TABLE task_materializations (
    group_name TEXT NOT NULL,
    source TEXT NOT NULL,
    episode TEXT NOT NULL,
    decision_task TEXT NOT NULL,
    receipt TEXT NOT NULL CHECK (json_valid(receipt)),
    PRIMARY KEY (group_name, source, episode),
    UNIQUE (group_name, decision_task),
    FOREIGN KEY (group_name, decision_task) REFERENCES task_models(group_name, task)
);
CREATE TRIGGER immutable_task_grant_history_update BEFORE UPDATE ON task_grant_history
BEGIN SELECT RAISE(ABORT, 'task grant history is immutable'); END;
CREATE TRIGGER immutable_task_grant_history_delete BEFORE DELETE ON task_grant_history
BEGIN SELECT RAISE(ABORT, 'task grant history is immutable'); END;
CREATE TRIGGER immutable_task_materialization_update BEFORE UPDATE ON task_materializations
BEGIN SELECT RAISE(ABORT, 'task materialization receipts are immutable'); END;
CREATE TRIGGER immutable_task_materialization_delete BEFORE DELETE ON task_materializations
BEGIN SELECT RAISE(ABORT, 'task materialization receipts are immutable'); END;

-- Setup temporarily disables FK enforcement; fail inside its transaction.
CREATE TABLE task_model_fk_check (violations INTEGER CHECK (violations = 0));
INSERT INTO task_model_fk_check SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE task_model_fk_check;
PRAGMA user_version = 21;
