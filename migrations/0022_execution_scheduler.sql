-- Execution accounting is independent of task state and notification delivery.
CREATE TABLE execution_clock (
 group_name TEXT PRIMARY KEY REFERENCES groups(name),
 observed INTEGER NOT NULL, generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 discontinuity INTEGER NOT NULL DEFAULT 0 CHECK(discontinuity IN (0,1))
);
CREATE TABLE execution_tasks (
 group_name TEXT NOT NULL, task TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 fence INTEGER NOT NULL DEFAULT 0 CHECK(fence>=0),
 policy TEXT NOT NULL CHECK(json_valid(policy)),
 continuation TEXT CHECK(continuation IS NULL OR json_valid(continuation)),
 lifecycle_ready INTEGER NOT NULL CHECK(lifecycle_ready IN (0,1)),
 clock_ack INTEGER NOT NULL DEFAULT 0 CHECK(clock_ack>=0),
 due_at INTEGER NOT NULL, hard_due INTEGER NOT NULL,
 scanned INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(group_name,task),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task)
);
CREATE INDEX execution_due ON execution_tasks(scanned,group_name,task);
CREATE TABLE execution_budgets (
 group_name TEXT NOT NULL, task TEXT NOT NULL,
 max_attempts INTEGER NOT NULL CHECK(max_attempts>0),
 elapsed_seconds INTEGER NOT NULL CHECK(elapsed_seconds>0),
 cost_limit INTEGER CHECK(cost_limit IS NULL OR cost_limit>0), cost_unit TEXT,
 attempts_spent INTEGER NOT NULL DEFAULT 0 CHECK(attempts_spent>=0),
 attempts_reserved INTEGER NOT NULL DEFAULT 0 CHECK(attempts_reserved>=0),
 cost_spent INTEGER NOT NULL DEFAULT 0 CHECK(cost_spent>=0),
 cost_reserved INTEGER NOT NULL DEFAULT 0 CHECK(cost_reserved>=0),
 unknown_cost INTEGER NOT NULL DEFAULT 0 CHECK(unknown_cost>=0),
 anchor INTEGER, deadline INTEGER,
 PRIMARY KEY(group_name,task),
 FOREIGN KEY(group_name,task) REFERENCES task_models(group_name,task),
 CHECK((cost_limit IS NULL)=(cost_unit IS NULL)),
 CHECK((anchor IS NULL)=(deadline IS NULL))
);
CREATE TABLE execution_attempts (
 id TEXT PRIMARY KEY, group_name TEXT NOT NULL, task TEXT NOT NULL,
 fence INTEGER NOT NULL CHECK(fence>0), owner TEXT NOT NULL,
 owner_binding INTEGER NOT NULL CHECK(owner_binding>0),
 inputs TEXT NOT NULL CHECK(json_valid(inputs)),
 runtime TEXT NOT NULL CHECK(json_valid(runtime)),
 runtime_key TEXT NOT NULL, dispatch_key TEXT NOT NULL UNIQUE,
 predecessor TEXT REFERENCES execution_attempts(id),
 state TEXT NOT NULL CHECK(state IN ('reserved','dispatching','running','stop_requested','uncertain','closed')),
 holds_slot INTEGER NOT NULL DEFAULT 1 CHECK(holds_slot IN (0,1)),
 admitted INTEGER NOT NULL DEFAULT 0 CHECK(admitted IN (0,1)),
 created INTEGER NOT NULL, observed INTEGER NOT NULL,
 reconcile_at INTEGER NOT NULL, closed_at INTEGER,
 closure TEXT CHECK(closure IS NULL OR json_valid(closure)),
 UNIQUE(group_name,task,fence),
 UNIQUE(id,group_name,task),
 UNIQUE(id,group_name),
 FOREIGN KEY(group_name,task) REFERENCES execution_tasks(group_name,task),
 CHECK((state='closed')=(holds_slot=0)),
 CHECK((state='closed')=(closed_at IS NOT NULL)),
 CHECK((state='closed')=(closure IS NOT NULL))
);
CREATE UNIQUE INDEX execution_one_attempt ON execution_attempts(group_name,task) WHERE holds_slot=1;
CREATE INDEX execution_unfinished ON execution_attempts(holds_slot,reconcile_at,id);
CREATE TRIGGER execution_attempt_identity BEFORE UPDATE ON execution_attempts
WHEN OLD.id<>NEW.id OR OLD.group_name<>NEW.group_name OR OLD.task<>NEW.task
 OR OLD.fence<>NEW.fence OR OLD.owner<>NEW.owner OR OLD.owner_binding<>NEW.owner_binding
 OR OLD.inputs<>NEW.inputs OR OLD.runtime<>NEW.runtime OR OLD.runtime_key<>NEW.runtime_key
 OR OLD.dispatch_key<>NEW.dispatch_key OR OLD.predecessor IS NOT NEW.predecessor
 OR OLD.created<>NEW.created OR OLD.admitted>NEW.admitted
 OR (OLD.state='closed' AND (OLD.closure IS NOT NEW.closure OR OLD.state<>NEW.state OR OLD.closed_at IS NOT NEW.closed_at))
BEGIN SELECT RAISE(ABORT,'immutable execution identity/closure'); END;
CREATE TRIGGER execution_attempt_no_delete BEFORE DELETE ON execution_attempts
BEGIN SELECT RAISE(ABORT,'execution history cannot be deleted'); END;
CREATE TABLE execution_slots (
 runtime_key TEXT PRIMARY KEY, attempt TEXT NOT NULL UNIQUE REFERENCES execution_attempts(id)
);
CREATE TABLE execution_dispatches (
 attempt TEXT PRIMARY KEY REFERENCES execution_attempts(id),
 request TEXT NOT NULL CHECK(json_valid(request)),
 phase TEXT NOT NULL CHECK(phase IN ('prepared','exposed','settled')),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 transmissions INTEGER NOT NULL DEFAULT 0 CHECK(transmissions BETWEEN 0 AND 3),
 lease_owner TEXT, lease_until INTEGER NOT NULL DEFAULT 0
);
CREATE TRIGGER execution_dispatch_immutable BEFORE UPDATE OF request,attempt ON execution_dispatches
BEGIN SELECT RAISE(ABORT,'immutable dispatch request'); END;
CREATE TABLE execution_charges (
 attempt TEXT NOT NULL REFERENCES execution_attempts(id), group_name TEXT NOT NULL,
 account TEXT NOT NULL, cost_cap INTEGER CHECK(cost_cap IS NULL OR cost_cap>=0),
 cost_unit TEXT, settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0,1)),
 actual_cost INTEGER CHECK(actual_cost IS NULL OR actual_cost>=0),
 PRIMARY KEY(attempt,group_name,account),
 FOREIGN KEY(group_name,account) REFERENCES execution_budgets(group_name,task),
 FOREIGN KEY(attempt,group_name) REFERENCES execution_attempts(id,group_name),
 CHECK((cost_cap IS NULL)=(cost_unit IS NULL))
);
CREATE TRIGGER execution_charge_identity BEFORE UPDATE ON execution_charges
WHEN OLD.attempt<>NEW.attempt OR OLD.group_name<>NEW.group_name OR OLD.account<>NEW.account
 OR OLD.cost_cap IS NOT NEW.cost_cap OR OLD.cost_unit IS NOT NEW.cost_unit OR OLD.settled>NEW.settled
 OR (OLD.actual_cost IS NOT NULL AND OLD.actual_cost IS NOT NEW.actual_cost)
BEGIN SELECT RAISE(ABORT,'immutable execution allocation'); END;
CREATE TRIGGER execution_charge_no_delete BEFORE DELETE ON execution_charges
BEGIN SELECT RAISE(ABORT,'execution allocation cannot be deleted'); END;
CREATE TRIGGER execution_budget_lifetime BEFORE UPDATE ON execution_budgets
WHEN OLD.anchor IS NOT NULL AND (OLD.anchor IS NOT NEW.anchor OR NEW.deadline IS NULL OR NEW.deadline>OLD.deadline)
 OR NEW.attempts_spent<OLD.attempts_spent OR NEW.cost_spent<OLD.cost_spent
BEGIN SELECT RAISE(ABORT,'execution lifetime cannot be reset'); END;
CREATE TABLE execution_events (
 id INTEGER PRIMARY KEY, group_name TEXT NOT NULL, task TEXT NOT NULL,
 attempt TEXT REFERENCES execution_attempts(id), kind TEXT NOT NULL,
 payload TEXT NOT NULL CHECK(json_valid(payload)), created INTEGER NOT NULL,
 FOREIGN KEY(group_name,task) REFERENCES execution_tasks(group_name,task),
 FOREIGN KEY(attempt,group_name,task) REFERENCES execution_attempts(id,group_name,task)
);
CREATE INDEX execution_history ON execution_events(group_name,task,id);
CREATE TRIGGER execution_event_immutable_update BEFORE UPDATE ON execution_events
BEGIN SELECT RAISE(ABORT,'immutable execution event'); END;
CREATE TRIGGER execution_event_immutable_delete BEFORE DELETE ON execution_events
BEGIN SELECT RAISE(ABORT,'immutable execution event'); END;
CREATE TABLE execution_receipts (
 producer TEXT NOT NULL, key TEXT NOT NULL, canonical TEXT NOT NULL,
 result TEXT NOT NULL CHECK(json_valid(result)), PRIMARY KEY(producer,key)
);
CREATE TRIGGER execution_receipt_immutable_update BEFORE UPDATE ON execution_receipts
BEGIN SELECT RAISE(ABORT,'immutable execution receipt'); END;
CREATE TRIGGER execution_receipt_immutable_delete BEFORE DELETE ON execution_receipts
BEGIN SELECT RAISE(ABORT,'immutable execution receipt'); END;
CREATE TABLE execution_causes (
 id TEXT PRIMARY KEY, group_name TEXT NOT NULL, task TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 code TEXT NOT NULL, detail TEXT NOT NULL, responsible TEXT NOT NULL,
 opened INTEGER NOT NULL, review_at INTEGER NOT NULL, hard_due INTEGER NOT NULL,
 escalated INTEGER NOT NULL DEFAULT 0 CHECK(escalated IN (0,1)),
 case_ref TEXT, settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0,1)),
 disposition TEXT CHECK(disposition IS NULL OR json_valid(disposition)),
 FOREIGN KEY(group_name,task) REFERENCES execution_tasks(group_name,task)
);
CREATE UNIQUE INDEX execution_current_cause ON execution_causes(group_name,task,code) WHERE settled=0;
CREATE INDEX execution_cause_due ON execution_causes(settled,hard_due,id);
CREATE TABLE execution_cursors (
 group_name TEXT PRIMARY KEY REFERENCES groups(name), model_event INTEGER NOT NULL DEFAULT 0,
 last_scan INTEGER NOT NULL DEFAULT 0, driver_task TEXT NOT NULL DEFAULT ''
);
-- Controller ownership fences DB work only. Its exit never proves runtime closure.
CREATE TABLE execution_controller (
 id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 owner TEXT, last_group TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL DEFAULT 'stopped' CHECK(state IN ('running','stopped')),
 observed INTEGER NOT NULL DEFAULT 0, last_error TEXT,
 CHECK((state='running')=(owner IS NOT NULL))
);
INSERT INTO execution_controller(id) VALUES(1);
CREATE TABLE execution_controller_runs (
 generation INTEGER PRIMARY KEY CHECK(generation>0), owner TEXT NOT NULL UNIQUE,
 started INTEGER NOT NULL, finished INTEGER,
 outcome TEXT CHECK(outcome IS NULL OR json_valid(outcome)),
 CHECK((finished IS NULL)=(outcome IS NULL))
);
CREATE TABLE execution_controller_dispatches (
 id TEXT PRIMARY KEY, generation INTEGER NOT NULL REFERENCES execution_controller_runs(generation),
 owner TEXT NOT NULL, attempt TEXT NOT NULL REFERENCES execution_attempts(id),
 dispatch_revision INTEGER NOT NULL CHECK(dispatch_revision>0),
 request TEXT NOT NULL CHECK(json_valid(request)), created INTEGER NOT NULL,
 valid_until INTEGER NOT NULL CHECK(valid_until>created),
 UNIQUE(attempt,dispatch_revision)
);
CREATE TRIGGER execution_controller_dispatch_no_update BEFORE UPDATE ON execution_controller_dispatches
BEGIN SELECT RAISE(ABORT,'immutable controller dispatch receipt'); END;
CREATE TRIGGER execution_controller_dispatch_no_delete BEFORE DELETE ON execution_controller_dispatches
BEGIN SELECT RAISE(ABORT,'immutable controller dispatch receipt'); END;
CREATE TABLE execution_observations (
 attempt TEXT NOT NULL REFERENCES execution_attempts(id), sequence INTEGER NOT NULL CHECK(sequence>0),
 receipt TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
 PRIMARY KEY(attempt,sequence), UNIQUE(attempt,receipt)
);
CREATE TRIGGER execution_observation_immutable_update BEFORE UPDATE ON execution_observations
BEGIN SELECT RAISE(ABORT,'immutable execution observation'); END;
CREATE TRIGGER execution_observation_immutable_delete BEFORE DELETE ON execution_observations
BEGIN SELECT RAISE(ABORT,'immutable execution observation'); END;
CREATE TABLE execution_fk_check(violations INTEGER CHECK(violations=0));
INSERT INTO execution_fk_check SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE execution_fk_check;
PRAGMA user_version=22;
