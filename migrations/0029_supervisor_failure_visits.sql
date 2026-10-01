-- Original scheduler visit identity and Recovery's atomic owner commit evidence.
CREATE TABLE execution_supervisor_visits (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 nonce TEXT NOT NULL UNIQUE CHECK(length(nonce)=36),
 group_name TEXT NOT NULL REFERENCES groups(name),
 home_machine TEXT NOT NULL,
 job TEXT NOT NULL CHECK(job='supervise'),
 generation INTEGER NOT NULL REFERENCES execution_controller_runs(generation),
 owner TEXT NOT NULL,
 attempt_sequence INTEGER NOT NULL UNIQUE CHECK(attempt_sequence>0),
 opened INTEGER NOT NULL CHECK(opened>=0),
 deadline INTEGER NOT NULL CHECK(deadline>opened AND deadline-opened<=10),
 page_nonce TEXT CHECK(page_nonce IS NULL OR length(page_nonce)=36),
 gate TEXT NOT NULL DEFAULT 'open' CHECK(gate IN ('open','closed')),
 closed_at INTEGER CHECK(closed_at IS NULL OR closed_at>=deadline),
 UNIQUE(id,nonce,group_name),
 CHECK((gate='open')=(closed_at IS NULL))
);
CREATE INDEX execution_supervisor_visit_due
 ON execution_supervisor_visits(group_name,job,gate,deadline,id);
CREATE TRIGGER execution_supervisor_visit_identity BEFORE UPDATE ON execution_supervisor_visits
 WHEN NEW.id<>OLD.id OR NEW.nonce<>OLD.nonce OR NEW.group_name<>OLD.group_name
 OR NEW.home_machine<>OLD.home_machine OR NEW.job<>OLD.job OR NEW.generation<>OLD.generation
 OR NEW.owner<>OLD.owner OR NEW.attempt_sequence<>OLD.attempt_sequence
 OR NEW.opened<>OLD.opened OR NEW.deadline<>OLD.deadline
 OR OLD.gate='closed'
 OR NOT ((NEW.gate='open' AND OLD.page_nonce IS NULL AND NEW.page_nonce IS NOT NULL)
     OR (NEW.gate='closed' AND NEW.page_nonce IS OLD.page_nonce))
 BEGIN SELECT RAISE(ABORT,'immutable supervisor visit or closed gate'); END;
CREATE TRIGGER execution_supervisor_visit_no_delete BEFORE DELETE ON execution_supervisor_visits
 BEGIN SELECT RAISE(ABORT,'immutable supervisor visit'); END;

CREATE TABLE execution_supervisor_commits (
 visit INTEGER PRIMARY KEY,
 nonce TEXT NOT NULL,
 group_name TEXT NOT NULL,
 schema INTEGER NOT NULL CHECK(schema IN (1,2)),
 canonical TEXT NOT NULL CHECK(json_valid(canonical) AND length(CAST(canonical AS BLOB))<=32768),
 digest TEXT NOT NULL CHECK(length(digest)=64),
 FOREIGN KEY(visit,nonce,group_name) REFERENCES execution_supervisor_visits(id,nonce,group_name)
);
CREATE TRIGGER execution_supervisor_commit_no_update BEFORE UPDATE ON execution_supervisor_commits
 BEGIN SELECT RAISE(ABORT,'immutable supervisor owner receipt'); END;
CREATE TRIGGER execution_supervisor_commit_no_delete BEFORE DELETE ON execution_supervisor_commits
 BEGIN SELECT RAISE(ABORT,'immutable supervisor owner receipt'); END;

CREATE TABLE execution_supervisor_failures (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 group_name TEXT NOT NULL REFERENCES groups(name),
 home_machine TEXT NOT NULL,
 job TEXT NOT NULL CHECK(job='supervise'),
 first_visit INTEGER NOT NULL UNIQUE REFERENCES execution_supervisor_visits(id),
 episode TEXT NOT NULL UNIQUE CHECK(length(episode)=36),
 source_key TEXT NOT NULL CHECK(json_valid(source_key)),
 opened INTEGER NOT NULL CHECK(opened>=0),
 due_at INTEGER NOT NULL CHECK(due_at>opened),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 state TEXT NOT NULL DEFAULT 'unresolved' CHECK(state IN ('unresolved','resolved')),
 settlement_visit INTEGER REFERENCES execution_supervisor_commits(visit),
 CHECK((state='unresolved')=(settlement_visit IS NULL))
);
CREATE UNIQUE INDEX execution_supervisor_failure_original
 ON execution_supervisor_failures(group_name,home_machine,job) WHERE state='unresolved';
CREATE INDEX execution_supervisor_failure_inventory
 ON execution_supervisor_failures(group_name,job,state,id);
CREATE INDEX execution_supervisor_failure_projection
 ON execution_supervisor_failures(group_name,id);
CREATE TRIGGER execution_supervisor_failure_identity BEFORE UPDATE ON execution_supervisor_failures
 WHEN NEW.id<>OLD.id OR NEW.group_name<>OLD.group_name OR NEW.home_machine<>OLD.home_machine
 OR NEW.job<>OLD.job OR NEW.first_visit<>OLD.first_visit OR NEW.episode<>OLD.episode
 OR NEW.source_key<>OLD.source_key OR NEW.opened<>OLD.opened OR NEW.due_at<>OLD.due_at
 OR OLD.state='resolved' OR NEW.revision<>OLD.revision+1
 BEGIN SELECT RAISE(ABORT,'immutable supervisor failure identity'); END;
CREATE TRIGGER execution_supervisor_failure_no_delete BEFORE DELETE ON execution_supervisor_failures
 BEGIN SELECT RAISE(ABORT,'immutable supervisor failure responsibility'); END;

CREATE TABLE execution_supervisor_failure_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 failure INTEGER NOT NULL REFERENCES execution_supervisor_failures(id),
 visit INTEGER NOT NULL REFERENCES execution_supervisor_visits(id),
 kind TEXT NOT NULL CHECK(kind IN ('first_failure','visit_failed','resolved')),
 created INTEGER NOT NULL CHECK(created>=0),
 payload TEXT NOT NULL CHECK(json_valid(payload) AND length(CAST(payload AS BLOB))<=4096),
 UNIQUE(failure,visit,kind)
);
CREATE TRIGGER execution_supervisor_failure_event_no_update BEFORE UPDATE ON execution_supervisor_failure_events
 BEGIN SELECT RAISE(ABORT,'immutable supervisor failure event'); END;
CREATE TRIGGER execution_supervisor_failure_event_no_delete BEFORE DELETE ON execution_supervisor_failure_events
 BEGIN SELECT RAISE(ABORT,'immutable supervisor failure event'); END;
CREATE TEMP TABLE supervisor_failure_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO supervisor_failure_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE supervisor_failure_fk_guard;
PRAGMA user_version=29;
