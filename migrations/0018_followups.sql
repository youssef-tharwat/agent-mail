-- Attention metadata never changes a task or mail disposition.
CREATE TABLE followup_policy (
 group_name TEXT PRIMARY KEY REFERENCES groups(name),
 mode TEXT NOT NULL DEFAULT 'observe' CHECK(mode IN ('observe','enabled')),
 interval_seconds INTEGER NOT NULL DEFAULT 900 CHECK(interval_seconds BETWEEN 60 AND 86400),
 max_seconds INTEGER NOT NULL DEFAULT 3600 CHECK(max_seconds BETWEEN 240 AND 604800),
 notifier TEXT,
 updated INTEGER NOT NULL DEFAULT 0
);
INSERT INTO followup_policy(group_name) SELECT name FROM groups;
CREATE TRIGGER followup_group AFTER INSERT ON groups BEGIN
 INSERT INTO followup_policy(group_name) VALUES(NEW.name);
END;
CREATE TABLE followups (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 group_name TEXT NOT NULL REFERENCES groups(name),
 task TEXT,
 task_version INTEGER NOT NULL DEFAULT 0,
 message INTEGER,
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 authority INTEGER NOT NULL REFERENCES mailboxes(id),
 version INTEGER NOT NULL DEFAULT 0,
 opened INTEGER NOT NULL,
 retrieved_at INTEGER,
 retrieved_binding INTEGER,
 checkpoint TEXT,
 dependency_ready_at INTEGER,
 next_check INTEGER NOT NULL,
 escalate_at INTEGER NOT NULL,
 stage INTEGER NOT NULL DEFAULT 0 CHECK(stage BETWEEN 0 AND 3),
 scanned INTEGER NOT NULL DEFAULT 0,
 FOREIGN KEY(group_name,task) REFERENCES work_items(group_name,id),
 FOREIGN KEY(message,recipient) REFERENCES deliveries(message,recipient),
 CHECK((task IS NULL) <> (message IS NULL)),
 UNIQUE(group_name,task), UNIQUE(message,recipient)
);
CREATE INDEX followup_scan ON followups(scanned,id);
CREATE TABLE followup_history (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 followup INTEGER NOT NULL REFERENCES followups(id),
 version INTEGER NOT NULL,
 actor INTEGER NOT NULL REFERENCES mailboxes(id),
 key TEXT NOT NULL,
 canonical TEXT NOT NULL,
 snapshot TEXT NOT NULL,
 created INTEGER NOT NULL,
 UNIQUE(actor,key)
);
CREATE TABLE attention_occurrences (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 followup INTEGER NOT NULL REFERENCES followups(id),
 plan_version INTEGER NOT NULL,
 stage INTEGER NOT NULL CHECK(stage BETWEEN 1 AND 3),
 reason TEXT NOT NULL DEFAULT 'reminder' CHECK(reason IN ('reminder','escalation','dependency_ready')),
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 created INTEGER NOT NULL,
 retrieved_at INTEGER,
 operator_after INTEGER,
 operator_attempts INTEGER NOT NULL DEFAULT 0,
 operator_next INTEGER NOT NULL DEFAULT 0,
 operator_state TEXT NOT NULL DEFAULT 'pending',
 operator_detail TEXT,
 UNIQUE(followup,plan_version,stage)
);
CREATE INDEX attention_recipient ON attention_occurrences(recipient,id);
CREATE TRIGGER followup_mail AFTER INSERT ON deliveries BEGIN
 INSERT INTO followups(group_name,message,recipient,authority,opened,next_check,escalate_at)
 SELECT b.group_name,NEW.message,b.id,m.sender,m.created,m.created+p.max_seconds,m.created+p.max_seconds
 FROM messages m JOIN mailboxes b ON b.id=NEW.recipient JOIN followup_policy p ON p.group_name=b.group_name
 WHERE m.id=NEW.message AND NEW.state='pending' AND b.remote_machine IS NULL;
END;
CREATE TRIGGER followup_task AFTER INSERT ON work_items WHEN NEW.open=1 BEGIN
 INSERT INTO followups(group_name,task,task_version,recipient,authority,opened,next_check,escalate_at)
 SELECT NEW.group_name,NEW.id,NEW.version,b.id,a.id,NEW.updated,NEW.updated+p.max_seconds,NEW.updated+p.max_seconds
 FROM mailboxes b JOIN mailboxes a ON a.group_name=b.group_name AND a.name=NEW.writer
 JOIN followup_policy p ON p.group_name=b.group_name
 WHERE b.group_name=NEW.group_name AND b.name=NEW.owner AND b.remote_machine IS NULL;
END;
CREATE TRIGGER followup_task_revision AFTER UPDATE OF version ON work_items WHEN NEW.version<>OLD.version BEGIN
 UPDATE followups SET task_version=NEW.version,version=version+1,checkpoint=NULL,stage=0,
 recipient=(SELECT id FROM mailboxes WHERE group_name=NEW.group_name AND name=NEW.owner),
 retrieved_at=NULL,retrieved_binding=NULL,dependency_ready_at=NULL,
 next_check=MIN(escalate_at,NEW.updated+(SELECT interval_seconds FROM followup_policy WHERE group_name=NEW.group_name)),scanned=0
 WHERE group_name=NEW.group_name AND task=NEW.id;
 INSERT OR IGNORE INTO followups(group_name,task,task_version,recipient,authority,opened,next_check,escalate_at)
 SELECT NEW.group_name,NEW.id,NEW.version,b.id,a.id,NEW.updated,NEW.updated+p.max_seconds,NEW.updated+p.max_seconds
 FROM mailboxes b JOIN mailboxes a ON a.group_name=b.group_name AND a.name=NEW.writer
 JOIN followup_policy p ON p.group_name=b.group_name
 WHERE NEW.open=1 AND b.group_name=NEW.group_name AND b.name=NEW.owner AND b.remote_machine IS NULL;
END;
-- Historical obligations get a migration grace period and no invented retrieval.
INSERT OR IGNORE INTO followups(group_name,message,recipient,authority,opened,next_check,escalate_at)
SELECT b.group_name,d.message,b.id,m.sender,m.created,unixepoch()+3600,unixepoch()+3600
FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient
WHERE d.state='pending' AND b.remote_machine IS NULL;
INSERT OR IGNORE INTO followups(group_name,task,task_version,recipient,authority,opened,next_check,escalate_at)
SELECT w.group_name,w.id,w.version,b.id,a.id,w.updated,unixepoch()+3600,unixepoch()+3600
FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner
JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer
WHERE w.open=1 AND b.remote_machine IS NULL;
CREATE VIEW active_followups AS
SELECT f.* FROM followups f JOIN mailboxes b ON b.id=f.recipient
WHERE b.agent_state='registered' AND b.remote_machine IS NULL AND (
 (f.message IS NOT NULL AND EXISTS(SELECT 1 FROM deliveries d WHERE d.message=f.message AND d.recipient=f.recipient AND d.state='pending'))
 OR (f.task IS NOT NULL AND EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=f.group_name AND w.id=f.task AND w.open=1 AND w.version=f.task_version AND w.owner=b.name))
);
CREATE VIEW active_attention AS
SELECT o.* FROM attention_occurrences o JOIN active_followups f ON f.id=o.followup AND f.version=o.plan_version
WHERE f.stage=o.stage OR (o.reason='dependency_ready' AND f.dependency_ready_at IS NOT NULL);
-- Expand the event vocabulary while preserving event IDs and receipt foreign keys.
PRAGMA legacy_alter_table=ON;
CREATE TABLE coordination_events_new (
 id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 kind TEXT NOT NULL CHECK(kind IN ('mail_pending','mail_changed','work_changed','attention_due')),
 subject TEXT NOT NULL, version INTEGER NOT NULL, created INTEGER NOT NULL,
 wake INTEGER NOT NULL DEFAULT 1, cancellation INTEGER NOT NULL DEFAULT 0,
 UNIQUE(recipient,kind,subject,version)
);
INSERT INTO coordination_events_new SELECT id,recipient,kind,subject,version,created,wake,cancellation FROM coordination_events;
DROP TABLE coordination_events;
ALTER TABLE coordination_events_new RENAME TO coordination_events;
PRAGMA legacy_alter_table=OFF;
CREATE INDEX coordination_by_recipient ON coordination_events(recipient,id);
DROP VIEW pending_work_events;
DROP VIEW herdr_wake_events;
DROP VIEW wake_events;
CREATE VIEW wake_events AS
SELECT e.* FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.wake=1 AND NOT EXISTS(SELECT 1 FROM coordination_events n WHERE n.recipient=e.recipient AND n.kind=e.kind AND n.subject=e.subject AND n.id>e.id)
AND (
 (e.kind='mail_pending' AND EXISTS(SELECT 1 FROM deliveries d WHERE d.recipient=e.recipient AND CAST(d.message AS TEXT)=e.subject AND d.state='pending'))
 OR (e.kind='work_changed' AND (
  EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=b.group_name AND w.id=e.subject AND w.open=1 AND w.owner=b.name)
  OR EXISTS(SELECT 1 FROM work_snapshots s WHERE s.group_name=b.group_name AND s.work_id=e.subject AND s.owner=b.name AND json_extract(s.snapshot,'$.state') NOT IN ('done','accepted','cancelled'))
 ))
 OR (e.kind='attention_due' AND EXISTS(SELECT 1 FROM active_attention o JOIN followups f ON f.id=o.followup JOIN followup_policy p ON p.group_name=f.group_name WHERE CAST(o.id AS TEXT)=e.subject AND o.recipient=e.recipient AND p.mode='enabled'))
);
CREATE VIEW herdr_wake_events AS SELECT e.* FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
CREATE VIEW pending_work_events AS SELECT e.recipient,e.created,e.id FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.kind IN ('work_changed','attention_due') AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
PRAGMA user_version=18;
