-- A task has one persistent, writer-owned dependency contract. Edges are a
-- projection of that contract, never a second registry to keep in sync.
CREATE TABLE task_dependency_plans (
 group_name TEXT NOT NULL,
 source TEXT NOT NULL,
 plan TEXT NOT NULL CHECK(json_valid(plan) AND json_array_length(plan,'$.requirements')<=32),
 PRIMARY KEY(group_name,source),
 FOREIGN KEY(group_name,source) REFERENCES work_items(group_name,id)
);

-- Preserve former link history; give its active prerequisites explicit outcomes.
INSERT INTO task_dependency_plans(group_name,source,plan)
SELECT group_name,source,json_object('mode','all','requirements',json_group_array(
 json_object('task',target,'states',json('["done","accepted"]'),'accepted_revision',NULL)))
FROM (SELECT DISTINCT group_name,source,target FROM task_relations WHERE kind='dependency' AND active=1)
GROUP BY group_name,source;
UPDATE task_relations SET active=0 WHERE kind='dependency' AND active=1;

-- Upgrade durable wire/cache snapshots from their own historical facts, not the
-- task's later state. Envelope identities, versions and receipts stay intact.
CREATE TEMP TABLE task_graph_imports(location TEXT,group_name TEXT,key TEXT,snapshot TEXT CHECK(json_array_length(snapshot,'$.dependencies.requirements')<=32));
INSERT INTO task_graph_imports SELECT 'outbox',NULL,event_id,json_extract(payload,'$.event.data') FROM outbox
WHERE json_extract(payload,'$.event.kind')='relation_snapshot' AND json_extract(payload,'$.event.data.dependencies') IS NULL;
INSERT INTO task_graph_imports SELECT 'cache',group_name,source,snapshot FROM task_relation_snapshots
WHERE json_extract(snapshot,'$.dependencies') IS NULL;
UPDATE task_graph_imports SET snapshot=json_set(snapshot,'$.dependencies',json_object('mode','all','requirements',json((
 SELECT json_group_array(json_object('task',target,'states',json('["done","accepted"]'),'accepted_revision',NULL))
 FROM (SELECT DISTINCT json_extract(r.value,'$.target') AS target FROM json_each(task_graph_imports.snapshot,'$.relations') r
 WHERE json_extract(r.value,'$.kind')='dependency' AND json_extract(r.value,'$.active')=1)
))), '$.relations',json((SELECT json_group_array(json(CASE WHEN json_extract(r.value,'$.kind')='dependency' THEN json_set(r.value,'$.active',json('false')) ELSE r.value END)) FROM json_each(task_graph_imports.snapshot,'$.relations') r)));
UPDATE outbox SET payload=json_set(payload,'$.event.kind','task_graph_snapshot','$.event.data',json((SELECT snapshot FROM task_graph_imports WHERE location='outbox' AND key=outbox.event_id)))
WHERE event_id IN (SELECT key FROM task_graph_imports WHERE location='outbox');
UPDATE task_relation_snapshots SET snapshot=(SELECT snapshot FROM task_graph_imports i WHERE i.location='cache' AND i.group_name=task_relation_snapshots.group_name AND i.key=task_relation_snapshots.source)
WHERE EXISTS(SELECT 1 FROM task_graph_imports i WHERE i.location='cache' AND i.group_name=task_relation_snapshots.group_name AND i.key=task_relation_snapshots.source);
DROP TABLE task_graph_imports;

CREATE VIEW task_dependency_edges AS
SELECT p.group_name,p.source,json_extract(r.value,'$.task') AS target,
 r.value AS requirement
FROM task_dependency_plans p,json_each(p.plan,'$.requirements') r;

CREATE VIEW task_dependency_evaluations AS
SELECT e.*,w.state,w.accepted_revision,w.version AS target_version,
 (w.id IS NOT NULL AND EXISTS(SELECT 1 FROM json_each(e.requirement,'$.states') s WHERE s.value=w.state)
 AND (json_extract(e.requirement,'$.accepted_revision') IS NULL
 OR json_extract(e.requirement,'$.accepted_revision')=w.accepted_revision)) AS satisfied
FROM task_dependency_edges e LEFT JOIN work_items w ON w.group_name=e.group_name AND w.id=e.target;

CREATE VIEW task_readiness AS
SELECT facts.*,CASE WHEN total=0 THEN 1 WHEN mode='any' THEN satisfied>0 ELSE satisfied=total END AS ready
FROM (
 SELECT w.group_name,w.id AS task,COALESCE(json_extract(p.plan,'$.mode'),'all') AS mode,
 COALESCE(json_array_length(p.plan,'$.requirements'),0) AS total,
 (SELECT COUNT(*) FROM task_dependency_evaluations e WHERE e.group_name=w.group_name AND e.source=w.id AND e.satisfied) AS satisfied
 FROM work_items w LEFT JOIN task_dependency_plans p ON p.group_name=w.group_name AND p.source=w.id
) facts;

ALTER TABLE followups ADD COLUMN dependencies_ready INTEGER NOT NULL DEFAULT 1;
UPDATE followups SET dependencies_ready=COALESCE((SELECT ready FROM task_readiness r WHERE r.group_name=followups.group_name AND r.task=followups.task),1),scanned=0
WHERE task IS NOT NULL;

-- Invalidate queued readiness in the target transaction, before any worker can
-- claim it. A new aggregate transition gets a new attention generation while
-- retaining the hard deadline, retrieval receipts, and escalation stage.
CREATE TRIGGER task_dependency_changed AFTER UPDATE OF state,accepted_revision ON work_items
WHEN NEW.state<>OLD.state OR NEW.accepted_revision IS NOT OLD.accepted_revision
BEGIN
 UPDATE followups SET
  version=version+CASE WHEN dependencies_ready<>(SELECT ready FROM task_readiness r WHERE r.group_name=followups.group_name AND r.task=followups.task) THEN 1 ELSE 0 END,
  dependency_ready_at=CASE WHEN dependencies_ready<>(SELECT ready FROM task_readiness r WHERE r.group_name=followups.group_name AND r.task=followups.task) THEN NULL ELSE dependency_ready_at END,
  dependencies_ready=(SELECT ready FROM task_readiness r WHERE r.group_name=followups.group_name AND r.task=followups.task),scanned=0
 WHERE group_name=NEW.group_name AND task IN (SELECT source FROM task_dependency_edges WHERE group_name=NEW.group_name AND target=NEW.id);
END;
CREATE TRIGGER task_dependency_plan_changed AFTER INSERT ON task_dependency_plans
BEGIN
 UPDATE followups SET dependencies_ready=(SELECT ready FROM task_readiness WHERE group_name=NEW.group_name AND task=NEW.source),dependency_ready_at=NULL,scanned=0
 WHERE group_name=NEW.group_name AND task=NEW.source;
END;
CREATE TRIGGER task_dependency_plan_updated AFTER UPDATE ON task_dependency_plans
BEGIN
 UPDATE followups SET dependencies_ready=(SELECT ready FROM task_readiness WHERE group_name=NEW.group_name AND task=NEW.source),dependency_ready_at=NULL,scanned=0
 WHERE group_name=NEW.group_name AND task=NEW.source;
END;
CREATE INDEX task_parent_lookup ON task_relations(group_name,kind,active,target,source);

-- A subtask's material progress prompts the parent's decision owner to inspect
-- that child. This is an ordinary task fact, never an acceptance decision.
CREATE TRIGGER subtask_progress AFTER UPDATE OF state,accepted_revision ON work_items
WHEN NEW.state<>OLD.state OR NEW.accepted_revision IS NOT OLD.accepted_revision
BEGIN
 INSERT INTO coordination_events(recipient,kind,subject,version,created,wake)
 SELECT b.id,'work_changed',NEW.id,NEW.version,NEW.updated,1
 FROM task_relations r JOIN work_items p ON p.group_name=r.group_name AND p.id=r.target
 JOIN mailboxes b ON b.group_name=p.group_name AND b.name=p.writer
 WHERE r.group_name=NEW.group_name AND r.source=NEW.id AND r.kind='parent' AND r.active=1 AND p.open=1
 ON CONFLICT(recipient,kind,subject,version) DO UPDATE SET wake=1;
END;
CREATE TRIGGER subtask_created AFTER INSERT ON task_relations WHEN NEW.kind='parent' AND NEW.active=1
BEGIN
 INSERT INTO coordination_events(recipient,kind,subject,version,created,wake)
 SELECT b.id,'work_changed',c.id,NEW.version,NEW.updated,1
 FROM work_items p JOIN work_items c ON c.group_name=p.group_name AND c.id=NEW.source
 JOIN mailboxes b ON b.group_name=p.group_name AND b.name=p.writer
 WHERE p.group_name=NEW.group_name AND p.id=NEW.target AND p.open=1
 ON CONFLICT(recipient,kind,subject,version) DO UPDATE SET wake=1;
END;

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
  OR EXISTS(SELECT 1 FROM task_relations r JOIN work_items p ON p.group_name=r.group_name AND p.id=r.target WHERE r.group_name=b.group_name AND r.source=e.subject AND r.kind='parent' AND r.active=1 AND p.open=1 AND p.writer=b.name)
 ))
 OR (e.kind='attention_due' AND EXISTS(SELECT 1 FROM active_attention o JOIN followups f ON f.id=o.followup JOIN followup_policy p ON p.group_name=f.group_name WHERE CAST(o.id AS TEXT)=e.subject AND o.recipient=e.recipient AND p.mode='enabled'))
);
CREATE VIEW herdr_wake_events AS SELECT e.* FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
CREATE VIEW pending_work_events AS SELECT e.recipient,e.created,e.id FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.kind IN ('work_changed','attention_due') AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
DROP VIEW attention_events;
CREATE VIEW attention_events AS
SELECT e.*,CASE
 WHEN e.cancellation=1 THEN 'stop_work'
 WHEN e.kind='mail_pending' THEN CASE WHEN m.intent='response' THEN 'response_available' ELSE 'unread_request' END
 WHEN e.kind='work_changed' THEN CASE WHEN EXISTS(
  SELECT 1 FROM task_relations r JOIN work_items p ON p.group_name=r.group_name AND p.id=r.target
  WHERE r.group_name=b.group_name AND r.source=e.subject AND r.kind='parent' AND r.active=1 AND p.open=1 AND p.writer=b.name
 ) THEN 'subtask_changed' ELSE 'assignment_changed' END
 ELSE COALESCE((SELECT CASE o.reason WHEN 'dependency_ready' THEN 'dependency_ready' WHEN 'escalation' THEN 'review_due' ELSE 'reminder_due' END FROM attention_occurrences o WHERE o.recipient=e.recipient AND CAST(o.id AS TEXT)=e.subject),'reminder_due')
 END AS reason
FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
LEFT JOIN messages m ON e.kind='mail_pending' AND m.id=CAST(e.subject AS INTEGER)
WHERE b.agent_state='registered'
AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id)
AND (EXISTS(SELECT 1 FROM herdr_wake_events w WHERE w.id=e.id)
 OR EXISTS(SELECT 1 FROM cancellation_events c WHERE c.id=e.id));
DELETE FROM recovery_emissions;
PRAGMA user_version=27;
