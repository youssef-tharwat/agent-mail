-- Wake intent is separate from durable event history and delivery receipts.
ALTER TABLE coordination_events ADD COLUMN wake INTEGER NOT NULL DEFAULT 1;
ALTER TABLE coordination_events ADD COLUMN cancellation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE codex_wakes ADD COLUMN scanned INTEGER NOT NULL DEFAULT 0;
UPDATE codex_wakes SET scanned=delivered;
UPDATE coordination_events SET wake=0 WHERE kind='mail_changed';
-- Historical work origins are not assumed: a replacement must still recover them.
DROP TRIGGER work_event_insert;
DROP TRIGGER work_event_update;
CREATE TRIGGER work_event_insert AFTER INSERT ON work_items BEGIN
 INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created,wake,cancellation)
 SELECT b.id,'work_changed',NEW.id,NEW.version,NEW.updated,b.name=NEW.owner,0
 FROM mailboxes b
 WHERE b.group_name=NEW.group_name AND b.name IN (NEW.owner,NEW.writer);
END;
CREATE TRIGGER work_event_update AFTER UPDATE OF version ON work_items WHEN OLD.version<>NEW.version BEGIN
 INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created,wake,cancellation)
 SELECT b.id,'work_changed',NEW.id,NEW.version,NEW.updated,
  (b.name=NEW.owner AND NEW.open=1),
  (b.name=OLD.owner AND (NEW.open=0 OR NEW.owner<>OLD.owner))
 FROM mailboxes b
 WHERE b.group_name=NEW.group_name AND b.name IN (OLD.owner,NEW.owner,NEW.writer);
END;
DROP TRIGGER mail_event_change;
CREATE TRIGGER mail_event_change AFTER UPDATE OF state ON deliveries WHEN OLD.state<>NEW.state BEGIN
 INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created,wake)
 SELECT id,'mail_changed',CAST(NEW.message AS TEXT),NEW.recipient,unixepoch(),0
 FROM mailboxes WHERE id=NEW.recipient OR id=(SELECT sender FROM messages WHERE id=NEW.message);
END;
CREATE VIEW wake_events AS
SELECT e.* FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.wake=1
AND NOT EXISTS(SELECT 1 FROM coordination_events n WHERE n.recipient=e.recipient AND n.kind=e.kind AND n.subject=e.subject AND n.id>e.id)
AND (
 (e.kind='mail_pending' AND EXISTS(SELECT 1 FROM deliveries d WHERE d.recipient=e.recipient AND CAST(d.message AS TEXT)=e.subject AND d.state='pending'))
 OR (e.kind='work_changed' AND (
  EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=b.group_name AND w.id=e.subject AND (w.open=1 AND w.owner=b.name))
  OR EXISTS(SELECT 1 FROM work_snapshots s WHERE s.group_name=b.group_name AND s.work_id=e.subject AND (json_extract(s.snapshot,'$.open')=1 AND s.owner=b.name))
 ))
);
CREATE VIEW cancellation_events AS
SELECT e.* FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.kind='work_changed' AND e.cancellation=1
AND NOT EXISTS(SELECT 1 FROM coordination_events n WHERE n.recipient=e.recipient AND n.kind=e.kind AND n.subject=e.subject AND n.id>e.id)
AND EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=b.group_name AND w.id=e.subject AND (w.open=0 OR w.owner<>b.name));
DROP VIEW pending_work_events;
CREATE VIEW pending_work_events AS
SELECT e.recipient,e.created,e.id FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.kind='work_changed' AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
PRAGMA user_version=9;
