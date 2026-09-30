-- Herdr retries belong to actionable event generations, not business obligations.
ALTER TABLE mailboxes ADD COLUMN wake_attempted INTEGER NOT NULL DEFAULT 0;
CREATE VIEW herdr_wake_events AS
SELECT e.* FROM wake_events e JOIN mailboxes b ON b.id=e.recipient
WHERE NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
CREATE TRIGGER herdr_wake_binding_reset AFTER UPDATE OF binding_version ON mailboxes
WHEN NEW.binding_version<>OLD.binding_version BEGIN
 UPDATE mailboxes SET wake_attempted=0,attempts=0,next_wake=0,alerted=0 WHERE id=NEW.id;
END;
PRAGMA user_version=17;
