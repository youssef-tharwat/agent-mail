-- Events and their fixed subscriptions are committed with the domain change.
CREATE TABLE coordination_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    recipient INTEGER NOT NULL REFERENCES mailboxes(id),
    kind TEXT NOT NULL CHECK(kind IN ('mail_pending','mail_changed','work_changed')),
    subject TEXT NOT NULL,
    version INTEGER NOT NULL,
    created INTEGER NOT NULL,
    UNIQUE(recipient,kind,subject,version)
);
CREATE INDEX coordination_by_recipient ON coordination_events(recipient,id);
CREATE TABLE event_receipts (
    recipient INTEGER NOT NULL REFERENCES mailboxes(id),
    binding_version INTEGER NOT NULL,
    event INTEGER NOT NULL REFERENCES coordination_events(id),
    PRIMARY KEY(recipient,binding_version,event)
);
CREATE TABLE hook_emissions (
    recipient INTEGER NOT NULL REFERENCES mailboxes(id),
    binding_version INTEGER NOT NULL,
    client_session TEXT NOT NULL,
    last_event INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt INTEGER NOT NULL DEFAULT 0,
    stop_used INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(recipient,binding_version,client_session)
);
CREATE TABLE work_decisions (
    actor INTEGER NOT NULL REFERENCES mailboxes(id),
    key TEXT NOT NULL,
    canonical TEXT NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY(actor,key)
);
CREATE TRIGGER mail_event_insert AFTER INSERT ON deliveries BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT NEW.recipient,'mail_pending',CAST(NEW.message AS TEXT),0,created FROM messages WHERE id=NEW.message;
END;
CREATE TRIGGER mail_event_change AFTER UPDATE OF state ON deliveries
WHEN OLD.state <> NEW.state BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT id,'mail_changed',CAST(NEW.message AS TEXT),NEW.recipient,unixepoch()
    FROM mailboxes WHERE id=NEW.recipient OR id=(SELECT sender FROM messages WHERE id=NEW.message);
END;
CREATE TRIGGER work_event_insert AFTER INSERT ON work_items BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT id,'work_changed',NEW.id,NEW.version,NEW.updated FROM mailboxes
    WHERE group_name=NEW.group_name AND name IN (NEW.owner,NEW.writer);
END;
CREATE TRIGGER work_event_update AFTER UPDATE OF version ON work_items
WHEN OLD.version <> NEW.version BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT id,'work_changed',NEW.id,NEW.version,NEW.updated FROM mailboxes
    WHERE group_name=NEW.group_name AND name IN (OLD.owner,NEW.owner,NEW.writer);
END;
-- Existing obligations are available to subscribers after upgrade.
INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
SELECT d.recipient,'mail_pending',CAST(d.message AS TEXT),0,m.created
FROM deliveries d JOIN messages m ON m.id=d.message WHERE d.state='pending';
INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
SELECT b.id,'work_changed',w.id,w.version,w.updated FROM work_items w JOIN mailboxes b
ON b.group_name=w.group_name AND b.name IN (w.owner,w.writer) WHERE w.open=1;
CREATE TRIGGER snapshot_event_insert AFTER INSERT ON work_snapshots BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT id,'work_changed',NEW.work_id,NEW.home_version,NEW.synced_at FROM mailboxes
    WHERE group_name=NEW.group_name AND name=NEW.owner;
END;
CREATE TRIGGER snapshot_event_update AFTER UPDATE OF home_version ON work_snapshots
WHEN NEW.home_version > OLD.home_version BEGIN
    INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
    SELECT id,'work_changed',NEW.work_id,NEW.home_version,NEW.synced_at FROM mailboxes
    WHERE group_name=NEW.group_name AND name IN (OLD.owner,NEW.owner);
END;
INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created)
SELECT b.id,'work_changed',s.work_id,s.home_version,s.synced_at FROM work_snapshots s JOIN mailboxes b
ON b.group_name=s.group_name AND b.name=s.owner;
CREATE VIEW pending_work_events AS
SELECT e.recipient,e.created,e.id FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
WHERE e.kind='work_changed'
AND NOT EXISTS (SELECT 1 FROM coordination_events newer WHERE newer.recipient=e.recipient AND newer.kind=e.kind AND newer.subject=e.subject AND newer.id>e.id)
AND NOT EXISTS (SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id);
PRAGMA user_version = 7;
