ALTER TABLE mailboxes ADD COLUMN agent_state TEXT NOT NULL DEFAULT 'registered' CHECK(agent_state IN ('registered','retired'));
ALTER TABLE mailboxes ADD COLUMN agent_version INTEGER NOT NULL DEFAULT 1 CHECK(agent_version > 0);
ALTER TABLE mailboxes ADD COLUMN agent_updated INTEGER NOT NULL DEFAULT 0;
UPDATE mailboxes SET agent_updated=unixepoch();
CREATE TABLE agent_changes (
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 version INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('registered','retired')),
 reason TEXT NOT NULL,
 changed INTEGER NOT NULL,
 PRIMARY KEY(recipient,version)
);
INSERT INTO agent_changes SELECT id,agent_version,agent_state,'Imported existing registration',agent_updated FROM mailboxes;
CREATE TABLE agent_decisions (
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 expected_version INTEGER NOT NULL,
 canonical TEXT NOT NULL,
 result TEXT NOT NULL,
 PRIMARY KEY(recipient,expected_version)
);
CREATE TRIGGER agent_created AFTER INSERT ON mailboxes BEGIN
 UPDATE mailboxes SET agent_updated=unixepoch() WHERE id=NEW.id;
 INSERT INTO agent_changes VALUES(NEW.id,1,NEW.agent_state,'Registered',unixepoch());
END;
CREATE TRIGGER agent_rebound AFTER UPDATE OF binding ON mailboxes
WHEN OLD.binding <> NEW.binding AND OLD.agent_state='registered' AND NEW.agent_state=OLD.agent_state BEGIN
 UPDATE mailboxes SET agent_version=agent_version+1,agent_updated=unixepoch() WHERE id=NEW.id;
 INSERT INTO agent_changes SELECT id,agent_version,agent_state,'Runtime binding changed',agent_updated FROM mailboxes WHERE id=NEW.id;
END;
CREATE TRIGGER registered_recipient BEFORE INSERT ON deliveries
WHEN EXISTS(SELECT 1 FROM mailboxes WHERE id=NEW.recipient AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'recipient is retired');
END;
CREATE TRIGGER registered_sender BEFORE INSERT ON messages
WHEN EXISTS(SELECT 1 FROM mailboxes WHERE id=NEW.sender AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'sender is retired');
END;
CREATE TRIGGER registered_task_insert BEFORE INSERT ON work_items
WHEN NEW.open=1 AND EXISTS(SELECT 1 FROM mailboxes WHERE group_name=NEW.group_name AND name IN (NEW.owner,NEW.writer) AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'open task requires registered owner and writer');
END;
CREATE TRIGGER registered_task_update BEFORE UPDATE ON work_items
WHEN NEW.open=1 AND EXISTS(SELECT 1 FROM mailboxes WHERE group_name=NEW.group_name AND name IN (NEW.owner,NEW.writer) AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'open task requires registered owner and writer');
END;
CREATE TRIGGER registered_snapshot_insert BEFORE INSERT ON work_snapshots
WHEN json_extract(NEW.snapshot,'$.state') NOT IN ('done','accepted','cancelled') AND EXISTS(SELECT 1 FROM mailboxes WHERE group_name=NEW.group_name AND name=NEW.owner AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'open snapshot requires registered owner');
END;
CREATE TRIGGER registered_snapshot_update BEFORE UPDATE ON work_snapshots
WHEN json_extract(NEW.snapshot,'$.state') NOT IN ('done','accepted','cancelled') AND EXISTS(SELECT 1 FROM mailboxes WHERE group_name=NEW.group_name AND name=NEW.owner AND agent_state='retired') BEGIN
 SELECT RAISE(ABORT,'open snapshot requires registered owner');
END;
PRAGMA user_version=15;
