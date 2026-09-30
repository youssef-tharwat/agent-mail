-- Only new groups change default. Existing operator choices survive upgrades.
DROP TRIGGER followup_group;
CREATE TRIGGER followup_group AFTER INSERT ON groups BEGIN
 INSERT INTO followup_policy(group_name,mode) VALUES(NEW.name,'enabled');
END;

-- A durable offer describes exactly the records included in a runtime input.
-- Acceptance alone is never a completed turn or a business disposition.
CREATE TABLE turn_offers (
 id TEXT PRIMARY KEY,
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 runtime TEXT NOT NULL CHECK(runtime IN ('codex','hook')),
 session TEXT NOT NULL,
 turn TEXT,
 state TEXT NOT NULL DEFAULT 'offered' CHECK(state IN ('offered','completed','abandoned')),
 created INTEGER NOT NULL,
 completed_at INTEGER
);
CREATE INDEX turn_offers_pending ON turn_offers(recipient,binding_version,runtime,session,state);
CREATE TABLE turn_offer_items (
 offer TEXT NOT NULL REFERENCES turn_offers(id),
 followup INTEGER NOT NULL REFERENCES followups(id),
 plan_version INTEGER NOT NULL,
 stage INTEGER NOT NULL,
 PRIMARY KEY(offer,followup,plan_version,stage)
);

-- Dependency changes go to the front of the next hinted reconciliation page.
CREATE TRIGGER followup_dependency_task AFTER UPDATE ON work_items BEGIN
 UPDATE followups SET scanned=0 WHERE group_name=NEW.group_name
 AND json_extract(checkpoint,'$.waiting.kind')='task'
 AND json_extract(checkpoint,'$.waiting.id')=NEW.id;
END;
CREATE TRIGGER followup_dependency_mail AFTER UPDATE OF state ON deliveries BEGIN
 UPDATE followups SET scanned=0 WHERE json_extract(checkpoint,'$.waiting.kind')='mail'
 AND json_extract(checkpoint,'$.waiting.id')=NEW.message;
END;
CREATE TRIGGER followup_dependency_reply AFTER INSERT ON messages WHEN NEW.reply_to IS NOT NULL BEGIN
 UPDATE followups SET scanned=0 WHERE json_extract(checkpoint,'$.waiting.kind')='mail'
 AND json_extract(checkpoint,'$.waiting.id')=NEW.reply_to;
END;
PRAGMA user_version=19;
