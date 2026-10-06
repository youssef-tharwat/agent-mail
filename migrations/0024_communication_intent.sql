-- Existing messages, including replies, retain their original obligations.
ALTER TABLE messages ADD COLUMN intent TEXT NOT NULL DEFAULT 'request'
CHECK(intent IN ('request','notice','response'))
CHECK(intent='request' OR deadline IS NULL)
CHECK(intent<>'response' OR reply_to IS NOT NULL);

CREATE TABLE relay_capabilities (
 machine TEXT NOT NULL,
 capability TEXT NOT NULL,
 PRIMARY KEY(machine,capability)
);
CREATE TABLE attention_dispatch (
 recipient INTEGER PRIMARY KEY REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 token TEXT NOT NULL UNIQUE,
 consumer TEXT NOT NULL,
 expires INTEGER NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 1,
 items TEXT NOT NULL CHECK(json_valid(items))
);
ALTER TABLE claude_inboxes ADD COLUMN pending_attention TEXT;
-- Previous queued prompts carry no exact batch identity. Recover their sources
-- through the current projection rather than accepting an unscoped receipt.
UPDATE claude_inboxes SET pending_id=NULL,pending_event=0;
CREATE TABLE attention_batch_receipts (
 token TEXT PRIMARY KEY,
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL
);
CREATE TABLE attention_attempts (
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 event INTEGER NOT NULL REFERENCES coordination_events(id),
 attempts INTEGER NOT NULL CHECK(attempts BETWEEN 1 AND 3),
 next_attempt INTEGER NOT NULL,
 alerted INTEGER NOT NULL DEFAULT 0 CHECK(alerted IN (0,1)),
 PRIMARY KEY(recipient,binding_version,event)
);
-- Carry saved attempts into the per-reason ledger once during migration.
-- A schema upgrade must not restart an already exhausted delivery budget.
WITH budgets AS (
 SELECT e.recipient,b.binding_version,e.id AS event,
 MIN(3,MAX(CASE WHEN b.wake_attempted>=e.id THEN b.attempts ELSE 0 END,
 CASE WHEN n.attempted>=e.id THEN n.attempts ELSE 0 END)) AS attempts,
 MAX(CASE WHEN b.wake_attempted>=e.id THEN b.next_wake ELSE 0 END,
 CASE WHEN n.attempted>=e.id THEN n.next_attempt ELSE 0 END) AS next_attempt,b.alerted
 FROM herdr_wake_events e JOIN mailboxes b ON b.id=e.recipient
 LEFT JOIN runtime_wakes n ON n.recipient=b.id AND n.binding_version=b.binding_version
)
INSERT INTO attention_attempts(recipient,binding_version,event,attempts,next_attempt,alerted)
SELECT recipient,binding_version,event,attempts,next_attempt,alerted FROM budgets WHERE attempts>0;
CREATE TABLE message_observations (
 message INTEGER NOT NULL,
 recipient INTEGER NOT NULL,
 binding_version INTEGER NOT NULL,
 PRIMARY KEY(message,recipient,binding_version),
 FOREIGN KEY(message,recipient) REFERENCES deliveries(message,recipient)
);
CREATE TABLE external_pr_facts (
 repository TEXT NOT NULL,
 number INTEGER NOT NULL CHECK(number>0),
 state TEXT CHECK(state IN ('open','closed','merged')),
 head TEXT,
 merge_commit TEXT,
 checked_at INTEGER NOT NULL,
 error TEXT,
 PRIMARY KEY(repository,number)
);
-- Recovery metadata only tracks which guide/context epoch has been restored.
-- Refresh running sessions once because their delivery instructions changed.
DROP TABLE hook_emissions;
CREATE TABLE recovery_emissions (
 recipient INTEGER NOT NULL REFERENCES mailboxes(id),
 binding_version INTEGER NOT NULL,
 client_session TEXT NOT NULL,
 PRIMARY KEY(recipient,binding_version,client_session)
);

DROP TRIGGER mail_event_insert;
CREATE TRIGGER mail_event_insert AFTER INSERT ON deliveries BEGIN
 INSERT OR IGNORE INTO coordination_events(recipient,kind,subject,version,created,wake)
 SELECT NEW.recipient,'mail_pending',CAST(NEW.message AS TEXT),0,created,intent<>'notice'
 FROM messages WHERE id=NEW.message;
END;
DROP TRIGGER followup_mail;
CREATE TRIGGER followup_mail AFTER INSERT ON deliveries BEGIN
 INSERT INTO followups(group_name,message,recipient,authority,opened,next_check,escalate_at)
 SELECT b.group_name,NEW.message,b.id,m.sender,m.created,m.created+p.max_seconds,m.created+p.max_seconds
 FROM messages m JOIN mailboxes b ON b.id=NEW.recipient JOIN followup_policy p ON p.group_name=b.group_name
 WHERE m.id=NEW.message AND m.intent='request' AND NEW.state='pending' AND b.remote_machine IS NULL;
END;

-- Link authenticated responses independently of their transport. Receiving a
-- response satisfies a reply predicate without changing the request disposition.
CREATE TRIGGER response_link AFTER INSERT ON deliveries
WHEN (SELECT intent FROM messages WHERE id=NEW.message)='response'
BEGIN
 UPDATE deliveries SET reply_id=COALESCE(reply_id,NEW.message)
 WHERE message=(SELECT reply_to FROM messages WHERE id=NEW.message)
 AND recipient=(SELECT sender FROM messages WHERE id=NEW.message)
 AND EXISTS(SELECT 1 FROM messages parent WHERE parent.id=deliveries.message
 AND parent.sender=NEW.recipient AND parent.intent='request');
END;

-- Audit subscribers keep all events; model consumers share this current projection.
CREATE INDEX attention_delivery_order ON coordination_events(recipient,cancellation DESC,id);
CREATE VIEW attention_events AS
SELECT e.*,CASE
 WHEN e.cancellation=1 THEN 'stop_work'
 WHEN e.kind='mail_pending' THEN CASE WHEN m.intent='response' THEN 'response_available' ELSE 'unread_request' END
 WHEN e.kind='work_changed' THEN 'assignment_changed'
 ELSE COALESCE((SELECT CASE o.reason WHEN 'dependency_ready' THEN 'dependency_ready' WHEN 'escalation' THEN 'review_due' ELSE 'reminder_due' END FROM attention_occurrences o WHERE o.recipient=e.recipient AND CAST(o.id AS TEXT)=e.subject),'reminder_due')
 END AS reason
FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient
LEFT JOIN messages m ON e.kind='mail_pending' AND m.id=CAST(e.subject AS INTEGER)
WHERE b.agent_state='registered'
AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id)
AND (EXISTS(SELECT 1 FROM herdr_wake_events w WHERE w.id=e.id)
 OR EXISTS(SELECT 1 FROM cancellation_events c WHERE c.id=e.id));
PRAGMA user_version=24;
