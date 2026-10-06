-- Historical mail never recorded the task version the sender observed. Preserve
-- its task association and disposition, and recover conversation roots instead
-- of inventing an observed task revision. Queued relay traffic joins the same
-- root calculation, including messages forwarded without a local inbox copy.
UPDATE messages SET global_id =
 lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
 substr(lower(hex(randomblob(2))),2) || '-8' ||
 substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6)))
WHERE global_id IS NULL;

ALTER TABLE messages ADD COLUMN context TEXT
CHECK(context IS NULL OR COALESCE(CASE WHEN json_valid(context) THEN
 CASE json_extract(context,'$.kind')
 WHEN 'task' THEN
  json_type(context,'$.id')='text' AND
  length(json_extract(context,'$.id')) BETWEEN 1 AND 48 AND
  json_extract(context,'$.id') NOT GLOB '*[^A-Za-z0-9_.-]*' AND
  json_type(context,'$.version')='integer' AND json_extract(context,'$.version')>0 AND
  COALESCE(json_extract(context,'$.id')=work_id,0)
 WHEN 'conversation' THEN
  json_type(context,'$.id')='text' AND length(json_extract(context,'$.id'))=36 AND
  substr(json_extract(context,'$.id'),9,1)='-' AND
  substr(json_extract(context,'$.id'),14,1)='-' AND
  substr(json_extract(context,'$.id'),19,1)='-' AND
  substr(json_extract(context,'$.id'),24,1)='-' AND
  length(replace(json_extract(context,'$.id'),'-',''))=32 AND
  replace(json_extract(context,'$.id'),'-','') NOT GLOB '*[^0-9a-f]*' AND
  json_extract(context,'$.id')<>'00000000-0000-0000-0000-000000000000'
 ELSE 0 END
 ELSE 0 END,0));

CREATE TEMP TABLE mail_context_roots (
 id TEXT PRIMARY KEY NOT NULL,
 root TEXT NOT NULL
);
WITH RECURSIVE
 queued AS (
  SELECT CASE json_extract(payload,'$.event.kind')
   WHEN 'message' THEN json_extract(payload,'$.event.data')
   ELSE json_extract(payload,'$.event.data.message') END AS message
  FROM outbox WHERE json_extract(payload,'$.event.kind') IN ('message','typed_message')
 ),
 links(id,parent) AS (
  SELECT m.global_id,p.global_id FROM messages m LEFT JOIN messages p ON p.id=m.reply_to
  UNION
  SELECT json_extract(message,'$.id'),json_extract(message,'$.reply_to') FROM queued
 ),
 roots(id,root) AS (
  SELECT id,COALESCE(parent,id) FROM links l
  WHERE parent IS NULL OR NOT EXISTS(SELECT 1 FROM links p WHERE p.id=l.parent)
  UNION ALL
  SELECT child.id,parent.root FROM links child JOIN roots parent ON child.parent=parent.id
 )
INSERT INTO mail_context_roots SELECT id,root FROM roots;

UPDATE messages SET context=json_object('kind','conversation','id',
 (SELECT root FROM mail_context_roots WHERE id=messages.global_id));

-- Refuse unresolved/cyclic history atomically, rather than silently losing it.
CREATE TEMP TABLE mail_context_validation (valid INTEGER NOT NULL CHECK(valid=1));
INSERT INTO mail_context_validation SELECT
 NOT EXISTS(SELECT 1 FROM messages WHERE context IS NULL OR json_extract(context,'$.id') IS NULL);
DROP TABLE mail_context_validation;

WITH pending AS (
 SELECT event_id,
  CASE json_extract(payload,'$.event.kind')
   WHEN 'message' THEN json_extract(payload,'$.event.data')
   ELSE json_extract(payload,'$.event.data.message') END AS message,
  CASE json_extract(payload,'$.event.kind')
   WHEN 'message' THEN 'request' ELSE json_extract(payload,'$.event.data.intent') END AS intent
 FROM outbox WHERE json_extract(payload,'$.event.kind') IN ('message','typed_message')
)
UPDATE outbox SET payload=json_set(payload,'$.event',json_object(
 'kind','context_message','data',json_object(
  'intent',(SELECT intent FROM pending WHERE pending.event_id=outbox.event_id),
  'context',json_object('kind','conversation','id',(
   SELECT r.root FROM pending p JOIN mail_context_roots r ON r.id=json_extract(p.message,'$.id')
   WHERE p.event_id=outbox.event_id)),
  'message',json((SELECT message FROM pending WHERE pending.event_id=outbox.event_id)))))
WHERE event_id IN (SELECT event_id FROM pending);
DROP TABLE mail_context_roots;

CREATE TRIGGER require_mail_context BEFORE INSERT ON messages
WHEN NEW.context IS NULL BEGIN
 SELECT RAISE(ABORT,'message context is required');
END;
CREATE TRIGGER immutable_mail_context BEFORE UPDATE OF context,work_id,reply_to ON messages
WHEN NEW.context IS NOT OLD.context OR NEW.work_id IS NOT OLD.work_id OR NEW.reply_to IS NOT OLD.reply_to
BEGIN
 SELECT RAISE(ABORT,'message context and associations are immutable');
END;
CREATE TRIGGER inherit_mail_context BEFORE INSERT ON messages
WHEN NEW.reply_to IS NOT NULL AND NOT EXISTS(
 SELECT 1 FROM messages p JOIN mailboxes sender ON sender.id=NEW.sender
 JOIN mailboxes parent_sender ON parent_sender.id=p.sender
 WHERE p.id=NEW.reply_to AND sender.group_name=parent_sender.group_name
 AND json_extract(NEW.context,'$.kind')=json_extract(p.context,'$.kind')
 AND json_extract(NEW.context,'$.id')=json_extract(p.context,'$.id')
 AND json_extract(NEW.context,'$.version') IS json_extract(p.context,'$.version')
 AND NEW.work_id IS p.work_id)
BEGIN
 SELECT RAISE(ABORT,'reply must inherit its parent context');
END;
CREATE INDEX message_by_conversation ON messages(json_extract(context,'$.id'),id)
WHERE json_extract(context,'$.kind')='conversation';

-- Preserve reply ancestry when its parent is not replicated to this recipient.
ALTER TABLE messages ADD COLUMN parent_global_id TEXT;
UPDATE messages SET parent_global_id=(SELECT p.global_id FROM messages p WHERE p.id=messages.reply_to);
CREATE TRIGGER immutable_mail_parent BEFORE UPDATE OF parent_global_id ON messages
WHEN NEW.parent_global_id IS NOT OLD.parent_global_id
BEGIN
 SELECT RAISE(ABORT,'message parent is immutable');
END;
CREATE TRIGGER match_local_mail_parent BEFORE INSERT ON messages
WHEN NEW.reply_to IS NOT NULL AND NEW.parent_global_id IS NOT (
 SELECT global_id FROM messages WHERE id=NEW.reply_to)
BEGIN
 SELECT RAISE(ABORT,'local and global message parents must agree');
END;

-- Refresh the required send grammar on the next input hook, preserving receipts.
DELETE FROM recovery_emissions;
PRAGMA user_version=26;
