-- A retrieved request from a task's writer to its current owner shares that
-- task's follow-through schedule. Unread deliveries, explicit deadlines and
-- independent mail checkpoints retain their own schedule and authority.
CREATE VIEW task_request_schedules AS
SELECT mail.id AS followup, task.id AS task_followup
FROM active_followups mail
JOIN messages m ON m.id=mail.message
JOIN active_followups task ON task.group_name=mail.group_name
 AND task.task=m.work_id AND task.recipient=mail.recipient
 AND task.authority=mail.authority
JOIN work_items w ON w.group_name=task.group_name AND w.id=task.task
 AND w.version=task.task_version
JOIN mailboxes writer ON writer.id=mail.authority
 AND writer.group_name=w.group_name AND writer.name=w.writer
JOIN mailboxes b ON b.id=mail.recipient
WHERE m.intent='request' AND m.deadline IS NULL
 AND mail.checkpoint IS NULL AND mail.retrieved_at IS NOT NULL
 AND mail.retrieved_binding=b.binding_version;

-- Keep all business obligations in active_followups. Only their independent
-- scheduling authorities participate in reminders, escalation and deadlines.
CREATE VIEW scheduled_followups AS
SELECT f.* FROM active_followups f
WHERE NOT EXISTS(SELECT 1 FROM task_request_schedules s WHERE s.followup=f.id);

DROP VIEW active_attention;
CREATE VIEW active_attention AS
SELECT o.* FROM attention_occurrences o
JOIN scheduled_followups f ON f.id=o.followup AND f.version=o.plan_version
WHERE f.stage=o.stage OR (o.reason='dependency_ready' AND f.dependency_ready_at IS NOT NULL);

-- Refresh the scheduling instructions once on each client's next input hook.
-- This is recovery metadata, not a delivery receipt or retry-budget reset.
DELETE FROM recovery_emissions;

PRAGMA user_version=25;
