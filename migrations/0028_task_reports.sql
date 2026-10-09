-- A task result is immutable evidence plus an ordinary mandatory request.
-- Existing mail, dispositions, receipts and follow-up budgets remain unchanged.
CREATE TABLE task_reports (
 message INTEGER NOT NULL PRIMARY KEY REFERENCES messages(id),
 group_name TEXT NOT NULL,
 work_id TEXT NOT NULL,
 reporter INTEGER NOT NULL REFERENCES mailboxes(id),
 report_key TEXT NOT NULL,
 canonical TEXT NOT NULL CHECK(json_valid(canonical)),
 revision TEXT NOT NULL,
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 -- Reporting holds owner reminders only for this still-current assignment.
 authority_version INTEGER NOT NULL CHECK(authority_version>0),
 decision_message INTEGER NOT NULL REFERENCES messages(id),
 UNIQUE(reporter,report_key),
 FOREIGN KEY(group_name,work_id) REFERENCES work_items(group_name,id)
);
CREATE INDEX task_report_lookup ON task_reports(group_name,work_id,message);

-- Consent applies to an exact live endpoint, never to another pane or session.
CREATE TABLE herdr_session_policy (
 endpoint TEXT PRIMARY KEY CHECK(json_valid(endpoint)),
 auto_prompt INTEGER NOT NULL CHECK(auto_prompt IN (0,1))
);
-- Conflicting legacy group choices migrate conservatively to notification-only.
-- An explicit operator choice can subsequently enable the shared endpoint once.
INSERT INTO herdr_session_policy(endpoint,auto_prompt)
SELECT json_array(g.socket,b.pane,json_extract(b.binding,'$.terminal'),json_extract(b.binding,'$.agent'),json_extract(b.binding,'$.session_kind'),json_extract(b.binding,'$.session_value')),MIN(g.auto_prompt)
FROM mailboxes b JOIN groups g ON g.name=b.group_name
WHERE b.pane IS NOT NULL AND b.agent_state='registered'
GROUP BY g.socket,b.pane,json_extract(b.binding,'$.terminal'),json_extract(b.binding,'$.agent'),json_extract(b.binding,'$.session_kind'),json_extract(b.binding,'$.session_value');
-- A later launch must obtain its own consent, not inherit a group default.
UPDATE groups SET auto_prompt=0;
-- Existing native launches load the updated result/session operating guide once.
-- This changes guide emission only, preserving delivery and business receipts.
DELETE FROM recovery_emissions;
PRAGMA user_version=28;
