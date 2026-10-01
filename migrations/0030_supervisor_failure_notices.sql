-- Evolve only the shared notice projection after actual supervisor visits29.
CREATE TEMP TABLE notice30_predecessor(version INTEGER CHECK(version=29));
INSERT INTO notice30_predecessor SELECT user_version FROM pragma_user_version;
DROP TABLE notice30_predecessor;

-- The only direct child is batch_items; preserve every frozen member and ID.
CREATE TEMP TABLE notice30_saved_notices AS SELECT * FROM operator_notices;
CREATE TEMP TABLE notice30_saved_items AS SELECT * FROM operator_notice_batch_items;
DROP TABLE operator_notice_batch_items;
DROP TABLE operator_notices;
CREATE TABLE operator_notices (
 id INTEGER PRIMARY KEY,
 group_name TEXT NOT NULL REFERENCES groups(name),
 source_kind TEXT NOT NULL CHECK(source_kind IN ('attention_occurrence','operator_obligation','supervisor_failure')),
 source_id INTEGER NOT NULL,
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 revision INTEGER NOT NULL CHECK(revision>0),
 source_snapshot TEXT NOT NULL CHECK(json_valid(source_snapshot)),
 responsible TEXT NOT NULL,
 unresolved INTEGER NOT NULL CHECK(unresolved IN (0,1)),
 due_at INTEGER NOT NULL,
 first_dirty INTEGER NOT NULL,
 accepted_revision INTEGER NOT NULL DEFAULT 0 CHECK(accepted_revision>=0),
 accepted_generation INTEGER CHECK(accepted_generation>=0),
 state TEXT NOT NULL CHECK(state IN ('pending','reserved','exposed','accepted','failed','uncertain','unconfigured','retired')),
 batch TEXT,
 UNIQUE(group_name,source_kind,source_id)
);
CREATE INDEX operator_notice_due ON operator_notices(group_name,due_at,id);
INSERT INTO operator_notices SELECT * FROM notice30_saved_notices;
CREATE TABLE operator_notice_batch_items (
 batch TEXT NOT NULL REFERENCES operator_notice_batches(id),
 notice INTEGER NOT NULL REFERENCES operator_notices(id),
 revision INTEGER NOT NULL,
 account INTEGER NOT NULL REFERENCES operator_notice_accounts(id),
 source_snapshot TEXT NOT NULL CHECK(json_valid(source_snapshot)),
 PRIMARY KEY(batch,notice)
);
INSERT INTO operator_notice_batch_items SELECT * FROM notice30_saved_items;
CREATE TRIGGER operator_item_no_update BEFORE UPDATE ON operator_notice_batch_items
BEGIN SELECT RAISE(ABORT,'operator batch membership is immutable'); END;
CREATE TRIGGER operator_item_no_delete BEFORE DELETE ON operator_notice_batch_items
BEGIN SELECT RAISE(ABORT,'operator batch membership retains history'); END;
CREATE TEMP TABLE notice30_copy_guard(valid INTEGER CHECK(valid=1));
INSERT INTO notice30_copy_guard SELECT
 (NOT EXISTS(SELECT * FROM notice30_saved_notices EXCEPT SELECT * FROM operator_notices))
 AND (NOT EXISTS(SELECT * FROM operator_notices EXCEPT SELECT * FROM notice30_saved_notices))
 AND (NOT EXISTS(SELECT * FROM notice30_saved_items EXCEPT SELECT * FROM operator_notice_batch_items))
 AND (NOT EXISTS(SELECT * FROM operator_notice_batch_items EXCEPT SELECT * FROM notice30_saved_items));
DROP TABLE notice30_copy_guard;
DROP TABLE notice30_saved_items;
DROP TABLE notice30_saved_notices;

ALTER TABLE operator_notice_projection ADD COLUMN infrastructure_after INTEGER NOT NULL DEFAULT 0 CHECK(infrastructure_after>=0);
ALTER TABLE operator_notice_projection ADD COLUMN next_class TEXT NOT NULL DEFAULT 'infrastructure' CHECK(next_class IN ('ordinary','infrastructure'));
ALTER TABLE operator_notice_batches ADD COLUMN notice_class TEXT NOT NULL DEFAULT 'ordinary' CHECK(notice_class IN ('ordinary','infrastructure'));
DROP TRIGGER operator_batch_frozen;
CREATE TRIGGER operator_batch_frozen BEFORE UPDATE OF id,group_name,generation,route,payload,owner,lease_until,notice_class ON operator_notice_batches
BEGIN SELECT RAISE(ABORT,'operator batch identity and bytes are immutable'); END;
CREATE TEMP TABLE notice30_fk_guard(violations INTEGER CHECK(violations=0));
INSERT INTO notice30_fk_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE notice30_fk_guard;
PRAGMA user_version=30;
