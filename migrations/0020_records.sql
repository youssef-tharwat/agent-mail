CREATE TABLE shared_records (
 group_name TEXT NOT NULL REFERENCES groups(name), id TEXT NOT NULL,
 writer TEXT NOT NULL, title TEXT NOT NULL, current_revision INTEGER NOT NULL CHECK(current_revision>0),
 updated INTEGER NOT NULL, PRIMARY KEY(group_name,id),
 FOREIGN KEY(group_name,writer) REFERENCES mailboxes(group_name,name)
);
CREATE TABLE record_revisions (
 group_name TEXT NOT NULL, record_id TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0),
 title TEXT NOT NULL, body TEXT NOT NULL, summary TEXT NOT NULL,
 actor INTEGER NOT NULL REFERENCES mailboxes(id), actor_generation INTEGER NOT NULL,
 reason TEXT NOT NULL, supersedes INTEGER, created INTEGER NOT NULL,
 canonical TEXT NOT NULL, result TEXT NOT NULL,
 PRIMARY KEY(group_name,record_id,revision),
 FOREIGN KEY(group_name,record_id) REFERENCES shared_records(group_name,id),
 FOREIGN KEY(group_name,record_id,supersedes) REFERENCES record_revisions(group_name,record_id,revision)
);
CREATE TRIGGER record_revision_immutable_update BEFORE UPDATE ON record_revisions BEGIN SELECT RAISE(ABORT,'record revisions are immutable'); END;
CREATE TRIGGER record_revision_immutable_delete BEFORE DELETE ON record_revisions BEGIN SELECT RAISE(ABORT,'record revisions are immutable'); END;
CREATE TABLE record_links (
 group_name TEXT NOT NULL, target_kind TEXT NOT NULL CHECK(target_kind IN ('task','message')), target_id TEXT NOT NULL,
 record_id TEXT NOT NULL, revision INTEGER NOT NULL,
 actor INTEGER NOT NULL REFERENCES mailboxes(id), actor_generation INTEGER NOT NULL, created INTEGER NOT NULL,
 PRIMARY KEY(group_name,target_kind,target_id,record_id,revision),
 FOREIGN KEY(group_name,record_id,revision) REFERENCES record_revisions(group_name,record_id,revision)
);
CREATE INDEX record_links_target ON record_links(group_name,target_kind,target_id);
PRAGMA user_version = 20;
CREATE TABLE record_snapshots (
 group_name TEXT NOT NULL REFERENCES groups(name),record_id TEXT NOT NULL,revision INTEGER NOT NULL,
 writer TEXT NOT NULL,title TEXT NOT NULL,summary TEXT NOT NULL,supersedes INTEGER,reason TEXT NOT NULL,created INTEGER NOT NULL,
 result TEXT NOT NULL,PRIMARY KEY(group_name,record_id,revision)
);
CREATE TABLE remote_record_links (
 group_name TEXT NOT NULL, target_kind TEXT NOT NULL,target_id TEXT NOT NULL,record_id TEXT NOT NULL,revision INTEGER NOT NULL,
 PRIMARY KEY(group_name,target_kind,target_id,record_id,revision),
 FOREIGN KEY(group_name,record_id,revision) REFERENCES record_snapshots(group_name,record_id,revision)
);
CREATE VIEW record_read_revisions AS
 SELECT group_name,record_id,revision,title,summary,supersedes,reason,created,result FROM record_revisions
 UNION ALL
 SELECT group_name,record_id,revision,title,summary,supersedes,reason,created,result FROM record_snapshots;
CREATE VIEW record_read_heads AS
 SELECT group_name,id,writer,current_revision FROM shared_records
 UNION ALL
 SELECT group_name,record_id AS id,writer,MAX(revision) AS current_revision FROM record_snapshots GROUP BY group_name,record_id;
CREATE VIEW record_read_links AS
 SELECT group_name,target_kind,target_id,record_id,revision FROM record_links
 UNION
 SELECT group_name,target_kind,target_id,record_id,revision FROM remote_record_links WHERE target_kind='task'
 UNION
 SELECT l.group_name,l.target_kind,CAST(m.id AS TEXT),l.record_id,l.revision FROM remote_record_links l JOIN messages m ON m.global_id=l.target_id WHERE l.target_kind='message';
