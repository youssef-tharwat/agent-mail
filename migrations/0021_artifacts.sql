CREATE TABLE artifact_blobs (
 group_name TEXT NOT NULL REFERENCES groups(name), digest TEXT NOT NULL,
 codec TEXT NOT NULL, format_version INTEGER NOT NULL DEFAULT 1,
 original_size INTEGER NOT NULL CHECK(original_size>=0), stored_size INTEGER NOT NULL CHECK(stored_size>=0),
 created INTEGER NOT NULL, PRIMARY KEY(group_name,digest)
);
CREATE TABLE artifacts (
 group_name TEXT NOT NULL REFERENCES groups(name), id TEXT NOT NULL,
 snapshot TEXT NOT NULL, canonical TEXT NOT NULL, created INTEGER NOT NULL,
 digest TEXT, revision INTEGER NOT NULL DEFAULT 1, pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)),
 PRIMARY KEY(group_name,id)
);
CREATE TABLE artifact_links (
 group_name TEXT NOT NULL, artifact_id TEXT NOT NULL, target TEXT NOT NULL,
 created INTEGER NOT NULL, PRIMARY KEY(group_name,artifact_id,target),
 FOREIGN KEY(group_name,artifact_id) REFERENCES artifacts(group_name,id)
);
CREATE TABLE artifact_audit (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, group_name TEXT NOT NULL,
 artifact_id TEXT, action TEXT NOT NULL, actor TEXT NOT NULL, details TEXT NOT NULL,
 changed INTEGER NOT NULL
);
CREATE INDEX artifact_digest ON artifacts(group_name,digest);
PRAGMA user_version=21;
