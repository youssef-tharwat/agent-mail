//! Group-visible text records with immutable revisions and exact task/mail references.
use crate::{
    bounded, name,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};

/// Maximum UTF-8 bytes stored in one record revision.
pub const RECORD_BODY_LIMIT: usize = 65536;
/// Initial fields for a group record. Its creator is its designated writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordDraft {
    /// Stable group-local record identifier.
    pub id: String,
    /// Human-readable title (256 UTF-8 bytes).
    pub title: String,
    /// Full shared text (64 KiB).
    pub body: String,
    /// Compact recovery description (512 UTF-8 bytes).
    pub summary: String,
}
/// A compare-and-swap update. An identical retry returns the original revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordUpdate {
    /// Current revision observed before the update.
    pub revision: i64,
    /// Replacement title.
    pub title: String,
    /// Replacement full text.
    pub body: String,
    /// Replacement compact summary.
    pub summary: String,
    /// Required explanation, including the correction reason where applicable.
    pub reason: String,
}
/// Full immutable record revision, retrieved explicitly on demand.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordRevision {
    /// Enclosing group.
    pub group_name: String,
    /// Stable record ID.
    pub id: String,
    /// Immutable revision number.
    pub revision: i64,
    /// Designated writer; task ownership does not grant this authority.
    pub writer: String,
    /// Title of this revision.
    pub title: String,
    /// Complete text.
    pub body: String,
    /// Bounded recovery description.
    pub summary: String,
    /// Explanation of this revision.
    pub reason: String,
    /// Previous revision explicitly superseded by this correction.
    pub supersedes: Option<i64>,
    /// Identity generation that wrote this revision.
    pub actor_generation: i64,
    /// Unix creation timestamp.
    pub created: i64,
}
/// Compact current/history record information without the full body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSummary {
    /// Stable record identifier.
    pub id: String,
    /// Title at this revision.
    pub title: String,
    /// Designated writer.
    pub writer: String,
    /// Exact revision number.
    pub revision: i64,
    /// Current head revision, useful for detecting superseded references.
    pub current_revision: i64,
    /// Bounded description.
    pub summary: String,
    /// Previous revision superseded, where applicable.
    pub supersedes: Option<i64>,
    /// Correction or creation reason.
    pub reason: String,
    /// Revision timestamp.
    pub created: i64,
}
/// Target of an exact shared-record revision link.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordTarget {
    /// Task identifier; its current writer may attach references.
    Task(String),
    /// Message identifier; its sender may attach references.
    Message(i64),
}
impl RecordTarget {
    fn parts(&self) -> Result<(&'static str, String)> {
        match self {
            Self::Task(id) => {
                name(id)?;
                Ok(("task", id.clone()))
            }
            Self::Message(id) => {
                ensure!(*id > 0, "invalid message ID");
                Ok(("message", id.to_string()))
            }
        }
    }
}
fn fields(title: &str, body: &str, summary: &str) -> Result<()> {
    bounded(title, 256, "record title")?;
    bounded(body, RECORD_BODY_LIMIT, "record body")?;
    ensure!(
        serde_json::to_string(body)?.len() <= 200 * 1024,
        "record body exceeds encoded relay byte limit"
    );
    bounded(summary, 512, "record summary")?;
    ensure!(
        !title.trim().is_empty() && !body.trim().is_empty() && !summary.trim().is_empty(),
        "record title, body and summary are required"
    );
    Ok(())
}
async fn home(tx: &mut Transaction<'_, Sqlite>, actor: &Mailbox) -> Result<()> {
    let local: String = sqlx::query_scalar("SELECT id FROM node LIMIT 1")
        .fetch_one(&mut **tx)
        .await?;
    let home: String = sqlx::query_scalar("SELECT home_machine FROM groups WHERE name=?")
        .bind(&actor.group_name)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        local == home,
        "shared records are writable only on the home machine"
    );
    Ok(())
}
fn summary(row: sqlx::sqlite::SqliteRow) -> Result<RecordSummary> {
    Ok(RecordSummary {
        id: row.try_get("record_id")?,
        title: row.try_get("title")?,
        writer: row.try_get("writer")?,
        revision: row.try_get("revision")?,
        current_revision: row.try_get("current_revision")?,
        summary: row.try_get("summary")?,
        supersedes: row.try_get("supersedes")?,
        reason: row.try_get("reason")?,
        created: row.try_get("created")?,
    })
}
impl Store {
    /// Create a record; identical same-generation retries return revision one.
    /// # Errors
    /// Invalid fields, stale identity, remote writes or a conflicting existing ID.
    pub async fn record_create(
        &self,
        actor: &Mailbox,
        draft: RecordDraft,
        now: i64,
    ) -> Result<RecordRevision> {
        name(&draft.id)?;
        fields(&draft.title, &draft.body, &draft.summary)?;
        let canonical = serde_json::to_string(&draft)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        home(&mut tx, actor).await?;
        if let Some(row)=sqlx::query("SELECT actor,actor_generation,canonical,result FROM record_revisions WHERE group_name=? AND record_id=? AND revision=1").bind(&actor.group_name).bind(&draft.id).fetch_optional(&mut *tx).await? {
            ensure!(row.try_get::<i64,_>("actor")?==actor.id && row.try_get::<i64,_>("actor_generation")?==actor.binding_version && row.try_get::<String,_>("canonical")?==canonical,"record ID already created with different content or identity");
            return Ok(serde_json::from_str(&row.try_get::<String,_>("result")?)?);
        }
        let item = RecordRevision {
            group_name: actor.group_name.clone(),
            id: draft.id,
            revision: 1,
            writer: actor.name.clone(),
            title: draft.title,
            body: draft.body,
            summary: draft.summary,
            reason: "created".into(),
            supersedes: None,
            actor_generation: actor.binding_version,
            created: now,
        };
        sqlx::query("INSERT INTO shared_records(group_name,id,writer,title,current_revision,updated) VALUES(?,?,?,?,1,?)").bind(&item.group_name).bind(&item.id).bind(&item.writer).bind(&item.title).bind(now).execute(&mut *tx).await?;
        insert_revision(&mut tx, actor, &item, &canonical).await?;
        enqueue_record(&mut tx, &item, now).await?;
        tx.commit().await?;
        Ok(item)
    }
    /// Write a new revision that explicitly supersedes the observed head.
    /// # Errors
    /// Stale actor, wrong writer, version conflict, conflicting retry or invalid fields.
    pub async fn record_update(
        &self,
        actor: &Mailbox,
        id: &str,
        update: RecordUpdate,
        now: i64,
    ) -> Result<RecordRevision> {
        name(id)?;
        fields(&update.title, &update.body, &update.summary)?;
        bounded(&update.reason, 512, "record correction reason")?;
        ensure!(
            update.revision > 0 && !update.reason.trim().is_empty(),
            "positive revision and correction reason are required"
        );
        let next = update
            .revision
            .checked_add(1)
            .context("record revision overflow")?;
        let canonical = serde_json::to_string(&update)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        home(&mut tx, actor).await?;
        let row = sqlx::query(
            "SELECT writer,current_revision FROM shared_records WHERE group_name=? AND id=?",
        )
        .bind(&actor.group_name)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .context("record not found in this group")?;
        ensure!(
            row.try_get::<String, _>("writer")? == actor.name,
            "only the designated record writer may update this record"
        );
        if let Some(old)=sqlx::query("SELECT actor,actor_generation,canonical,result FROM record_revisions WHERE group_name=? AND record_id=? AND revision=?").bind(&actor.group_name).bind(id).bind(next).fetch_optional(&mut *tx).await? {
            ensure!(old.try_get::<i64,_>("actor")?==actor.id && old.try_get::<i64,_>("actor_generation")?==actor.binding_version && old.try_get::<String,_>("canonical")?==canonical,"record revision conflict or retry content changed");
            return Ok(serde_json::from_str(&old.try_get::<String,_>("result")?)?);
        }
        ensure!(
            row.try_get::<i64, _>("current_revision")? == update.revision,
            "record revision conflict; read current record before retrying"
        );
        let item = RecordRevision {
            group_name: actor.group_name.clone(),
            id: id.into(),
            revision: next,
            writer: actor.name.clone(),
            title: update.title,
            body: update.body,
            summary: update.summary,
            reason: update.reason,
            supersedes: Some(update.revision),
            actor_generation: actor.binding_version,
            created: now,
        };
        let changed=sqlx::query("UPDATE shared_records SET title=?,current_revision=?,updated=? WHERE group_name=? AND id=? AND current_revision=?").bind(&item.title).bind(next).bind(now).bind(&actor.group_name).bind(id).bind(update.revision).execute(&mut *tx).await?;
        ensure!(changed.rows_affected() == 1, "record revision conflict");
        insert_revision(&mut tx, actor, &item, &canonical).await?;
        enqueue_record(&mut tx, &item, now).await?;
        tx.commit().await?;
        Ok(item)
    }
    /// Fetch the exact revision or current head without resolving mail or accepting work.
    /// # Errors
    /// Stale identity or an absent record/revision in the actor's group.
    pub async fn record_show(
        &self,
        actor: &Mailbox,
        id: &str,
        revision: Option<i64>,
    ) -> Result<RecordRevision> {
        name(id)?;
        if let Some(r) = revision {
            ensure!(r > 0, "invalid record revision");
        }
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let result: String=sqlx::query_scalar("SELECT r.result FROM record_read_revisions r JOIN record_read_heads s ON s.group_name=r.group_name AND s.id=r.record_id WHERE r.group_name=? AND r.record_id=? AND r.revision=COALESCE(?,s.current_revision)").bind(&actor.group_name).bind(id).bind(revision).fetch_optional(&mut *tx).await?.context("record revision not found in this group")?;
        let item = serde_json::from_str(&result)?;
        tx.commit().await?;
        Ok(item)
    }
    /// List at most six current record summaries after a stable identifier cursor.
    /// # Errors
    /// Invalid cursor, stale actor or database errors.
    pub async fn record_list(&self, actor: &Mailbox, after: &str) -> Result<Vec<RecordSummary>> {
        if !after.is_empty() {
            name(after)?;
        }
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows=sqlx::query("SELECT r.record_id,r.title,s.writer,r.revision,s.current_revision,r.summary,r.supersedes,r.reason,r.created FROM record_read_heads s JOIN record_read_revisions r ON r.group_name=s.group_name AND r.record_id=s.id AND r.revision=s.current_revision WHERE s.group_name=? AND s.id>? ORDER BY s.id LIMIT 6").bind(&actor.group_name).bind(after).fetch_all(&mut *tx).await?;
        let result = rows.into_iter().map(summary).collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(result)
    }
    /// Read up to twenty immutable revision summaries, newest first, before a revision cursor.
    /// # Errors
    /// Invalid identifier/cursor, stale actor, absent record or database errors.
    pub async fn record_history(
        &self,
        actor: &Mailbox,
        id: &str,
        before: Option<i64>,
    ) -> Result<Vec<RecordSummary>> {
        name(id)?;
        if let Some(r) = before {
            ensure!(r > 0, "invalid history cursor");
        }
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let exists: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM record_read_heads WHERE group_name=? AND id=?",
        )
        .bind(&actor.group_name)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(exists == 1, "record not found in this group");
        let rows=sqlx::query("SELECT r.record_id,r.title,s.writer,r.revision,s.current_revision,r.summary,r.supersedes,r.reason,r.created FROM record_read_revisions r JOIN record_read_heads s ON s.group_name=r.group_name AND s.id=r.record_id WHERE r.group_name=? AND r.record_id=? AND (? IS NULL OR r.revision<?) ORDER BY r.revision DESC LIMIT 20").bind(&actor.group_name).bind(id).bind(before).bind(before).fetch_all(&mut *tx).await?;
        let result = rows.into_iter().map(summary).collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(result)
    }
    /// Attach an exact immutable revision to a task or message. Identical links are safe to retry.
    /// # Errors
    /// Stale actor, absent revision/target, wrong target writer or too many references.
    pub async fn record_link(
        &self,
        actor: &Mailbox,
        target: &RecordTarget,
        id: &str,
        revision: i64,
        now: i64,
    ) -> Result<()> {
        name(id)?;
        ensure!(revision > 0, "invalid record revision");
        let (kind, target_id) = target.parts()?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        home(&mut tx, actor).await?;
        match target {
            RecordTarget::Task(task) => {
                let writer: Option<String> =
                    sqlx::query_scalar("SELECT writer FROM work_items WHERE group_name=? AND id=?")
                        .bind(&actor.group_name)
                        .bind(task)
                        .fetch_optional(&mut *tx)
                        .await?;
                ensure!(
                    writer.as_deref() == Some(&actor.name),
                    "only the current task writer may link record revisions"
                );
            }
            RecordTarget::Message(message) => {
                let sender: Option<i64> =
                    sqlx::query_scalar("SELECT sender FROM messages WHERE id=?")
                        .bind(message)
                        .fetch_optional(&mut *tx)
                        .await?;
                ensure!(
                    sender == Some(actor.id),
                    "only the message sender may link record revisions"
                );
            }
        }
        let exists: i64=sqlx::query_scalar("SELECT count(*) FROM record_revisions WHERE group_name=? AND record_id=? AND revision=?").bind(&actor.group_name).bind(id).bind(revision).fetch_one(&mut *tx).await?;
        ensure!(exists == 1, "record revision not found in this group");
        let old: i64=sqlx::query_scalar("SELECT count(*) FROM record_links WHERE group_name=? AND target_kind=? AND target_id=? AND record_id=? AND revision=?").bind(&actor.group_name).bind(kind).bind(&target_id).bind(id).bind(revision).fetch_one(&mut *tx).await?;
        if old == 0 {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM record_links WHERE group_name=? AND target_kind=? AND target_id=?").bind(&actor.group_name).bind(kind).bind(&target_id).fetch_one(&mut *tx).await?;
            ensure!(count < 16, "too many record references on this target");
            sqlx::query("INSERT INTO record_links(group_name,target_kind,target_id,record_id,revision,actor,actor_generation,created) VALUES(?,?,?,?,?,?,?,?)").bind(&actor.group_name).bind(kind).bind(target_id).bind(id).bind(revision).bind(actor.id).bind(actor.binding_version).bind(now).execute(&mut *tx).await?;
        }
        let result: String = sqlx::query_scalar(
            "SELECT result FROM record_revisions WHERE group_name=? AND record_id=? AND revision=?",
        )
        .bind(&actor.group_name)
        .bind(id)
        .bind(revision)
        .fetch_one(&mut *tx)
        .await?;
        let item: RecordRevision = serde_json::from_str(&result)?;
        enqueue_record(&mut tx, &item, now).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Read bounded exact record references. Task links are shared; message links retain mailbox visibility.
    /// # Errors
    /// Stale actor, absent target or unauthorized message access.
    pub async fn record_links(
        &self,
        actor: &Mailbox,
        target: &RecordTarget,
    ) -> Result<Vec<RecordSummary>> {
        let (kind, target_id) = target.parts()?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        match target {
            RecordTarget::Task(id) => {
                let count: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM (SELECT group_name,id FROM work_items UNION ALL SELECT group_name,work_id AS id FROM work_snapshots) WHERE group_name=? AND id=?",
                )
                .bind(&actor.group_name)
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                ensure!(count == 1, "task not found in this group");
            }
            RecordTarget::Message(id) => {
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND b.group_name=? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?))").bind(id).bind(&actor.group_name).bind(actor.id).bind(actor.id).fetch_one(&mut *tx).await?;
                ensure!(count == 1, "message not visible to this participant");
            }
        }
        let rows=sqlx::query("SELECT r.record_id,r.title,s.writer,r.revision,s.current_revision,r.summary,r.supersedes,r.reason,r.created FROM record_read_links l JOIN record_read_revisions r ON r.group_name=l.group_name AND r.record_id=l.record_id AND r.revision=l.revision JOIN record_read_heads s ON s.group_name=r.group_name AND s.id=r.record_id WHERE l.group_name=? AND l.target_kind=? AND l.target_id=? ORDER BY l.record_id,l.revision LIMIT 16").bind(&actor.group_name).bind(kind).bind(target_id).fetch_all(&mut *tx).await?;
        let result = rows.into_iter().map(summary).collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(result)
    }
}
async fn insert_revision(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    item: &RecordRevision,
    canonical: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO record_revisions(group_name,record_id,revision,title,body,summary,actor,actor_generation,reason,supersedes,created,canonical,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(&item.group_name).bind(&item.id).bind(item.revision).bind(&item.title).bind(&item.body).bind(&item.summary).bind(actor.id).bind(actor.binding_version).bind(&item.reason).bind(item.supersedes).bind(item.created).bind(canonical).bind(serde_json::to_string(item)?).execute(&mut **tx).await?;
    Ok(())
}

/// Home-authored revision and its exact links, portable across mailbox IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSnapshot {
    /// Immutable group text revision.
    pub record: RecordRevision,
    /// Group task IDs referencing this revision.
    pub tasks: Vec<String>,
    /// Global message UUIDs referencing this revision; mail contents stay private.
    pub messages: Vec<uuid::Uuid>,
}
async fn snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    item: &RecordRevision,
) -> Result<RecordSnapshot> {
    let tasks=sqlx::query_scalar("SELECT target_id FROM record_links WHERE group_name=? AND record_id=? AND revision=? AND target_kind='task' ORDER BY target_id").bind(&item.group_name).bind(&item.id).bind(item.revision).fetch_all(&mut **tx).await?;
    let ids: Vec<String>=sqlx::query_scalar("SELECT m.global_id FROM record_links l JOIN messages m ON CAST(m.id AS TEXT)=l.target_id WHERE l.group_name=? AND l.record_id=? AND l.revision=? AND l.target_kind='message' ORDER BY m.id").bind(&item.group_name).bind(&item.id).bind(item.revision).fetch_all(&mut **tx).await?;
    let messages = ids
        .into_iter()
        .map(|s| uuid::Uuid::parse_str(&s))
        .collect::<std::result::Result<_, _>>()?;
    Ok(RecordSnapshot {
        record: item.clone(),
        tasks,
        messages,
    })
}
pub(crate) async fn enqueue_record(
    tx: &mut Transaction<'_, Sqlite>,
    item: &RecordRevision,
    now: i64,
) -> Result<()> {
    let event = snapshot(tx, item).await?;
    let routes:Vec<String>=sqlx::query_scalar("SELECT DISTINCT remote_machine FROM mailboxes WHERE group_name=? AND remote_machine IS NOT NULL").bind(&item.group_name).fetch_all(&mut **tx).await?;
    for machine in routes {
        enqueue_chunks(tx, uuid::Uuid::parse_str(&machine)?, &event, now).await?;
    }
    Ok(())
}
/// Queue exact revision history for a newly routed group participant.
pub(crate) async fn enqueue_records_for_route(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    machine: uuid::Uuid,
    now: i64,
) -> Result<()> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT result FROM record_revisions WHERE group_name=? ORDER BY record_id,revision",
    )
    .bind(group)
    .fetch_all(&mut **tx)
    .await?;
    for result in rows {
        let item: RecordRevision = serde_json::from_str(&result)?;
        let event = snapshot(tx, &item).await?;
        enqueue_chunks(tx, machine, &event, now).await?;
    }
    Ok(())
}
/// Apply a revision after relay validates that the origin is the group's home.
pub(crate) async fn apply_record_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    event: &RecordSnapshot,
) -> Result<()> {
    let r = &event.record;
    name(&r.group_name)?;
    name(&r.id)?;
    name(&r.writer)?;
    fields(&r.title, &r.body, &r.summary)?;
    bounded(&r.reason, 512, "record reason")?;
    ensure!(
        r.revision > 0 && r.actor_generation > 0 && r.created > 0,
        "invalid record snapshot metadata"
    );
    ensure!(
        r.supersedes
            == if r.revision == 1 {
                None
            } else {
                Some(r.revision - 1)
            },
        "invalid superseding record revision"
    );
    ensure!(
        event.tasks.len() + event.messages.len() <= 1024,
        "too many snapshot links"
    );
    let result = serde_json::to_string(r)?;
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT result FROM record_snapshots WHERE group_name=? AND record_id=? AND revision=?",
    )
    .bind(&r.group_name)
    .bind(&r.id)
    .bind(r.revision)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(old) = existing {
        ensure!(old == result, "immutable record snapshot content changed");
    } else {
        sqlx::query("INSERT INTO record_snapshots(group_name,record_id,revision,writer,title,summary,supersedes,reason,created,result) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&r.group_name).bind(&r.id).bind(r.revision).bind(&r.writer).bind(&r.title).bind(&r.summary).bind(r.supersedes).bind(&r.reason).bind(r.created).bind(result).execute(&mut **tx).await?;
    }
    for task in &event.tasks {
        name(task)?;
        sqlx::query("INSERT OR IGNORE INTO remote_record_links(group_name,target_kind,target_id,record_id,revision) VALUES(?,'task',?,?,?)").bind(&r.group_name).bind(task).bind(&r.id).bind(r.revision).execute(&mut **tx).await?;
    }
    for message in &event.messages {
        sqlx::query("INSERT OR IGNORE INTO remote_record_links(group_name,target_kind,target_id,record_id,revision) VALUES(?,'message',?,?,?)").bind(&r.group_name).bind(message.to_string()).bind(&r.id).bind(r.revision).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn enqueue_chunks(
    tx: &mut Transaction<'_, Sqlite>,
    machine: uuid::Uuid,
    event: &RecordSnapshot,
    now: i64,
) -> Result<()> {
    let pages = event
        .tasks
        .len()
        .max(event.messages.len())
        .div_ceil(64)
        .max(1);
    for page in 0..pages {
        let start = page * 64;
        let tasks = event
            .tasks
            .get(start..event.tasks.len().min(start + 64))
            .unwrap_or_default()
            .to_vec();
        let messages = event
            .messages
            .get(start..event.messages.len().min(start + 64))
            .unwrap_or_default()
            .to_vec();
        crate::relay::enqueue(
            tx,
            machine,
            crate::relay::Event::RecordSnapshot(RecordSnapshot {
                record: event.record.clone(),
                tasks,
                messages,
            }),
            now,
        )
        .await?;
    }
    Ok(())
}
