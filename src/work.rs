//! Versioned work records and atomic decisions stored beside durable mail.
//!
//! Creation and updates are restricted to the group's home machine. Decisions
//! validate actor generations and expected versions, and may resolve a related
//! message in the same transaction. Task actionability is derived from its typed lifecycle status. Times are Unix seconds.

use crate::states::TaskState;
use crate::{
    bounded, name, relay,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

const SCOPE_LIMIT: usize = 1024;
const ACTION_LIMIT: usize = 512;
const EVIDENCE_LIMIT: usize = 16;

/// Initial work fields before versioning and actor metadata are assigned.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkDraft {
    /// Persistent identifier for this record.
    pub id: String,
    /// Description of the work’s boundaries and intended outcome.
    pub scope: String,
    /// Participant responsible for the assignment.
    pub owner: String,
    /// Stored business state; it does not imply transport delivery.
    pub state: TaskState,
    /// Next business action expected from the owner.
    pub next_action: String,
    /// Deadline in Unix seconds; in patches, Some(None) explicitly clears it.
    pub deadline: Option<i64>,
    /// References or notes supporting the current work state.
    pub evidence: Vec<String>,
}

/// Partial work changes; missing fields retain their current values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkPatch {
    /// Participant responsible for the assignment.
    pub owner: Option<String>,
    /// Stored business state; it does not imply transport delivery.
    pub state: Option<TaskState>,
    /// Next business action expected from the owner.
    pub next_action: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_update"
    )]
    /// Deadline in Unix seconds; in patches, Some(None) explicitly clears it.
    pub deadline: Option<Option<i64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_update"
    )]
    /// Accepted revision; in patches, Some(None) explicitly clears it.
    pub accepted_revision: Option<Option<String>>,
    /// References or notes supporting the current work state.
    pub evidence: Option<Vec<String>>,
}

// Missing means unchanged; JSON null explicitly clears nullable work fields.
fn nullable_update<'de, D, T>(deserializer: D) -> std::result::Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

/// A work transition whose retry identity is derived from its expected version.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkUpdate {
    /// Version observed before deciding the change.
    pub version: i64,
    /// Explanation of the authorized change.
    pub reason: String,
    /// Fields to change; omitted fields remain unchanged.
    pub patch: WorkPatch,
    /// Linked inbox request resolved in the same transaction.
    pub resolve_message: Option<i64>,
}

/// One authorized work decision, including the related obligation to resolve.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkDecision {
    /// Caller-supplied idempotency key; retries must preserve their original content.
    pub key: String,
    /// Record or protocol version used to validate this operation.
    pub version: i64,
    /// Partial changes applied after validating the expected version.
    pub patch: WorkPatch,
    /// Explicit explanation for the business change.
    pub reason: String,
    /// Optional message to resolve in the same work-decision transaction.
    pub resolve_message: Option<i64>,
}

/// A versioned work record with ownership, progress, and synchronization metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    /// Enrolled group containing this record.
    pub group_name: String,
    /// Persistent identifier for this record.
    pub id: String,
    /// Description of the work’s boundaries and intended outcome.
    pub scope: String,
    /// Participant responsible for the assignment.
    pub owner: String,
    /// Participant that originally created the work record.
    pub writer: String,
    /// Stored business state; it does not imply transport delivery.
    pub state: TaskState,
    /// Next business action expected from the owner.
    pub next_action: String,
    /// Deadline in Unix seconds; in patches, Some(None) explicitly clears it.
    pub deadline: Option<i64>,
    /// Accepted revision; in patches, Some(None) explicitly clears it.
    pub accepted_revision: Option<String>,
    /// References or notes supporting the current work state.
    pub evidence: Vec<String>,
    /// Record or protocol version used to validate this operation.
    pub version: i64,
    /// Last business update timestamp in Unix seconds.
    pub updated: i64,
    /// Last remote snapshot receipt time in Unix seconds, when applicable.
    #[serde(default)]
    pub synced_at: Option<i64>,
    /// Message identifiers associated with this work record.
    #[serde(default)]
    pub linked_messages: Vec<i64>,
}

/// Compact work fields used in paginated recovery views.
#[derive(Debug, Serialize)]
pub struct WorkSummary {
    /// Persistent identifier for this record.
    pub id: String,
    /// Description of the work’s boundaries and intended outcome.
    pub scope: String,
    /// Participant responsible for the assignment.
    pub owner: String,
    /// Stored business state; it does not imply transport delivery.
    pub state: TaskState,
    /// Next business action expected from the owner.
    pub next_action: String,
    /// Deadline in Unix seconds; in patches, Some(None) explicitly clears it.
    pub deadline: Option<i64>,
    /// Accepted revision; in patches, Some(None) explicitly clears it.
    pub accepted_revision: Option<String>,
    /// Record or protocol version used to validate this operation.
    pub version: i64,
    /// Last remote snapshot receipt time in Unix seconds, when applicable.
    pub synced_at: Option<i64>,
}

/// An audit entry containing a work revision and its change reason.
#[derive(Debug, Serialize)]
pub struct WorkChange {
    /// Record or protocol version used to validate this operation.
    pub version: i64,
    /// Participant responsible for this recorded revision.
    pub actor: String,
    /// Explicit explanation for the business change.
    pub reason: String,
    /// Complete work state at this revision.
    pub snapshot: WorkItem,
    /// Revision creation timestamp in Unix seconds.
    pub changed: i64,
}

struct WorkRow {
    group_name: String,
    id: String,
    scope: String,
    owner: String,
    writer: String,
    state: TaskState,
    next_action: String,
    deadline: Option<i64>,
    accepted_revision: Option<String>,
    evidence: String,
    version: i64,
    updated: i64,
}

impl TryFrom<WorkRow> for WorkItem {
    type Error = anyhow::Error;

    fn try_from(row: WorkRow) -> Result<Self> {
        Ok(Self {
            group_name: row.group_name,
            id: row.id,
            scope: row.scope,
            owner: row.owner,
            writer: row.writer,
            state: row.state,
            next_action: row.next_action,
            deadline: row.deadline,
            accepted_revision: row.accepted_revision,
            evidence: serde_json::from_str(&row.evidence).context("decode work evidence")?,
            version: row.version,
            updated: row.updated,
            synced_at: None,
            linked_messages: Vec::new(),
        })
    }
}

fn validate_evidence(evidence: &[String]) -> Result<()> {
    ensure!(
        evidence.len() <= EVIDENCE_LIMIT,
        "too many evidence references"
    );
    for reference in evidence {
        bounded(reference, 256, "evidence reference")?;
        ensure!(!reference.trim().is_empty(), "evidence reference is empty");
    }
    Ok(())
}

fn validate_fields(item: &WorkItem) -> Result<()> {
    name(&item.id)?;
    name(&item.owner)?;
    name(&item.writer)?;
    bounded(&item.scope, SCOPE_LIMIT, "scope")?;
    ensure!(!item.scope.trim().is_empty(), "scope is required");
    bounded(&item.next_action, ACTION_LIMIT, "next action")?;
    ensure!(
        !item.next_action.trim().is_empty(),
        "next action is required"
    );
    if let Some(revision) = &item.accepted_revision {
        bounded(revision, 128, "accepted revision")?;
        ensure!(!revision.trim().is_empty(), "accepted revision is empty");
    }
    if let Some(deadline) = item.deadline {
        ensure!(deadline > 0, "deadline must be a positive Unix timestamp");
    }
    validate_evidence(&item.evidence)
}

impl Store {
    /// Create a versioned work record and notify its owner atomically.
    ///
    /// # Errors
    /// The actor or fields are invalid, this is not the home machine, the owner is missing, or persistence fails.
    pub async fn work_create(
        &self,
        actor: &Mailbox,
        draft: WorkDraft,
        now: i64,
    ) -> Result<WorkItem> {
        let canonical = serde_json::to_string(&draft)?;
        let item = WorkItem {
            group_name: actor.group_name.clone(),
            id: draft.id,
            scope: draft.scope,
            owner: draft.owner,
            writer: actor.name.clone(),
            state: draft.state,
            next_action: draft.next_action,
            deadline: draft.deadline,
            accepted_revision: None,
            evidence: draft.evidence,
            version: 1,
            updated: now,
            synced_at: None,
            linked_messages: Vec::new(),
        };
        validate_fields(&item)?;
        let evidence = serde_json::to_string(&item.evidence)?;
        let snapshot = serde_json::to_string(&item)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let home = sqlx::query!(
            "SELECT home_machine FROM groups WHERE name=?",
            actor.group_name
        )
        .fetch_one(&mut *tx)
        .await?
        .home_machine;
        let local = sqlx::query!("SELECT id FROM node LIMIT 1")
            .fetch_one(&mut *tx)
            .await?
            .id;
        ensure!(
            home == local,
            "work records are writable only on the home machine"
        );
        if let Some(old) = sqlx::query!(
            "SELECT actor,canonical,result FROM work_creations WHERE group_name=? AND work_id=?",
            actor.group_name,
            item.id
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            ensure!(
                old.actor == actor.id && old.canonical == canonical,
                "work ID already created with different content or writer"
            );
            return Ok(serde_json::from_str(&old.result)?);
        }
        ensure!(
            sqlx::query!(
                "SELECT id FROM work_items WHERE group_name=? AND id=?",
                actor.group_name,
                item.id
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_none(),
            "work ID already exists without creation provenance; inspect it with work show"
        );
        ensure!(
            sqlx::query!(
                "SELECT id FROM mailboxes WHERE group_name=? AND name=? AND agent_state='registered'",
                item.group_name,
                item.owner
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
            "work owner is not bound in this group"
        );
        let state = item.state.as_str();
        let open = item.state.is_open();
        sqlx::query!("INSERT INTO work_items(group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            item.group_name, item.id, item.scope, item.owner, item.writer, state,
            open, item.next_action, item.deadline, item.accepted_revision, evidence,
            item.version, item.updated).execute(&mut *tx).await?;
        sqlx::query!("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES (?,?,?,?,?,?,?)",
            item.group_name, item.id, item.version, actor.name, "created", snapshot, now)
            .execute(&mut *tx).await?;
        sqlx::query!("INSERT INTO work_creations(group_name,work_id,actor,canonical,result) VALUES(?,?,?,?,?)",actor.group_name,item.id,actor.id,canonical,snapshot).execute(&mut *tx).await?;
        relay::enqueue_snapshot(&mut tx, &item, None, now).await?;
        Self::retrieve_tx(
            &mut tx,
            actor,
            crate::states::EventKind::WorkChanged,
            &item.id,
            item.version,
        )
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(item)
    }

    /// Read a work record visible to the participant’s group.
    ///
    /// # Errors
    /// The actor is stale, the record is absent, or decoding or querying fails.
    pub async fn work_show(&self, actor: &Mailbox, id: &str) -> Result<WorkItem> {
        name(id)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let row = sqlx::query_as!(WorkRow,
            "SELECT group_name,id,scope,owner,writer,state AS 'state: TaskState',next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND id=?",
            actor.group_name, id)
            .fetch_optional(&mut *tx).await?;
        let mut item = if let Some(row) = row {
            WorkItem::try_from(row)?
        } else {
            let snapshot = sqlx::query!(
                "SELECT snapshot,synced_at FROM work_snapshots WHERE group_name=? AND work_id=?",
                actor.group_name,
                id
            )
            .fetch_optional(&mut *tx)
            .await?
            .context("work item not found in this group")?;
            let mut item: WorkItem = serde_json::from_str(&snapshot.snapshot)?;
            item.synced_at = Some(snapshot.synced_at);
            item
        };
        let links = sqlx::query!("SELECT m.id FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? AND m.work_id=? ORDER BY m.id DESC LIMIT 20",
            actor.group_name, id).fetch_all(&mut *tx).await?;
        item.linked_messages = links.into_iter().map(|row| row.id).collect();
        Self::retrieve_tx(
            &mut tx,
            actor,
            crate::states::EventKind::WorkChanged,
            id,
            item.version,
        )
        .await?;
        tx.commit().await?;
        Ok(item)
    }

    /// Read a bounded page of open work after an identifier cursor.
    ///
    /// # Errors
    /// The actor is stale or stored work cannot be queried or decoded.
    pub async fn work_list(&self, actor: &Mailbox, after: &str) -> Result<Vec<WorkSummary>> {
        if !after.is_empty() {
            name(after)?;
        }
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let locals = sqlx::query_as!(WorkRow,
            "SELECT group_name,id,scope,owner,writer,state AS 'state: TaskState',next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND (owner=? OR writer=?) AND open=1 AND id>? ORDER BY id LIMIT 6",
            actor.group_name, actor.name, actor.name, after)
            .fetch_all(&mut *tx).await?;
        let mut rows: Vec<WorkSummary> = locals
            .into_iter()
            .map(|item| WorkSummary {
                id: item.id,
                scope: item.scope,
                owner: item.owner,
                state: item.state,
                next_action: item.next_action,
                deadline: item.deadline,
                accepted_revision: item.accepted_revision,
                version: item.version,
                synced_at: None,
            })
            .collect();
        let snapshots = sqlx::query!("SELECT snapshot,synced_at FROM work_snapshots WHERE group_name=? AND owner=? AND work_id>? ORDER BY work_id LIMIT 6",
            actor.group_name, actor.name, after).fetch_all(&mut *tx).await?;
        for snapshot in snapshots {
            let item: WorkItem = serde_json::from_str(&snapshot.snapshot)?;
            if item.state.is_open() {
                rows.push(WorkSummary {
                    id: item.id,
                    scope: item.scope,
                    owner: item.owner,
                    state: item.state,
                    next_action: item.next_action,
                    deadline: item.deadline,
                    accepted_revision: item.accepted_revision,
                    version: item.version,
                    synced_at: Some(snapshot.synced_at),
                });
            }
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        rows.truncate(6);
        tx.commit().await?;
        Ok(rows)
    }

    /// Apply a retry-safe work update and optional linked-message resolution.
    ///
    /// # Errors
    /// Authority, version, replay content, fields or persistence is invalid.
    pub async fn update_work(
        &self,
        actor: &Mailbox,
        id: &str,
        update: WorkUpdate,
        now: i64,
    ) -> Result<WorkItem> {
        self.apply_work(
            actor,
            id,
            WorkDecision {
                key: format!("update:{id}:{}", update.version),
                version: update.version,
                reason: update.reason,
                patch: update.patch,
                resolve_message: update.resolve_message,
            },
            now,
        )
        .await
    }

    async fn apply_work(
        &self,
        actor: &Mailbox,
        id: &str,
        decision: WorkDecision,
        now: i64,
    ) -> Result<WorkItem> {
        let canonical = serde_json::to_string(&(id, &decision))?;
        let WorkDecision {
            key,
            version: expected,
            patch,
            reason,
            resolve_message,
        } = decision;
        name(id)?;
        bounded(&reason, 512, "change reason")?;
        ensure!(!reason.trim().is_empty(), "change reason is required");
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let home = sqlx::query!(
            "SELECT home_machine FROM groups WHERE name=?",
            actor.group_name
        )
        .fetch_one(&mut *tx)
        .await?
        .home_machine;
        let local = sqlx::query!("SELECT id FROM node LIMIT 1")
            .fetch_one(&mut *tx)
            .await?
            .id;
        ensure!(
            home == local,
            "work records are writable only on the home machine"
        );
        let row = sqlx::query_as!(WorkRow,
            "SELECT group_name,id,scope,owner,writer,state AS 'state: TaskState',next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND id=?",
            actor.group_name, id)
            .fetch_optional(&mut *tx).await?.context("work item not found in this group")?;
        let mut item: WorkItem = row.try_into()?;
        let previous_owner = item.owner.clone();
        ensure!(
            item.writer == actor.name,
            "only the designated writer may update this work item"
        );
        if !key.is_empty() {
            if let Some(old) = sqlx::query!(
                "SELECT canonical,result FROM work_decisions WHERE actor=? AND key=?",
                actor.id,
                key
            )
            .fetch_optional(&mut *tx)
            .await?
            {
                ensure!(
                    old.canonical == canonical,
                    "decision key reused with different content"
                );
                return Ok(serde_json::from_str(&old.result)?);
            }
        }

        ensure!(
            item.version == expected,
            "work version conflict; read current record before retrying"
        );
        if let Some(owner) = patch.owner {
            item.owner = owner;
        }
        if let Some(state) = patch.state {
            item.state = state;
        }
        if let Some(next_action) = patch.next_action {
            item.next_action = next_action;
        }
        if let Some(deadline) = patch.deadline {
            item.deadline = deadline;
        }
        if let Some(revision) = patch.accepted_revision {
            item.accepted_revision = revision;
        }
        if let Some(evidence) = patch.evidence {
            item.evidence = evidence;
        }
        item.version = item
            .version
            .checked_add(1)
            .context("work version overflow")?;
        item.updated = now;
        validate_fields(&item)?;
        ensure!(
            sqlx::query!(
                "SELECT id FROM mailboxes WHERE group_name=? AND name=? AND agent_state='registered'",
                item.group_name,
                item.owner
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
            "work owner is not bound in this group"
        );
        let evidence = serde_json::to_string(&item.evidence)?;
        let snapshot = serde_json::to_string(&item)?;
        let state = item.state.as_str();
        let open = item.state.is_open();
        let result = sqlx::query!("UPDATE work_items SET owner=?,state=?,open=?,next_action=?,deadline=?,accepted_revision=?,evidence=?,version=?,updated=? WHERE group_name=? AND id=? AND version=?",
            item.owner, state, open, item.next_action, item.deadline,
            item.accepted_revision, evidence, item.version, item.updated, item.group_name, item.id,
            expected).execute(&mut *tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "work version conflict; read current record before retrying"
        );
        sqlx::query!("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES (?,?,?,?,?,?,?)",
            item.group_name, item.id, item.version, actor.name, reason, snapshot, now)
            .execute(&mut *tx).await?;
        relay::enqueue_snapshot(&mut tx, &item, Some(&previous_owner), now).await?;
        Self::retrieve_tx(
            &mut tx,
            actor,
            crate::states::EventKind::WorkChanged,
            &item.id,
            item.version,
        )
        .await?;

        if let Some(message) = resolve_message {
            let linked = sqlx::query!("SELECT work_id FROM messages WHERE id=?", message)
                .fetch_optional(&mut *tx)
                .await?;
            ensure!(
                linked.and_then(|r| r.work_id).as_deref() == Some(id),
                "resolved message must reference this work item"
            );
            Self::resolve_tx(&mut tx, actor, message, &reason, None, now).await?;
        }
        if !key.is_empty() {
            let result = serde_json::to_string(&item)?;
            sqlx::query!(
                "INSERT INTO work_decisions(actor,key,canonical,result) VALUES(?,?,?,?)",
                actor.id,
                key,
                canonical,
                result
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(item)
    }

    /// Read the recorded revisions of work in the participant’s group.
    ///
    /// # Errors
    /// The actor is stale, the work is absent, or querying or decoding fails.
    pub async fn work_history(&self, actor: &Mailbox, id: &str) -> Result<Vec<WorkChange>> {
        name(id)?;
        self.work_show(actor, id).await?;
        ensure!(
            sqlx::query!(
                "SELECT id FROM work_items WHERE group_name=? AND id=?",
                actor.group_name,
                id
            )
            .fetch_optional(self.pool())
            .await?
            .is_some(),
            "change history is available on the home machine"
        );
        let rows = sqlx::query!("SELECT version,actor,reason,snapshot,changed FROM work_changes WHERE group_name=? AND work_id=? ORDER BY version DESC LIMIT 20",
            actor.group_name, id).fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(WorkChange {
                    version: row.version,
                    actor: row.actor,
                    reason: row.reason,
                    snapshot: serde_json::from_str(&row.snapshot)?,
                    changed: row.changed,
                })
            })
            .collect()
    }
}

/// Opt-in group task discovery. Empty states selects all lifecycle states.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkListQuery {
    /// Exact lifecycle filters; empty includes terminal records.
    #[serde(default)]
    pub states: Vec<TaskState>,
    /// Optional owner filter.
    pub owner: Option<String>,
    /// Scoped continuation from the preceding page.
    pub cursor: Option<String>,
    /// Page size, from one to one hundred.
    pub limit: Option<usize>,
}

fn page_size(limit: Option<usize>) -> Result<usize> {
    let limit = limit.unwrap_or(20);
    ensure!((1..=100).contains(&limit), "page limit must be 1..100");
    Ok(limit)
}

fn decode_cursor(cursor: Option<&str>, scope: &str) -> Result<(String, i64)> {
    match cursor {
        None => Ok((String::new(), i64::MAX)),
        Some(cursor) => {
            bounded(cursor, 4096, "cursor")?;
            let (saved, after, ceiling): (String, String, i64) =
                serde_json::from_str(cursor).context("invalid page cursor")?;
            ensure!(
                saved == scope,
                "cursor belongs to another query or identity"
            );
            Ok((after, ceiling))
        }
    }
}

fn encode_cursor(scope: &str, after: &str, ceiling: i64) -> Result<String> {
    Ok(serde_json::to_string(&(scope, after, ceiling))?)
}

impl Store {
    /// Enumerate group-visible tasks without changing task state or retrieval receipts.
    pub async fn work_list_page(
        &self,
        actor: &Mailbox,
        query: WorkListQuery,
    ) -> Result<serde_json::Value> {
        use sqlx::Row;
        let limit = page_size(query.limit)?;
        let scope = serde_json::to_string(&(
            "tasks",
            &actor.group_name,
            actor.id,
            actor.binding_version,
            &query.states,
            &query.owner,
        ))?;
        let (after, _) = decode_cursor(query.cursor.as_deref(), &scope)?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query("WITH visible AS (SELECT group_name,id,scope,owner,writer,state,next_action,accepted_revision,version FROM work_items UNION ALL SELECT group_name,work_id,json_extract(snapshot,'$.scope'),json_extract(snapshot,'$.owner'),json_extract(snapshot,'$.writer'),json_extract(snapshot,'$.state'),json_extract(snapshot,'$.next_action'),json_extract(snapshot,'$.accepted_revision'),json_extract(snapshot,'$.version') FROM work_snapshots s WHERE NOT EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=s.group_name AND w.id=s.work_id)) SELECT * FROM visible WHERE group_name=? AND id>? AND (? IS NULL OR owner=?) AND (?='[]' OR state IN (SELECT value FROM json_each(?))) ORDER BY id LIMIT ?")
            .bind(&actor.group_name).bind(after).bind(&query.owner).bind(&query.owner).bind(serde_json::to_string(&query.states)?).bind(serde_json::to_string(&query.states)?).bind((limit + 1) as i64).fetch_all(&mut *tx).await?;
        let more = rows.len() > limit;
        let items = rows.iter().take(limit).map(|r| serde_json::json!({"id":r.get::<String,_>("id"),"scope":r.get::<String,_>("scope"),"owner":r.get::<String,_>("owner"),"writer":r.get::<String,_>("writer"),"state":r.get::<String,_>("state"),"version":r.get::<i64,_>("version"),"next_action":r.get::<String,_>("next_action"),"accepted_revision":r.get::<Option<String>,_>("accepted_revision")})).collect::<Vec<_>>();
        let next = if more {
            Some(encode_cursor(
                &scope,
                items.last().unwrap()["id"].as_str().unwrap(),
                i64::MAX,
            )?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(
            serde_json::json!({"items":items,"more":more,"next_cursor":next,"ordering":"id_ascending","visibility":"group","authority":"home_or_cached_home_snapshot"}),
        )
    }

    /// Read stable descending revisions; new revisions are outside an existing cursor's ceiling.
    pub async fn work_history_page(
        &self,
        actor: &Mailbox,
        id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<serde_json::Value> {
        use sqlx::Row;
        name(id)?;
        let limit = page_size(limit)?;
        let scope = serde_json::to_string(&(
            "history",
            &actor.group_name,
            actor.id,
            actor.binding_version,
            id,
        ))?;
        let (after, ceiling) = decode_cursor(cursor, &scope)?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let current: Option<i64> =
            sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(current) = current else {
            let snapshot: Option<String> = sqlx::query_scalar(
                "SELECT snapshot FROM work_snapshots WHERE group_name=? AND work_id=?",
            )
            .bind(&actor.group_name)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
            let snapshot: WorkItem =
                serde_json::from_str(&snapshot.context("task not found in this group")?)?;
            tx.commit().await?;
            return Ok(
                serde_json::json!({"items":[],"more":false,"next_cursor":null,"complete":false,"current_snapshot":snapshot,"reason":"complete retained history requires the home machine"}),
            );
        };
        let ceiling = ceiling.min(current);
        let before = if after.is_empty() {
            i64::MAX
        } else {
            after.parse::<i64>()?
        };
        let rows = sqlx::query("SELECT version,actor,reason,snapshot,changed FROM work_changes WHERE group_name=? AND work_id=? AND version<=? AND version<? ORDER BY version DESC LIMIT ?").bind(&actor.group_name).bind(id).bind(ceiling).bind(before).bind((limit+1) as i64).fetch_all(&mut *tx).await?;
        let more = rows.len() > limit;
        let items = rows.iter().take(limit).map(|r| Ok(serde_json::json!({"version":r.get::<i64,_>("version"),"actor":r.get::<String,_>("actor"),"reason":r.get::<String,_>("reason"),"snapshot":serde_json::from_str::<serde_json::Value>(&r.get::<String,_>("snapshot"))?,"changed":r.get::<i64,_>("changed")}))).collect::<Result<Vec<_>>>()?;
        let next = if more {
            Some(encode_cursor(
                &scope,
                &items.last().unwrap()["version"].to_string(),
                ceiling,
            )?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(serde_json::json!({"items":items,"more":more,"next_cursor":next,"ceiling":ceiling}))
    }

    /// Enumerate linked message identifiers and visibility without private bodies or summaries.
    pub async fn work_messages_page(
        &self,
        actor: &Mailbox,
        id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<serde_json::Value> {
        use sqlx::Row;
        name(id)?;
        let limit = page_size(limit)?;
        let scope = serde_json::to_string(&(
            "messages",
            &actor.group_name,
            actor.id,
            actor.binding_version,
            id,
        ))?;
        let (after, ceiling) = decode_cursor(cursor, &scope)?;
        let before = if after.is_empty() {
            i64::MAX
        } else {
            after.parse::<i64>()?
        };
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=? UNION ALL SELECT 1 FROM work_snapshots WHERE group_name=? AND work_id=?)",
        )
        .bind(&actor.group_name)
        .bind(id)
        .bind(&actor.group_name)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(exists, "task not found on this home machine");
        let ceiling = if ceiling == i64::MAX {
            sqlx::query_scalar::<_,Option<i64>>("SELECT MAX(m.id) FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? AND m.work_id=?").bind(&actor.group_name).bind(id).fetch_one(&mut *tx).await?.unwrap_or(0)
        } else {
            ceiling
        };
        let rows=sqlx::query("SELECT m.id,m.created,b.name AS sender,(m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?)) AS readable FROM messages m JOIN mailboxes b ON b.id=m.sender WHERE b.group_name=? AND m.work_id=? AND m.id<=? AND m.id<? ORDER BY m.id DESC LIMIT ?").bind(actor.id).bind(actor.id).bind(&actor.group_name).bind(id).bind(ceiling).bind(before).bind((limit+1) as i64).fetch_all(&mut *tx).await?;
        let more = rows.len() > limit;
        let items=rows.iter().take(limit).map(|r|serde_json::json!({"id":r.get::<i64,_>("id"),"created":r.get::<i64,_>("created"),"sender":r.get::<String,_>("sender"),"body_visible":r.get::<bool,_>("readable")})).collect::<Vec<_>>();
        let next = if more {
            Some(encode_cursor(
                &scope,
                &items.last().unwrap()["id"].to_string(),
                ceiling,
            )?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(serde_json::json!({"items":items,"more":more,"next_cursor":next,"ceiling":ceiling}))
    }
}

/// Explicit decision authority handoff. Owner and private mail remain independently authorized.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterTransfer {
    /// Observed task version.
    pub version: i64,
    /// Expected old writer, also required for operator recovery.
    pub old_writer: String,
    /// Enrolled destination mailbox local to the home machine.
    pub new_writer: String,
    /// Observed destination generation, guarding replacement races.
    pub new_binding_version: i64,
    /// Explanation recorded in immutable decision history.
    pub reason: String,
}

impl Store {
    /// Transfer authority as the current writer.
    pub async fn work_transfer(
        &self,
        actor: &Mailbox,
        id: &str,
        transfer: WriterTransfer,
        now: i64,
    ) -> Result<WorkItem> {
        self.transfer_work(Some(actor), &actor.group_name, id, transfer, now)
            .await
    }

    /// Explicit local operator recovery; call only from the sessionless operator command.
    pub async fn operator_work_transfer(
        &self,
        group: &str,
        id: &str,
        transfer: WriterTransfer,
        now: i64,
    ) -> Result<WorkItem> {
        self.transfer_work(None, group, id, transfer, now).await
    }

    async fn transfer_work(
        &self,
        actor: Option<&Mailbox>,
        group: &str,
        id: &str,
        transfer: WriterTransfer,
        now: i64,
    ) -> Result<WorkItem> {
        use sqlx::Row;
        name(group)?;
        name(id)?;
        name(&transfer.old_writer)?;
        name(&transfer.new_writer)?;
        ensure!(
            transfer.old_writer != transfer.new_writer,
            "writer transfer needs a different destination"
        );
        bounded(&transfer.reason, 512, "transfer reason")?;
        ensure!(
            !transfer.reason.trim().is_empty(),
            "transfer reason is required"
        );
        let actor_key = actor.map_or_else(
            || "operator:local".to_owned(),
            |a| format!("mailbox:{}:{}", a.id, a.binding_version),
        );
        let canonical = serde_json::to_string(&transfer)?;
        let mut tx = self.pool().begin().await?;
        if let Some(actor) = actor {
            Self::lock_actor(&mut tx, actor).await?;
        } else {
            sqlx::query("UPDATE node SET id=id WHERE 0")
                .execute(&mut *tx)
                .await?;
        }
        let home: bool = sqlx::query_scalar(
            "SELECT home_machine=(SELECT id FROM node LIMIT 1) FROM groups WHERE name=?",
        )
        .bind(group)
        .fetch_optional(&mut *tx)
        .await?
        .context("group not found")?;
        ensure!(home, "task transfers require the home machine");
        if let Some(old)=sqlx::query("SELECT actor,canonical,result FROM task_transfers WHERE group_name=? AND work_id=? AND expected=?").bind(group).bind(id).bind(transfer.version).fetch_optional(&mut *tx).await? {
            ensure!(old.get::<String,_>("actor")==actor_key && old.get::<String,_>("canonical")==canonical,"transfer retry differs from committed transfer");
            return Ok(serde_json::from_str(&old.get::<String,_>("result"))?);
        }
        let row=sqlx::query_as!(WorkRow,"SELECT group_name,id,scope,owner,writer,state AS 'state: TaskState',next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND id=?",group,id).fetch_optional(&mut *tx).await?.context("task not found")?;
        let mut item: WorkItem = row.try_into()?;
        ensure!(
            item.version == transfer.version && item.writer == transfer.old_writer,
            "writer or version conflict"
        );
        if let Some(actor) = actor {
            ensure!(
                actor.name == item.writer,
                "only current writer may transfer authority"
            );
        }
        let new_id:i64=sqlx::query_scalar("SELECT id FROM mailboxes WHERE group_name=? AND name=? AND binding_version=? AND agent_state='registered' AND remote_machine IS NULL").bind(group).bind(&transfer.new_writer).bind(transfer.new_binding_version).fetch_optional(&mut *tx).await?.context("destination must be a current registered local mailbox on the home machine; generation may be stale or retired")?;
        let prior_followup=sqlx::query("SELECT version,checkpoint,stage,next_check,retrieved_at,retrieved_binding,dependency_ready_at FROM followups WHERE group_name=? AND task=?").bind(group).bind(id).fetch_optional(&mut *tx).await?;
        item.writer = transfer.new_writer.clone();
        item.version = item
            .version
            .checked_add(1)
            .context("task version overflow")?;
        item.updated = now;
        let snapshot = serde_json::to_string(&item)?;
        sqlx::query("UPDATE work_items SET writer=?,version=?,updated=? WHERE group_name=? AND id=? AND version=?").bind(&item.writer).bind(item.version).bind(now).bind(group).bind(id).bind(transfer.version).execute(&mut *tx).await?;
        let audit_reason = format!(
            "writer transfer {} -> {} ({}): {}",
            transfer.old_writer, transfer.new_writer, actor_key, transfer.reason
        );
        sqlx::query("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES(?,?,?,?,?,?,?)").bind(group).bind(id).bind(item.version).bind(&actor_key).bind(audit_reason).bind(&snapshot).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_transfers(group_name,work_id,expected,actor,canonical,result,old_writer,new_writer,operator,changed) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(group).bind(id).bind(transfer.version).bind(actor_key).bind(canonical).bind(snapshot).bind(&transfer.old_writer).bind(&transfer.new_writer).bind(actor.is_none()).bind(now).execute(&mut *tx).await?;
        // Follow-up authority moves; escalation boundaries and owner obligations remain intact.
        if let Some(prior) = prior_followup {
            // Generic task revisions invalidate a checkpoint; a pure writer handoff preserves it.
            sqlx::query("UPDATE followups SET version=?,checkpoint=?,stage=?,next_check=?,retrieved_at=?,retrieved_binding=?,dependency_ready_at=? WHERE group_name=? AND task=?")
                .bind(prior.get::<i64,_>("version")).bind(prior.get::<Option<String>,_>("checkpoint")).bind(prior.get::<i64,_>("stage")).bind(prior.get::<i64,_>("next_check")).bind(prior.get::<Option<i64>,_>("retrieved_at")).bind(prior.get::<Option<i64>,_>("retrieved_binding")).bind(prior.get::<Option<i64>,_>("dependency_ready_at")).bind(group).bind(id).execute(&mut *tx).await?;
        }

        sqlx::query("UPDATE followups SET authority=? WHERE group_name=? AND task=?")
            .bind(new_id)
            .bind(group)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE attention_occurrences SET recipient=? WHERE stage=3 AND followup IN (SELECT id FROM followups WHERE group_name=? AND task=?)").bind(new_id).bind(group).bind(id).execute(&mut *tx).await?;
        relay::enqueue_snapshot(&mut tx, &item, None, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(item)
    }
}
