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
