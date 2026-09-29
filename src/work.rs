//! Versioned work records stored beside durable mail.

use crate::{
    bounded, name, relay,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

const SCOPE_LIMIT: usize = 1024;
const ACTION_LIMIT: usize = 512;
const EVIDENCE_LIMIT: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkDraft {
    pub id: String,
    pub scope: String,
    pub owner: String,
    pub state: String,
    pub next_action: String,
    pub deadline: Option<i64>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkPatch {
    pub owner: Option<String>,
    pub state: Option<String>,
    pub open: Option<bool>,
    pub next_action: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_update"
    )]
    pub deadline: Option<Option<i64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_update"
    )]
    pub accepted_revision: Option<Option<String>>,
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

/// One authorized work decision, including the related obligation to resolve.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkDecision {
    pub key: String,
    pub version: i64,
    pub patch: WorkPatch,
    pub reason: String,
    pub resolve_message: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub group_name: String,
    pub id: String,
    pub scope: String,
    pub owner: String,
    pub writer: String,
    pub state: String,
    pub open: bool,
    pub next_action: String,
    pub deadline: Option<i64>,
    pub accepted_revision: Option<String>,
    pub evidence: Vec<String>,
    pub version: i64,
    pub updated: i64,
    #[serde(default)]
    pub synced_at: Option<i64>,
    #[serde(default)]
    pub linked_messages: Vec<i64>,
}

#[derive(Debug, Serialize)]
pub struct WorkSummary {
    pub id: String,
    pub scope: String,
    pub owner: String,
    pub state: String,
    pub next_action: String,
    pub deadline: Option<i64>,
    pub accepted_revision: Option<String>,
    pub version: i64,
    pub synced_at: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct WorkChange {
    pub version: i64,
    pub actor: String,
    pub reason: String,
    pub snapshot: WorkItem,
    pub changed: i64,
}

struct WorkRow {
    group_name: String,
    id: String,
    scope: String,
    owner: String,
    writer: String,
    state: String,
    open: i64,
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
            open: row.open != 0,
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
    name(&item.state)?;
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
    pub async fn work_create(
        &self,
        actor: &Mailbox,
        draft: WorkDraft,
        now: i64,
    ) -> Result<WorkItem> {
        let item = WorkItem {
            group_name: actor.group_name.clone(),
            id: draft.id,
            scope: draft.scope,
            owner: draft.owner,
            writer: actor.name.clone(),
            state: draft.state,
            open: true,
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
        let mut tx = self.pool.begin().await?;
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
        ensure!(
            sqlx::query!(
                "SELECT id FROM mailboxes WHERE group_name=? AND name=?",
                item.group_name,
                item.owner
            )
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
            "work owner is not bound in this group"
        );
        sqlx::query!("INSERT INTO work_items(group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            item.group_name, item.id, item.scope, item.owner, item.writer, item.state,
            item.open, item.next_action, item.deadline, item.accepted_revision, evidence,
            item.version, item.updated).execute(&mut *tx).await?;
        sqlx::query!("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES (?,?,?,?,?,?,?)",
            item.group_name, item.id, item.version, actor.name, "created", snapshot, now)
            .execute(&mut *tx).await?;
        relay::enqueue_snapshot(&mut tx, &item, None, now).await?;
        tx.commit().await?;
        Ok(item)
    }

    pub async fn work_show(&self, actor: &Mailbox, id: &str) -> Result<WorkItem> {
        name(id)?;
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let row = sqlx::query_as!(WorkRow,
            "SELECT group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND id=?",
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
        tx.commit().await?;
        Ok(item)
    }

    pub async fn work_list(&self, actor: &Mailbox, after: &str) -> Result<Vec<WorkSummary>> {
        if !after.is_empty() {
            name(after)?;
        }
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let locals = sqlx::query_as!(WorkRow,
            "SELECT group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND (owner=? OR writer=?) AND open=1 AND id>? ORDER BY id LIMIT 6",
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
            if item.open {
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

    pub async fn work_update(
        &self,
        actor: &Mailbox,
        id: &str,
        expected: i64,
        patch: WorkPatch,
        reason: &str,
        now: i64,
    ) -> Result<WorkItem> {
        self.apply_work(
            actor,
            id,
            WorkDecision {
                key: String::new(),
                version: expected,
                patch,
                reason: reason.to_owned(),
                resolve_message: None,
            },
            now,
        )
        .await
    }

    pub async fn work_decide(
        &self,
        actor: &Mailbox,
        id: &str,
        decision: WorkDecision,
        now: i64,
    ) -> Result<WorkItem> {
        name(&decision.key)?;
        self.apply_work(actor, id, decision, now).await
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
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
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
            "SELECT group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated FROM work_items WHERE group_name=? AND id=?",
            actor.group_name, id)
            .fetch_optional(&mut *tx).await?.context("work item not found in this group")?;
        let mut item: WorkItem = row.try_into()?;
        let previous_owner = item.owner.clone();
        ensure!(
            item.writer == actor.name,
            "only the designated writer may update this work item"
        );
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
        if let Some(open) = patch.open {
            item.open = open;
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
                "SELECT id FROM mailboxes WHERE group_name=? AND name=?",
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
        let result = sqlx::query!("UPDATE work_items SET owner=?,state=?,open=?,next_action=?,deadline=?,accepted_revision=?,evidence=?,version=?,updated=? WHERE group_name=? AND id=? AND version=?",
            item.owner, item.state, item.open, item.next_action, item.deadline,
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
        Ok(item)
    }

    pub async fn work_history(&self, actor: &Mailbox, id: &str) -> Result<Vec<WorkChange>> {
        name(id)?;
        self.work_show(actor, id).await?;
        ensure!(
            sqlx::query!(
                "SELECT id FROM work_items WHERE group_name=? AND id=?",
                actor.group_name,
                id
            )
            .fetch_optional(&self.pool)
            .await?
            .is_some(),
            "change history is available on the home machine"
        );
        let rows = sqlx::query!("SELECT version,actor,reason,snapshot,changed FROM work_changes WHERE group_name=? AND work_id=? ORDER BY version DESC LIMIT 20",
            actor.group_name, id).fetch_all(&self.pool).await?;
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
