//! Recipient-scoped coordination events and durable transport receipts.
//!
//! Replay is bounded and checked against the actor's binding generation. A receipt
//! acknowledges one event for that generation, never a business decision. Hook
//! reservations consume a retry budget before emission; emission is not proof that
//! the client consumed the payload.

use crate::states::EventKind;
use crate::{
    bounded,
    store::{Mailbox, Store},
};
use anyhow::{Result, ensure};
use serde::Serialize;

/// A durable change hint addressed to one participant.
#[derive(Debug, Clone, Serialize)]
pub struct Notification {
    /// Persistent identifier for this record.
    pub id: i64,
    /// Event or diagnostic category.
    pub kind: EventKind,
    /// Identifier of the message, work record, or other changed subject.
    pub subject: String,
    /// Record or protocol version used to validate this operation.
    pub version: i64,
}

impl Store {
    // Retrieval receipts stop transport retries, never resolve business requests.
    pub(crate) async fn retrieve_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        actor: &Mailbox,
        kind: EventKind,
        subject: &str,
        version: i64,
    ) -> Result<()> {
        Self::retrieve_followup_tx(tx, actor, kind, subject, version, crate::now()?).await?;
        {
            let kind = kind.as_str();
            let inserted=sqlx::query!("INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) SELECT recipient,?,id FROM coordination_events WHERE recipient=? AND kind=? AND subject=? AND version<=?",actor.binding_version,actor.id,kind,subject,version).execute(&mut **tx).await?.rows_affected();
            if inserted > 0 {
                sqlx::query!("UPDATE runtime_wakes SET attempts=0,next_attempt=0 WHERE recipient=? AND binding_version=? AND EXISTS(SELECT 1 FROM attention_dispatch d,json_each(d.items,'$.items') i WHERE d.recipient=? AND d.binding_version=? AND json_extract(i.value,'$.kind')=? AND json_extract(i.value,'$.subject')=? AND json_extract(i.value,'$.revision')<=?)",actor.id,actor.binding_version,actor.id,actor.binding_version,kind,subject,version).execute(&mut **tx).await?;
            }
        }
        Ok(())
    }

    /// Record retrieval of exactly the records included in a bounded response.
    ///
    /// # Errors
    /// The binding is stale or persistence fails.
    pub async fn retrieved(
        &self,
        actor: &Mailbox,
        mail: &[i64],
        work: &[(String, i64)],
    ) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        for id in mail {
            Self::retrieve_tx(&mut tx, actor, EventKind::MailPending, &id.to_string(), 0).await?;
        }
        for (id, version) in work {
            Self::retrieve_tx(&mut tx, actor, EventKind::WorkChanged, id, *version).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Replay a bounded page of unacknowledged events for this binding.
    ///
    /// # Errors
    /// The cursor is negative, the binding is stale, or the query fails.
    pub async fn notifications(&self, actor: &Mailbox, after: i64) -> Result<Vec<Notification>> {
        ensure!(after >= 0, "event cursor must be nonnegative");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query_as!(Notification,
            "SELECT e.id,e.kind AS 'kind: EventKind',e.subject,e.version FROM coordination_events e WHERE e.recipient=? AND e.id>? AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=? AND r.event=e.id) ORDER BY e.id LIMIT 6",
            actor.id, after, actor.binding_version).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// Read bounded, coalesced change hints for this participant.
    ///
    /// # Errors
    /// The binding is stale or the query fails.
    pub async fn latest_changes(&self, actor: &Mailbox) -> Result<Vec<Notification>> {
        self.latest_changes_since(actor, 0).await
    }
    async fn latest_changes_since(&self, actor: &Mailbox, after: i64) -> Result<Vec<Notification>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query_as!(Notification,
            "SELECT e.id,e.kind AS 'kind: EventKind',e.subject,e.version FROM coordination_events e WHERE e.recipient=? AND e.id>? AND NOT EXISTS(SELECT 1 FROM coordination_events newer WHERE newer.recipient=e.recipient AND newer.kind=e.kind AND newer.subject=e.subject AND newer.id>e.id) ORDER BY e.id DESC LIMIT 6",actor.id,after).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// Report outstanding notifications and restored recovery epochs.
    ///
    /// # Errors
    /// The database queries fail.
    pub async fn notification_status(&self) -> Result<serde_json::Value> {
        self.notification_status_for(None).await
    }
    /// Inspect notification budgets within the selected group.
    /// # Errors
    /// Database queries fail.
    pub async fn notification_status_for(&self, group: Option<&str>) -> Result<serde_json::Value> {
        let rows = sqlx::query!("SELECT b.group_name,b.name,b.binding_version,COUNT(e.id) AS count FROM mailboxes b JOIN coordination_events e ON e.recipient=b.id WHERE (? IS NULL OR b.group_name=?) AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=b.id AND r.binding_version=b.binding_version AND r.event=e.id) GROUP BY b.id ORDER BY b.id",group,group).fetch_all(self.pool()).await?;
        let emissions = sqlx::query!("SELECT b.group_name,b.name FROM recovery_emissions h JOIN mailboxes b ON b.id=h.recipient AND b.binding_version=h.binding_version WHERE (? IS NULL OR b.group_name=?) ORDER BY b.id LIMIT 20",group,group).fetch_all(self.pool()).await?;
        Ok(
            serde_json::json!({"unacknowledged":rows.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"binding_version":r.binding_version,"count":r.count})).collect::<Vec<_>>(),"recovery_epochs":emissions.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"delivery_confirmed":false})).collect::<Vec<_>>()}),
        )
    }

    /// Acknowledge one transport event for the current binding generation.
    ///
    /// # Errors
    /// The actor is stale, the event belongs elsewhere, or persistence fails.
    pub async fn acknowledge(&self, actor: &Mailbox, event: i64) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let exists = sqlx::query!(
            "SELECT id FROM coordination_events WHERE id=? AND recipient=?",
            event,
            actor.id
        )
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(
            exists.is_some(),
            "event does not belong to this participant"
        );
        sqlx::query!(
            "INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) VALUES(?,?,?)",
            actor.id,
            actor.binding_version,
            event
        )
        .execute(&mut *tx)
        .await?;
        Self::reset_empty(&mut tx, &actor.group_name).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Mark a client session’s recovery state dirty.
    /// pretending PostCompact stdout is injected into the model's context.
    ///
    /// # Errors
    /// The session identifier is invalid, the actor is stale, or persistence fails.
    pub async fn invalidate_hook(&self, actor: &Mailbox, session: &str) -> Result<()> {
        bounded(session, 160, "client session")?;
        ensure!(!session.is_empty(), "hook requires a client session ID");
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("DELETE FROM recovery_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Whether this recovery epoch still needs the bundled operating instructions.
    pub async fn hook_needs_instructions(&self, actor: &Mailbox, session: &str) -> Result<bool> {
        Ok(sqlx::query!("SELECT recipient FROM recovery_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session).fetch_optional(self.pool()).await?.is_none())
    }

    pub(crate) async fn reserve_recovery(
        &self,
        actor: &Mailbox,
        session: &crate::names::SessionId,
        reset: bool,
    ) -> Result<bool> {
        let session = session.as_str();
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let inserted=sqlx::query!("INSERT OR IGNORE INTO recovery_emissions(recipient,binding_version,client_session) VALUES(?,?,?)",actor.id,actor.binding_version,session).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(reset || inserted != 0)
    }
}

impl Store {
    pub(crate) async fn scan_passive(&self, actor: &Mailbox, through: i64) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("UPDATE runtime_wakes SET scanned=MAX(scanned,?) WHERE recipient=? AND binding_version=? AND NOT EXISTS(SELECT 1 FROM wake_events WHERE recipient=? AND id>scanned AND id<=?)",through,actor.id,actor.binding_version,actor.id,through).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
