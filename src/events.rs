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
        if actor.binding.herdr().is_some() {
            let kind = kind.as_str();
            sqlx::query!("INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) SELECT recipient,?,id FROM coordination_events WHERE recipient=? AND kind=? AND subject=? AND version<=?",actor.binding_version,actor.id,kind,subject,version).execute(&mut **tx).await?;
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

    pub(crate) async fn herdr_attention(&self, actor: &Mailbox) -> Result<(bool, bool)> {
        let row = sqlx::query!("SELECT MAX(e.id) AS latest,b.wake_attempted AS 'wake_attempted!: i64' FROM mailboxes b LEFT JOIN herdr_wake_events e ON e.recipient=b.id WHERE b.id=? AND b.binding_version=? GROUP BY b.id",actor.id,actor.binding_version).fetch_one(self.pool()).await?;
        Ok((
            row.latest.is_some(),
            row.latest.is_some_and(|id| id > row.wake_attempted),
        ))
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

    /// Report outstanding notifications and persisted hook emission budgets.
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
        let emissions = sqlx::query!("SELECT b.group_name,b.name,h.attempts,h.next_attempt,h.stop_used FROM hook_emissions h JOIN mailboxes b ON b.id=h.recipient AND b.binding_version=h.binding_version WHERE (? IS NULL OR b.group_name=?) ORDER BY h.next_attempt DESC LIMIT 20",group,group).fetch_all(self.pool()).await?;
        Ok(
            serde_json::json!({"unacknowledged":rows.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"binding_version":r.binding_version,"count":r.count})).collect::<Vec<_>>(),"hook_attempts":emissions.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"attempts":r.attempts,"next_attempt":r.next_attempt,"stop_used":r.stop_used,"delivery_confirmed":false})).collect::<Vec<_>>()}),
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
        sqlx::query!("DELETE FROM hook_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Whether this recovery epoch still needs the bundled operating instructions.
    pub async fn hook_needs_instructions(&self, actor: &Mailbox, session: &str) -> Result<bool> {
        Ok(sqlx::query!("SELECT recipient FROM hook_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session).fetch_optional(self.pool()).await?.is_none())
    }

    /// Reserve a bounded hook emission at the supplied Unix timestamp.
    /// SessionStart always restores state, including after an ambiguous previous emission.
    ///
    /// # Errors
    /// The session is invalid, the actor is stale, time overflows, or persistence fails.
    pub async fn reserve_hook(
        &self,
        actor: &Mailbox,
        session: &str,
        reset: bool,
        stop: bool,
        now: i64,
    ) -> Result<bool> {
        bounded(session, 160, "client session")?;
        ensure!(!session.is_empty(), "hook requires a client session ID");
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("INSERT OR IGNORE INTO hook_emissions(recipient,binding_version,client_session) VALUES(?,?,?)",actor.id,actor.binding_version,session)
            .execute(&mut *tx).await?;
        let latest = sqlx::query!(
            "SELECT COALESCE(MAX(id),0) AS 'id!: i64' FROM coordination_events WHERE recipient=?",
            actor.id
        )
        .fetch_one(&mut *tx)
        .await?
        .id;
        let row = sqlx::query!("SELECT last_event,attempts,next_attempt,stop_used FROM hook_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session)
            .fetch_one(&mut *tx).await?;
        let changed = latest > row.last_event;
        // One Stop continuation per recovery epoch, even if events keep arriving.
        let actionable = sqlx::query!(
            "SELECT id FROM wake_events WHERE recipient=? AND id>? LIMIT 1",
            actor.id,
            row.last_event
        )
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        let emit = reset
            || (stop && row.stop_used == 0 && actionable)
            || (!stop && (changed || (row.attempts < 3 && now >= row.next_attempt)));
        if emit {
            let attempts = if reset || changed {
                1
            } else {
                row.attempts + 1
            };
            let stop_used = if reset {
                0
            } else if stop {
                1
            } else {
                row.stop_used
            };
            let next = now
                .checked_add(300)
                .ok_or_else(|| anyhow::anyhow!("clock overflow"))?;
            sqlx::query!("UPDATE hook_emissions SET last_event=?,attempts=?,next_attempt=?,stop_used=? WHERE recipient=? AND binding_version=? AND client_session=?",latest,attempts,next,stop_used,actor.id,actor.binding_version,session)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(emit)
    }
}

impl Store {
    pub(crate) async fn needs_cancellation(&self, actor: &Mailbox, after: i64) -> Result<bool> {
        Ok(sqlx::query!(
            "SELECT id FROM cancellation_events WHERE recipient=? AND id>? LIMIT 1",
            actor.id,
            after
        )
        .fetch_optional(self.pool())
        .await?
        .is_some())
    }
    pub(crate) async fn needs_wake(&self, actor: &Mailbox, after: i64) -> Result<bool> {
        Ok(sqlx::query!(
            "SELECT id FROM wake_events WHERE recipient=? AND id>? LIMIT 1",
            actor.id,
            after
        )
        .fetch_optional(self.pool())
        .await?
        .is_some())
    }
    pub(crate) async fn scan_passive(&self, actor: &Mailbox, through: i64) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("UPDATE runtime_wakes SET scanned=MAX(scanned,?) WHERE recipient=? AND binding_version=? AND NOT EXISTS(SELECT 1 FROM wake_events WHERE recipient=? AND id>scanned AND id<=?)",through,actor.id,actor.binding_version,actor.id,through).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl Store {
    pub(crate) async fn delivery_text(
        &self,
        actor: &Mailbox,
        challenge: Option<&str>,
        after: i64,
    ) -> Result<String> {
        Self::notification_text(
            actor,
            challenge,
            self.latest_changes_since(actor, after).await?,
            6000,
        )
    }

    pub(crate) async fn herdr_notification_text(
        &self,
        actor: &Mailbox,
        challenge: Option<&str>,
    ) -> Result<String> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let events = sqlx::query_as!(Notification,"SELECT id,kind AS 'kind: EventKind',subject,version FROM herdr_wake_events WHERE recipient=? ORDER BY id DESC LIMIT 6",actor.id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Self::notification_text(actor, challenge, events, 480)
    }

    fn notification_text(
        actor: &Mailbox,
        challenge: Option<&str>,
        events: Vec<Notification>,
        limit: usize,
    ) -> Result<String> {
        let mut visible = events
            .iter()
            .rev()
            .take(5)
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let mut more = events.len() > visible.len();
        loop {
            let changes = crate::watch::Changes::collect(
                visible
                    .iter()
                    .map(|event| (event.kind, event.subject.clone(), event.version)),
            );
            let payload = serde_json::json!({"new_mail":changes.new_mail,"mail_updates":changes.mail_updates,"tasks":changes.tasks,"more":more});
            let mut text = if limit <= 480 {
                payload.to_string()
            } else {
                format!(
                    "Agent Mail changes (IDs only). Fetch with task show or mail show; Stop only assignments that are closed or reassigned. Group: {}.\n{}",
                    actor.group_name, payload
                )
            };
            if let Some(nonce) = challenge {
                text.push('\n');
                text.push_str(&crate::verification::challenge(actor, nonce));
            }
            if text.len() <= limit {
                return Ok(text);
            }
            ensure!(
                !visible.is_empty(),
                "compact notification exceeds {limit} bytes"
            );
            visible.remove(0);
            more = true;
        }
    }
}
