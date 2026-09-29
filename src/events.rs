//! Durable, recipient-scoped notifications. Receipts are transport facts, not decisions.
use crate::{
    bounded,
    store::{Mailbox, Store},
};
use anyhow::{Result, ensure};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Notification {
    pub id: i64,
    pub kind: String,
    pub subject: String,
    pub version: i64,
}

impl Store {
    /// Replay unacknowledged events for this binding; old sessions cannot hide new work.
    pub async fn notifications(&self, actor: &Mailbox, after: i64) -> Result<Vec<Notification>> {
        ensure!(after >= 0, "event cursor must be nonnegative");
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query_as!(Notification,
            "SELECT e.id,e.kind,e.subject,e.version FROM coordination_events e WHERE e.recipient=? AND e.id>? AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=? AND r.event=e.id) ORDER BY e.id LIMIT 6",
            actor.id, after, actor.binding_version).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// Coalesced current change hints, newest first. Detailed event replay uses notifications.
    pub async fn latest_changes(&self, actor: &Mailbox) -> Result<Vec<Notification>> {
        let mut tx = self.pool.begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query_as!(Notification,
            "SELECT e.id,e.kind,e.subject,e.version FROM coordination_events e WHERE e.recipient=? AND NOT EXISTS(SELECT 1 FROM coordination_events newer WHERE newer.recipient=e.recipient AND newer.kind=e.kind AND newer.subject=e.subject AND newer.id>e.id) ORDER BY e.id DESC LIMIT 6",actor.id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
    }

    pub async fn notification_status(&self) -> Result<serde_json::Value> {
        let rows = sqlx::query!("SELECT b.group_name,b.name,b.binding_version,COUNT(e.id) AS count FROM mailboxes b JOIN coordination_events e ON e.recipient=b.id WHERE b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=b.id AND r.binding_version=b.binding_version AND r.event=e.id) GROUP BY b.id ORDER BY b.id").fetch_all(&self.pool).await?;
        let emissions = sqlx::query!("SELECT b.group_name,b.name,h.attempts,h.next_attempt,h.stop_used FROM hook_emissions h JOIN mailboxes b ON b.id=h.recipient AND b.binding_version=h.binding_version ORDER BY h.next_attempt DESC LIMIT 20").fetch_all(&self.pool).await?;
        Ok(
            serde_json::json!({"unacknowledged":rows.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"binding_version":r.binding_version,"count":r.count})).collect::<Vec<_>>(),"hook_attempts":emissions.into_iter().map(|r|serde_json::json!({"group":r.group_name,"participant":r.name,"attempts":r.attempts,"next_attempt":r.next_attempt,"stop_used":r.stop_used,"delivery_confirmed":false})).collect::<Vec<_>>()}),
        )
    }

    /// Acknowledge one confirmed transport delivery, never a whole unverified cursor range.
    pub async fn acknowledge(&self, actor: &Mailbox, event: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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

    /// Older clients may not emit SessionStart(compact). Mark recovery dirty without
    /// pretending PostCompact stdout is injected into the model's context.
    pub async fn invalidate_hook(&self, actor: &Mailbox, session: &str) -> Result<()> {
        bounded(session, 160, "client session")?;
        ensure!(!session.is_empty(), "hook requires a client session ID");
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("DELETE FROM hook_emissions WHERE recipient=? AND binding_version=? AND client_session=?",actor.id,actor.binding_version,session).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Reserve a bounded hook emission. This is an attempt, never a delivery receipt.
    /// SessionStart always restores state, including after an ambiguous previous emission.
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
        let mut tx = self.pool.begin().await?;
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
        let emit = reset
            || (stop && row.stop_used == 0 && changed)
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
