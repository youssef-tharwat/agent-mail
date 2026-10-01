//! Runtime completion receipts for bounded, versioned offers. A queue receipt is
//! only an offer. Only a correlated successful turn can trigger corrective attention.
use crate::{
    events::Notification,
    store::{Mailbox, Store},
};
use anyhow::Result;
use sqlx::{Row, Sqlite, Transaction};

pub(crate) async fn offer_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    runtime: &str,
    session: &str,
    events: &[Notification],
    time: i64,
) -> Result<()> {
    sqlx::query("INSERT OR IGNORE INTO turn_offers(id,recipient,binding_version,runtime,session,created) SELECT ?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM followup_policy WHERE group_name=? AND mode='enabled')")
        .bind(id).bind(actor.id).bind(actor.binding_version).bind(runtime).bind(session).bind(time).bind(&actor.group_name).execute(&mut **tx).await?;
    for event in events {
        // Historical events, superseded task revisions and hidden/truncated records
        // cannot become obligations of this turn. Attention can address authority.
        sqlx::query("INSERT OR IGNORE INTO turn_offer_items(offer,followup,plan_version,stage) SELECT ?,f.id,f.version,f.stage FROM active_followups f WHERE EXISTS(SELECT 1 FROM turn_offers WHERE id=? AND recipient=? AND binding_version=? AND state='offered') AND f.group_name=? AND ((f.recipient=? AND f.stage=0 AND ((?='mail_pending' AND CAST(f.message AS TEXT)=?) OR (?='work_changed' AND f.task=? AND f.task_version=?))) OR (?='attention_due' AND EXISTS(SELECT 1 FROM active_attention o WHERE o.followup=f.id AND CAST(o.id AS TEXT)=? AND o.recipient=?)))")
            .bind(id).bind(id).bind(actor.id).bind(actor.binding_version).bind(&actor.group_name).bind(actor.id)
            .bind(event.kind.as_str()).bind(&event.subject).bind(event.kind.as_str()).bind(&event.subject).bind(event.version)
            .bind(event.kind.as_str()).bind(&event.subject).bind(actor.id).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(crate) async fn complete_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    session: &str,
    turn: &str,
    time: i64,
) -> Result<()> {
    let changed = sqlx::query("UPDATE turn_offers SET state='completed',turn=?,completed_at=? WHERE id=? AND recipient=? AND binding_version=? AND session=? AND state='offered'")
        .bind(turn).bind(time).bind(id).bind(actor.id).bind(actor.binding_version).bind(session).execute(&mut **tx).await?;
    if changed.rows_affected() == 0 {
        return Ok(());
    }
    let items =
        sqlx::query("SELECT followup,plan_version,stage FROM turn_offer_items WHERE offer=?")
            .bind(id)
            .fetch_all(&mut **tx)
            .await?;
    for item in items {
        crate::followup::turn_completed(
            tx,
            actor,
            item.get("followup"),
            item.get("plan_version"),
            item.get("stage"),
            time,
        )
        .await?;
    }
    Ok(())
}

impl Store {
    pub(crate) async fn hook_offer(
        &self,
        actor: &Mailbox,
        session: &str,
        reset: bool,
        events: &[Notification],
        time: i64,
    ) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if reset {
            // Session restarts/user inputs are boundaries, never invented completions.
            sqlx::query("UPDATE turn_offers SET state='abandoned' WHERE recipient=? AND binding_version=? AND runtime='hook' AND state='offered'")
                .bind(actor.id).bind(actor.binding_version).execute(&mut *tx).await?;
        }
        let existing: Option<String> = sqlx::query_scalar("SELECT id FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='hook' AND session=? AND state='offered' ORDER BY created DESC LIMIT 1")
            .bind(actor.id).bind(actor.binding_version).bind(session).fetch_optional(&mut *tx).await?;
        let id = existing.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        offer_tx(&mut tx, actor, &id, "hook", session, events, time).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn finish_hook(
        &self,
        actor: &Mailbox,
        session: &str,
        success: bool,
        time: i64,
    ) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='hook' AND session=? AND state='offered'")
            .bind(actor.id).bind(actor.binding_version).bind(session).fetch_all(&mut *tx).await?;
        for id in ids {
            if success {
                complete_tx(&mut tx, actor, &id, session, &id, time).await?;
            } else {
                sqlx::query("UPDATE turn_offers SET state='abandoned' WHERE id=?")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }
}
