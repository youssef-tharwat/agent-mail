//! Current attention and shared, generation-scoped delivery reservations.
//!
//! Audit replay is independent. A claimed batch is not a receipt; only explicit
//! ingestion or retrieval stops its hints. Business dispositions remain separate.
use crate::{
    names::{DeliveryConsumer, GroupName, ParticipantName},
    states::{AttentionReason, EventKind},
    store::{Mailbox, Store},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction};

const PAGE: usize = 5;
const LEASE_SECONDS: i64 = 300;

/// An outstanding reason addressed to the current participant binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow)]
pub struct Item {
    /// Durable event identity, scoped by the batch's binding generation.
    pub event: i64,
    /// Domain record category.
    pub kind: EventKind,
    /// Authorized source identifier; fetch details through the ordinary API.
    pub subject: String,
    /// Exact source revision or attention occurrence version.
    pub revision: i64,
    /// Why the recipient should reconsider this source now.
    pub reason: AttentionReason,
}
/// A bounded snapshot of outstanding attention; reading it records no receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    /// Coordination group of the authenticated recipient.
    pub group: GroupName,
    /// Participant addressed by this snapshot.
    pub participant: ParticipantName,
    /// Current binding generation.
    pub binding_version: i64,
    /// Priority cancellations followed by a fair page of other reasons.
    pub items: Vec<Item>,
    /// Further outstanding reasons remain available after handling this page.
    pub more: bool,
}
/// A shared reservation requiring confirmed ingestion before acknowledgement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    /// Opaque receipt identity; never a business decision.
    pub token: String,
    /// Consumer owning this batch until expiry or acknowledgement.
    pub consumer: DeliveryConsumer,
    /// Lease expiry in Unix seconds.
    pub expires: i64,
    /// Precisely the records represented by this delivery.
    #[serde(flatten)]
    pub attention: Snapshot,
}

struct ReservationStatus {
    token: String,
    observed: bool,
    held: bool,
}

async fn reservation_status(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    snapshot: &Snapshot,
    time: i64,
) -> Result<Option<ReservationStatus>> {
    let old = sqlx::query!(
        "SELECT binding_version,token,expires,items FROM attention_dispatch WHERE recipient=?",
        actor.id
    )
    .fetch_optional(&mut **tx)
    .await?;
    let Some(old) = old else { return Ok(None) };
    let saved: Snapshot = serde_json::from_str(&old.items)?;
    let valid =
        old.binding_version == actor.binding_version && valid_items(tx, actor, &saved).await?;
    let mut all_seen = true;
    for item in &saved.items {
        all_seen &= observed(tx, actor, item.event).await?;
    }
    let cancellation = snapshot
        .items
        .iter()
        .any(|item| item.reason == AttentionReason::StopWork && !saved.items.contains(item));
    Ok(Some(ReservationStatus {
        token: old.token,
        observed: valid && all_seen,
        held: valid && !all_seen && !cancellation && old.expires > time,
    }))
}

async fn current(tx: &mut Transaction<'_, Sqlite>, actor: &Mailbox) -> Result<Snapshot> {
    let mut items = select_items(tx, actor, None).await?;
    let more = items.len() > PAGE;
    items.truncate(PAGE);
    Ok(Snapshot {
        group: actor.group_name.parse()?,
        participant: actor.name.parse()?,
        binding_version: actor.binding_version,
        items,
        more,
    })
}

async fn source_current(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    item: &Item,
) -> Result<bool> {
    if item.kind == EventKind::AttentionDue && item.reason != AttentionReason::ReviewDue {
        return crate::followup::occurrence_current(tx, actor, &item.subject).await;
    }
    Ok(true)
}

async fn select_items(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    time: Option<i64>,
) -> Result<Vec<Item>> {
    let mut items = Vec::new();
    let mut priority = 2;
    let mut after = 0;
    loop {
        let rows=sqlx::query_as::<_,Item>("SELECT e.id AS event,e.kind,e.subject,e.version AS revision,e.reason FROM attention_events e LEFT JOIN attention_attempts a ON a.recipient=e.recipient AND a.binding_version=? AND a.event=e.id WHERE e.recipient=? AND (? OR a.event IS NULL OR (a.attempts<3 AND a.next_attempt<=?)) AND (e.cancellation<? OR (e.cancellation=? AND e.id>?)) ORDER BY e.cancellation DESC,e.id LIMIT 32")
            .bind(actor.binding_version).bind(actor.id).bind(time.is_none()).bind(time.unwrap_or_default()).bind(priority).bind(priority).bind(after).fetch_all(&mut **tx).await?;
        let last_page = rows.len() < 32;
        for item in rows {
            priority = i64::from(item.reason == AttentionReason::StopWork);
            after = item.event;
            if source_current(tx, actor, &item).await? {
                items.push(item);
            }
            if items.len() > PAGE {
                return Ok(items);
            }
        }
        if last_page {
            return Ok(items);
        }
    }
}

pub(crate) async fn has_delivery_candidates(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    time: i64,
) -> Result<bool> {
    Ok(!select_items(tx, actor, Some(time)).await?.is_empty())
}

async fn observed(tx: &mut Transaction<'_, Sqlite>, actor: &Mailbox, event: i64) -> Result<bool> {
    Ok(sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM event_receipts WHERE recipient=? AND binding_version=? AND event=?)",actor.id,actor.binding_version,event).fetch_one(&mut **tx).await? != 0)
}

async fn valid_items(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    snapshot: &Snapshot,
) -> Result<bool> {
    if snapshot.binding_version != actor.binding_version
        || snapshot.group.as_str() != actor.group_name
        || snapshot.participant.as_str() != actor.name
    {
        return Ok(false);
    }
    for item in &snapshot.items {
        if observed(tx, actor, item.event).await? {
            continue;
        }
        let exists=sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM attention_events WHERE recipient=? AND id=? AND version=?)",actor.id,item.event,item.revision).fetch_one(&mut **tx).await? != 0;
        if !exists || !source_current(tx, actor, item).await? {
            return Ok(false);
        }
    }
    Ok(!snapshot.items.is_empty())
}

impl Store {
    /// Inspect outstanding attention without claiming delivery or settling work.
    /// # Errors
    /// The binding is stale or persistence fails.
    pub async fn attention_snapshot(&self, actor: &Mailbox) -> Result<Snapshot> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let snapshot = current(&mut tx, actor).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    pub(crate) async fn attention_candidates(
        &self,
        actor: &Mailbox,
        time: i64,
    ) -> Result<Vec<Item>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let items = select_items(&mut tx, actor, Some(time)).await?;
        tx.commit().await?;
        Ok(items)
    }

    pub(crate) async fn claimable_attention_candidates(
        &self,
        actor: &Mailbox,
        time: i64,
    ) -> Result<Vec<Item>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let snapshot = current(&mut tx, actor).await?;
        let held = reservation_status(&mut tx, actor, &snapshot, time)
            .await?
            .is_some_and(|reservation| reservation.held);
        let items = if held {
            Vec::new()
        } else {
            select_items(&mut tx, actor, Some(time)).await?
        };
        tx.commit().await?;
        Ok(items)
    }

    /// Reserve one attention batch across competing runtime delivery paths.
    /// Existing reservations wait for bounded expiry, including after a restart.
    /// A newly valid cancellation supersedes an ordinary outstanding batch.
    /// # Errors
    /// Identity, consumer name, clock arithmetic or persistence is invalid.
    pub async fn claim_attention(
        &self,
        actor: &Mailbox,
        consumer: DeliveryConsumer,
        time: i64,
    ) -> Result<Option<Batch>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let policy = sqlx::query!(
            "SELECT paused,auto_prompt FROM groups WHERE name=?",
            actor.group_name
        )
        .fetch_one(&mut *tx)
        .await?;
        let runtime_held = sqlx::query_scalar!("SELECT enabled=0 AS 'held!: bool' FROM runtime_policy WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).fetch_optional(&mut *tx).await?.unwrap_or(false);
        if policy.paused != 0
            || runtime_held
            || (consumer == DeliveryConsumer::Herdr
                && !Self::herdr_policy_tx(&mut tx, actor).await?)
        {
            tx.commit().await?;
            return Ok(None);
        }
        let snapshot = current(&mut tx, actor).await?;
        if let Some(reservation) = reservation_status(&mut tx, actor, &snapshot, time).await? {
            if reservation.observed {
                sqlx::query!("INSERT OR IGNORE INTO attention_batch_receipts(token,recipient,binding_version) VALUES(?,?,?)",reservation.token,actor.id,actor.binding_version).execute(&mut *tx).await?;
            }
            if reservation.held {
                tx.commit().await?;
                return Ok(None);
            }
        }
        // Budgets belong to individual reasons. Skip exhausted or cooling-down
        // reasons so one lost receipt cannot starve later actionable sources.
        let mut items = select_items(&mut tx, actor, Some(time)).await?;
        let page = if consumer == DeliveryConsumer::Herdr {
            1
        } else {
            PAGE
        };
        let more = snapshot.more || items.len() > page;
        items.truncate(page);
        if items.is_empty() {
            sqlx::query!("DELETE FROM attention_dispatch WHERE recipient=?", actor.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(None);
        }
        let token = uuid::Uuid::new_v4().to_string();
        let expires = time
            .checked_add(LEASE_SECONDS)
            .ok_or_else(|| anyhow::anyhow!("clock overflow"))?;
        let snapshot = Snapshot {
            items,
            more,
            ..snapshot
        };
        let mut attempts = 1;
        for item in &snapshot.items {
            let count = sqlx::query_scalar!("INSERT INTO attention_attempts(recipient,binding_version,event,attempts,next_attempt) VALUES(?,?,?,1,?) ON CONFLICT(recipient,binding_version,event) DO UPDATE SET attempts=attempts+1,next_attempt=excluded.next_attempt RETURNING attempts",actor.id,actor.binding_version,item.event,expires).fetch_one(&mut *tx).await?;
            attempts = attempts.max(count);
        }
        let items = serde_json::to_string(&snapshot)?;
        if consumer == DeliveryConsumer::Herdr {
            let event = snapshot
                .items
                .iter()
                .map(|item| item.event)
                .max()
                .ok_or_else(|| anyhow::anyhow!("empty attention reservation"))?;
            sqlx::query!("UPDATE mailboxes SET attempts=?,next_wake=?,wake_attempted=? WHERE id=? AND binding_version=?",attempts,expires,event,actor.id,actor.binding_version).execute(&mut *tx).await?;
        }
        let consumer_text = consumer.to_string();
        sqlx::query!("INSERT INTO attention_dispatch(recipient,binding_version,token,consumer,expires,items,attempts) VALUES(?,?,?,?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,token=excluded.token,consumer=excluded.consumer,expires=excluded.expires,items=excluded.items,attempts=excluded.attempts",actor.id,actor.binding_version,token,consumer_text,expires,items,attempts).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(Batch {
            token,
            consumer,
            expires,
            attention: snapshot,
        }))
    }

    /// Confirm ingestion of exactly one claimed batch for the current binding.
    /// Omitted page records and business dispositions remain unchanged.
    /// # Errors
    /// The token is absent, replaced, stale, or persistence fails.
    pub async fn acknowledge_attention(&self, actor: &Mailbox, token: &str) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        Self::acknowledge_attention_tx(&mut tx, actor, token).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(())
    }

    pub(crate) async fn acknowledge_attention_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        token: &str,
    ) -> Result<()> {
        if sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM attention_batch_receipts WHERE token=? AND recipient=? AND binding_version=?)",token,actor.id,actor.binding_version).fetch_one(&mut **tx).await? != 0 {return Ok(());}
        let row=sqlx::query!("SELECT items FROM attention_dispatch WHERE recipient=? AND binding_version=? AND token=?",actor.id,actor.binding_version,token).fetch_optional(&mut **tx).await?.ok_or_else(||anyhow::anyhow!("attention batch is absent or belongs to another generation"))?;
        let snapshot: Snapshot = serde_json::from_str(&row.items)?;
        ensure!(
            valid_items(tx, actor, &snapshot).await?,
            "attention source changed; recover current attention"
        );
        for item in snapshot.items {
            sqlx::query!("INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) SELECT ?,?,id FROM coordination_events WHERE id=? AND recipient=?",actor.id,actor.binding_version,item.event,actor.id).execute(&mut **tx).await?;
        }
        sqlx::query!(
            "DELETE FROM attention_dispatch WHERE recipient=? AND token=?",
            actor.id,
            token
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query!(
            "INSERT INTO attention_batch_receipts(token,recipient,binding_version) VALUES(?,?,?)",
            token,
            actor.id,
            actor.binding_version
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query!("UPDATE runtime_wakes SET attempts=0,next_attempt=0 WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).execute(&mut **tx).await?;
        sqlx::query!(
            "UPDATE mailboxes SET attempts=0,next_wake=0 WHERE id=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Recheck the exact claimed sources while holding the binding lock before I/O.
    pub(crate) async fn validate_attention_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        batch: &Batch,
    ) -> Result<bool> {
        if sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM attention_dispatch WHERE recipient=? AND binding_version=? AND token=?)",actor.id,actor.binding_version,batch.token).fetch_one(&mut **tx).await? == 0 || !valid_items(tx,actor,&batch.attention).await? { return Ok(false); }
        for item in &batch.attention.items {
            if !observed(tx, actor, item.event).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Render a claimed batch for a model without implying task completion.
    /// # Errors
    /// The batch cannot be encoded or exceeds the runtime payload bound.
    pub(crate) fn attention_text(batch: &Batch, needs_ack: bool) -> Result<String> {
        if batch.consumer == DeliveryConsumer::Herdr {
            let item = batch
                .attention
                .items
                .first()
                .ok_or_else(|| anyhow::anyhow!("empty attention reservation"))?;
            let (category, command) = match item.kind {
                EventKind::MailPending | EventKind::MailChanged => ("new_mail", "mail"),
                EventKind::WorkChanged => ("tasks", "task"),
                EventKind::AttentionDue => ("followups", "attention"),
            };
            // Herdr has a small terminal prompt budget. Exactly this one source
            // is reserved; fetching it records its scoped retrieval receipt.
            return Ok(format!(
                "Agent Mail changes: {category} {}; run agent-mail --group {} {command} show {}; act or checkpoint. More: agent-mail attention session.",
                item.subject, batch.attention.group, item.subject
            ));
        }
        let changes = crate::watch::Changes::collect(
            batch
                .attention
                .items
                .iter()
                .map(|i| (i.kind, i.subject.clone(), i.revision)),
        );
        let value = serde_json::json!({"attention":batch.attention.items,"new_mail":changes.new_mail,"mail_updates":changes.mail_updates,"tasks":changes.tasks,"followups":changes.followups,"more":batch.attention.more});
        let receipt = if needs_ack {
            format!(
                " After consuming this batch, run `agent-mail --group={} attention acknowledge {}`.",
                batch.attention.group, batch.token
            )
        } else {
            String::new()
        };
        let text = format!(
            "Agent Mail changes requiring attention: fetch the listed sources; act or checkpoint. Responses need no reply or resolution. Stop only assignments currently closed or reassigned.{receipt}\n{value}"
        );
        ensure!(text.len() <= 5500, "attention batch exceeds runtime limit");
        Ok(text)
    }
}
