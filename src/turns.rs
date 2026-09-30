//! Runtime completion receipts for bounded, versioned offers.
use crate::{
    events::Notification,
    store::{Mailbox, Store},
};
use anyhow::{Result, ensure};
use sqlx::{Row, Sqlite, Transaction};

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct OfferItem {
    pub id: i64,
    pub version: i64,
    pub stage: i64,
    pub task: Option<String>,
    pub message: Option<i64>,
}

/// Capture plan versions in the same read snapshot as the render inputs.
pub(crate) async fn capture_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    events: &[Notification],
) -> Result<Vec<OfferItem>> {
    let mut items = Vec::new();
    for event in events {
        let rows = sqlx::query_as::<_, OfferItem>("SELECT f.id,f.version,f.stage,f.task,f.message FROM active_followups f WHERE f.group_name=? AND EXISTS(SELECT 1 FROM followup_policy WHERE group_name=f.group_name AND mode='enabled') AND ((f.recipient=? AND f.stage=0 AND ((?='mail_pending' AND CAST(f.message AS TEXT)=?) OR (?='work_changed' AND f.task=? AND f.task_version=?))) OR (?='attention_due' AND EXISTS(SELECT 1 FROM active_attention o WHERE o.followup=f.id AND CAST(o.id AS TEXT)=? AND o.recipient=? AND o.plan_version=?)))")
            .bind(&actor.group_name).bind(actor.id).bind(event.kind.as_str()).bind(&event.subject)
            .bind(event.kind.as_str()).bind(&event.subject).bind(event.version)
            .bind(event.kind.as_str()).bind(&event.subject).bind(actor.id).bind(event.version)
            .fetch_all(&mut **tx).await?;
        items.extend(rows);
    }
    items.sort_by_key(|i| (i.id, i.version, i.stage));
    items.dedup_by_key(|i| (i.id, i.version, i.stage));
    Ok(items)
}

/// Never replace a captured version with one read after rendering.
async fn append_items_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    items: &[OfferItem],
) -> Result<()> {
    for item in items {
        sqlx::query("INSERT OR IGNORE INTO turn_offer_items(offer,followup,plan_version,stage) SELECT ?,f.id,f.version,f.stage FROM active_followups f WHERE f.id=? AND f.version=? AND f.stage=? AND f.group_name=? AND EXISTS(SELECT 1 FROM followup_policy WHERE group_name=f.group_name AND mode='enabled') AND EXISTS(SELECT 1 FROM turn_offers WHERE id=? AND recipient=? AND binding_version=? AND state='offered')")
            .bind(id).bind(item.id).bind(item.version).bind(item.stage).bind(&actor.group_name)
            .bind(id).bind(actor.id).bind(actor.binding_version).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(crate) struct NativeSnapshot {
    pub text: String,
    pub nonce: Option<String>,
}

pub(crate) async fn native_snapshot_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    session: &str,
) -> Result<Option<NativeSnapshot>> {
    let row = sqlx::query("SELECT recipient,binding_version,runtime,session,native_payload,native_nonce FROM turn_offers WHERE id=?")
        .bind(id).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    ensure!(
        row.get::<i64, _>("recipient") == actor.id
            && row.get::<i64, _>("binding_version") == actor.binding_version
            && row.get::<String, _>("runtime") == "codex"
            && row.get::<String, _>("session") == session,
        "native offer identity mismatch"
    );
    let text = row
        .get::<Option<String>, _>("native_payload")
        .ok_or_else(|| {
            anyhow::anyhow!("native offer has no immutable payload; require a new input ID")
        })?;
    Ok(Some(NativeSnapshot {
        text,
        nonce: row.get("native_nonce"),
    }))
}

pub(crate) async fn reserve_native_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    session: &str,
    delivery: &crate::events::Delivery,
    nonce: Option<&str>,
    time: i64,
) -> Result<NativeSnapshot> {
    if let Some(snapshot) = native_snapshot_tx(tx, actor, id, session).await? {
        return Ok(snapshot);
    }
    ensure!(
        delivery.text.len() <= 6000,
        "native offer exceeds byte budget"
    );
    sqlx::query("INSERT INTO turn_offers(id,recipient,binding_version,runtime,session,created,native_payload,native_nonce) VALUES(?,?,?,'codex',?,?,?,?)")
        .bind(id).bind(actor.id).bind(actor.binding_version).bind(session).bind(time).bind(&delivery.text).bind(nonce).execute(&mut **tx).await?;
    append_items_tx(tx, actor, id, &delivery.items).await?;
    Ok(NativeSnapshot {
        text: delivery.text.clone(),
        nonce: nonce.map(str::to_owned),
    })
}

pub(crate) async fn abandon_hooks_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
) -> Result<()> {
    sqlx::query("UPDATE turn_offers SET state='abandoned' WHERE recipient=? AND binding_version=? AND runtime='hook' AND state='offered'")
        .bind(actor.id).bind(actor.binding_version).execute(&mut **tx).await?;
    Ok(())
}

pub(crate) async fn begin_hook_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    session: &str,
    reset: bool,
    time: i64,
) -> Result<String> {
    if reset {
        abandon_hooks_tx(tx, actor).await?;
    }
    let existing: Option<String> = sqlx::query_scalar("SELECT id FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='hook' AND session=? AND state='offered' ORDER BY created DESC LIMIT 1")
        .bind(actor.id).bind(actor.binding_version).bind(session).fetch_optional(&mut **tx).await?;
    if let Some(id) = existing {
        return Ok(id);
    }
    let id = uuid::Uuid::new_v4().to_string();
    // Input identity also fences retrieval in observation mode. Capturing and
    // appending attention items remains gated by the follow-through policy.
    sqlx::query("INSERT INTO turn_offers(id,recipient,binding_version,runtime,session,created) VALUES(?,?,?,'hook',?,?)")
        .bind(&id).bind(actor.id).bind(actor.binding_version).bind(session).bind(time).execute(&mut **tx).await?;
    Ok(id)
}

pub(crate) async fn append_hook_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: &str,
    session: &str,
    items: &[OfferItem],
) -> Result<bool> {
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM turn_offers WHERE id=? AND recipient=? AND binding_version=? AND runtime='hook' AND session=? AND state='offered')")
        .bind(id).bind(actor.id).bind(actor.binding_version).bind(session).fetch_one(&mut **tx).await?;
    if current {
        append_items_tx(tx, actor, id, items).await?;
    }
    Ok(current)
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

pub(crate) async fn finish_hook_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    session: &str,
    success: bool,
    time: i64,
) -> Result<()> {
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM turn_offers WHERE recipient=? AND binding_version=? AND runtime='hook' AND session=? AND state='offered'")
        .bind(actor.id).bind(actor.binding_version).bind(session).fetch_all(&mut **tx).await?;
    for id in ids {
        if success {
            complete_tx(tx, actor, &id, session, &id, time).await?;
        } else {
            sqlx::query("UPDATE turn_offers SET state='abandoned' WHERE id=?")
                .bind(id)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

impl Store {
    pub(crate) async fn native_snapshot(
        &self,
        actor: &Mailbox,
        id: &str,
        session: &str,
    ) -> Result<Option<NativeSnapshot>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let snapshot = native_snapshot_tx(&mut tx, actor, id, session).await?;
        tx.commit().await?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        followup::{Checkpoint, Source},
        states::{HookEvent, NativeRuntime, TaskState},
        work::WorkDraft,
    };

    #[tokio::test]
    async fn enabling_policy_cannot_retrofit_an_observation_mode_input() -> Result<()> {
        use crate::followup::{Mode, PolicyPatch};
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "worker", false).await?;
        let actor = store.mailbox("g", "worker").await?;
        let time = crate::now()?;
        store
            .patch_followups(
                "g",
                &PolicyPatch {
                    mode: Some(Mode::Observe),
                    ..Default::default()
                },
                time,
            )
            .await?;
        store
            .work_create(
                &actor,
                WorkDraft {
                    id: "work".into(),
                    scope: "Inspect".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Inspect".into(),
                    deadline: None,
                    evidence: vec![],
                },
                time,
            )
            .await?;
        let observed = store.delivery(&actor, None, 0).await?;
        assert!(observed.items.is_empty());
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        reserve_native_tx(&mut tx, &actor, "native", "session", &observed, None, time).await?;
        tx.commit().await?;
        store
            .patch_followups(
                "g",
                &PolicyPatch {
                    mode: Some(Mode::Enabled),
                    ..Default::default()
                },
                time + 1,
            )
            .await?;
        let enabled = store.delivery(&actor, None, 0).await?;
        assert_eq!(enabled.items.len(), 1);
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        reserve_native_tx(
            &mut tx,
            &actor,
            "native",
            "session",
            &enabled,
            None,
            time + 2,
        )
        .await?;
        complete_tx(
            &mut tx,
            &actor,
            "native",
            "session",
            "original-turn",
            time + 3,
        )
        .await?;
        tx.commit().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM turn_offer_items")
                .fetch_one(store.pool())
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT stage FROM followups WHERE task='work'")
                .fetch_one(store.pool())
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn rendered_versions_cannot_be_replaced_during_finalization() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "worker", false).await?;
        let actor = store.mailbox("g", "worker").await?;
        let time = crate::now()?;
        store
            .work_create(
                &actor,
                WorkDraft {
                    id: "work".into(),
                    scope: "Inspect".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Inspect".into(),
                    deadline: None,
                    evidence: vec![],
                },
                time,
            )
            .await?;
        let rendered = store.delivery(&actor, Some("original-nonce"), 0).await?;
        assert_eq!(rendered.items.len(), 1);
        store
            .checkpoint(
                &actor,
                Source::Task {
                    id: "work".into(),
                    version: 1,
                },
                "new-plan",
                Checkpoint {
                    version: 0,
                    next_step: "Inspect new evidence".into(),
                    next_check_at: time + 90,
                    waiting: None,
                    evidence: vec![],
                    extend_until: None,
                    reason: None,
                },
                time + 1,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        let token = begin_hook_tx(&mut tx, &actor, "session", true, time).await?;
        assert!(append_hook_tx(&mut tx, &actor, &token, "session", &rendered.items).await?);
        reserve_native_tx(
            &mut tx,
            &actor,
            "native",
            "session",
            &rendered,
            Some("original-nonce"),
            time,
        )
        .await?;
        tx.commit().await?;
        let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM turn_offer_items")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(items, 0, "new plan cannot be adopted onto old render input");
        let later = store.delivery(&actor, Some("later-nonce"), 0).await?;
        assert_eq!(later.items[0].version, 1);
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        let retry = reserve_native_tx(
            &mut tx,
            &actor,
            "native",
            "session",
            &later,
            Some("later-nonce"),
            time + 2,
        )
        .await?;
        tx.commit().await?;
        assert_eq!(retry.text, rendered.text);
        assert_eq!(retry.nonce.as_deref(), Some("original-nonce"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM turn_offer_items")
                .fetch_one(store.pool())
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn launch_replacement_serializes_with_offer_finalization() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "worker", false).await?;
        let actor = store.mailbox("g", "worker").await?;
        let time = crate::now()?;
        store
            .begin_launch(&actor, "A", NativeRuntime::Codex)
            .await?;
        store
            .observe_hook(&actor, "A", "session", HookEvent::SessionStart, time)
            .await?;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        let token = begin_hook_tx(&mut tx, &actor, "session", true, time).await?;
        assert!(crate::readiness::hook_current_tx(&mut tx, &actor, Some("A"), "session").await?);
        let replacement = {
            let store = store.clone();
            let actor = actor.clone();
            tokio::spawn(async move { store.begin_launch(&actor, "B", NativeRuntime::Codex).await })
        };
        assert!(append_hook_tx(&mut tx, &actor, &token, "session", &[]).await?);
        tx.commit().await?;
        replacement.await??;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        assert!(!crate::readiness::hook_current_tx(&mut tx, &actor, Some("A"), "session").await?);
        assert!(!append_hook_tx(&mut tx, &actor, &token, "session", &[]).await?);
        tx.commit().await?;
        let state: String = sqlx::query_scalar("SELECT state FROM turn_offers WHERE id=?")
            .bind(token)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(state, "abandoned");
        Ok(())
    }
}
