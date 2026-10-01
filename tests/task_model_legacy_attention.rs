//! Legacy business-input equality controls; no native execution witnesses.
mod support;

use agent_mail::{
    followup::{self, Checkpoint, Mode, Policy, Source},
    states::TaskState,
    store::{Mailbox, Store},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;

async fn fixture() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox, i64)> {
    let root = tempfile::tempdir()?;
    let store = Store::open(root.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "owner", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    let now = agent_mail::now()?;
    store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Enabled,
                interval_seconds: 60,
                max_seconds: 240,
                // The shared dispatcher can prove failure before spawn. An
                // arbitrary child's exit leaves descendant/effect uncertainty.
                notifier: Some(vec![
                    root.path().join("missing-notifier").display().to_string(),
                ]),
            },
            now,
        )
        .await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "work".into(),
                scope: "Review changes".into(),
                owner: "owner".into(),
                state: TaskState::Active,
                next_action: "Review changes".into(),
                deadline: None,
                evidence: vec![],
            },
            now,
        )
        .await?;
    Ok((root, store, writer, owner, now))
}

fn identical_update(version: i64) -> WorkUpdate {
    WorkUpdate {
        version,
        reason: "Record the same business input for audit".into(),
        patch: WorkPatch {
            next_action: Some("Review changes".into()),
            evidence: Some(vec![]),
            ..Default::default()
        },
        resolve_message: None,
    }
}

#[tokio::test]
async fn legacy_noop_keeps_escalation_current_and_operator_budget_spent() -> Result<()> {
    let (_root, store, writer, _owner, now) = fixture().await?;
    let pool = support::pool(&store).await?;
    followup::reconcile(&store, now + 241).await?;
    for attempt in 0..3 {
        followup::notify_operators(&store, now + 542 + 300 * attempt).await?;
    }
    let before: (i64, i64, i64, i64, String) = sqlx::query_as(
        "SELECT id,followup,plan_version,operator_attempts,operator_state FROM active_attention WHERE stage=3",
    ).fetch_one(&pool).await?;
    let spending = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!((spending.exposures, spending.state.as_str()), (3, "failed"));
    let attention_before: String = sqlx::query_scalar(
        "SELECT json_object('id',id,'plan',plan_version,'stage',stage,'retrieved',retrieved_at,'after',operator_after,'attempts',operator_attempts,'next',operator_next,'state',operator_state,'detail',operator_detail) FROM attention_occurrences WHERE id=?",
    ).bind(before.0).fetch_one(&pool).await?;
    let boundary: (i64, i64) =
        sqlx::query_as("SELECT opened,escalate_at FROM followups WHERE task='work'")
            .fetch_one(&pool)
            .await?;
    store
        .update_work(&writer, "work", identical_update(1), now + 1500)
        .await?;
    let after: (i64, i64, i64, i64, String) = sqlx::query_as(
        "SELECT id,followup,plan_version,operator_attempts,operator_state FROM active_attention WHERE stage=3",
    ).fetch_one(&pool).await?;
    assert_eq!(after, before, "the original escalation must remain active");
    followup::reconcile(&store, now + 2000).await?;
    followup::notify_operators(&store, now + 2000).await?;
    let retained = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(retained.id, spending.id);
    assert_eq!(retained.account, spending.account);
    assert_eq!(retained.source_key, spending.source_key);
    assert_eq!(retained.episode, spending.episode);
    assert_eq!(retained.route_generation, spending.route_generation);
    assert_eq!(retained.exposures, spending.exposures);
    assert_eq!(retained.next_attempt, spending.next_attempt);
    assert_eq!(retained.outstanding_batch, spending.outstanding_batch);
    let attention_after: String = sqlx::query_scalar(
        "SELECT json_object('id',id,'plan',plan_version,'stage',stage,'retrieved',retrieved_at,'after',operator_after,'attempts',operator_attempts,'next',operator_next,'state',operator_state,'detail',operator_detail) FROM attention_occurrences WHERE id=?",
    ).bind(before.0).fetch_one(&pool).await?;
    assert_eq!(attention_after, attention_before);
    let after_boundary: (i64, i64) =
        sqlx::query_as("SELECT opened,escalate_at FROM followups WHERE task='work'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(after_boundary, boundary);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_occurrences WHERE followup=? AND stage=3",
    )
    .bind(before.1)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        count, 1,
        "an audit revision must not reopen operator delivery"
    );
    Ok(())
}

#[tokio::test]
async fn legacy_noop_preserves_audit_cas_retry_and_changed_input_still_invalidates() -> Result<()> {
    let (_root, store, writer, owner, now) = fixture().await?;
    store.work_show(&owner, "work").await?;
    store
        .checkpoint(
            &owner,
            Source::Task {
                id: "work".into(),
                version: 1,
            },
            "planned",
            Checkpoint {
                version: 0,
                next_step: "Review changes".into(),
                next_check_at: now + 90,
                waiting: None,
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            now + 1,
        )
        .await?;
    let pool = support::pool(&store).await?;
    let before: (i64, Option<String>, Option<i64>, Option<i64>, i64) = sqlx::query_as(
        "SELECT version,checkpoint,retrieved_at,retrieved_binding,escalate_at FROM followups WHERE task='work'",
    ).fetch_one(&pool).await?;
    assert!(before.1.is_some() && before.2.is_some() && before.3.is_some());
    let applied = store
        .update_work(&writer, "work", identical_update(1), now + 2)
        .await?;
    assert_eq!(
        applied.version, 2,
        "audit revisions still consume business CAS"
    );
    let replay = store
        .update_work(&writer, "work", identical_update(1), now + 3)
        .await?;
    assert_eq!(
        serde_json::to_value(replay)?,
        serde_json::to_value(applied)?
    );
    let mut conflict = identical_update(1);
    conflict.reason = "Different content under the consumed key".into();
    assert!(
        store
            .update_work(&writer, "work", conflict, now + 4)
            .await
            .is_err()
    );
    let history = store.work_history(&writer, "work").await?;
    assert_eq!(history.iter().filter(|entry| entry.version == 2).count(), 1);
    assert_eq!(
        history
            .iter()
            .find(|entry| entry.version == 2)
            .unwrap()
            .reason,
        "Record the same business input for audit"
    );
    let after: (i64, Option<String>, Option<i64>, Option<i64>, i64) = sqlx::query_as(
        "SELECT version,checkpoint,retrieved_at,retrieved_binding,escalate_at FROM followups WHERE task='work'",
    ).fetch_one(&pool).await?;
    assert_eq!(after, before);
    let wake: i64 = sqlx::query_scalar("SELECT max(wake) FROM coordination_events WHERE kind='work_changed' AND subject='work' AND version=2")
        .fetch_one(&pool).await?;
    assert_eq!(wake, 0);
    let mut changed = identical_update(2);
    changed.patch.next_action = Some("Review a newly supplied input".into());
    store.update_work(&writer, "work", changed, now + 5).await?;
    let changed_attention: (i64, Option<String>, Option<i64>, Option<i64>, i64) = sqlx::query_as(
        "SELECT version,checkpoint,retrieved_at,retrieved_binding,escalate_at FROM followups WHERE task='work'",
    ).fetch_one(&pool).await?;
    assert_eq!(
        changed_attention,
        (before.0 + 1, None, None, None, before.4)
    );
    let wake: i64 = sqlx::query_scalar("SELECT max(wake) FROM coordination_events WHERE kind='work_changed' AND subject='work' AND version=3")
        .fetch_one(&pool).await?;
    assert_eq!(
        wake, 1,
        "genuinely changed input still requires owner attention"
    );
    Ok(())
}
