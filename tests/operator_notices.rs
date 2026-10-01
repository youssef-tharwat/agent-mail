//! Actual shared dispatcher controls; require the owner's real schema21--25.
mod support;
use agent_mail::{
    followup::{self, Mode, Policy, PolicyPatch},
    store::{Publish, Store},
};
use anyhow::Result;
use std::time::Duration;

async fn fixture() -> Result<(tempfile::TempDir, Store, i64)> {
    let root = tempfile::tempdir()?;
    let store = Store::open(root.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "worker", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let now = agent_mail::now()?;
    store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Enabled,
                interval_seconds: 60,
                max_seconds: 240,
                notifier: None,
            },
            now,
        )
        .await?;
    store
        .publish(
            &writer,
            Publish {
                recipients: vec!["worker".into()],
                key: "source".into(),
                summary: "Decision needed".into(),
                body: "Original unresolved source".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            now,
        )
        .await?;
    followup::reconcile(&store, now + 241).await?;
    Ok((root, store, now + 542))
}

async fn route(store: &Store, args: Vec<String>, now: i64) -> Result<()> {
    store
        .patch_followups(
            "g",
            &PolicyPatch {
                notifier: Some(Some(args)),
                ..Default::default()
            },
            now,
        )
        .await?;
    Ok(())
}

async fn wait_file(path: &std::path::Path) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn physical_old_route_acceptance_cannot_advance_repaired_watermark_or_reexpose() -> Result<()>
{
    let (root, store, now) = fixture().await?;
    let exposed = root.path().join("exposed");
    let release = root.path().join("release");
    let replacement = root.path().join("replacement");
    route(
        &store,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "cat >/dev/null; : >\"$1\"; while [ ! -f \"$2\" ]; do sleep 0.01; done; exit 0".into(),
            "control".into(),
            exposed.display().to_string(),
            release.display().to_string(),
        ],
        now,
    )
    .await?;
    let sender = store.clone();
    let sending = tokio::spawn(async move { followup::notify_operators(&sender, now).await });
    let barrier = wait_file(&exposed).await;
    if let Err(error) = barrier {
        std::fs::write(&release, b"release")?;
        sending.await??;
        return Err(error);
    }
    let original = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(original.exposures, 1);
    assert_eq!(original.state, "exposed");
    route(
        &store,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            ": >\"$1\"".into(),
            "replacement".into(),
            replacement.display().to_string(),
        ],
        now + 1,
    )
    .await?;
    std::fs::write(&release, b"release")?;
    sending.await??;
    let view = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(view.route_generation, original.route_generation + 1);
    assert_eq!(
        view.accepted_revision, 0,
        "old physical result must not advance current watermark"
    );
    assert_eq!(view.accepted_generation, None);
    assert_eq!(view.state, "uncertain");
    assert!(view.outstanding_batch.is_some());
    assert!(view.unresolved);
    followup::notify_operators(&store, now + 1000).await?;
    assert!(
        !replacement.exists(),
        "unknown old sender must block the repaired route"
    );
    let status = store.followup_status(Some("g"), now + 1000).await?;
    assert_eq!(
        status["operator_notifications"][0]["transport_accepted_current"],
        false
    );
    assert_ne!(status["operator_notifications"][0]["state"], "accepted");
    let pool = support::pool(&store).await?;
    let spent: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
        .fetch_one(&pool)
        .await?;
    assert_eq!(spent, 1);
    let accepted_old: bool = sqlx::query_scalar("SELECT json_extract(payload,'$.transport_accepted') FROM operator_notice_events WHERE kind='transport_result' ORDER BY id DESC LIMIT 1").fetch_one(&pool).await?;
    assert!(
        accepted_old,
        "preserve old acceptance evidence even while fencing its current projection"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn parent_acceptance_retains_unknown_descendant_hold_and_original_responsibility()
-> Result<()> {
    let (root, store, now) = fixture().await?;
    let descendant = root.path().join("descendant-finished");
    route(
        &store,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "cat >/dev/null; (sleep 1; : >\"$1\") & exit 0".into(),
            "control".into(),
            descendant.display().to_string(),
        ],
        now,
    )
    .await?;
    followup::notify_operators(&store, now).await?;
    let accepted = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(accepted.accepted_revision, accepted.revision);
    assert_eq!(
        accepted.accepted_generation,
        Some(accepted.route_generation)
    );
    assert_eq!(accepted.state, "uncertain");
    assert!(accepted.outstanding_batch.is_some());
    assert!(accepted.unresolved);
    wait_file(&descendant).await?;
    followup::notify_operators(&store, now + 1000).await?;
    let held = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(
        held.exposures, 1,
        "a test-observed child exit is not an API closure receipt"
    );
    let status = store.followup_status(Some("g"), now + 1000).await?;
    assert_eq!(
        status["operator_notifications"][0]["transport_accepted_current"],
        true
    );
    assert_eq!(status["totals"]["escalated"], 1);
    let worker = store.mailbox("g", "worker").await?;
    assert_eq!(store.inbox(&worker, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn actual_spawn_failure_budget_survives_identical_config_and_route_history() -> Result<()> {
    let (root, store, now) = fixture().await?;
    let missing = root.path().join("missing-executable").display().to_string();
    let args = vec![missing];
    route(&store, args.clone(), now).await?;
    let mut original_generation = None;
    for attempt in 0..3 {
        let at = now + attempt * 300;
        followup::notify_operators(&store, at).await?;
        let view = store.operator_notices("g", 0, 100).await?.remove(0);
        original_generation.get_or_insert(view.route_generation);
        assert_eq!(view.exposures, attempt + 1);
        assert_eq!(
            view.state, "failed",
            "spawn failed before any child existed"
        );
        route(&store, args.clone(), at + 1).await?;
        let unchanged = store.operator_notices("g", 0, 100).await?.remove(0);
        assert_eq!(unchanged.route_generation, original_generation.unwrap());
        assert_eq!(unchanged.next_attempt, view.next_attempt);
        assert_eq!(unchanged.exposures, view.exposures);
    }
    followup::notify_operators(&store, now + 1000).await?;
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 3);
    route(
        &store,
        vec![root.path().join("another-missing").display().to_string()],
        now + 1001,
    )
    .await?;
    followup::notify_operators(&store, now + 1001).await?;
    let repaired = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(repaired.route_generation, original_generation.unwrap() + 1);
    assert_eq!(repaired.exposures, 1);
    let pool = support::pool(&store).await?;
    let total: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
        .fetch_one(&pool)
        .await?;
    assert_eq!(total, 4, "repair retains every old generation's spending");
    let legacy: i64 =
        sqlx::query_scalar("SELECT sum(operator_attempts) FROM attention_occurrences")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        legacy, 0,
        "new dispatcher must not write a second legacy counter"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn saved_observe_and_pause_block_dispatch_without_spending() -> Result<()> {
    let (root, store, now) = fixture().await?;
    route(
        &store,
        vec![root.path().join("missing").display().to_string()],
        now,
    )
    .await?;
    store
        .patch_followups(
            "g",
            &PolicyPatch {
                mode: Some(Mode::Observe),
                ..Default::default()
            },
            now,
        )
        .await?;
    followup::notify_operators(&store, now).await?;
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 0);
    store
        .patch_followups(
            "g",
            &PolicyPatch {
                mode: Some(Mode::Enabled),
                ..Default::default()
            },
            now,
        )
        .await?;
    let pool = support::pool(&store).await?;
    sqlx::query("UPDATE groups SET paused=1 WHERE name='g'")
        .execute(&pool)
        .await?;
    followup::notify_operators(&store, now + 1000).await?;
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 0);
    assert_eq!(
        store.followup_status(Some("g"), now + 1000).await?["totals"]["escalated"],
        1
    );
    sqlx::query("UPDATE groups SET paused=0 WHERE name='g'")
        .execute(&pool)
        .await?;
    followup::notify_operators(&store, now + 1000).await?;
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 1);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn timed_out_parent_is_bounded_and_never_reexposed() -> Result<()> {
    let (_root, store, now) = fixture().await?;
    route(
        &store,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "cat >/dev/null; exec sleep 20".into(),
        ],
        now,
    )
    .await?;
    let started = tokio::time::Instant::now();
    followup::notify_operators(&store, now).await?;
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "five-second transport bound plus database bookkeeping"
    );
    let held = store.operator_notices("g", 0, 100).await?.remove(0);
    assert_eq!(held.state, "uncertain");
    assert_eq!(held.accepted_revision, 0);
    assert!(held.outstanding_batch.is_some());
    followup::notify_operators(&store, now + 1000).await?;
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 1);
    Ok(())
}

#[tokio::test]
async fn a_paused_group_does_not_starve_later_dispatch_groups() -> Result<()> {
    let (root, store, now) = fixture().await?;
    let start = now - 542;
    for group in ["a", "g", "z"] {
        if group != "g" {
            store.enroll(group, None).await?;
            store.register(group, "writer", false).await?;
            store.register(group, "worker", false).await?;
        }
        store
            .configure_followups(
                group,
                &Policy {
                    mode: Mode::Enabled,
                    interval_seconds: 60,
                    max_seconds: 240,
                    notifier: Some(vec![root.path().join("missing").display().to_string()]),
                },
                start,
            )
            .await?;
        if group != "g" {
            let writer = store.mailbox(group, "writer").await?;
            store
                .publish(
                    &writer,
                    Publish {
                        recipients: vec!["worker".into()],
                        key: "source".into(),
                        summary: "Decision".into(),
                        body: "Original source".into(),
                        due_after: None,
                        reply_to: None,
                        work_id: None,
                    },
                    start,
                )
                .await?;
        }
    }
    followup::reconcile(&store, start + 241).await?;
    let pool = support::pool(&store).await?;
    sqlx::query("UPDATE groups SET paused=1 WHERE name='a'")
        .execute(&pool)
        .await?;
    for _ in 0..3 {
        followup::notify_operators(&store, now).await?;
    }
    assert_eq!(store.operator_notices("a", 0, 100).await?[0].exposures, 0);
    assert_eq!(store.operator_notices("g", 0, 100).await?[0].exposures, 1);
    assert_eq!(store.operator_notices("z", 0, 100).await?[0].exposures, 1);
    pool.close().await;
    Ok(())
}
