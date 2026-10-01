//! Behavioral controls copied unchanged into the frozen base and integrated head.
//! An assertion failure is evidence only after the named remote run records it.
mod support;
use agent_mail::{
    followup::{self, Checkpoint, Mode, Policy, PolicyPatch, Source},
    states::TaskState,
    store::{Publish, Store},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::{Result, ensure};
use sqlx::Row;

async fn fixture() -> Result<(tempfile::TempDir, Store, i64)> {
    let root = tempfile::tempdir()?;
    let store = Store::open(root.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "owner", false).await?;
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
    Ok((root, store, now))
}

#[tokio::test]
async fn identical_business_update_preserves_checkpoint_and_original_retrieval() -> Result<()> {
    let (_root, store, now) = fixture().await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
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
    let before = sqlx::query("SELECT checkpoint,retrieved_at,retrieved_binding,escalate_at FROM followups WHERE task='work'").fetch_one(&pool).await?;
    ensure!(
        before.get::<Option<String>, _>("checkpoint").is_some(),
        "control checkpoint not created"
    );
    ensure!(
        before.get::<Option<i64>, _>("retrieved_at").is_some(),
        "control retrieval not created"
    );
    store
        .update_work(
            &writer,
            "work",
            WorkUpdate {
                version: 1,
                reason: "Repeat identical business fields for audit".into(),
                patch: WorkPatch {
                    next_action: Some("Review changes".into()),
                    evidence: Some(vec![]),
                    ..Default::default()
                },
                resolve_message: None,
            },
            now + 2,
        )
        .await?;
    let after = sqlx::query("SELECT checkpoint,retrieved_at,retrieved_binding,escalate_at FROM followups WHERE task='work'").fetch_one(&pool).await?;
    assert_eq!(
        before.get::<Option<String>, _>("checkpoint"),
        after.get::<Option<String>, _>("checkpoint"),
        "identical business content lost its checkpoint"
    );
    for field in ["retrieved_at", "retrieved_binding", "escalate_at"] {
        assert_eq!(
            before.get::<Option<i64>, _>(field),
            after.get::<Option<i64>, _>(field),
            "changed {field}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn old_notifier_result_cannot_accept_a_repaired_route() -> Result<()> {
    let (root, store, now) = fixture().await?;
    let writer = store.mailbox("g", "writer").await?;
    let marker = root.path().join("exposed");
    let release = root.path().join("release");
    // Test-owned transport with an explicit barrier. No production source patch.
    let script = "cat >/dev/null; : >\"$1\"; while [ ! -f \"$2\" ]; do sleep 0.01; done; exit 0";
    store
        .patch_followups(
            "g",
            &PolicyPatch {
                notifier: Some(Some(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    script.into(),
                    "control".into(),
                    marker.display().to_string(),
                    release.display().to_string(),
                ])),
                ..Default::default()
            },
            now,
        )
        .await?;
    store
        .publish(
            &writer,
            Publish {
                recipients: vec!["owner".into()],
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
    let sender = store.clone();
    let sending = tokio::spawn(async move { followup::notify_operators(&sender, now + 542).await });
    let barrier = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    if barrier.is_err() {
        std::fs::write(&release, b"release")?;
        sending.await??;
        anyhow::bail!("old sender never reached exposure barrier");
    }
    // The old process is still alive. Repair must fence its eventual result.
    store
        .patch_followups(
            "g",
            &PolicyPatch {
                notifier: Some(Some(vec!["/usr/bin/false".into()])),
                ..Default::default()
            },
            now + 543,
        )
        .await?;
    std::fs::write(&release, b"release")?;
    sending.await??;
    let status = store.followup_status(Some("g"), now + 544).await?;
    assert_ne!(
        status["operator_notifications"][0]["state"], "accepted",
        "old route completion marked the repaired route accepted"
    );
    Ok(())
}
