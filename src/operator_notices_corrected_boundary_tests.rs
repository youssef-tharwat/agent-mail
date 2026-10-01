use super::*;
use crate::{states::TaskState, store::Mailbox, work::WorkDraft};

struct BoundaryFixture {
    root: tempfile::TempDir,
    store: Store,
    writer: Mailbox,
    source: recovery::Obligation,
    case: i64,
    occurrence: i64,
    notice: i64,
    old_due: i64,
    marker: std::path::PathBuf,
}

async fn boundary_fixture(task: bool) -> Result<BoundaryFixture> {
    let (root, store, opened) = fixture(0).await?;
    let writer = store.mailbox("g", "writer").await?;
    let marker = root.path().join("notifier-payload.json");
    store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Enabled,
                interval_seconds: 60,
                max_seconds: 240,
                notifier: Some(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "cat > \"$1\"".into(),
                    "notice-test".into(),
                    marker.to_string_lossy().into_owned(),
                ]),
            },
            opened,
        )
        .await?;
    let source = if task {
        let work = store
            .work_create(
                &writer,
                WorkDraft {
                    id: "boundary-task".into(),
                    scope: "Inspect original evidence".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Inspect original evidence".into(),
                    deadline: None,
                    evidence: vec![],
                },
                opened,
            )
            .await?;
        recovery::Obligation::Task {
            id: work.id,
            version: work.version,
        }
    } else {
        let message = store
            .publish(
                &writer,
                Publish {
                    recipients: vec!["worker".into()],
                    key: "boundary-delivery".into(),
                    summary: "Inspect original evidence".into(),
                    body: "Original unresolved source".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                opened,
            )
            .await?;
        recovery::Obligation::Delivery {
            message,
            recipient: "worker".into(),
        }
    };
    let observed = store.inspect_obligation(&writer, source.clone()).await?;
    let plan = observed.plan.context("original source plan")?;
    followup::reconcile(&store, plan.escalate_at + 1).await?;
    let (occurrence, old_due): (i64, i64) = sqlx::query_as(
        "SELECT id,operator_after FROM attention_occurrences WHERE followup=? AND stage=3",
    )
    .bind(plan.id)
    .fetch_one(store.pool())
    .await?;
    assert!(old_due > plan.escalate_at + 2);
    let case = store
        .recover_expired_obligation(
            &writer,
            "boundary-case",
            source.clone(),
            plan.escalate_at + 2,
        )
        .await?;
    let mut tx = store.pool().begin().await?;
    let notice = project_notice_tx(
        &mut tx,
        "g",
        NoticeSource::AttentionOccurrence(occurrence),
        plan.escalate_at + 2,
    )
    .await?;
    tx.commit().await?;
    Ok(BoundaryFixture {
        root,
        store,
        writer,
        source,
        case: case.id,
        occurrence,
        notice,
        old_due,
        marker,
    })
}

async fn correct_boundary(f: &BoundaryFixture, at: i64, boundary: i64) -> Result<()> {
    let current = f
        .store
        .inspect_obligation(&f.writer, f.source.clone())
        .await?;
    let plan = current.plan.context("current source plan")?;
    let mut tx = f.store.pool().begin().await?;
    let case = recovery::load_case_tx(&mut tx, "g", f.case).await?;
    tx.rollback().await?;
    let result = f
        .store
        .correct_obligation(
            &f.writer,
            recovery::SourceCorrection {
                key: "correct-boundary".into(),
                source: f.source.clone(),
                expected_plan: recovery::ExpectedPlan::Present {
                    id: plan.id,
                    version: plan.version,
                },
                case_versions: std::collections::BTreeMap::from([(case.id, case.version)]),
                reason: "Original authority accepts a future source review".into(),
                evidence: vec!["source:corrected-boundary".into()],
                next_step: "Review the original source at the corrected boundary".into(),
                next_check_at: boundary - 10,
                escalation_at: boundary,
            },
            at,
        )
        .await?;
    assert_eq!(result["plan"]["after"]["stage"], 0);
    assert_eq!(result["plan"]["after"]["escalate_at"], boundary);
    let mut tx = f.store.pool().begin().await?;
    let corrected = recovery::load_case_tx(&mut tx, "g", f.case).await?;
    assert_eq!(corrected.hard_due, boundary);
    assert_eq!(corrected.episode, case.episode);
    assert_eq!(corrected.original_due, case.original_due);
    tx.rollback().await?;
    Ok(())
}

async fn occurrence_audit(f: &BoundaryFixture) -> Result<String> {
    Ok(sqlx::query_scalar("SELECT json_object('id',id,'followup',followup,'plan_version',plan_version,'stage',stage,'reason',reason,'recipient',recipient,'created',created,'operator_after',operator_after) FROM attention_occurrences WHERE id=?")
        .bind(f.occurrence).fetch_one(f.store.pool()).await?)
}

async fn no_early_corrected_alias(task: bool) -> Result<()> {
    let f = boundary_fixture(task).await?;
    let audit = occurrence_audit(&f).await?;
    let before = f.store.operator_notices("g", 0, 100).await?;
    let account = before[0].account;
    let boundary = f.old_due + 1000;
    // Correct after the real stage-three occurrence, before its original grace.
    correct_boundary(&f, f.old_due - 1, boundary).await?;
    dispatch_notices(&f.store, f.old_due).await?;
    assert!(
        !f.marker.exists(),
        "historical attention must not transmit before the corrected boundary"
    );
    let spent: i64 =
        sqlx::query_scalar("SELECT COALESCE(sum(exposures),0) FROM operator_notice_spending")
            .fetch_one(f.store.pool())
            .await?;
    assert_eq!(spent, 0, "correction must prevent early exposure spending");
    let notices = f.store.operator_notices("g", 0, 100).await?;
    assert!(notices.iter().all(|n| n.account == account));
    assert!(notices.iter().all(|n| {
        n.source_snapshot["due_at"]
            .as_i64()
            .is_some_and(|due| due >= boundary)
    }));
    assert_eq!(occurrence_audit(&f).await?, audit);
    // A permanent suppression would fail this real dispatcher/I/O control.
    dispatch_notices(&f.store, boundary).await?;
    let payload: Value = serde_json::from_slice(&std::fs::read(&f.marker)?)?;
    assert_eq!(
        payload["items"].as_array().context("notice items")?.len(),
        1
    );
    let spent: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
        .fetch_one(f.store.pool())
        .await?;
    assert_eq!(spent, 1);
    assert_eq!(occurrence_audit(&f).await?, audit);
    Ok(())
}

#[tokio::test]
async fn corrected_task_boundary_blocks_historical_attention_until_due() -> Result<()> {
    no_early_corrected_alias(true).await
}

#[tokio::test]
async fn corrected_delivery_boundary_blocks_historical_attention_until_due() -> Result<()> {
    no_early_corrected_alias(false).await
}

#[tokio::test]
async fn current_plan_attention_keeps_its_own_operator_grace() -> Result<()> {
    let f = boundary_fixture(false).await?;
    let current = f
        .store
        .inspect_obligation(&f.writer, f.source.clone())
        .await?;
    let plan = current.plan.context("current source plan")?;
    let recovery::Obligation::Delivery { message, .. } = f.source else {
        anyhow::bail!("delivery fixture required");
    };
    let boundary = f.old_due + 10_000;
    f.store
        .checkpoint(
            &f.writer,
            followup::Source::Mail { id: message },
            "current-plan-hold",
            followup::Checkpoint {
                version: plan.version,
                next_step: "Review the external dependency".into(),
                next_check_at: f.old_due + 10,
                waiting: Some(followup::WaitFor::External {
                    responsible: "writer".into(),
                    reason: "Named external evidence is unavailable".into(),
                }),
                evidence: vec![],
                extend_until: Some(boundary),
                reason: Some("Original sender extends this review".into()),
            },
            f.old_due + 1,
        )
        .await?;
    followup::reconcile(&f.store, f.old_due + 11).await?;
    let (occurrence, grace): (i64, i64) = sqlx::query_as(
        "SELECT id,operator_after FROM attention_occurrences WHERE followup=? AND stage=3 AND plan_version=?",
    ).bind(plan.id).bind(plan.version + 1).fetch_one(f.store.pool()).await?;
    assert!(grace < boundary);
    let mut tx = f.store.pool().begin().await?;
    let current = source_tx(
        &mut tx,
        "g",
        NoticeSource::AttentionOccurrence(occurrence),
        grace,
    )
    .await?;
    assert_eq!(
        current.due_at, grace,
        "current owner escalation keeps its own grace"
    );
    let historical = source_tx(
        &mut tx,
        "g",
        NoticeSource::AttentionOccurrence(f.occurrence),
        grace,
    )
    .await?;
    assert_eq!(
        historical.due_at, boundary,
        "obsolete alias follows the corrected boundary"
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn correction_after_reservation_cancels_without_exposure() -> Result<()> {
    for task in [true, false] {
        let f = boundary_fixture(task).await?;
        let audit = occurrence_audit(&f).await?;
        let mut tx = f.store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "reserved", f.old_due)
            .await?
            .context("real reserved batch")?;
        let membership: String = sqlx::query_scalar(
            "SELECT source_snapshot FROM operator_notice_batch_items WHERE batch=?",
        )
        .bind(&batch.id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        correct_boundary(&f, f.old_due + 1, f.old_due + 1000).await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "reserved", f.old_due + 2)
                .await?
                .is_none(),
            "pre-I/O source revalidation must refuse the old reservation"
        );
        tx.commit().await?;
        let (state, exposed): (String, Option<i64>) =
            sqlx::query_as("SELECT state,exposed_at FROM operator_notice_batches WHERE id=?")
                .bind(&batch.id)
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!(state, "cancelled");
        assert_eq!(exposed, None);
        let saved: String = sqlx::query_scalar(
            "SELECT source_snapshot FROM operator_notice_batch_items WHERE batch=?",
        )
        .bind(&batch.id)
        .fetch_one(f.store.pool())
        .await?;
        assert_eq!(saved, membership);
        let spent: i64 = sqlx::query_scalar("SELECT sum(exposures) FROM operator_notice_spending")
            .fetch_one(f.store.pool())
            .await?;
        assert_eq!(spent, 0);
        assert!(!f.marker.exists());
        assert_eq!(occurrence_audit(&f).await?, audit);
    }
    Ok(())
}

#[tokio::test]
async fn correction_preserves_spending_and_inflight_evidence_after_restart() -> Result<()> {
    for uncertain in [false, true] {
        let mut f = boundary_fixture(false).await?;
        let audit = occurrence_audit(&f).await?;
        let mut tx = f.store.pool().begin().await?;
        let batch = reserve_operator_notice_batch_tx(&mut tx, "g", "inflight", f.old_due)
            .await?
            .context("real reserved batch")?;
        expose_operator_notice_batch_tx(&mut tx, "g", &batch.id, "inflight", f.old_due)
            .await?
            .context("real exposure")?;
        if uncertain {
            finish_operator_notice_batch_tx(
                &mut tx,
                "g",
                &batch.id,
                "inflight",
                TransportResult::Uncertain,
                "completion is unknown",
                f.old_due + 1,
            )
            .await?;
        }
        let spending: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT account,generation,exposures,next_at FROM operator_notice_spending",
        )
        .fetch_one(&mut *tx)
        .await?;
        let membership: String = sqlx::query_scalar(
            "SELECT source_snapshot FROM operator_notice_batch_items WHERE batch=?",
        )
        .bind(&batch.id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        let boundary = f.old_due + 1000;
        correct_boundary(&f, f.old_due + 2, boundary).await?;
        let mut tx = f.store.pool().begin().await?;
        project_notice_tx(
            &mut tx,
            "g",
            NoticeSource::AttentionOccurrence(f.occurrence),
            f.old_due + 3,
        )
        .await?;
        let state: String =
            sqlx::query_scalar("SELECT state FROM operator_notice_batches WHERE id=?")
                .bind(&batch.id)
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(state, if uncertain { "uncertain" } else { "exposed" });
        tx.commit().await?;
        f.store.close().await;
        f.store = Store::open(f.root.path(), false).await?;
        // Even after the corrected time and cooldown, unknown closure still holds.
        dispatch_notices(&f.store, boundary).await?;
        assert!(!f.marker.exists());
        let after: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT account,generation,exposures,next_at FROM operator_notice_spending",
        )
        .fetch_one(f.store.pool())
        .await?;
        assert_eq!(after, spending);
        assert_eq!(after.2, 1);
        let saved: String = sqlx::query_scalar(
            "SELECT source_snapshot FROM operator_notice_batch_items WHERE batch=?",
        )
        .bind(&batch.id)
        .fetch_one(f.store.pool())
        .await?;
        assert_eq!(saved, membership);
        let account: i64 = sqlx::query_scalar("SELECT account FROM operator_notices WHERE id=?")
            .bind(f.notice)
            .fetch_one(f.store.pool())
            .await?;
        assert_eq!(account, spending.0);
        assert_eq!(occurrence_audit(&f).await?, audit);
    }
    Ok(())
}
