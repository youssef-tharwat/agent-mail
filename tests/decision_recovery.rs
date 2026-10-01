//! Authoritative source and finite-case persistence controls.
mod support;
use agent_mail::{
    decision_recovery::Obligation,
    store::{Mailbox, Publish, Store},
};
use anyhow::Result;

async fn fixture() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox, i64)> {
    let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
    assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
    let dir = tempfile::Builder::new()
        .prefix("recovery-")
        .tempdir_in(root)?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "sender", false).await?;
    store.register("g", "recipient", false).await?;
    let sender = store.mailbox("g", "sender").await?;
    let recipient = store.mailbox("g", "recipient").await?;
    Ok((dir, store, sender, recipient, 1_700_000_000))
}

async fn request(store: &Store, sender: &Mailbox, key: &str, now: i64) -> Result<i64> {
    store
        .publish(
            sender,
            Publish {
                recipients: vec!["recipient".into()],
                key: key.into(),
                summary: "Review".into(),
                body: "Review source evidence".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            now,
        )
        .await
}

#[tokio::test]
async fn inspection_has_no_receipt_and_cases_survive_checkpoint_versions() -> Result<()> {
    let (_dir, store, sender, _recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "m", opened).await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    let view = store.inspect_obligation(&sender, source.clone()).await?;
    assert!(view.unresolved);
    let pool = support::pool(&store).await?;
    let retrieved: Option<i64> =
        sqlx::query_scalar("SELECT retrieved_at FROM followups WHERE message=?")
            .bind(message)
            .fetch_one(&pool)
            .await?;
    assert_eq!(retrieved, None);
    let case = store
        .recover_expired_obligation(&sender, "recover", source.clone(), opened + 4000)
        .await?;
    assert_eq!(case.state, "operator_required");
    assert!(case.decision_task.is_none());
    assert!(case.capability_hold.is_some());
    // A changed metadata version is not another episode or allocation.
    sqlx::query("UPDATE followups SET version=version+1 WHERE message=?")
        .bind(message)
        .execute(&pool)
        .await?;
    let same = store
        .recover_expired_obligation(&sender, "another-inspection", source, opened + 4001)
        .await?;
    assert_eq!(
        (same.id, same.operator_obligation),
        (case.id, case.operator_obligation)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_cases")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_obligations")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM deliveries WHERE message=?")
            .bind(message)
            .fetch_one(&pool)
            .await?,
        "pending"
    );
    Ok(())
}

#[tokio::test]
async fn wrong_authority_stale_binding_and_changed_retry_leave_no_partial_writes() -> Result<()> {
    let (_dir, store, sender, recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "m", opened).await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    assert!(
        store
            .recover_expired_obligation(&recipient, "bad", source.clone(), opened + 4000)
            .await
            .is_err()
    );
    let mut stale = sender.clone();
    stale.binding_version += 1;
    assert!(
        store
            .recover_expired_obligation(&stale, "stale", source.clone(), opened + 4000)
            .await
            .is_err()
    );
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_audit")
            .fetch_one(&pool)
            .await?,
        0
    );
    let case = store
        .recover_expired_obligation(&sender, "retry", source.clone(), opened + 4000)
        .await?;
    let other = request(&store, &sender, "other", opened).await?;
    assert!(
        store
            .recover_expired_obligation(
                &sender,
                "retry",
                Obligation::Delivery {
                    message: other,
                    recipient: "recipient".into()
                },
                opened + 4000
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE deliveries SET state='resolved' WHERE message=?")
        .bind(message)
        .execute(&pool)
        .await?;
    assert_eq!(
        store
            .recover_expired_obligation(&sender, "retry", source.clone(), opened + 9000)
            .await?,
        case
    );
    assert!(
        store
            .recover_expired_obligation(&sender, "fresh", source, opened + 9000)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_cases")
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_recovery_has_one_case_and_operator_obligation() -> Result<()> {
    let (_dir, store, sender, _recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "m", opened).await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    let (left, right) = tokio::join!(
        store.recover_expired_obligation(&sender, "left", source.clone(), opened + 4000),
        store.recover_expired_obligation(&sender, "right", source, opened + 4000),
    );
    let (left, right) = (left?, right?);
    assert_eq!(
        (left.id, left.operator_obligation),
        (right.id, right.operator_obligation)
    );
    Ok(())
}

#[tokio::test]
async fn case_correction_preserves_original_boundary_and_does_not_replenish_a_task() -> Result<()> {
    use agent_mail::decision_recovery::CaseCorrection;
    let (_dir, store, sender, recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "m", opened).await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    let case = store
        .recover_expired_obligation(&sender, "recover", source.clone(), opened + 4000)
        .await?;
    let request = CaseCorrection {
        key: "correct-case".into(),
        case_id: case.id,
        version: case.version,
        source: store.inspect_obligation(&sender, source).await?,
        reason: "Original authority commits to review".into(),
        evidence: vec!["case:review-plan".into()],
        review_at: opened + 4100,
        hard_due: opened + 4200,
    };
    assert!(
        store
            .correct_decision_case(&recipient, request.clone(), opened + 4001)
            .await
            .is_err()
    );
    let corrected = store
        .correct_decision_case(&sender, request.clone(), opened + 4001)
        .await?;
    assert_eq!(corrected.id, case.id);
    assert_eq!(corrected.original_due, case.original_due);
    assert_eq!(corrected.original_source, case.original_source);
    assert_eq!(corrected.operator_obligation, case.operator_obligation);
    assert_eq!(corrected.state, "held");
    assert!(corrected.decision_task.is_none());
    assert_eq!(
        store
            .correct_decision_case(&sender, request.clone(), opened + 9000)
            .await?,
        corrected
    );
    let mut stale = request;
    stale.key = "stale".into();
    assert!(
        store
            .correct_decision_case(&sender, stale, opened + 4002)
            .await
            .is_err()
    );
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT escalate_at FROM followups WHERE message=?")
            .bind(message)
            .fetch_one(&pool)
            .await?,
        opened + 3600
    );
    Ok(())
}

#[tokio::test]
async fn direct_source_correction_cas_and_historical_replay_preserve_case_identity() -> Result<()> {
    use agent_mail::decision_recovery::{ExpectedPlan, SourceCorrection};
    let (_dir, store, sender, recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "source-correction", opened).await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    let case = store
        .recover_expired_obligation(&sender, "recover-source", source.clone(), opened + 4000)
        .await?;
    let observed = store.inspect_obligation(&sender, source.clone()).await?;
    let plan = observed.plan.as_ref().unwrap();
    let mut correction = SourceCorrection {
        key: "source-correction".into(),
        source: source.clone(),
        expected_plan: ExpectedPlan::Present {
            id: plan.id,
            version: plan.version,
        },
        case_versions: std::collections::BTreeMap::from([(case.id, case.version)]),
        reason: "Original sender corrects finite review".into(),
        evidence: vec!["source:review".into()],
        next_step: "Review the named source".into(),
        next_check_at: opened + 4100,
        escalation_at: opened + 4200,
    };
    assert!(
        store
            .correct_obligation(&recipient, correction.clone(), opened + 4001)
            .await
            .is_err()
    );
    correction.case_versions.clear();
    assert!(
        store
            .correct_obligation(&sender, correction.clone(), opened + 4001)
            .await
            .is_err()
    );
    assert_eq!(
        store.inspect_obligation(&sender, source.clone()).await?,
        observed
    );
    correction.case_versions.insert(case.id, case.version);
    let receipt = store
        .correct_obligation(&sender, correction.clone(), opened + 4001)
        .await?;
    assert_eq!(
        receipt["plan"]["before"]["opened"],
        receipt["plan"]["after"]["opened"]
    );
    assert_eq!(
        receipt["source_after"]["business_deadline"],
        receipt["source_before"]["business_deadline"]
    );
    assert_eq!(receipt["cases"][0]["original_due"], case.original_due);
    assert_eq!(
        receipt["cases"][0]["operator_obligation"],
        case.operator_obligation
    );
    assert_eq!(receipt["cases"][0]["hard_due"], opened + 4200);
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>("SELECT retrieved_at FROM followups WHERE message=?")
            .bind(message)
            .fetch_one(&pool)
            .await?,
        None
    );
    sqlx::query("UPDATE deliveries SET state='resolved' WHERE message=?")
        .bind(message)
        .execute(&pool)
        .await?;
    assert_eq!(
        store
            .correct_obligation(&sender, correction.clone(), opened + 9000)
            .await?,
        receipt
    );
    correction.reason = "Changed content under used key".into();
    assert!(
        store
            .correct_obligation(&sender, correction, opened + 9000)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM decision_audit WHERE operation='source_correction'"
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn explicit_missing_plan_repair_uses_original_source_opening_without_receipt() -> Result<()> {
    use agent_mail::decision_recovery::{ExpectedPlan, SourceCorrection};
    let (_dir, store, sender, _recipient, opened) = fixture().await?;
    let message = request(&store, &sender, "missing-plan", opened).await?;
    let pool = support::pool(&store).await?;
    sqlx::query("DELETE FROM followups WHERE message=?")
        .bind(message)
        .execute(&pool)
        .await?;
    let source = Obligation::Delivery {
        message,
        recipient: "recipient".into(),
    };
    assert!(
        store
            .inspect_obligation(&sender, source.clone())
            .await?
            .plan
            .is_none()
    );
    let receipt = store
        .correct_obligation(
            &sender,
            SourceCorrection {
                key: "repair-plan".into(),
                source,
                expected_plan: ExpectedPlan::Absent,
                case_versions: Default::default(),
                reason: "Explicit missing metadata repair".into(),
                evidence: vec!["source:present".into()],
                next_step: "Review source".into(),
                next_check_at: opened + 4100,
                escalation_at: opened + 4200,
            },
            opened + 4000,
        )
        .await?;
    assert_eq!(receipt["plan"]["before"], serde_json::Value::Null);
    assert_eq!(receipt["plan"]["after"]["opened"], opened);
    assert_eq!(
        receipt["plan"]["after"]["retrieved_at"],
        serde_json::Value::Null
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM deliveries WHERE message=?")
            .bind(message)
            .fetch_one(&pool)
            .await?,
        "pending"
    );
    Ok(())
}
