//! Independent scans cover absent metadata, retired owners and persisted cursors.
mod support;
use agent_mail::{
    decision_supervisor::{supervise_recovery_page, supervision_status},
    store::{Publish, Store},
};
use anyhow::Result;

#[tokio::test]
async fn supervisor_repairs_missing_plans_without_agents_or_notifier_success() -> Result<()> {
    let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
    assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
    let dir = tempfile::Builder::new()
        .prefix("supervision-")
        .tempdir_in(root)?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "sender", false).await?;
    store.register("g", "owner", false).await?;
    let sender = store.mailbox("g", "sender").await?;
    let opened = 1_700_000_000;
    let pool = support::pool(&store).await?;
    for n in 0..5 {
        store
            .publish(
                &sender,
                Publish {
                    recipients: vec!["owner".into()],
                    key: format!("m{n}"),
                    summary: "Review".into(),
                    body: "Evidence".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                opened,
            )
            .await?;
    }
    // Fault injection: one source loses metadata before any history/occurrence.
    sqlx::query("DELETE FROM followups WHERE message=(SELECT min(id) FROM messages)")
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE mailboxes SET agent_state='retired' WHERE name='owner'")
        .execute(&pool)
        .await?;
    assert!(supervision_status(&store, "g", opened, 10).await?.stale);
    let first = supervise_recovery_page(&store, "g", opened + 4000, 2).await?;
    assert!(!first.completed_scan);
    assert!(first.source_cursor > 0);
    assert_eq!(first.missing_plans_scanned, 1);
    assert!(
        first
            .capability_hold
            .contains("shared_notifier_unavailable")
    );
    // A restart releases the setup Store's exclusive schema lock and both pools.
    pool.close().await;
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    let pool = support::pool(&store).await?;
    for tick in 1..8 {
        supervise_recovery_page(&store, "g", opened + 4000 + tick, 2).await?;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_cases")
            .fetch_one(&pool)
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM operator_obligations WHERE state='escalated'"
        )
        .fetch_one(&pool)
        .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM followups")
            .fetch_one(&pool)
            .await?,
        4,
        "supervision cannot invent a plan"
    );
    assert!(
        supervision_status(&store, "g", opened + 5000, 10)
            .await?
            .stale
    );
    assert!(
        supervision_status(&store, "g", opened, 10).await?.stale,
        "backward clock is unhealthy"
    );
    // Transport does not resolve anything; actual source settlement does.
    sqlx::query("UPDATE deliveries SET state='resolved'")
        .execute(&pool)
        .await?;
    for tick in 10..18 {
        supervise_recovery_page(&store, "g", opened + 4000 + tick, 2).await?;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM operator_obligations WHERE state='superseded'"
        )
        .fetch_one(&pool)
        .await?,
        5
    );
    Ok(())
}
