use super::*;
use crate::execution_driver::{Controller, JobKind};

// The negative owner fixture fails a REAL scheduled visit. No positive visit,
// failure episode, receipt, controller or source authority is inserted by a test.
pub(super) async fn fail_actual_visit(
    store: &Store,
    controller: &Controller,
    group: &str,
) -> Result<i64> {
    sqlx::query("DELETE FROM decision_supervision WHERE group_name=?")
        .bind(group)
        .execute(store.pool())
        .await?;
    let page = controller.tick_job(store, JobKind::Supervise).await?;
    assert_eq!(page.group.as_deref(), Some(group));
    assert!(page.error.is_some(), "{page:?}");
    assert_eq!(page.visits_completed, 0);
    let (visit, deadline): (i64, i64) = sqlx::query_as(
        "SELECT id,deadline FROM execution_supervisor_visits WHERE group_name=? ORDER BY id DESC LIMIT 1",
    ).bind(group).fetch_one(store.pool()).await?;
    let mut tx = store.pool().begin().await?;
    assert!(
        decision_supervisor::validate_supervisor_commit_tx(&mut tx, visit)
            .await?
            .is_none()
    );
    tx.rollback().await?;
    Ok(deadline)
}

#[tokio::test]
async fn sole_dispatcher_exposes_actual_failure_with_empty_ordinary_inventory() -> Result<()> {
    let (_root, store, now) = fixture(0).await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    dispatch_notices(&store, now).await?;
    let rows = sqlx::query("SELECT n.account,n.unresolved,n.state,s.exposures FROM operator_notices n JOIN operator_notice_spending s ON s.account=n.account WHERE n.source_kind='supervisor_failure'")
        .fetch_all(store.pool()).await?;
    assert_eq!(
        rows.len(),
        1,
        "actual failed visit must become an operator notice"
    );
    assert_eq!(rows[0].get::<i64, _>("exposures"), 1);
    assert!(rows[0].get::<bool, _>("unresolved"));
    assert_eq!(
        rows[0].get::<String, _>("state"),
        "uncertain",
        "actual notifier parent exit never proves descendant closure"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_notice_batches")
            .fetch_one(store.pool())
            .await?,
        1
    );
    controller
        .finish(&store, crate::now()?, "bridge control joined")
        .await?;
    Ok(())
}

#[tokio::test]
async fn sole_dispatcher_exposes_failure_despite_real_ordinary_projection_error() -> Result<()> {
    let (_root, store, now, _message) = obligation_fixture().await?;
    let controller = Controller::acquire(&store, crate::now()?).await?;
    let now = now.max(fail_actual_visit(&store, &controller, "g").await?);
    sqlx::query("UPDATE operator_obligations SET evidence='{}' WHERE group_name='g'")
        .execute(store.pool())
        .await?;
    let mut tx = store.pool().begin().await?;
    assert!(
        project_notice_page_tx(&mut tx, "g", now, 100)
            .await
            .is_err()
    );
    tx.rollback().await?;
    dispatch_notices(&store, now).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM operator_notices WHERE source_kind='supervisor_failure'"
        )
        .fetch_one(store.pool())
        .await?,
        1
    );
    assert!(
        dispatch_notices(&store, now + 1).await.is_err(),
        "ordinary error stays visible and its committed turn cannot starve infrastructure"
    );
    dispatch_notices(&store, now + 1000).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_notice_batches")
            .fetch_one(store.pool())
            .await?,
        1,
        "unknown original sender blocks another batch"
    );
    let evidence: String =
        sqlx::query_scalar("SELECT evidence FROM operator_obligations WHERE group_name='g'")
            .fetch_one(store.pool())
            .await?;
    assert_eq!(evidence, "{}", "notifier never repairs the poisoned source");
    controller
        .finish(&store, crate::now()?, "bridge control joined")
        .await?;
    Ok(())
}
