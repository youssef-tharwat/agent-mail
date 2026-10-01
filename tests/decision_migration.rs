//! Recovery migration must follow the actual model/execution/runtime sequence.
mod support;
use agent_mail::store::Store;
use anyhow::Result;
use sqlx::Connection;

#[tokio::test]
async fn migration_refuses_a_pre_runtime_schema_without_partial_recovery_tables() -> Result<()> {
    let mut connection = sqlx::SqliteConnection::connect("sqlite::memory:").await?;
    // This is a refusal control, not a placeholder runtime migration or a pass
    // against invented schema23. No recovery table is permitted on this input.
    let mut tx = connection.begin().await?;
    assert!(
        sqlx::raw_sql(include_str!("../migrations/0024_decision_recovery.sql"))
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await?;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM sqlite_master WHERE name LIKE 'decision_%' OR name='operator_obligations'").fetch_one(&mut connection).await?, 0);
    Ok(())
}

#[tokio::test]
async fn normal_composed_migration_preserves_foreign_keys_and_has_no_transport_ledger() -> Result<()>
{
    let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
    assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
    let dir = tempfile::Builder::new()
        .prefix("migration-")
        .tempdir_in(root)?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    let pool = support::pool(&store).await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await?;
    assert!(
        version >= 24,
        "actual22/23 and central24 wiring required; no skipped migrations"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pragma_foreign_key_check")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM decision_supervision WHERE group_name='g' AND heartbeat IS NULL"
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    let columns: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('operator_obligations')")
            .fetch_all(&pool)
            .await?;
    for forbidden in [
        "attempts",
        "lease",
        "route_generation",
        "sent",
        "transport_state",
    ] {
        assert!(
            !columns.iter().any(|column| column == forbidden),
            "shared notifier owns {forbidden}"
        );
    }
    Ok(())
}
