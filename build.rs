//! Build the embedded migration schema used to check SQL queries at compile time.
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=migrations/0001_mail.sql");
    println!("cargo:rerun-if-changed=migrations/0002_work.sql");
    println!("cargo:rerun-if-changed=migrations/0003_prompt_mode.sql");
    println!("cargo:rerun-if-changed=migrations/0004_relay.sql");
    println!("cargo:rerun-if-changed=migrations/0005_auto_sync.sql");
    println!("cargo:rerun-if-changed=migrations/0006_participants.sql");
    println!("cargo:rerun-if-changed=migrations/0007_events.sql");
    println!("cargo:rerun-if-changed=migrations/0008_codex_wake.sql");
    println!("cargo:rerun-if-changed=migrations/0009_attention.sql");
    println!("cargo:rerun-if-changed=migrations/0010_native.sql");
    println!("cargo:rerun-if-changed=migrations/0011_claude_inbox.sql");
    println!("cargo:rerun-if-changed=migrations/0012_cli_workflows.sql");
    println!("cargo:rerun-if-changed=migrations/0013_readiness.sql");
    println!("cargo:rerun-if-changed=migrations/0014_typed_states.sql");
    println!("cargo:rerun-if-changed=migrations/0015_agents.sql");
    println!("cargo:rerun-if-changed=migrations/0016_delivery_probes.sql");
    println!("cargo:rerun-if-changed=migrations/0017_herdr_wakes.sql");
    println!("cargo:rerun-if-changed=migrations/0018_followups.sql");
    println!("cargo:rerun-if-changed=migrations/0019_turn_followthrough.sql");
    println!("cargo:rerun-if-changed=migrations/0020_native_offer_snapshots.sql");
    println!("cargo:rerun-if-changed=migrations/0021_task_contracts.sql");
    println!("cargo:rerun-if-changed=migrations/0022_execution_scheduler.sql");
    println!("cargo:rerun-if-changed=migrations/0023_runtime_adapters.sql");
    println!("cargo:rerun-if-changed=migrations/0024_decision_recovery.sql");
    println!("cargo:rerun-if-changed=migrations/0025_progress_notifications.sql");
    println!("cargo:rerun-if-changed=migrations/0026_execution_driver_cursors.sql");
    println!("cargo:rerun-if-changed=migrations/0027_managed_artifact_lifecycle.sql");
    println!("cargo:rerun-if-changed=migrations/0028_execution_yield_reviews.sql");
    println!("cargo:rerun-if-changed=migrations/0029_supervisor_failure_visits.sql");
    println!("cargo:rerun-if-changed=migrations/0030_supervisor_failure_notices.sql");
    println!("cargo:rerun-if-changed=migrations/0031_runtime_capture_custody.sql");
    println!("cargo:rerun-if-changed=migrations/0032_reclamation_fairness.sql");
    let path = PathBuf::from(env::var("OUT_DIR")?).join("compile-schema.db");
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            use sqlx::Connection;
            let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
            sqlx::raw_sql(include_str!("migrations/0001_mail.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0002_work.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0003_prompt_mode.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0004_relay.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0005_auto_sync.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0006_participants.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0007_events.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0008_codex_wake.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0009_attention.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0010_native.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0011_claude_inbox.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0012_cli_workflows.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0013_readiness.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0014_typed_states.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0015_agents.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0016_delivery_probes.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0017_herdr_wakes.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0018_followups.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0019_turn_followthrough.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0020_native_offer_snapshots.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0021_task_contracts.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0022_execution_scheduler.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0023_runtime_adapters.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0024_decision_recovery.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0025_progress_notifications.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0026_execution_driver_cursors.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!(
                "migrations/0027_managed_artifact_lifecycle.sql"
            ))
            .execute(&mut connection)
            .await?;
            sqlx::raw_sql(include_str!("migrations/0028_execution_yield_reviews.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!(
                "migrations/0029_supervisor_failure_visits.sql"
            ))
            .execute(&mut connection)
            .await?;
            sqlx::raw_sql(include_str!(
                "migrations/0030_supervisor_failure_notices.sql"
            ))
            .execute(&mut connection)
            .await?;
            sqlx::raw_sql(include_str!("migrations/0031_runtime_capture_custody.sql"))
                .execute(&mut connection)
                .await?;
            sqlx::raw_sql(include_str!("migrations/0032_reclamation_fairness.sql"))
                .execute(&mut connection)
                .await?;
            connection.close().await
        })?;
    println!("cargo:rustc-env=DATABASE_URL=sqlite://{}", path.display());
    Ok(())
}
