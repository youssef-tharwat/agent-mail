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
            connection.close().await
        })?;
    println!("cargo:rustc-env=DATABASE_URL=sqlite://{}", path.display());
    Ok(())
}
