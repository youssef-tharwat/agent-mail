use agent_mail::store::Store;
use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::fs;

#[tokio::test]
async fn existing_version_four_state_upgrades_to_opt_in_sync() -> Result<()> {
    let temp = tempfile::Builder::new()
        .prefix("agent-mail-migration-")
        .tempdir_in("/tmp")?;
    let root = temp.path().join("state");
    let migrations = temp.path().join("old-migrations");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&migrations)?;
    for (name, source) in [
        ("0001_mail.sql", include_str!("../migrations/0001_mail.sql")),
        ("0002_work.sql", include_str!("../migrations/0002_work.sql")),
        (
            "0003_prompt_mode.sql",
            include_str!("../migrations/0003_prompt_mode.sql"),
        ),
        (
            "0004_relay.sql",
            include_str!("../migrations/0004_relay.sql"),
        ),
    ] {
        fs::write(migrations.join(name), source)?;
    }
    let options = SqliteConnectOptions::new()
        .filename(root.join("mail.db"))
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    sqlx::migrate::Migrator::new(migrations.as_path())
        .await?
        .run(&pool)
        .await?;
    pool.close().await;

    let (store, _guard) = Store::open(&root, true).await?;
    let version = sqlx::query!("PRAGMA user_version")
        .fetch_one(&store.pool)
        .await?;
    assert_eq!(version.user_version, Some(5));
    let peer = uuid::Uuid::new_v4();
    store.add_peer(peer, "test-host").await?;
    assert!(!store.peers_status().await?[0].auto_sync);
    Ok(())
}
