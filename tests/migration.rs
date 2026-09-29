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
    assert_eq!(version.user_version, Some(8));
    let peer = uuid::Uuid::new_v4();
    store.add_peer(peer, "test-host").await?;
    assert!(!store.peers_status().await?[0].auto_sync);
    Ok(())
}

#[tokio::test]
async fn populated_version_five_keeps_mail_work_bindings_and_foreign_keys() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let (root, pool) = version_five(&temp).await?;
    // Historical schema fixture: current query macros intentionally see only the new schema.
    sqlx::raw_sql(r#"
        INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
        INSERT INTO groups(name,socket,home_machine) VALUES ('g','/tmp/herdr.sock','00000000-0000-4000-8000-000000000001');
        INSERT INTO mailboxes(id,group_name,name,pane,terminal,agent,session_kind,session_value,attempts,next_wake,alerted)
            VALUES (41,'g','a','w1:p1','t1','codex','id','session-a',0,0,0),
                   (42,'g','b','w1:p2','t2','codex','id','session-b',2,1600,1);
        INSERT INTO mailboxes(id,group_name,name,pane,terminal,agent,session_kind,session_value,remote_machine)
            VALUES (43,'g','remote','remote:old','','','','','00000000-0000-4000-8000-000000000002');
        INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated)
            VALUES ('g','api','Review API','b','a','active','Review abc123',1000);
        INSERT INTO messages(id,sender,dedup_key,canonical,summary,body,created,due,work_id)
            VALUES (71,41,'key','{}','Review abc123','Evidence: abc123',1000,1900,'api');
        INSERT INTO deliveries(message,recipient) VALUES (71,42);
        INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed)
            VALUES ('g','api',1,'a','Assigned','{"group_name":"g","id":"api","scope":"Review API","owner":"b","writer":"a","state":"active","open":true,"next_action":"Review abc123","deadline":null,"accepted_revision":null,"evidence":[],"version":1,"updated":1000}',1000);
    "#).execute(&pool).await?;
    pool.close().await;
    assert!(Store::open(&root, false).await.is_err());
    let (store, _guard) = Store::open(&root, true).await?;
    let worker = store.mailbox("g", "b").await?;
    assert_eq!(worker.id, 42);
    assert_eq!(
        (worker.attempts, worker.next_wake, worker.alerted),
        (2, 1600, 1)
    );
    let binding = worker.binding.herdr().unwrap();
    assert_eq!(binding.pane, "w1:p2");
    assert_eq!(binding.session_value, "session-b");
    assert_eq!(store.inbox(&worker, 0).await?[0].id, 71);
    assert_eq!(store.message(&worker, 71).await?.body, "Evidence: abc123");
    assert_eq!(store.work_show(&worker, "api").await?.writer, "a");
    assert_eq!(
        store.work_history(&worker, "api").await?[0].reason,
        "Assigned"
    );
    assert_eq!(
        store.mailbox("g", "remote").await?.binding.runtime(),
        "remote"
    );
    assert!(
        sqlx::query!("DELETE FROM mailboxes WHERE id=42")
            .execute(&store.pool)
            .await
            .is_err()
    );
    store.register("g", "extra", false).await?;
    assert!(store.mailbox("g", "extra").await?.id > 43);
    store.resolve(&worker, 71, "Reviewed", None, 2000).await?;
    assert!(store.inbox(&worker, 0).await?.is_empty());
    Ok(())
}

async fn version_five(temp: &tempfile::TempDir) -> Result<(std::path::PathBuf, sqlx::SqlitePool)> {
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
        (
            "0005_auto_sync.sql",
            include_str!("../migrations/0005_auto_sync.sql"),
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
    Ok((root, pool))
}

#[tokio::test]
async fn invalid_legacy_references_abort_the_migration_atomically() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let (root, pool) = version_five(&temp).await?;
    let mut connection = pool.acquire().await?;
    sqlx::query!("PRAGMA foreign_keys=OFF")
        .execute(&mut *connection)
        .await?;
    sqlx::query!("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due) VALUES (999,'orphan','{}','orphan','',0,1)")
        .execute(&mut *connection).await?;
    drop(connection);
    pool.close().await;
    assert!(Store::open(&root, true).await.is_err());
    let options = SqliteConnectOptions::new().filename(root.join("mail.db"));
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    assert_eq!(
        sqlx::query!("PRAGMA user_version")
            .fetch_one(&pool)
            .await?
            .user_version,
        Some(5)
    );
    let columns = sqlx::query!(
        "SELECT COUNT(*) AS 'count!:i64' FROM pragma_table_info('mailboxes') WHERE name='terminal'"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        columns.count, 1,
        "original schema must survive the failed migration"
    );
    let messages = sqlx::query!("SELECT COUNT(*) AS 'count!:i64' FROM messages")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        messages.count, 1,
        "failed migration must preserve original data"
    );
    Ok(())
}
