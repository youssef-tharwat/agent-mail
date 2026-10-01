//! Regression coverage for migration behavior.
mod support;
use agent_mail::{
    store::Store,
    upgrade::{self, OpenMode},
};
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

    let store = upgrade::open(&root, OpenMode::Existing).await?;
    let version = sqlx::query!("PRAGMA user_version")
        .fetch_one(&support::pool(&store).await?)
        .await?;
    assert_eq!(version.user_version, Some(32));
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
    let store = upgrade::open(&root, OpenMode::Existing).await?;
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
        agent_mail::states::BindingKind::Remote
    );
    assert!(
        sqlx::query!("DELETE FROM mailboxes WHERE id=42")
            .execute(&support::pool(&store).await?)
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
    assert!(upgrade::open(&root, OpenMode::Existing).await.is_err());
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

#[tokio::test]
async fn published_six_eight_nine_and_ten_upgrade_with_binding_and_receipts_intact() -> Result<()> {
    for version in [6, 8, 9, 10] {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("state");
        let migrations = temp.path().join("migrations");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&migrations)?;
        for entry in fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_string_lossy();
            if text.ends_with(".sql") && text[..4].parse::<u32>()? <= version {
                fs::copy(entry.path(), migrations.join(name))?;
            }
        }
        let pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(root.join("mail.db"))
                    .create_if_missing(true),
            )
            .await?;
        sqlx::migrate::Migrator::new(migrations.as_path())
            .await?
            .run(&pool)
            .await?;
        // Historical schema fixture; runtime SQL is only for pre-migration schemas.
        sqlx::raw_sql(r#"
          INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
          INSERT INTO groups(name,socket,home_machine) VALUES ('g','','00000000-0000-4000-8000-000000000001');
          INSERT INTO mailboxes(id,group_name,name,binding) VALUES (1,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}');
          INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated) VALUES ('g','task','Review','owner','owner','active','Review',100);
        "#).execute(&pool).await?;
        if version == 10 {
            sqlx::raw_sql("INSERT INTO runtime_wakes(recipient,binding_version,socket,thread,delivered,attempts,next_attempt) VALUES(1,1,'/tmp/test.sock','00000000-0000-4000-8000-000000000003',1,2,400);").execute(&pool).await?;
        } else if version >= 8 {
            sqlx::raw_sql("INSERT INTO codex_wakes(recipient,binding_version,socket,thread,delivered,attempts,next_attempt) VALUES(1,1,'/tmp/test.sock','00000000-0000-4000-8000-000000000003',1,2,400);").execute(&pool).await?;
        }
        pool.close().await;
        let store = upgrade::open(&root, OpenMode::Existing).await?;
        let actor = store.mailbox("g", "owner").await?;
        assert_eq!(store.work_show(&actor, "task").await?.version, 1);
        if version >= 8 {
            let endpoint = sqlx::query!(
                "SELECT runtime,scanned,delivered,attempts,next_attempt FROM runtime_wakes WHERE recipient=1"
            )
            .fetch_one(&support::pool(&store).await?)
            .await?;
            assert_eq!(endpoint.runtime, "codex");
            assert_eq!(
                endpoint.scanned,
                if version == 8 { endpoint.delivered } else { 0 }
            );
            assert_eq!((endpoint.attempts, endpoint.next_attempt), (2, 400));
        }
    }
    Ok(())
}

#[tokio::test]
async fn typed_lifecycle_migration_validates_history_and_rolls_back_ambiguity() -> Result<()> {
    for (state, open, history, succeeds) in [
        ("active", 1, "active", true),
        ("invented", 1, "active", false),
        ("accepted", 1, "active", false),
        ("active", 0, "active", false),
        ("active", 1, "old-custom-state", false),
    ] {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("state");
        let migrations = temp.path().join("migrations");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&migrations)?;
        for entry in fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_string_lossy();
            if text.ends_with(".sql") && text[..4].parse::<u32>()? <= 13 {
                fs::copy(entry.path(), migrations.join(name))?;
            }
        }
        let options = SqliteConnectOptions::new()
            .filename(root.join("mail.db"))
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .connect_with(options.clone())
            .await?;
        sqlx::migrate::Migrator::new(migrations.as_path())
            .await?
            .run(&pool)
            .await?;
        sqlx::raw_sql(r#"
            INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
            INSERT INTO groups(name,socket,home_machine) VALUES ('g','','00000000-0000-4000-8000-000000000001');
            INSERT INTO mailboxes(id,group_name,name,binding) VALUES (1,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}');
        "#).execute(&pool).await?;
        sqlx::query!("INSERT INTO work_items(group_name,id,scope,owner,writer,state,open,next_action,updated) VALUES ('g','task','Review','owner','owner',?,?,'Review',100)",state,open).execute(&pool).await?;
        let snapshot = serde_json::json!({"group_name":"g","id":"task","scope":"Review","owner":"owner","writer":"owner","state":history,"open":true,"next_action":"Review","deadline":null,"accepted_revision":null,"evidence":[],"version":1,"updated":100}).to_string();
        sqlx::query!("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES('g','task',1,'owner','Created',?,100)",snapshot).execute(&pool).await?;
        pool.close().await;
        let migrated = upgrade::open(&root, OpenMode::Existing).await;
        if succeeds {
            let store = migrated?;
            let actor = store.mailbox("g", "owner").await?;
            assert_eq!(
                store.work_history(&actor, "task").await?[0].snapshot.state,
                agent_mail::states::TaskState::Active
            );
            let pool = support::pool(&store).await?;
            let row = sqlx::query!(
                "SELECT snapshot FROM work_changes WHERE group_name='g' AND work_id='task'"
            )
            .fetch_one(&pool)
            .await?;
            assert!(
                serde_json::from_str::<serde_json::Value>(&row.snapshot)?
                    .get("open")
                    .is_none()
            );
        } else {
            let error = migrated.unwrap_err();
            assert!(format!("{error:#}").contains("task_state_requires_explicit_migration"));
            let pool = SqlitePoolOptions::new().connect_with(options).await?;
            assert_eq!(
                sqlx::query!("PRAGMA user_version")
                    .fetch_one(&pool)
                    .await?
                    .user_version,
                Some(13)
            );
            assert_eq!(
                sqlx::query!(
                    "SELECT snapshot FROM work_changes WHERE group_name='g' AND work_id='task'"
                )
                .fetch_one(&pool)
                .await?
                .snapshot,
                snapshot
            );
            let row = sqlx::query!(
                "SELECT state,open FROM work_items WHERE group_name='g' AND id='task'"
            )
            .fetch_one(&pool)
            .await?;
            assert_eq!((row.state.as_str(), row.open), (state, open));
        }
    }
    Ok(())
}

#[tokio::test]
async fn version_sixteen_upgrades_without_changing_business_or_legacy_budgets() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let migrations = temp.path().join("migrations");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&migrations)?;
    for entry in fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy()[..4].parse::<u32>()? > 16 {
            continue;
        }
        fs::copy(entry.path(), migrations.join(entry.file_name()))?;
    }
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join("mail.db"))
                .create_if_missing(true),
        )
        .await?;
    sqlx::migrate::Migrator::new(migrations.as_path())
        .await?
        .run(&pool)
        .await?;
    sqlx::raw_sql(r#"
      INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
      INSERT INTO groups(name,socket,home_machine) VALUES ('g','','00000000-0000-4000-8000-000000000001');
      INSERT INTO mailboxes(id,group_name,name,binding,attempts,next_wake,alerted) VALUES (1,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}',3,400,1);
      INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated) VALUES ('g','task','Review','owner','owner','review','Review report',100);
    "#).execute(&pool).await?;
    pool.close().await;
    let store = upgrade::open(&root, OpenMode::Existing).await?;
    let actor = store.mailbox("g", "owner").await?;
    assert_eq!(
        (actor.attempts, actor.next_wake, actor.alerted),
        (3, 400, 1)
    );
    assert_eq!(store.work_show(&actor, "task").await?.version, 1);
    let pool = support::pool(&store).await?;
    let row = sqlx::query!("SELECT wake_attempted FROM mailboxes WHERE id=1")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.wake_attempted, 0);
    assert_eq!(
        sqlx::query!("PRAGMA user_version")
            .fetch_one(&pool)
            .await?
            .user_version,
        Some(32)
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_first_use_migrates_once_and_keeps_a_verified_backup() -> Result<()> {
    let temp = tempfile::Builder::new()
        .prefix("am-upgrade-")
        .tempdir_in("/tmp")?;
    let (root, pool) = version_five(&temp).await?;
    pool.close().await;
    let (first, second) = tokio::join!(
        upgrade::open(&root, OpenMode::Existing),
        upgrade::open(&root, OpenMode::Existing)
    );
    first?.close().await;
    second?.close().await;
    let backups = fs::read_dir(root.join("backups"))?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(backups.len(), 1);
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(backups[0].path())
                .read_only(true),
        )
        .await?;
    assert_eq!(
        sqlx::query!("PRAGMA user_version")
            .fetch_one(&pool)
            .await?
            .user_version,
        Some(5)
    );
    assert_eq!(
        sqlx::query!("PRAGMA quick_check")
            .fetch_one(&pool)
            .await?
            .quick_check
            .as_deref(),
        Some("ok")
    );
    pool.close().await;
    assert!(!root.join("upgrade.json").exists());
    Ok(())
}

#[tokio::test]
async fn version_eighteen_preserves_saved_policies_and_all_existing_records() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let migrations = temp.path().join("migrations");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&migrations)?;
    for entry in fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy()[..4].parse::<u32>()? <= 18 {
            fs::copy(entry.path(), migrations.join(entry.file_name()))?;
        }
    }
    let options = SqliteConnectOptions::new().filename(root.join("mail.db"));
    let pool = SqlitePoolOptions::new()
        .connect_with(options.clone().create_if_missing(true))
        .await?;
    sqlx::migrate::Migrator::new(migrations.as_path())
        .await?
        .run(&pool)
        .await?;
    sqlx::raw_sql(r#"
        INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
        INSERT INTO groups(name,socket,home_machine) VALUES
          ('observe','','00000000-0000-4000-8000-000000000001'),
          ('enabled','','00000000-0000-4000-8000-000000000001');
        UPDATE followup_policy SET interval_seconds=120,max_seconds=600,notifier='["/usr/bin/true","saved"]',updated=123;
        UPDATE followup_policy SET mode='enabled' WHERE group_name='enabled';
        INSERT INTO mailboxes(id,group_name,name,binding,attempts,next_wake,alerted) VALUES
          (1,'enabled','writer','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}',2,400,1),
          (2,'enabled','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000003"}',3,500,1);
        INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated) VALUES
          ('enabled','task','Review','owner','writer','review','Await approval',100);
        INSERT INTO messages(id,sender,dedup_key,canonical,summary,body,created,due,work_id) VALUES
          (71,1,'migration','{}','Review','Evidence',100,2000,'task');
        INSERT INTO deliveries(message,recipient) VALUES (71,2);
        INSERT INTO event_receipts(recipient,binding_version,event) SELECT recipient,1,id FROM coordination_events;
        UPDATE followups SET version=1,checkpoint='{"version":0,"next_step":"Await approval","next_check_at":500,"waiting":{"kind":"external","responsible":"writer","reason":"Approval required"},"evidence":[]}',next_check=500,retrieved_at=150,retrieved_binding=1;
        INSERT INTO followup_history(followup,version,actor,key,canonical,snapshot,created)
          SELECT id,1,2,'checkpoint-'||id,'{}','{}',150 FROM followups;
    "#).execute(&pool).await?;
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'_sqlx_migrations' ORDER BY name")
        .fetch_all(&pool).await?;
    let before = snapshot(&pool, &tables).await?;
    pool.close().await;

    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .arg("--state-dir")
        .arg(&root)
        .args(["--group", "enabled", "status", "--check", "owner"])
        .output()
        .await?;
    assert!(!output.status.success());
    assert!(!root.join("backups").exists());
    let unchanged = SqlitePoolOptions::new()
        .connect_with(options.read_only(true))
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&unchanged)
            .await?,
        18
    );
    assert_eq!(snapshot(&unchanged, &tables).await?, before);
    unchanged.close().await;
    let store = upgrade::open(&root, OpenMode::Existing).await?;
    let pool = support::pool(&store).await?;
    assert_eq!(snapshot(&pool, &tables).await?, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        32
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    store.enroll("new", None).await?;
    assert_eq!(
        store.followup_policy("new").await?.mode,
        agent_mail::followup::Mode::Enabled
    );
    assert_eq!(
        store.followup_policy("observe").await?.mode,
        agent_mail::followup::Mode::Observe
    );
    assert_eq!(
        store.followup_policy("enabled").await?.notifier,
        Some(vec!["/usr/bin/true".into(), "saved".into()])
    );

    let backups = fs::read_dir(root.join("backups"))?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(backups.len(), 1);
    let backup = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(backups[0].path())
                .read_only(true),
        )
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&backup)
            .await?,
        18
    );
    assert_eq!(snapshot(&backup, &tables).await?, before);
    Ok(())
}

#[tokio::test]
async fn automatic_open_never_initializes_missing_state_or_downgrades() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("missing");
    assert!(upgrade::open(&root, OpenMode::Existing).await.is_err());
    assert!(!root.exists());
    let store = upgrade::open(&root, OpenMode::Initialize).await?;
    store.enroll("g", None).await?;
    store.close().await;
    let pool = SqlitePoolOptions::new()
        .connect_with(SqliteConnectOptions::new().filename(root.join("mail.db")))
        .await?;
    sqlx::query!("PRAGMA user_version=33")
        .execute(&pool)
        .await?;
    pool.close().await;
    let error = upgrade::open(&root, OpenMode::Existing).await.unwrap_err();
    assert!(error.to_string().contains("downgrade"));
    assert!(!root.join("backups").exists());
    Ok(())
}

#[tokio::test]
async fn backup_failure_leaves_the_original_store_untouched() -> Result<()> {
    let temp = tempfile::Builder::new()
        .prefix("am-backup-")
        .tempdir_in("/tmp")?;
    let (root, pool) = version_five(&temp).await?;
    pool.close().await;
    fs::write(root.join("backups"), "blocks backup directory creation")?;
    assert!(upgrade::open(&root, OpenMode::Existing).await.is_err());
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join("mail.db"))
                .read_only(true),
        )
        .await?;
    assert_eq!(
        sqlx::query!("PRAGMA user_version")
            .fetch_one(&pool)
            .await?
            .user_version,
        Some(5)
    );
    assert_eq!(
        sqlx::query!("SELECT COUNT(*) AS 'count!:i64' FROM pragma_table_info('mailboxes') WHERE name='terminal'")
            .fetch_one(&pool)
            .await?
            .count,
        1
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn scoped_cli_commands_upgrade_but_diagnostics_remain_read_only() -> Result<()> {
    let temp = tempfile::Builder::new()
        .prefix("am-cli-upgrade-")
        .tempdir_in("/tmp")?;
    let (root, pool) = version_five(&temp).await?;
    pool.close().await;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    let diagnostic = tokio::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["--group", "g", "status", "--check"])
        .output()
        .await?;
    assert!(!diagnostic.status.success());
    let report: serde_json::Value = serde_json::from_slice(&diagnostic.stdout)?;
    assert_eq!(report["checks"][0]["check"], "database");
    assert!(!root.join("backups").exists());
    // Selection of a sole group happens before normal command dispatch. This
    // path must migrate rather than reject the historical schema at selection.
    let output = tokio::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["remote", "id"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Remote id has no group selection, so exercise that path separately on a
    // fresh old store with an ordinary group-scoped status.
    let temp = tempfile::Builder::new()
        .prefix("am-scoped-upgrade-")
        .tempdir_in("/tmp")?;
    let (root, pool) = version_five(&temp).await?;
    // Seed a historical group so selection can succeed after migration.
    sqlx::raw_sql("INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001'); INSERT INTO groups(name,socket,home_machine) VALUES ('g','','00000000-0000-4000-8000-000000000001');").execute(&pool).await?;
    pool.close().await;
    let output = tokio::process::Command::new(binary)
        .arg("--state-dir")
        .arg(&root)
        .args(["--group", "g", "status", "--json"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.join("backups").is_dir());
    Ok(())
}

async fn snapshot(
    pool: &sqlx::SqlitePool,
    tables: &[String],
) -> Result<Vec<(String, Vec<String>)>> {
    let mut result = Vec::new();
    for table in tables {
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                .bind(table)
                .fetch_all(pool)
                .await?;
        let columns = columns
            .iter()
            .map(|name| format!("\"{}\"", name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT json_array({columns}) FROM \"{}\"",
            table.replace('"', "\"\"")
        );
        let mut rows: Vec<String> = sqlx::query_scalar(&query).fetch_all(pool).await?;
        rows.sort();
        result.push((table.clone(), rows));
    }
    Ok(result)
}

#[tokio::test]
async fn version_nineteen_abandons_only_unreconstructible_native_offers() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let migrations = temp.path().join("migrations");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&migrations)?;
    for entry in fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy()[..4].parse::<u32>()? <= 19 {
            fs::copy(entry.path(), migrations.join(entry.file_name()))?;
        }
    }
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join("mail.db"))
                .create_if_missing(true),
        )
        .await?;
    sqlx::migrate::Migrator::new(migrations.as_path())
        .await?
        .run(&pool)
        .await?;
    sqlx::raw_sql(r#"
        INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001');
        INSERT INTO groups(name,socket,home_machine) VALUES ('g','','00000000-0000-4000-8000-000000000001');
        INSERT INTO mailboxes(id,group_name,name,binding,attempts,next_wake) VALUES
          (1,'g','writer','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}',2,400),
          (2,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000003"}',3,500);
        INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated) VALUES
          ('g','task','Review','owner','writer','active','Inspect evidence',100);
        INSERT INTO runtime_wakes(recipient,binding_version,socket,thread,runtime,attempts,attempted,next_attempt)
          VALUES(2,1,'/tmp/fixture-codex.sock','00000000-0000-4000-8000-000000000004','codex',3,5,900);
        INSERT INTO turn_offers(id,recipient,binding_version,runtime,session,state,turn,created,completed_at) VALUES
          ('native-open',2,1,'codex','old-session','offered','old-turn',100,NULL),
          ('native-completed',2,1,'codex','old-session','completed','done-turn',100,102),
          ('hook-open',2,1,'hook','hook-session','offered',NULL,100,NULL);
        INSERT INTO turn_offer_items(offer,followup,plan_version,stage)
          SELECT o.id,f.id,0,0 FROM turn_offers o CROSS JOIN followups f;
    "#).execute(&pool).await?;
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('_sqlx_migrations','turn_offers') ORDER BY name")
        .fetch_all(&pool).await?;
    let before = snapshot(&pool, &tables).await?;
    type Receipt = (String, String, Option<String>, i64, Option<i64>);
    let receipts: Vec<Receipt> =
        sqlx::query_as("SELECT id,session,turn,created,completed_at FROM turn_offers ORDER BY id")
            .fetch_all(&pool)
            .await?;
    pool.close().await;
    let store = upgrade::open(&root, OpenMode::Existing).await?;
    let pool = support::pool(&store).await?;
    assert_eq!(
        snapshot(&pool, &tables).await?,
        before,
        "business records, history and budgets must survive correction"
    );
    assert_eq!(
        sqlx::query_as::<_, Receipt>(
            "SELECT id,session,turn,created,completed_at FROM turn_offers ORDER BY id",
        )
        .fetch_all(&pool)
        .await?,
        receipts
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>("SELECT id,state FROM turn_offers ORDER BY id")
            .fetch_all(&pool)
            .await?,
        vec![
            ("hook-open".into(), "offered".into()),
            ("native-completed".into(), "completed".into()),
            ("native-open".into(), "abandoned".into()),
        ]
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM turn_offers WHERE native_payload IS NOT NULL OR native_nonce IS NOT NULL").fetch_one(&pool).await?, 0);
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    let backups = fs::read_dir(root.join("backups"))?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(backups.len(), 1);
    let backup = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(backups[0].path())
                .read_only(true),
        )
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&backup)
            .await?,
        19
    );
    assert_eq!(snapshot(&backup, &tables).await?, before);
    Ok(())
}
