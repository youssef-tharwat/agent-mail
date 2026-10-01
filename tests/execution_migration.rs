//! Upgrade from actual schema21: preserve existing work and create no attempts.
mod support;
use agent_mail::{
    store::Store,
    upgrade::{self, OpenMode},
};
use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::{fs, path::Path};

#[tokio::test]
async fn execution_upgrade_preserves_legacy_state_and_does_not_create_permission() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let old = temp.path().join("schema21");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&old)?;
    for entry in fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if name.ends_with(".sql") && name < "0022" {
            fs::copy(entry.path(), old.join(name))?;
        }
    }
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join("mail.db"))
                .create_if_missing(true)
                .foreign_keys(true),
        )
        .await?;
    sqlx::migrate::Migrator::new(old.as_path())
        .await?
        .run(&pool)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        21
    );
    sqlx::raw_sql(r#"
      INSERT INTO node VALUES('00000000-0000-4000-8000-000000000001');
      INSERT INTO groups(name,socket,home_machine) VALUES('g','','00000000-0000-4000-8000-000000000001');
      INSERT INTO mailboxes(id,group_name,name,binding,attempts,next_wake,alerted) VALUES(1,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}',3,400,1);
      INSERT INTO work_items(group_name,id,scope,owner,writer,state,next_action,updated) VALUES('g','legacy','Old scope','owner','owner','review','Read report',100);
    "#).execute(&pool).await?;
    pool.close().await;
    assert!(Store::open(&root, false).await.is_err());
    let store = upgrade::open(&root, OpenMode::Existing).await?;
    let actor = store.mailbox("g", "owner").await?;
    assert_eq!(
        (actor.attempts, actor.next_wake, actor.alerted),
        (3, 400, 1)
    );
    assert_eq!(store.work_show(&actor, "legacy").await?.version, 1);
    let pool = support::pool(&store).await?;
    let migrations = sqlx::migrate::Migrator::new(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("migrations")
            .as_path(),
    )
    .await?;
    let latest_version = migrations
        .iter()
        .map(|migration| migration.version)
        .max()
        .expect("actual migrations");
    assert!(latest_version >= 22, "execution migration must be present");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        latest_version
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_attempts")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_budgets")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    pool.close().await;
    Ok(())
}

/// Negative persisted-history fixture: these bytes test migration preservation,
/// never runtime capability, valid admission, or a positive model decision.
#[tokio::test]
async fn migration26_preserves_uncertain_original_and_does_not_backfill_authority() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let old = temp.path().join("schema25");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&old)?;
    for entry in fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if name.ends_with(".sql") && name < "0026" {
            fs::copy(entry.path(), old.join(name))?;
        }
    }
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(root.join("mail.db"))
                .create_if_missing(true)
                .foreign_keys(true),
        )
        .await?;
    sqlx::migrate::Migrator::new(old.as_path())
        .await?
        .run(&pool)
        .await
        .context("migration26 fixture: prepare original schema25")?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        25
    );
    sqlx::raw_sql(r#"
      INSERT INTO node VALUES('00000000-0000-4000-8000-000000000001');
      INSERT INTO groups(name,socket,home_machine) VALUES('g','','00000000-0000-4000-8000-000000000001');
      -- Required legacy owner/writer FK parent, not runtime capability proof.
      INSERT INTO mailboxes(id,group_name,name,binding) VALUES(1,'g','owner','{"runtime":"standalone","session":"00000000-0000-4000-8000-000000000002"}');
      INSERT INTO work_items(group_name,id,scope,owner,writer,state,open,next_action,updated) VALUES('g','legacy','Old scope','owner','owner','cancelled',0,'Retain physical cleanup',100);
      INSERT INTO task_models(group_name,task,contract,authorization,input_epoch) VALUES('g','legacy','{}','{}',1);
      INSERT INTO execution_tasks(group_name,task,fence,policy,lifecycle_ready,due_at,hard_due) VALUES('g','legacy',1,'{}',0,130,700);
      INSERT INTO execution_budgets(group_name,task,max_attempts,elapsed_seconds,attempts_reserved,anchor,deadline) VALUES('g','legacy',4,600,1,100,700);
      INSERT INTO execution_attempts(id,group_name,task,fence,owner,owner_binding,inputs,runtime,runtime_key,dispatch_key,state,admitted,created,observed,reconcile_at) VALUES('old','g','legacy',1,'owner',1,'{}','{}','original','dispatch-original','uncertain',1,100,120,130);
      INSERT INTO execution_slots VALUES('original','old');
      INSERT INTO execution_dispatches(attempt,request,phase,revision,transmissions) VALUES('old','{"original":true}','exposed',2,1);
      INSERT INTO execution_charges(attempt,group_name,account) VALUES('old','g','legacy');
      INSERT INTO execution_events(group_name,task,attempt,kind,payload,created) VALUES('g','legacy','old','legacy-history','{"original":true}',100);
      INSERT INTO execution_receipts(producer,key,canonical,result) VALUES('legacy','original','{ "bytes": 1 }','{"original":true}');
      UPDATE execution_controller SET last_group='g';
      INSERT INTO execution_cursors(group_name,model_event,last_scan,driver_task) VALUES('g',17,120,'legacy');
    "#).execute(&pool).await.context("migration26 fixture: insert negative legacy history before upgrade")?;
    pool.close().await;
    assert!(Store::open(&root, false).await.is_err());
    let store = upgrade::open(&root, OpenMode::Existing)
        .await
        .context("migration26: upgrade original schema25 through registered migrations")?;
    let pool = support::pool(&store).await?;
    let cursors: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT job,last_group,attempts,completed FROM execution_driver_cursors ORDER BY job",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        cursors,
        vec![
            ("claim".into(), "".into(), 0, 0),
            ("reconcile".into(), "".into(), 0, 0),
            ("repair".into(), "g".into(), 0, 0),
            ("supervise".into(), "".into(), 0, 0)
        ]
    );
    let original: (String, i64, i64, i64) = sqlx::query_as(
        "SELECT state,holds_slot,admitted,reconcile_at FROM execution_attempts WHERE id='old'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(original, ("uncertain".into(), 1, 1, 130));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT attempt FROM execution_slots WHERE runtime_key='original'"
        )
        .fetch_one(&pool)
        .await?,
        "old"
    );
    let budget: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT anchor,deadline,attempts_spent,attempts_reserved FROM execution_budgets",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(budget, (100, 700, 0, 1));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT canonical FROM execution_receipts")
            .fetch_one(&pool)
            .await?,
        "{ \"bytes\": 1 }"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_events WHERE kind='progress_claim_basis'"
        )
        .fetch_one(&pool)
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_events")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_events WHERE kind='yield_review'"
        )
        .fetch_one(&pool)
        .await?,
        0
    );
    let yield_index:String=sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='index' AND name='execution_original_yield_review'").fetch_one(&pool).await?;
    assert!(
        yield_index.contains("UNIQUE INDEX") && yield_index.contains("WHERE kind='yield_review'")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT driver_task FROM execution_cursors WHERE group_name='g'"
        )
        .fetch_one(&pool)
        .await?,
        "legacy"
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    pool.close().await;
    Ok(())
}
