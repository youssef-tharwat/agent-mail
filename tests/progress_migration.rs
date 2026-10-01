//! Real contiguous migration controls; missing owner migrations are an error.
use anyhow::{Context, Result, ensure};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

async fn through(version: u32) -> Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::query("PRAGMA foreign_keys=ON").execute(&pool).await?;
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for number in 1..=version {
        let prefix = format!("{number:04}_");
        let mut paths = std::fs::read_dir(&directory)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with(&prefix))
            })
            .collect::<Vec<_>>();
        ensure!(
            paths.len() == 1,
            "actual migration {number} must exist exactly once"
        );
        let path = paths.pop().context("migration path")?;
        sqlx::raw_sql(&std::fs::read_to_string(path)?)
            .execute(&pool)
            .await?;
        let actual: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        ensure!(actual == i64::from(number), "migration order mismatch");
    }
    Ok(pool)
}

#[tokio::test]
async fn retains_all_alias_spending_and_exact_unknown_provenance() -> Result<()> {
    let pool = through(24).await?;
    sqlx::query("INSERT INTO node(id) VALUES('fixture-home')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO groups(name,socket,home_machine) VALUES('g','','fixture-home')")
        .execute(&pool)
        .await?;
    for (id, name) in [(1, "writer"), (2, "worker")] {
        // Migration 6 stores routing in binding; pane is a generated projection.
        let binding = serde_json::json!({
            "runtime": "herdr",
            "pane": format!("pane-{id}"),
            "terminal": format!("term-{id}"),
            "agent": "codex",
            "session_kind": "codex",
            "session_value": format!("session-{id}")
        });
        sqlx::query("INSERT INTO mailboxes(id,group_name,name,binding) VALUES(?,'g',?,?)")
            .bind(id)
            .bind(name)
            .bind(binding.to_string())
            .execute(&pool)
            .await?;
    }
    sqlx::query("UPDATE followup_policy SET mode='observe' WHERE group_name='g'")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO messages(id,sender,dedup_key,canonical,summary,body,created,due) VALUES(1,1,'original','{}','Decision','Original source',100,300)").execute(&pool).await?;
    sqlx::query("INSERT INTO deliveries(message,recipient) VALUES(1,2)")
        .execute(&pool)
        .await?;
    let plan: i64 = sqlx::query_scalar("SELECT id FROM followups WHERE message=1 AND recipient=2")
        .fetch_one(&pool)
        .await?;
    sqlx::query("UPDATE followups SET retrieved_at=222,retrieved_binding=4 WHERE id=?")
        .bind(plan)
        .execute(&pool)
        .await?;
    for (version, attempts, state) in [(0, 2, "failed"), (1, 1, "accepted"), (2, 3, "attempting")] {
        sqlx::query("INSERT INTO attention_occurrences(followup,plan_version,stage,recipient,created,operator_after,operator_attempts,operator_next,operator_state,retrieved_at) VALUES(?,?,3,1,110,300,?,600,?,150)")
            .bind(plan).bind(version).bind(attempts).bind(state).execute(&pool).await?;
    }
    sqlx::raw_sql(include_str!(
        "../migrations/0025_progress_notifications.sql"
    ))
    .execute(&pool)
    .await?;
    let accounts: i64 = sqlx::query_scalar("SELECT count(*) FROM operator_notice_accounts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        accounts, 1,
        "occurrence aliases must share one cause account"
    );
    let exposures: i64 = sqlx::query_scalar("SELECT exposures FROM operator_notice_spending")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        exposures, 6,
        "retain all historical spending, including over current cap"
    );
    let rows=sqlx::query("SELECT state,occurrence_retrieved_at,occurrence_retrieved_binding,provenance FROM operator_notice_legacy ORDER BY occurrence").fetch_all(&pool).await?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1].get::<String, _>("state"), "accepted");
    assert_eq!(rows[2].get::<String, _>("state"), "attempting");
    for row in rows {
        assert_eq!(
            row.get::<Option<i64>, _>("occurrence_retrieved_at"),
            Some(150)
        );
        assert_eq!(
            row.get::<Option<i64>, _>("occurrence_retrieved_binding"),
            None,
            "never copy newer plan binding onto an old occurrence"
        );
        let provenance: serde_json::Value =
            serde_json::from_str(&row.get::<String, _>("provenance"))?;
        assert!(provenance["historical_route_generation"].is_null());
        assert_eq!(provenance["followup_retrieved_binding_at_migration"], 4);
    }
    let mode: String = sqlx::query_scalar("SELECT mode FROM followup_policy WHERE group_name='g'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(mode, "observe");
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn schema25_refuses_skipping_actual_recovery24() -> Result<()> {
    let pool = through(23).await?;
    assert!(
        sqlx::raw_sql(include_str!(
            "../migrations/0025_progress_notifications.sql"
        ))
        .execute(&pool)
        .await
        .is_err()
    );
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await?;
    assert_eq!(version, 23);
    Ok(())
}

#[tokio::test]
async fn schema30_preserves_exact_notice_history_spending_uncertainty_and_foreign_keys()
-> Result<()> {
    let pool = through(29).await?;
    sqlx::query("INSERT INTO node(id) VALUES('migration-home')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO groups(name,socket,home_machine) VALUES('g','','migration-home')")
        .execute(&pool)
        .await?;
    for (id, name) in [(1, "writer"), (2, "worker")] {
        let binding = serde_json::json!({"runtime":"herdr","pane":format!("pane-{id}"),
            "terminal":format!("term-{id}"),"agent":"codex","session_kind":"codex", "session_value":format!("session-{id}")});
        sqlx::query("INSERT INTO mailboxes(id,group_name,name,binding) VALUES(?,'g',?,?)")
            .bind(id)
            .bind(name)
            .bind(binding.to_string())
            .execute(&pool)
            .await?;
    }
    sqlx::query("INSERT INTO messages(id,sender,dedup_key,canonical,summary,body,created,due) VALUES(1,1,'original','{}','Original','Retained evidence',100,300)")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO deliveries(message,recipient) VALUES(1,2)")
        .execute(&pool)
        .await?;
    let followup: i64 =
        sqlx::query_scalar("SELECT id FROM followups WHERE message=1 AND recipient=2")
            .fetch_one(&pool)
            .await?;
    sqlx::query("INSERT INTO attention_occurrences(id,followup,plan_version,stage,recipient,created,operator_after,operator_attempts,operator_next,operator_state) VALUES(1,?,0,3,1,110,300,6,900,'attempting')")
        .bind(followup).execute(&pool).await?;
    // These are pre-upgrade ledger fixtures, not successful supervisor proofs.
    sqlx::raw_sql(r#"
        INSERT INTO operator_notice_routes(group_name,generation,route,changed)
          VALUES('g',7,'{"notifier":["/usr/bin/false"],"socket":""}',120);
        INSERT INTO operator_notice_accounts(id,group_name,source_key,episode)
          VALUES(10,'g','["delivery",1,2]','obligation'),(20,'g','retained-key','original-cause');
        INSERT INTO operator_notice_spending(account,generation,exposures,next_at)
          VALUES(10,7,6,900),(10,6,3,700),(20,7,3,901);
        INSERT INTO operator_notices(id,group_name,source_kind,source_id,account,revision,source_snapshot,responsible,unresolved,due_at,first_dirty,accepted_revision,accepted_generation,state,batch)
          VALUES(30,'g','attention_occurrence',1,10,4,'{"exact":"old bytes"}','writer',1,300,100,2,6,'uncertain','old-unknown'),
                (40,'g','operator_obligation',9,20,3,'{"exact":"accepted bytes"}','writer',1,301,101,3,7,'accepted','old-accepted');
        INSERT INTO operator_notice_batches(id,group_name,generation,route,payload,owner,lease_until,state,exposed_at,finished_at,detail)
          VALUES('old-unknown','g',7,'{}','{"saved":"unknown"}','old-owner',150,'uncertain',121,122,'closure unknown'),
                ('old-accepted','g',7,'{}','{"saved":"accepted"}','old-owner',151,'accepted',122,123,'retained receipt');
        INSERT INTO operator_notice_batch_items(batch,notice,revision,account,source_snapshot)
          VALUES('old-unknown',30,4,10,'{"exact":"old bytes"}'),('old-accepted',40,3,20,'{"exact":"accepted bytes"}');
        INSERT INTO operator_notice_events(id,group_name,batch,kind,payload,created)
          VALUES(50,'g','old-unknown','transport_result','{"sender_uncertain":true}',122),
                (51,'g','old-accepted','transport_result','{"transport_accepted":true}',123);
        INSERT INTO operator_notice_projection(group_name,attention_after,obligation_after) VALUES('g',73,82);
        UPDATE operator_notice_dispatch_cursor SET last_group='g' WHERE singleton=1;
        INSERT INTO operator_notice_repairs(group_name,key,canonical,generation) VALUES('g','old-repair','{"saved":"exact"}',7);
        INSERT INTO operator_notice_legacy(occurrence,account,attempts,state,next_at,detail,occurrence_retrieved_at,occurrence_retrieved_binding,provenance)
          VALUES(1,10,6,'attempting',900,'original unknown sender',NULL,NULL,'{"historical_route_generation":null}');
    "#).execute(&pool).await?;
    let tables = [
        "operator_notice_routes",
        "operator_notice_accounts",
        "operator_notice_spending",
        "operator_notices",
        "operator_notice_batches",
        "operator_notice_batch_items",
        "operator_notice_events",
        "operator_notice_projection",
        "operator_notice_dispatch_cursor",
        "operator_notice_repairs",
        "operator_notice_legacy",
    ];
    let mut old_columns = Vec::new();
    for table in tables {
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(&pool)
            .await?
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!(
            "CREATE TEMP TABLE retained_{table} AS SELECT * FROM {table}"
        ))
        .execute(&pool)
        .await?;
        old_columns.push((table, columns));
    }
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(include_str!(
        "../migrations/0030_supervisor_failure_notices.sql"
    ))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    for (table, columns) in old_columns {
        for (left, right) in [
            (table.to_owned(), format!("retained_{table}")),
            (format!("retained_{table}"), table.to_owned()),
        ] {
            let difference: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM (SELECT {columns} FROM {left} EXCEPT SELECT {columns} FROM {right})"))
                .fetch_one(&pool).await?;
            assert_eq!(
                difference, 0,
                "every old column and row of {table} must be exact"
            );
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        30
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM operator_notice_batches WHERE notice_class='ordinary'"
        )
        .fetch_one(&pool)
        .await?,
        2
    );
    assert_eq!(sqlx::query_as::<_, (i64, String)>("SELECT infrastructure_after,next_class FROM operator_notice_projection WHERE group_name='g'")
        .fetch_one(&pool).await?, (0, "infrastructure".into()));
    assert!(sqlx::query("UPDATE operator_notice_batches SET notice_class='infrastructure' WHERE id='old-unknown'")
        .execute(&pool).await.is_err());
    assert!(
        sqlx::query("UPDATE operator_notice_batch_items SET revision=5 WHERE batch='old-unknown'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM operator_notice_batch_items WHERE batch='old-unknown'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM operator_notice_events WHERE id=50")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn schema30_refuses_missing_actual29_without_changing_old_tables() -> Result<()> {
    let pool = through(28).await?;
    let original: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='operator_notices'",
    )
    .fetch_one(&pool)
    .await?;
    let mut tx = pool.begin().await?;
    assert!(
        sqlx::raw_sql(include_str!(
            "../migrations/0030_supervisor_failure_notices.sql"
        ))
        .execute(&mut *tx)
        .await
        .is_err()
    );
    tx.rollback().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?,
        28
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='operator_notices'"
        )
        .fetch_one(&pool)
        .await?,
        original
    );
    Ok(())
}
