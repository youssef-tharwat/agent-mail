//! Progress authority/storage controls; these do not claim runtime execution.
mod support;
use agent_mail::{
    progress::{Milestone, PolicyChange, ProgressPolicy},
    store::{Mailbox, Store},
    task_graph::TaskCreate,
};
use anyhow::Result;
use serde_json::json;

async fn fixture() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox)> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let writer_token = store.register("g", "writer", false).await?;
    let worker_token = store.register("g", "worker", false).await?;
    let writer = store.authenticate("g", Some(&writer_token)).await?;
    let worker = store.authenticate("g", Some(&worker_token)).await?;
    let request: TaskCreate = serde_json::from_value(
        json!({"key":"create-job","reason":"Isolated progress control","expected_parent_versions":{},"draft":{
            "work":{"id":"job","scope":"Write report","owner":"worker","state":"ready","next_action":"Write report","deadline":null,"evidence":[]},
            "contract":{"deliverable":"Report","criteria":[{"id":"report","description":"Complete report"}],"allowed_scope":["write report"],"completion":"writer_acceptance","allow_delegation":false,"allow_input_invalidation":true,"budget":{"max_attempts":4,"max_elapsed_seconds":600,"max_cost":null}},
            "authorization":{"state":"authorized","source":{"kind":"direct","authority_ref":"isolated progress fixture"},"approved_scope":["write report"],"reason":"Explicit fixture scope"},"requirements":[],"parent":null
        }}),
    )?;
    store.task_create(&writer, request, 100).await?;
    Ok((temp, store, writer, worker))
}

fn policy() -> PolicyChange {
    PolicyChange {
        key: "policy".into(),
        task_version: 1,
        expected_revision: None,
        reason: "Declare report milestone".into(),
        policy: ProgressPolicy {
            milestones: vec![Milestone {
                id: "report-ready".into(),
                criterion_ids: vec!["report".into()],
                scope_units: vec!["write report".into()],
            }],
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn independent_policy_cas_replay_and_source_versions() -> Result<()> {
    let (_temp, store, writer, _worker) = fixture().await?;
    let before = store.task_inspect(&writer, "job").await?;
    let change = policy();
    let first = store.progress_policy(&writer, "job", &change, 101).await?;
    let replay = store.progress_policy(&writer, "job", &change, 900).await?;
    assert_eq!(first.record, replay.record);
    let after = store.task_inspect(&writer, "job").await?;
    assert_eq!(before.work.version, after.work.version);
    assert_eq!(
        before
            .model
            .as_ref()
            .expect("contracted before")
            .input_epoch,
        after.model.as_ref().expect("contracted after").input_epoch
    );
    assert_eq!(change.policy.max_segments_without_milestone, 2);
    assert_eq!(change.policy.max_elapsed_without_milestone, None);
    let mut conflict = change.clone();
    conflict.reason = "Changed replay".into();
    assert!(
        store
            .progress_policy(&writer, "job", &conflict, 102)
            .await
            .is_err()
    );
    let mut stale = change;
    stale.key = "stale".into();
    assert!(
        store
            .progress_policy(&writer, "job", &stale, 102)
            .await
            .is_err()
    );
    assert_eq!(
        store.progress_history(&writer, "job", 0, 100).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn worker_cannot_set_policy_or_leave_a_rejected_business_record() -> Result<()> {
    let (_temp, store, writer, worker) = fixture().await?;
    assert!(
        store
            .progress_policy(&worker, "job", &policy(), 101)
            .await
            .is_err()
    );
    assert!(
        store
            .progress_history(&writer, "job", 0, 100)
            .await?
            .is_empty()
    );
    let pool = support::pool(&store).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_progress")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn milestone_identity_cannot_change_meaning_and_history_is_immutable() -> Result<()> {
    let (_temp, store, writer, _worker) = fixture().await?;
    let first = store
        .progress_policy(&writer, "job", &policy(), 101)
        .await?;
    let mut next = policy();
    next.key = "changed-scope".into();
    next.expected_revision = Some(first.revision);
    next.policy.milestones[0].scope_units = vec!["outside contract".into()];
    assert!(
        store
            .progress_policy(&writer, "job", &next, 102)
            .await
            .is_err()
    );
    let pool = support::pool(&store).await?;
    assert!(
        sqlx::query("UPDATE progress_records SET created=999 WHERE id=?")
            .bind(first.record)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM progress_records WHERE id=?")
            .bind(first.record)
            .execute(&pool)
            .await
            .is_err()
    );
    Ok(())
}
