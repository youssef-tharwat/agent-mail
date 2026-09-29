//! State boundaries reject ambiguity before producing durable side effects.
mod support;
use agent_mail::{
    states::TaskState,
    store::Store,
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn lifecycle_controls_recovery_and_database_projection() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "worker", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let worker = store.mailbox("g", "worker").await?;
    let mut task = store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: TaskState::Open,
                next_action: "Review".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    let pool = support::pool(&store).await?;
    for (state, actionable) in [
        (TaskState::Ready, true),
        (TaskState::Active, true),
        (TaskState::Blocked, true),
        (TaskState::Review, true),
        (TaskState::Done, false),
        (TaskState::Active, true),
        (TaskState::Accepted, false),
        (TaskState::Ready, true),
        (TaskState::Cancelled, false),
    ] {
        let update = WorkUpdate {
            version: task.version,
            reason: "Explicit writer decision".into(),
            patch: WorkPatch {
                state: Some(state),
                ..Default::default()
            },
            resolve_message: None,
        };
        assert!(
            store
                .update_work(&worker, "task", update.clone(), 101)
                .await
                .is_err()
        );
        task = store.update_work(&writer, "task", update, 102).await?;
        assert_eq!(task.state.is_open(), actionable);
        assert_eq!(!store.work_list(&worker, "").await?.is_empty(), actionable);
        let row =
            sqlx::query!("SELECT state,open FROM work_items WHERE group_name='g' AND id='task'")
                .fetch_one(&pool)
                .await?;
        assert_eq!(row.state, state.as_str());
        assert_eq!(row.open != 0, actionable);
        assert!(serde_json::to_value(&task)?.get("open").is_none());
        assert_eq!(
            store.work_history(&writer, "task").await?[0].snapshot.state,
            state
        );
    }
    let before = store.latest_changes(&worker).await?;
    assert!(
        sqlx::query!(
            "UPDATE work_items SET state='accepted',open=1 WHERE group_name='g' AND id='task'"
        )
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query!("UPDATE work_items SET state='invented' WHERE group_name='g' AND id='task'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert_eq!(
        store.work_show(&writer, "task").await?.version,
        task.version
    );
    assert_eq!(
        serde_json::to_value(before)?,
        serde_json::to_value(store.latest_changes(&worker).await?)?
    );
    Ok(())
}

#[test]
fn input_rejects_unknown_states_and_obsolete_open_flags() {
    for patch in [
        json!({"state":"reviewing"}),
        json!({"state":"accepted","open":true}),
        json!({"open":false}),
    ] {
        assert!(serde_json::from_value::<WorkPatch>(patch).is_err());
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("must-not-exist");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .args([
            "--state-dir",
            root.to_str().unwrap(),
            "task",
            "create",
            "task",
            "Review",
            "--owner",
            "worker",
            "--state",
            "reviewing",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("reviewing") && error.contains("accepted"),
        "{error}"
    );
    assert!(!root.exists());
}
