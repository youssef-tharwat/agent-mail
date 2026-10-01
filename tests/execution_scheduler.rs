//! Public boundary controls; real runtime lifecycle is independently witnessed.
use agent_mail::{execution::ExecutionPolicy, states::TaskState, store::Store, work::WorkDraft};
use anyhow::Result;

#[test]
fn default_strategy_boundary_has_no_arbitrary_five_minute_timeout() {
    let policy = ExecutionPolicy::default();
    assert_eq!(policy.closed_segment_limit, 2);
    assert_eq!(policy.no_progress_seconds, None);
}

#[tokio::test]
async fn legacy_task_remains_explicitly_untracked() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let credential = store.register("g", "writer", false).await?;
    let writer = store.authenticate("g", Some(&credential)).await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "legacy".into(),
                scope: "legacy wire".into(),
                owner: "writer".into(),
                state: TaskState::Ready,
                next_action: "manual action".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    let page = store.execution_reconcile("g", 110).await?;
    assert!(page.tasks.is_empty());
    let view = store.execution_inspect(&writer, "legacy").await?;
    assert!(view.revision.is_none());
    assert!(!view.lifecycle_ready);
    assert!(view.attempt.is_none());
    assert!(view.budgets.is_empty());
    Ok(())
}
