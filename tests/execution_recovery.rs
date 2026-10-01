//! Repair preserves actual model eligibility without granting runtime capability.
mod support;
use agent_mail::{
    states::TaskState,
    store::Store,
    task_graph::{
        AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract, Criterion,
        TaskCreate, TaskDraft,
    },
    work::WorkDraft,
};
use anyhow::Result;
use std::collections::BTreeMap;

#[tokio::test]
async fn repair_preserves_original_model_lifecycle_without_runtime_capability() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let credential = store.register("g", "writer", false).await?;
    let writer = store.authenticate("g", Some(&credential)).await?;
    store
        .task_create(
            &writer,
            TaskCreate {
                key: "create".into(),
                reason: "repair control".into(),
                expected_parent_versions: BTreeMap::new(),
                draft: TaskDraft {
                    work: WorkDraft {
                        id: "job".into(),
                        scope: "artifact".into(),
                        owner: "writer".into(),
                        state: TaskState::Ready,
                        next_action: "write artifact".into(),
                        deadline: None,
                        evidence: vec![],
                    },
                    contract: Contract {
                        deliverable: "artifact".into(),
                        criteria: vec![Criterion {
                            id: "artifact".into(),
                            description: "report exists".into(),
                        }],
                        allowed_scope: vec!["artifact".into()],
                        completion: Completion::WriterAcceptance,
                        allow_delegation: true,
                        allow_input_invalidation: true,
                        budget: Budget {
                            max_attempts: 4,
                            max_elapsed_seconds: 600,
                            max_cost: None,
                        },
                    },
                    authorization: Authorization {
                        state: AuthorityState::Authorized,
                        source: AuthoritySource::Direct {
                            authority_ref: "fixture".into(),
                        },
                        approved_scope: vec!["artifact".into()],
                        reason: "fixture".into(),
                    },
                    requirements: vec![],
                    parent: None,
                },
            },
            100,
        )
        .await?;
    // The actual assembled model invokes scheduler sync inside task creation.
    let first = store.execution_reconcile("g", 150).await?;
    let second = store.execution_reconcile("g", 200).await?;
    assert_eq!(first.model_event, second.model_event);
    let view = store.execution_inspect(&writer, "job").await?;
    assert!(view.lifecycle_ready);
    assert_eq!(view.budgets[0].anchor, Some(100));
    assert_eq!(view.budgets[0].deadline, Some(700));
    assert_eq!(view.budgets[0].attempts_spent, 0);
    assert!(
        view.causes
            .iter()
            .any(|cause| cause.code == "runtime_unavailable" && cause.responsible == "writer")
    );
    let pool = support::pool(&store).await?;
    let counts: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM execution_attempts),(SELECT count(*) FROM execution_causes WHERE code='runtime_unavailable' AND settled=0)").fetch_one(&pool).await?;
    assert_eq!(counts, (0, 1));
    pool.close().await;
    Ok(())
}
