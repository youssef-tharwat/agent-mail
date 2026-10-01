//! Model controls use isolated stores. Fixtures do not witness physical runtime effects.
mod support;

use agent_mail::{
    states::TaskState,
    store::{Mailbox, Store},
    task_graph::{
        AuthoritySource, AuthorityState, Authorization, Budget, CandidateDraft, CandidateRequest,
        Change, Completion, Contract, Criterion, CriterionEvidence, DecisionAction, DecisionPolicy,
        DecisionPolicyDecision, DecisionSourceExpectation, JudgeGrant, JudgeGrantDecision,
        OutcomeChange, OutcomeKind, ParentLink, Phase, Requirement, TaskAdopt, TaskCreate,
        TaskDecision, TaskDraft, TaskResult,
    },
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;
use std::collections::BTreeMap;

struct Fixture {
    _temp: tempfile::TempDir,
    store: Store,
    writer: Mailbox,
    worker: Mailbox,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let writer = store.register("g", "writer", false).await?;
        let worker = store.register("g", "worker", false).await?;
        Ok(Self {
            writer: store.authenticate("g", Some(&writer)).await?,
            worker: store.authenticate("g", Some(&worker)).await?,
            _temp: temp,
            store,
        })
    }
}
fn draft(id: &str) -> TaskCreate {
    TaskCreate {
        key: format!("create:{id}"),
        reason: "Finite model control".into(),
        expected_parent_versions: BTreeMap::new(),
        draft: TaskDraft {
            work: WorkDraft {
                id: id.into(),
                scope: "Write controlled artifact".into(),
                owner: "worker".into(),
                state: TaskState::Ready,
                next_action: "Write artifact".into(),
                deadline: None,
                evidence: vec![],
            },
            contract: Contract {
                deliverable: "Finite report".into(),
                criteria: vec![Criterion {
                    id: "report".into(),
                    description: "Report describes the accepted inputs".into(),
                }],
                allowed_scope: vec!["write demo artifact".into()],
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
                    authority_ref: "operator-approved-isolated-fixture".into(),
                },
                approved_scope: vec!["write demo artifact".into()],
                reason: "Explicit isolated control".into(),
            },
            requirements: vec![],
            parent: None,
        },
    }
}
fn decision(version: i64, key: &str) -> TaskDecision {
    TaskDecision {
        key: key.into(),
        version,
        reason: "Audited writer correction".into(),
        work_patch: WorkPatch::default(),
        scope: Change::Keep,
        contract: Change::Keep,
        authorization: Change::Keep,
        requirements: Change::Keep,
        parent: Change::Keep,
        expected_parent_versions: BTreeMap::new(),
        clear_invalidation: false,
        outcome: OutcomeChange::Keep,
        resolve_message: None,
    }
}
fn after(task: &str, outcome: OutcomeKind) -> Requirement {
    Requirement {
        task: task.into(),
        outcome,
        revision: None,
    }
}

// Explicit historical-model fixture, NOT a scheduler success or closure witness.
// It gives the reader/invalidation controls an existing accepted outcome while
// focused writer/closure integration controls below exercise the real API.
async fn historical_acceptance(f: &Fixture, task: &str, revision: &str) -> Result<String> {
    let view = f.store.task_inspect(&f.writer, task).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, task, view.work.version, Phase::Accept)
        .await?;
    let id = uuid::Uuid::new_v4().to_string();
    let result = TaskResult {
        id: id.clone(),
        task: task.into(),
        outcome: Some(OutcomeKind::Accepted),
        revision: revision.into(),
        summary: "Explicit historical fixture".into(),
        criterion_evidence: vec![CriterionEvidence {
            criterion_id: "report".into(),
            references: vec!["fixture-only".into()],
        }],
        inputs: Some(inputs),
        actor: "writer".into(),
        created: 101,
    };
    let pool = support::pool(&f.store).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO task_results(id,group_name,task,kind,payload,created) VALUES(?,'g',?,'accepted',?,101)").bind(&id).bind(task).bind(serde_json::to_string(&result)?).execute(&mut *tx).await?;
    sqlx::query("UPDATE task_models SET current_outcome=? WHERE group_name='g' AND task=?")
        .bind(&id)
        .bind(task)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE work_items SET state='accepted',open=0,accepted_revision=? WHERE group_name='g' AND id=?").bind(revision).bind(task).execute(&mut *tx).await?;
    tx.commit().await?;
    pool.close().await;
    Ok(id)
}

async fn writer_candidate(f: &Fixture, task: &str, revision: &str) -> Result<TaskResult> {
    let view = f.store.task_inspect(&f.writer, task).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, task, view.work.version, Phase::Accept)
        .await?;
    f.store
        .task_candidate(
            &f.writer,
            task,
            CandidateRequest {
                version: view.work.version,
                key: format!("candidate:{revision}"),
                candidate: CandidateDraft {
                    revision: revision.into(),
                    summary: "Writer-reviewed artifact".into(),
                    criterion_evidence: vec![CriterionEvidence {
                        criterion_id: "report".into(),
                        references: vec!["fixture:reviewed-artifact".into()],
                    }],
                    inputs,
                },
            },
            101,
        )
        .await
}

#[tokio::test]
async fn successful_writer_outcome_checks_lifecycle_and_advances_dependents_atomically()
-> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .task_create(&f.writer, draft("producer"), 100)
        .await?;
    let mut consumer = draft("consumer");
    consumer.draft.requirements = vec![after("producer", OutcomeKind::Accepted)];
    f.store.task_create(&f.writer, consumer, 100).await?;
    let candidate = writer_candidate(&f, "producer", "artifact-one").await?;
    let producer = f.store.task_inspect(&f.writer, "producer").await?;
    let mut accept = decision(producer.work.version, "accept-real");
    accept.outcome = OutcomeChange::Success {
        kind: OutcomeKind::Accepted,
        candidate: candidate.id.clone(),
    };
    let pool = support::pool(&f.store).await?;
    let anchor: Option<i64> = sqlx::query_scalar(
        "SELECT anchor FROM execution_budgets WHERE group_name='g' AND task='consumer'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(anchor, None);
    // Explicit missing-lifecycle fixture: model readiness alone cannot bypass
    // the scheduler's actual successful-closure predicate.
    sqlx::query(
        "UPDATE execution_tasks SET lifecycle_ready=0 WHERE group_name='g' AND task='producer'",
    )
    .execute(&pool)
    .await?;
    assert!(
        f.store
            .task_decide(&f.writer, "producer", accept.clone(), 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("execution_lifecycle_unavailable")
    );
    assert_eq!(
        f.store
            .task_inspect(&f.writer, "producer")
            .await?
            .work
            .version,
        producer.work.version
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_results WHERE kind='accepted'")
            .fetch_one(&pool)
            .await?,
        0
    );
    sqlx::query(
        "UPDATE execution_tasks SET lifecycle_ready=1 WHERE group_name='g' AND task='producer'",
    )
    .execute(&pool)
    .await?;
    let accepted = f
        .store
        .task_decide(&f.writer, "producer", accept.clone(), 102)
        .await?;
    assert_eq!(accepted.work.state, TaskState::Accepted);
    assert_eq!(
        accepted.work.accepted_revision.as_deref(),
        Some("artifact-one")
    );
    assert!(
        accepted
            .model
            .as_ref()
            .unwrap()
            .invalidation_causes
            .is_empty()
    );
    assert!(
        f.store
            .task_model_readiness(&f.writer, "consumer", Phase::Execute)
            .await?
            .causes
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT anchor FROM execution_budgets WHERE group_name='g' AND task='consumer'"
        )
        .fetch_one(&pool)
        .await?,
        Some(102)
    );
    let replay = f
        .store
        .task_decide(&f.writer, "producer", accept, 103)
        .await?;
    assert_eq!(
        replay.model.unwrap().current_outcome,
        accepted.model.unwrap().current_outcome
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_results WHERE kind='accepted'")
            .fetch_one(&pool)
            .await?,
        1
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn budget_changes_preserve_anchor_deadline_and_rollback_with_failed_business_write()
-> Result<()> {
    let f = Fixture::new().await?;
    let original = f.store.task_create(&f.writer, draft("task"), 100).await?;
    let mut contract = original.model.unwrap().contract;
    contract.budget.max_attempts = 8;
    contract.budget.max_elapsed_seconds = 1200;
    let mut update = decision(1, "budget");
    update.contract = Change::Set(contract);
    let pool = support::pool(&f.store).await?;
    let initial: (i64,Option<i64>,Option<i64>) = sqlx::query_as("SELECT max_attempts,anchor,deadline FROM execution_budgets WHERE group_name='g' AND task='task'").fetch_one(&pool).await?;
    let mut fails = update.clone();
    fails.key = "budget-and-bad-mail".into();
    fails.resolve_message = Some(i64::MAX);
    assert!(
        f.store
            .task_decide(&f.writer, "task", fails, 101)
            .await
            .is_err()
    );
    assert_eq!(sqlx::query_as::<_,(i64,Option<i64>,Option<i64>)>("SELECT max_attempts,anchor,deadline FROM execution_budgets WHERE group_name='g' AND task='task'").fetch_one(&pool).await?, initial);
    f.store.task_decide(&f.writer, "task", update, 102).await?;
    let changed: (i64,Option<i64>,Option<i64>) = sqlx::query_as("SELECT max_attempts,anchor,deadline FROM execution_budgets WHERE group_name='g' AND task='task'").fetch_one(&pool).await?;
    assert_eq!(changed, (8, initial.1, initial.2));
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn actual_parent_change_preserves_optional_success_and_refuses_started_subtree() -> Result<()>
{
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("parent"), 100).await?;
    let outcome = historical_acceptance(&f, "parent", "parent-artifact").await?;
    let parent = f.store.task_inspect(&f.writer, "parent").await?;
    let mut held = draft("held");
    held.draft.work.state = TaskState::Blocked;
    f.store.task_create(&f.writer, held, 101).await?;
    let link = ParentLink {
        task: "parent".into(),
        required: false,
        outcome: OutcomeKind::Accepted,
        revision: None,
    };
    let mut attach = decision(1, "optional-attach");
    attach.parent = Change::Set(Some(link.clone()));
    attach
        .expected_parent_versions
        .insert("parent".into(), parent.work.version);
    f.store.task_decide(&f.writer, "held", attach, 102).await?;
    let after = f.store.task_inspect(&f.writer, "parent").await?;
    assert_eq!(after.work.state, TaskState::Accepted);
    assert_eq!(after.work.version, parent.work.version + 1);
    assert_eq!(
        after.model.as_ref().unwrap().input_epoch,
        parent.model.as_ref().unwrap().input_epoch
    );
    assert_eq!(
        after.model.as_ref().unwrap().current_outcome.as_deref(),
        Some(outcome.as_str())
    );
    f.store
        .task_create(&f.writer, draft("started"), 103)
        .await?;
    let mut move_started = decision(1, "move-started");
    move_started.parent = Change::Set(Some(link));
    move_started
        .expected_parent_versions
        .insert("parent".into(), after.work.version);
    assert!(
        f.store
            .task_decide(&f.writer, "started", move_started, 104)
            .await
            .unwrap_err()
            .to_string()
            .contains("execution_started_subtree_move_unavailable")
    );
    assert_eq!(
        f.store
            .task_inspect(&f.writer, "parent")
            .await?
            .work
            .version,
        after.work.version
    );
    assert!(
        f.store
            .task_inspect(&f.writer, "started")
            .await?
            .model
            .unwrap()
            .parent
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn success_cannot_use_a_superseded_candidate_or_relabel_its_inputs() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("task"), 100).await?;
    let old = writer_candidate(&f, "task", "artifact-old").await?;
    let current = writer_candidate(&f, "task", "artifact-current").await?;
    let view = f.store.task_inspect(&f.writer, "task").await?;
    let mut request = decision(view.work.version, "old-candidate");
    request.outcome = OutcomeChange::Success {
        kind: OutcomeKind::Accepted,
        candidate: old.id,
    };
    assert!(
        f.store
            .task_decide(&f.writer, "task", request.clone(), 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("current_candidate_required")
    );
    request.key = "relabel-current".into();
    request.outcome = OutcomeChange::Success {
        kind: OutcomeKind::Accepted,
        candidate: current.id,
    };
    request.work_patch.next_action = Some("Use different semantic inputs".into());
    assert!(
        f.store
            .task_decide(&f.writer, "task", request, 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_candidate_inputs")
    );
    assert_eq!(
        f.store.task_inspect(&f.writer, "task").await?.work.version,
        view.work.version
    );
    Ok(())
}

#[tokio::test]
async fn finite_contract_is_atomic_and_exact_retry_preserves_original_receipt() -> Result<()> {
    let f = Fixture::new().await?;
    let mut invalid = draft("bad");
    invalid.draft.contract.criteria.clear();
    assert!(f.store.task_create(&f.writer, invalid, 100).await.is_err());
    let pool = support::pool(&f.store).await?;
    for table in [
        "work_items",
        "work_changes",
        "task_models",
        "task_decisions",
        "task_model_events",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&pool)
                .await?,
            0
        );
    }
    let request = draft("root");
    let first = f.store.task_create(&f.writer, request.clone(), 100).await?;
    assert_eq!(first.execution_hold, "scheduler_admission_required");
    let mut update = decision(first.work.version, "change");
    update.work_patch.next_action = Some("Review artifact".into());
    f.store.task_decide(&f.writer, "root", update, 101).await?;
    let replay = f.store.task_create(&f.writer, request.clone(), 102).await?;
    assert_eq!(serde_json::to_value(first)?, serde_json::to_value(replay)?);
    let mut conflict = request;
    conflict.draft.contract.deliverable = "Different report".into();
    assert!(f.store.task_create(&f.writer, conflict, 103).await.is_err());
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn predicates_satisfy_without_pins_and_reopen_fences_captured_outcomes() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .task_create(&f.writer, draft("producer"), 100)
        .await?;
    let mut consumer = draft("consumer");
    consumer.draft.requirements = vec![after("producer", OutcomeKind::Accepted)];
    f.store.task_create(&f.writer, consumer, 100).await?;
    let waiting = f.store.task_inspect(&f.writer, "consumer").await?;
    assert_eq!(waiting.work.state, TaskState::Ready);
    assert!(
        waiting
            .readiness
            .causes
            .iter()
            .any(|c| c.code == "waiting_outcome")
    );
    let first_outcome = historical_acceptance(&f, "producer", "same-revision").await?;
    let ready = f.store.task_inspect(&f.writer, "consumer").await?;
    assert!(ready.readiness.causes.is_empty());
    assert_eq!(ready.work.version, waiting.work.version);
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, "consumer", ready.work.version, Phase::Accept)
        .await?;
    assert_eq!(inputs.prerequisite_outcome_ids["producer"], first_outcome);
    let candidate = f
        .store
        .task_candidate(
            &f.writer,
            "consumer",
            CandidateRequest {
                version: ready.work.version,
                key: "candidate".into(),
                candidate: CandidateDraft {
                    revision: "consumer-artifact".into(),
                    summary: "Produced against O1".into(),
                    criterion_evidence: vec![CriterionEvidence {
                        criterion_id: "report".into(),
                        references: vec!["artifact:consumer".into()],
                    }],
                    inputs,
                },
            },
            102,
        )
        .await?;
    let producer = f.store.task_inspect(&f.writer, "producer").await?;
    let mut reopen = decision(producer.work.version, "reopen");
    reopen.work_patch.state = Some(TaskState::Ready);
    f.store
        .task_decide(&f.writer, "producer", reopen, 103)
        .await?;
    let stale = f.store.task_inspect(&f.writer, "consumer").await?;
    assert_eq!(stale.work.state, TaskState::Blocked);
    assert_eq!(
        stale.model.as_ref().unwrap().current_candidate.as_ref(),
        Some(&candidate.id)
    );
    assert!(!stale.model.as_ref().unwrap().invalidation_causes.is_empty());
    let second_outcome = historical_acceptance(&f, "producer", "same-revision").await?;
    assert_ne!(first_outcome, second_outcome);
    assert_eq!(
        f.store
            .task_inspect(&f.writer, "consumer")
            .await?
            .work
            .state,
        TaskState::Blocked
    );
    assert_eq!(
        f.store
            .task_results(&f.writer, "producer", None)
            .await?
            .items
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_only_satisfies_an_explicit_compensation_predicate() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("source"), 100).await?;
    for (id, kind) in [
        ("success", OutcomeKind::Accepted),
        ("compensate", OutcomeKind::Cancelled),
    ] {
        let mut request = draft(id);
        request.draft.requirements = vec![after("source", kind)];
        f.store.task_create(&f.writer, request, 100).await?;
    }
    let mut cancel = decision(1, "cancel");
    cancel.outcome = OutcomeChange::Negative {
        kind: OutcomeKind::Cancelled,
        revision: "decision:cancel".into(),
    };
    f.store
        .task_decide(&f.writer, "source", cancel, 101)
        .await?;
    assert!(
        f.store
            .task_inspect(&f.writer, "compensate")
            .await?
            .readiness
            .causes
            .is_empty()
    );
    assert!(
        !f.store
            .task_inspect(&f.writer, "success")
            .await?
            .readiness
            .causes
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn required_children_guard_acceptance_without_blocking_parent_coordination() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("parent"), 100).await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "parent".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.draft.authorization.source = AuthoritySource::Parent {
        task: "parent".into(),
    };
    child.expected_parent_versions.insert("parent".into(), 1);
    let child_created = f.store.task_create(&f.writer, child.clone(), 101).await?;
    let parent = f.store.task_inspect(&f.writer, "parent").await?;
    assert_eq!(parent.work.version, 2);
    assert!(parent.readiness.causes.is_empty());
    assert!(
        f.store
            .task_capture_inputs(&f.writer, "parent", parent.work.version, Phase::Accept)
            .await
            .is_err()
    );
    let child_inputs = f
        .store
        .task_capture_inputs(&f.writer, "child", 1, Phase::Execute)
        .await?;
    let mut note = decision(parent.work.version, "parent-progress");
    note.work_patch.next_action = Some("Assemble child report".into());
    f.store.task_decide(&f.writer, "parent", note, 102).await?;
    let child_now = f.store.task_inspect(&f.writer, "child").await?;
    assert_eq!(
        child_now.model.as_ref().unwrap().input_epoch,
        child_inputs.input_epoch
    );
    let replay = f.store.task_create(&f.writer, child, 103).await?;
    assert_eq!(
        serde_json::to_value(child_created)?,
        serde_json::to_value(replay)?
    );
    let mut stale_child = draft("second");
    stale_child.draft.parent = Some(ParentLink {
        task: "parent".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    stale_child
        .expected_parent_versions
        .insert("parent".into(), 1);
    assert!(
        f.store
            .task_create(&f.writer, stale_child, 104)
            .await
            .is_err()
    );
    Ok(())
}

// Paired controls against the existing model increment. The accepted result is
// an explicit historical fixture, not evidence of scheduler/native success.
async fn optional_child_preserves_parent_success(adopt: bool) -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("parent"), 100).await?;
    let outcome = historical_acceptance(&f, "parent", "artifact:parent").await?;
    let before = f.store.task_inspect(&f.writer, "parent").await?;
    let mut child = draft("optional");
    child.draft.parent = Some(ParentLink {
        task: "parent".into(),
        required: false,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.draft.authorization.source = AuthoritySource::Parent {
        task: "parent".into(),
    };
    child
        .expected_parent_versions
        .insert("parent".into(), before.work.version);
    if adopt {
        let legacy = f
            .store
            .work_create(&f.writer, child.draft.work, 102)
            .await?;
        f.store
            .task_adopt(
                &f.writer,
                "optional",
                TaskAdopt {
                    key: "adopt-optional".into(),
                    version: legacy.version,
                    reason: "Attach an optional historical child".into(),
                    contract: child.draft.contract,
                    authorization: child.draft.authorization,
                    requirements: child.draft.requirements,
                    parent: child.draft.parent,
                    expected_parent_versions: child.expected_parent_versions,
                },
                103,
            )
            .await?;
    } else {
        f.store.task_create(&f.writer, child, 103).await?;
    }
    let after = f.store.task_inspect(&f.writer, "parent").await?;
    assert_eq!(
        after.work.state,
        TaskState::Accepted,
        "optional child must preserve accepted parent disposition"
    );
    let model = after.model.as_ref().unwrap();
    assert_eq!(model.current_outcome.as_deref(), Some(outcome.as_str()));
    assert_eq!(model.input_epoch, before.model.unwrap().input_epoch);
    assert_eq!(after.work.version, before.work.version + 1);
    assert!(model.invalidation_causes.is_empty());
    assert_eq!(after.work.accepted_revision, before.work.accepted_revision);
    Ok(())
}

#[tokio::test]
async fn optional_child_creation_preserves_accepted_parent() -> Result<()> {
    optional_child_preserves_parent_success(false).await
}

#[tokio::test]
async fn optional_child_adoption_preserves_accepted_parent() -> Result<()> {
    optional_child_preserves_parent_success(true).await
}

#[tokio::test]
async fn mixed_cycles_and_concurrent_opposite_edges_roll_back() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("a"), 100).await?;
    f.store.task_create(&f.writer, draft("b"), 100).await?;
    let mut a = decision(1, "a-after-b");
    a.requirements = Change::Set(vec![after("b", OutcomeKind::Accepted)]);
    let mut b = decision(1, "b-after-a");
    b.requirements = Change::Set(vec![after("a", OutcomeKind::Accepted)]);
    let (left, right) = tokio::join!(
        f.store.task_decide(&f.writer, "a", a, 101),
        f.store.task_decide(&f.writer, "b", b, 101)
    );
    assert_ne!(
        left.is_ok(),
        right.is_ok(),
        "exactly one opposing edge may commit"
    );
    let parent = f.store.task_create(&f.writer, draft("parent"), 102).await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "parent".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child
        .expected_parent_versions
        .insert("parent".into(), parent.work.version);
    child.draft.requirements = vec![after("parent", OutcomeKind::Accepted)];
    assert!(
        f.store
            .task_create(&f.writer, child, 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("blocking_cycle")
    );
    assert!(f.store.work_show(&f.writer, "child").await.is_err());
    assert_eq!(
        f.store
            .task_inspect(&f.writer, "parent")
            .await?
            .work
            .version,
        parent.work.version
    );
    Ok(())
}

#[tokio::test]
async fn revoked_then_restored_ancestor_authority_does_not_revive_old_inputs() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("parent"), 100).await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "parent".into(),
        required: false,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("parent".into(), 1);
    child.draft.authorization.source = AuthoritySource::Parent {
        task: "parent".into(),
    };
    f.store.task_create(&f.writer, child, 101).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, "child", 1, Phase::Accept)
        .await?;
    let parent = f.store.task_inspect(&f.writer, "parent").await?;
    let authority = parent.model.as_ref().unwrap().authorization.clone();
    let mut revoke = decision(parent.work.version, "revoke");
    let mut revoked = authority.clone();
    revoked.state = AuthorityState::Revoked;
    revoke.authorization = Change::Set(revoked);
    let revoked = f
        .store
        .task_decide(&f.writer, "parent", revoke, 102)
        .await?;
    let mut restore = decision(revoked.work.version, "restore");
    restore.authorization = Change::Set(authority);
    f.store
        .task_decide(&f.writer, "parent", restore, 103)
        .await?;
    let child = f.store.task_inspect(&f.writer, "child").await?;
    assert!(child.model.as_ref().unwrap().input_epoch > inputs.input_epoch);
    assert_eq!(child.work.state, TaskState::Blocked);
    let stale = CandidateDraft {
        revision: "old".into(),
        summary: "Old authority".into(),
        criterion_evidence: vec![CriterionEvidence {
            criterion_id: "report".into(),
            references: vec!["old".into()],
        }],
        inputs,
    };
    assert!(
        f.store
            .task_candidate(
                &f.writer,
                "child",
                CandidateRequest {
                    version: child.work.version,
                    key: "stale".into(),
                    candidate: stale
                },
                104
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn legacy_wire_and_adoption_preserve_history_and_close_legacy_mutation_bypass() -> Result<()>
{
    let f = Fixture::new().await?;
    let request = draft("legacy");
    let legacy = f
        .store
        .work_create(&f.writer, request.draft.work.clone(), 100)
        .await?;
    let accepted = f
        .store
        .update_work(
            &f.writer,
            "legacy",
            WorkUpdate {
                version: legacy.version,
                reason: "Historical legacy attestation".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    ..Default::default()
                },
                resolve_message: None,
            },
            101,
        )
        .await?;
    let wire = serde_json::to_value(&accepted)?;
    assert!(wire.get("model").is_none());
    let adopted = f
        .store
        .task_adopt(
            &f.writer,
            "legacy",
            TaskAdopt {
                key: "adopt".into(),
                version: accepted.version,
                reason: "Explicit adoption".into(),
                contract: request.draft.contract,
                authorization: request.draft.authorization,
                requirements: vec![],
                parent: None,
                expected_parent_versions: BTreeMap::new(),
            },
            102,
        )
        .await?;
    assert_eq!(adopted.work.state, TaskState::Review);
    assert!(adopted.model.unwrap().current_outcome.is_none());
    let bypass = WorkUpdate {
        version: adopted.work.version,
        reason: "Must not bypass".into(),
        patch: WorkPatch {
            state: Some(TaskState::Accepted),
            ..Default::default()
        },
        resolve_message: None,
    };
    assert!(
        f.store
            .update_work(&f.writer, "legacy", bypass, 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("contracted_task_requires_task_decision")
    );
    assert_eq!(
        f.store.work_show(&f.writer, "legacy").await?.version,
        adopted.work.version
    );
    assert!(
        f.store
            .work_history(&f.writer, "legacy")
            .await?
            .iter()
            .any(|c| c.snapshot.state == TaskState::Accepted)
    );
    Ok(())
}

#[tokio::test]
async fn missing_scheduler_is_explicit_and_progress_grants_do_not_grant_success() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("task"), 100).await?;
    let grant = JudgeGrantDecision {
        key: "judge".into(),
        task_version: 1,
        expected_revision: None,
        reason: "Explicit milestone-only grant".into(),
        grant: JudgeGrant {
            id: "review-report".into(),
            decider: "worker".into(),
            milestone: "report-ready".into(),
            criterion_ids: vec!["report".into()],
            authority_ref: "operator/fixture".into(),
            revoked: false,
        },
    };
    assert!(
        f.store
            .task_judge_grant(&f.worker, "task", grant.clone(), 101)
            .await
            .is_err()
    );
    let receipt = f
        .store
        .task_judge_grant(&f.writer, "task", grant.clone(), 101)
        .await?;
    assert_eq!(receipt.revision, 1);
    assert_eq!(
        f.store.task_inspect(&f.writer, "task").await?.work.version,
        1
    );
    let mut success = decision(1, "success");
    success.outcome = OutcomeChange::Success {
        kind: OutcomeKind::Accepted,
        candidate: "not-a-closure-proof".into(),
    };
    assert!(
        f.store
            .task_decide(&f.writer, "task", success, 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("current_candidate_required")
    );
    assert_eq!(
        f.store.task_inspect(&f.writer, "task").await?.work.version,
        1
    );
    let mut revoke = grant.clone();
    revoke.key = "revoke-judge".into();
    revoke.expected_revision = Some(1);
    revoke.grant.revoked = true;
    assert_eq!(
        f.store
            .task_judge_grant(&f.writer, "task", revoke, 103)
            .await?
            .revision,
        2
    );
    assert_eq!(
        f.store
            .task_judge_grant(&f.writer, "task", grant, 104)
            .await?
            .revision,
        1
    );
    Ok(())
}

#[tokio::test]
async fn incomplete_projection_and_missing_local_prerequisite_never_admit() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("a"), 100).await?;
    let mut b = draft("b");
    b.draft.requirements = vec![after("a", OutcomeKind::Accepted)];
    f.store.task_create(&f.writer, b, 100).await?;
    let pool = support::pool(&f.store).await?;
    sqlx::query("DELETE FROM task_blocking_edges WHERE group_name='g'")
        .execute(&pool)
        .await?;
    assert!(
        f.store
            .task_inspect(&f.writer, "b")
            .await
            .unwrap_err()
            .to_string()
            .contains("graph_validation_incomplete")
    );
    assert!(
        f.store
            .task_decide(&f.writer, "b", decision(1, "invalid-graph"), 101)
            .await
            .is_err()
    );
    let mut absent = draft("absent");
    absent.draft.requirements = vec![after("not-local", OutcomeKind::Accepted)];
    assert!(f.store.task_create(&f.writer, absent, 102).await.is_err());
    assert!(f.store.work_show(&f.writer, "absent").await.is_err());
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn a_rebound_owner_invalidates_stored_success_on_reconciliation() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("task"), 100).await?;
    let outcome = historical_acceptance(&f, "task", "artifact:original-binding").await?;
    f.store.register("g", "worker", true).await?;
    let changed = f
        .store
        .task_reconcile_inputs(&f.writer, "task", 102)
        .await?;
    assert_eq!(changed, vec!["task"]);
    let task = f.store.task_inspect(&f.writer, "task").await?;
    assert_eq!(task.work.state, TaskState::Review);
    assert!(task.model.unwrap().current_outcome.is_none());
    assert!(
        f.store
            .task_results(&f.writer, "task", None)
            .await?
            .items
            .iter()
            .any(|r| r.id == outcome)
    );
    assert!(
        f.store
            .task_reconcile_inputs(&f.writer, "task", 103)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn milestone_grant_revision_and_inputs_are_checked_at_judgment_time() -> Result<()> {
    use agent_mail::task_graph::JudgeGrantRef;
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("task"), 100).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, "task", 1, Phase::Execute)
        .await?;
    let grant = JudgeGrantDecision {
        key: "grant".into(),
        task_version: 1,
        expected_revision: None,
        reason: "Bounded milestone judge".into(),
        grant: JudgeGrant {
            id: "judge".into(),
            decider: "worker".into(),
            milestone: "report-ready".into(),
            criterion_ids: vec!["report".into()],
            authority_ref: "operator/control".into(),
            revoked: false,
        },
    };
    f.store
        .task_judge_grant(&f.writer, "task", grant.clone(), 101)
        .await?;
    let selected = JudgeGrantRef {
        id: "judge".into(),
        revision: 1,
    };
    assert_eq!(
        f.store
            .task_judgment_authority(&f.worker, &inputs, "report-ready", Some(&selected))
            .await?,
        vec!["report"]
    );
    assert!(
        f.store
            .task_judgment_authority(&f.worker, &inputs, "different", Some(&selected))
            .await
            .is_err()
    );
    let mut revoke = grant;
    revoke.key = "revoke".into();
    revoke.expected_revision = Some(1);
    revoke.grant.revoked = true;
    f.store
        .task_judge_grant(&f.writer, "task", revoke, 102)
        .await?;
    assert!(
        f.store
            .task_judgment_authority(&f.worker, &inputs, "report-ready", Some(&selected))
            .await
            .is_err()
    );
    assert!(
        f.store
            .task_judgment_authority(&f.writer, &inputs, "report-ready", None)
            .await
            .is_ok()
    );
    let mut change = decision(1, "new-inputs");
    change.work_patch.next_action = Some("Produce amended artifact".into());
    f.store.task_decide(&f.writer, "task", change, 103).await?;
    assert!(
        f.store
            .task_judgment_authority(&f.writer, &inputs, "report-ready", None)
            .await
            .is_err()
    );
    Ok(())
}

fn decision_policy_request(source: &str, key: &str) -> DecisionPolicyDecision {
    let mut contract = draft("policy-template").draft.contract;
    contract.allow_delegation = false;
    DecisionPolicyDecision {
        key: key.into(),
        expected_revision: None,
        source: DecisionSourceExpectation {
            source: agent_mail::decision_recovery::Obligation::Task {
                id: source.into(),
                version: 1,
            },
            input_epoch: None,
            candidate: None,
            outcome: None,
        },
        policy: DecisionPolicy {
            id: "source-recovery".into(),
            contract,
            actions: vec![DecisionAction::Recommend, DecisionAction::Cancel],
            reviewer: Some("worker".into()),
            allow_writer_fallback: true,
            deadline: 500,
            authority_ref: "explicit-source-writer-consent".into(),
            revoked: false,
        },
        reason: "Authorize one bounded decision for each validated source episode".into(),
    }
}

#[tokio::test]
async fn legacy_decision_policy_requires_original_writer_and_preserves_revoked_history()
-> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .work_create(&f.writer, draft("legacy-source").draft.work, 100)
        .await?;
    let request = decision_policy_request("legacy-source", "authorize-recovery");
    let error = f
        .store
        .decision_policy(&f.worker, request.clone(), 101)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("original_source_writer_required")
    );
    let receipt = f
        .store
        .decision_policy(&f.writer, request.clone(), 101)
        .await?;
    assert_eq!(receipt.revision, 1);
    assert_eq!(receipt.writer, "writer");
    assert_eq!(receipt.source_key, r#"["task","legacy-source"]"#);
    let mut revoke = request.clone();
    revoke.key = "revoke-recovery".into();
    revoke.expected_revision = Some(1);
    revoke.policy.revoked = true;
    assert_eq!(
        f.store
            .decision_policy(&f.writer, revoke, 102)
            .await?
            .revision,
        2
    );
    // Exact historical replay cannot restore the live policy, even after expiry.
    assert_eq!(
        f.store
            .decision_policy(&f.writer, request, 600)
            .await?
            .revision,
        1
    );
    let current: (i64, i64) = sqlx::query_as("SELECT revision,revoked FROM task_decision_policies WHERE group_name='g' AND id='source-recovery'")
        .fetch_one(&support::pool(&f.store).await?).await?;
    assert_eq!(current, (2, 1));
    let history: i64 = sqlx::query_scalar("SELECT count(*) FROM task_decision_policy_history")
        .fetch_one(&support::pool(&f.store).await?)
        .await?;
    assert_eq!(history, 2);
    let model_count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_models")
        .fetch_one(&support::pool(&f.store).await?)
        .await?;
    assert_eq!(
        model_count, 0,
        "source adoption is not arbitrary model creation"
    );
    Ok(())
}

#[tokio::test]
async fn decision_policy_rejects_stale_sources_and_cannot_move_authority_to_another_source()
-> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .work_create(&f.writer, draft("first-source").draft.work, 100)
        .await?;
    f.store
        .work_create(&f.writer, draft("second-source").draft.work, 100)
        .await?;
    let mut stale = decision_policy_request("first-source", "stale-source");
    stale.source.source = agent_mail::decision_recovery::Obligation::Task {
        id: "first-source".into(),
        version: 2,
    };
    assert!(
        f.store
            .decision_policy(&f.writer, stale, 101)
            .await
            .unwrap_err()
            .to_string()
            .contains("decision_source_conflict")
    );
    f.store
        .decision_policy(
            &f.writer,
            decision_policy_request("first-source", "first-policy"),
            101,
        )
        .await?;
    let mut moved = decision_policy_request("second-source", "move-policy");
    moved.expected_revision = Some(1);
    assert!(
        f.store
            .decision_policy(&f.writer, moved, 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("decision_policy_identity_immutable")
    );
    let mut duplicate = decision_policy_request("first-source", "second-policy");
    duplicate.policy.id = "another-policy".into();
    assert!(
        f.store
            .decision_policy(&f.writer, duplicate, 102)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_decision_policy_history")
        .fetch_one(&support::pool(&f.store).await?)
        .await?;
    assert_eq!(
        count, 1,
        "failed mutations leave no authority audit or receipt"
    );
    Ok(())
}
