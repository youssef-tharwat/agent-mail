//! Model publication guards only; these do not witness physical publication or attempts.
mod support;

use agent_mail::{
    states::TaskState,
    store::{Mailbox, Store},
    task_graph::{
        AuthorityState, Change, InputSnapshot, InputValidity, OutcomeChange, ParentLink, Phase,
        TaskCreate, TaskDecision, validate_publication_inputs_tx,
    },
    work::WorkPatch,
};
use anyhow::Result;
use std::collections::BTreeMap;

const SCOPE: &str = "publish report artifact";

struct Fixture {
    _temp: tempfile::TempDir,
    store: Store,
    writer: Mailbox,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let credential = store.register("g", "writer", false).await?;
        store.register("g", "worker", false).await?;
        let writer = store.authenticate("g", Some(&credential)).await?;
        Ok(Self {
            _temp: temp,
            store,
            writer,
        })
    }

    async fn inputs(&self, phase: Phase) -> Result<InputSnapshot> {
        let view = self.store.task_inspect(&self.writer, "report").await?;
        self.store
            .task_capture_inputs(&self.writer, "report", view.work.version, phase)
            .await
    }
}

fn draft(id: &str) -> TaskCreate {
    serde_json::from_value(serde_json::json!({
        "key":format!("create:{id}"), "reason":"Publication model control",
        "expected_parent_versions":{},
        "draft":{
            "work":{"id":id,"owner":"worker","state":"ready","scope":SCOPE,
                    "next_action":"Publish the report","deadline":null,"evidence":[]},
            "contract":{"deliverable":"One report","criteria":[{"id":"report","description":"Report is complete"}],
                        "allowed_scope":[SCOPE],"completion":"writer_acceptance",
                        "allow_delegation":true,"allow_input_invalidation":true,
                        "budget":{"max_attempts":3,"max_elapsed_seconds":600,"max_cost":null}},
            "authorization":{"state":"authorized","source":{"kind":"direct","authority_ref":"isolated test approval"},
                             "approved_scope":[SCOPE],"reason":"Explicit scope"},
            "requirements":[],"parent":null
        }
    })).expect("closed task fixture")
}

fn decision(version: i64, key: &str) -> TaskDecision {
    TaskDecision {
        key: key.into(),
        version,
        reason: "Publication guard control".into(),
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

#[tokio::test]
async fn publication_preserves_phase_scope_and_observed_business_version() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("report"), 100).await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: true,
        outcome: agent_mail::task_graph::OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("report".into(), 1);
    f.store.task_create(&f.writer, child, 101).await?;
    let original = f.inputs(Phase::Execute).await?;
    let mut note = decision(original.task_version_at_capture, "note-only");
    note.work_patch.evidence = Some(vec!["review-notes".into()]);
    let current = f.store.task_decide(&f.writer, "report", note, 102).await?;
    let pool = support::pool(&f.store).await?;
    let mut tx = pool.begin().await?;
    let observed = validate_publication_inputs_tx(&mut tx, &original, SCOPE).await?;
    assert_eq!(observed.phase, Phase::Execute);
    assert_eq!(observed.task_version, current.work.version);
    assert!(observed.task_version > original.task_version_at_capture);
    assert_eq!(observed.input_epoch, original.input_epoch);
    assert_eq!(observed.scope_unit, SCOPE);
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, "publish any artifact")
            .await
            .unwrap_err()
            .to_string()
            .contains("publication_scope_not_authorized")
    );
    let mut relabeled = original.clone();
    relabeled.phase = Phase::Accept;
    assert!(
        validate_publication_inputs_tx(&mut tx, &relabeled, SCOPE)
            .await
            .is_err()
    );
    tx.rollback().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn current_inputs_do_not_override_review_or_blocked_lifecycle() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("report"), 100).await?;
    let execute = f.inputs(Phase::Execute).await?;
    let mut review = decision(1, "review");
    review.work_patch.state = Some(TaskState::Review);
    let view = f
        .store
        .task_decide(&f.writer, "report", review, 101)
        .await?;
    assert_eq!(
        f.store.task_input_validity(&f.writer, &execute).await?,
        InputValidity::Current
    );
    let accept = f.inputs(Phase::Accept).await?;
    let pool = support::pool(&f.store).await?;
    let mut tx = pool.begin().await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &execute, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("publication_model_hold")
    );
    assert_eq!(
        validate_publication_inputs_tx(&mut tx, &accept, SCOPE)
            .await?
            .phase,
        Phase::Accept
    );
    tx.rollback().await?;
    let mut blocked = decision(view.work.version, "blocked");
    blocked.work_patch.state = Some(TaskState::Blocked);
    f.store
        .task_decide(&f.writer, "report", blocked, 102)
        .await?;
    let mut tx = pool.begin().await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &accept, SCOPE)
            .await
            .is_err()
    );
    tx.rollback().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn revoked_authority_and_rebinding_fence_original_publication_inputs() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("report"), 100).await?;
    let original = f.inputs(Phase::Execute).await?;
    f.store.register("g", "worker", true).await?;
    let pool = support::pool(&f.store).await?;
    let mut tx = pool.begin().await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_publication_inputs")
    );
    tx.rollback().await?;
    let current = f.inputs(Phase::Execute).await?;
    let view = f.store.task_inspect(&f.writer, "report").await?;
    let mut authority = view.model.unwrap().authorization;
    authority.state = AuthorityState::Revoked;
    let mut revoke = decision(view.work.version, "revoke");
    revoke.authorization = Change::Set(authority);
    f.store
        .task_decide(&f.writer, "report", revoke, 102)
        .await?;
    let mut tx = pool.begin().await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &current, SCOPE)
            .await
            .is_err()
    );
    tx.rollback().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn incomplete_or_unintegrated_action_projection_never_allows_publication() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.task_create(&f.writer, draft("report"), 100).await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: true,
        outcome: agent_mail::task_graph::OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("report".into(), 1);
    f.store.task_create(&f.writer, child, 101).await?;
    let original = f.inputs(Phase::Execute).await?;
    let pool = support::pool(&f.store).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM task_blocking_edges WHERE group_name='g' AND owner='model'")
        .execute(&mut *tx)
        .await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("projection differs from authoritative source inventory")
    );
    tx.rollback().await?;
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO task_blocking_edges(group_name,owner,source,consumer,prerequisite,kind) VALUES('g','recovery','missing-source',?,?, 'hold')")
        .bind(r#"{"task":"report","action":{"kind":"publish_artifact"}}"#)
        .bind(r#"{"task":"report","action":{"kind":"accept_result"}}"#)
        .execute(&mut *tx).await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("projection differs from authoritative source inventory")
    );
    tx.rollback().await?;
    let mut tx = pool.begin().await?;
    sqlx::query("PRAGMA user_version=33")
        .execute(&mut *tx)
        .await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("publication_projection_owner_integration_unavailable")
    );
    tx.rollback().await?;
    pool.close().await;
    Ok(())
}
