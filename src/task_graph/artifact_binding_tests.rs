//! Actual model transactions only; no runtime admission or physical evidence.
use super::*;

const SCOPE: &str = "publish report artifact";

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
        let writer_key = store.register("g", "writer", false).await?;
        let worker_key = store.register("g", "worker", false).await?;
        let writer = store.authenticate("g", Some(&writer_key)).await?;
        let worker = store.authenticate("g", Some(&worker_key)).await?;
        store.task_create(&writer, draft("report"), 100).await?;
        Ok(Self {
            _temp: temp,
            store,
            writer,
            worker,
        })
    }

    async fn inputs(&self) -> Result<InputSnapshot> {
        let view = self.store.task_inspect(&self.writer, "report").await?;
        self.store
            .task_capture_inputs(&self.writer, "report", view.work.version, Phase::Execute)
            .await
    }

    async fn binding(&self) -> Result<ArtifactBindingProvenance> {
        let view = self.store.task_inspect(&self.writer, "report").await?;
        let mut tx = self.store.pool().begin().await?;
        let proof = validate_artifact_binding_tx(
            &mut tx,
            &self.writer,
            "report",
            view.work.version,
            SCOPE,
            101,
        )
        .await?;
        let provenance = proof.into_provenance();
        tx.rollback().await?;
        // This is audit serialization, not an authority constructor. No runtime
        // binding/attempt is claimed by a model-only fixture.
        Ok(serde_json::from_str(&serde_json::to_string(&provenance)?)?)
    }

    async fn current(
        &self,
        provenance: &ArtifactBindingProvenance,
        inputs: &InputSnapshot,
    ) -> Result<()> {
        let mut tx = self.store.pool().begin().await?;
        let result =
            validate_artifact_binding_current_tx(&mut tx, provenance, inputs, SCOPE, 105).await;
        tx.rollback().await?;
        result
    }
}

fn draft(id: &str) -> TaskCreate {
    serde_json::from_value(serde_json::json!({
        "key":format!("create:{id}"), "reason":"Actual model binding control", "expected_parent_versions":{},
        "draft":{
            "work":{"id":id,"owner":"worker","state":"ready","scope":SCOPE,
                    "next_action":"Publish the report","deadline":null,"evidence":[]},
            "contract":{"deliverable":"One report","criteria":[{"id":"report","description":"Report is complete"}],
                        "allowed_scope":[SCOPE],"completion":"writer_acceptance",
                        "allow_delegation":true,"allow_input_invalidation":true,
                        "budget":{"max_attempts":3,"max_elapsed_seconds":600,"max_cost":null}},
            "authorization":{"state":"authorized","source":{"kind":"direct","authority_ref":"isolated fixture approval"},
                             "approved_scope":[SCOPE],"reason":"Explicit scope"},
            "requirements":[],"parent":null
        }
    })).expect("closed task fixture")
}

fn decision(version: i64, key: &str) -> TaskDecision {
    TaskDecision {
        key: key.into(),
        version,
        reason: "Actual binding guard control".into(),
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
async fn writer_provisioning_cas_is_separate_from_semantic_input_validity() -> Result<()> {
    let f = Fixture::new().await?;
    let original = f.inputs().await?;
    let binding = f.binding().await?;
    assert_eq!(binding.issuer_mailbox(), f.writer.id);
    assert_ne!(binding.issuer_mailbox(), f.worker.id);
    let mut tx = f.store.pool().begin().await?;
    assert!(
        validate_artifact_binding_tx(&mut tx, &f.worker, "report", 1, SCOPE, 101)
            .await
            .unwrap_err()
            .to_string()
            .contains("designated_writer_required")
    );
    assert!(
        validate_artifact_binding_tx(&mut tx, &f.writer, "report", 0, SCOPE, 101)
            .await
            .unwrap_err()
            .to_string()
            .contains("task_version_conflict")
    );
    assert!(
        validate_artifact_binding_tx(
            &mut tx,
            &f.writer,
            "report",
            1,
            "arbitrary destination",
            101
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("artifact_binding_scope_not_authorized")
    );
    tx.rollback().await?;

    let mut note = decision(1, "note-only");
    note.work_patch.evidence = Some(vec!["review-note".into()]);
    let view = f.store.task_decide(&f.writer, "report", note, 102).await?;
    assert!(view.work.version > binding.task_version_at_binding());
    assert_eq!(
        f.store.task_input_validity(&f.writer, &original).await?,
        InputValidity::Current
    );
    f.current(&binding, &original).await?;
    let mut tx = f.store.pool().begin().await?;
    assert_eq!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await?
            .task_version,
        view.work.version
    );
    assert!(
        validate_artifact_binding_tx(&mut tx, &f.writer, "report", 1, SCOPE, 103)
            .await
            .is_err()
    );
    tx.rollback().await?;

    let mut blocked = decision(view.work.version, "block-work");
    blocked.work_patch.state = Some(TaskState::Blocked);
    let view = f
        .store
        .task_decide(&f.writer, "report", blocked, 103)
        .await?;
    let mut tx = f.store.pool().begin().await?;
    // Binding configuration grants neither readiness nor publication.
    validate_artifact_binding_tx(&mut tx, &f.writer, "report", view.work.version, SCOPE, 104)
        .await?;
    assert!(
        validate_publication_inputs_tx(&mut tx, &original, SCOPE)
            .await
            .is_err()
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn full_contract_and_live_authority_are_rechecked_after_provisioning() -> Result<()> {
    let f = Fixture::new().await?;
    let binding = f.binding().await?;
    let view = f.store.task_inspect(&f.writer, "report").await?;
    let mut contract = view.model.context("model missing")?.contract;
    contract.budget.max_attempts += 1;
    let mut change = decision(view.work.version, "change-budget");
    change.contract = Change::Set(contract);
    f.store
        .task_decide(&f.writer, "report", change, 102)
        .await?;
    let current_inputs = f.inputs().await?;
    assert!(
        f.current(&binding, &current_inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_contract_changed")
    );

    let fresh_binding = f.binding().await?;
    f.current(&fresh_binding, &current_inputs).await?;
    let view = f.store.task_inspect(&f.writer, "report").await?;
    let mut authority = view.model.context("model missing")?.authorization;
    authority.state = AuthorityState::Revoked;
    let mut revoke = decision(view.work.version, "revoke-scope");
    revoke.authorization = Change::Set(authority);
    f.store
        .task_decide(&f.writer, "report", revoke, 103)
        .await?;
    assert!(
        f.current(&fresh_binding, &current_inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_authority_unavailable")
    );
    Ok(())
}

#[tokio::test]
async fn writer_and_execution_owner_rebinding_have_independent_guards() -> Result<()> {
    let f = Fixture::new().await?;
    let binding = f.binding().await?;
    let inputs = f.inputs().await?;
    f.store.register("g", "worker", true).await?;
    assert!(
        f.current(&binding, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_artifact_binding_inputs")
    );
    let fresh_inputs = f.inputs().await?;
    f.current(&binding, &fresh_inputs).await?;
    f.store.register("g", "writer", true).await?;
    assert!(
        f.current(&binding, &fresh_inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_issuer_changed")
    );
    Ok(())
}

#[tokio::test]
async fn audit_bytes_cannot_substitute_for_current_issuer_contract_or_phase() -> Result<()> {
    let f = Fixture::new().await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("report".into(), 1);
    f.store.task_create(&f.writer, child, 101).await?;
    let inputs = f.inputs().await?;
    let binding = f.binding().await?;
    f.current(&binding, &inputs).await?;
    let mut accept = inputs.clone();
    accept.phase = Phase::Accept;
    assert!(
        f.current(&binding, &accept)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_artifact_binding_inputs")
    );
    let mut wrong = binding.clone();
    wrong.issuer_mailbox = f.worker.id;
    wrong.issuer = f.worker.name.clone();
    wrong.issuer_binding_version = f.worker.binding_version;
    assert!(
        f.current(&wrong, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_writer_changed")
    );
    let mut wrong = binding.clone();
    let mut contract: Contract = serde_json::from_str(wrong.contract_json())?;
    contract.budget.max_attempts += 1;
    wrong.contract_json = canonical(&contract)?;
    assert!(
        f.current(&wrong, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_contract_digest_mismatch")
    );
    // A digest is public audit identity, never proof of the original writer.
    wrong.contract_digest = artifact_contract_digest(wrong.contract_json());
    assert!(
        f.current(&wrong, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_contract_changed")
    );
    let mut wrong = binding.clone();
    wrong.schema_version = 2;
    assert!(
        f.current(&wrong, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_schema_unsupported")
    );
    let mut value = serde_json::to_value(&binding)?;
    value["replacement_destination"] = serde_json::json!("anything");
    assert!(serde_json::from_value::<ArtifactBindingProvenance>(value).is_err());
    let mut tx = f.store.pool().begin().await?;
    assert!(
        validate_artifact_binding_current_tx(&mut tx, &binding, &inputs, "different scope", 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_input_mismatch")
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn complete_graph_and_exact_schema_remain_mandatory_for_both_guards() -> Result<()> {
    let f = Fixture::new().await?;
    let mut child = draft("child");
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: true,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.expected_parent_versions.insert("report".into(), 1);
    f.store.task_create(&f.writer, child, 101).await?;
    let inputs = f.inputs().await?;
    let binding = f.binding().await?;
    let mut tx = f.store.pool().begin().await?;
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(schema, 32);
    validate_artifact_binding_current_tx(&mut tx, &binding, &inputs, SCOPE, 102).await?;
    validate_publication_inputs_tx(&mut tx, &inputs, SCOPE).await?;
    sqlx::query("DELETE FROM task_blocking_edges WHERE group_name='g' AND owner='model'")
        .execute(&mut *tx)
        .await?;
    assert!(
        validate_artifact_binding_current_tx(&mut tx, &binding, &inputs, SCOPE, 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("projection differs from authoritative source inventory")
    );
    assert!(
        validate_artifact_binding_tx(
            &mut tx,
            &f.writer,
            "report",
            inputs.task_version_at_capture,
            SCOPE,
            102
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    let mut tx = f.store.pool().begin().await?;
    sqlx::query("PRAGMA user_version=33")
        .execute(&mut *tx)
        .await?;
    assert!(
        validate_artifact_binding_current_tx(&mut tx, &binding, &inputs, SCOPE, 102)
            .await
            .unwrap_err()
            .to_string()
            .contains("graph_validation_incomplete")
    );
    assert!(
        validate_publication_inputs_tx(&mut tx, &inputs, SCOPE)
            .await
            .unwrap_err()
            .to_string()
            .contains("publication_projection_owner_integration_unavailable")
    );
    tx.rollback().await?;
    f.current(&binding, &inputs).await?;
    Ok(())
}

#[tokio::test]
async fn inherited_authority_is_rechecked_against_original_ancestor_inputs() -> Result<()> {
    let f = Fixture::new().await?;
    let mut child = draft("delegated");
    child.draft.parent = Some(ParentLink {
        task: "report".into(),
        required: false,
        outcome: OutcomeKind::Accepted,
        revision: None,
    });
    child.draft.authorization.source = AuthoritySource::Parent {
        task: "report".into(),
    };
    child.expected_parent_versions.insert("report".into(), 1);
    let child = f.store.task_create(&f.writer, child, 101).await?;
    let inputs = f
        .store
        .task_capture_inputs(&f.writer, "delegated", child.work.version, Phase::Execute)
        .await?;
    let mut tx = f.store.pool().begin().await?;
    let binding = validate_artifact_binding_tx(
        &mut tx,
        &f.writer,
        "delegated",
        child.work.version,
        SCOPE,
        101,
    )
    .await?
    .into_provenance();
    validate_artifact_binding_current_tx(&mut tx, &binding, &inputs, SCOPE, 101).await?;
    tx.rollback().await?;
    let parent = f.store.task_inspect(&f.writer, "report").await?;
    let mut authority = parent.model.context("parent model missing")?.authorization;
    authority.reason = "New recorded parent authority basis".into();
    let mut change = decision(parent.work.version, "parent-authority-change");
    change.authorization = Change::Set(authority);
    f.store
        .task_decide(&f.writer, "report", change, 102)
        .await?;
    assert!(
        f.current(&binding, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_artifact_binding_inputs")
    );
    let parent = f.store.task_inspect(&f.writer, "report").await?;
    let mut authority = parent.model.context("parent model missing")?.authorization;
    authority.state = AuthorityState::Revoked;
    let mut revoke = decision(parent.work.version, "parent-authority-revoke");
    revoke.authorization = Change::Set(authority);
    f.store
        .task_decide(&f.writer, "report", revoke, 103)
        .await?;
    assert!(
        f.current(&binding, &inputs)
            .await
            .unwrap_err()
            .to_string()
            .contains("artifact_binding_authority_unavailable")
    );
    Ok(())
}

#[tokio::test]
async fn caller_rollback_preserves_atomic_creation_and_binding_validation() -> Result<()> {
    let f = Fixture::new().await?;
    let mut tx = f.store.pool().begin().await?;
    let created = Store::task_create_tx(&mut tx, &f.writer, draft("uncommitted"), 101).await?;
    validate_artifact_binding_tx(
        &mut tx,
        &f.writer,
        "uncommitted",
        created.work.version,
        SCOPE,
        101,
    )
    .await?;
    assert!(
        validate_artifact_binding_tx(
            &mut tx,
            &f.writer,
            "uncommitted",
            created.work.version,
            "refused scope",
            101
        )
        .await
        .is_err()
    );
    tx.rollback().await?;
    for query in [
        "SELECT count(*) FROM work_items WHERE group_name='g' AND id='uncommitted'",
        "SELECT count(*) FROM task_models WHERE group_name='g' AND task='uncommitted'",
        "SELECT count(*) FROM task_decisions WHERE key='create:uncommitted'",
    ] {
        let count: i64 = sqlx::query_scalar(query).fetch_one(f.store.pool()).await?;
        assert_eq!(count, 0, "{query}");
    }
    let actual = f
        .store
        .task_create(&f.writer, draft("uncommitted"), 102)
        .await?;
    assert_eq!(actual.work.version, 1);
    Ok(())
}
