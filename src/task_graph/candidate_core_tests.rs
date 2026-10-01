//! Actual Candidate transactions; no native or writer-review completion proof.
use super::transaction_tests::{
    ContinuationFixture, continuation_fixture, continuation_state, phase_candidate, request,
};
use super::*;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::UnixListener, sync::Notify};

type HintKey = (PathBuf, i64, String);

#[derive(Default)]
struct CommitGate {
    reached: Notify,
    release: Notify,
}

fn gates() -> &'static Mutex<BTreeMap<HintKey, Arc<CommitGate>>> {
    static GATES: OnceLock<Mutex<BTreeMap<HintKey, Arc<CommitGate>>>> = OnceLock::new();
    GATES.get_or_init(Mutex::default)
}

struct GateRegistration {
    key: HintKey,
    gate: Arc<CommitGate>,
}

impl GateRegistration {
    fn new(root: &Path, actor: &Mailbox, key: &str) -> Self {
        let key = (root.to_owned(), actor.id, key.to_owned());
        let gate = Arc::new(CommitGate::default());
        assert!(
            gates()
                .lock()
                .expect("commit gates")
                .insert(key.clone(), gate.clone())
                .is_none()
        );
        Self { key, gate }
    }
}

impl Drop for GateRegistration {
    fn drop(&mut self) {
        gates().lock().expect("commit gates").remove(&self.key);
        self.gate.release.notify_one();
    }
}

// Called only after the real public transaction commits, before its real hint.
pub(super) async fn committed_before_hint(root: &Path, actor: &Mailbox, key: &str) {
    let gate = gates()
        .lock()
        .expect("commit gates")
        .get(&(root.to_owned(), actor.id, key.to_owned()))
        .cloned();
    if let Some(gate) = gate {
        gate.reached.notify_one();
        gate.release.notified().await;
    }
}

async fn materialized_request(key: &str) -> Result<(ContinuationFixture, CandidateRequest)> {
    let f = continuation_fixture(true, false, false).await?;
    let current = f.store.task_inspect(&f.writer, &f.task).await?;
    let candidate = phase_candidate(&f.store, &f.reviewer, &current, key).await?;
    Ok((f, candidate))
}

async fn persist(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    task: &str,
    request: CandidateRequest,
) -> Result<PendingCandidateWrite> {
    match Store::persist_candidate_with_reconciliation_tx(
        tx,
        actor,
        normalize_candidate_request(task, request)?,
        117,
    )
    .await?
    {
        CandidateWrite::Fresh(pending) => Ok(pending),
        CandidateWrite::Historical(_) => anyhow::bail!("expected genuine fresh candidate"),
    }
}

#[tokio::test]
async fn fresh_candidate_witness_precedes_actual_phase_finalizer() -> Result<()> {
    let (f, request) = materialized_request("observe-candidate").await?;
    let before = continuation_state(&f.store).await?;
    let mut tx = f.store.pool().begin().await?;
    let pending = persist(&mut tx, &f.reviewer, &f.task, request).await?;
    validate_applied_model_decision_tx(&mut tx, pending.applied()).await?;
    let receipt = materialized_receipt_tx(&mut tx, "g", &f.task)
        .await?
        .context("real materialization missing")?;
    let (hook_version, _) = pending
        .applied()
        .case_after(receipt.case_id)
        .context("real case reconciliation missing")?;
    let after = crate::decision_recovery::finalize_decision_change_tx(
        &mut tx,
        pending
            .finalization
            .phase
            .as_ref()
            .context("real Candidate phase missing")?,
        pending.applied(),
        117,
    )
    .await?;
    assert!(after.version > hook_version);
    sync_after_decision_phase_tx(&mut tx, "g", &f.task, 117).await?;
    let error = validate_applied_model_decision_tx(&mut tx, pending.applied())
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("reconciled_case_after_state_changed"),
        "{error:#}"
    );
    // A separately finalized phase cannot be finished or receipted twice.
    assert!(
        Store::finish_candidate_phase_and_receipt_tx(&mut tx, pending, 117)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM task_decisions WHERE key='observe-candidate'",
        )
        .fetch_one(&mut *tx)
        .await?,
        0
    );
    tx.rollback().await?;
    assert_eq!(continuation_state(&f.store).await?, before);
    Ok(())
}

#[tokio::test]
async fn late_candidate_receipt_failure_rolls_back_real_phase_and_projection() -> Result<()> {
    let (f, request) = materialized_request("candidate-late-failure").await?;
    sqlx::query("CREATE TRIGGER fail_candidate_receipt BEFORE INSERT ON task_decisions WHEN NEW.key='candidate-late-failure' BEGIN SELECT RAISE(ABORT,'forced_candidate_receipt'); END")
        .execute(f.store.pool()).await?;
    let before = continuation_state(&f.store).await?;
    let mut tx = f.store.pool().begin().await?;
    let pending = persist(&mut tx, &f.reviewer, &f.task, request.clone()).await?;
    let receipt = materialized_receipt_tx(&mut tx, "g", &f.task)
        .await?
        .context("real materialization missing")?;
    let (hook_version, _) = pending
        .applied()
        .case_after(receipt.case_id)
        .context("real reconciliation missing")?;
    let error = Store::finish_candidate_phase_and_receipt_tx(&mut tx, pending, 117)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("forced_candidate_receipt"),
        "{error:#}"
    );
    let after = crate::decision_recovery::load_case_tx(&mut tx, "g", receipt.case_id).await?;
    assert!(
        after.version > hook_version,
        "failure must follow the genuine finalizer"
    );
    let graph = load_graph_tx(&mut tx, "g", std::slice::from_ref(&f.task)).await?;
    validate_projection_tx(&mut tx, "g", &graph).await?;
    tx.rollback().await?;
    assert_eq!(continuation_state(&f.store).await?, before);
    let error = f
        .store
        .task_candidate(&f.reviewer, &f.task, request.clone(), 117)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("forced_candidate_receipt"),
        "{error:#}"
    );
    assert_eq!(continuation_state(&f.store).await?, before);
    sqlx::query("DROP TRIGGER fail_candidate_receipt")
        .execute(f.store.pool())
        .await?;
    let actual = f
        .store
        .task_candidate(&f.reviewer, &f.task, request, 117)
        .await?;
    assert_eq!(actual.actor, f.reviewer.name);
    Ok(())
}

#[tokio::test]
async fn saved_pending_candidate_refuses_rolled_back_and_reused_event_ids() -> Result<()> {
    let (f, request) = materialized_request("candidate-saved-proof").await?;
    let before = continuation_state(&f.store).await?;
    let mut tx = f.store.pool().begin().await?;
    let saved = persist(&mut tx, &f.reviewer, &f.task, request.clone()).await?;
    let old_event = saved.applied().root_event().context("real event missing")?;
    let old_operation = saved.applied().operation().to_owned();
    let old_result = saved.result().id.clone();
    tx.rollback().await?;
    assert_eq!(continuation_state(&f.store).await?, before);

    let mut tx = f.store.pool().begin().await?;
    let fresh = persist(&mut tx, &f.reviewer, &f.task, request.clone()).await?;
    assert_eq!(
        fresh.applied().root_event(),
        Some(old_event),
        "SQLite reused the rolled-back event ID"
    );
    assert_ne!(fresh.applied().operation(), old_operation);
    assert_ne!(fresh.result().id, old_result);
    let error = Store::finish_candidate_phase_and_receipt_tx(&mut tx, saved, 117)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("model_after_state_changed"),
        "{error:#}"
    );
    validate_applied_model_decision_tx(&mut tx, fresh.applied()).await?;
    let actual = Store::finish_candidate_phase_and_receipt_tx(&mut tx, fresh, 117).await?;
    tx.commit().await?;
    assert_ne!(actual.id, old_result);

    let committed = continuation_state(&f.store).await?;
    let mut tx = f.store.pool().begin().await?;
    let replay = Store::persist_candidate_with_reconciliation_tx(
        &mut tx,
        &f.reviewer,
        normalize_candidate_request(&f.task, request)?,
        5000,
    )
    .await?;
    let CandidateWrite::Historical(replay) = replay else {
        anyhow::bail!("exact receipt unexpectedly returned fresh authority");
    };
    assert_eq!(serde_json::to_value(replay)?, serde_json::to_value(actual)?);
    tx.rollback().await?;
    assert_eq!(continuation_state(&f.store).await?, committed);
    Ok(())
}

#[tokio::test]
async fn ordinary_pending_candidate_also_requires_retained_actual_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let task = store.task_create(&writer, request("plain"), 100).await?;
    let request = phase_candidate(&store, &writer, &task, "plain-pending").await?;
    let before = continuation_state(&store).await?;
    let mut tx = store.pool().begin().await?;
    let pending = persist(&mut tx, &writer, "plain", request).await?;
    assert!(pending.finalization.phase.is_none());
    validate_applied_model_decision_tx(&mut tx, pending.applied()).await?;
    tx.rollback().await?;
    let mut tx = store.pool().begin().await?;
    let error = Store::finish_candidate_phase_and_receipt_tx(&mut tx, pending, 117)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("actual_model_event_missing"),
        "{error:#}"
    );
    tx.rollback().await?;
    assert_eq!(continuation_state(&store).await?, before);
    Ok(())
}

#[tokio::test]
async fn public_candidate_commit_and_exact_retry_preserve_real_hint_semantics() -> Result<()> {
    for lose_hint in [false, true] {
        let (f, request) = materialized_request("candidate-public-hint").await?;
        let listener = UnixListener::bind(crate::stream::socket(f.store.root()))?;
        let registration = GateRegistration::new(f.store.root(), &f.reviewer, &request.key);
        let store = f.store.clone();
        let actor = f.reviewer.clone();
        let task = f.task.clone();
        let original = request.clone();
        let operation =
            tokio::spawn(async move { store.task_candidate(&actor, &task, original, 117).await });
        let reached =
            tokio::time::timeout(Duration::from_secs(3), registration.gate.reached.notified())
                .await;
        if reached.is_err() {
            operation.abort();
            let _ = operation.await;
            anyhow::bail!("actual Candidate commit gate timed out");
        }
        // A separate connection sees the complete receipt, final phase and
        // validated projection while the actual public operation has sent no hint.
        let mut tx = f.store.pool().begin().await?;
        let canonical = normalize_candidate_request(&f.task, request.clone())?.canonical;
        let committed: TaskResult = replay_tx(&mut tx, &f.reviewer, &request.key, &canonical)
            .await?
            .context("actual committed Candidate receipt missing")?;
        let graph = load_graph_tx(&mut tx, "g", std::slice::from_ref(&f.task)).await?;
        validate_projection_tx(&mut tx, "g", &graph).await?;
        let receipt = materialized_receipt_tx(&mut tx, "g", &f.task)
            .await?
            .context("materialization missing")?;
        let case = crate::decision_recovery::load_case_tx(&mut tx, "g", receipt.case_id).await?;
        assert!(!case.requires_reassessment);
        assert_eq!(
            graph.records[&f.task].model.current_candidate.as_deref(),
            Some(committed.id.as_str())
        );
        tx.rollback().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
        if lose_hint {
            operation.abort();
            assert!(operation.await.is_err_and(|error| error.is_cancelled()));
        } else {
            registration.gate.release.notify_one();
            let result = operation.await??;
            assert_eq!(
                serde_json::to_value(result)?,
                serde_json::to_value(&committed)?
            );
            let (mut stream, _) =
                tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
            let mut bytes = Vec::new();
            tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes)).await??;
            assert_eq!(bytes, b"{\"method\":\"changed\",\"version\":1}\n");
        }
        let before_retry = continuation_state(&f.store).await?;
        // The original version, inputs and request replay after the real deadline.
        let replay = f
            .store
            .task_candidate(&f.reviewer, &f.task, request, 5000)
            .await?;
        assert_eq!(
            serde_json::to_value(replay)?,
            serde_json::to_value(committed)?
        );
        assert_eq!(continuation_state(&f.store).await?, before_retry);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }
    Ok(())
}
