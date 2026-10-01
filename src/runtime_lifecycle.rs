//! Authenticated managed-target lifecycle and immutable operation receipts.
//!
//! Configuration and lifecycle facts do not grant native execution capability.
//! Qualification is a separate collector-owned protocol.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction};

use crate::{
    bounded,
    execution::Checked,
    runtime_adapter::{
        ManagedTargetRegistration, ManagedTargetSpec, ResolvedManagedPolicy,
        register_managed_target_tx, resolve_managed_policy,
    },
    runtime_effects::ContentDigest,
    store::{Mailbox, Store},
};

/// Writer-authorized binding of a task to one controlled text-artifact destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedArtifactBindingRequest {
    /// Stable group-scoped external binding identity.
    pub id: String,
    /// Exact retry key.
    pub key: String,
    /// Writer's retained reason.
    pub reason: String,
    /// Original task identity; corrections cannot change it.
    pub task: String,
    /// Provisioning business-version CAS.
    pub expected_task_version: i64,
    /// Exact registered target identity.
    pub target: String,
    /// Exact immutable target generation.
    pub target_generation: i64,
    /// Controlled group-scoped destination identity.
    pub destination: String,
    /// Literal authorized contract scope.
    pub scope_unit: String,
    /// Complete permitted canonical virtual paths.
    pub allowed_paths: Vec<String>,
    /// Absent creates; present corrects only this binding.
    pub expected_binding_revision: Option<i64>,
}

/// Public finite text-profile limits, measured in UTF-8 bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTextLimits {
    /// Maximum complete native answer.
    pub result_bytes: usize,
    /// Maximum files.
    pub files: usize,
    /// Maximum bytes per file.
    pub file_bytes: usize,
    /// Maximum aggregate file bytes.
    pub artifact_bytes: usize,
    /// Maximum canonical manifest bytes.
    pub manifest_bytes: usize,
}

/// Explicit original admission basis for one genuine unfinished native result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedYieldRequest {
    /// Actual Yield report with original correlation and stable retry key.
    pub report: crate::execution::ExecutionReport,
    /// Business version captured at original admission.
    pub task_version: i64,
    /// Followup version captured at original admission.
    pub checkpoint_version: i64,
    /// Bounded unfinished next step, never an extension request.
    pub next_step: String,
    /// Finite relative interval, further constrained by trusted deadlines.
    pub review_after_seconds: u32,
}

/// Immutable result of the combined report/checkpoint/review transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedYieldReceipt {
    /// Actual report ledger event.
    pub report_event: i64,
    /// Unmodified public snapshot returned by the checkpoint owner.
    pub checkpoint: serde_json::Value,
    /// Trusted original recording time, preserved on replay.
    pub recorded_at: i64,
    /// Actual clamped review time; closure may only make review earlier.
    pub next_check_at: i64,
}

impl Store {
    /// Atomically record a genuine Yield report, task checkpoint and scheduler review.
    /// The clock is sampled under the writer reservation. Exact replay returns the original
    /// whole receipt before any current positive check or new time calculation.
    ///
    /// # Errors
    /// Rejects changed retries, stale original basis and any component/proof failure.
    /// A failure after component writes rolls back the complete transaction.
    pub async fn record_managed_yield(
        &self,
        actor: &Mailbox,
        request: &ManagedYieldRequest,
        now: i64,
    ) -> Result<Checked<ManagedYieldReceipt>> {
        use crate::{execution, followup, runtime_adapter::ManagedRuntimeGate};
        ensure!(
            now >= 0
                && request.task_version > 0
                && request.checkpoint_version >= 0
                && matches!(request.report.kind, execution::ReportKind::Yield)
                && (1..=crate::runtime_effects::MAX_REVIEW_AFTER_SECONDS)
                    .contains(&request.review_after_seconds),
            "invalid_managed_yield_request"
        );
        bounded(&request.next_step, 512, "yield next step")?;
        ensure!(
            !request.next_step.trim().is_empty() && request.report.evidence.len() <= 16,
            "invalid_managed_yield_checkpoint"
        );
        let canonical = serde_json::to_string(&(actor.binding_version, request))?;
        bounded(&canonical, 65536, "managed yield request")?;
        let c = &request.report.correlation;
        let mut tx = self.pool().begin().await?;
        authenticate_producer_tx(&mut tx, actor, c).await?;
        let prior: Option<(i64, i64, String, String)> = sqlx::query_as("SELECT actor,actor_binding,canonical_request,receipt FROM runtime_yield_receipts WHERE attempt=? AND retry_key=?")
            .bind(&c.attempt).bind(&request.report.key).fetch_optional(&mut *tx).await?;
        if let Some((original_actor, binding, old, receipt)) = prior {
            ensure!(
                original_actor == actor.id && binding == actor.binding_version && old == canonical,
                "managed_yield_retry_conflict"
            );
            bounded(&receipt, 262144, "stored managed yield receipt")?;
            let receipt = serde_json::from_str(&receipt)?;
            tx.commit().await?;
            return Ok(Checked::Ready(receipt));
        }
        let admitted = admitted_artifact_tx(&mut tx, c).await?;
        ensure!(
            request.task_version == admitted.checkpoint.task_version
                && request.checkpoint_version == admitted.checkpoint.version,
            "managed_yield_original_basis_conflict"
        );
        let recorded_at = crate::now()?;
        ensure!(now <= recorded_at, "managed_yield_clock_before_call");
        let plan = match execution::prepare_yield_review_tx(
            &mut tx,
            &ManagedRuntimeGate,
            actor,
            &request.report,
            &admitted.checkpoint,
            request.review_after_seconds,
            recorded_at,
        )
        .await?
        {
            Checked::Ready(plan) => plan,
            Checked::Held(holds) => {
                tx.commit().await?;
                return Ok(Checked::Held(holds));
            }
        };
        let Checked::Ready(report_event) = execution::record_report_tx(
            &mut tx,
            &ManagedRuntimeGate,
            actor,
            &request.report,
            plan.recorded_at(),
        )
        .await?
        else {
            anyhow::bail!("managed_yield_report_held; rollback required");
        };
        let checkpoint_key = format!(
            "managed-yield:{}",
            ContentDigest::of_bytes(canonical.as_bytes()).as_str()
        );
        let checkpoint = followup::checkpoint_tx(
            &mut tx,
            actor,
            followup::Source::Task {
                id: c.task.clone(),
                version: admitted.checkpoint.task_version,
            },
            &checkpoint_key,
            followup::Checkpoint {
                version: admitted.checkpoint.version,
                next_step: request.next_step.clone(),
                next_check_at: plan.review_at(),
                waiting: None,
                evidence: request.report.evidence.clone(),
                extend_until: None,
                reason: None,
            },
            plan.recorded_at(),
        )
        .await?;
        ensure!(
            !checkpoint.is_replay(),
            "managed_yield_requires_fresh_combined_checkpoint"
        );
        let review = execution::record_yield_review_tx(
            &mut tx,
            &ManagedRuntimeGate,
            actor,
            &plan,
            report_event,
            &checkpoint,
            crate::now()?,
        )
        .await?;
        let receipt = ManagedYieldReceipt {
            report_event: review.report_event,
            checkpoint: checkpoint.into_snapshot(),
            recorded_at: review.recorded_at,
            next_check_at: review.review_at,
        };
        let encoded = serde_json::to_string(&receipt)?;
        bounded(&encoded, 262144, "managed yield receipt")?;
        sqlx::query("INSERT INTO runtime_yield_receipts(attempt,retry_key,actor,actor_binding,canonical_request,receipt,created) VALUES(?,?,?,?,?,?,?)")
            .bind(&c.attempt).bind(&request.report.key).bind(actor.id).bind(actor.binding_version)
            .bind(canonical).bind(encoded).bind(receipt.recorded_at).execute(&mut *tx).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(Checked::Ready(receipt))
    }
}

impl Default for ManagedTextLimits {
    fn default() -> Self {
        use crate::runtime_effects::*;
        Self {
            result_bytes: TEXT_RESULT_LIMIT,
            files: TEXT_FILE_LIMIT,
            file_bytes: TEXT_FILE_BYTES,
            artifact_bytes: TEXT_ARTIFACT_BYTES,
            manifest_bytes: TEXT_MANIFEST_BYTES,
        }
    }
}

/// Current binding projection, or the immutable original result of keyed provisioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedArtifactBindingView {
    /// Stable external binding identity.
    pub id: String,
    /// Immutable binding revision.
    pub revision: i64,
    /// Original task.
    pub task: String,
    /// Domain-separated digest of the complete normalized contract.
    pub contract_digest: ContentDigest,
    /// Registered target identity.
    pub target: String,
    /// Original target generation.
    pub target_generation: i64,
    /// Protected destination identity.
    pub destination: String,
    /// Observed destination CAS generation.
    pub destination_generation: i64,
    /// Observed immutable selection, including initial absence.
    pub selected_manifest: Option<ContentDigest>,
    /// Literal authorized scope.
    pub scope_unit: String,
    /// Complete allowed virtual paths.
    pub allowed_paths: Vec<String>,
    /// Finite text producer bounds.
    pub limits: ManagedTextLimits,
    /// Original binding revision creation time.
    pub created: i64,
}

fn artifact_identifier(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
        "invalid_artifact_identifier"
    );
    Ok(())
}

/// Original protected admission facts, never accepted from a public/native caller.
pub(crate) struct AdmittedArtifact {
    pub(crate) binding_id: String,
    pub(crate) destination: String,
    pub(crate) destination_generation: i64,
    pub(crate) destination_manifest: Option<ContentDigest>,
    pub(crate) scope_unit: String,
    pub(crate) allowed_paths: Vec<String>,
    pub(crate) authority: crate::task_graph::ArtifactBindingProvenance,
    pub(crate) checkpoint: crate::followup::CheckpointTaskBasis,
    pub(crate) specification: ManagedTargetSpec,
}

pub(crate) async fn authenticate_producer_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    c: &crate::execution::Correlation,
) -> Result<()> {
    authenticate_owner_tx(tx, actor).await?;
    let original: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts a JOIN runtime_segments s ON s.attempt=a.id WHERE a.id=? AND a.group_name=? AND a.task=? AND a.fence=? AND a.dispatch_key=? AND a.owner=? AND a.owner_binding=?)")
        .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
        .bind(&actor.name).bind(actor.binding_version).fetch_one(&mut **tx).await?;
    ensure!(
        actor.group_name == c.group && original,
        "managed_original_producer_conflict"
    );
    Ok(())
}

/// Called only in the actual admission transaction, after scheduler admission.
pub(crate) async fn capture_artifact_admission_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    c: &crate::execution::Correlation,
    now: i64,
) -> Result<Option<AdmittedArtifact>> {
    authenticate_producer_tx(tx, actor, c).await?;
    let (target, generation, profile): (String, i64, String) = sqlx::query_as("SELECT s.target,s.target_generation,v.profile FROM runtime_segments s JOIN runtime_target_versions v ON v.target=s.target AND v.generation=s.target_generation WHERE s.attempt=? AND s.launch_committed=1 AND s.state='running' AND s.tombstoned=0")
        .bind(&c.attempt).fetch_one(&mut **tx).await?;
    if profile == "read_only" {
        return Ok(None);
    }
    ensure!(
        profile == "staged_files",
        "unsupported_managed_artifact_profile"
    );
    let candidates: Vec<String> = sqlx::query_scalar("SELECT b.id FROM runtime_artifact_bindings b JOIN runtime_artifact_binding_versions v ON v.identity=b.identity AND v.revision=b.revision WHERE b.group_name=? AND b.task=? AND v.target=? AND v.target_generation=? ORDER BY b.id LIMIT 2")
        .bind(&c.group).bind(&c.task).bind(target).bind(generation).fetch_all(&mut **tx).await?;
    ensure!(
        candidates.len() == 1,
        "managed_artifact_binding_missing_or_ambiguous"
    );
    let binding = binding_row_tx(tx, &c.group, &candidates[0])
        .await?
        .context("managed_artifact_binding_missing")?;
    bounded(&binding.authority, 262144, "stored artifact provenance")?;
    let provenance: crate::task_graph::ArtifactBindingProvenance =
        serde_json::from_str(&binding.authority)?;
    let inputs: String =
        sqlx::query_scalar("SELECT inputs FROM execution_attempts WHERE id=? AND admitted=1")
            .bind(&c.attempt)
            .fetch_one(&mut **tx)
            .await?;
    let inputs: crate::task_graph::InputSnapshot = serde_json::from_str(&inputs)?;
    crate::task_graph::validate_artifact_binding_current_tx(
        tx,
        &provenance,
        &inputs,
        &binding.scope_unit,
        now,
    )
    .await?;
    let basis = crate::followup::checkpoint_task_basis_tx(tx, actor, &c.task).await?;
    let bytes = serde_json::to_string(&basis)?;
    sqlx::query("INSERT INTO runtime_artifact_admissions(attempt,binding,binding_revision,destination_generation,destination_manifest,task_version,followup_version,basis,created) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(&c.attempt).bind(&binding.identity).bind(binding.revision).bind(binding.generation)
        .bind(&binding.manifest).bind(basis.task_version).bind(basis.version).bind(bytes).bind(now)
        .execute(&mut **tx).await?;
    Ok(Some(admitted_artifact_tx(tx, c).await?))
}

/// Resolve only the original immutable admission/binding revision, never a newer basis.
pub(crate) async fn admitted_artifact_tx(
    tx: &mut Transaction<'_, Sqlite>,
    c: &crate::execution::Correlation,
) -> Result<AdmittedArtifact> {
    #[derive(sqlx::FromRow)]
    struct Row {
        id: String,
        destination: String,
        destination_generation: i64,
        destination_manifest: Option<String>,
        scope_unit: String,
        allowed_paths: String,
        authority: String,
        basis: String,
        specification: String,
        task_version: i64,
        followup_version: i64,
    }
    let row: Row = sqlx::query_as("SELECT b.id,v.destination,a.destination_generation,a.destination_manifest,v.scope_unit,v.allowed_paths,v.authority,a.basis,t.specification,a.task_version,a.followup_version FROM runtime_artifact_admissions a JOIN runtime_segments s ON s.attempt=a.attempt JOIN runtime_artifact_bindings b ON b.identity=a.binding AND b.group_name=s.group_name AND b.task=s.task JOIN runtime_artifact_binding_versions v ON v.identity=a.binding AND v.revision=a.binding_revision AND v.target=s.target AND v.target_generation=s.target_generation JOIN runtime_target_versions t ON t.target=s.target AND t.generation=s.target_generation WHERE s.attempt=? AND s.group_name=? AND s.task=? AND s.fence=? AND s.dispatch_key=?")
        .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
        .fetch_one(&mut **tx).await?;
    bounded(&row.authority, 262144, "stored artifact provenance")?;
    bounded(&row.basis, 262144, "stored checkpoint basis")?;
    bounded(&row.allowed_paths, 8192, "stored artifact paths")?;
    bounded(&row.specification, 16384, "stored target specification")?;
    let checkpoint: crate::followup::CheckpointTaskBasis = serde_json::from_str(&row.basis)?;
    let authority: crate::task_graph::ArtifactBindingProvenance =
        serde_json::from_str(&row.authority)?;
    ensure!(
        checkpoint.task_version == row.task_version
            && checkpoint.version == row.followup_version
            && checkpoint.group_name == c.group
            && checkpoint.task == c.task
            && authority.group() == c.group
            && authority.task() == c.task
            && authority.scope_unit() == row.scope_unit,
        "protected_artifact_admission_conflict"
    );
    Ok(AdmittedArtifact {
        binding_id: row.id,
        destination: row.destination,
        destination_generation: row.destination_generation,
        destination_manifest: row
            .destination_manifest
            .map(ContentDigest::parse)
            .transpose()?,
        scope_unit: row.scope_unit,
        allowed_paths: normalized_paths(&serde_json::from_str::<Vec<String>>(&row.allowed_paths)?)?,
        authority,
        checkpoint,
        specification: serde_json::from_str(&row.specification)?,
    })
}

fn normalized_paths(paths: &[String]) -> Result<Vec<String>> {
    use crate::runtime_effects::{ArtifactFile, ArtifactManifest, TEXT_FILE_LIMIT};
    ensure!(
        !paths.is_empty() && paths.len() <= TEXT_FILE_LIMIT,
        "invalid_artifact_path_count"
    );
    let mut manifest = ArtifactManifest {
        version: 1,
        files: Vec::new(),
    };
    for path in paths {
        bounded(path, 256, "artifact path")?;
        manifest.files.push(ArtifactFile {
            path: path.clone(),
            digest: ContentDigest::of_bytes(b""),
            bytes: 0,
        });
    }
    let normalized = ArtifactManifest::decode(&manifest.canonical_bytes()?)?;
    Ok(normalized.files.into_iter().map(|file| file.path).collect())
}

#[derive(sqlx::FromRow)]
struct BindingRow {
    identity: String,
    id: String,
    task: String,
    revision: i64,
    target: String,
    target_generation: i64,
    destination: String,
    generation: i64,
    manifest: Option<String>,
    scope_unit: String,
    allowed_paths: String,
    authority: String,
    created: i64,
}

impl BindingRow {
    fn view(&self) -> Result<ManagedArtifactBindingView> {
        bounded(&self.authority, 262144, "stored artifact authority")?;
        bounded(&self.allowed_paths, 8192, "stored artifact paths")?;
        let provenance: crate::task_graph::ArtifactBindingProvenance =
            serde_json::from_str(&self.authority)?;
        Ok(ManagedArtifactBindingView {
            id: self.id.clone(),
            revision: self.revision,
            task: self.task.clone(),
            contract_digest: ContentDigest::parse(provenance.contract_digest().into())?,
            target: self.target.clone(),
            target_generation: self.target_generation,
            destination: self.destination.clone(),
            destination_generation: self.generation,
            selected_manifest: self
                .manifest
                .clone()
                .map(ContentDigest::parse)
                .transpose()?,
            scope_unit: self.scope_unit.clone(),
            allowed_paths: normalized_paths(&serde_json::from_str::<Vec<String>>(
                &self.allowed_paths,
            )?)?,
            limits: ManagedTextLimits::default(),
            created: self.created,
        })
    }
}

async fn binding_row_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    id: &str,
) -> Result<Option<BindingRow>> {
    Ok(sqlx::query_as("SELECT b.identity,b.id,b.task,b.revision,v.target,v.target_generation,v.destination,d.generation,d.manifest,v.scope_unit,v.allowed_paths,v.authority,v.created FROM runtime_artifact_bindings b JOIN runtime_artifact_binding_versions v ON v.identity=b.identity AND v.revision=b.revision JOIN runtime_destinations d ON d.id=v.destination WHERE b.group_name=? AND b.id=?")
        .bind(group).bind(id).fetch_optional(&mut **tx).await?)
}

impl Store {
    /// Provision a binding using the actual designated writer's model proof.
    ///
    /// # Errors
    /// Rejects authority/CAS conflicts, affected held work, cross-task destinations and malformed paths.
    pub async fn bind_managed_artifact(
        &self,
        actor: &Mailbox,
        request: &ManagedArtifactBindingRequest,
        now: i64,
    ) -> Result<ManagedArtifactBindingView> {
        validate_operation(&request.key, &request.reason, now)?;
        artifact_identifier(&request.id)?;
        artifact_identifier(&request.destination)?;
        crate::name(&request.task)?;
        bounded(&request.target, 128, "artifact target")?;
        ensure!(
            !request.target.is_empty()
                && request.expected_task_version > 0
                && request.target_generation > 0
                && request
                    .expected_binding_revision
                    .is_none_or(|revision| revision > 0),
            "invalid_artifact_binding_versions"
        );
        let mut request = request.clone();
        request.allowed_paths = normalized_paths(&request.allowed_paths)?;
        let canonical = serde_json::to_string(&(actor.binding_version, &request))?;
        bounded(&canonical, 16384, "artifact binding request")?;
        let mut tx = self.pool().begin().await?;
        authenticate_owner_tx(&mut tx, actor).await?;
        let prior: Option<(i64, String, String)> = sqlx::query_as("SELECT actor_binding,canonical_request,receipt FROM runtime_binding_receipts WHERE group_name=? AND actor=? AND retry_key=?")
            .bind(&actor.group_name).bind(actor.id).bind(&request.key).fetch_optional(&mut *tx).await?;
        if let Some((binding, old, receipt)) = prior {
            ensure!(
                binding == actor.binding_version && old == canonical,
                "artifact_binding_retry_conflict"
            );
            bounded(&receipt, 16384, "stored binding receipt")?;
            let view = serde_json::from_str(&receipt)?;
            tx.commit().await?;
            return Ok(view);
        }
        let proof = crate::task_graph::validate_artifact_binding_tx(
            &mut tx,
            actor,
            &request.task,
            request.expected_task_version,
            &request.scope_unit,
            now,
        )
        .await?;
        let provenance = proof.into_provenance();
        let old = binding_row_tx(&mut tx, &actor.group_name, &request.id).await?;
        let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1)")
            .bind(&actor.group_name).bind(&request.task).fetch_one(&mut *tx).await?;
        ensure!(!held, "artifact_binding_has_held_attempts");
        let (identity, revision) = if let Some(old) = old {
            ensure!(
                old.task == request.task && request.expected_binding_revision == Some(old.revision),
                "artifact_binding_identity_or_revision_conflict"
            );
            let outstanding: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_effects WHERE destination=? AND state NOT IN ('published','abandoned'))")
                .bind(&old.destination).fetch_one(&mut *tx).await?;
            ensure!(!outstanding, "artifact_destination_has_unresolved_effects");
            (
                old.identity,
                old.revision
                    .checked_add(1)
                    .context("artifact_binding_revision_overflow")?,
            )
        } else {
            ensure!(
                request.expected_binding_revision.is_none(),
                "artifact_binding_missing"
            );
            (uuid::Uuid::new_v4().to_string(), 1)
        };
        let target_valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_targets t JOIN runtime_target_lifecycle l ON l.target=t.id JOIN runtime_target_versions v ON v.target=t.id AND v.generation=t.current_generation JOIN work_items w ON w.group_name=t.group_name AND w.id=? JOIN mailboxes m ON m.group_name=t.group_name AND m.name=v.owner AND m.binding_version=v.owner_binding WHERE t.id=? AND t.group_name=? AND t.current_generation=? AND l.retired=0 AND v.profile='staged_files' AND w.owner=v.owner AND m.remote_machine IS NULL AND m.agent_state='registered')")
            .bind(&request.task).bind(&request.target).bind(&actor.group_name).bind(request.target_generation).fetch_one(&mut *tx).await?;
        ensure!(target_valid, "artifact_target_owner_or_generation_conflict");
        let destination: Option<(String, i64, Option<String>)> = sqlx::query_as("SELECT d.target,d.target_generation,o.task FROM runtime_destinations d LEFT JOIN runtime_destination_owners o ON o.destination=d.id AND o.group_name=d.group_name WHERE d.id=? AND d.group_name=?")
            .bind(&request.destination).bind(&actor.group_name).fetch_optional(&mut *tx).await?;
        if let Some((target, generation, task)) = destination {
            ensure!(
                target == request.target
                    && generation == request.target_generation
                    && task.as_deref() == Some(request.task.as_str()),
                "artifact_destination_owner_conflict"
            );
            let outstanding: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_effects WHERE destination=? AND state NOT IN ('published','abandoned'))")
                .bind(&request.destination).fetch_one(&mut *tx).await?;
            ensure!(!outstanding, "artifact_destination_has_unresolved_effects");
        } else {
            sqlx::query("INSERT INTO runtime_destinations(id,group_name,target,target_generation) VALUES(?,?,?,?)")
                .bind(&request.destination).bind(&actor.group_name).bind(&request.target).bind(request.target_generation).execute(&mut *tx).await?;
            sqlx::query(
                "INSERT INTO runtime_destination_owners(destination,group_name,task) VALUES(?,?,?)",
            )
            .bind(&request.destination)
            .bind(&actor.group_name)
            .bind(&request.task)
            .execute(&mut *tx)
            .await?;
        }
        if revision == 1 {
            sqlx::query("INSERT INTO runtime_artifact_bindings(identity,group_name,id,task,revision) VALUES(?,?,?,?,1)")
                .bind(&identity).bind(&actor.group_name).bind(&request.id).bind(&request.task).execute(&mut *tx).await?;
        } else {
            sqlx::query(
                "UPDATE runtime_artifact_bindings SET revision=? WHERE identity=? AND revision=?",
            )
            .bind(revision)
            .bind(&identity)
            .bind(revision - 1)
            .execute(&mut *tx)
            .await?;
        }
        let authority = serde_json::to_string(&provenance)?;
        bounded(&authority, 262144, "artifact provenance")?;
        sqlx::query("INSERT INTO runtime_artifact_binding_versions(identity,revision,target,target_generation,destination,scope_unit,allowed_paths,authority,created) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(&identity).bind(revision).bind(&request.target).bind(request.target_generation)
            .bind(&request.destination).bind(&request.scope_unit).bind(serde_json::to_string(&request.allowed_paths)?)
            .bind(authority).bind(now).execute(&mut *tx).await?;
        let view = binding_row_tx(&mut tx, &actor.group_name, &request.id)
            .await?
            .context("artifact_binding_missing_after_write")?
            .view()?;
        sqlx::query("INSERT INTO runtime_binding_receipts(id,group_name,actor,actor_binding,retry_key,canonical_request,binding,receipt,created) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(uuid::Uuid::new_v4().to_string()).bind(&actor.group_name).bind(actor.id).bind(actor.binding_version)
            .bind(&request.key).bind(canonical).bind(&identity).bind(serde_json::to_string(&view)?).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(view)
    }

    /// Read an exact binding in the authenticated home group without changing it.
    ///
    /// # Errors
    /// Rejects invalid actors/selectors and corrupt retained data.
    pub async fn managed_artifact_binding(
        &self,
        actor: &Mailbox,
        id: &str,
        now: i64,
    ) -> Result<Option<ManagedArtifactBindingView>> {
        artifact_identifier(id)?;
        ensure!(now >= 0, "invalid_artifact_observation_time");
        let mut tx = self.pool().begin().await?;
        Store::check_actor(&mut tx, actor).await?;
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mailboxes m JOIN groups g ON g.name=m.group_name WHERE m.id=? AND m.name=? AND m.group_name=? AND g.home_machine=(SELECT id FROM node))")
            .bind(actor.id).bind(&actor.name).bind(&actor.group_name).fetch_one(&mut *tx).await?;
        ensure!(valid, "artifact_read_requires_authenticated_home_actor");
        let view = binding_row_tx(&mut tx, &actor.group_name, id)
            .await?
            .map(|row| row.view())
            .transpose()?;
        tx.commit().await?;
        Ok(view)
    }
}

/// A keyed configuration request. Creation omits both expected versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTargetConfiguration {
    /// Stable caller retry identity.
    pub key: String,
    /// Operator's bounded reason, retained exactly.
    pub reason: String,
    /// Complete target specification.
    pub spec: ManagedTargetSpec,
    /// Current generation required for correction.
    pub expected_generation: Option<i64>,
    /// Current lifecycle revision required for correction.
    pub expected_revision: Option<i64>,
}

/// A lifecycle operation with explicit current-state expectations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTargetChange {
    /// Stable caller retry identity.
    pub key: String,
    /// Operator's bounded reason.
    pub reason: String,
    /// Exact registered target identity.
    pub target: String,
    /// Expected immutable configuration generation.
    pub expected_generation: i64,
    /// Expected current lifecycle revision.
    pub expected_revision: i64,
    /// Requested operation; no action accepts caller-supplied witnesses.
    pub action: ManagedTargetAction,
}

/// Public lifecycle operations; qualification references confer no authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedTargetAction {
    /// Stop new positive admission while preserving original cleanup obligations.
    Disable,
    /// Permanently archive a target after all affected attempts and effects close.
    Retire,
    /// Require an actual protected qualification from the separate collector.
    Enable {
        /// Exact protected qualification identity, never inline evidence.
        qualification: String,
    },
}

/// Historical operation result. Read the capability view for current state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTargetLifecycleReceipt {
    /// Immutable receipt identity.
    pub id: String,
    /// Exact target identity.
    pub target: String,
    /// Generation at the original operation.
    pub generation: i64,
    /// Lifecycle revision at the original operation.
    pub revision: i64,
    /// Admission switch at the original operation.
    pub enabled: bool,
    /// Archive state at the original operation.
    pub retired: bool,
    /// Operation name, including explicit legacy registration audit.
    pub operation: String,
    /// Original trusted recording time.
    pub created: i64,
}

pub(crate) async fn authenticate_owner_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
) -> Result<()> {
    Store::lock_actor(tx, actor).await?;
    let actual: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mailboxes m JOIN groups g ON g.name=m.group_name WHERE m.id=? AND m.group_name=? AND m.name=? AND m.binding_version=? AND m.agent_state='registered' AND m.remote_machine IS NULL AND g.home_machine=(SELECT id FROM node))")
        .bind(actor.id).bind(&actor.group_name).bind(&actor.name).bind(actor.binding_version)
        .fetch_one(&mut **tx).await?;
    ensure!(
        actual && !matches!(actor.binding, crate::identity::Binding::Remote { .. }),
        "managed lifecycle requires an authenticated home-local actor"
    );
    Ok(())
}

fn validate_operation(key: &str, reason: &str, now: i64) -> Result<()> {
    bounded(key, 128, "runtime operation key")?;
    bounded(reason, 4096, "runtime operation reason")?;
    ensure!(
        !key.is_empty() && !key.chars().any(char::is_control),
        "invalid_runtime_operation_key"
    );
    ensure!(
        !reason.trim().is_empty(),
        "runtime_operation_reason_required"
    );
    ensure!(now >= 0, "invalid_runtime_operation_time");
    Ok(())
}

async fn replay_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    key: &str,
    canonical: &str,
) -> Result<Option<ManagedTargetLifecycleReceipt>> {
    let old: Option<(i64, String, String)> = sqlx::query_as("SELECT actor_binding,canonical_request,receipt FROM runtime_lifecycle_receipts WHERE group_name=? AND actor=? AND retry_key=?")
        .bind(&actor.group_name).bind(actor.id).bind(key).fetch_optional(&mut **tx).await?;
    old.map(|(binding, request, receipt)| {
        ensure!(
            binding == actor.binding_version && request == canonical,
            "runtime_operation_key_conflict"
        );
        bounded(&receipt, 16384, "stored runtime lifecycle receipt")?;
        Ok(serde_json::from_str(&receipt)?)
    })
    .transpose()
}

async fn receipt_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    key: &str,
    canonical: &str,
    target: &str,
    operation: &str,
    now: i64,
) -> Result<ManagedTargetLifecycleReceipt> {
    let (generation, enabled, revision, retired): (i64, bool, i64, bool) = sqlx::query_as("SELECT t.current_generation,t.enabled,l.revision,l.retired FROM runtime_targets t JOIN runtime_target_lifecycle l ON l.target=t.id WHERE t.id=? AND t.group_name=?")
        .bind(target).bind(&actor.group_name).fetch_one(&mut **tx).await?;
    let receipt = ManagedTargetLifecycleReceipt {
        id: uuid::Uuid::new_v4().to_string(),
        target: target.into(),
        generation,
        revision,
        enabled,
        retired,
        operation: operation.into(),
        created: now,
    };
    sqlx::query("INSERT INTO runtime_lifecycle_receipts(id,group_name,actor,actor_binding,retry_key,canonical_request,target,receipt,created) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(&receipt.id).bind(&actor.group_name).bind(actor.id).bind(actor.binding_version)
        .bind(key).bind(canonical).bind(target).bind(serde_json::to_string(&receipt)?).bind(now)
        .execute(&mut **tx).await?;
    Ok(receipt)
}

pub(crate) async fn record_legacy_registration_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    spec: &ManagedTargetSpec,
    expected_generation: Option<i64>,
    policy: &ResolvedManagedPolicy,
    registration: &ManagedTargetRegistration,
    now: i64,
) -> Result<()> {
    // The legacy caller supplied no reason or retry key. Record only actual input;
    // deduplicate its canonical call without converting its result to keyed replay.
    let canonical = serde_json::to_string(&(
        "legacy_registration",
        actor.binding_version,
        spec,
        expected_generation,
        &policy.policy_digest,
        &policy.executable_digest,
    ))?;
    let key = format!(
        "legacy:{}",
        ContentDigest::of_bytes(canonical.as_bytes()).as_str()
    );
    if replay_tx(tx, actor, &key, &canonical).await?.is_none() {
        receipt_tx(
            tx,
            actor,
            &key,
            &canonical,
            &registration.identity,
            "legacy_registration",
            now,
        )
        .await?;
    }
    Ok(())
}

impl Store {
    /// Configure a target and record its keyed receipt in one writer transaction.
    ///
    /// # Errors
    /// Rejects changed retry bytes, unauthorized actors, stale versions and held attempts.
    pub async fn configure_managed_target(
        &self,
        actor: &Mailbox,
        request: &ManagedTargetConfiguration,
        now: i64,
    ) -> Result<ManagedTargetLifecycleReceipt> {
        validate_operation(&request.key, &request.reason, now)?;
        request.spec.validate()?;
        ensure!(
            request.spec.owner == actor.name,
            "managed target belongs to another owner"
        );
        ensure!(
            matches!(
                (request.expected_generation, request.expected_revision),
                (None, None) | (Some(1..), Some(1..))
            ),
            "both configuration versions are required"
        );
        let key = format!("key:{}", request.key);
        let canonical = serde_json::to_string(&("configure", actor.binding_version, request))?;
        // A historical result does not depend on the continued availability of policy files.
        let mut tx = self.pool().begin().await?;
        authenticate_owner_tx(&mut tx, actor).await?;
        if let Some(receipt) = replay_tx(&mut tx, actor, &key, &canonical).await? {
            tx.commit().await?;
            return Ok(receipt);
        }
        tx.rollback().await?;
        let policy = resolve_managed_policy(self.root(), &request.spec)?;
        let mut tx = self.pool().begin().await?;
        authenticate_owner_tx(&mut tx, actor).await?;
        if let Some(receipt) = replay_tx(&mut tx, actor, &key, &canonical).await? {
            tx.commit().await?;
            return Ok(receipt);
        }
        let current: Option<(i64, i64, bool)> = sqlx::query_as("SELECT t.current_generation,l.revision,l.retired FROM runtime_targets t JOIN runtime_target_lifecycle l ON l.target=t.id WHERE t.group_name=? AND t.name=?")
            .bind(&actor.group_name).bind(&request.spec.target).fetch_optional(&mut *tx).await?;
        match current {
            Some((generation, revision, retired)) => {
                ensure!(!retired, "managed_target_retired");
                ensure!(
                    request.expected_generation == Some(generation)
                        && request.expected_revision == Some(revision),
                    "managed_target_version_conflict"
                );
            }
            None => ensure!(
                request.expected_generation.is_none(),
                "managed_target_missing"
            ),
        }
        let registered = register_managed_target_tx(
            &mut tx,
            actor,
            &request.spec,
            request.expected_generation,
            &policy,
            now,
        )
        .await?;
        let receipt = receipt_tx(
            &mut tx,
            actor,
            &key,
            &canonical,
            &registered.identity,
            "configure",
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(receipt)
    }

    /// Apply an explicit target lifecycle change without deleting historical facts.
    ///
    /// # Errors
    /// Rejects unauthorized actors, changed retries, stale versions and unsafe retirement.
    pub async fn change_managed_target(
        &self,
        actor: &Mailbox,
        request: &ManagedTargetChange,
        now: i64,
    ) -> Result<Checked<ManagedTargetLifecycleReceipt>> {
        validate_operation(&request.key, &request.reason, now)?;
        bounded(&request.target, 128, "managed target")?;
        ensure!(
            !request.target.is_empty()
                && request.expected_generation > 0
                && request.expected_revision > 0,
            "invalid_managed_target_change"
        );
        if let ManagedTargetAction::Enable { qualification } = &request.action {
            bounded(qualification, 128, "qualification identity")?;
            ensure!(
                !qualification.is_empty() && !qualification.chars().any(char::is_control),
                "invalid_qualification_identity"
            );
        }
        let key = format!("key:{}", request.key);
        let canonical = serde_json::to_string(&("change", actor.binding_version, request))?;
        let mut tx = self.pool().begin().await?;
        authenticate_owner_tx(&mut tx, actor).await?;
        if let Some(receipt) = replay_tx(&mut tx, actor, &key, &canonical).await? {
            tx.commit().await?;
            return Ok(Checked::Ready(receipt));
        }
        let (generation, enabled, revision, retired, owner, binding): (i64, bool, i64, bool, String, i64) = sqlx::query_as("SELECT t.current_generation,t.enabled,l.revision,l.retired,v.owner,v.owner_binding FROM runtime_targets t JOIN runtime_target_lifecycle l ON l.target=t.id JOIN runtime_target_versions v ON v.target=t.id AND v.generation=t.current_generation WHERE t.id=? AND t.group_name=?")
            .bind(&request.target).bind(&actor.group_name).fetch_optional(&mut *tx).await?.context("managed_target_missing")?;
        ensure!(
            owner == actor.name && binding == actor.binding_version,
            "managed_target_owner_conflict"
        );
        ensure!(
            generation == request.expected_generation && revision == request.expected_revision,
            "managed_target_version_conflict"
        );
        ensure!(!retired, "managed_target_retired");
        let operation = match request.action {
            ManagedTargetAction::Enable { .. } => {
                // Schema27 intentionally has no collector, permit or qualifying witness writer.
                // An old deserializable witness or a supplied ID cannot enable this target.
                tx.commit().await?;
                return Ok(Checked::Held(vec![
                    "managed_native_qualification_unavailable".into(),
                ]));
            }
            ManagedTargetAction::Disable => "disable",
            ManagedTargetAction::Retire => {
                let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE group_name=? AND holds_slot=1 AND json_extract(runtime,'$.identity')=?) OR EXISTS(SELECT 1 FROM runtime_effects e JOIN runtime_segments s ON s.attempt=e.attempt WHERE s.target=? AND e.state NOT IN ('published','abandoned'))")
                    .bind(&actor.group_name).bind(&request.target).bind(&request.target).fetch_one(&mut *tx).await?;
                ensure!(!held, "managed_target_has_unresolved_work");
                "retire"
            }
        };
        sqlx::query("UPDATE runtime_targets SET enabled=0 WHERE id=?")
            .bind(&request.target)
            .execute(&mut *tx)
            .await?;
        if operation == "retire" {
            // The target trigger already advanced a previously enabled target once.
            sqlx::query(
                "UPDATE runtime_target_lifecycle SET retired=1,revision=revision+? WHERE target=?",
            )
            .bind(i64::from(!enabled))
            .bind(&request.target)
            .execute(&mut *tx)
            .await?;
        }
        let receipt = receipt_tx(
            &mut tx,
            actor,
            &key,
            &canonical,
            &request.target,
            operation,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(Checked::Ready(receipt))
    }
}
