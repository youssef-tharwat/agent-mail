//! Finite local contracts and immutable result inputs under one SQLite writer reservation.
//!
//! Model readiness is only the business predicate. It never grants execution:
//! scheduler policy, attempts, closure and runtime containment remain separate
//! authorities. Model changes compose real lifecycle and recovery guards in one
//! transaction; no model observation is a runtime admission or effect receipt.
//! Legacy records and relay shapes remain unchanged until explicit adoption.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    bounded, name,
    states::TaskState,
    store::{Mailbox, Store},
    work::{WorkDraft, WorkItem, WorkPatch},
};

const MAX_TASKS: usize = 1_000;
const MAX_EDGES: usize = 10_000;
const MAX_REQUEST: usize = 64 * 1024;
// Complete scheduler/recovery source inventories have been integrated for
// these exact schemas. A later migration requires another owner review.
const INTEGRATED_OWNER_SCHEMAS: [i64; 8] = [25, 26, 27, 28, 29, 30, 31, 32];

/// One observable acceptance condition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Criterion {
    /// Stable identifier within this contract.
    pub id: String,
    /// What the writer will verify.
    pub description: String,
}

/// Required successful disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completion {
    /// Only explicit writer acceptance is successful.
    WriterAcceptance,
    /// The writer may record completed work without acceptance.
    CompletionAllowed,
}

/// Finite limits; the scheduler owns all usage and elapsed anchors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// Positive lifetime attempt ceiling.
    pub max_attempts: u32,
    /// Positive elapsed allowance from first business eligibility.
    pub max_elapsed_seconds: u64,
    /// Optional exact cost in the declared integer unit.
    pub max_cost: Option<CostLimit>,
}

/// Cost never uses floating point or treats unknown usage as zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostLimit {
    /// Positive integer amount in the named smallest unit.
    pub amount: u64,
    /// Explicit unit, interpreted by the scheduler and adapter.
    pub unit: String,
}

/// Complete finite task contract, independent of delivery capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Concrete finite deliverable.
    pub deliverable: String,
    /// Nonempty individually identified conditions.
    pub criteria: Vec<Criterion>,
    /// Literal agreed authority units; these are not path sandbox rules.
    pub allowed_scope: Vec<String>,
    /// Successful disposition policy.
    pub completion: Completion,
    /// Whether children may inherit an exact scope subset.
    pub allow_delegation: bool,
    /// Audited consent to negative invalidation; required for contracted work.
    pub allow_input_invalidation: bool,
    /// Scheduler-owned lifetime limits.
    pub budget: Budget,
}

/// Current scope authority; only the designated writer may grant or restore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityState {
    /// Explicitly authorized within the recorded scope.
    Authorized,
    /// A responsible decision is required before execution.
    Held,
    /// Prior authority was withdrawn.
    Revoked,
}

/// Audited origin of scope authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthoritySource {
    /// An explicit writer attestation referencing real authorization.
    Direct {
        /// Opaque reference; not independently verified permission.
        authority_ref: String,
    },
    /// Exact subset inherited from this task's same-writer parent.
    Parent {
        /// Locally authoritative parent identifier.
        task: String,
    },
}

/// Persisted authorization, never inferred from delivery or a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    /// Current disposition.
    pub state: AuthorityState,
    /// Direct or inherited authority.
    pub source: AuthoritySource,
    /// Explicit scope covered by that authority.
    pub approved_scope: Vec<String>,
    /// Audited explanation.
    pub reason: String,
}

/// Exact business outcome required by an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeKind {
    /// Writer accepted the result.
    Accepted,
    /// Writer completed under a compatible contract.
    Completed,
    /// Explicit cancellation, never implicit success.
    Cancelled,
    /// Explicit failure, never implicit success.
    Failed,
}
impl OutcomeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
    fn successful(self) -> bool {
        matches!(self, Self::Accepted | Self::Completed)
    }
}

/// An ALL-of predicate; omitted revision matches the current qualifying outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    /// Same-group, locally authoritative contracted task.
    pub task: String,
    /// Explicit required disposition.
    pub outcome: OutcomeKind,
    /// Optional exact revision; no mandatory pin ceremony.
    pub revision: Option<String>,
}

/// Single-parent hierarchy, separate from launch prerequisites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentLink {
    /// Same-writer parent.
    pub task: String,
    /// Whether this child's success guards parent acceptance.
    pub required: bool,
    /// Accepted or completed; cancellation cannot satisfy this edge.
    pub outcome: OutcomeKind,
    /// Optional exact child-result revision.
    pub revision: Option<String>,
}

/// Complete model creation draft alongside the stable legacy work shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDraft {
    /// Stable legacy work fields.
    pub work: WorkDraft,
    /// Finite deliverable and limits.
    pub contract: Contract,
    /// Explicit writer authorization or hold.
    pub authorization: Authorization,
    /// ALL-of prerequisites.
    pub requirements: Vec<Requirement>,
    /// Optional same-writer parent.
    pub parent: Option<ParentLink>,
}

/// Model-only creation; composite execution provisioning is not yet available.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCreate {
    /// Retry identity; identical requests return their original receipt.
    pub key: String,
    /// Audited intent.
    pub reason: String,
    /// Complete finite assignment.
    pub draft: TaskDraft,
    /// Exact affected parent version; empty for a root task.
    pub expected_parent_versions: BTreeMap<String, i64>,
}

/// Complete explicit adoption of a legacy task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdopt {
    /// Retry identity.
    pub key: String,
    /// Observed legacy task version.
    pub version: i64,
    /// Why the writer is adopting this assignment.
    pub reason: String,
    /// Full finite contract.
    pub contract: Contract,
    /// Full current authority.
    pub authorization: Authorization,
    /// ALL-of prerequisites.
    pub requirements: Vec<Requirement>,
    /// Optional same-writer parent.
    pub parent: Option<ParentLink>,
    /// Exact affected parent versions.
    pub expected_parent_versions: BTreeMap<String, i64>,
}

/// Explicit retain/replace semantics, including replacement with `None`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "operation", content = "value", rename_all = "snake_case")]
pub enum Change<T> {
    /// Preserve the current field.
    #[default]
    Keep,
    /// Replace with the supplied full value.
    Set(T),
}

/// Criterion evidence belongs to an immutable candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriterionEvidence {
    /// Existing contract criterion.
    pub criterion_id: String,
    /// Nonempty bounded references.
    pub references: Vec<String>,
}

/// Execute and assembled-result acceptance capture different child predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Prerequisites and authority permit coordinating work.
    Execute,
    /// Prerequisites and required children qualify the assembled result.
    Accept,
}

/// Immutable concrete inputs; task version is audit metadata, not semantic validity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSnapshot {
    /// Group containing every input.
    pub group: String,
    /// Consumer task.
    pub task: String,
    /// Observed business version.
    pub task_version_at_capture: i64,
    /// Semantic invalidation epoch.
    pub input_epoch: i64,
    /// Current owner binding at capture.
    pub owner_binding_generation: i64,
    /// Captured phase.
    pub phase: Phase,
    /// Concrete immutable prerequisite outcome IDs.
    pub prerequisite_outcome_ids: BTreeMap<String, String>,
    /// Concrete required-child IDs, populated only for Accept.
    pub required_child_outcome_ids: BTreeMap<String, String>,
    /// Canonical authority bytes: collision-free equality, independent of progress.
    pub ancestor_authority_digests: BTreeMap<String, String>,
}

/// Caller attests that this artifact was produced/revalidated against these exact inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateDraft {
    /// Immutable result revision/digest, not a freshness shortcut.
    pub revision: String,
    /// Concise description of the assembled result.
    pub summary: String,
    /// Evidence for every criterion.
    pub criterion_evidence: Vec<CriterionEvidence>,
    /// Previously captured Accept-phase inputs, never silently replaced.
    pub inputs: InputSnapshot,
}

/// Writer outcome selection; successful variants require the scheduler closure integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeChange {
    /// Keep current outcome.
    Keep,
    /// Withdraw current outcome with history retained.
    Withdraw,
    /// Select an immutable reviewed candidate.
    Success {
        /// Accepted or Completed.
        kind: OutcomeKind,
        /// Existing immutable candidate.
        candidate: String,
    },
    /// Explicit negative disposition, possible while execution cleanup remains owed.
    Negative {
        /// Cancelled or Failed.
        kind: OutcomeKind,
        /// Result identity.
        revision: String,
    },
}

/// Audited writer decision, applied atomically with graph, outcome and linked Mail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDecision {
    /// Retry identity.
    pub key: String,
    /// Exact observed task version.
    pub version: i64,
    /// Audited reason.
    pub reason: String,
    /// Existing work fields; terminal transitions require an outcome below.
    pub work_patch: WorkPatch,
    /// Optional replacement scope summary.
    pub scope: Change<String>,
    /// Contract replacement.
    pub contract: Change<Contract>,
    /// Explicit authority replacement.
    pub authorization: Change<Authorization>,
    /// Prerequisite replacement.
    pub requirements: Change<Vec<Requirement>>,
    /// Parent replacement/removal.
    pub parent: Change<Option<ParentLink>>,
    /// Exact versions of affected old/new parents.
    pub expected_parent_versions: BTreeMap<String, i64>,
    /// Explicit stale-input revalidation decision; not an implicit grant.
    pub clear_invalidation: bool,
    /// Outcome operation.
    pub outcome: OutcomeChange,
    /// Optional linked pending Mail resolved in the same transaction.
    pub resolve_message: Option<i64>,
}

/// Durable result; old rows remain inspectable after withdrawal or invalidation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResult {
    /// Unique immutable identity.
    pub id: String,
    /// Owning task.
    pub task: String,
    /// None denotes a candidate, which cannot satisfy prerequisites.
    pub outcome: Option<OutcomeKind>,
    /// Artifact/decision identity.
    pub revision: String,
    /// Result description or negative reason.
    pub summary: String,
    /// Successful evidence, empty for negative outcomes.
    pub criterion_evidence: Vec<CriterionEvidence>,
    /// Captured success inputs; negative outcomes need no invented success snapshot.
    pub inputs: Option<InputSnapshot>,
    /// Actual authenticated writer.
    pub actor: String,
    /// Recorded time.
    pub created: i64,
}

/// Persistent model state alongside the unchanged work record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModel {
    /// Finite contract.
    pub contract: Contract,
    /// Current authority.
    pub authorization: Authorization,
    /// Semantic epoch.
    pub input_epoch: i64,
    /// Same-writer parent.
    pub parent: Option<ParentLink>,
    /// ALL-of predicates.
    pub requirements: Vec<Requirement>,
    /// Current outcome, distinct from immutable history.
    pub current_outcome: Option<String>,
    /// Current immutable candidate; may remain visible while stale.
    pub current_candidate: Option<String>,
    /// Exact root operation IDs awaiting writer revalidation.
    pub invalidation_causes: Vec<String>,
}

/// A specific unmet business predicate with its responsible writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessCause {
    /// Stable machine-readable reason.
    pub code: String,
    /// Task whose predicate or authority is involved.
    pub task: String,
    /// Responsible business writer.
    pub responsible: String,
    /// Required predicate, where relevant.
    pub expected: Option<Requirement>,
    /// Actual immutable result, where available.
    pub actual_outcome: Option<String>,
}

/// Business-only evaluation. An empty list never bypasses the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelReadiness {
    /// All currently observed model blockers.
    pub causes: Vec<ReadinessCause>,
}

/// Versioned local envelope; this is not the legacy relay representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskView {
    /// Local envelope version.
    pub schema_version: u32,
    /// Stable work shape.
    pub work: WorkItem,
    /// None means explicit legacy tracking.
    pub model: Option<TaskModel>,
    /// Business predicates, separate from execution support.
    pub readiness: ModelReadiness,
    /// Execution still requires the scheduler's independent admission checks.
    pub execution_hold: String,
}

#[derive(Debug, Clone)]
struct Record {
    work: WorkItem,
    model: TaskModel,
}
#[derive(Debug, Clone)]
struct Graph {
    records: BTreeMap<String, Record>,
    bindings: BTreeMap<String, i64>,
    results: BTreeMap<String, TaskResult>,
    external: BTreeSet<ProjectionEdge>,
    recovery_holds: BTreeMap<String, Vec<ReadinessCause>>,
    materialization_holds: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionEdge {
    owner: String,
    source: String,
    source_version: Option<i64>,
    edge: BlockingEdge,
}

fn text_required(value: &str, limit: usize, label: &str) -> Result<()> {
    bounded(value, limit, label)?;
    ensure!(!value.trim().is_empty(), "{label} is required");
    Ok(())
}
fn canonical<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_string(value)?;
    bounded(&value, MAX_REQUEST, "task request")?;
    Ok(value)
}
fn normalize_contract(contract: &mut Contract, authority: &mut Authorization) -> Result<()> {
    text_required(&contract.deliverable, 1024, "deliverable")?;
    ensure!(
        contract.allow_input_invalidation,
        "input_invalidation_consent_required"
    );
    ensure!(
        !contract.criteria.is_empty() && contract.criteria.len() <= 32,
        "contract requires 1..32 criteria"
    );
    contract.criteria.sort_by(|a, b| a.id.cmp(&b.id));
    let mut seen = BTreeSet::new();
    for criterion in &contract.criteria {
        name(&criterion.id)?;
        text_required(&criterion.description, 512, "criterion")?;
        ensure!(seen.insert(&criterion.id), "duplicate criterion");
    }
    for scopes in [&mut contract.allowed_scope, &mut authority.approved_scope] {
        ensure!(
            !scopes.is_empty() && scopes.len() <= 32,
            "scope requires 1..32 units"
        );
        scopes.sort();
        ensure!(
            !scopes.windows(2).any(|v| v[0] == v[1]),
            "duplicate scope unit"
        );
        for scope in scopes.iter() {
            text_required(scope, 256, "scope unit")?;
        }
        ensure!(
            scopes.iter().map(String::len).sum::<usize>() <= 1024,
            "scope exceeds 1024 bytes"
        );
    }
    ensure!(
        contract
            .allowed_scope
            .iter()
            .all(|s| authority.approved_scope.contains(s)),
        "scope_not_authorized"
    );
    text_required(&authority.reason, 512, "authority reason")?;
    match &authority.source {
        AuthoritySource::Direct { authority_ref } => {
            text_required(authority_ref, 256, "authority reference")?
        }
        AuthoritySource::Parent { task } => name(task)?,
    }
    ensure!(
        contract.budget.max_attempts > 0
            && contract.budget.max_elapsed_seconds > 0
            && contract.budget.max_elapsed_seconds <= i64::MAX as u64,
        "finite_positive_budget_required"
    );
    if let Some(cost) = &contract.budget.max_cost {
        ensure!(cost.amount > 0, "cost ceiling must be positive");
        text_required(&cost.unit, 48, "cost unit")?;
    }
    bounded(&serde_json::to_string(contract)?, 16 * 1024, "contract")?;
    Ok(())
}
fn normalize_graph(requirements: &mut [Requirement], parent: &Option<ParentLink>) -> Result<()> {
    ensure!(requirements.len() <= 32, "too many prerequisites");
    requirements.sort_by(|a, b| a.task.cmp(&b.task));
    ensure!(
        !requirements.windows(2).any(|v| v[0].task == v[1].task),
        "duplicate prerequisite"
    );
    for r in requirements {
        name(&r.task)?;
        if let Some(revision) = &r.revision {
            text_required(revision, 128, "revision")?;
        }
    }
    if let Some(parent) = parent {
        name(&parent.task)?;
        ensure!(
            parent.outcome.successful(),
            "required child must have a success predicate"
        );
        if let Some(revision) = &parent.revision {
            text_required(revision, 128, "child revision")?;
        }
    }
    Ok(())
}
fn request_identity(key: &str, reason: &str) -> Result<()> {
    text_required(key, 128, "decision key")?;
    text_required(reason, 512, "decision reason")
}

pub(crate) async fn guard_legacy_update_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
) -> Result<()> {
    let found: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_models WHERE group_name=? AND task=?")
            .bind(group)
            .bind(task)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        found == 0,
        "contracted_task_requires_task_decision: use the guarded model API"
    );
    Ok(())
}
async fn authenticate_tx(tx: &mut Transaction<'_, Sqlite>, actor: &Mailbox) -> Result<()> {
    Store::lock_actor(tx, actor).await?;
    let home: i64 = sqlx::query_scalar("SELECT count(*) FROM groups g JOIN node n ON n.id=g.home_machine JOIN mailboxes m ON m.group_name=g.name WHERE m.id=? AND m.name=? AND g.name=? AND m.remote_machine IS NULL")
        .bind(actor.id).bind(&actor.name).bind(&actor.group_name).fetch_one(&mut **tx).await?;
    ensure!(
        home == 1,
        "unsupported_remote_or_invalid_actor: contracted tasks require the authenticated home writer"
    );
    Ok(())
}
async fn replay_tx<T: for<'a> Deserialize<'a>>(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    key: &str,
    request: &str,
) -> Result<Option<T>> {
    let old = sqlx::query("SELECT canonical,result FROM task_decisions WHERE actor=? AND key=?")
        .bind(actor.id)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    old.map(|old| {
        ensure!(
            old.get::<String, _>("canonical") == request,
            "decision_key_conflict"
        );
        Ok(serde_json::from_str(&old.get::<String, _>("result"))?)
    })
    .transpose()
}
async fn save_receipt_tx<T: Serialize>(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    key: &str,
    request: &str,
    result: &T,
) -> Result<()> {
    sqlx::query("INSERT INTO task_decisions(actor,key,canonical,result) VALUES(?,?,?,?)")
        .bind(actor.id)
        .bind(key)
        .bind(request)
        .bind(serde_json::to_string(result)?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn load_graph_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    seeds: &[String],
) -> Result<Graph> {
    let external = external_sources_tx(tx, group).await?;
    let projected = projected_edges_tx(tx, group).await?;
    let mut external_neighbors: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for projection in external.iter().chain(&projected) {
        let a = &projection.edge.consumer.task;
        let b = &projection.edge.prerequisite.task;
        external_neighbors
            .entry(a.clone())
            .or_default()
            .insert(b.clone());
        external_neighbors
            .entry(b.clone())
            .or_default()
            .insert(a.clone());
    }
    // Traverse the complete affected component in both directions. Missing
    // referenced models remain errors during graph validation; bounds never
    // turn a truncated traversal into a successful admission.
    let mut pending: VecDeque<String> = seeds.iter().cloned().collect();
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    while let Some(task) = pending.pop_front() {
        if !seen.insert(task.clone()) {
            continue;
        }
        ensure!(
            seen.len() <= MAX_TASKS,
            "graph_validation_incomplete: component exceeds 1000 tasks"
        );
        let row = sqlx::query("SELECT task,contract,authorization,input_epoch,parent_predicate,current_outcome,current_candidate,invalidation_causes FROM task_models WHERE group_name=? AND task=?")
            .bind(group).bind(&task).fetch_optional(&mut **tx).await?;
        if let Some(row) = row {
            rows.push(row);
        }
        let neighbors: Vec<String> = sqlx::query_scalar("SELECT parent FROM task_models WHERE group_name=? AND task=? AND parent IS NOT NULL UNION SELECT task FROM task_models WHERE group_name=? AND parent=? UNION SELECT prerequisite FROM task_requirements WHERE group_name=? AND consumer=? UNION SELECT consumer FROM task_requirements WHERE group_name=? AND prerequisite=? LIMIT 1001")
            .bind(group).bind(&task).bind(group).bind(&task).bind(group).bind(&task).bind(group).bind(&task).fetch_all(&mut **tx).await?;
        ensure!(
            neighbors.len() <= MAX_TASKS,
            "graph_validation_incomplete: neighbor bound exceeded"
        );
        pending.extend(neighbors);
        if let Some(neighbors) = external_neighbors.get(&task) {
            pending.extend(neighbors.iter().cloned());
        }
    }
    let mut records = BTreeMap::new();
    let mut results = BTreeMap::new();
    for row in rows {
        let task: String = row.try_get("task")?;
        let requirements = sqlx::query("SELECT prerequisite,outcome,revision FROM task_requirements WHERE group_name=? AND consumer=? ORDER BY prerequisite")
            .bind(group).bind(&task).fetch_all(&mut **tx).await?.into_iter().map(|row| {
                let kind: String = row.try_get("outcome")?;
                Ok(Requirement { task: row.try_get("prerequisite")?, outcome: serde_json::from_value(serde_json::Value::String(kind))?, revision: row.try_get("revision")? })
            }).collect::<Result<Vec<_>>>()?;
        let current_outcome: Option<String> = row.try_get("current_outcome")?;
        if let Some(id) = &current_outcome {
            let payload: Option<String> = sqlx::query_scalar("SELECT payload FROM task_results WHERE group_name=? AND task=? AND id=? AND kind<>'candidate'")
                .bind(group).bind(&task).bind(id).fetch_optional(&mut **tx).await?;
            let result: TaskResult =
                serde_json::from_str(&payload.context("current outcome projection is missing")?)?;
            ensure!(
                result.id == *id && result.task == task && result.outcome.is_some(),
                "invalid current outcome projection"
            );
            results.insert(id.clone(), result);
        }
        let current_candidate: Option<String> = row.try_get("current_candidate")?;
        if let Some(id) = &current_candidate {
            let payload: String = sqlx::query_scalar("SELECT payload FROM task_results WHERE group_name=? AND task=? AND id=? AND kind='candidate'")
                .bind(group).bind(&task).bind(id).fetch_one(&mut **tx).await?;
            let result: TaskResult = serde_json::from_str(&payload)?;
            ensure!(
                result.id == *id && result.task == task && result.outcome.is_none(),
                "invalid candidate projection"
            );
            results.insert(id.clone(), result);
        }
        let parent: Option<String> = row.try_get("parent_predicate")?;
        records.insert(
            task.clone(),
            Record {
                work: Store::work_load_tx(tx, group, &task).await?,
                model: TaskModel {
                    contract: serde_json::from_str(&row.try_get::<String, _>("contract")?)?,
                    authorization: serde_json::from_str(
                        &row.try_get::<String, _>("authorization")?,
                    )?,
                    input_epoch: row.try_get("input_epoch")?,
                    parent: parent.map(|s| serde_json::from_str(&s)).transpose()?,
                    requirements,
                    current_outcome,
                    current_candidate,
                    invalidation_causes: serde_json::from_str(
                        &row.try_get::<String, _>("invalidation_causes")?,
                    )?,
                },
            },
        );
    }
    let bindings = sqlx::query("SELECT name,binding_version FROM mailboxes WHERE group_name=? AND remote_machine IS NULL AND agent_state='registered'")
        .bind(group).fetch_all(&mut **tx).await?.into_iter().map(|row| Ok((row.try_get("name")?, row.try_get("binding_version")?))).collect::<Result<BTreeMap<_,_>>>()?;
    let mut recovery_holds = BTreeMap::new();
    for task in records.keys() {
        let holds = crate::decision_recovery::recovery_admission_holds_tx(tx, group, task).await?;
        recovery_holds.insert(
            task.clone(),
            holds
                .into_iter()
                .map(|hold| ReadinessCause {
                    code: format!(
                        "recovery_reassessment:case:{}:version:{}:event:{}:{}",
                        hold.case_id, hold.case_version, hold.model_event, hold.reason
                    ),
                    task: task.clone(),
                    responsible: hold.responsible,
                    expected: None,
                    actual_outcome: None,
                })
                .collect(),
        );
    }
    let mut materialization_holds = BTreeSet::new();
    for task in records.keys() {
        let receipt: Option<String> = sqlx::query_scalar(
            "SELECT receipt FROM task_materializations WHERE group_name=? AND decision_task=?",
        )
        .bind(group)
        .bind(task)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(receipt) = receipt {
            let receipt: MaterializedDecisionReceipt = serde_json::from_str(&receipt)?;
            let current: i64 = sqlx::query_scalar("SELECT count(*) FROM task_decision_policies p JOIN mailboxes m ON m.id=p.issuer WHERE p.group_name=? AND p.id=? AND p.source=? AND p.writer=? AND p.issuer=? AND p.revision=? AND p.revoked=0 AND m.name=p.writer AND m.group_name=p.group_name AND m.agent_state='registered' AND m.remote_machine IS NULL")
                .bind(group).bind(&receipt.policy.id).bind(&receipt.source_key).bind(&receipt.writer)
                .bind(receipt.issuer).bind(receipt.policy_revision).fetch_one(&mut **tx).await?;
            let request: DecisionMaterializationRequest = serde_json::from_str(&receipt.canonical)?;
            let source =
                crate::decision_recovery::inspect_source_tx(tx, group, &request.source.source)
                    .await?;
            if current != 1
                || !source.unresolved
                || source.source != request.source.source
                || source.input_epoch != request.source.input_epoch
                || source.candidate != request.source.candidate
                || source.outcome != request.source.outcome
                || source.authority_id != receipt.issuer
            {
                materialization_holds.insert(task.clone());
            }
        }
    }
    Ok(Graph {
        records,
        bindings,
        results,
        external,
        recovery_holds,
        materialization_holds,
    })
}

/// Typed action references; unknown scopes never become disconnected graph nodes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "scope", rename_all = "snake_case")]
pub enum TaskAction {
    /// All declared actions; expanded before traversal.
    WholeTask,
    /// Coordinating or producing output.
    Execute,
    /// Writer acceptance of the assembled artifact.
    AcceptResult,
    /// Publication of an artifact.
    PublishArtifact,
    /// A literal existing contract scope unit.
    Scope(String),
}

/// One task action in the shared blocking graph.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionNode {
    /// Existing same-group task.
    pub task: String,
    /// Declared action.
    pub action: TaskAction,
}

/// Typed edge whose meaning has been validated by its source owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockingEdge {
    /// Consumer that is waiting.
    pub consumer: ActionNode,
    /// Action that must finish first.
    pub prerequisite: ActionNode,
    /// Durable source kind, retained in cycle evidence.
    pub kind: String,
}

fn model_edges(graph: &Graph) -> BTreeSet<BlockingEdge> {
    let mut edges = BTreeSet::new();
    for (task, record) in &graph.records {
        for requirement in &record.model.requirements {
            for action in [
                TaskAction::Execute,
                TaskAction::AcceptResult,
                TaskAction::PublishArtifact,
            ] {
                edges.insert(BlockingEdge {
                    consumer: ActionNode {
                        task: task.clone(),
                        action,
                    },
                    prerequisite: ActionNode {
                        task: requirement.task.clone(),
                        action: TaskAction::AcceptResult,
                    },
                    kind: "prerequisite".into(),
                });
            }
        }
        if let Some(parent) = &record.model.parent {
            if parent.required {
                edges.insert(BlockingEdge {
                    consumer: ActionNode {
                        task: parent.task.clone(),
                        action: TaskAction::AcceptResult,
                    },
                    prerequisite: ActionNode {
                        task: task.clone(),
                        action: TaskAction::AcceptResult,
                    },
                    kind: "required_child".into(),
                });
            }
        }
    }
    edges
}

fn expand_node(graph: &Graph, node: &ActionNode) -> Result<Vec<ActionNode>> {
    let record = graph
        .records
        .get(&node.task)
        .context("graph source references missing local task")?;
    let actions = match &node.action {
        TaskAction::WholeTask => {
            let mut actions = vec![
                TaskAction::Execute,
                TaskAction::AcceptResult,
                TaskAction::PublishArtifact,
            ];
            actions.extend(
                record
                    .model
                    .contract
                    .allowed_scope
                    .iter()
                    .cloned()
                    .map(TaskAction::Scope),
            );
            actions
        }
        TaskAction::Scope(scope) => {
            ensure!(
                record.model.contract.allowed_scope.contains(scope),
                "undeclared_graph_scope"
            );
            vec![node.action.clone()]
        }
        _ => vec![node.action.clone()],
    };
    Ok(actions
        .into_iter()
        .map(|action| ActionNode {
            task: node.task.clone(),
            action,
        })
        .collect())
}

fn check_edges(graph: &Graph, edges: &BTreeSet<BlockingEdge>) -> Result<()> {
    let mut adjacency: BTreeMap<ActionNode, Vec<(ActionNode, String)>> = BTreeMap::new();
    let mut count = 0usize;
    for edge in edges {
        for from in expand_node(graph, &edge.consumer)? {
            for to in expand_node(graph, &edge.prerequisite)? {
                count += 1;
                ensure!(
                    count <= MAX_EDGES,
                    "graph_validation_incomplete: expanded edge bound exceeded"
                );
                adjacency.entry(to.clone()).or_default();
                adjacency
                    .entry(from.clone())
                    .or_default()
                    .push((to, edge.kind.clone()));
            }
        }
    }
    ensure!(
        adjacency.len() <= MAX_TASKS,
        "graph_validation_incomplete: action-node bound exceeded"
    );
    // Iterative depth-first search retains the typed path and avoids stack growth.
    let mut done = BTreeSet::new();
    for root in adjacency.keys() {
        if done.contains(root) {
            continue;
        }
        let mut active = BTreeSet::from([root.clone()]);
        let mut stack = vec![(root.clone(), 0usize)];
        while let Some((node, index)) = stack.last().cloned() {
            let outgoing = &adjacency[&node];
            if index == outgoing.len() {
                let (node, _) = stack.pop().context("graph traversal lost node")?;
                active.remove(&node);
                done.insert(node);
                continue;
            }
            let (next, kind) = outgoing[index].clone();
            stack.last_mut().context("graph traversal lost node")?.1 += 1;
            ensure!(
                !active.contains(&next),
                "blocking_cycle: {:?} --{}--> {:?}; path {:?}",
                node,
                kind,
                next,
                stack
            );
            if done.contains(&next) {
                continue;
            }
            active.insert(next.clone());
            stack.push((next, 0));
        }
    }
    Ok(())
}

fn validate_model_graph(graph: &Graph) -> Result<()> {
    ensure!(
        graph.records.len() <= MAX_TASKS,
        "graph_validation_incomplete: component task bound exceeded"
    );
    for (task, record) in &graph.records {
        let mut visited = BTreeSet::from([task.clone()]);
        let mut current = record;
        while let Some(parent) = &current.model.parent {
            ensure!(
                visited.insert(parent.task.clone()),
                "parent_cycle: {:?}",
                visited
            );
            let target = graph
                .records
                .get(&parent.task)
                .context("unsupported_remote_or_missing_parent")?;
            ensure!(
                target.work.writer == record.work.writer,
                "cross_writer_hierarchy_unsupported"
            );
            current = target;
        }
        if let AuthoritySource::Parent { task: source } = &record.model.authorization.source {
            ensure!(
                record.model.parent.as_ref().map(|p| &p.task) == Some(source),
                "inherited_authority_requires_matching_parent"
            );
        }
        for requirement in &record.model.requirements {
            ensure!(&requirement.task != task, "self_prerequisite");
            let target = graph
                .records
                .get(&requirement.task)
                .context("unsupported_remote_or_missing_prerequisite")?;
            ensure!(
                requirement.outcome != OutcomeKind::Completed
                    || target.model.contract.completion == Completion::CompletionAllowed,
                "completed_cannot_bypass_writer_acceptance"
            );
        }
        if let Some(parent) = &record.model.parent {
            ensure!(
                parent.outcome != OutcomeKind::Completed
                    || record.model.contract.completion == Completion::CompletionAllowed,
                "child_completed_cannot_bypass_writer_acceptance"
            );
        }
    }
    check_edges(graph, &model_edges(graph))
}

async fn external_sources_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
) -> Result<BTreeSet<ProjectionEdge>> {
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        INTEGRATED_OWNER_SCHEMAS.contains(&schema),
        "graph_validation_incomplete: owner schema {schema} unavailable"
    );
    let mut edges = BTreeSet::new();
    for source in crate::execution::scheduler_blocking_sources_tx(tx, group).await? {
        for edge in source.edges {
            ensure!(
                edges.insert(ProjectionEdge {
                    owner: "scheduler".into(),
                    source: source.source.clone(),
                    source_version: None,
                    edge,
                }),
                "graph_validation_incomplete: duplicate scheduler source edge"
            );
        }
    }
    for source in crate::decision_recovery::recovery_blocking_edges_tx(tx, group).await? {
        ensure!(
            edges.insert(ProjectionEdge {
                owner: "recovery".into(),
                source: source.source,
                source_version: Some(source.case_version),
                edge: source.edge,
            }),
            "graph_validation_incomplete: duplicate recovery source edge"
        );
    }
    ensure!(
        edges.len() <= MAX_EDGES,
        "graph_validation_incomplete: external edge bound exceeded"
    );
    Ok(edges)
}

async fn projected_edges_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
) -> Result<BTreeSet<ProjectionEdge>> {
    let rows = sqlx::query("SELECT owner,source,source_version,consumer,prerequisite,kind FROM task_blocking_edges WHERE group_name=? LIMIT 10001")
        .bind(group).fetch_all(&mut **tx).await?;
    ensure!(
        rows.len() <= MAX_EDGES,
        "graph_validation_incomplete: projection edge bound exceeded"
    );
    rows.into_iter()
        .map(|row| {
            let owner: String = row.try_get("owner")?;
            ensure!(
                matches!(owner.as_str(), "model" | "scheduler" | "recovery"),
                "graph_validation_incomplete: owner {owner} integration unavailable"
            );
            Ok(ProjectionEdge {
                owner,
                source: row.try_get("source")?,
                source_version: row.try_get("source_version")?,
                edge: BlockingEdge {
                    consumer: serde_json::from_str(&row.try_get::<String, _>("consumer")?)?,
                    prerequisite: serde_json::from_str(&row.try_get::<String, _>("prerequisite")?)?,
                    kind: row.try_get("kind")?,
                },
            })
        })
        .collect()
}

fn touches_graph(graph: &Graph, edge: &BlockingEdge) -> bool {
    graph.records.contains_key(&edge.consumer.task)
        || graph.records.contains_key(&edge.prerequisite.task)
}

async fn validate_projection_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    graph: &Graph,
) -> Result<()> {
    let actual = projected_edges_tx(tx, group)
        .await?
        .into_iter()
        .filter(|projection| touches_graph(graph, &projection.edge))
        .collect::<BTreeSet<_>>();
    let mut expected = graph
        .external
        .iter()
        .filter(|projection| touches_graph(graph, &projection.edge))
        .cloned()
        .collect::<BTreeSet<_>>();
    for edge in model_edges(graph) {
        expected.insert(ProjectionEdge {
            owner: "model".into(),
            source: edge.consumer.task.clone(),
            source_version: Some(graph.records[&edge.consumer.task].work.version),
            edge,
        });
    }
    ensure!(
        actual == expected,
        "graph_validation_incomplete: projection differs from authoritative source inventory"
    );
    validate_model_graph(graph)?;
    check_edges(
        graph,
        &expected
            .into_iter()
            .map(|projection| projection.edge)
            .collect(),
    )
}

async fn write_projection_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    graph: &Graph,
) -> Result<()> {
    validate_model_graph(graph)?;
    let tasks = serde_json::to_string(&graph.records.keys().collect::<Vec<_>>())?;
    sqlx::query("DELETE FROM task_blocking_edges WHERE group_name=? AND owner='model' AND source IN (SELECT value FROM json_each(?))").bind(group).bind(tasks).execute(&mut **tx).await?;
    for edge in model_edges(graph) {
        sqlx::query("INSERT INTO task_blocking_edges(group_name,owner,source,source_version,consumer,prerequisite,kind) VALUES(?,'model',?,?,?,?,?)")
            .bind(group).bind(&edge.consumer.task).bind(graph.records[&edge.consumer.task].work.version)
            .bind(serde_json::to_string(&edge.consumer)?).bind(serde_json::to_string(&edge.prerequisite)?).bind(&edge.kind).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Refresh only named recovery sources after the owner has authorized and
/// persisted its source mutation. This grants no source authority. The caller
/// must roll back its entire transaction if source or complete graph validation
/// fails; projection writes and source writes must never commit separately.
pub(crate) async fn refresh_recovery_projection_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    cases: &BTreeMap<i64, i64>,
) -> Result<()> {
    reserve_group_tx(tx, group).await?;
    ensure!(
        cases.len() <= MAX_TASKS,
        "graph_validation_incomplete: recovery source bound exceeded"
    );
    let mut sources = BTreeSet::new();
    let mut seeds = BTreeSet::new();
    for (case, version) in cases {
        let current = crate::decision_recovery::load_case_tx(tx, group, *case).await?;
        ensure!(
            current.version == *version,
            "recovery_projection_case_conflict"
        );
        if let crate::decision_recovery::Obligation::Task { id, .. } =
            &current.current_source.source
        {
            if current.current_source.input_epoch.is_some() {
                seeds.insert(id.clone());
            }
        }
        seeds.extend(current.decision_task);
        sources.insert(format!("case:{case}"));
    }
    let expected = external_sources_tx(tx, group).await?;
    let old = projected_edges_tx(tx, group).await?;
    let selected = |projection: &&ProjectionEdge| {
        projection.owner == "recovery" && sources.contains(&projection.source)
    };
    for projection in old
        .iter()
        .filter(selected)
        .chain(expected.iter().filter(selected))
    {
        seeds.insert(projection.edge.consumer.task.clone());
        seeds.insert(projection.edge.prerequisite.task.clone());
    }
    for source in &sources {
        sqlx::query(
            "DELETE FROM task_blocking_edges WHERE group_name=? AND owner='recovery' AND source=?",
        )
        .bind(group)
        .bind(source)
        .execute(&mut **tx)
        .await?;
    }
    for projection in expected.iter().filter(selected) {
        sqlx::query("INSERT INTO task_blocking_edges(group_name,owner,source,source_version,consumer,prerequisite,kind) VALUES(?,'recovery',?,?,?,?,?)")
            .bind(group).bind(&projection.source).bind(projection.source_version)
            .bind(serde_json::to_string(&projection.edge.consumer)?).bind(serde_json::to_string(&projection.edge.prerequisite)?)
            .bind(&projection.edge.kind).execute(&mut **tx).await?;
    }
    if !seeds.is_empty() {
        let graph = load_graph_tx(tx, group, &seeds.into_iter().collect::<Vec<_>>()).await?;
        validate_projection_tx(tx, group, &graph).await?;
    }
    Ok(())
}

fn authority_snapshot(graph: &Graph, task: &str) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let mut current = graph.records.get(task).context("task model missing")?;
    let mut seen = BTreeSet::from([task.to_owned()]);
    while let Some(parent) = &current.model.parent {
        ensure!(seen.insert(parent.task.clone()), "parent_cycle");
        current = graph
            .records
            .get(&parent.task)
            .context("parent model missing")?;
        result.insert(
            parent.task.clone(),
            serde_json::to_string(&(
                &current.model.authorization,
                &current.model.contract.allowed_scope,
                current.model.contract.allow_delegation,
                has_negative_outcome(graph, current),
            ))?,
        );
    }
    Ok(result)
}
fn has_negative_outcome(graph: &Graph, record: &Record) -> bool {
    record
        .model
        .current_outcome
        .as_ref()
        .and_then(|id| graph.results.get(id))
        .and_then(|r| r.outcome)
        .is_some_and(|kind| !kind.successful())
}
fn authority_current(graph: &Graph, task: &str) -> bool {
    let mut current = task;
    let mut seen = BTreeSet::new();
    while seen.insert(current.to_owned()) {
        let Some(record) = graph.records.get(current) else {
            return false;
        };
        if record.model.authorization.state != AuthorityState::Authorized
            || has_negative_outcome(graph, record)
            || graph.materialization_holds.contains(current)
        {
            return false;
        }
        if let AuthoritySource::Parent { task: parent } = &record.model.authorization.source {
            let Some(parent) = graph.records.get(parent) else {
                return false;
            };
            if !parent.model.contract.allow_delegation
                || !record
                    .model
                    .authorization
                    .approved_scope
                    .iter()
                    .all(|scope| {
                        parent.model.contract.allowed_scope.contains(scope)
                            && parent.model.authorization.approved_scope.contains(scope)
                    })
            {
                return false;
            }
        }
        match &record.model.parent {
            Some(parent) => current = &parent.task,
            None => return true,
        }
    }
    false
}
fn snapshot_base_current(graph: &Graph, snapshot: &InputSnapshot) -> Result<bool> {
    let Some(record) = graph.records.get(&snapshot.task) else {
        return Ok(false);
    };
    Ok(snapshot.group == record.work.group_name
        && snapshot.input_epoch == record.model.input_epoch
        && graph.bindings.get(&record.work.owner) == Some(&snapshot.owner_binding_generation)
        && authority_current(graph, &snapshot.task)
        && authority_snapshot(graph, &snapshot.task)? == snapshot.ancestor_authority_digests)
}
fn valid_outcomes(graph: &Graph) -> Result<BTreeSet<String>> {
    let mut valid = BTreeSet::new();
    for record in graph.records.values() {
        if let Some(id) = &record.model.current_outcome {
            let result = graph
                .results
                .get(id)
                .context("outcome projection missing")?;
            let kind = result.outcome.context("candidate is not an outcome")?;
            if !kind.successful()
                || (record.model.invalidation_causes.is_empty()
                    && result
                        .inputs
                        .as_ref()
                        .map(|inputs| snapshot_base_current(graph, inputs))
                        .transpose()?
                        .unwrap_or(false))
            {
                valid.insert(id.clone());
            }
        }
    }
    loop {
        let mut remove = Vec::new();
        for id in &valid {
            let result = &graph.results[id];
            if result.outcome.is_some_and(OutcomeKind::successful) {
                let inputs = result.inputs.as_ref().context("success inputs missing")?;
                if !snapshot_edges_current(graph, inputs, &valid)? {
                    remove.push(id.clone());
                }
            }
        }
        if remove.is_empty() {
            return Ok(valid);
        }
        for id in remove {
            valid.remove(&id);
        }
    }
}
fn matching_outcome(
    graph: &Graph,
    requirement: &Requirement,
    valid: &BTreeSet<String>,
) -> Option<String> {
    let id = graph
        .records
        .get(&requirement.task)?
        .model
        .current_outcome
        .as_ref()?;
    if !valid.contains(id) {
        return None;
    }
    let outcome = graph.results.get(id)?;
    (outcome.outcome == Some(requirement.outcome)
        && requirement
            .revision
            .as_ref()
            .is_none_or(|revision| revision == &outcome.revision))
    .then(|| id.clone())
}
fn required_children(graph: &Graph, task: &str) -> Vec<Requirement> {
    graph
        .records
        .iter()
        .filter_map(|(child, record)| {
            let parent = record.model.parent.as_ref()?;
            (parent.task == task && parent.required).then(|| Requirement {
                task: child.clone(),
                outcome: parent.outcome,
                revision: parent.revision.clone(),
            })
        })
        .collect()
}
fn snapshot_edges_current(
    graph: &Graph,
    snapshot: &InputSnapshot,
    valid: &BTreeSet<String>,
) -> Result<bool> {
    let record = graph
        .records
        .get(&snapshot.task)
        .context("snapshot task missing")?;
    let resolve = |requirements: &[Requirement]| -> Option<BTreeMap<String, String>> {
        requirements
            .iter()
            .map(|r| matching_outcome(graph, r, valid).map(|id| (r.task.clone(), id)))
            .collect()
    };
    let children = if snapshot.phase == Phase::Accept {
        required_children(graph, &snapshot.task)
    } else {
        Vec::new()
    };
    Ok(
        resolve(&record.model.requirements).as_ref() == Some(&snapshot.prerequisite_outcome_ids)
            && resolve(&children).as_ref() == Some(&snapshot.required_child_outcome_ids),
    )
}
fn evaluate(graph: &Graph, task: &str, phase: Phase) -> Result<ModelReadiness> {
    let mut readiness = evaluate_with_outcomes(graph, task, phase, &valid_outcomes(graph)?)?;
    // Ordinary phases may use every declared scope. Recovery's constraint is
    // checked here, while model invalidation below evaluates business changes
    // before recovery reconciles obsolete action aliases.
    readiness.causes.extend(external_action_causes(
        graph,
        task,
        &match phase {
            Phase::Execute => TaskAction::Execute,
            Phase::Accept => TaskAction::AcceptResult,
        },
        true,
    )?);
    Ok(readiness)
}
fn evaluate_with_outcomes(
    graph: &Graph,
    task: &str,
    phase: Phase,
    valid: &BTreeSet<String>,
) -> Result<ModelReadiness> {
    let record = graph.records.get(task).context("task model missing")?;
    let mut causes = Vec::new();
    let mut cause =
        |code: &str, target: &str, expected: Option<Requirement>, actual: Option<String>| {
            causes.push(ReadinessCause {
                code: code.into(),
                task: target.into(),
                responsible: graph
                    .records
                    .get(target)
                    .map_or_else(|| record.work.writer.clone(), |r| r.work.writer.clone()),
                expected,
                actual_outcome: actual,
            });
        };
    if !authority_current(graph, task) {
        cause("authority_held", task, None, None);
    }
    if !graph.bindings.contains_key(&record.work.owner) {
        cause("owner_unavailable", task, None, None);
    }
    let state_ok = match phase {
        Phase::Execute => matches!(
            record.work.state,
            TaskState::Open | TaskState::Ready | TaskState::Active
        ),
        Phase::Accept => matches!(
            record.work.state,
            TaskState::Open | TaskState::Ready | TaskState::Active | TaskState::Review
        ),
    };
    if !state_ok {
        cause("state_hold", task, None, None);
    }
    if !record.model.invalidation_causes.is_empty() {
        cause("input_revalidation_required", task, None, None);
    }
    let mut requirements = record.model.requirements.clone();
    if phase == Phase::Accept {
        requirements.extend(required_children(graph, task));
    }
    for requirement in requirements {
        if matching_outcome(graph, &requirement, valid).is_none() {
            let actual = graph
                .records
                .get(&requirement.task)
                .and_then(|r| r.model.current_outcome.clone());
            let target = requirement.task.clone();
            cause("waiting_outcome", &target, Some(requirement), actual);
        }
    }
    Ok(ModelReadiness { causes })
}

fn external_action_causes(
    graph: &Graph,
    task: &str,
    action: &TaskAction,
    include_scopes: bool,
) -> Result<Vec<ReadinessCause>> {
    let record = graph.records.get(task).context("task model missing")?;
    let mut causes = graph.recovery_holds.get(task).cloned().unwrap_or_default();
    for projection in &graph.external {
        if projection.edge.consumer.task != task {
            continue;
        }
        let held = expand_node(graph, &projection.edge.consumer)?
            .iter()
            .any(|node| {
                &node.action == action
                    || (include_scopes && matches!(node.action, TaskAction::Scope(_)))
            });
        if held {
            causes.push(ReadinessCause {
                code: format!("waiting_action:{}:{}", projection.owner, projection.source),
                task: task.into(),
                responsible: record.work.writer.clone(),
                expected: None,
                actual_outcome: None,
            });
        }
    }
    Ok(causes)
}
fn capture(graph: &Graph, task: &str, phase: Phase) -> Result<InputSnapshot> {
    let readiness = evaluate(graph, task, phase)?;
    ensure!(
        readiness.causes.is_empty(),
        "model_not_ready: {}",
        serde_json::to_string(&readiness)?
    );
    let record = &graph.records[task];
    let valid = valid_outcomes(graph)?;
    let resolve = |requirements: &[Requirement]| -> Result<BTreeMap<String, String>> {
        requirements
            .iter()
            .map(|r| {
                Ok((
                    r.task.clone(),
                    matching_outcome(graph, r, &valid)
                        .context("predicate changed within snapshot")?,
                ))
            })
            .collect()
    };
    Ok(InputSnapshot {
        group: record.work.group_name.clone(),
        task: task.into(),
        task_version_at_capture: record.work.version,
        input_epoch: record.model.input_epoch,
        owner_binding_generation: graph.bindings[&record.work.owner],
        phase,
        prerequisite_outcome_ids: resolve(&record.model.requirements)?,
        required_child_outcome_ids: if phase == Phase::Accept {
            resolve(&required_children(graph, task))?
        } else {
            BTreeMap::new()
        },
        ancestor_authority_digests: authority_snapshot(graph, task)?,
    })
}
fn view(graph: &Graph, task: &str) -> Result<TaskView> {
    let record = graph.records.get(task).context("task model missing")?;
    Ok(TaskView {
        schema_version: 1,
        work: record.work.clone(),
        model: Some(record.model.clone()),
        readiness: evaluate(graph, task, Phase::Execute)?,
        execution_hold: "scheduler_admission_required".into(),
    })
}

fn event_view(graph: &Graph, task: &str) -> Result<TaskView> {
    let record = graph.records.get(task).context("event task missing")?;
    // Recovery consumes immutable source fields before reconciling its old
    // waits. Evaluating those waits against a corrected scope here could veto
    // the correction before recovery can replace them with its explicit hold.
    Ok(TaskView {
        schema_version: 1,
        work: record.work.clone(),
        model: Some(record.model.clone()),
        readiness: ModelReadiness {
            causes: vec![ReadinessCause {
                code: "transaction_reconciliation_pending".into(),
                task: task.into(),
                responsible: record.work.writer.clone(),
                expected: None,
                actual_outcome: None,
            }],
        },
        execution_hold: "transaction_reconciliation_pending".into(),
    })
}

fn bump(value: i64) -> Result<i64> {
    value.checked_add(1).context("task counter overflow")
}
fn model_changed(a: &Record, b: &Record) -> Result<bool> {
    Ok(
        serde_json::to_string(&a.model)? != serde_json::to_string(&b.model)?
            || serde_json::to_string(&a.work)? != serde_json::to_string(&b.work)?,
    )
}
fn affected_parents(old: Option<&ParentLink>, new: Option<&ParentLink>) -> BTreeSet<String> {
    old.into_iter().chain(new).map(|p| p.task.clone()).collect()
}
fn record_parent_attachment(graph: &mut Graph, child: &str) -> Result<()> {
    let parent = graph
        .records
        .get(child)
        .context("child missing")?
        .model
        .parent
        .clone();
    let Some(parent) = parent else {
        return Ok(());
    };
    let record = graph
        .records
        .get_mut(&parent.task)
        .context("parent missing")?;
    // Every attachment consumes the parent's exact CAS and is auditable.
    // Only required children change the parent's assembled-result inputs.
    record.work.version = bump(record.work.version)?;
    if parent.required {
        record.model.input_epoch = bump(record.model.input_epoch)?;
    }
    Ok(())
}
fn guard_parents(
    graph: &Graph,
    expected: &BTreeMap<String, i64>,
    affected: &BTreeSet<String>,
) -> Result<()> {
    ensure!(
        expected.keys().cloned().collect::<BTreeSet<_>>() == *affected,
        "exact_parent_versions_required"
    );
    for (parent, version) in expected {
        ensure!(
            graph
                .records
                .get(parent)
                .context("parent missing")?
                .work
                .version
                == *version,
            "parent_version_conflict: {parent}"
        );
    }
    Ok(())
}
fn invalidate_record(record: &mut Record, operation: &str) {
    if !record
        .model
        .invalidation_causes
        .iter()
        .any(|cause| cause == operation)
    {
        record.model.invalidation_causes.push(operation.into());
    }
    if matches!(record.work.state, TaskState::Accepted | TaskState::Done) {
        record.work.state = TaskState::Review;
    } else {
        record.work.state = TaskState::Blocked;
    }
    record.model.current_outcome = None;
    record.work.accepted_revision = None;
    record.work.next_action =
        "Review changed task inputs and explicitly revalidate or cancel".into();
}
fn propagate_invalidation(graph: &mut Graph, before: &Graph, operation: &str) -> Result<()> {
    let old_valid = valid_outcomes(before)?;
    let mut changed = BTreeSet::new();
    loop {
        let valid = valid_outcomes(graph)?;
        let mut invalid = Vec::new();
        for (task, record) in &graph.records {
            if changed.contains(task) {
                continue;
            }
            let Some(old) = before.records.get(task) else {
                continue;
            };
            let authority_changed =
                authority_snapshot(before, task)? != authority_snapshot(graph, task)?;
            let mut captured_stale = false;
            for id in [
                old.model.current_candidate.as_ref(),
                old.model.current_outcome.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if record.model.current_candidate.as_ref() != Some(id)
                    && record.model.current_outcome.as_ref() != Some(id)
                {
                    continue;
                }
                if let Some(inputs) = before
                    .results
                    .get(id)
                    .and_then(|result| result.inputs.as_ref())
                {
                    if snapshot_base_current(before, inputs)?
                        && snapshot_edges_current(before, inputs, &old_valid)?
                        && (!snapshot_base_current(graph, inputs)?
                            || !snapshot_edges_current(graph, inputs, &valid)?)
                    {
                        captured_stale = true;
                    }
                }
            }
            let lost_readiness = evaluate_with_outcomes(before, task, Phase::Execute, &old_valid)?
                .causes
                .is_empty()
                && !evaluate_with_outcomes(graph, task, Phase::Execute, &valid)?
                    .causes
                    .is_empty();
            if authority_changed
                || captured_stale
                || (lost_readiness && record.work.state == old.work.state)
            {
                invalid.push((
                    task.clone(),
                    authority_changed,
                    captured_stale || lost_readiness,
                ));
            }
        }
        if invalid.is_empty() {
            break;
        }
        for (task, authority_changed, needs_hold) in invalid {
            let record = graph
                .records
                .get_mut(&task)
                .context("invalidation task missing")?;
            let old = &before.records[&task];
            if record.model.input_epoch == old.model.input_epoch {
                record.model.input_epoch = bump(record.model.input_epoch)?;
            }
            let negative = record
                .model
                .current_outcome
                .as_ref()
                .and_then(|id| graph.results.get(id))
                .and_then(|result| result.outcome)
                .is_some_and(|kind| !kind.successful());
            if !negative
                && (needs_hold
                    || (authority_changed
                        && matches!(
                            old.work.state,
                            TaskState::Active | TaskState::Accepted | TaskState::Done
                        )))
            {
                invalidate_record(record, operation);
            }
            changed.insert(task);
        }
    }
    Ok(())
}

async fn persist_model_tx(tx: &mut Transaction<'_, Sqlite>, record: &Record) -> Result<()> {
    let m = &record.model;
    sqlx::query("INSERT INTO task_models(group_name,task,contract,authorization,input_epoch,parent,parent_predicate,current_outcome,current_candidate,invalidation_causes) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_name,task) DO UPDATE SET contract=excluded.contract,authorization=excluded.authorization,input_epoch=excluded.input_epoch,parent=excluded.parent,parent_predicate=excluded.parent_predicate,current_outcome=excluded.current_outcome,current_candidate=excluded.current_candidate,invalidation_causes=excluded.invalidation_causes")
        .bind(&record.work.group_name).bind(&record.work.id).bind(serde_json::to_string(&m.contract)?).bind(serde_json::to_string(&m.authorization)?).bind(m.input_epoch)
        .bind(m.parent.as_ref().map(|p| &p.task)).bind(m.parent.as_ref().map(serde_json::to_string).transpose()?).bind(&m.current_outcome).bind(&m.current_candidate)
        .bind(serde_json::to_string(&m.invalidation_causes)?).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM task_requirements WHERE group_name=? AND consumer=?")
        .bind(&record.work.group_name)
        .bind(&record.work.id)
        .execute(&mut **tx)
        .await?;
    for requirement in &m.requirements {
        sqlx::query("INSERT INTO task_requirements(group_name,consumer,prerequisite,outcome,revision) VALUES(?,?,?,?,?)")
            .bind(&record.work.group_name).bind(&record.work.id).bind(&requirement.task).bind(requirement.outcome.as_str()).bind(&requirement.revision).execute(&mut **tx).await?;
    }
    Ok(())
}
struct ModelOperation<'a> {
    root: &'a str,
    id: &'a str,
    reason: &'a str,
    now: i64,
    origin: ModelOrigin,
}

#[derive(Clone, Copy)]
enum ModelOrigin {
    Writer,
    PolicyReviewer,
    SystemInvalidation,
    SystemMaterialization,
}

/// Actual immutable events and resulting recovery revisions from one mutation.
/// Private fields and no deserialization prevent caller-asserted completion.
#[derive(Debug)]
pub(crate) struct AppliedModelDecision {
    group: String,
    root: String,
    operation: String,
    events: BTreeMap<String, (i64, i64)>,
    reconciled: BTreeMap<i64, (i64, i64)>,
    actor: String,
    outcome: Option<(String, OutcomeKind)>,
}

impl AppliedModelDecision {
    pub(crate) fn group(&self) -> &str {
        &self.group
    }
    pub(crate) fn task(&self) -> &str {
        &self.root
    }
    pub(crate) fn operation(&self) -> &str {
        &self.operation
    }
    pub(crate) fn case_after(&self, id: i64) -> Option<(i64, i64)> {
        self.reconciled.get(&id).copied()
    }
    pub(crate) fn root_event(&self) -> Option<(i64, i64)> {
        self.events.get(&self.root).copied()
    }
    pub(crate) fn actor(&self) -> &str {
        &self.actor
    }
    pub(crate) fn outcome(&self) -> Option<(&str, OutcomeKind)> {
        self.outcome.as_ref().map(|(id, kind)| (id.as_str(), *kind))
    }
}

/// Canonical request syntax only; authentication and current authority are
/// checked by the persistence layer. Private fields prevent inconsistent keys.
pub(crate) struct NormalizedCandidateRequest {
    task: String,
    key: String,
    version: i64,
    candidate: CandidateDraft,
    canonical: String,
}

pub(crate) fn normalize_candidate_request(
    task: &str,
    request: CandidateRequest,
) -> Result<NormalizedCandidateRequest> {
    let CandidateRequest {
        version,
        key,
        mut candidate,
    } = request;
    let key = key.as_str();
    name(task)?;
    text_required(key, 128, "candidate key")?;
    text_required(&candidate.revision, 128, "candidate revision")?;
    text_required(&candidate.summary, 1024, "candidate summary")?;
    candidate
        .criterion_evidence
        .sort_by(|a, b| a.criterion_id.cmp(&b.criterion_id));
    for evidence in &mut candidate.criterion_evidence {
        evidence.references.sort();
        ensure!(
            !evidence.references.is_empty() && evidence.references.len() <= 16,
            "criterion_evidence_required"
        );
        ensure!(
            !evidence.references.windows(2).any(|v| v[0] == v[1]),
            "duplicate evidence reference"
        );
        for reference in &evidence.references {
            text_required(reference, 256, "evidence reference")?;
        }
    }
    let canonical = canonical(&("candidate", task, version, &candidate))?;
    Ok(NormalizedCandidateRequest {
        task: task.into(),
        key: key.into(),
        version,
        candidate,
        canonical,
    })
}

/// Historical replay carries no fresh model or Recovery transition authority.
#[derive(Debug)]
pub(crate) enum CandidateWrite {
    Historical(TaskResult),
    Fresh(PendingCandidateWrite),
}

/// Genuine persisted candidate awaiting its ordinary phase and retry receipt.
/// The caller owns the transaction and must roll it back on any later error.
/// No Clone/Deserialize: observation borrows the real witness; finishing consumes it.
#[derive(Debug)]
pub(crate) struct PendingCandidateWrite {
    result: TaskResult,
    finalization: Box<PendingCandidateFinalization>,
}

#[derive(Debug)]
struct PendingCandidateFinalization {
    actor: Mailbox,
    key: String,
    canonical: String,
    applied: AppliedModelDecision,
    phase: Option<crate::decision_recovery::ValidatedDecisionChange>,
}

impl PendingCandidateWrite {
    pub(crate) fn result(&self) -> &TaskResult {
        &self.result
    }

    pub(crate) fn applied(&self) -> &AppliedModelDecision {
        &self.finalization.applied
    }
}

#[derive(Debug)]
pub(crate) struct ModelDecisionApplication {
    pub(crate) view: TaskView,
    /// Historical exact retry returns its view without fresh after-state authority.
    pub(crate) applied: Option<AppliedModelDecision>,
}

pub(crate) async fn validate_applied_model_decision_tx(
    tx: &mut Transaction<'_, Sqlite>,
    applied: &AppliedModelDecision,
) -> Result<()> {
    crate::decision_recovery::reserve_home_tx(tx, &applied.group).await?;
    ensure!(
        applied.events.contains_key(&applied.root),
        "model_root_event_missing"
    );
    for (task, (event, version)) in &applied.events {
        let row = sqlx::query("SELECT e.operation,e.root_task,e.actor,e.snapshot,w.version FROM task_model_events e JOIN work_items w ON w.group_name=e.group_name AND w.id=e.task WHERE e.group_name=? AND e.task=? AND e.id=? AND e.task_version=?")
            .bind(&applied.group).bind(task).bind(event).bind(version).fetch_optional(&mut **tx).await?
            .context("actual_model_event_missing")?;
        ensure!(
            row.get::<String, _>("operation") == applied.operation
                && row.get::<String, _>("root_task") == applied.root
                && row.get::<String, _>("actor") == applied.actor
                && row.get::<i64, _>("version") == *version,
            "model_after_state_changed"
        );
        let observed: TaskView = serde_json::from_str(&row.get::<String, _>("snapshot"))?;
        let model = observed.model.context("model_event_contract_missing")?;
        if task == &applied.root {
            ensure!(
                model.current_outcome.as_deref()
                    == applied.outcome.as_ref().map(|(id, _)| id.as_str()),
                "model_outcome_witness_mismatch"
            );
            if let Some((outcome, kind)) = &applied.outcome {
                let actual: String = sqlx::query_scalar(
                    "SELECT kind FROM task_results WHERE group_name=? AND task=? AND id=?",
                )
                .bind(&applied.group)
                .bind(task)
                .bind(outcome)
                .fetch_one(&mut **tx)
                .await?;
                ensure!(actual == kind.as_str(), "model_outcome_kind_mismatch");
            }
        }
        let current = load_graph_tx(tx, &applied.group, std::slice::from_ref(task)).await?;
        let actual = current
            .records
            .get(task)
            .context("model_after_state_missing")?;
        ensure!(
            canonical(&actual.model)? == canonical(&model)?
                && actual.work.version == observed.work.version
                && actual.work.state == observed.work.state,
            "model_event_after_state_mismatch"
        );
    }
    for (id, (version, event)) in &applied.reconciled {
        let case = crate::decision_recovery::load_case_tx(tx, &applied.group, *id).await?;
        ensure!(
            case.version == *version,
            "reconciled_case_after_state_changed"
        );
        let found: i64 = sqlx::query_scalar("SELECT count(*) FROM decision_cases WHERE group_name=? AND id=? AND version=? AND reassessment_event=?")
            .bind(&applied.group).bind(id).bind(version).bind(event).fetch_one(&mut **tx).await?;
        ensure!(
            found == 1 && applied.events.values().any(|(actual, _)| actual == event),
            "actual_case_reconciliation_event_missing"
        );
    }
    Ok(())
}

async fn persist_changes_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    actor_name: &str,
    before: &Graph,
    graph: &mut Graph,
    operation: ModelOperation<'_>,
) -> Result<AppliedModelDecision> {
    let ModelOperation {
        root,
        id: operation,
        reason,
        now,
        origin,
    } = operation;
    validate_model_graph(graph)?;
    let mut changed = Vec::new();
    for (task, record) in &mut graph.records {
        let old = before.records.get(task);
        if old
            .map(|old| model_changed(old, record))
            .transpose()?
            .unwrap_or(true)
        {
            if let Some(old) = old {
                record.work.version = bump(old.work.version)?;
                record.work.updated = now;
                Store::work_write_tx(tx, &old.work, &record.work, actor_name, reason).await?;
            }
            persist_model_tx(tx, record).await?;
            changed.push(task.clone());
        }
    }
    let mut events = BTreeMap::new();
    for task in &changed {
        let record = &graph.records[task];
        let event = sqlx::query("INSERT INTO task_model_events(group_name,task,task_version,input_epoch,operation,origin,actor,root_task,reason,snapshot,previous_snapshot,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(group).bind(task).bind(record.work.version).bind(record.model.input_epoch).bind(operation)
            .bind(if task.as_str() != root { "system_invalidation" } else { match origin { ModelOrigin::Writer => "writer", ModelOrigin::PolicyReviewer => "policy_reviewer", ModelOrigin::SystemInvalidation => "system_invalidation", ModelOrigin::SystemMaterialization => "system_materialization" } }).bind(actor_name).bind(root).bind(reason)
            .bind(serde_json::to_string(&event_view(graph, task)?)?)
            .bind(before.records.get(task).map(|old| serde_json::to_string(&(&old.work, &old.model))).transpose()?)
            .bind(now).execute(&mut **tx).await?.last_insert_rowid();
        events.insert(task.clone(), (event, record.work.version));
    }
    ensure!(!changed.is_empty(), "model_mutation_requires_actual_event");
    let recovery =
        crate::decision_recovery::reconcile_model_changes_tx(tx, group, operation, now).await?;
    write_projection_tx(tx, group, graph).await?;
    let mut cases = BTreeMap::new();
    let mut reconciled = BTreeMap::new();
    for change in recovery {
        ensure!(
            change.source == format!("case:{}", change.case_id) && change.model_event > 0,
            "invalid_recovery_reconciliation_source"
        );
        cases.insert(change.case_id, change.case_version);
        reconciled.insert(change.case_id, (change.case_version, change.model_event));
    }
    refresh_recovery_projection_tx(tx, group, &cases).await?;
    let seeds = graph.records.keys().cloned().collect::<Vec<_>>();
    *graph = load_graph_tx(tx, group, &seeds).await?;
    validate_projection_tx(tx, group, graph).await?;
    let affected = graph.records.keys().cloned().collect::<Vec<_>>();
    crate::execution::sync_model_tx(tx, group, &affected, now).await?;
    Ok(AppliedModelDecision {
        group: group.into(),
        root: root.into(),
        operation: operation.into(),
        events,
        reconciled,
        actor: actor_name.into(),
        outcome: graph
            .records
            .get(root)
            .and_then(|record| record.model.current_outcome.as_ref())
            .and_then(|id| {
                graph
                    .results
                    .get(id)
                    .and_then(|result| result.outcome.map(|kind| (id.clone(), kind)))
            }),
    })
}
async fn insert_result_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    result: &TaskResult,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO task_results(id,group_name,task,kind,payload,created) VALUES(?,?,?,?,?,?)",
    )
    .bind(&result.id)
    .bind(group)
    .bind(&result.task)
    .bind(result.outcome.map_or("candidate", OutcomeKind::as_str))
    .bind(serde_json::to_string(result)?)
    .bind(result.created)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
fn initial_model(
    contract: Contract,
    authorization: Authorization,
    requirements: Vec<Requirement>,
    parent: Option<ParentLink>,
) -> TaskModel {
    TaskModel {
        contract,
        authorization,
        input_epoch: 1,
        parent,
        requirements,
        current_outcome: None,
        current_candidate: None,
        invalidation_causes: Vec::new(),
    }
}

impl Store {
    /// Create a finite task with its model, recovery projection and scheduler accounting.
    ///
    /// One transaction creates the business record, validates the combined graph,
    /// initializes finite default scheduling and saves the canonical receipt.
    /// Runtime qualification and saved group policy can still hold execution.
    ///
    /// # Errors
    /// Rejects invalid authority, graph, bounds, parent CAS, remote scope or retry conflicts.
    pub async fn task_create(
        &self,
        actor: &Mailbox,
        request: TaskCreate,
        now: i64,
    ) -> Result<TaskView> {
        let mut tx = self.pool().begin().await?;
        let result = Self::task_create_tx(&mut tx, actor, request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    // Caller owns commit/rollback. Failure requires rolling back the whole
    // transaction; successful writes include the exact original retry receipt.
    pub(crate) async fn task_create_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        mut request: TaskCreate,
        now: i64,
    ) -> Result<TaskView> {
        request_identity(&request.key, &request.reason)?;
        normalize_contract(
            &mut request.draft.contract,
            &mut request.draft.authorization,
        )?;
        normalize_graph(&mut request.draft.requirements, &request.draft.parent)?;
        ensure!(
            request.draft.work.state.is_open(),
            "new contracted task requires an unfinished state"
        );
        let canonical = canonical(&("create", &request))?;
        authenticate_tx(tx, actor).await?;
        if let Some(old) = replay_tx(tx, actor, &request.key, &canonical).await? {
            return Ok(old);
        }
        let seeds = graph_seeds(
            &request.draft.work.id,
            &request.draft.requirements,
            request.draft.parent.as_ref(),
        );
        let before = load_graph_tx(tx, &actor.group_name, &seeds).await?;
        validate_projection_tx(tx, &actor.group_name, &before).await?;
        ensure!(
            before.bindings.contains_key(&request.draft.work.owner),
            "contracted_remote_or_unavailable_owner"
        );
        let exists: i64 =
            sqlx::query_scalar("SELECT count(*) FROM work_items WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(&request.draft.work.id)
                .fetch_one(&mut **tx)
                .await?;
        ensure!(exists == 0, "task_identity_already_exists");
        let parents = affected_parents(None, request.draft.parent.as_ref());
        guard_parents(&before, &request.expected_parent_versions, &parents)?;
        let TaskDraft {
            work,
            contract,
            authorization,
            requirements,
            parent,
        } = request.draft;
        let work = Self::work_create_tx(tx, actor, work, now).await?;
        let id = work.id.clone();
        let mut graph = before.clone();
        graph.records.insert(
            id.clone(),
            Record {
                work,
                model: initial_model(contract, authorization, requirements, parent),
            },
        );
        record_parent_attachment(&mut graph, &id)?;
        let operation = uuid::Uuid::new_v4().to_string();
        validate_model_graph(&graph)?;
        validate_inheritance(&graph, &id)?;
        propagate_invalidation(&mut graph, &before, &operation)?;
        // Insert the new model before any requirement FK references it.
        persist_model_tx(tx, &graph.records[&id]).await?;
        persist_changes_tx(
            tx,
            &actor.group_name,
            &actor.name,
            &before,
            &mut graph,
            ModelOperation {
                root: &id,
                id: &operation,
                reason: &request.reason,
                now,
                origin: ModelOrigin::Writer,
            },
        )
        .await?;
        let result = view(&graph, &id)?;
        save_receipt_tx(tx, actor, &request.key, &canonical, &result).await?;
        Ok(result)
    }

    /// Explicitly adopt legacy work, retaining old evidence as history.
    ///
    /// # Errors
    /// Requires its home writer, current version, complete contract and exact parent guards.
    pub async fn task_adopt(
        &self,
        actor: &Mailbox,
        task: &str,
        request: TaskAdopt,
        now: i64,
    ) -> Result<TaskView> {
        let mut tx = self.pool().begin().await?;
        let result = Self::task_adopt_tx(&mut tx, actor, task, request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    // Caller owns commit/rollback. Failure requires rolling back the whole
    // transaction; successful writes include the exact original retry receipt.
    pub(crate) async fn task_adopt_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        task: &str,
        mut request: TaskAdopt,
        now: i64,
    ) -> Result<TaskView> {
        name(task)?;
        request_identity(&request.key, &request.reason)?;
        normalize_contract(&mut request.contract, &mut request.authorization)?;
        normalize_graph(&mut request.requirements, &request.parent)?;
        let canonical = canonical(&("adopt", task, &request))?;
        authenticate_tx(tx, actor).await?;
        if let Some(old) = replay_tx(tx, actor, &request.key, &canonical).await? {
            return Ok(old);
        }
        let seeds = graph_seeds(task, &request.requirements, request.parent.as_ref());
        let before = load_graph_tx(tx, &actor.group_name, &seeds).await?;
        validate_projection_tx(tx, &actor.group_name, &before).await?;
        ensure!(
            !before.records.contains_key(task),
            "already_contracted: adoption cannot reset lifetime state"
        );
        let old_work = Self::work_load_tx(tx, &actor.group_name, task).await?;
        ensure!(old_work.writer == actor.name, "designated_writer_required");
        ensure!(old_work.version == request.version, "task_version_conflict");
        ensure!(
            before.bindings.contains_key(&old_work.owner),
            "contracted_remote_or_unavailable_owner"
        );
        let parents = affected_parents(None, request.parent.as_ref());
        guard_parents(&before, &request.expected_parent_versions, &parents)?;
        let mut work = old_work.clone();
        if !work.state.is_open() {
            work.state = TaskState::Review;
        }
        work.accepted_revision = None;
        work.version = bump(work.version)?;
        work.updated = now;
        let mut graph = before.clone();
        graph.records.insert(
            task.into(),
            Record {
                work,
                model: initial_model(
                    request.contract,
                    request.authorization,
                    request.requirements,
                    request.parent,
                ),
            },
        );
        record_parent_attachment(&mut graph, task)?;
        let operation = uuid::Uuid::new_v4().to_string();
        validate_model_graph(&graph)?;
        validate_inheritance(&graph, task)?;
        propagate_invalidation(&mut graph, &before, &operation)?;
        Self::work_write_tx(
            tx,
            &old_work,
            &graph.records[task].work,
            &actor.name,
            &request.reason,
        )
        .await?;
        persist_model_tx(tx, &graph.records[task]).await?;
        persist_changes_tx(
            tx,
            &actor.group_name,
            &actor.name,
            &before,
            &mut graph,
            ModelOperation {
                root: task,
                id: &operation,
                reason: &request.reason,
                now,
                origin: ModelOrigin::Writer,
            },
        )
        .await?;
        let result = view(&graph, task)?;
        save_receipt_tx(tx, actor, &request.key, &canonical, &result).await?;
        Ok(result)
    }

    /// Inspect local model state without adopting or repairing records.
    ///
    /// # Errors
    /// Authentication, missing task, corrupt projection or bounded graph validation fails.
    pub async fn task_inspect(&self, actor: &Mailbox, task: &str) -> Result<TaskView> {
        name(task)?;
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let graph = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &graph).await?;
        let result = if graph.records.contains_key(task) {
            view(&graph, task)?
        } else {
            TaskView {
                schema_version: 1,
                work: Self::work_load_tx(&mut tx, &actor.group_name, task).await?,
                model: None,
                readiness: ModelReadiness { causes: Vec::new() },
                execution_hold: "legacy_untracked".into(),
            }
        };
        tx.commit().await?;
        Ok(result)
    }

    /// Read owned local work across all business states without advancing it.
    ///
    /// Terminal work remains visible for separately observed cleanup projections.
    /// Existing graph validators may reserve the SQLite writer; this method rolls
    /// back its read transaction and persists no retrieval, repair or event.
    ///
    /// # Errors
    /// Rejects stale or mismatched actors, foreign home stores, invalid bounds,
    /// and incomplete/corrupt graphs rather than returning an empty success.
    pub async fn task_tracking_page(
        &self,
        actor: &Mailbox,
        after: &str,
        limit: u32,
    ) -> Result<TaskTrackingPage> {
        ensure!((1..=50).contains(&limit), "task_tracking_page_limit");
        if !after.is_empty() {
            name(after)?;
        }
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let actor_home: i64 = sqlx::query_scalar("SELECT count(*) FROM mailboxes m JOIN groups g ON g.name=m.group_name JOIN node n ON n.id=g.home_machine WHERE m.id=? AND m.name=? AND m.group_name=? AND m.remote_machine IS NULL AND m.agent_state='registered'")
            .bind(actor.id).bind(&actor.name).bind(&actor.group_name).fetch_one(&mut *tx).await?;
        ensure!(actor_home == 1, "task_tracking_requires_actual_home_actor");
        let mut ids: Vec<String> = sqlx::query_scalar("SELECT id FROM work_items WHERE group_name=? AND (owner=? OR writer=?) AND id>? ORDER BY id LIMIT ?")
            .bind(&actor.group_name).bind(&actor.name).bind(&actor.name).bind(after)
            .bind(i64::from(limit) + 1).fetch_all(&mut *tx).await?;
        let has_more = ids.len() > usize::try_from(limit)?;
        ids.truncate(usize::try_from(limit)?);
        let next_cursor = if has_more { ids.last().cloned() } else { None };
        let graph = load_graph_tx(&mut tx, &actor.group_name, &ids).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &graph).await?;
        let mut items = Vec::with_capacity(ids.len());
        for id in ids {
            items.push(if graph.records.contains_key(&id) {
                view(&graph, &id)?
            } else {
                TaskView {
                    schema_version: 1,
                    work: Self::work_load_tx(&mut tx, &actor.group_name, &id).await?,
                    model: None,
                    readiness: ModelReadiness { causes: Vec::new() },
                    execution_hold: "legacy_untracked".into(),
                }
            });
        }
        tx.rollback().await?;
        Ok(TaskTrackingPage {
            items,
            next_cursor,
            has_more,
        })
    }

    /// Capture exact business inputs without claiming an execution slot.
    ///
    /// # Errors
    /// Requires the current home writer, or the assigned policy reviewer for
    /// Accept inputs, and qualifying predicates; stale or held work fails.
    pub async fn task_capture_inputs(
        &self,
        actor: &Mailbox,
        task: &str,
        version: i64,
        phase: Phase,
    ) -> Result<InputSnapshot> {
        name(task)?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        let graph = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &graph).await?;
        let record = graph.records.get(task).context("task model missing")?;
        let materialization = materialized_receipt_tx(&mut tx, &actor.group_name, task).await?;
        ensure!(
            record.work.writer == actor.name
                || (phase == Phase::Accept
                    && assigned_policy_reviewer(record, actor, materialization.as_ref())),
            "designated_writer_required"
        );
        ensure!(record.work.version == version, "task_version_conflict");
        let snapshot = capture_inputs_tx(&mut tx, &actor.group_name, task, phase).await?;
        tx.commit().await?;
        Ok(snapshot)
    }
}

fn apply_work_patch(work: &mut WorkItem, patch: &WorkPatch) -> Result<()> {
    ensure!(
        patch.accepted_revision.is_none(),
        "accepted_revision_requires_immutable_outcome"
    );
    if let Some(owner) = &patch.owner {
        work.owner = owner.clone();
    }
    if let Some(state) = patch.state {
        work.state = state;
    }
    if let Some(action) = &patch.next_action {
        work.next_action = action.clone();
    }
    if let Some(deadline) = patch.deadline {
        work.deadline = deadline;
    }
    if let Some(evidence) = &patch.evidence {
        work.evidence = evidence.clone();
    }
    crate::work::validate_fields(work)
}
fn normalize_decision(request: &mut TaskDecision) -> Result<()> {
    request_identity(&request.key, &request.reason)?;
    if let Change::Set(requirements) = &mut request.requirements {
        normalize_graph(requirements, &None)?;
    }
    if let Change::Set(parent) = &request.parent {
        normalize_graph(&mut [], parent)?;
    }
    Ok(())
}

fn successful_result(
    graph: &Graph,
    task: &str,
    kind: OutcomeKind,
    candidate: &str,
    actor: &str,
    now: i64,
) -> Result<TaskResult> {
    let record = graph.records.get(task).context("task missing")?;
    ensure!(
        record.model.current_candidate.as_deref() == Some(candidate),
        "current_candidate_required"
    );
    let candidate = graph.results.get(candidate).context("candidate missing")?;
    ensure!(
        candidate.task == task && candidate.outcome.is_none(),
        "candidate_identity_conflict"
    );
    ensure!(
        kind == OutcomeKind::Accepted
            || (kind == OutcomeKind::Completed
                && record.model.contract.completion == Completion::CompletionAllowed),
        "writer_acceptance_required"
    );
    let inputs = candidate
        .inputs
        .as_ref()
        .context("candidate inputs missing")?;
    ensure!(
        inputs.task == task
            && inputs.group == record.work.group_name
            && inputs.phase == Phase::Accept,
        "candidate_requires_exact_accept_inputs"
    );
    ensure!(
        snapshot_base_current(graph, inputs)?
            && snapshot_edges_current(graph, inputs, &valid_outcomes(graph)?)?,
        "stale_candidate_inputs"
    );
    ensure!(
        evaluate(graph, task, Phase::Accept)?.causes.is_empty(),
        "candidate_inputs_held"
    );
    ensure!(
        record
            .model
            .contract
            .criteria
            .iter()
            .map(|criterion| &criterion.id)
            .eq(candidate
                .criterion_evidence
                .iter()
                .map(|evidence| &evidence.criterion_id))
            && candidate
                .criterion_evidence
                .iter()
                .all(|evidence| !evidence.references.is_empty()),
        "exact_criterion_evidence_required"
    );
    Ok(TaskResult {
        id: uuid::Uuid::new_v4().to_string(),
        task: task.into(),
        outcome: Some(kind),
        revision: candidate.revision.clone(),
        summary: candidate.summary.clone(),
        criterion_evidence: candidate.criterion_evidence.clone(),
        inputs: Some(inputs.clone()),
        actor: actor.into(),
        created: now,
    })
}

impl Store {
    /// Apply a current writer decision and its deterministic negative consequences.
    ///
    /// Successful disposition requires an exact current Accept-phase candidate
    /// and the scheduler's real closure guard. Budget and parent changes preserve
    /// the scheduler's original lifetime accounting in the same transaction.
    ///
    /// # Errors
    /// Rejects unauthorized, stale, conflicting, incomplete or unsupported decisions atomically.
    pub async fn task_decide(
        &self,
        actor: &Mailbox,
        task: &str,
        request: TaskDecision,
        now: i64,
    ) -> Result<TaskView> {
        let mut tx = self.pool().begin().await?;
        let result = Self::task_decide_tx(&mut tx, actor, task, request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    // Caller owns commit/rollback. Failure requires rolling back the whole
    // transaction; successful writes include the exact original retry receipt.
    pub(crate) async fn task_decide_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        task: &str,
        request: TaskDecision,
        now: i64,
    ) -> Result<TaskView> {
        Ok(
            Self::task_decide_with_reconciliation_tx(tx, actor, task, request, now)
                .await?
                .view,
        )
    }

    pub(crate) async fn task_decide_with_reconciliation_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        task: &str,
        mut request: TaskDecision,
        now: i64,
    ) -> Result<ModelDecisionApplication> {
        name(task)?;
        normalize_decision(&mut request)?;
        // Normalize complete replacements before retry comparison, but validate
        // covering current authority after the reserved read below.
        if let Change::Set(contract) = &mut request.contract {
            contract.criteria.sort_by(|a, b| a.id.cmp(&b.id));
            contract.allowed_scope.sort();
        }
        if let Change::Set(authority) = &mut request.authorization {
            authority.approved_scope.sort();
        }
        let canonical = canonical(&("decide", task, &request))?;
        authenticate_tx(tx, actor).await?;
        if let Some(old) = replay_tx(tx, actor, &request.key, &canonical).await? {
            return Ok(ModelDecisionApplication {
                view: old,
                applied: None,
            });
        }
        let requirements = match &request.requirements {
            Change::Set(value) => value.as_slice(),
            Change::Keep => &[],
        };
        let parent = match &request.parent {
            Change::Set(value) => value.as_ref(),
            Change::Keep => None,
        };
        let seeds = graph_seeds(task, requirements, parent);
        let before = load_graph_tx(tx, &actor.group_name, &seeds).await?;
        validate_projection_tx(tx, &actor.group_name, &before).await?;
        let old = before.records.get(task).context("task model missing")?;
        ensure!(old.work.writer == actor.name, "designated_writer_required");
        ensure!(old.work.version == request.version, "task_version_conflict");
        let mut record = old.clone();
        apply_work_patch(&mut record.work, &request.work_patch)?;
        if let Change::Set(scope) = &request.scope {
            record.work.scope = scope.clone();
        }
        if let Change::Set(contract) = &request.contract {
            if contract
                .allowed_scope
                .iter()
                .any(|scope| !old.model.contract.allowed_scope.contains(scope))
            {
                ensure!(
                    matches!(request.authorization, Change::Set(_)),
                    "scope_expansion_requires_explicit_authorization"
                );
            }
            record.model.contract = contract.clone();
        }
        if let Change::Set(authority) = &request.authorization {
            record.model.authorization = authority.clone();
        }
        if let Change::Set(requirements) = &request.requirements {
            record.model.requirements = requirements.clone();
        }
        if let Change::Set(parent) = &request.parent {
            record.model.parent = parent.clone();
        }
        // A supplied unchanged parent is still an explicit graph decision.
        let parents = if matches!(request.parent, Change::Set(_)) {
            affected_parents(old.model.parent.as_ref(), record.model.parent.as_ref())
        } else {
            BTreeSet::new()
        };
        guard_parents(&before, &request.expected_parent_versions, &parents)?;
        normalize_contract(&mut record.model.contract, &mut record.model.authorization)?;
        normalize_graph(&mut record.model.requirements, &record.model.parent)?;
        guard_materialized_decision_update_tx(tx, &actor.group_name, old, &record).await?;
        if record.model.contract.budget != old.model.contract.budget
            || record.model.parent != old.model.parent
        {
            crate::execution::guard_model_change_tx(
                tx,
                &actor.group_name,
                task,
                &record.model.contract.budget,
                record
                    .model
                    .parent
                    .as_ref()
                    .map(|parent| parent.task.as_str()),
                now,
            )
            .await?;
        }
        ensure!(
            before.bindings.contains_key(&record.work.owner),
            "contracted_remote_or_unavailable_owner"
        );
        let semantic = record.model.contract != old.model.contract
            || record.model.authorization != old.model.authorization
            || record.model.requirements != old.model.requirements
            || record.model.parent != old.model.parent
            || record.work.owner != old.work.owner
            || record.work.scope != old.work.scope
            || record.work.next_action != old.work.next_action;
        if semantic {
            record.model.input_epoch = bump(record.model.input_epoch)?;
        }
        if request.clear_invalidation {
            record.model.invalidation_causes.clear();
        }
        if old.model.current_outcome.is_some()
            && record.work.state.is_open()
            && !old.work.state.is_open()
        {
            record.model.current_outcome = None;
            record.model.current_candidate = None;
            record.work.accepted_revision = None;
            if !semantic {
                record.model.input_epoch = bump(record.model.input_epoch)?;
            }
        }
        let mut new_result = None;
        match &request.outcome {
            OutcomeChange::Keep => {
                ensure!(
                    record.work.state.is_open()
                        || (record.work.state == old.work.state
                            && record.model.current_outcome.is_some()),
                    "terminal_task_requires_outcome"
                );
            }
            OutcomeChange::Withdraw => {
                record.model.current_outcome = None;
                record.model.current_candidate = None;
                record.work.accepted_revision = None;
                record.work.state = TaskState::Review;
                if record.model.input_epoch == old.model.input_epoch {
                    record.model.input_epoch = bump(record.model.input_epoch)?;
                }
            }
            OutcomeChange::Negative { kind, revision } => {
                ensure!(!kind.successful(), "negative_disposition_required");
                text_required(revision, 128, "negative result identity")?;
                let result = TaskResult {
                    id: uuid::Uuid::new_v4().to_string(),
                    task: task.into(),
                    outcome: Some(*kind),
                    revision: revision.clone(),
                    summary: request.reason.clone(),
                    criterion_evidence: Vec::new(),
                    inputs: None,
                    actor: actor.name.clone(),
                    created: now,
                };
                record.work.state = if *kind == OutcomeKind::Cancelled {
                    TaskState::Cancelled
                } else {
                    TaskState::Done
                };
                record.work.accepted_revision = None;
                record.model.current_outcome = Some(result.id.clone());
                if record.model.input_epoch == old.model.input_epoch {
                    record.model.input_epoch = bump(record.model.input_epoch)?;
                }
                new_result = Some(result);
            }
            OutcomeChange::Success { kind, .. } => {
                ensure!(kind.successful(), "successful_disposition_required");
                // A terminal state is derived from the selected result. It must
                // not hide the current lifecycle hold during candidate checks.
                if !record.work.state.is_open() {
                    let target = if *kind == OutcomeKind::Accepted {
                        TaskState::Accepted
                    } else {
                        TaskState::Done
                    };
                    ensure!(record.work.state == target, "outcome_state_conflict");
                    record.work.state = old.work.state;
                }
            }
        }
        crate::work::validate_fields(&record.work)?;
        let mut graph = before.clone();
        graph.records.insert(task.into(), record);
        if let OutcomeChange::Success { kind, candidate } = &request.outcome {
            let result = successful_result(&graph, task, *kind, candidate, &actor.name, now)?;
            crate::execution::guard_success_tx(tx, &actor.group_name, task).await?;
            let current = graph.records.get_mut(task).context("task missing")?;
            current.work.state = if *kind == OutcomeKind::Accepted {
                TaskState::Accepted
            } else {
                TaskState::Done
            };
            current.work.accepted_revision =
                (*kind == OutcomeKind::Accepted).then(|| result.revision.clone());
            current.model.current_outcome = Some(result.id.clone());
            new_result = Some(result);
        }
        if matches!(request.parent, Change::Set(_)) {
            let new_parent = graph.records[task].model.parent.clone();
            for parent in &parents {
                let changed_required = old.model.parent != new_parent
                    && old
                        .model
                        .parent
                        .iter()
                        .chain(new_parent.iter())
                        .any(|link| link.task == *parent && link.required);
                let parent_record = graph.records.get_mut(parent).context("parent missing")?;
                parent_record.work.version = bump(parent_record.work.version)?;
                if changed_required {
                    parent_record.model.input_epoch = bump(parent_record.model.input_epoch)?;
                }
            }
        }
        if let Some(result) = &new_result {
            graph.results.insert(result.id.clone(), result.clone());
        }
        validate_model_graph(&graph)?;
        if matches!(request.authorization, Change::Set(_))
            || matches!(request.contract, Change::Set(_))
        {
            validate_inheritance(&graph, task)?;
        }
        let operation = uuid::Uuid::new_v4().to_string();
        propagate_invalidation(&mut graph, &before, &operation)?;
        // An explicit clear cannot grant authority or rewrite a stale candidate.
        if request.clear_invalidation {
            ensure!(
                authority_current(&graph, task),
                "clear_invalidation_requires_current_authority"
            );
        }
        if let Some(result) = &new_result {
            insert_result_tx(tx, &actor.group_name, result).await?;
        }
        // Even a no-op writer decision has an auditable CAS receipt/version.
        if !model_changed(old, &graph.records[task])? {
            graph
                .records
                .get_mut(task)
                .context("task missing")?
                .work
                .updated = now;
            graph
                .records
                .get_mut(task)
                .context("task missing")?
                .work
                .version = bump(old.work.version)?;
        }
        let applied = persist_changes_tx(
            tx,
            &actor.group_name,
            &actor.name,
            &before,
            &mut graph,
            ModelOperation {
                root: task,
                id: &operation,
                reason: &request.reason,
                now,
                origin: ModelOrigin::Writer,
            },
        )
        .await?;
        if let Some(message) = request.resolve_message {
            let linked: Option<String> =
                sqlx::query_scalar("SELECT work_id FROM messages WHERE id=?")
                    .bind(message)
                    .fetch_optional(&mut **tx)
                    .await?
                    .flatten();
            ensure!(
                linked.as_deref() == Some(task),
                "resolved_message_must_reference_task"
            );
            Self::resolve_tx(tx, actor, message, &request.reason, None, now).await?;
        }
        let result = view(&graph, task)?;
        save_receipt_tx(tx, actor, &request.key, &canonical, &result).await?;
        Ok(ModelDecisionApplication {
            view: result,
            applied: Some(applied),
        })
    }

    /// Record a writer or assigned policy reviewer candidate against exact Accept inputs.
    ///
    /// # Errors
    /// Stale inputs, incomplete evidence, unauthorized writer or retry conflicts roll back everything.
    pub async fn task_candidate(
        &self,
        actor: &Mailbox,
        task: &str,
        request: CandidateRequest,
        now: i64,
    ) -> Result<TaskResult> {
        // Keep canonical syntax validation before opening the public transaction.
        let request = normalize_candidate_request(task, request)?;
        #[cfg(test)]
        let hint_key = request.key.clone();
        let mut tx = self.pool().begin().await?;
        let result =
            match Self::persist_candidate_with_reconciliation_tx(&mut tx, actor, request, now)
                .await?
            {
                CandidateWrite::Historical(result) => return Ok(result),
                CandidateWrite::Fresh(pending) => {
                    Self::finish_candidate_phase_and_receipt_tx(&mut tx, pending, now).await?
                }
            };
        tx.commit().await?;
        #[cfg(test)]
        candidate_core_tests::committed_before_hint(self.root(), actor, &hint_key).await;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    /// Persist the actual result/event and mandatory reconciliation, stopping
    /// before the ordinary Candidate phase changes its reconciled case version.
    /// No commit or hint; the caller must distinguish historical replay explicitly.
    pub(crate) async fn persist_candidate_with_reconciliation_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        request: NormalizedCandidateRequest,
        now: i64,
    ) -> Result<CandidateWrite> {
        let NormalizedCandidateRequest {
            task,
            key,
            version,
            candidate,
            canonical,
        } = request;
        let task = task.as_str();
        authenticate_tx(tx, actor).await?;
        if let Some(old) = replay_tx(tx, actor, &key, &canonical).await? {
            return Ok(CandidateWrite::Historical(old));
        }
        let before = load_graph_tx(tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(tx, &actor.group_name, &before).await?;
        let record = before.records.get(task).context("task model missing")?;
        let materialization = materialized_receipt_tx(tx, &actor.group_name, task).await?;
        ensure!(
            (record.work.writer == actor.name
                || assigned_policy_reviewer(record, actor, materialization.as_ref()))
                && record.work.version == version,
            "writer_or_task_version_conflict"
        );
        ensure!(
            candidate.inputs.task == task
                && candidate.inputs.group == actor.group_name
                && candidate.inputs.phase == Phase::Accept,
            "candidate_requires_exact_accept_inputs"
        );
        ensure!(
            evaluate(&before, task, Phase::Accept)?.causes.is_empty(),
            "candidate_inputs_held"
        );
        ensure!(
            snapshot_base_current(&before, &candidate.inputs)?
                && snapshot_edges_current(&before, &candidate.inputs, &valid_outcomes(&before)?)?,
            "stale_candidate_inputs"
        );
        let expected: Vec<_> = record
            .model
            .contract
            .criteria
            .iter()
            .map(|c| &c.id)
            .collect();
        let supplied: Vec<_> = candidate
            .criterion_evidence
            .iter()
            .map(|e| &e.criterion_id)
            .collect();
        ensure!(expected == supplied, "exact_criterion_evidence_required");
        let phase = if let Some(receipt) = materialization {
            ensure!(now < receipt.deadline, "materialized_deadline_expired");
            let case =
                crate::decision_recovery::load_case_tx(tx, &actor.group_name, receipt.case_id)
                    .await?;
            Some(
                crate::decision_recovery::prepare_decision_change_tx(
                    tx,
                    &actor.group_name,
                    case.id,
                    case.version,
                    task,
                    crate::decision_recovery::DecisionChangeKind::Candidate,
                    now,
                )
                .await?,
            )
        } else {
            None
        };
        let result = TaskResult {
            id: uuid::Uuid::new_v4().to_string(),
            task: task.into(),
            outcome: None,
            revision: candidate.revision,
            summary: candidate.summary,
            criterion_evidence: candidate.criterion_evidence,
            inputs: Some(candidate.inputs),
            actor: actor.name.clone(),
            created: now,
        };
        let mut graph = before.clone();
        graph
            .records
            .get_mut(task)
            .context("task missing")?
            .model
            .current_candidate = Some(result.id.clone());
        graph.results.insert(result.id.clone(), result.clone());
        insert_result_tx(tx, &actor.group_name, &result).await?;
        let operation = uuid::Uuid::new_v4().to_string();
        let applied = persist_changes_tx(
            tx,
            &actor.group_name,
            &actor.name,
            &before,
            &mut graph,
            ModelOperation {
                root: task,
                id: &operation,
                reason: "recorded immutable candidate",
                now,
                origin: if record.work.writer == actor.name {
                    ModelOrigin::Writer
                } else {
                    ModelOrigin::PolicyReviewer
                },
            },
        )
        .await?;
        Ok(CandidateWrite::Fresh(PendingCandidateWrite {
            result,
            finalization: Box::new(PendingCandidateFinalization {
                actor: actor.clone(),
                key,
                canonical,
                applied,
                phase,
            }),
        }))
    }

    /// Consume one genuine pending write and run the existing finalizer, model
    /// projection synchronization and keyed receipt. No commit or hint occurs.
    pub(crate) async fn finish_candidate_phase_and_receipt_tx(
        tx: &mut Transaction<'_, Sqlite>,
        pending: PendingCandidateWrite,
        now: i64,
    ) -> Result<TaskResult> {
        authenticate_tx(tx, &pending.finalization.actor).await?;
        // This also binds the persisted UUID operation, so rolled-back proofs
        // cannot become current merely because SQLite reuses an event row ID.
        validate_applied_model_decision_tx(tx, pending.applied()).await?;
        let result = pending.result();
        let row = sqlx::query("SELECT r.payload,m.current_candidate FROM task_results r JOIN task_models m ON m.group_name=r.group_name AND m.task=r.task WHERE r.group_name=? AND r.task=? AND r.id=? AND r.kind='candidate'")
            .bind(pending.applied().group()).bind(&result.task).bind(&result.id)
            .fetch_optional(&mut **tx).await?.context("pending_candidate_result_missing")?;
        ensure!(
            row.get::<Option<String>, _>("current_candidate").as_deref()
                == Some(result.id.as_str())
                && row.get::<String, _>("payload") == serde_json::to_string(result)?,
            "pending_candidate_result_changed"
        );
        let PendingCandidateWrite {
            result,
            finalization,
        } = pending;
        let PendingCandidateFinalization {
            actor,
            key,
            canonical,
            applied,
            phase,
        } = *finalization;
        ensure!(
            replay_tx::<TaskResult>(tx, &actor, &key, &canonical)
                .await?
                .is_none(),
            "candidate_receipt_already_finished"
        );
        if let Some(phase) = phase {
            crate::decision_recovery::finalize_decision_change_tx(tx, &phase, &applied, now)
                .await?;
            sync_after_decision_phase_tx(tx, &actor.group_name, &result.task, now).await?;
        }
        save_receipt_tx(tx, &actor, &key, &canonical, &result).await?;
        Ok(result)
    }

    /// Retrieve immutable result history, including withdrawn or stale outcomes.
    ///
    /// # Errors
    /// Authentication, task visibility or decoding fails.
    pub async fn task_results(
        &self,
        actor: &Mailbox,
        task: &str,
        after: Option<i64>,
    ) -> Result<TaskResultsPage> {
        name(task)?;
        let cursor = after.unwrap_or(0);
        ensure!(cursor >= 0, "invalid_result_cursor");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        Self::work_load_tx(&mut tx, &actor.group_name, task).await?;
        let mut rows = sqlx::query("SELECT sequence,payload FROM task_results WHERE group_name=? AND task=? AND sequence>? ORDER BY sequence LIMIT 101")
            .bind(&actor.group_name).bind(task).bind(cursor).fetch_all(&mut *tx).await?;
        let has_more = rows.len() > 100;
        rows.truncate(100);
        let next_cursor = rows
            .last()
            .map(|row| row.try_get("sequence"))
            .transpose()?
            .unwrap_or(cursor);
        let items = rows
            .iter()
            .map(|row| Ok(serde_json::from_str(&row.try_get::<String, _>("payload")?)?))
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(TaskResultsPage {
            items,
            next_cursor,
            has_more,
        })
    }
}

fn validate_inheritance(graph: &Graph, task: &str) -> Result<()> {
    let record = graph.records.get(task).context("task missing")?;
    if let AuthoritySource::Parent { task: parent } = &record.model.authorization.source {
        let parent = graph.records.get(parent).context("parent missing")?;
        ensure!(
            parent.model.contract.allow_delegation,
            "parent_delegation_not_authorized"
        );
        ensure!(
            record
                .model
                .authorization
                .approved_scope
                .iter()
                .all(|scope| parent.model.contract.allowed_scope.contains(scope)
                    && parent.model.authorization.approved_scope.contains(scope)),
            "child_scope_not_subset"
        );
    }
    Ok(())
}

/// Current semantic input status, distinct from runnable or successful execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputValidity {
    /// Every concrete input and binding still matches.
    Current,
    /// The captured artifact requires explicit revalidation.
    Stale,
}

/// Exact task source selected by a decision or recovery case.
///
/// This contains consistency guards, not delegated authority. Case/episode and
/// persisted grant validation remain the composing owners' responsibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSourceExpectation {
    /// Locally authoritative source group.
    pub group: String,
    /// Contracted source task.
    pub task: String,
    /// Exact observed business version, including note-only changes.
    pub task_version: i64,
    /// Exact semantic epoch.
    pub input_epoch: i64,
    /// Original current candidate, including explicit absence.
    pub current_candidate: Option<String>,
    /// Original current outcome, including explicit absence.
    pub current_outcome: Option<String>,
}

impl TryFrom<&TaskView> for TaskSourceExpectation {
    type Error = anyhow::Error;

    fn try_from(view: &TaskView) -> Result<Self> {
        let model = view.model.as_ref().context("contracted_source_required")?;
        Ok(Self {
            group: view.work.group_name.clone(),
            task: view.work.id.clone(),
            task_version: view.work.version,
            input_epoch: model.input_epoch,
            current_candidate: model.current_candidate.clone(),
            current_outcome: model.current_outcome.clone(),
        })
    }
}

/// Validate an exact decision source without granting permission to dispose it.
///
/// Held or revoked work can still need a responsible decision, so this helper
/// reports its real readiness instead of requiring runnable work. Actual source
/// writes must additionally use the authenticated writer path or a specifically
/// permitted persisted policy operation. It never commits or changes the source.
///
/// # Errors
/// Missing/untracked source, changed version/epoch/candidate/outcome, incomplete
/// model graph or an unavailable external projection owner fails closed.
pub async fn validate_task_source_tx(
    tx: &mut Transaction<'_, Sqlite>,
    expected: &TaskSourceExpectation,
) -> Result<TaskView> {
    name(&expected.task)?;
    reserve_group_tx(tx, &expected.group).await?;
    let graph = load_graph_tx(tx, &expected.group, std::slice::from_ref(&expected.task)).await?;
    validate_projection_tx(tx, &expected.group, &graph).await?;
    let current = view(&graph, &expected.task)?;
    ensure!(
        TaskSourceExpectation::try_from(&current)? == *expected,
        "decision_source_conflict"
    );
    Ok(current)
}

// Internal owner seams. A real owner must hold a writer reservation before
// composing these with attempt, phase or source/case writes. Reserving the group
// here prevents an accidental unreserved graph check from admitting a race.
async fn reserve_group_tx(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<()> {
    let result = sqlx::query("UPDATE groups SET paused=paused WHERE name=?")
        .bind(group)
        .execute(&mut **tx)
        .await?;
    ensure!(result.rows_affected() == 1, "group missing");
    Ok(())
}

pub(crate) async fn evaluate_task_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
    phase: Phase,
) -> Result<ModelReadiness> {
    reserve_group_tx(tx, group).await?;
    let graph = load_graph_tx(tx, group, &[task.to_owned()]).await?;
    validate_projection_tx(tx, group, &graph).await?;
    evaluate(&graph, task, phase)
}

pub(crate) async fn capture_inputs_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
    phase: Phase,
) -> Result<InputSnapshot> {
    reserve_group_tx(tx, group).await?;
    let graph = load_graph_tx(tx, group, &[task.to_owned()]).await?;
    validate_projection_tx(tx, group, &graph).await?;
    capture(&graph, task, phase)
}

pub(crate) async fn validate_inputs_tx(
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &InputSnapshot,
) -> Result<InputValidity> {
    reserve_group_tx(tx, &snapshot.group).await?;
    let graph = load_graph_tx(tx, &snapshot.group, std::slice::from_ref(&snapshot.task)).await?;
    validate_projection_tx(tx, &snapshot.group, &graph).await?;
    Ok(
        if snapshot_base_current(&graph, snapshot)?
            && snapshot_edges_current(&graph, snapshot, &valid_outcomes(&graph)?)?
        {
            InputValidity::Current
        } else {
            InputValidity::Stale
        },
    )
}

/// Writer authority checked in the caller's current transaction. Only the
/// provisioning helper can construct it; runtime consumes it in that same
/// transaction when persisting the protected binding.
#[derive(Debug)]
pub(crate) struct ArtifactBindingAuthority {
    provenance: ArtifactBindingProvenance,
}

impl ArtifactBindingAuthority {
    pub(crate) fn into_provenance(self) -> ArtifactBindingProvenance {
        self.provenance
    }
}

/// Immutable stored evidence of binding provenance, not a permission token.
/// Runtime may deserialize this only from its protected original binding or
/// attempt row; public/native requests cannot supply replacement provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactBindingProvenance {
    schema_version: u16,
    group: String,
    task: String,
    issuer_mailbox: i64,
    issuer: String,
    issuer_binding_version: i64,
    task_version_at_binding: i64,
    contract_json: String,
    contract_digest: String,
    scope_unit: String,
}

impl ArtifactBindingProvenance {
    pub(crate) fn group(&self) -> &str {
        &self.group
    }
    pub(crate) fn task(&self) -> &str {
        &self.task
    }
    pub(crate) fn issuer_mailbox(&self) -> i64 {
        self.issuer_mailbox
    }
    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }
    pub(crate) fn issuer_binding_version(&self) -> i64 {
        self.issuer_binding_version
    }
    pub(crate) fn task_version_at_binding(&self) -> i64 {
        self.task_version_at_binding
    }
    pub(crate) fn contract_json(&self) -> &str {
        &self.contract_json
    }
    pub(crate) fn contract_digest(&self) -> &str {
        &self.contract_digest
    }
    pub(crate) fn scope_unit(&self) -> &str {
        &self.scope_unit
    }
}

fn artifact_contract_digest(contract_json: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"agent-mail/artifact-binding-contract/v1\0");
    digest.update(contract_json.as_bytes());
    format!("{:x}", digest.finalize())
}

async fn artifact_binding_contract_tx(
    tx: &mut Transaction<'_, Sqlite>,
    graph: &Graph,
    task: &str,
    scope_unit: &str,
    now: i64,
) -> Result<String> {
    let record = graph.records.get(task).context("task model missing")?;
    ensure!(
        authority_current(graph, task),
        "artifact_binding_authority_unavailable"
    );
    let mut contract = record.model.contract.clone();
    let mut authority = record.model.authorization.clone();
    normalize_contract(&mut contract, &mut authority)?;
    let contract_json = canonical(&contract)?;
    ensure!(
        contract_json == canonical(&record.model.contract)?,
        "artifact_binding_contract_not_canonical"
    );
    ensure!(
        contract.allowed_scope.iter().any(|s| s == scope_unit)
            && authority.approved_scope.iter().any(|s| s == scope_unit),
        "artifact_binding_scope_not_authorized"
    );
    if let Some(receipt) = materialized_receipt_tx(tx, &record.work.group_name, task).await? {
        ensure!(now < receipt.deadline, "materialized_deadline_expired");
    }
    Ok(contract_json)
}

/// Check the actual writer and provisioning CAS without claiming readiness or
/// capturing execution inputs. Runtime persists the resulting provenance and
/// binding atomically in this transaction. No commit or filesystem I/O occurs.
pub(crate) async fn validate_artifact_binding_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    task: &str,
    expected_task_version: i64,
    scope_unit: &str,
    now: i64,
) -> Result<ArtifactBindingAuthority> {
    name(task)?;
    text_required(scope_unit, 256, "artifact binding scope")?;
    authenticate_tx(tx, actor).await?;
    let graph = load_graph_tx(tx, &actor.group_name, &[task.to_owned()]).await?;
    validate_projection_tx(tx, &actor.group_name, &graph).await?;
    let record = graph.records.get(task).context("task model missing")?;
    ensure!(
        record.work.writer == actor.name,
        "designated_writer_required"
    );
    ensure!(
        record.work.version == expected_task_version,
        "task_version_conflict"
    );
    let contract_json = artifact_binding_contract_tx(tx, &graph, task, scope_unit, now).await?;
    Ok(ArtifactBindingAuthority {
        provenance: ArtifactBindingProvenance {
            schema_version: 1,
            group: actor.group_name.clone(),
            task: task.into(),
            issuer_mailbox: actor.id,
            issuer: actor.name.clone(),
            issuer_binding_version: actor.binding_version,
            task_version_at_binding: record.work.version,
            contract_digest: artifact_contract_digest(&contract_json),
            contract_json,
            scope_unit: scope_unit.into(),
        },
    })
}

/// Revalidate original protected provenance without impersonating its writer.
/// Runtime separately authenticates its actual producer and original persisted
/// binding/attempt in this transaction. It must also call the publication guard
/// at pin and selection. This check grants no execution or reusable authority.
pub(crate) async fn validate_artifact_binding_current_tx(
    tx: &mut Transaction<'_, Sqlite>,
    original: &ArtifactBindingProvenance,
    inputs: &InputSnapshot,
    scope_unit: &str,
    now: i64,
) -> Result<()> {
    ensure!(
        original.schema_version == 1,
        "artifact_binding_schema_unsupported"
    );
    name(original.group())?;
    name(original.task())?;
    name(original.issuer())?;
    text_required(original.scope_unit(), 256, "artifact binding scope")?;
    text_required(
        original.contract_json(),
        MAX_REQUEST,
        "artifact binding contract",
    )?;
    ensure!(
        original.issuer_mailbox() > 0
            && original.issuer_binding_version() > 0
            && original.task_version_at_binding() > 0,
        "artifact_binding_identity_invalid"
    );
    bounded(
        &serde_json::to_string(original)?,
        256 * 1024,
        "artifact binding provenance",
    )?;
    ensure!(
        original.group() == inputs.group
            && original.task() == inputs.task
            && original.scope_unit() == scope_unit,
        "artifact_binding_input_mismatch"
    );
    ensure!(
        original.contract_digest() == artifact_contract_digest(original.contract_json()),
        "artifact_binding_contract_digest_mismatch"
    );
    reserve_group_tx(tx, original.group()).await?;
    let issuer: i64 = sqlx::query_scalar("SELECT count(*) FROM mailboxes m JOIN groups g ON g.name=m.group_name JOIN node n ON n.id=g.home_machine WHERE m.id=? AND m.name=? AND m.group_name=? AND m.binding_version=? AND m.remote_machine IS NULL AND m.agent_state='registered'")
        .bind(original.issuer_mailbox()).bind(original.issuer()).bind(original.group())
        .bind(original.issuer_binding_version()).fetch_one(&mut **tx).await?;
    ensure!(issuer == 1, "artifact_binding_issuer_changed");
    let graph = load_graph_tx(tx, original.group(), &[original.task().to_owned()]).await?;
    validate_projection_tx(tx, original.group(), &graph).await?;
    let record = graph
        .records
        .get(original.task())
        .context("task model missing")?;
    ensure!(
        record.work.writer == original.issuer(),
        "artifact_binding_writer_changed"
    );
    let contract =
        artifact_binding_contract_tx(tx, &graph, original.task(), scope_unit, now).await?;
    ensure!(
        contract == original.contract_json(),
        "artifact_binding_contract_changed"
    );
    ensure!(
        snapshot_base_current(&graph, inputs)?
            && snapshot_edges_current(&graph, inputs, &valid_outcomes(&graph)?)?,
        "stale_artifact_binding_inputs"
    );
    Ok(())
}

/// Model facts observed for one publication within the caller's transaction.
///
/// This is audit data, not a reusable permission token. The caller must also
/// validate its original persisted attempt, fence, runtime and destination in
/// this same writer transaction before recording any new effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicationModelObservation {
    /// Group containing the task and every resolved input.
    pub group: String,
    /// Task whose original input snapshot was checked.
    pub task: String,
    /// Current business version, independent of the snapshot's audit version.
    pub task_version: i64,
    /// Current semantic input epoch; it must equal the original snapshot.
    pub input_epoch: i64,
    /// Unchanged original phase; Execute is never upgraded to Accept.
    pub phase: Phase,
    /// Exact literal scope unit covered by the current contract and authority.
    pub scope_unit: String,
}

/// Check the model's current publication guards in a caller-owned transaction.
///
/// The snapshot must come from the caller's original persisted attempt or
/// candidate. This helper does not establish its provenance or authenticate an
/// execution: composing owners must do that in the same transaction. It neither
/// commits nor performs runtime/filesystem I/O. An exact historical effect retry
/// must return its old receipt without using this observation for another write.
///
/// # Errors
/// Rejects stale inputs, held phase/authority/actions, undeclared scope and
/// incomplete authoritative source projections. Every source owner must be
/// integrated before a newer schema can be treated as complete.
pub async fn validate_publication_inputs_tx(
    tx: &mut Transaction<'_, Sqlite>,
    original: &InputSnapshot,
    declared_scope_unit: &str,
) -> Result<PublicationModelObservation> {
    text_required(declared_scope_unit, 256, "publication scope unit")?;
    reserve_group_tx(tx, &original.group).await?;
    let schema: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        INTEGRATED_OWNER_SCHEMAS.contains(&schema),
        "publication_projection_owner_integration_unavailable: schema {schema}"
    );
    let graph = load_graph_tx(tx, &original.group, std::slice::from_ref(&original.task)).await?;
    // The real scheduler and recovery enumerators inspect source rows even if
    // every cached projection row has been deleted.
    validate_projection_tx(tx, &original.group, &graph).await?;
    let valid = valid_outcomes(&graph)?;
    ensure!(
        snapshot_base_current(&graph, original)?
            && snapshot_edges_current(&graph, original, &valid)?,
        "stale_publication_inputs"
    );
    let readiness = evaluate(&graph, &original.task, original.phase)?;
    ensure!(
        readiness.causes.is_empty(),
        "publication_model_hold: {}",
        serde_json::to_string(&readiness)?
    );
    for action in [
        TaskAction::PublishArtifact,
        TaskAction::Scope(declared_scope_unit.into()),
    ] {
        ensure!(
            external_action_causes(&graph, &original.task, &action, false)?.is_empty(),
            "publication_action_held"
        );
    }
    let record = graph
        .records
        .get(&original.task)
        .context("task model missing")?;
    ensure!(
        record
            .model
            .contract
            .allowed_scope
            .iter()
            .any(|scope| scope == declared_scope_unit)
            && record
                .model
                .authorization
                .approved_scope
                .iter()
                .any(|scope| scope == declared_scope_unit),
        "publication_scope_not_authorized"
    );
    Ok(PublicationModelObservation {
        group: original.group.clone(),
        task: original.task.clone(),
        task_version: record.work.version,
        input_epoch: record.model.input_epoch,
        phase: original.phase,
        scope_unit: declared_scope_unit.into(),
    })
}

/// Narrow authority to judge one milestone; it cannot accept results or grant scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JudgeGrant {
    /// Stable grant identifier within the task.
    pub id: String,
    /// Authenticated local decider mailbox, independent of runtime rebinding.
    pub decider: String,
    /// Exact milestone interpreted by the progress owner.
    pub milestone: String,
    /// Contract criterion IDs this milestone may qualify.
    pub criterion_ids: Vec<String>,
    /// Real authorization reference attested by the source writer.
    pub authority_ref: String,
    /// Explicit revocation; histories are retained.
    pub revoked: bool,
}

/// Writer operation for a narrow milestone judgment grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JudgeGrantDecision {
    /// Retry identity.
    pub key: String,
    /// Observed source task version.
    pub task_version: i64,
    /// None creates; otherwise exact prior grant revision.
    pub expected_revision: Option<i64>,
    /// Audited reason.
    pub reason: String,
    /// Full explicit grant or revocation.
    pub grant: JudgeGrant,
}

/// Immutable grant-operation receipt; replays do not grant fresh authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JudgeGrantReceipt {
    /// Source task identity.
    pub task: String,
    /// Observed source task version.
    pub task_version: i64,
    /// New grant revision.
    pub revision: i64,
    /// Exact audited grant.
    pub grant: JudgeGrant,
}

impl Store {
    /// Persist a source-writer grant scoped to one progress milestone.
    ///
    /// This does not qualify a report: the progress owner must validate its
    /// milestone policy and append the judgment in the same reserved transaction.
    ///
    /// # Errors
    /// Unauthorized writer, remote decider, stale versions, missing criteria or conflicting replay fails.
    pub async fn task_judge_grant(
        &self,
        actor: &Mailbox,
        task: &str,
        mut request: JudgeGrantDecision,
        now: i64,
    ) -> Result<JudgeGrantReceipt> {
        name(task)?;
        request_identity(&request.key, &request.reason)?;
        name(&request.grant.id)?;
        name(&request.grant.decider)?;
        name(&request.grant.milestone)?;
        text_required(
            &request.grant.authority_ref,
            256,
            "judgment authority reference",
        )?;
        request.grant.criterion_ids.sort();
        ensure!(
            !request.grant.criterion_ids.is_empty()
                && request.grant.criterion_ids.len() <= 32
                && !request.grant.criterion_ids.windows(2).any(|v| v[0] == v[1]),
            "distinct_judgment_criteria_required"
        );
        let canonical = canonical(&("judge_grant", task, &request))?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            return Ok(old);
        }
        let graph = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        let record = graph.records.get(task).context("task model missing")?;
        ensure!(
            record.work.writer == actor.name,
            "designated_writer_required"
        );
        ensure!(
            record.work.version == request.task_version,
            "task_version_conflict"
        );
        ensure!(
            graph.bindings.contains_key(&request.grant.decider),
            "local_judgment_decider_required"
        );
        ensure!(
            request.grant.criterion_ids.iter().all(|id| record
                .model
                .contract
                .criteria
                .iter()
                .any(|c| &c.id == id)),
            "judgment_criterion_missing"
        );
        let prior = sqlx::query(
            "SELECT revision,kind FROM task_grants WHERE group_name=? AND task=? AND id=?",
        )
        .bind(&actor.group_name)
        .bind(task)
        .bind(&request.grant.id)
        .fetch_optional(&mut *tx)
        .await?;
        let prior_revision = prior
            .as_ref()
            .map(|r| r.try_get::<i64, _>("revision"))
            .transpose()?;
        ensure!(
            prior_revision == request.expected_revision,
            "grant_revision_conflict"
        );
        if let Some(prior) = prior {
            ensure!(
                prior.try_get::<String, _>("kind")? == "progress_judge",
                "grant_kind_conflict"
            );
        }
        let revision = bump(prior_revision.unwrap_or(0))?;
        let payload = serde_json::to_string(&request.grant)?;
        sqlx::query("INSERT INTO task_grants(group_name,task,id,revision,kind,payload,revoked) VALUES(?,?,?,?,'progress_judge',?,?) ON CONFLICT(group_name,task,id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,revoked=excluded.revoked")
            .bind(&actor.group_name).bind(task).bind(&request.grant.id).bind(revision).bind(&payload).bind(request.grant.revoked).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_grant_history(group_name,task,id,revision,payload,actor,reason,created) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&actor.group_name).bind(task).bind(&request.grant.id).bind(revision).bind(payload).bind(&actor.name).bind(&request.reason).bind(now).execute(&mut *tx).await?;
        let result = JudgeGrantReceipt {
            task: task.into(),
            task_version: request.task_version,
            revision,
            grant: request.grant,
        };
        save_receipt_tx(&mut tx, actor, &request.key, &canonical, &result).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
}

/// Finite source dispositions authorized by a standing decision policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAction {
    /// Return evidence and advice to the original writer.
    Recommend,
    /// Original writer may grant a bounded scheduler strategy continuation.
    ContinueStrategy,
    /// Original writer may accept an exact current candidate.
    Accept,
    /// Original writer may complete under a compatible contract.
    Complete,
    /// Original writer may cancel the source.
    Cancel,
    /// Original writer may fail the source.
    Fail,
}

/// Exact observed source input, including original Mail recipient identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionSourceExpectation {
    /// Original task revision or original delivery selector.
    pub source: crate::decision_recovery::Obligation,
    /// Semantic epoch for contracted work; absent for legacy work and Mail.
    pub input_epoch: Option<i64>,
    /// Exact current immutable candidate, if any.
    pub candidate: Option<String>,
    /// Exact current immutable outcome, if any.
    pub outcome: Option<String>,
}

/// Persisted finite assignment template; it grants no source disposition itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionPolicy {
    /// Stable policy identity within the group.
    pub id: String,
    /// Finite task contract derived verbatim when materializing an episode.
    pub contract: Contract,
    /// Explicit allowed source dispositions; reviewer advice never executes them.
    pub actions: Vec<DecisionAction>,
    /// Local initial reviewer; absence assigns the original writer.
    pub reviewer: Option<String>,
    /// Permit the single reviewer-to-writer transition without new allowance.
    pub allow_writer_fallback: bool,
    /// Absolute policy and task deadline ceiling.
    pub deadline: i64,
    /// Audited original source-writer consent.
    pub authority_ref: String,
    /// Revocation prevents new mutations, without deleting history or receipts.
    pub revoked: bool,
}

/// Authenticated source-writer operation on a standing decision policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionPolicyDecision {
    /// Exact retry identity.
    pub key: String,
    /// Current policy revision, or absent for first adoption.
    pub expected_revision: Option<i64>,
    /// Current original source guard; a linked task cannot substitute for Mail.
    pub source: DecisionSourceExpectation,
    /// Complete finite policy or explicit revocation.
    pub policy: DecisionPolicy,
    /// Audited explanation.
    pub reason: String,
}

/// Historical policy receipt; reading it never creates new authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionPolicyReceipt {
    /// Stable canonical original-source identity.
    pub source_key: String,
    /// Original business writer or Mail sender.
    pub writer: String,
    /// Stored policy revision.
    pub revision: i64,
    /// Exact normalized policy.
    pub policy: DecisionPolicy,
}

/// Exact finite reviewer-to-writer transition; no assignment or budget is supplied.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionWriterFallback {
    /// Immutable request identity within the actual actor mailbox.
    pub key: String,
    /// Observed decision task version.
    pub version: i64,
    /// Observed original recovery case version.
    pub case_version: i64,
    /// Exact standing policy revision that created this decision.
    pub policy_revision: i64,
    /// Audited explanation of the single phase change.
    pub reason: String,
}

/// Original-writer request for one atomic source continuation and decision outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionContinuation {
    /// Exact outer retry identity, distinct from the ordinary outcome key.
    pub key: String,
    /// Original case revision observed before the private stage.
    pub case_version: i64,
    /// Standing consent revision used by the actual materialization.
    pub policy_revision: i64,
    /// Existing scheduler request; its finite bounds still require validation.
    pub continuation: crate::execution::ContinueStrategy,
    /// Ordinary successful candidate selection, with no other edits.
    pub decision: TaskDecision,
}

/// Actual committed source disposition and ordinary decision outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionContinuationReceipt {
    /// Final ordinary decision view after graph and scheduler synchronization.
    pub decision: TaskView,
    /// Final handled case, with its original responsibility and boundaries.
    pub case: crate::decision_recovery::DecisionCase,
}

impl Store {
    /// Apply finite source continuation and the ordinary decision outcome together.
    ///
    /// Recovery stages only the exact original case's typed dependency under a
    /// private before/after proof. The source grant, ordinary outcome and case
    /// finalizer commit together. Held rolls back the entire staged transaction.
    ///
    /// # Errors
    /// Rejects stale authority, policy, candidate or case; unrelated request
    /// edits, unresolved cleanup and persistence failures roll back all effects.
    pub async fn decision_continue_strategy(
        &self,
        actor: &Mailbox,
        task: &str,
        mut request: DecisionContinuation,
        now: i64,
    ) -> Result<crate::execution::Checked<DecisionContinuationReceipt>> {
        use crate::execution::Checked;
        name(task)?;
        request_identity(&request.key, &request.decision.reason)?;
        normalize_decision(&mut request.decision)?;
        ensure!(
            request.key != request.decision.key,
            "distinct_decision_continuation_keys_required"
        );
        let canonical = canonical(&("decision_continue_strategy", task, &request))?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            return Ok(Checked::Ready(old));
        }
        ensure!(
            serde_json::to_value(&request.decision.work_patch)?
                == serde_json::to_value(WorkPatch::default())?
                && matches!(&request.decision.scope, Change::Keep)
                && matches!(&request.decision.contract, Change::Keep)
                && matches!(&request.decision.authorization, Change::Keep)
                && matches!(&request.decision.requirements, Change::Keep)
                && matches!(&request.decision.parent, Change::Keep)
                && request.decision.expected_parent_versions.is_empty()
                && !request.decision.clear_invalidation
                && request.decision.resolve_message.is_none(),
            "decision_continuation_requires_outcome_only"
        );
        let receipt = materialized_receipt_tx(&mut tx, &actor.group_name, task)
            .await?
            .context("actual_materialized_decision_required")?;
        ensure!(
            actor.id == receipt.issuer && actor.name == receipt.writer,
            "original_source_writer_required"
        );
        ensure!(
            request.policy_revision == receipt.policy_revision
                && receipt
                    .policy
                    .actions
                    .contains(&DecisionAction::ContinueStrategy),
            "explicit_strategy_continuation_policy_required"
        );
        ensure!(
            now < receipt.deadline && request.continuation.expires_at <= receipt.deadline,
            "materialized_deadline_expired_or_exceeded"
        );
        let before = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &before).await?;
        ensure!(
            authority_current(&before, task),
            "current_materialized_authority_required"
        );
        let old = before.records.get(task).context("decision model missing")?;
        ensure!(
            old.work.version == request.decision.version,
            "task_version_conflict"
        );
        let OutcomeChange::Success { kind, candidate } = &request.decision.outcome else {
            anyhow::bail!("ordinary_decision_success_required");
        };
        // This checks actual candidate inputs before either source mutation.
        // The ordinary writer path checks again against its proposed model.
        successful_result(&before, task, *kind, candidate, &actor.name, now)?;
        let case =
            crate::decision_recovery::load_case_tx(&mut tx, &actor.group_name, receipt.case_id)
                .await?;
        let source: crate::execution::ExecutionSourceGuard = serde_json::from_value(
            case.execution_guard
                .clone()
                .context("actual_execution_source_required")?,
        )?;
        ensure!(
            matches!(&case.current_source.source,
                crate::decision_recovery::Obligation::Task { id, .. }
                    if id == &source.cause_ref.source_task && id != task),
            "original_work_source_required"
        );
        let phase = crate::decision_recovery::prepare_decision_change_tx(
            &mut tx,
            &actor.group_name,
            receipt.case_id,
            request.case_version,
            task,
            crate::decision_recovery::DecisionChangeKind::Outcome,
            now,
        )
        .await?;
        let authority = crate::decision_recovery::validate_case_authority_tx(
            &mut tx,
            actor,
            receipt.case_id,
            request.case_version,
            &source,
        )
        .await?;
        let stage = crate::decision_recovery::stage_decision_continuation_tx(
            &mut tx, actor, phase, authority, now,
        )
        .await?;
        let applied_source = match crate::execution::apply_continue_strategy_tx(
            &mut tx,
            stage.authority(),
            &request.continuation,
            now,
        )
        .await?
        {
            Checked::Held(reasons) => {
                // This wrapper commits neither its staged dependency removal
                // nor scheduler observations when another real hold remains.
                tx.rollback().await?;
                return Ok(Checked::Held(reasons));
            }
            Checked::Ready(applied) => applied,
        };
        let applied_model =
            Self::task_decide_with_reconciliation_tx(&mut tx, actor, task, request.decision, now)
                .await?
                .applied
                .context("new_decision_outcome_receipt_required")?;
        let case = crate::decision_recovery::finalize_staged_decision_continuation_tx(
            &mut tx,
            actor,
            stage,
            &applied_source,
            &applied_model,
            now,
        )
        .await?;
        // Removing a typed case edge can split a component. Keep every task
        // from the before graph plus the actual source in the final sync.
        let mut seeds = before.records.keys().cloned().collect::<Vec<_>>();
        seeds.extend(applied_model.events.keys().cloned());
        seeds.push(source.cause_ref.source_task);
        let graph = load_graph_tx(&mut tx, &actor.group_name, &seeds).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &graph).await?;
        crate::execution::sync_model_tx(
            &mut tx,
            &actor.group_name,
            &graph.records.keys().cloned().collect::<Vec<_>>(),
            now,
        )
        .await?;
        let result = DecisionContinuationReceipt {
            decision: view(&graph, task)?,
            case: crate::decision_recovery::load_case_tx(&mut tx, &actor.group_name, case.id)
                .await?,
        };
        save_receipt_tx(&mut tx, actor, &request.key, &canonical, &result).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(Checked::Ready(result))
    }

    /// Transfer the finite decision to its original writer under standing consent.
    ///
    /// The ordinary task, immutable results, original case and all execution
    /// accounting stay in place. The old candidate remains in result history;
    /// the new phase must capture fresh inputs before publishing another one.
    ///
    /// # Errors
    /// Rejects nonwriters, changed/revoked policy or source, stale versions,
    /// expired bounds, a repeated phase, or any model/recovery graph conflict.
    pub async fn decision_writer_fallback(
        &self,
        actor: &Mailbox,
        task: &str,
        request: DecisionWriterFallback,
        now: i64,
    ) -> Result<TaskView> {
        name(task)?;
        request_identity(&request.key, &request.reason)?;
        let canonical = canonical(&("decision_writer_fallback", task, &request))?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            return Ok(old);
        }
        let receipt = materialized_receipt_tx(&mut tx, &actor.group_name, task)
            .await?
            .context("actual_materialized_decision_required")?;
        ensure!(
            actor.id == receipt.issuer && actor.name == receipt.writer,
            "original_source_writer_required"
        );
        ensure!(
            request.policy_revision == receipt.policy_revision
                && receipt.policy.allow_writer_fallback,
            "standing_writer_fallback_required"
        );
        ensure!(now < receipt.deadline, "materialized_deadline_expired");
        let before = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &before).await?;
        let old = before.records.get(task).context("decision model missing")?;
        ensure!(old.work.version == request.version, "task_version_conflict");
        ensure!(
            authority_current(&before, task),
            "current_materialized_authority_required"
        );
        ensure!(
            receipt.policy.reviewer.as_deref() == Some(old.work.owner.as_str())
                && old.work.owner != receipt.writer,
            "original_reviewer_phase_required"
        );
        let phase = crate::decision_recovery::prepare_decision_change_tx(
            &mut tx,
            &actor.group_name,
            receipt.case_id,
            request.case_version,
            task,
            crate::decision_recovery::DecisionChangeKind::WriterFallback,
            now,
        )
        .await?;
        let mut graph = before.clone();
        let current = graph
            .records
            .get_mut(task)
            .context("decision model missing")?;
        current.work.owner = receipt.writer;
        current.model.input_epoch = bump(current.model.input_epoch)?;
        current.model.current_candidate = None;
        let operation = uuid::Uuid::new_v4().to_string();
        propagate_invalidation(&mut graph, &before, &operation)?;
        let applied = persist_changes_tx(
            &mut tx,
            &actor.group_name,
            &actor.name,
            &before,
            &mut graph,
            ModelOperation {
                root: task,
                id: &operation,
                reason: &request.reason,
                now,
                origin: ModelOrigin::Writer,
            },
        )
        .await?;
        crate::decision_recovery::finalize_decision_change_tx(&mut tx, &phase, &applied, now)
            .await?;
        let graph = sync_after_decision_phase_tx(&mut tx, &actor.group_name, task, now).await?;
        let result = view(&graph, task)?;
        save_receipt_tx(&mut tx, actor, &request.key, &canonical, &result).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }

    /// Adopt or revise a finite decision policy as the original source authority.
    ///
    /// This only persists consent. Recovery still validates the real case and
    /// causal episode before model materialization under the same transaction.
    ///
    /// # Errors
    /// Rejects nonwriters, stale source/policy guards, remote reviewers, invalid
    /// limits, conflicting source identity and nonidentical retries.
    pub async fn decision_policy(
        &self,
        actor: &Mailbox,
        mut request: DecisionPolicyDecision,
        now: i64,
    ) -> Result<DecisionPolicyReceipt> {
        request_identity(&request.key, &request.reason)?;
        name(&request.policy.id)?;
        text_required(
            &request.policy.authority_ref,
            256,
            "decision authority reference",
        )?;
        request.policy.actions.sort();
        ensure!(
            !request.policy.actions.is_empty()
                && !request
                    .policy
                    .actions
                    .windows(2)
                    .any(|pair| pair[0] == pair[1]),
            "distinct_decision_actions_required"
        );
        ensure!(
            request.policy.reviewer.is_some() || !request.policy.allow_writer_fallback,
            "reviewer_required_for_phase_fallback"
        );
        let mut authorization = Authorization {
            state: AuthorityState::Authorized,
            source: AuthoritySource::Direct {
                authority_ref: request.policy.authority_ref.clone(),
            },
            approved_scope: request.policy.contract.allowed_scope.clone(),
            reason: request.reason.clone(),
        };
        normalize_contract(&mut request.policy.contract, &mut authorization)?;
        ensure!(
            !request.policy.contract.allow_delegation,
            "decision_delegation_forbidden"
        );
        let canonical = canonical(&("decision_policy", &request))?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(receipt) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            return Ok(receipt);
        }
        let source = crate::decision_recovery::inspect_source_tx(
            &mut tx,
            &actor.group_name,
            &request.source.source,
        )
        .await?;
        ensure!(
            source.authority_id == actor.id && source.authority == actor.name,
            "original_source_writer_required"
        );
        ensure!(
            source.source == request.source.source
                && source.input_epoch == request.source.input_epoch
                && source.candidate == request.source.candidate
                && source.outcome == request.source.outcome,
            "decision_source_conflict"
        );
        if !request.policy.revoked {
            ensure!(source.unresolved, "decision_source_settled");
            ensure!(request.policy.deadline > now, "decision_policy_expired");
            if let Some(reviewer) = &request.policy.reviewer {
                name(reviewer)?;
                ensure!(reviewer != &actor.name, "reviewer_is_original_writer");
                let local: i64 = sqlx::query_scalar("SELECT count(*) FROM mailboxes WHERE group_name=? AND name=? AND agent_state='registered' AND remote_machine IS NULL")
                    .bind(&actor.group_name).bind(reviewer).fetch_one(&mut *tx).await?;
                ensure!(local == 1, "local_decision_reviewer_required");
            }
        }
        let prior = sqlx::query("SELECT source,issuer,writer,revision FROM task_decision_policies WHERE group_name=? AND id=?")
            .bind(&actor.group_name).bind(&request.policy.id).fetch_optional(&mut *tx).await?;
        let previous = prior.as_ref().map(|row| row.get::<i64, _>("revision"));
        ensure!(
            previous == request.expected_revision,
            "decision_policy_revision_conflict"
        );
        if let Some(prior) = prior {
            ensure!(
                prior.get::<String, _>("source") == source.source_key
                    && prior.get::<i64, _>("issuer") == actor.id
                    && prior.get::<String, _>("writer") == actor.name,
                "decision_policy_identity_immutable"
            );
        }
        let revision = bump(previous.unwrap_or(0))?;
        let payload = serde_json::to_string(&request.policy)?;
        sqlx::query("INSERT INTO task_decision_policies(group_name,id,source,issuer,writer,revision,payload,revoked) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(group_name,id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,revoked=excluded.revoked")
            .bind(&actor.group_name).bind(&request.policy.id).bind(&source.source_key)
            .bind(actor.id).bind(&actor.name).bind(revision).bind(&payload).bind(request.policy.revoked)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_decision_policy_history(group_name,id,revision,source_guard,payload,issuer,reason,created) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&actor.group_name).bind(&request.policy.id).bind(revision)
            .bind(serde_json::to_string(&source)?).bind(payload).bind(actor.id).bind(&request.reason).bind(now)
            .execute(&mut *tx).await?;
        let receipt = DecisionPolicyReceipt {
            source_key: source.source_key,
            writer: actor.name.clone(),
            revision,
            policy: request.policy,
        };
        save_receipt_tx(&mut tx, actor, &request.key, &canonical, &receipt).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(receipt)
    }
}

/// References protected policy and case rows; serialized inputs grant no authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DecisionMaterializationRequest {
    pub policy: String,
    pub policy_revision: i64,
    pub case_id: i64,
    pub case_version: i64,
    pub source: DecisionSourceExpectation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MaterializedDecisionReceipt {
    canonical: String,
    pub task: String,
    pub case_id: i64,
    pub source_key: String,
    pub episode: String,
    pub policy_revision: i64,
    pub policy: DecisionPolicy,
    pub writer: String,
    pub issuer: i64,
    pub deadline: i64,
    pub created: i64,
}

/// Refusal leaves the caller's original durable case/operator responsibility intact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaterializationRefusal {
    PolicyMissing,
    PolicyAmbiguous,
    PolicySourceChanged,
    CaseUnavailable,
    PolicyRevisionChanged,
    PolicyRevoked,
    PolicyExpired,
    AuthorityUnavailable,
    ReviewerUnavailable,
}

#[derive(Debug)]
pub(crate) enum DecisionMaterialization {
    Materialized(MaterializedDecisionReceipt),
    Replayed(MaterializedDecisionReceipt),
    Refused(MaterializationRefusal),
}

async fn materialized_receipt_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
) -> Result<Option<MaterializedDecisionReceipt>> {
    let receipt: Option<String> = sqlx::query_scalar(
        "SELECT receipt FROM task_materializations WHERE group_name=? AND decision_task=?",
    )
    .bind(group)
    .bind(task)
    .fetch_optional(&mut **tx)
    .await?;
    receipt
        .map(|receipt| serde_json::from_str(&receipt).map_err(Into::into))
        .transpose()
}

fn assigned_policy_reviewer(
    record: &Record,
    actor: &Mailbox,
    receipt: Option<&MaterializedDecisionReceipt>,
) -> bool {
    receipt.is_some_and(|receipt| {
        receipt.policy.reviewer.as_deref() == Some(actor.name.as_str())
            && record.work.owner == actor.name
            && record.work.writer == receipt.writer
    })
}

async fn sync_after_decision_phase_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
    now: i64,
) -> Result<Graph> {
    let graph = load_graph_tx(tx, group, &[task.to_owned()]).await?;
    validate_projection_tx(tx, group, &graph).await?;
    crate::execution::sync_model_tx(
        tx,
        group,
        &graph.records.keys().cloned().collect::<Vec<_>>(),
        now,
    )
    .await?;
    Ok(graph)
}

async fn guard_materialized_decision_update_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    before: &Record,
    proposed: &Record,
) -> Result<()> {
    let receipt: Option<String> = sqlx::query_scalar(
        "SELECT receipt FROM task_materializations WHERE group_name=? AND decision_task=?",
    )
    .bind(group)
    .bind(&before.work.id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(receipt) = receipt else {
        return Ok(());
    };
    let receipt: MaterializedDecisionReceipt = serde_json::from_str(&receipt)?;
    ensure!(
        proposed.work.owner == before.work.owner,
        "materialized_phase_transition_required"
    );
    ensure!(
        proposed.work.writer == receipt.writer
            && proposed.work.scope == receipt.policy.contract.deliverable
            && proposed.model.parent.is_none()
            && proposed.model.requirements.is_empty(),
        "materialized_assignment_is_finite"
    );
    ensure!(
        proposed
            .work
            .deadline
            .is_some_and(|deadline| deadline <= receipt.deadline),
        "materialized_deadline_ceiling"
    );
    let proposed_budget = &proposed.model.contract.budget;
    let ceiling = &receipt.policy.contract.budget;
    ensure!(
        proposed_budget.max_attempts <= ceiling.max_attempts
            && proposed_budget.max_elapsed_seconds <= ceiling.max_elapsed_seconds,
        "materialized_budget_ceiling"
    );
    if let Some(ceiling) = &ceiling.max_cost {
        ensure!(
            proposed_budget
                .max_cost
                .as_ref()
                .is_some_and(|limit| limit.unit == ceiling.unit && limit.amount <= ceiling.amount),
            "materialized_cost_ceiling"
        );
    }
    let mut contract = proposed.model.contract.clone();
    contract.budget = receipt.policy.contract.budget.clone();
    ensure!(
        contract == receipt.policy.contract,
        "materialized_contract_is_immutable"
    );
    Ok(())
}

/// Select genuine stored consent for one recovery-owned case in the caller's
/// transaction. Historical receipts replay before fresh eligibility checks.
/// Refusal never clears operator responsibility or allocates a task/allowance.
pub(crate) async fn materialize_recovery_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case_id: i64,
    case_version: i64,
    now: i64,
) -> Result<DecisionMaterialization> {
    use DecisionMaterialization::Refused;
    use MaterializationRefusal::{
        AuthorityUnavailable, CaseUnavailable, PolicyAmbiguous, PolicyExpired, PolicyMissing,
        PolicyRevoked, PolicySourceChanged,
    };
    name(group)?;
    ensure!(
        case_id > 0 && case_version > 0,
        "positive_materialization_guards_required"
    );
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let identity = crate::decision_recovery::read_case_identity_tx(tx, group, case_id).await?;
    let prior: Option<String> = sqlx::query_scalar(
        "SELECT receipt FROM task_materializations WHERE group_name=? AND source=? AND episode=?",
    )
    .bind(group)
    .bind(&identity.source_key)
    .bind(&identity.episode)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(prior) = prior {
        let receipt: MaterializedDecisionReceipt = serde_json::from_str(&prior)?;
        let request: DecisionMaterializationRequest = serde_json::from_str(&receipt.canonical)?;
        ensure!(
            request.case_id == case_id,
            "materialization_replay_case_conflict"
        );
        return materialize_decision_task_tx(tx, group, &request, now).await;
    }
    let case = crate::decision_recovery::load_case_tx(tx, group, case_id).await?;
    ensure!(
        case.version == case_version,
        "decision case revision conflict"
    );
    if case.requires_reassessment
        || !matches!(case.state.as_str(), "held" | "decision_pending")
        || now >= case.hard_due
        || !case.current_source.unresolved
    {
        return Ok(Refused(CaseUnavailable));
    }
    // The registry has a UNIQUE source constraint. Still reject ambiguity
    // explicitly, rather than granting authority from an arbitrary first row.
    let policies = sqlx::query("SELECT id,source,issuer,writer,revision,payload,revoked FROM task_decision_policies WHERE group_name=? AND source=? LIMIT 2")
        .bind(group).bind(&identity.source_key).fetch_all(&mut **tx).await?;
    let row = match policies.as_slice() {
        [] => return Ok(Refused(PolicyMissing)),
        [row] => row,
        _ => return Ok(Refused(PolicyAmbiguous)),
    };
    let policy_id: String = row.get("id");
    let revision: i64 = row.get("revision");
    let issuer: i64 = row.get("issuer");
    let writer: String = row.get("writer");
    let payload: String = row.get("payload");
    let policy: DecisionPolicy = serde_json::from_str(&payload)?;
    ensure!(
        policy.id == policy_id
            && revision > 0
            && policy.revoked == row.get::<bool, _>("revoked")
            && row.get::<String, _>("source") == identity.source_key
            && issuer == identity.original_source.authority_id
            && writer == identity.authority,
        "decision_policy_registry_corrupt"
    );
    let history = sqlx::query("SELECT source_guard,payload,issuer FROM task_decision_policy_history WHERE group_name=? AND id=? AND revision=?")
        .bind(group).bind(&policy_id).bind(revision).fetch_optional(&mut **tx).await?
        .context("decision_policy_history_missing")?;
    ensure!(
        history.get::<String, _>("payload") == payload && history.get::<i64, _>("issuer") == issuer,
        "decision_policy_history_corrupt"
    );
    let consent: crate::decision_recovery::ObligationView =
        serde_json::from_str(&history.get::<String, _>("source_guard"))?;
    ensure!(
        consent.source_key == identity.source_key
            && consent.authority_id == issuer
            && consent.authority == writer,
        "decision_policy_history_identity_corrupt"
    );
    if policy.revoked {
        return Ok(Refused(PolicyRevoked));
    }
    if now >= policy.deadline {
        return Ok(Refused(PolicyExpired));
    }
    let episode = crate::decision_recovery::validate_decision_episode_tx(
        tx,
        group,
        case_id,
        case_version,
        now,
    )
    .await?;
    let source = episode.source();
    if !source.authority_registered {
        return Ok(Refused(AuthorityUnavailable));
    }
    if consent.source != source.source
        || consent.input_epoch != source.input_epoch
        || consent.candidate != source.candidate
        || consent.outcome != source.outcome
        || consent.authority_id != source.authority_id
        || consent.authority != source.authority
        || consent.source_key != source.source_key
    {
        return Ok(Refused(PolicySourceChanged));
    }
    // Validate the normalized finite consent; volatile plan/checkpoint metadata
    // deliberately does not participate in source identity.
    name(&policy.id)?;
    text_required(&policy.authority_ref, 256, "decision authority reference")?;
    ensure!(
        !policy.actions.is_empty() && policy.actions.windows(2).all(|pair| pair[0] < pair[1]),
        "distinct_decision_actions_required"
    );
    ensure!(
        policy.reviewer.is_some() || !policy.allow_writer_fallback,
        "reviewer_required_for_phase_fallback"
    );
    if let Some(reviewer) = &policy.reviewer {
        name(reviewer)?;
        ensure!(reviewer != &writer, "reviewer_is_original_writer");
    }
    let mut contract = policy.contract.clone();
    let mut authorization = Authorization {
        state: AuthorityState::Authorized,
        source: AuthoritySource::Direct {
            authority_ref: policy.authority_ref.clone(),
        },
        approved_scope: contract.allowed_scope.clone(),
        reason: "Validate original finite source consent".into(),
    };
    normalize_contract(&mut contract, &mut authorization)?;
    ensure!(
        contract == policy.contract && !contract.allow_delegation,
        "decision_policy_contract_corrupt"
    );
    let request = DecisionMaterializationRequest {
        policy: policy_id,
        policy_revision: revision,
        case_id,
        case_version,
        source: DecisionSourceExpectation {
            source: source.source.clone(),
            input_epoch: source.input_epoch,
            candidate: source.candidate.clone(),
            outcome: source.outcome.clone(),
        },
    };
    materialize_decision_task_tx(tx, group, &request, now).await
}

// This is the only system creation path: every assignment field comes from the
// stored original-writer policy and the recovery owner's actual episode proof.
// A refusal is returned before model writes, so a caller can commit its case.
pub(crate) async fn materialize_decision_task_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    request: &DecisionMaterializationRequest,
    now: i64,
) -> Result<DecisionMaterialization> {
    use DecisionMaterialization::{Materialized, Refused, Replayed};
    use MaterializationRefusal::{
        AuthorityUnavailable, PolicyExpired, PolicyMissing, PolicyRevisionChanged, PolicyRevoked,
        ReviewerUnavailable,
    };
    name(group)?;
    name(&request.policy)?;
    ensure!(
        request.case_id > 0 && request.case_version > 0 && request.policy_revision > 0,
        "positive_materialization_guards_required"
    );
    crate::decision_recovery::reserve_home_tx(tx, group).await?;
    let identity =
        crate::decision_recovery::read_case_identity_tx(tx, group, request.case_id).await?;
    let canonical = canonical(request)?;
    let prior: Option<String> = sqlx::query_scalar(
        "SELECT receipt FROM task_materializations WHERE group_name=? AND source=? AND episode=?",
    )
    .bind(group)
    .bind(&identity.source_key)
    .bind(&identity.episode)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(prior) = prior {
        let receipt: MaterializedDecisionReceipt = serde_json::from_str(&prior)?;
        ensure!(
            receipt.canonical == canonical
                && receipt.case_id == identity.id
                && receipt.source_key == identity.source_key
                && receipt.episode == identity.episode
                && receipt.writer == identity.authority,
            "materialization_replay_conflict"
        );
        return Ok(Replayed(receipt));
    }
    let row = sqlx::query("SELECT source,issuer,writer,revision,payload,revoked FROM task_decision_policies WHERE group_name=? AND id=?")
        .bind(group).bind(&request.policy).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(Refused(PolicyMissing));
    };
    ensure!(
        identity.group == group
            && row.get::<String, _>("source") == identity.source_key
            && row.get::<String, _>("writer") == identity.authority
            && row.get::<i64, _>("issuer") == identity.original_source.authority_id,
        "materialization_policy_source_mismatch"
    );
    if row.get::<i64, _>("revision") != request.policy_revision {
        return Ok(Refused(PolicyRevisionChanged));
    }
    let mut policy: DecisionPolicy = serde_json::from_str(&row.get::<String, _>("payload"))?;
    if row.get::<bool, _>("revoked") || policy.revoked {
        return Ok(Refused(PolicyRevoked));
    }
    if now >= policy.deadline {
        return Ok(Refused(PolicyExpired));
    }
    let episode = crate::decision_recovery::validate_decision_episode_tx(
        tx,
        group,
        request.case_id,
        request.case_version,
        now,
    )
    .await?;
    let source = episode.source();
    ensure!(
        source.source == request.source.source
            && source.input_epoch == request.source.input_epoch
            && source.candidate == request.source.candidate
            && source.outcome == request.source.outcome
            && source.source_key == identity.source_key
            && source.authority == identity.authority
            && source.authority_id == identity.original_source.authority_id,
        "materialization_source_conflict"
    );
    if !source.authority_registered {
        return Ok(Refused(AuthorityUnavailable));
    }
    let writer = episode.authority().to_owned();
    let owner = policy.reviewer.clone().unwrap_or_else(|| writer.clone());
    let task = format!("decision-{}", episode.case_id());
    let before = load_graph_tx(tx, group, std::slice::from_ref(&task)).await?;
    validate_projection_tx(tx, group, &before).await?;
    if !before.bindings.contains_key(&owner) {
        return Ok(Refused(ReviewerUnavailable));
    }
    let deadline = policy.deadline.min(episode.hard_due());
    let mut authorization = Authorization {
        state: AuthorityState::Authorized,
        source: AuthoritySource::Direct {
            authority_ref: policy.authority_ref.clone(),
        },
        approved_scope: policy.contract.allowed_scope.clone(),
        reason: format!(
            "Standing policy {} revision {}",
            policy.id, request.policy_revision
        ),
    };
    normalize_contract(&mut policy.contract, &mut authorization)?;
    ensure!(
        !policy.contract.allow_delegation,
        "decision_delegation_forbidden"
    );
    let receipt = MaterializedDecisionReceipt {
        canonical,
        task: task.clone(),
        case_id: episode.case_id(),
        source_key: episode.source_key().into(),
        episode: episode.episode().into(),
        policy_revision: request.policy_revision,
        policy: policy.clone(),
        writer: writer.clone(),
        issuer: source.authority_id,
        deadline,
        created: now,
    };
    let work = WorkItem {
        group_name: group.into(),
        id: task.clone(),
        scope: policy.contract.deliverable.clone(),
        owner,
        writer,
        state: TaskState::Ready,
        next_action: "Produce the finite policy decision with criterion evidence".into(),
        deadline: Some(deadline),
        accepted_revision: None,
        evidence: vec![
            format!("decision-case:{}", episode.case_id()),
            format!("decision-policy:{}:{}", policy.id, request.policy_revision),
        ],
        version: 1,
        updated: now,
        synced_at: None,
        linked_messages: Vec::new(),
    };
    crate::work::validate_fields(&work)?;
    let exists: i64 =
        sqlx::query_scalar("SELECT count(*) FROM work_items WHERE group_name=? AND id=?")
            .bind(group)
            .bind(&task)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(exists == 0, "materialized_task_identity_conflict");
    let reason = format!(
        "system_materialization from policy {}:{} issued by {}",
        policy.id, request.policy_revision, receipt.issuer
    );
    sqlx::query("INSERT INTO work_items(group_name,id,scope,owner,writer,state,open,next_action,deadline,accepted_revision,evidence,version,updated) VALUES(?,?,?,?,?,'ready',1,?,?,NULL,?,1,?)")
        .bind(group).bind(&task).bind(&work.scope).bind(&work.owner).bind(&work.writer)
        .bind(&work.next_action).bind(deadline).bind(serde_json::to_string(&work.evidence)?).bind(now)
        .execute(&mut **tx).await?;
    sqlx::query("INSERT INTO work_changes(group_name,work_id,version,actor,reason,snapshot,changed) VALUES(?,?,1,'system_materialization',?,?,?)")
        .bind(group).bind(&task).bind(&reason).bind(serde_json::to_string(&work)?).bind(now).execute(&mut **tx).await?;
    crate::relay::enqueue_snapshot(tx, &work, None, now).await?;
    let mut graph = before.clone();
    graph.records.insert(
        task.clone(),
        Record {
            work,
            model: initial_model(policy.contract, authorization, Vec::new(), None),
        },
    );
    persist_model_tx(tx, &graph.records[&task]).await?;
    // The scheduler must observe the true MaterializedDecision source kind on
    // its first initialization. This receipt remains immutable for all retries.
    sqlx::query("INSERT INTO task_materializations(group_name,source,episode,decision_task,receipt) VALUES(?,?,?,?,?)")
        .bind(group).bind(&receipt.source_key).bind(&receipt.episode).bind(&task)
        .bind(serde_json::to_string(&receipt)?).execute(&mut **tx).await?;
    let operation = uuid::Uuid::new_v4().to_string();
    persist_changes_tx(
        tx,
        group,
        "system_materialization",
        &before,
        &mut graph,
        ModelOperation {
            root: &task,
            id: &operation,
            reason: &reason,
            now,
            origin: ModelOrigin::SystemMaterialization,
        },
    )
    .await?;
    // Link after the creation event so recovery cannot misclassify creation as
    // a change to an already-linked decision task and require reassessment.
    let case =
        crate::decision_recovery::link_materialized_decision_tx(tx, &episode, &task, now).await?;
    refresh_recovery_projection_tx(tx, group, &BTreeMap::from([(case.id, case.version)])).await?;
    graph = load_graph_tx(tx, group, std::slice::from_ref(&task)).await?;
    validate_projection_tx(tx, group, &graph).await?;
    crate::execution::sync_model_tx(
        tx,
        group,
        &graph.records.keys().cloned().collect::<Vec<_>>(),
        now,
    )
    .await?;
    Ok(Materialized(receipt))
}

/// Exact grant revision selected by a progress judgment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JudgeGrantRef {
    /// Grant identity within the source task.
    pub id: String,
    /// Observed current revision.
    pub revision: i64,
}

pub(crate) async fn validate_progress_judge_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    inputs: &InputSnapshot,
    milestone: &str,
    grant: Option<&JudgeGrantRef>,
) -> Result<Vec<String>> {
    authenticate_tx(tx, actor).await?;
    ensure!(
        actor.group_name == inputs.group,
        "cross_group_judgment_unsupported"
    );
    validate_judge_source_tx(tx, &actor.name, inputs, milestone, grant).await
}

/// Revalidate authority for an existing authenticated progress judgment. The
/// progress owner must obtain these arguments from its immutable judgment/report
/// rows; this does not authenticate a new judgment or authorize any new write.
/// Stable mailbox identity survives runtime rebinding. Current source inputs and
/// the exact grant revision remain mandatory; no historical Mailbox is forged.
///
/// # Errors
/// Rejects changed identity, input provenance, authority or grant revision.
pub async fn validate_stored_progress_judge_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor_id: i64,
    actor_name: &str,
    inputs: &InputSnapshot,
    milestone: &str,
    grant: Option<&JudgeGrantRef>,
) -> Result<Vec<String>> {
    reserve_group_tx(tx, &inputs.group).await?;
    let exists: i64 = sqlx::query_scalar("SELECT count(*) FROM mailboxes m JOIN groups g ON g.name=m.group_name JOIN node n ON n.id=g.home_machine WHERE m.id=? AND m.name=? AND m.group_name=? AND m.remote_machine IS NULL")
        .bind(actor_id).bind(actor_name).bind(&inputs.group).fetch_one(&mut **tx).await?;
    ensure!(exists == 1, "stored_judgment_actor_identity_changed");
    validate_judge_source_tx(tx, actor_name, inputs, milestone, grant).await
}

async fn validate_judge_source_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor_name: &str,
    inputs: &InputSnapshot,
    milestone: &str,
    grant: Option<&JudgeGrantRef>,
) -> Result<Vec<String>> {
    name(milestone)?;
    let graph = load_graph_tx(tx, &inputs.group, std::slice::from_ref(&inputs.task)).await?;
    validate_projection_tx(tx, &inputs.group, &graph).await?;
    let record = graph
        .records
        .get(&inputs.task)
        .context("task model missing")?;
    // Held execution alone does not deny a progress judgment. Exact semantic
    // inputs and provenance remain mandatory, independent of runnable state.
    ensure!(
        inputs.input_epoch == record.model.input_epoch
            && graph.bindings.get(&record.work.owner) == Some(&inputs.owner_binding_generation)
            && inputs.ancestor_authority_digests == authority_snapshot(&graph, &inputs.task)?
            && snapshot_edges_current(&graph, inputs, &valid_outcomes(&graph)?)?,
        "stale_judgment_inputs"
    );
    if actor_name == record.work.writer {
        ensure!(
            grant.is_none(),
            "writer_judgment_does_not_use_a_decider_grant"
        );
        return Ok(record
            .model
            .contract
            .criteria
            .iter()
            .map(|c| c.id.clone())
            .collect());
    }
    let grant = grant.context("judgment_grant_required")?;
    let row = sqlx::query("SELECT revision,kind,payload,revoked FROM task_grants WHERE group_name=? AND task=? AND id=?").bind(&inputs.group).bind(&inputs.task).bind(&grant.id).fetch_optional(&mut **tx).await?.context("judgment grant missing")?;
    ensure!(
        row.try_get::<i64, _>("revision")? == grant.revision
            && row.try_get::<String, _>("kind")? == "progress_judge"
            && row.try_get::<i64, _>("revoked")? == 0,
        "judgment_grant_stale_or_revoked"
    );
    let current: JudgeGrant = serde_json::from_str(&row.try_get::<String, _>("payload")?)?;
    ensure!(
        !current.revoked && current.decider == actor_name && current.milestone == milestone,
        "judgment_outside_grant"
    );
    ensure!(
        current.criterion_ids.iter().all(|id| record
            .model
            .contract
            .criteria
            .iter()
            .any(|c| &c.id == id)),
        "judgment_criterion_changed"
    );
    Ok(current.criterion_ids)
}

fn graph_seeds(
    task: &str,
    requirements: &[Requirement],
    parent: Option<&ParentLink>,
) -> Vec<String> {
    let mut seeds = vec![task.to_owned()];
    seeds.extend(requirements.iter().map(|r| r.task.clone()));
    seeds.extend(parent.map(|p| p.task.clone()));
    seeds
}

/// Versioned immutable candidate operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRequest {
    /// Exact observed task version.
    pub version: i64,
    /// Retry identity.
    pub key: String,
    /// Artifact and its attested exact input snapshot.
    pub candidate: CandidateDraft,
}

impl Store {
    /// Read business readiness without claiming scheduler admission.
    ///
    /// # Errors
    /// Actor, group, graph completeness or source records are invalid.
    pub async fn task_model_readiness(
        &self,
        actor: &Mailbox,
        task: &str,
        phase: Phase,
    ) -> Result<ModelReadiness> {
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        let result = evaluate_task_tx(&mut tx, &actor.group_name, task, phase).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Check exact captured inputs; this read never authorizes a subsequent mutation.
    ///
    /// # Errors
    /// Authentication, group mismatch or incomplete graph validation fails.
    pub async fn task_input_validity(
        &self,
        actor: &Mailbox,
        inputs: &InputSnapshot,
    ) -> Result<InputValidity> {
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        ensure!(
            actor.group_name == inputs.group,
            "cross_group_inputs_unsupported"
        );
        let result = validate_inputs_tx(&mut tx, inputs).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Inspect narrow judgment authority; progress must revalidate inside its own write transaction.
    ///
    /// # Errors
    /// Missing/revoked grant, stale inputs, unauthorized actor or graph validation fails.
    pub async fn task_judgment_authority(
        &self,
        actor: &Mailbox,
        inputs: &InputSnapshot,
        milestone: &str,
        grant: Option<&JudgeGrantRef>,
    ) -> Result<Vec<String>> {
        let mut tx = self.pool().begin().await?;
        let result = validate_progress_judge_tx(&mut tx, actor, inputs, milestone, grant).await?;
        tx.commit().await?;
        Ok(result)
    }
}

impl Store {
    /// Persist deterministic negative holds for stale stored candidates or outcomes.
    ///
    /// Any authenticated local observer may remove stale readiness; this never
    /// grants authority, accepts work, releases execution slots or repairs effects.
    /// Service integration must call it independently of notification delivery.
    ///
    /// # Errors
    /// Authentication, incomplete graph or persistence fails atomically.
    pub async fn task_reconcile_inputs(
        &self,
        actor: &Mailbox,
        task: &str,
        now: i64,
    ) -> Result<Vec<String>> {
        name(task)?;
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        let before = load_graph_tx(&mut tx, &actor.group_name, &[task.to_owned()]).await?;
        validate_projection_tx(&mut tx, &actor.group_name, &before).await?;
        ensure!(before.records.contains_key(task), "task model missing");
        let valid = valid_outcomes(&before)?;
        let mut graph = before.clone();
        let operation = uuid::Uuid::new_v4().to_string();
        let mut changed = Vec::new();
        for (id, record) in &before.records {
            if !record.model.invalidation_causes.is_empty() {
                continue;
            }
            if record
                .model
                .current_outcome
                .as_ref()
                .and_then(|outcome| before.results.get(outcome))
                .and_then(|result| result.outcome)
                .is_some_and(|kind| !kind.successful())
            {
                continue;
            }
            let mut stale = false;
            for result in [
                record.model.current_candidate.as_ref(),
                record.model.current_outcome.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(inputs) = before
                    .results
                    .get(result)
                    .and_then(|result| result.inputs.as_ref())
                {
                    if !snapshot_base_current(&before, inputs)?
                        || !snapshot_edges_current(&before, inputs, &valid)?
                    {
                        stale = true;
                    }
                }
            }
            if stale {
                let record = graph
                    .records
                    .get_mut(id)
                    .context("reconciliation task missing")?;
                record.model.input_epoch = bump(record.model.input_epoch)?;
                invalidate_record(record, &operation);
                changed.push(id.clone());
            }
        }
        if !changed.is_empty() {
            persist_changes_tx(
                &mut tx,
                &actor.group_name,
                &actor.name,
                &before,
                &mut graph,
                ModelOperation {
                    root: task,
                    id: &operation,
                    reason: "Observed stale immutable inputs; responsible writer must revalidate",
                    now,
                    origin: ModelOrigin::SystemInvalidation,
                },
            )
            .await?;
        }
        tx.commit().await?;
        if !changed.is_empty() {
            crate::stream::hint(self.root()).await;
        }
        Ok(changed)
    }
}

/// Stable append-order result history, independent of UUID ordering and clocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultsPage {
    /// At most 100 immutable results.
    pub items: Vec<TaskResult>,
    /// Last returned durable sequence; use as the next `after` cursor.
    pub next_cursor: i64,
    /// Additional results existed in the read snapshot.
    pub has_more: bool,
}

/// One bounded home-store observation of work owned or written by the actor.
/// Runtime, progress and recovery APIs provide separate owner observations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTrackingPage {
    /// Actual model views, or explicit legacy-untracked views, in ID order.
    pub items: Vec<TaskView>,
    /// Last returned ID when another page exists.
    pub next_cursor: Option<String>,
    /// Additional visible work exists after this page.
    pub has_more: bool,
}

#[cfg(test)]
mod artifact_binding_tests;

#[cfg(test)]
mod candidate_core_tests;

#[cfg(test)]
mod transaction_tests {
    use super::*;

    pub(super) async fn phase_candidate(
        store: &Store,
        actor: &Mailbox,
        task: &TaskView,
        key: &str,
    ) -> Result<CandidateRequest> {
        Ok(CandidateRequest {
            version: task.work.version,
            key: key.into(),
            candidate: CandidateDraft {
                revision: "review-evidence-v1".into(),
                summary: "Evidence for original writer review".into(),
                criterion_evidence: task
                    .model
                    .as_ref()
                    .context("model missing")?
                    .contract
                    .criteria
                    .iter()
                    .map(|criterion| CriterionEvidence {
                        criterion_id: criterion.id.clone(),
                        references: vec!["review-evidence".into()],
                    })
                    .collect(),
                inputs: store
                    .task_capture_inputs(actor, &task.work.id, task.work.version, Phase::Accept)
                    .await?,
            },
        })
    }

    pub(super) struct ContinuationFixture {
        _temp: tempfile::TempDir,
        pub(super) store: Store,
        pub(super) writer: Mailbox,
        pub(super) reviewer: Mailbox,
        pub(super) task: String,
        policy: DecisionPolicyDecision,
        request: DecisionContinuation,
    }

    // This cfg(test) gate creates actual scheduler bookkeeping only. It never
    // supplies a closure receipt or claims native runtime qualification.
    struct UnclosedTestRuntime;
    impl crate::execution::RuntimeGate for UnclosedTestRuntime {
        async fn target(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Option<crate::execution::RuntimeTarget>> {
            Ok(Some(crate::execution::RuntimeTarget {
                identity: "model-slot-fixture".into(),
                concurrency_key: "model-slot-fixture".into(),
                generation: 1,
                profile: "test-only".into(),
                durable_dedupe: true,
                cost_caps: BTreeMap::new(),
            }))
        }
        async fn current(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            _: crate::execution::CurrentUse,
            _: i64,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn closed(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            _: &str,
        ) -> Result<Option<crate::execution::ClosedRuntime>> {
            Ok(None)
        }
        async fn observation(
            &self,
            _: &mut Transaction<'_, Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            _: &str,
        ) -> Result<Option<crate::execution::RuntimeObservation>> {
            Ok(None)
        }
    }

    pub(super) async fn continuation_fixture(
        explicit_action: bool,
        retained_scope: bool,
        held_attempt: bool,
    ) -> Result<ContinuationFixture> {
        continuation_fixture_with_blockers(explicit_action, retained_scope, held_attempt, true)
            .await
    }

    #[tokio::test]
    async fn artifact_binding_respects_actual_materialized_decision_deadline() -> Result<()> {
        let f = continuation_fixture(true, false, false).await?;
        let current = f.store.task_inspect(&f.writer, &f.task).await?;
        let scope = current
            .model
            .as_ref()
            .context("decision model missing")?
            .contract
            .allowed_scope[0]
            .clone();
        let inputs = f
            .store
            .task_capture_inputs(&f.writer, &f.task, current.work.version, Phase::Accept)
            .await?;
        let mut tx = f.store.pool().begin().await?;
        let receipt = materialized_receipt_tx(&mut tx, "g", &f.task)
            .await?
            .context("actual materialization missing")?;
        let binding = validate_artifact_binding_tx(
            &mut tx,
            &f.writer,
            &f.task,
            current.work.version,
            &scope,
            receipt.deadline - 1,
        )
        .await?
        .into_provenance();
        validate_artifact_binding_current_tx(
            &mut tx,
            &binding,
            &inputs,
            &scope,
            receipt.deadline - 1,
        )
        .await?;
        assert!(
            validate_artifact_binding_tx(
                &mut tx,
                &f.writer,
                &f.task,
                current.work.version,
                &scope,
                receipt.deadline
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("materialized_deadline_expired")
        );
        assert!(
            validate_artifact_binding_current_tx(
                &mut tx,
                &binding,
                &inputs,
                &scope,
                receipt.deadline
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("materialized_deadline_expired")
        );
        tx.rollback().await?;
        Ok(())
    }

    async fn continuation_fixture_with_blockers(
        explicit_action: bool,
        retained_scope: bool,
        held_attempt: bool,
        replace_blockers: bool,
    ) -> Result<ContinuationFixture> {
        use crate::decision_recovery::{self, DecisionBlockersRequest, ScopedBlocker};
        use crate::execution::{self, Checked, ExecutionCauseRef};
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        for name in ["writer", "reviewer"] {
            store.register("g", name, false).await?;
        }
        let writer = store.mailbox("g", "writer").await?;
        let reviewer = store.mailbox("g", "reviewer").await?;
        let mut source = request("source");
        source.draft.work.owner = "reviewer".into();
        source.draft.contract.budget.max_elapsed_seconds = 600;
        source.draft.contract.budget.max_attempts = 4;
        store.task_create(&writer, source, 100).await?;
        let mut tx = store.pool().begin().await?;
        crate::progress::change_policy_tx(
            &mut tx,
            &writer,
            "source",
            &crate::progress::PolicyChange {
                key: "finite-progress".into(),
                task_version: 1,
                expected_revision: None,
                reason: "Real elapsed strategy cause".into(),
                policy: crate::progress::ProgressPolicy {
                    max_segments_without_milestone: 2,
                    max_elapsed_without_milestone: Some(10),
                    milestones: vec![],
                },
            },
            100,
        )
        .await?;
        tx.commit().await?;
        if held_attempt {
            let revision = store
                .execution_inspect(&writer, "source")
                .await?
                .revision
                .context("execution revision missing")?;
            let mut tx = store.pool().begin().await?;
            let Checked::Ready(correlation) = execution::claim_attempt_tx(
                &mut tx,
                &UnclosedTestRuntime,
                &execution::ClaimRequest {
                    group: "g".into(),
                    task: "source".into(),
                    revision,
                    key: "original-held-attempt".into(),
                },
                100,
            )
            .await?
            else {
                anyhow::bail!("original attempt reservation held")
            };
            assert!(matches!(
                execution::expose_dispatch_tx(
                    &mut tx,
                    &UnclosedTestRuntime,
                    &correlation,
                    "test-dispatcher",
                    1,
                    101
                )
                .await?,
                Checked::Ready(_)
            ));
            assert!(matches!(
                execution::admit_execution_tx(&mut tx, &UnclosedTestRuntime, &correlation, 102)
                    .await?,
                Checked::Ready(_)
            ));
            tx.commit().await?;
        }
        store.execution_reconcile("g", 111).await?;
        let execution = store.execution_inspect(&writer, "source").await?;
        let cause = execution
            .causes
            .iter()
            .find(|c| c.code == "no_progress_elapsed")
            .context("actual elapsed cause missing")?;
        let mut tx = store.pool().begin().await?;
        let case = decision_recovery::ensure_execution_case_tx(
            &mut tx,
            &ExecutionCauseRef {
                group: "g".into(),
                source_task: "source".into(),
                cause_generation: cause.id.clone(),
            },
            112,
        )
        .await?;
        refresh_recovery_projection_tx(&mut tx, "g", &BTreeMap::from([(case.id, case.version)]))
            .await?;
        tx.commit().await?;
        let expected = DecisionSourceExpectation {
            source: case.current_source.source.clone(),
            input_epoch: case.current_source.input_epoch,
            candidate: case.current_source.candidate.clone(),
            outcome: case.current_source.outcome.clone(),
        };
        let mut contract = store
            .task_inspect(&writer, "source")
            .await?
            .model
            .context("source contract missing")?
            .contract;
        contract.allow_delegation = false;
        let policy = DecisionPolicyDecision {
            key: "source-continuation-policy".into(),
            expected_revision: None,
            source: expected.clone(),
            reason: "Actual finite source consent".into(),
            policy: DecisionPolicy {
                id: "continuation-review".into(),
                contract,
                actions: vec![if explicit_action {
                    DecisionAction::ContinueStrategy
                } else {
                    DecisionAction::Recommend
                }],
                reviewer: Some("reviewer".into()),
                allow_writer_fallback: true,
                deadline: case.hard_due,
                authority_ref: "source-writer-consent".into(),
                revoked: false,
            },
        };
        store.decision_policy(&writer, policy.clone(), 113).await?;
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Materialized(materialized) = materialize_decision_task_tx(
            &mut tx,
            "g",
            &DecisionMaterializationRequest {
                policy: policy.policy.id.clone(),
                policy_revision: 1,
                case_id: case.id,
                case_version: case.version,
                source: expected,
            },
            114,
        )
        .await?
        else {
            anyhow::bail!("actual decision materialization refused")
        };
        tx.commit().await?;
        let task = materialized.task;
        let initial_responsible: Vec<String> = sqlx::query_scalar(
            "SELECT responsible FROM decision_blockers WHERE case_id=? ORDER BY ordinal",
        )
        .bind(case.id)
        .fetch_all(store.pool())
        .await?;
        assert_eq!(initial_responsible, vec![reviewer.name.clone()]);
        let before = store.task_inspect(&writer, &task).await?;
        let draft = phase_candidate(&store, &reviewer, &before, "actual-review").await?;
        let candidate = store.task_candidate(&reviewer, &task, draft, 115).await?;
        let current_case = store.decision_case(&writer, case.id).await?;
        let mut actions = vec![TaskAction::Execute];
        if retained_scope {
            actions.push(TaskAction::Scope("report".into()));
        }
        let installed = if replace_blockers {
            store
                .set_decision_blockers(
                    &writer,
                    DecisionBlockersRequest {
                        key: "actual-case-blockers".into(),
                        case_id: case.id,
                        case_version: current_case.version,
                        reason: "Original Work waits for this finite decision".into(),
                        blockers: actions
                            .into_iter()
                            .map(|action| ScopedBlocker {
                                selector: ActionNode {
                                    task: "source".into(),
                                    action,
                                },
                                waiting: Some(ActionNode {
                                    task: task.clone(),
                                    action: TaskAction::AcceptResult,
                                }),
                                responsible: writer.name.clone(),
                                reason: "Source decision prerequisite".into(),
                                evidence: vec![
                                    "actual materialization and reviewer candidate".into(),
                                ],
                            })
                            .collect(),
                    },
                    116,
                )
                .await?
        } else {
            current_case
        };
        let mut decision = decision(
            store.task_inspect(&writer, &task).await?.work.version,
            "ordinary-outcome",
        );
        decision.outcome = OutcomeChange::Success {
            kind: OutcomeKind::Accepted,
            candidate: candidate.id,
        };
        let request = DecisionContinuation {
            key: "atomic-source-and-outcome".into(),
            case_version: installed.version,
            policy_revision: 1,
            decision,
            continuation: execution::ContinueStrategy {
                key: "one-source-segment".into(),
                reason: "Original writer permits one bounded segment".into(),
                execution_revision: store
                    .execution_inspect(&writer, "source")
                    .await?
                    .revision
                    .context("source execution revision missing")?,
                additional_segments: 1,
                expires_at: 200,
            },
        };
        Ok(ContinuationFixture {
            _temp: temp,
            store,
            writer,
            reviewer,
            task,
            policy,
            request,
        })
    }

    // Compare actual durable rows, including audits and receipts, across failures.
    pub(super) async fn continuation_state(store: &Store) -> Result<BTreeMap<String, String>> {
        let mut snapshot = BTreeMap::new();
        for table in [
            "followups",
            "followup_history",
            "attention_occurrences",
            "coordination_events",
            "event_receipts",
            "outbox",
            "work_changes",
            "task_materializations",
            "task_decision_policies",
            "task_decision_policy_history",
            "work_items",
            "task_models",
            "task_results",
            "task_model_events",
            "task_decisions",
            "task_blocking_edges",
            "decision_cases",
            "decision_blockers",
            "decision_audit",
            "operator_obligations",
            "execution_tasks",
            "execution_causes",
            "execution_events",
            "execution_receipts",
            "execution_budgets",
            "execution_clock",
            "execution_attempts",
            "execution_slots",
        ] {
            let columns: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                    .bind(table)
                    .fetch_all(store.pool())
                    .await?;
            ensure!(!columns.is_empty(), "snapshot table missing: {table}");
            let fields = columns
                .iter()
                .map(|c| format!("'{c}',\"{c}\""))
                .collect::<Vec<_>>()
                .join(",");
            let order = columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT json_group_array(json(row_json)) FROM (SELECT json_object({fields}) AS row_json FROM {table} ORDER BY {order})"
            );
            let rows: String = sqlx::query_scalar(&sql).fetch_one(store.pool()).await?;
            snapshot.insert(table.into(), rows);
        }
        Ok(snapshot)
    }

    #[tokio::test]
    async fn recovery_selector_uses_real_policy_history_and_historical_receipt() -> Result<()> {
        let (_temp, store, writer, _reviewer, policy, request) = materializer_fixture().await?;
        let before = continuation_state(&store).await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            materialize_recovery_case_tx(&mut tx, "g", request.case_id, request.case_version, 4102)
                .await?,
            DecisionMaterialization::Refused(MaterializationRefusal::PolicyMissing)
        ));
        tx.commit().await?;
        assert_eq!(continuation_state(&store).await?, before);
        store.decision_policy(&writer, policy.clone(), 4102).await?;
        let before = continuation_state(&store).await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            materialize_recovery_case_tx(
                &mut tx,
                "g",
                request.case_id,
                request.case_version + 1,
                4103
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(continuation_state(&store).await?, before);
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Materialized(receipt) =
            materialize_recovery_case_tx(&mut tx, "g", request.case_id, request.case_version, 4103)
                .await?
        else {
            anyhow::bail!("actual standing consent did not materialize")
        };
        tx.commit().await?;
        let mut revoked = policy;
        revoked.key = "selector-revoke".into();
        revoked.expected_revision = Some(1);
        revoked.policy.revoked = true;
        store.decision_policy(&writer, revoked, 4104).await?;
        let before = continuation_state(&store).await?;
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Replayed(replayed) = materialize_recovery_case_tx(
            &mut tx,
            "g",
            request.case_id,
            request.case_version + 99,
            5000,
        )
        .await?
        else {
            anyhow::bail!("actual historical receipt was not replayed")
        };
        assert_eq!(
            serde_json::to_value(replayed)?,
            serde_json::to_value(receipt)?
        );
        tx.commit().await?;
        assert_eq!(continuation_state(&store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_selector_refusal_and_corrupt_history_preserve_responsibility() -> Result<()> {
        for mode in ["revoked", "expired", "history"] {
            let (_temp, store, writer, _reviewer, mut policy, request) =
                materializer_fixture().await?;
            if mode == "revoked" {
                policy.policy.revoked = true;
            }
            if mode == "expired" {
                policy.policy.deadline = 4103;
            }
            store.decision_policy(&writer, policy, 4102).await?;
            if mode == "history" {
                // Corrupt mutable registry bytes, leaving actual immutable history intact.
                sqlx::query("UPDATE task_decision_policies SET payload=json_set(payload,'$.authority_ref','tampered')")
                    .execute(store.pool()).await?;
            }
            let before = continuation_state(&store).await?;
            let mut tx = store.pool().begin().await?;
            let result = materialize_recovery_case_tx(
                &mut tx,
                "g",
                request.case_id,
                request.case_version,
                4103,
            )
            .await;
            match mode {
                "revoked" => assert!(matches!(
                    result?,
                    DecisionMaterialization::Refused(MaterializationRefusal::PolicyRevoked)
                )),
                "expired" => assert!(matches!(
                    result?,
                    DecisionMaterialization::Refused(MaterializationRefusal::PolicyExpired)
                )),
                _ => assert!(
                    format!("{:#}", result.unwrap_err())
                        .contains("decision_policy_history_corrupt")
                ),
            }
            tx.rollback().await?;
            assert_eq!(continuation_state(&store).await?, before);
        }
        Ok(())
    }

    #[tokio::test]
    async fn actual_work_fallback_transfers_only_initial_reviewer_responsibility_atomically()
    -> Result<()> {
        let f = continuation_fixture_with_blockers(true, false, false, false).await?;
        let before_task = f.store.task_inspect(&f.writer, &f.task).await?;
        let before_model = before_task.model.as_ref().context("model missing")?;
        let candidate_id = before_model
            .current_candidate
            .clone()
            .context("real reviewer candidate missing")?;
        let candidate_payload: String =
            sqlx::query_scalar("SELECT payload FROM task_results WHERE id=?")
                .bind(&candidate_id)
                .fetch_one(f.store.pool())
                .await?;
        let case_id: i64 = sqlx::query_scalar("SELECT case_id FROM decision_blockers")
            .fetch_one(f.store.pool())
            .await?;
        let before_case = f.store.decision_case(&f.writer, case_id).await?;
        let full_row_sql = "SELECT json_object('case_version',case_version,'ordinal',ordinal,'selector',selector,'waiting',waiting,'responsible',responsible,'reason',reason,'evidence',evidence) FROM decision_blockers WHERE case_id=? ORDER BY ordinal";
        let before_full: Vec<serde_json::Value> = sqlx::query_scalar::<_, String>(full_row_sql)
            .bind(case_id)
            .fetch_all(f.store.pool())
            .await?
            .into_iter()
            .map(|row| serde_json::from_str(&row))
            .collect::<std::result::Result<_, _>>()?;
        let row_sql = "SELECT json_object('ordinal',ordinal,'selector',selector,'waiting',waiting,'reason',reason,'evidence',evidence) FROM decision_blockers WHERE case_id=? ORDER BY ordinal";
        let before_rows: Vec<String> = sqlx::query_scalar(row_sql)
            .bind(case_id)
            .fetch_all(f.store.pool())
            .await?;
        assert_eq!(before_rows.len(), 1);
        let responsible: String =
            sqlx::query_scalar("SELECT responsible FROM decision_blockers WHERE case_id=?")
                .bind(case_id)
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!(responsible, f.reviewer.name);
        let budgets =
            serde_json::to_value(f.store.execution_inspect(&f.writer, &f.task).await?.budgets)?;
        let fallback = DecisionWriterFallback {
            key: "actual-work-fallback".into(),
            version: before_task.work.version,
            case_version: before_case.version,
            policy_revision: 1,
            reason: "Original writer takes finite decision".into(),
        };
        sqlx::query("CREATE TRIGGER fail_work_fallback BEFORE INSERT ON decision_audit WHEN NEW.operation='decision_phase_reconciled' BEGIN SELECT RAISE(ABORT,'forced_work_fallback'); END")
            .execute(f.store.pool()).await?;
        let before = continuation_state(&f.store).await?;
        let error = f
            .store
            .decision_writer_fallback(&f.writer, &f.task, fallback.clone(), 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_work_fallback"),
            "{error:#}"
        );
        assert_eq!(continuation_state(&f.store).await?, before);
        sqlx::query("DROP TRIGGER fail_work_fallback")
            .execute(f.store.pool())
            .await?;
        let after = f
            .store
            .decision_writer_fallback(&f.writer, &f.task, fallback.clone(), 117)
            .await?;
        let after_case = f.store.decision_case(&f.writer, case_id).await?;
        assert_eq!(after.work.owner, f.writer.name);
        assert_eq!(after.work.deadline, before_task.work.deadline);
        assert_eq!(
            after.model.as_ref().context("model missing")?.contract,
            before_model.contract
        );
        assert_eq!(after_case.hard_due, before_case.hard_due);
        assert_eq!(after_case.current_source, before_case.current_source);
        assert_eq!(
            serde_json::to_value(f.store.execution_inspect(&f.writer, &f.task).await?.budgets)?,
            budgets
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(row_sql)
                .bind(case_id)
                .fetch_all(f.store.pool())
                .await?,
            before_rows
        );
        let row =
            sqlx::query("SELECT responsible,case_version FROM decision_blockers WHERE case_id=?")
                .bind(case_id)
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!(row.get::<String, _>("responsible"), f.writer.name);
        assert_eq!(row.get::<i64, _>("case_version"), after_case.version);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT payload FROM task_results WHERE id=?")
                .bind(candidate_id)
                .fetch_one(f.store.pool())
                .await?,
            candidate_payload
        );
        let mut second = fallback;
        second.key = "forbidden-work-second-phase".into();
        second.version = after.work.version;
        second.case_version = after_case.version;
        let after_full: Vec<serde_json::Value> = sqlx::query_scalar::<_, String>(full_row_sql)
            .bind(case_id)
            .fetch_all(f.store.pool())
            .await?
            .into_iter()
            .map(|row| serde_json::from_str(&row))
            .collect::<std::result::Result<_, _>>()?;
        let canonical: String = sqlx::query_scalar("SELECT canonical FROM decision_audit WHERE case_id=? AND operation='decision_phase_reconciled' ORDER BY id DESC LIMIT 1")
            .bind(case_id).fetch_one(f.store.pool()).await?;
        let audit: serde_json::Value = serde_json::from_str(&canonical)?;
        assert_eq!(audit["owner_before"], f.reviewer.name);
        assert_eq!(audit["owner_after"], f.writer.name);
        assert_eq!(audit["blockers"], serde_json::to_value(before_full)?);
        assert_eq!(audit["blockers_after"], serde_json::to_value(after_full)?);
        let event: i64 = sqlx::query_scalar(
            "SELECT id FROM task_model_events WHERE task=? ORDER BY id DESC LIMIT 1",
        )
        .bind(&f.task)
        .fetch_one(f.store.pool())
        .await?;
        assert_eq!(audit["model_event"], event);
        assert!(
            f.store
                .decision_writer_fallback(&f.writer, &f.task, second, 118)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn tracking_page_preserves_terminal_legacy_and_held_work_without_writes() -> Result<()> {
        let f = continuation_fixture(true, false, true).await?;
        let mut legacy = request("z-terminal").draft.work;
        legacy.state = TaskState::Cancelled;
        f.store.work_create(&f.writer, legacy, 117).await?;
        let mut unrelated = request("unrelated").draft.work;
        unrelated.owner = f.reviewer.name.clone();
        f.store.work_create(&f.reviewer, unrelated, 117).await?;
        let before = continuation_state(&f.store).await?;
        let first = f.store.task_tracking_page(&f.writer, "", 1).await?;
        assert_eq!(first.items.len(), 1);
        assert!(first.has_more);
        let second = f
            .store
            .task_tracking_page(
                &f.writer,
                first.next_cursor.as_deref().context("cursor missing")?,
                1,
            )
            .await?;
        assert_eq!(second.items[0].work.id, "source");
        let last = f
            .store
            .task_tracking_page(
                &f.writer,
                second.next_cursor.as_deref().context("cursor missing")?,
                1,
            )
            .await?;
        assert_eq!(last.items[0].work.id, "z-terminal");
        assert_eq!(last.items[0].work.state, TaskState::Cancelled);
        assert!(last.items[0].model.is_none());
        assert_eq!(last.items[0].execution_hold, "legacy_untracked");
        assert!(!last.has_more && last.next_cursor.is_none());
        assert_eq!(continuation_state(&f.store).await?, before);
        for limit in [0, 51] {
            assert!(
                f.store
                    .task_tracking_page(&f.writer, "", limit)
                    .await
                    .is_err()
            );
        }
        let mut forged = f.writer.clone();
        forged.group_name = "another".into();
        assert!(f.store.task_tracking_page(&forged, "", 1).await.is_err());
        forged = f.writer.clone();
        forged.name = f.reviewer.name.clone();
        assert!(f.store.task_tracking_page(&forged, "", 1).await.is_err());
        assert_eq!(continuation_state(&f.store).await?, before);
        sqlx::query("UPDATE task_models SET contract='{}' WHERE task='source'")
            .execute(f.store.pool())
            .await?;
        let before = continuation_state(&f.store).await?;
        assert!(f.store.task_tracking_page(&f.writer, "", 50).await.is_err());
        assert_eq!(continuation_state(&f.store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn continuation_rejects_stale_case_candidate_policy_and_prior_inner_receipt() -> Result<()>
    {
        let f = continuation_fixture(true, false, false).await?;
        for mismatch in ["case", "policy"] {
            let mut invalid = f.request.clone();
            if mismatch == "case" {
                invalid.case_version += 1;
            } else {
                invalid.policy_revision += 1;
            }
            let before = continuation_state(&f.store).await?;
            assert!(
                f.store
                    .decision_continue_strategy(&f.writer, &f.task, invalid, 117)
                    .await
                    .is_err()
            );
            assert_eq!(continuation_state(&f.store).await?, before);
        }
        let task = f.store.task_inspect(&f.writer, &f.task).await?;
        let draft = phase_candidate(&f.store, &f.reviewer, &task, "revised-real-review").await?;
        f.store
            .task_candidate(&f.reviewer, &f.task, draft, 117)
            .await?;
        let before = continuation_state(&f.store).await?;
        let mut old_candidate = f.request.clone();
        old_candidate.decision.version =
            f.store.task_inspect(&f.writer, &f.task).await?.work.version;
        assert!(
            f.store
                .decision_continue_strategy(&f.writer, &f.task, old_candidate, 118)
                .await
                .is_err()
        );
        assert_eq!(continuation_state(&f.store).await?, before);

        let g = continuation_fixture(true, false, false).await?;
        let mut revoke = g.policy.clone();
        revoke.key = "revoke-before-continuation".into();
        revoke.expected_revision = Some(1);
        revoke.policy.revoked = true;
        g.store.decision_policy(&g.writer, revoke, 117).await?;
        let before = continuation_state(&g.store).await?;
        assert!(
            g.store
                .decision_continue_strategy(&g.writer, &g.task, g.request.clone(), 118)
                .await
                .is_err()
        );
        assert_eq!(continuation_state(&g.store).await?, before);

        let h = continuation_fixture(true, false, false).await?;
        // Create the actual ordinary inner receipt. Reusing it cannot turn its
        // historical outcome into fresh source-disposition authority.
        h.store
            .task_decide(&h.writer, &h.task, h.request.decision.clone(), 117)
            .await?;
        let before = continuation_state(&h.store).await?;
        assert!(
            h.store
                .decision_continue_strategy(&h.writer, &h.task, h.request, 118)
                .await
                .is_err()
        );
        assert_eq!(continuation_state(&h.store).await?, before);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM execution_events WHERE kind='strategy_continuation'"
            )
            .fetch_one(h.store.pool())
            .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn actual_nonempty_case_continuation_and_outcome_commit_and_replay() -> Result<()> {
        use crate::execution::Checked;
        let f = continuation_fixture(true, false, false).await?;
        let before_source = f.store.task_inspect(&f.writer, "source").await?;
        let before_source_budgets = serde_json::to_value(
            f.store
                .execution_inspect(&f.writer, "source")
                .await?
                .budgets,
        )?;
        let before_decision_budgets =
            serde_json::to_value(f.store.execution_inspect(&f.writer, &f.task).await?.budgets)?;
        let case_id: i64 = sqlx::query_scalar(
            "SELECT json_extract(receipt,'$.case_id') FROM task_materializations WHERE group_name='g' AND decision_task=?",
        )
        .bind(&f.task)
        .fetch_one(f.store.pool())
        .await?;
        let original_case = f.store.decision_case(&f.writer, case_id).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_blockers")
                .fetch_one(f.store.pool())
                .await?,
            1
        );
        let Checked::Ready(result) = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, f.request.clone(), 117)
            .await?
        else {
            anyhow::bail!("real typed blocker continuation held")
        };
        assert_eq!(result.case.state, "handled");
        assert_eq!(result.case.hard_due, original_case.hard_due);
        assert_eq!(result.case.original_source, original_case.original_source);
        assert_eq!(result.decision.work.state, TaskState::Accepted);
        let after_source = f.store.task_inspect(&f.writer, "source").await?;
        assert_eq!(after_source.work.version, before_source.work.version);
        assert_eq!(
            serde_json::to_value(after_source.model)?,
            serde_json::to_value(before_source.model)?
        );
        assert_eq!(
            serde_json::to_value(
                f.store
                    .execution_inspect(&f.writer, "source")
                    .await?
                    .budgets
            )?,
            before_source_budgets
        );
        assert_eq!(
            serde_json::to_value(f.store.execution_inspect(&f.writer, &f.task).await?.budgets)?,
            before_decision_budgets
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_blockers")
                .fetch_one(f.store.pool())
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM operator_obligations WHERE case_id=?"
            )
            .bind(result.case.id)
            .fetch_one(f.store.pool())
            .await?,
            "handled"
        );
        let mut revoke = f.policy.clone();
        revoke.key = "revoke-after-commit".into();
        revoke.expected_revision = Some(1);
        revoke.policy.revoked = true;
        f.store.decision_policy(&f.writer, revoke, 118).await?;
        let after = continuation_state(&f.store).await?;
        let Checked::Ready(replay) = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, f.request.clone(), 2000)
            .await?
        else {
            anyhow::bail!("historical outer receipt held")
        };
        assert_eq!(serde_json::to_value(replay)?, serde_json::to_value(result)?);
        assert_eq!(continuation_state(&f.store).await?, after);
        let mut conflict = f.request;
        conflict.continuation.additional_segments = 2;
        let error = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, conflict, 2000)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("decision_key_conflict"),
            "{error:#}"
        );
        assert_eq!(continuation_state(&f.store).await?, after);
        Ok(())
    }

    #[tokio::test]
    async fn retained_scope_hold_rolls_back_entire_continuation_stage() -> Result<()> {
        let f = continuation_fixture(true, true, false).await?;
        let before = continuation_state(&f.store).await?;
        let result = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, f.request, 117)
            .await?;
        let crate::execution::Checked::Held(reasons) = result else {
            anyhow::bail!("unrelated scope must hold")
        };
        assert!(
            reasons.iter().any(|r| r.starts_with("model:")),
            "{reasons:?}"
        );
        assert_eq!(continuation_state(&f.store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn post_source_and_post_model_failures_restore_every_staged_row() -> Result<()> {
        for (trigger, expected) in [
            (
                "CREATE TRIGGER fail_continuation BEFORE INSERT ON task_results WHEN NEW.kind='accepted' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM execution_events WHERE kind='strategy_continuation') THEN RAISE(ABORT,'forced_after_source') ELSE RAISE(ABORT,'source_not_applied') END; END",
                "forced_after_source",
            ),
            (
                "CREATE TRIGGER fail_continuation BEFORE INSERT ON decision_audit WHEN NEW.operation='decision_continuation_stage_completed' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM task_results WHERE kind='accepted') THEN RAISE(ABORT,'forced_after_model') ELSE RAISE(ABORT,'model_not_applied') END; END",
                "forced_after_model",
            ),
        ] {
            let f = continuation_fixture(true, false, false).await?;
            sqlx::query(trigger).execute(f.store.pool()).await?;
            let before = continuation_state(&f.store).await?;
            let error = f
                .store
                .decision_continue_strategy(&f.writer, &f.task, f.request.clone(), 117)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "expected {expected}: {error:#}"
            );
            assert_eq!(continuation_state(&f.store).await?, before);
            sqlx::query("DROP TRIGGER fail_continuation")
                .execute(f.store.pool())
                .await?;
            assert!(matches!(
                f.store
                    .decision_continue_strategy(&f.writer, &f.task, f.request, 117)
                    .await?,
                crate::execution::Checked::Ready(_)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn actual_held_attempt_keeps_original_slot_during_continuation_refusal() -> Result<()> {
        let f = continuation_fixture(true, false, true).await?;
        let before = continuation_state(&f.store).await?;
        let error = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, f.request, 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("execution_cleanup_unresolved"),
            "{error:#}"
        );
        assert_eq!(continuation_state(&f.store).await?, before);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM execution_attempts WHERE holds_slot=1"
            )
            .fetch_one(f.store.pool())
            .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn continuation_requires_original_writer_explicit_policy_and_outcome_only() -> Result<()>
    {
        let f = continuation_fixture(true, false, false).await?;
        let before = continuation_state(&f.store).await?;
        let error = f
            .store
            .decision_continue_strategy(&f.reviewer, &f.task, f.request.clone(), 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("original_source_writer_required"),
            "{error:#}"
        );
        let mut changed = f.request.clone();
        changed.decision.work_patch.next_action = Some("unrelated edit".into());
        let error = f
            .store
            .decision_continue_strategy(&f.writer, &f.task, changed, 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("decision_continuation_requires_outcome_only"),
            "{error:#}"
        );
        assert_eq!(continuation_state(&f.store).await?, before);
        let g = continuation_fixture(false, false, false).await?;
        let before = continuation_state(&g.store).await?;
        let error = g
            .store
            .decision_continue_strategy(&g.writer, &g.task, g.request, 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("explicit_strategy_continuation_policy_required"),
            "{error:#}"
        );
        assert_eq!(continuation_state(&g.store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn actual_reviewer_candidate_and_single_writer_phase_preserve_original_limits()
    -> Result<()> {
        let (_temp, store, writer, reviewer, policy, materialization) =
            materializer_fixture().await?;
        store.decision_policy(&writer, policy.clone(), 4102).await?;
        let mut tx = store.pool().begin().await?;
        let receipt =
            match materialize_decision_task_tx(&mut tx, "g", &materialization, 4103).await? {
                DecisionMaterialization::Materialized(receipt) => receipt,
                other => anyhow::bail!("unexpected materialization: {other:?}"),
            };
        tx.commit().await?;
        let original = store.task_inspect(&writer, &receipt.task).await?;
        let budget_sql = "SELECT json_object('max',max_attempts,'elapsed',elapsed_seconds,'cost_limit',cost_limit,'unit',cost_unit,'spent',attempts_spent,'reserved',attempts_reserved,'cost_spent',cost_spent,'cost_reserved',cost_reserved,'unknown',unknown_cost,'anchor',anchor,'deadline',deadline) FROM execution_budgets WHERE task=?";
        let budget: String = sqlx::query_scalar(budget_sql)
            .bind(&receipt.task)
            .fetch_one(store.pool())
            .await?;
        let candidate = phase_candidate(&store, &reviewer, &original, "review-candidate").await?;
        let result = store
            .task_candidate(&reviewer, &receipt.task, candidate.clone(), 4104)
            .await?;
        assert_eq!(result.actor, reviewer.name);
        let after_candidate = store.task_inspect(&writer, &receipt.task).await?;
        let case = store.decision_case(&writer, receipt.case_id).await?;
        assert!(!case.requires_reassessment);
        assert_ne!(case.state, "handled");
        assert_eq!(after_candidate.work.owner, reviewer.name);
        let origin: String = sqlx::query_scalar(
            "SELECT origin FROM task_model_events WHERE task=? ORDER BY id DESC LIMIT 1",
        )
        .bind(&receipt.task)
        .fetch_one(store.pool())
        .await?;
        assert_eq!(origin, "policy_reviewer");
        let fallback = DecisionWriterFallback {
            key: "one-writer-phase".into(),
            version: after_candidate.work.version,
            case_version: case.version,
            policy_revision: receipt.policy_revision,
            reason: "Original writer takes the one permitted decision phase".into(),
        };
        assert!(
            store
                .decision_writer_fallback(&reviewer, &receipt.task, fallback.clone(), 4105)
                .await
                .is_err()
        );
        let after = store
            .decision_writer_fallback(&writer, &receipt.task, fallback.clone(), 4105)
            .await?;
        assert_eq!(after.work.owner, writer.name);
        assert_eq!(after.work.writer, original.work.writer);
        assert_eq!(after.work.deadline, original.work.deadline);
        let model = after.model.as_ref().context("model missing")?;
        assert_eq!(
            model.contract,
            original.model.as_ref().context("model missing")?.contract
        );
        assert!(model.current_candidate.is_none());
        assert!(
            model.input_epoch
                > after_candidate
                    .model
                    .as_ref()
                    .context("model missing")?
                    .input_epoch
        );
        let after_budget: String = sqlx::query_scalar(budget_sql)
            .bind(&receipt.task)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(after_budget, budget);
        let after_case = store.decision_case(&writer, receipt.case_id).await?;
        assert!(!after_case.requires_reassessment);
        assert_eq!(after_case.hard_due, case.hard_due);
        assert_eq!(after_case.current_source, case.current_source);
        let mut repeated = fallback.clone();
        repeated.key = "forbidden-second-phase".into();
        repeated.version = after.work.version;
        repeated.case_version = after_case.version;
        assert!(
            store
                .decision_writer_fallback(&writer, &receipt.task, repeated, 4106)
                .await
                .is_err()
        );
        let replay = store
            .task_candidate(&reviewer, &receipt.task, candidate, 5000)
            .await?;
        assert_eq!(
            replay.id, result.id,
            "historical evidence survives the phase"
        );
        let mut revoke = policy;
        revoke.key = "revoke-phase-policy".into();
        revoke.expected_revision = Some(1);
        revoke.policy.revoked = true;
        store.decision_policy(&writer, revoke, 4106).await?;
        let replay = store
            .decision_writer_fallback(&writer, &receipt.task, fallback, 5000)
            .await?;
        assert_eq!(serde_json::to_value(replay)?, serde_json::to_value(&after)?);
        assert!(
            store
                .task_capture_inputs(&writer, &receipt.task, after.work.version, Phase::Accept)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn actual_candidate_phase_storage_failure_rolls_back_model_case_and_audit() -> Result<()>
    {
        let (_temp, store, writer, reviewer, policy, request) = materializer_fixture().await?;
        store.decision_policy(&writer, policy, 4102).await?;
        let mut tx = store.pool().begin().await?;
        let receipt = match materialize_decision_task_tx(&mut tx, "g", &request, 4103).await? {
            DecisionMaterialization::Materialized(receipt) => receipt,
            other => anyhow::bail!("unexpected materialization: {other:?}"),
        };
        tx.commit().await?;
        let task = store.task_inspect(&writer, &receipt.task).await?;
        let case = store.decision_case(&writer, receipt.case_id).await?;
        let request = phase_candidate(&store, &reviewer, &task, "atomic-review").await?;
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM task_model_events")
            .fetch_one(store.pool())
            .await?;
        sqlx::query("CREATE TRIGGER fail_phase_audit BEFORE INSERT ON decision_audit WHEN NEW.operation='decision_phase_reconciled' BEGIN SELECT RAISE(FAIL,'forced_phase_failure'); END")
            .execute(store.pool()).await?;
        let error = store
            .task_candidate(&reviewer, &receipt.task, request.clone(), 4104)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_phase_failure"),
            "{error:#}"
        );
        assert_eq!(
            serde_json::to_value(store.task_inspect(&writer, &receipt.task).await?)?,
            serde_json::to_value(task)?
        );
        assert_eq!(store.decision_case(&writer, receipt.case_id).await?, case);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_model_events")
                .fetch_one(store.pool())
                .await?,
            events
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_results")
                .fetch_one(store.pool())
                .await?,
            0
        );
        sqlx::query("DROP TRIGGER fail_phase_audit")
            .execute(store.pool())
            .await?;
        store
            .task_candidate(&reviewer, &receipt.task, request, 4105)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn model_after_event_receipt_tracks_actual_case_and_rejects_stale_state() -> Result<()> {
        let (_temp, store, writer, _reviewer, policy, request) = materializer_fixture().await?;
        store.decision_policy(&writer, policy, 4102).await?;
        let mut tx = store.pool().begin().await?;
        let receipt = match materialize_decision_task_tx(&mut tx, "g", &request, 4103).await? {
            DecisionMaterialization::Materialized(receipt) => receipt,
            other => anyhow::bail!("unexpected materialization: {other:?}"),
        };
        tx.commit().await?;
        let task = store.task_inspect(&writer, &receipt.task).await?;
        let mut change = decision(task.work.version, "cancel-finite-decision");
        change.outcome = OutcomeChange::Negative {
            kind: OutcomeKind::Cancelled,
            revision: "withdraw-review".into(),
        };
        let mut tx = store.pool().begin().await?;
        let applied = Store::task_decide_with_reconciliation_tx(
            &mut tx,
            &writer,
            &receipt.task,
            change.clone(),
            4104,
        )
        .await?;
        let proof = applied
            .applied
            .context("new mutation did not return actual receipt")?;
        validate_applied_model_decision_tx(&mut tx, &proof).await?;
        assert_eq!(proof.group(), "g");
        assert_eq!(proof.task(), receipt.task);
        assert_eq!(proof.actor(), "writer");
        assert_eq!(
            proof.outcome().map(|(_, kind)| kind),
            Some(OutcomeKind::Cancelled)
        );
        let (case_version, event) = proof
            .case_after(request.case_id)
            .context("mandatory recovery reconciliation missing from receipt")?;
        let actual: (i64, i64) =
            sqlx::query_as("SELECT version,reassessment_event FROM decision_cases WHERE id=?")
                .bind(request.case_id)
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(actual, (case_version, event));
        assert_eq!(proof.root_event(), Some((event, applied.view.work.version)));
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        let replay = Store::task_decide_with_reconciliation_tx(
            &mut tx,
            &writer,
            &receipt.task,
            change,
            4105,
        )
        .await?;
        assert!(
            replay.applied.is_none(),
            "history cannot mint fresh after-state authority"
        );
        validate_applied_model_decision_tx(&mut tx, &proof).await?;
        Store::task_decide_with_reconciliation_tx(
            &mut tx,
            &writer,
            &receipt.task,
            decision(applied.view.work.version, "later-audit"),
            4106,
        )
        .await?;
        assert!(
            validate_applied_model_decision_tx(&mut tx, &proof)
                .await
                .is_err()
        );
        tx.rollback().await?;
        Ok(())
    }

    async fn materializer_fixture() -> Result<(
        tempfile::TempDir,
        Store,
        Mailbox,
        Mailbox,
        DecisionPolicyDecision,
        DecisionMaterializationRequest,
    )> {
        use crate::decision_recovery::{CaseCorrection, Obligation};
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "reviewer", false).await?;
        let writer = store.mailbox("g", "writer").await?;
        let reviewer = store.mailbox("g", "reviewer").await?;
        let message = store
            .publish(
                &writer,
                crate::store::Publish {
                    recipients: vec!["reviewer".into()],
                    key: "original-source".into(),
                    summary: "Inspect source".into(),
                    body: "Original finite obligation".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                100,
            )
            .await?;
        let source = Obligation::Delivery {
            message,
            recipient: "reviewer".into(),
        };
        let case = store
            .recover_expired_obligation(&writer, "recover-source", source.clone(), 4100)
            .await?;
        let view = store.inspect_obligation(&writer, source.clone()).await?;
        // Use the real original-writer correction; no invented episode or case row.
        let case = store
            .correct_decision_case(
                &writer,
                CaseCorrection {
                    key: "finite-case-correction".into(),
                    case_id: case.id,
                    version: case.version,
                    source: view,
                    reason: "Permit one finite source review".into(),
                    evidence: vec!["source-consent".into()],
                    review_at: 4200,
                    hard_due: 4500,
                },
                4101,
            )
            .await?;
        let expected = DecisionSourceExpectation {
            source,
            input_epoch: None,
            candidate: None,
            outcome: None,
        };
        let mut contract = request("template").draft.contract;
        contract.allow_delegation = false;
        let policy = DecisionPolicyDecision {
            key: "source-policy".into(),
            expected_revision: None,
            source: expected.clone(),
            policy: DecisionPolicy {
                id: "finite-decision".into(),
                contract,
                actions: vec![DecisionAction::Recommend],
                reviewer: Some("reviewer".into()),
                allow_writer_fallback: true,
                deadline: 4600,
                authority_ref: "original-writer-explicit-consent".into(),
                revoked: false,
            },
            reason: "Finite explicit Mail adoption".into(),
        };
        let request = DecisionMaterializationRequest {
            policy: "finite-decision".into(),
            policy_revision: 1,
            case_id: case.id,
            case_version: case.version,
            source: expected,
        };
        Ok((temp, store, writer, reviewer, policy, request))
    }

    #[tokio::test]
    async fn materialization_refusal_rollback_and_historical_replay_preserve_original_case()
    -> Result<()> {
        let (_temp, store, writer, reviewer, policy, request) = materializer_fixture().await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            materialize_decision_task_tx(&mut tx, "g", &request, 4102).await?,
            DecisionMaterialization::Refused(MaterializationRefusal::PolicyMissing)
        ));
        tx.commit().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_obligations")
                .fetch_one(store.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
                .fetch_one(store.pool())
                .await?,
            0
        );
        store.decision_policy(&writer, policy.clone(), 4102).await?;
        let mut tx = store.pool().begin().await?;
        let rolled = materialize_decision_task_tx(&mut tx, "g", &request, 4103).await?;
        assert!(matches!(rolled, DecisionMaterialization::Materialized(_)));
        tx.rollback().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
                .fetch_one(store.pool())
                .await?,
            0
        );
        assert!(
            store
                .decision_case(&writer, request.case_id)
                .await?
                .decision_task
                .is_none()
        );
        let mut tx = store.pool().begin().await?;
        let receipt = match materialize_decision_task_tx(&mut tx, "g", &request, 4103).await? {
            DecisionMaterialization::Materialized(receipt) => receipt,
            other => anyhow::bail!("unexpected materialization: {other:?}"),
        };
        tx.commit().await?;
        let task = store.task_inspect(&writer, &receipt.task).await?;
        assert_eq!(task.work.writer, "writer");
        assert_eq!(task.work.owner, "reviewer");
        assert_eq!(task.work.deadline, Some(4500));
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT origin FROM task_model_events WHERE task=?")
                .bind(&receipt.task)
                .fetch_one(store.pool())
                .await?,
            "system_materialization"
        );
        assert!(
            !store
                .decision_case(&writer, request.case_id)
                .await?
                .requires_reassessment,
            "initial task creation cannot impersonate a later decision-input change"
        );
        let mut revoke = policy;
        revoke.key = "revoke-source-policy".into();
        revoke.expected_revision = Some(1);
        revoke.policy.revoked = true;
        store.decision_policy(&writer, revoke, 4104).await?;
        let mut tx = store.pool().begin().await?;
        let message = match request.source.source {
            crate::decision_recovery::Obligation::Delivery { message, .. } => message,
            crate::decision_recovery::Obligation::Task { .. } => unreachable!(),
        };
        Store::resolve_tx(
            &mut tx,
            &reviewer,
            message,
            "original source settled",
            None,
            4104,
        )
        .await?;
        tx.commit().await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            materialize_decision_task_tx(&mut tx, "g", &request, 5000).await?,
            DecisionMaterialization::Replayed(_)
        ));
        let mut changed = request.clone();
        changed.policy_revision = 2;
        assert!(
            materialize_decision_task_tx(&mut tx, "g", &changed, 5000)
                .await
                .is_err()
        );
        tx.rollback().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_materializations")
                .fetch_one(store.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
                .fetch_one(store.pool())
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_materializers_share_one_funded_task_and_revocation_refuses_new_creation()
    -> Result<()> {
        let (_temp, store, writer, _reviewer, policy, request) = materializer_fixture().await?;
        store.decision_policy(&writer, policy.clone(), 4102).await?;
        let apply = || async {
            let mut tx = store.pool().begin().await?;
            let result = materialize_decision_task_tx(&mut tx, "g", &request, 4103).await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(result)
        };
        let (one, two) = tokio::join!(apply(), apply());
        let outcomes = [one?, two?];
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| matches!(r, DecisionMaterialization::Materialized(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| matches!(r, DecisionMaterialization::Replayed(_)))
                .count(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_budgets")
                .fetch_one(store.pool())
                .await?,
            1
        );
        let (_other, store, writer, _reviewer, mut policy, request) =
            materializer_fixture().await?;
        policy.policy.revoked = true;
        store.decision_policy(&writer, policy, 4102).await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            materialize_decision_task_tx(&mut tx, "g", &request, 4103).await?,
            DecisionMaterialization::Refused(MaterializationRefusal::PolicyRevoked)
        ));
        tx.commit().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
                .fetch_one(store.pool())
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operator_obligations")
                .fetch_one(store.pool())
                .await?,
            1
        );
        Ok(())
    }

    fn decision(version: i64, key: &str) -> TaskDecision {
        TaskDecision {
            key: key.into(),
            version,
            reason: "Owned integration control".into(),
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

    pub(super) fn request(id: &str) -> TaskCreate {
        serde_json::from_value(serde_json::json!({
            "key":format!("create:{id}"),"reason":"Atomic composition control",
            "expected_parent_versions":{},
            "draft":{
                "work":{"id":id,"owner":"writer","state":"ready","scope":"report",
                        "next_action":"Prepare report","deadline":null,"evidence":[]},
                "contract":{"deliverable":"One report","criteria":[{"id":"report","description":"Report complete"}],
                            "allowed_scope":["report"],"completion":"writer_acceptance",
                            "allow_delegation":true,"allow_input_invalidation":true,
                            "budget":{"max_attempts":2,"max_elapsed_seconds":60,"max_cost":null}},
                "authorization":{"state":"authorized","source":{"kind":"direct","authority_ref":"isolated fixture"},
                                 "approved_scope":["report"],"reason":"Explicit"},
                "requirements":[],"parent":null
            }
        })).expect("closed task fixture")
    }

    #[tokio::test]
    async fn stored_judge_identity_survives_rebind_but_not_revocation_or_changed_inputs()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        for name in ["writer", "worker", "judge"] {
            store.register("g", name, false).await?;
        }
        let actor = store.mailbox("g", "writer").await?;
        let judge = store.mailbox("g", "judge").await?;
        let mut draft = request("source");
        draft.draft.work.owner = "worker".into();
        store.task_create(&actor, draft, 100).await?;
        let inputs = store
            .task_capture_inputs(&actor, "source", 1, Phase::Execute)
            .await?;
        let mut grant = JudgeGrantDecision {
            key: "grant".into(),
            task_version: 1,
            expected_revision: None,
            reason: "Finite milestone judgment".into(),
            grant: JudgeGrant {
                id: "review".into(),
                decider: "judge".into(),
                milestone: "report".into(),
                criterion_ids: vec!["report".into()],
                authority_ref: "fixture".into(),
                revoked: false,
            },
        };
        store
            .task_judge_grant(&actor, "source", grant.clone(), 101)
            .await?;
        let grant_ref = JudgeGrantRef {
            id: "review".into(),
            revision: 1,
        };
        // Only runtime binding changes; the original local mailbox identity and
        // execution-owner input generation remain unchanged.
        sqlx::query("UPDATE mailboxes SET binding_version=binding_version+1 WHERE group_name='g' AND name IN ('writer','judge')")
            .execute(store.pool()).await?;
        let mut tx = store.pool().begin().await?;
        assert_eq!(
            validate_stored_progress_judge_tx(
                &mut tx,
                actor.id,
                &actor.name,
                &inputs,
                "report",
                None
            )
            .await?,
            ["report"]
        );
        assert_eq!(
            validate_stored_progress_judge_tx(
                &mut tx,
                judge.id,
                &judge.name,
                &inputs,
                "report",
                Some(&grant_ref)
            )
            .await?,
            ["report"]
        );
        assert!(
            validate_stored_progress_judge_tx(
                &mut tx,
                judge.id,
                &actor.name,
                &inputs,
                "report",
                None
            )
            .await
            .is_err()
        );
        assert!(
            validate_progress_judge_tx(&mut tx, &actor, &inputs, "report", None)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let current_actor = store.mailbox("g", "writer").await?;
        grant.key = "revoke".into();
        grant.expected_revision = Some(1);
        grant.grant.revoked = true;
        store
            .task_judge_grant(&current_actor, "source", grant, 102)
            .await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            validate_stored_progress_judge_tx(
                &mut tx,
                judge.id,
                &judge.name,
                &inputs,
                "report",
                Some(&grant_ref)
            )
            .await
            .is_err()
        );
        assert!(
            validate_stored_progress_judge_tx(
                &mut tx,
                actor.id,
                &actor.name,
                &inputs,
                "report",
                None
            )
            .await
            .is_ok()
        );
        tx.rollback().await?;
        let mut correction = decision(1, "new-inputs");
        correction.work_patch.next_action = Some("Prepare corrected report".into());
        store
            .task_decide(&current_actor, "source", correction, 103)
            .await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            validate_stored_progress_judge_tx(
                &mut tx,
                actor.id,
                &actor.name,
                &inputs,
                "report",
                None
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_judgment_inputs")
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_source_inventory_rejects_an_unmapped_wait_without_cached_edges() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        let actor = store.mailbox("g", "writer").await?;
        store.task_create(&actor, request("source"), 100).await?;
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM task_blocking_edges WHERE owner='scheduler'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(count, 0);
        sqlx::query(
            "UPDATE execution_tasks SET continuation='{}' WHERE group_name='g' AND task='source'",
        )
        .execute(store.pool())
        .await?;
        let error = store
            .task_model_readiness(&actor, "source", Phase::Execute)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "scheduler_typed_continuation_unavailable",
            "actual readiness rejection: {error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn real_execution_case_preserves_responsibility_and_holds_after_source_correction()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        let actor = store.mailbox("g", "writer").await?;
        let initial = store.task_create(&actor, request("source"), 100).await?;
        let inputs = store
            .task_capture_inputs(&actor, "source", 1, Phase::Execute)
            .await?;
        // The actual scheduler's unavailable runtime produces a real negative
        // cause. No positive runtime or materialization witness is invented.
        store.execution_reconcile("g", 101).await?;
        let mut tx = store.pool().begin().await?;
        let causes = crate::execution::list_execution_causes_tx(&mut tx, "g", "", 100).await?;
        let cause = causes
            .iter()
            .find(|cause| cause.source_task == "source")
            .context("scheduler cause missing")?;
        let case = crate::decision_recovery::ensure_execution_case_tx(&mut tx, cause, 101).await?;
        tx.commit().await?;
        let mut change = decision(1, "correct-scope");
        let model = initial.model.context("model missing")?;
        let mut contract = model.contract;
        contract.allowed_scope = vec!["corrected report".into()];
        let mut authorization = model.authorization;
        authorization.approved_scope = contract.allowed_scope.clone();
        change.contract = Change::Set(contract);
        change.authorization = Change::Set(authorization);
        let corrected = store.task_decide(&actor, "source", change, 102).await?;
        assert!(
            corrected
                .readiness
                .causes
                .iter()
                .any(|cause| cause.code.starts_with("recovery_reassessment:"))
        );
        let current = store.decision_case(&actor, case.id).await?;
        assert!(current.requires_reassessment);
        assert_eq!(current.original_source, case.original_source);
        assert_eq!(current.operator_obligation, case.operator_obligation);
        assert_eq!(current.original_due, case.original_due);
        assert_eq!(current.hard_due, case.hard_due);
        assert!(current.version > case.version);
        let mut tx = store.pool().begin().await?;
        let graph = load_graph_tx(&mut tx, "g", &["source".into()]).await?;
        for action in [
            TaskAction::Execute,
            TaskAction::AcceptResult,
            TaskAction::PublishArtifact,
            TaskAction::Scope("corrected report".into()),
        ] {
            assert!(!external_action_causes(&graph, "source", &action, false)?.is_empty());
        }
        assert!(
            validate_publication_inputs_tx(&mut tx, &inputs, "corrected report")
                .await
                .is_err()
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn exact_source_guard_retains_candidate_identity_and_held_work_visibility() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let credential = store.register("g", "writer", false).await?;
        let actor = store.authenticate("g", Some(&credential)).await?;
        let initial = store.task_create(&actor, request("source"), 100).await?;
        let mut old = TaskSourceExpectation::try_from(&initial)?;
        let inputs = store
            .task_capture_inputs(&actor, "source", initial.work.version, Phase::Accept)
            .await?;
        store
            .task_candidate(
                &actor,
                "source",
                CandidateRequest {
                    version: initial.work.version,
                    key: "candidate".into(),
                    candidate: CandidateDraft {
                        revision: "artifact:one".into(),
                        summary: "Report".into(),
                        criterion_evidence: vec![CriterionEvidence {
                            criterion_id: "report".into(),
                            references: vec!["isolated-fixture".into()],
                        }],
                        inputs,
                    },
                },
                101,
            )
            .await?;
        let candidate = store.task_inspect(&actor, "source").await?;
        // Merely substituting the latest business version cannot silently
        // select a different immutable candidate for the original decision.
        old.task_version = candidate.work.version;
        let mut tx = store.pool().begin().await?;
        assert!(
            validate_task_source_tx(&mut tx, &old)
                .await
                .unwrap_err()
                .to_string()
                .contains("decision_source_conflict")
        );
        tx.rollback().await?;
        let current = TaskSourceExpectation::try_from(&candidate)?;
        let mut tx = store.pool().begin().await?;
        validate_task_source_tx(&mut tx, &current).await?;
        tx.rollback().await?;

        let held = store
            .task_decide(
                &actor,
                "source",
                TaskDecision {
                    key: "hold".into(),
                    version: candidate.work.version,
                    reason: "Decision remains needed while held".into(),
                    work_patch: WorkPatch {
                        state: Some(TaskState::Blocked),
                        ..WorkPatch::default()
                    },
                    scope: Change::Keep,
                    contract: Change::Keep,
                    authorization: Change::Keep,
                    requirements: Change::Keep,
                    parent: Change::Keep,
                    expected_parent_versions: BTreeMap::new(),
                    clear_invalidation: false,
                    outcome: OutcomeChange::Keep,
                    resolve_message: None,
                },
                102,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        let guarded =
            validate_task_source_tx(&mut tx, &TaskSourceExpectation::try_from(&held)?).await?;
        assert_eq!(guarded.work.state, TaskState::Blocked);
        assert!(!guarded.readiness.causes.is_empty());
        assert_eq!(guarded.execution_hold, "scheduler_admission_required");
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn caller_rollback_removes_create_adopt_decide_and_their_receipts() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        let credential = store.register("g", "writer", false).await?;
        let actor = store.authenticate("g", Some(&credential)).await?;
        let mut tx = store.pool().begin().await?;
        Store::task_create_tx(&mut tx, &actor, request("fresh"), 100).await?;
        tx.rollback().await?;
        assert!(store.task_inspect(&actor, "fresh").await.is_err());
        for table in [
            "work_items",
            "task_models",
            "task_model_events",
            "task_decisions",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(store.pool())
                .await?;
            assert_eq!(count, 0, "outer rollback must remove {table}");
        }

        let draft = request("legacy").draft;
        let legacy = store.work_create(&actor, draft.work, 101).await?;
        let mut tx = store.pool().begin().await?;
        Store::task_adopt_tx(
            &mut tx,
            &actor,
            "legacy",
            TaskAdopt {
                key: "adopt".into(),
                version: legacy.version,
                reason: "Transactional adoption".into(),
                contract: draft.contract,
                authorization: draft.authorization,
                requirements: draft.requirements,
                parent: draft.parent,
                expected_parent_versions: BTreeMap::new(),
            },
            102,
        )
        .await?;
        tx.rollback().await?;
        let untouched = store.task_inspect(&actor, "legacy").await?;
        assert!(untouched.model.is_none());
        assert_eq!(untouched.work.version, legacy.version);

        let created = store.task_create(&actor, request("source"), 103).await?;
        let mut tx = store.pool().begin().await?;
        let cancelled = Store::task_decide_tx(
            &mut tx,
            &actor,
            "source",
            TaskDecision {
                key: "cancel".into(),
                version: created.work.version,
                reason: "Atomic negative source decision".into(),
                work_patch: WorkPatch::default(),
                scope: Change::Keep,
                contract: Change::Keep,
                authorization: Change::Keep,
                requirements: Change::Keep,
                parent: Change::Keep,
                expected_parent_versions: BTreeMap::new(),
                clear_invalidation: false,
                outcome: OutcomeChange::Negative {
                    kind: OutcomeKind::Cancelled,
                    revision: "cancelled:control".into(),
                },
                resolve_message: None,
            },
            104,
        )
        .await?;
        assert_eq!(cancelled.work.state, TaskState::Cancelled);
        tx.rollback().await?;
        let unchanged = store.task_inspect(&actor, "source").await?;
        assert_eq!(unchanged.work.version, created.work.version);
        assert_eq!(unchanged.work.state, TaskState::Ready);
        assert!(unchanged.model.unwrap().current_outcome.is_none());
        let receipts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_decisions WHERE key IN ('adopt','cancel')",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(receipts, 0);
        assert!(
            store
                .task_results(&actor, "source", None)
                .await?
                .items
                .is_empty()
        );
        Ok(())
    }
}
