//! Durable execution accounting. Delivery, business outcomes and runtime proof
//! remain separate authorities. All owner seams compose in one SQLite writer
//! transaction; none performs runtime I/O or commits its caller's transaction.

use crate::{
    store::{Mailbox, Store},
    task_graph::{self, Budget, Contract, InputSnapshot, InputValidity, Phase},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{FromRow, Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet};

type Tx<'a> = Transaction<'a, Sqlite>;
const PAGE: i64 = 100;
const RECHECK: i64 = 30;
const MAX_JSON: usize = 65_536;

/// Finite policy ceiling retained with the original contract. Later progress
/// policy may narrow it; policy edits cannot grant a continuation or budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    /// Closed admitted segments before a strategy decision is required.
    pub closed_segment_limit: u32,
    /// Optional earlier boundary; absent means the original lifetime deadline.
    pub no_progress_seconds: Option<u64>,
}
impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            closed_segment_limit: 2,
            no_progress_seconds: None,
        }
    }
}

/// Exact attempt identity, also used by runtime FK rows and immutable receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Correlation {
    /// Home-local group.
    pub group: String,
    /// Contracted task ID.
    pub task: String,
    /// Globally unique stable attempt ID.
    pub attempt: String,
    /// Monotonically increasing task attempt fence.
    pub fence: i64,
    /// Immutable transport idempotency key.
    pub dispatch_key: String,
}

// These types are deliberately crate-private. Only the runtime owner implements
// this contract, by reading authenticated persisted facts in the caller's tx.
// A worker's report, serialized boolean, or environment switch is not a gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeTarget {
    pub identity: String,
    pub concurrency_key: String,
    pub generation: i64,
    pub profile: String,
    pub durable_dedupe: bool,
    pub cost_caps: BTreeMap<String, i64>,
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum CurrentUse {
    Dispatch,
    Admit,
    Report,
    Publish,
}

/// Runtime authenticates the original correlation independently of the current
/// mailbox binding. The sealed effect set covers every intent, including none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClosedRuntime {
    pub receipt: String,
    pub effect_set: String,
    pub admitted: bool,
    pub costs: BTreeMap<String, i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RuntimeObservation {
    pub receipt: String,
    pub sequence: i64,
    pub observed_at: i64,
    pub status: ObservationStatus,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum ObservationStatus {
    Active,
    Unknown,
    ExitObserved,
}

#[allow(async_fn_in_trait)]
pub(crate) trait RuntimeGate {
    async fn target(
        &self,
        tx: &mut Tx<'_>,
        group: &str,
        task: &str,
        owner: &str,
        binding: i64,
        now: i64,
    ) -> Result<Option<RuntimeTarget>>;
    async fn current(
        &self,
        tx: &mut Tx<'_>,
        correlation: &Correlation,
        target: &RuntimeTarget,
        purpose: CurrentUse,
        now: i64,
    ) -> Result<bool>;
    /// Return Some only after authenticated quiescence, late-start tombstone and
    /// complete sealed effect reconciliation. Unknown effect cost stays absent.
    async fn closed(
        &self,
        tx: &mut Tx<'_>,
        correlation: &Correlation,
        target: &RuntimeTarget,
        receipt: &str,
    ) -> Result<Option<ClosedRuntime>>;
    async fn observation(
        &self,
        tx: &mut Tx<'_>,
        correlation: &Correlation,
        target: &RuntimeTarget,
        receipt: &str,
    ) -> Result<Option<RuntimeObservation>>;
}
struct UnavailableRuntime;
impl RuntimeGate for UnavailableRuntime {
    async fn target(
        &self,
        _: &mut Tx<'_>,
        _: &str,
        _: &str,
        _: &str,
        _: i64,
        _: i64,
    ) -> Result<Option<RuntimeTarget>> {
        Ok(None)
    }
    async fn current(
        &self,
        _: &mut Tx<'_>,
        _: &Correlation,
        _: &RuntimeTarget,
        _: CurrentUse,
        _: i64,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn closed(
        &self,
        _: &mut Tx<'_>,
        _: &Correlation,
        _: &RuntimeTarget,
        _: &str,
    ) -> Result<Option<ClosedRuntime>> {
        Ok(None)
    }
    async fn observation(
        &self,
        _: &mut Tx<'_>,
        _: &Correlation,
        _: &RuntimeTarget,
        _: &str,
    ) -> Result<Option<RuntimeObservation>> {
        Ok(None)
    }
}

/// A durable responsible hold. Time passing escalates it, never grants authority.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ExecutionCause {
    /// Stable causal episode identity.
    pub id: String,
    /// Optimistic concurrency revision.
    pub revision: i64,
    /// Machine-readable predicate.
    pub code: String,
    /// Human-readable current explanation.
    pub detail: String,
    /// Writer or declared decision owner.
    pub responsible: String,
    /// Next finite review time.
    pub review_at: i64,
    /// Original escalation boundary, never refreshed by scanning.
    pub hard_due: i64,
    /// Whether the boundary has been reached.
    pub escalated: bool,
}

/// Stable source reference independent of task versions and Mail occurrences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionCauseRef {
    pub group: String,
    pub source_task: String,
    pub cause_generation: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionSourceGuard {
    pub cause_ref: ExecutionCauseRef,
    pub expected_cause_revision: i64,
    pub task_version: i64,
    pub input_epoch: i64,
    pub attempt: Option<Correlation>,
    pub candidate_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ExecutionSourceKind {
    Work,
    /// Recovery resolves this persisted original source/episode to its own case.
    /// A materialized decision is never treated as ordinary recursive work.
    MaterializedDecision {
        source: String,
        episode: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionCauseSnapshot {
    pub guard: ExecutionSourceGuard,
    pub source_kind: ExecutionSourceKind,
    pub cause: ExecutionCause,
    pub handoff_case: Option<String>,
    pub budgets: Vec<Account>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ExecutionCauseState {
    Current(Box<ExecutionCauseSnapshot>),
    Superseded(Value),
    Missing,
}

/// Complete source truth, including terminal business tasks and absent owners.
pub(crate) async fn inspect_execution_cause_tx(
    tx: &mut Tx<'_>,
    reference: &ExecutionCauseRef,
) -> Result<ExecutionCauseState> {
    reserve(tx, &reference.group).await?;
    let row: Option<(bool,Option<String>,Option<String>)> = sqlx::query_as("SELECT settled,disposition,case_ref FROM execution_causes WHERE id=? AND group_name=? AND task=?")
        .bind(&reference.cause_generation).bind(&reference.group).bind(&reference.source_task).fetch_optional(&mut **tx).await?;
    let Some((settled, disposition, handoff_case)) = row else {
        return Ok(ExecutionCauseState::Missing);
    };
    if settled {
        return Ok(ExecutionCauseState::Superseded(serde_json::from_str(
            &disposition.context("cause_disposition_missing")?,
        )?));
    }
    let cause: ExecutionCause = sqlx::query_as("SELECT id,revision,code,detail,responsible,review_at,hard_due,escalated FROM execution_causes WHERE id=?")
        .bind(&reference.cause_generation).fetch_one(&mut **tx).await?;
    let m = model(tx, &reference.group, &reference.source_task).await?;
    let (input_epoch, candidate_id): (i64, Option<String>) = sqlx::query_as(
        "SELECT input_epoch,current_candidate FROM task_models WHERE group_name=? AND task=?",
    )
    .bind(&reference.group)
    .bind(&reference.source_task)
    .fetch_one(&mut **tx)
    .await?;
    let held: Option<Attempt> = sqlx::query_as(
        "SELECT * FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
    )
    .bind(&reference.group)
    .bind(&reference.source_task)
    .fetch_optional(&mut **tx)
    .await?;
    let materialization: Option<(String, String)> = sqlx::query_as(
        "SELECT source,episode FROM task_materializations WHERE group_name=? AND decision_task=?",
    )
    .bind(&reference.group)
    .bind(&reference.source_task)
    .fetch_optional(&mut **tx)
    .await?;
    let source_kind = materialization.map_or(ExecutionSourceKind::Work, |(source, episode)| {
        ExecutionSourceKind::MaterializedDecision { source, episode }
    });
    let mut budgets = Vec::new();
    for id in ancestors(tx, &reference.group, &reference.source_task).await? {
        budgets.push(account(tx, &reference.group, &id).await?);
    }
    let guard = ExecutionSourceGuard {
        cause_ref: reference.clone(),
        expected_cause_revision: cause.revision,
        task_version: m.version,
        input_epoch,
        attempt: held.map(|a| a.correlation()),
        candidate_id,
    };
    Ok(ExecutionCauseState::Current(Box::new(
        ExecutionCauseSnapshot {
            guard,
            source_kind,
            cause,
            handoff_case,
            budgets,
        },
    )))
}

/// Bounded enumeration never filters by task state, owner liveness or case link.
/// Recovery must re-read each guard in its own disposition transaction.
pub(crate) async fn list_execution_causes_tx(
    tx: &mut Tx<'_>,
    group: &str,
    after: &str,
    limit: u32,
) -> Result<Vec<ExecutionCauseRef>> {
    reserve(tx, group).await?;
    ensure!(limit > 0 && limit <= 100, "invalid_execution_cause_page");
    let rows: Vec<(String,String)> = sqlx::query_as("SELECT task,id FROM execution_causes WHERE group_name=? AND settled=0 AND id>? ORDER BY id LIMIT ?")
        .bind(group).bind(after).bind(i64::from(limit)).fetch_all(&mut **tx).await?;
    Ok(rows
        .into_iter()
        .map(|(source_task, cause_generation)| ExecutionCauseRef {
            group: group.into(),
            source_task,
            cause_generation,
        })
        .collect())
}

/// Durable case-link evidence, never source disposition or execution authority.
/// Exact historical replay returns this receipt without reauthorizing work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionCaseAck {
    pub case_id: i64,
    pub case_version: i64,
    pub source_guard: ExecutionSourceGuard,
    pub ledger_event: i64,
}

/// Authenticate the original immutable case link without minting current proof.
/// Later case/task versions do not rewrite the original request or receipt.
/// A partial link is corruption, never permission to manufacture a replacement.
pub(crate) async fn execution_case_ack_tx(
    tx: &mut Tx<'_>,
    reference: &ExecutionCauseRef,
) -> Result<Option<ExecutionCaseAck>> {
    reserve(tx, &reference.group).await?;
    let (link,): (Option<String>,) = sqlx::query_as(
        "SELECT case_ref FROM execution_causes WHERE id=? AND group_name=? AND task=?",
    )
    .bind(&reference.cause_generation)
    .bind(&reference.group)
    .bind(&reference.source_task)
    .fetch_one(&mut **tx)
    .await
    .context("execution_case_ack_source_missing")?;
    let producer = format!("execution-case-ack:{}", reference.group);
    let stored: Option<(String, String)> = sqlx::query_as(
        "SELECT canonical,result FROM execution_receipts WHERE producer=? AND key=?",
    )
    .bind(&producer)
    .bind(&reference.cause_generation)
    .fetch_optional(&mut **tx)
    .await?;
    let events:Vec<(i64,String,Option<String>)>=sqlx::query_as("SELECT id,payload,attempt FROM execution_events WHERE group_name=? AND task=? AND kind='decision_case_linked' AND json_extract(payload,'$.source_guard.cause_ref.cause_generation')=? ORDER BY id LIMIT 2")
        .bind(&reference.group).bind(&reference.source_task).bind(&reference.cause_generation).fetch_all(&mut **tx).await?;
    let Some((request, result)) = stored else {
        ensure!(
            link.is_none() && events.is_empty(),
            "execution_case_ack_partial_link"
        );
        return Ok(None);
    };
    let ack: ExecutionCaseAck = serde_json::from_str(&result)?;
    ensure!(
        ack.case_id > 0
            && ack.case_version > 0
            && ack.ledger_event > 0
            && ack.source_guard.cause_ref == *reference
            && ack.source_guard.expected_cause_revision > 0
            && ack.source_guard.task_version > 0
            && ack.source_guard.input_epoch > 0,
        "execution_case_ack_identity_conflict"
    );
    ensure!(
        link.as_deref() == Some(ack.case_id.to_string().as_str()),
        "execution_case_ack_partial_link"
    );
    let expected = canonical(
        &json!({"case_id":ack.case_id,"case_version":ack.case_version,"source_guard":ack.source_guard}),
    )?;
    ensure!(
        request == expected && result == canonical(&ack)?,
        "execution_case_ack_receipt_conflict"
    );
    ensure!(
        events.len() == 1,
        "execution_case_ack_event_missing_or_ambiguous"
    );
    let (event_id, payload, event_attempt) = &events[0];
    ensure!(
        *event_id == ack.ledger_event
            && *payload == expected
            && event_attempt.as_deref()
                == ack
                    .source_guard
                    .attempt
                    .as_ref()
                    .map(|c| c.attempt.as_str()),
        "execution_case_ack_event_conflict"
    );
    if let Some(original) = &ack.source_guard.attempt {
        ensure!(
            original.group == reference.group && original.task == reference.source_task,
            "execution_case_ack_attempt_scope"
        );
        attempt(tx, original).await?;
    }
    Ok(Some(ack))
}

/// Writer-requested bounded exception to the progress guard. This never adds
/// task/ancestor attempts, cost, elapsed lifetime, scope or runtime authority.
///
/// The model's authenticated decision operation validates policy and case
/// authority before applying this request. Constructing or deserializing it
/// does not grant an allowance or authorize execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueStrategy {
    /// Stable retry key for this exact source continuation request.
    pub key: String,
    /// Audited reason for continuing the source task's strategy.
    pub reason: String,
    /// Expected execution revision of the original source task.
    pub execution_revision: i64,
    /// Additional closed, admitted segments permitted by the finite allowance.
    /// Existing task and ancestor budgets still limit execution.
    pub additional_segments: u32,
    /// Exclusive allowance expiry in Unix seconds, within existing lifetime limits.
    /// An exact historical retry never extends this timestamp.
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyAllowance {
    // Missing on historical receipts. Never reconstruct authority from current evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    progress_basis: Option<ProgressEvidence>,
    source_guard: ExecutionSourceGuard,
    source_kind: ExecutionSourceKind,
    case_id: i64,
    case_version: i64,
    actor_id: i64,
    inputs: InputSnapshot,
    after_fence: i64,
    anchor_report: Option<i64>,
    anchor_at: i64,
    additional_segments: u32,
    expires_at: i64,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyEnvelope {
    event: i64,
    allowance: StrategyAllowance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppliedRecord {
    envelope: StrategyEnvelope,
    cause_revision_after: i64,
}

/// Only execution can mint this non-deserializable witness after a real source
/// mutation. Recovery consumes it in the same transaction for its case/outcome
/// audit; it never accepts a caller-supplied success flag.
#[derive(Debug)]
pub(crate) struct AppliedDisposition {
    record: AppliedRecord,
}
impl AppliedDisposition {
    pub(crate) fn group(&self) -> &str {
        &self.record.envelope.allowance.source_guard.cause_ref.group
    }
    pub(crate) fn case_id(&self) -> i64 {
        self.record.envelope.allowance.case_id
    }
    pub(crate) fn case_version(&self) -> i64 {
        self.record.envelope.allowance.case_version
    }
    pub(crate) fn actor_id(&self) -> i64 {
        self.record.envelope.allowance.actor_id
    }
    pub(crate) fn source_before(&self) -> &ExecutionSourceGuard {
        &self.record.envelope.allowance.source_guard
    }
    pub(crate) fn source_kind(&self) -> &ExecutionSourceKind {
        &self.record.envelope.allowance.source_kind
    }
    pub(crate) fn execution_event(&self) -> i64 {
        self.record.envelope.event
    }
    pub(crate) fn cause_revision_after(&self) -> i64 {
        self.record.cause_revision_after
    }
    pub(crate) fn audit(&self) -> Result<Value> {
        Ok(serde_json::to_value(&self.record)?)
    }
}

async fn validate_strategy_envelope_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    envelope: &StrategyEnvelope,
) -> Result<()> {
    let allowance = &envelope.allowance;
    ensure!(
        allowance.source_guard.cause_ref.group == group
            && allowance.source_guard.cause_ref.source_task == task
            && allowance.inputs.group == group
            && allowance.inputs.task == task
            && (1..=10_000).contains(&allowance.additional_segments)
            && allowance.anchor_at >= 0
            && allowance.expires_at > allowance.anchor_at
            && allowance.after_fence >= 0,
        "invalid_strategy_envelope"
    );
    let payload: String = sqlx::query_scalar("SELECT payload FROM execution_events WHERE id=? AND group_name=? AND task=? AND kind='strategy_continuation'")
        .bind(envelope.event).bind(group).bind(task).fetch_one(&mut **tx).await.context("strategy_envelope_receipt_missing")?;
    // event() writes a Value. Use that same representation here so struct
    // field order cannot disagree with the immutable event's object key order.
    ensure!(
        payload == canonical(&serde_json::to_value(allowance)?)?,
        "strategy_envelope_receipt_conflict"
    );
    Ok(())
}

async fn continuation_allows_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    facts: &ExecutionProgressFacts,
    progress: &ProgressEvidence,
    now: i64,
) -> Result<bool> {
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT continuation FROM execution_tasks WHERE group_name=? AND task=?",
    )
    .bind(group)
    .bind(task)
    .fetch_one(&mut **tx)
    .await?;
    let Some(raw) = raw else {
        return Ok(false);
    };
    let envelope: StrategyEnvelope = serde_json::from_str(&raw)?;
    validate_strategy_envelope_tx(tx, group, task, &envelope).await?;
    let allowance = &envelope.allowance;
    if !allowance
        .progress_basis
        .as_ref()
        .is_some_and(|original| original.basis == progress.basis)
        || now >= allowance.expires_at
        || facts.clock_hold
        || facts.anchor_event != allowance.anchor_report
        || facts.anchor_at != Some(allowance.anchor_at)
        || task_graph::validate_inputs_tx(tx, &allowance.inputs).await? != InputValidity::Current
    {
        return Ok(false);
    }
    let used: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT id FROM execution_attempts WHERE group_name=? AND task=? AND fence>? AND admitted=1 AND state='closed' LIMIT ?)")
        .bind(group).bind(task).bind(allowance.after_fence).bind(i64::from(allowance.additional_segments)).fetch_one(&mut **tx).await?;
    Ok(used < i64::from(allowance.additional_segments))
}

/// Revalidate the immutable after-state before recovery finalizes the original
/// case. Recovery still owns its exact case/version CAS, graph refresh and the
/// ordinary decision-task outcome. This historical witness grants no new work.
pub(crate) async fn validate_applied_disposition_tx(
    tx: &mut Tx<'_>,
    applied: &AppliedDisposition,
) -> Result<()> {
    let reference = &applied.source_before().cause_ref;
    reserve(tx, &reference.group).await?;
    validate_strategy_envelope_tx(
        tx,
        &reference.group,
        &reference.source_task,
        &applied.record.envelope,
    )
    .await?;
    let (settled,revision,disposition): (bool,i64,Option<String>) = sqlx::query_as("SELECT settled,revision,disposition FROM execution_causes WHERE id=? AND group_name=? AND task=?")
        .bind(&reference.cause_generation).bind(&reference.group).bind(&reference.source_task).fetch_one(&mut **tx).await?;
    let expected = canonical(&applied.record)?;
    ensure!(
        settled
            && revision == applied.cause_revision_after()
            && disposition.as_deref() == Some(expected.as_str()),
        "applied_execution_disposition_conflict"
    );
    Ok(())
}

/// Recovery composes this mutation and its case/decision disposition atomically.
/// Held results retain responsible observations; storage/contract errors roll
/// back the caller's complete transaction. No original anchor or budget resets.
pub(crate) async fn apply_continue_strategy_tx(
    tx: &mut Tx<'_>,
    proof: &crate::decision_recovery::ValidatedCaseAuthority,
    request: &ContinueStrategy,
    now: i64,
) -> Result<Checked<AppliedDisposition>> {
    let actor = proof.actor();
    Store::lock_actor(tx, actor).await?;
    let before = proof.case().guard();
    let reference = &before.cause_ref;
    reserve(tx, &reference.group).await?;
    ensure!(
        !request.reason.trim().is_empty()
            && request.reason.len() <= 4096
            && (1..=10_000).contains(&request.additional_segments)
            && request.expires_at >= 0,
        "finite_strategy_required"
    );
    let producer = format!("continue-strategy:{}", actor.id);
    let bytes = canonical(
        &json!({"case":proof.case().case_id(),"case_version":proof.case().case_version(),"source_guard":before,"request":request}),
    )?;
    if let Some(record) = prior::<AppliedRecord>(tx, &producer, &request.key, &bytes).await? {
        let applied = AppliedDisposition { record };
        validate_applied_disposition_tx(tx, &applied).await?;
        return Ok(Checked::Ready(applied));
    }
    // Expiry governs a new application, not an authenticated historical retry.
    // Returning the old witness does not extend the persisted allowance.
    ensure!(request.expires_at > now, "strategy_already_expired");
    let current = crate::decision_recovery::validate_case_authority_tx(
        tx,
        actor,
        proof.case().case_id(),
        proof.case().case_version(),
        before,
    )
    .await?;
    crate::decision_recovery::guard_case_completion_tx(tx, &current).await?;
    guard_success_tx(tx, &reference.group, &reference.source_task).await?;
    let ExecutionCauseState::Current(cause_before) =
        inspect_execution_cause_tx(tx, reference).await?
    else {
        anyhow::bail!("strategy_cause_not_current")
    };
    ensure!(
        cause_before.guard == *before
            && matches!(
                cause_before.cause.code.as_str(),
                "strategy_decision_required" | "no_progress_elapsed"
            ),
        "strategy_cause_guard_conflict"
    );
    let e = execution(tx, &reference.group, &reference.source_task).await?;
    ensure!(
        e.revision == request.execution_revision,
        "execution_revision_conflict"
    );
    let holds = readiness(tx, &reference.group, &reference.source_task, now, true).await?;
    let other: Vec<String> = holds
        .iter()
        .filter(|code| {
            !matches!(
                code.as_str(),
                "strategy_decision_required" | "no_progress_elapsed"
            )
        })
        .cloned()
        .collect();
    if !other.is_empty() {
        return Ok(Checked::Held(other));
    }
    ensure!(!holds.is_empty(), "strategy_boundary_not_crossed");
    let policy: ExecutionPolicy = serde_json::from_str(
        &execution(tx, &reference.group, &reference.source_task)
            .await?
            .policy,
    )?;
    let (_, boundary, facts) =
        current_progress_tx(tx, &reference.group, &reference.source_task, &policy, now).await?;
    let progress_basis = Some(ProgressEvidence::from_boundary(&boundary)?);
    ensure!(
        !facts.clock_hold && facts.lifecycle_ready,
        "strategy_clock_or_lifecycle_hold"
    );
    ensure!(
        request.expires_at
            <= effective_deadline(tx, &reference.group, &reference.source_task).await?,
        "strategy_after_lifetime"
    );
    for account in &facts.budgets {
        let remaining = account
            .max_attempts
            .checked_sub(account.attempts_spent)
            .and_then(|n| n.checked_sub(account.attempts_reserved))
            .context("budget_overflow")?;
        ensure!(
            i64::from(request.additional_segments) <= remaining,
            "strategy_exceeds_remaining_attempts"
        );
    }
    let inputs =
        task_graph::capture_inputs_tx(tx, &reference.group, &reference.source_task, Phase::Execute)
            .await?;
    let allowance = StrategyAllowance {
        progress_basis,
        source_guard: before.clone(),
        source_kind: cause_before.source_kind.clone(),
        case_id: current.case().case_id(),
        case_version: current.case().case_version(),
        actor_id: actor.id,
        inputs,
        after_fence: e.fence,
        anchor_report: facts.anchor_event,
        anchor_at: facts.anchor_at.context("strategy_anchor_missing")?,
        additional_segments: request.additional_segments,
        expires_at: request.expires_at,
        reason: request.reason.clone(),
    };
    let id = event(
        tx,
        &reference.group,
        &reference.source_task,
        None,
        "strategy_continuation",
        serde_json::to_value(&allowance)?,
        now,
    )
    .await?;
    let envelope = StrategyEnvelope {
        event: id,
        allowance,
    };
    let changed = sqlx::query("UPDATE execution_tasks SET continuation=?,revision=revision+1,due_at=? WHERE group_name=? AND task=? AND revision=?")
        .bind(canonical(&envelope)?).bind(now).bind(&reference.group).bind(&reference.source_task).bind(e.revision).execute(&mut **tx).await?;
    ensure!(changed.rows_affected() == 1, "strategy_execution_conflict");
    let record = AppliedRecord {
        envelope,
        cause_revision_after: before
            .expected_cause_revision
            .checked_add(1)
            .context("cause_revision_overflow")?,
    };
    let changed = sqlx::query("UPDATE execution_causes SET settled=1,revision=revision+1,disposition=? WHERE id=? AND group_name=? AND task=? AND revision=? AND settled=0")
        .bind(canonical(&record)?).bind(&reference.cause_generation).bind(&reference.group).bind(&reference.source_task).bind(before.expected_cause_revision).execute(&mut **tx).await?;
    ensure!(changed.rows_affected() == 1, "strategy_cause_conflict");
    receipt(tx, &producer, &request.key, &bytes, &record).await?;
    Ok(Checked::Ready(AppliedDisposition { record }))
}

/// Compose only with the actual recovery owner module/schema24. Recovery's
/// proof is private and non-deserializable; still revalidate it in this writer
/// transaction before a new link. This metadata link does not change the cause
/// predicate/revision, settle its episode, or release an execution slot.
pub(crate) async fn ack_execution_decision_tx(
    tx: &mut Tx<'_>,
    proof: &crate::decision_recovery::ValidatedExecutionCase,
    now: i64,
) -> Result<ExecutionCaseAck> {
    let group = proof.group();
    reserve(tx, group).await?;
    ensure!(now >= 0, "invalid_execution_time");
    let expected = proof.guard();
    let producer = format!("execution-case-ack:{group}");
    let key = &expected.cause_ref.cause_generation;
    let bytes = canonical(
        &json!({"case_id":proof.case_id(),"case_version":proof.case_version(),"source_guard":expected}),
    )?;
    if let Some(original) = prior(tx, &producer, key, &bytes).await? {
        return Ok(original);
    }
    let current = crate::decision_recovery::validate_execution_case_tx(
        tx,
        group,
        proof.case_id(),
        proof.case_version(),
        expected,
    )
    .await?;
    ensure!(current.guard() == expected, "execution_case_guard_conflict");
    let case_ref = current.case_id().to_string();
    let changed = sqlx::query("UPDATE execution_causes SET case_ref=? WHERE id=? AND group_name=? AND task=? AND revision=? AND settled=0 AND (case_ref IS NULL OR case_ref=?)")
        .bind(&case_ref).bind(&expected.cause_ref.cause_generation).bind(group).bind(&expected.cause_ref.source_task)
        .bind(expected.expected_cause_revision).bind(&case_ref).execute(&mut **tx).await?;
    ensure!(changed.rows_affected() == 1, "execution_case_ack_conflict");
    let ledger_event = event(tx, group, &expected.cause_ref.source_task,
        expected.attempt.as_ref().map(|attempt| attempt.attempt.as_str()), "decision_case_linked",
        json!({"case_id":current.case_id(),"case_version":current.case_version(),"source_guard":expected}), now).await?;
    let result = ExecutionCaseAck {
        case_id: current.case_id(),
        case_version: current.case_version(),
        source_guard: expected.clone(),
        ledger_event,
    };
    receipt(tx, &producer, key, &bytes, &result).await?;
    Ok(result)
}

/// Source rows validated for the model owner's complete blocking projection.
#[derive(Debug, Clone)]
pub(crate) struct SchedulerBlockingSource {
    pub source: String,
    pub edges: Vec<task_graph::BlockingEdge>,
}

/// Current scheduling supports time-based due actions and a finite strategy
/// allowance backed by its actual immutable ledger receipt. No task/decision
/// wait may be inferred from an opaque continuation blob. Future typed waits
/// must add their source validation here before any owner can persist them.
/// This enumerates actual source rows, so a nonempty unsupported source fails
/// even when its projected edge rows were lost or never created.
pub(crate) async fn scheduler_blocking_sources_tx(
    tx: &mut Tx<'_>,
    group: &str,
) -> Result<Vec<SchedulerBlockingSource>> {
    reserve(tx, group).await?;
    let rows: Vec<(String,String,Option<String>)> = sqlx::query_as("SELECT task,policy,continuation FROM execution_tasks WHERE group_name=? ORDER BY task LIMIT 10001")
        .bind(group).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 10_000, "scheduler_projection_inventory_limit");
    let mut sources = Vec::with_capacity(rows.len());
    for (task, policy, continuation) in rows {
        let policy: ExecutionPolicy = serde_json::from_str(&policy)?;
        ensure!(
            policy.closed_segment_limit > 0 && policy.no_progress_seconds != Some(0),
            "invalid_execution_policy_source"
        );
        if let Some(raw) = continuation {
            let envelope: StrategyEnvelope =
                serde_json::from_str(&raw).context("scheduler_typed_continuation_unavailable")?;
            validate_strategy_envelope_tx(tx, group, &task, &envelope).await?;
        }
        sources.push(SchedulerBlockingSource {
            source: format!("continuation:{task}"),
            edges: Vec::new(),
        });
    }
    Ok(sources)
}

/// Inclusive account for the task or one of its ancestors.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Account {
    /// Charged task identity.
    pub task: String,
    /// Lifetime attempt ceiling.
    pub max_attempts: i64,
    /// Lifetime elapsed duration.
    pub elapsed_seconds: i64,
    /// Optional exact cost ceiling.
    pub cost_limit: Option<i64>,
    /// Exact integer unit.
    pub cost_unit: Option<String>,
    /// Settled admitted/exposed attempts.
    pub attempts_spent: i64,
    /// Attempts whose execution is not yet safely closed.
    pub attempts_reserved: i64,
    /// Known settled cost.
    pub cost_spent: i64,
    /// Reserved cost, including unknown cost after closure.
    pub cost_reserved: i64,
    /// Closed reservations with unknown final cost.
    pub unknown_cost: i64,
    /// First business eligibility, never reset by runtime discovery or reopening.
    pub anchor: Option<i64>,
    /// Absolute lifetime deadline.
    pub deadline: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
struct Attempt {
    id: String,
    group_name: String,
    task: String,
    fence: i64,
    owner: String,
    owner_binding: i64,
    inputs: String,
    runtime: String,
    runtime_key: String,
    dispatch_key: String,
    state: String,
    holds_slot: bool,
    admitted: bool,
    created: i64,
    observed: i64,
    reconcile_at: i64,
}
impl Attempt {
    fn correlation(&self) -> Correlation {
        Correlation {
            group: self.group_name.clone(),
            task: self.task.clone(),
            attempt: self.id.clone(),
            fence: self.fence,
            dispatch_key: self.dispatch_key.clone(),
        }
    }
}
#[derive(FromRow)]
struct ModelRow {
    owner: String,
    writer: String,
    state: String,
    version: i64,
    deadline: Option<i64>,
    contract: String,
    parent: Option<String>,
}
#[derive(FromRow)]
struct ExecutionRow {
    revision: i64,
    fence: i64,
    policy: String,
    lifecycle_ready: bool,
    clock_ack: i64,
    due_at: i64,
    hard_due: i64,
}

/// Readable scheduler projection. It cannot be used as an admission proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionView {
    /// Contract ID.
    pub task: String,
    /// Independent execution revision, absent for untracked legacy work.
    pub revision: Option<i64>,
    /// Original business state, including terminal tasks with cleanup.
    pub business_state: String,
    /// Exact currently held attempt, if any.
    pub attempt: Option<Correlation>,
    /// Durable execution state of that attempt.
    pub attempt_state: Option<String>,
    /// All current responsible holds.
    pub causes: Vec<ExecutionCause>,
    /// Inclusive task/ancestor usage.
    pub budgets: Vec<Account>,
    /// Next scheduled examination; it is not permission to run.
    pub due_at: Option<i64>,
    /// Whether creation/eligibility was composed by the model owner.
    pub lifecycle_ready: bool,
}

fn canonical<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_string(value)?;
    ensure!(value.len() <= MAX_JSON, "execution_payload_too_large");
    Ok(value)
}
fn finite(now: i64, seconds: i64) -> Result<i64> {
    ensure!(now >= 0 && seconds > 0, "invalid_execution_time");
    now.checked_add(seconds).context("execution_time_overflow")
}
fn terminal(state: &str) -> bool {
    matches!(state, "done" | "accepted" | "cancelled")
}
async fn reserve(tx: &mut Tx<'_>, group: &str) -> Result<()> {
    let changed = sqlx::query(
        "UPDATE groups SET paused=paused WHERE name=? AND home_machine=(SELECT id FROM node)",
    )
    .bind(group)
    .execute(&mut **tx)
    .await?;
    ensure!(
        changed.rows_affected() == 1,
        "execution_requires_home_group"
    );
    Ok(())
}
async fn model(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<ModelRow> {
    sqlx::query_as("SELECT w.owner,w.writer,w.state,w.version,w.deadline,m.contract,m.parent FROM work_items w JOIN task_models m ON m.group_name=w.group_name AND m.task=w.id WHERE w.group_name=? AND w.id=?")
        .bind(group).bind(task).fetch_one(&mut **tx).await.context("contracted_task_missing")
}
async fn execution(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<ExecutionRow> {
    sqlx::query_as("SELECT revision,fence,policy,lifecycle_ready,clock_ack,due_at,hard_due FROM execution_tasks WHERE group_name=? AND task=?")
        .bind(group).bind(task).fetch_one(&mut **tx).await.context("execution_lifecycle_unavailable")
}
async fn attempt(tx: &mut Tx<'_>, c: &Correlation) -> Result<Attempt> {
    let row: Attempt = sqlx::query_as("SELECT * FROM execution_attempts WHERE id=?")
        .bind(&c.attempt)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(row.correlation() == *c, "attempt_correlation_conflict");
    Ok(row)
}
async fn ancestors(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    let mut next = Some(task.to_owned());
    while let Some(id) = next {
        ensure!(
            ids.len() < 100 && seen.insert(id.clone()),
            "execution_ancestry_incomplete"
        );
        next = model(tx, group, &id).await?.parent;
        ids.push(id);
    }
    Ok(ids)
}
async fn account(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<Account> {
    sqlx::query_as("SELECT * FROM execution_budgets WHERE group_name=? AND task=?")
        .bind(group)
        .bind(task)
        .fetch_one(&mut **tx)
        .await
        .context("execution_budget_unavailable")
}
async fn event(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    attempt: Option<&str>,
    kind: &str,
    payload: Value,
    now: i64,
) -> Result<i64> {
    Ok(sqlx::query("INSERT INTO execution_events(group_name,task,attempt,kind,payload,created) VALUES(?,?,?,?,?,?)")
        .bind(group).bind(task).bind(attempt).bind(kind).bind(canonical(&payload)?).bind(now).execute(&mut **tx).await?.last_insert_rowid())
}
async fn prior<T: for<'de> Deserialize<'de>>(
    tx: &mut Tx<'_>,
    producer: &str,
    key: &str,
    bytes: &str,
) -> Result<Option<T>> {
    ensure!(!key.is_empty() && key.len() <= 256, "invalid_execution_key");
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT canonical,result FROM execution_receipts WHERE producer=? AND key=?",
    )
    .bind(producer)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|(old, result)| {
        ensure!(old == bytes, "execution_retry_conflict");
        Ok(serde_json::from_str(&result)?)
    })
    .transpose()
}
async fn receipt<T: Serialize>(
    tx: &mut Tx<'_>,
    producer: &str,
    key: &str,
    bytes: &str,
    value: &T,
) -> Result<()> {
    sqlx::query("INSERT INTO execution_receipts(producer,key,canonical,result) VALUES(?,?,?,?)")
        .bind(producer)
        .bind(key)
        .bind(bytes)
        .bind(canonical(value)?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn observe_clock(tx: &mut Tx<'_>, group: &str, now: i64) -> Result<(i64, bool)> {
    ensure!(now >= 0, "invalid_execution_time");
    sqlx::query("INSERT INTO execution_clock(group_name,observed) VALUES(?,?) ON CONFLICT(group_name) DO UPDATE SET generation=generation+CASE WHEN excluded.observed<observed THEN 1 ELSE 0 END,discontinuity=CASE WHEN excluded.observed<observed THEN 1 ELSE discontinuity END,observed=max(observed,excluded.observed)")
        .bind(group).bind(now).execute(&mut **tx).await?;
    let (generation, discontinuity): (i64, bool) =
        sqlx::query_as("SELECT generation,discontinuity FROM execution_clock WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    Ok((generation, discontinuity))
}
async fn cause(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    code: &str,
    detail: &str,
    now: i64,
) -> Result<()> {
    let writer = model(tx, group, task).await?.writer;
    let hard = effective_deadline(tx, group, task).await?;
    sqlx::query("INSERT INTO execution_causes(id,group_name,task,code,detail,responsible,opened,review_at,hard_due,escalated) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_name,task,code) WHERE settled=0 DO UPDATE SET detail=excluded.detail,responsible=excluded.responsible,review_at=min(execution_causes.hard_due,excluded.review_at),hard_due=min(execution_causes.hard_due,excluded.hard_due),escalated=max(execution_causes.escalated,excluded.escalated)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(group).bind(task).bind(code).bind(detail).bind(writer).bind(now).bind(finite(now, RECHECK)?.min(hard)).bind(hard).bind(now >= hard).execute(&mut **tx).await?;
    Ok(())
}
async fn effective_deadline(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<i64> {
    let mut deadline = i64::MAX;
    for id in ancestors(tx, group, task).await? {
        deadline = deadline.min(execution(tx, group, &id).await?.hard_due);
        deadline = deadline.min(account(tx, group, &id).await?.deadline.unwrap_or(i64::MAX));
        deadline = deadline.min(model(tx, group, &id).await?.deadline.unwrap_or(i64::MAX));
    }
    ensure!(deadline < i64::MAX, "execution_deadline_unavailable");
    Ok(deadline)
}

/// A validated original dispatch limit, narrowed by current task/ancestor limits.
/// This is deadline information, not permission to admit or publish an attempt.
#[derive(Debug)]
pub(crate) struct OriginalAttemptDeadline {
    deadline: i64,
}
impl OriginalAttemptDeadline {
    pub(crate) fn deadline(&self) -> i64 {
        self.deadline
    }
}

pub(crate) async fn original_attempt_deadline_tx(
    tx: &mut Tx<'_>,
    correlation: &Correlation,
) -> Result<OriginalAttemptDeadline> {
    let original = attempt(tx, correlation).await?;
    let request: String =
        sqlx::query_scalar("SELECT request FROM execution_dispatches WHERE attempt=?")
            .bind(&original.id)
            .fetch_one(&mut **tx)
            .await?;
    let dispatch: Value = serde_json::from_str(&request)?;
    let deadline = dispatch
        .get("deadline")
        .and_then(Value::as_i64)
        .context("invalid_original_dispatch_deadline")?;
    ensure!(
        deadline > original.created,
        "invalid_original_dispatch_deadline"
    );
    let inputs: InputSnapshot = serde_json::from_str(&original.inputs)?;
    let target: RuntimeTarget = serde_json::from_str(&original.runtime)?;
    let expected = canonical(
        &json!({"correlation":correlation,"inputs":inputs,"target":target,"deadline":deadline}),
    )?;
    ensure!(request == expected, "original_dispatch_identity_conflict");
    Ok(OriginalAttemptDeadline {
        deadline: deadline
            .min(effective_deadline(tx, &correlation.group, &correlation.task).await?),
    })
}

async fn settle_cause(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    code: &str,
    now: i64,
) -> Result<()> {
    sqlx::query("UPDATE execution_causes SET settled=1,revision=revision+1,disposition=? WHERE group_name=? AND task=? AND code=? AND settled=0")
        .bind(canonical(&json!({"predicate_cleared_at":now}))?).bind(group).bind(task).bind(code).execute(&mut **tx).await?;
    Ok(())
}

async fn initialize(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    now: i64,
    lifecycle: bool,
) -> Result<()> {
    let row = model(tx, group, task).await?;
    let contract: Contract = serde_json::from_str(&row.contract)?;
    let elapsed = i64::try_from(contract.budget.max_elapsed_seconds)?;
    let hard = finite(now, elapsed)?.min(row.deadline.unwrap_or(i64::MAX));
    sqlx::query("INSERT OR IGNORE INTO execution_tasks(group_name,task,policy,lifecycle_ready,due_at,hard_due) VALUES(?,?,?,?,?,?)")
        .bind(group).bind(task).bind(canonical(&ExecutionPolicy::default())?).bind(lifecycle).bind(now).bind(hard).execute(&mut **tx).await?;
    let cost = contract
        .budget
        .max_cost
        .as_ref()
        .map(|c| i64::try_from(c.amount))
        .transpose()?;
    sqlx::query("INSERT OR IGNORE INTO execution_budgets(group_name,task,max_attempts,elapsed_seconds,cost_limit,cost_unit) VALUES(?,?,?,?,?,?)")
        .bind(group).bind(task).bind(i64::from(contract.budget.max_attempts)).bind(elapsed).bind(cost).bind(contract.budget.max_cost.as_ref().map(|c| &c.unit)).execute(&mut **tx).await?;
    Ok(())
}

/// Model owner calls after all business rows/projections and before its receipt.
/// The complete affected set includes newly eligible consumers. A late repair
/// cannot fabricate an original eligibility timestamp; it stays unavailable.
pub(crate) async fn sync_model_tx(
    tx: &mut Tx<'_>,
    group: &str,
    tasks: &[String],
    now: i64,
) -> Result<()> {
    reserve(tx, group).await?;
    for task in tasks {
        for id in ancestors(tx, group, task).await? {
            initialize(tx, group, &id, now, true).await?;
        }
    }
    for task in tasks {
        let ready = task_graph::evaluate_task_tx(tx, group, task, Phase::Execute).await?;
        if ready.causes.is_empty() && execution(tx, group, task).await?.lifecycle_ready {
            for id in ancestors(tx, group, task).await? {
                let a = account(tx, group, &id).await?;
                let deadline = finite(now, a.elapsed_seconds)?;
                sqlx::query("UPDATE execution_budgets SET anchor=?,deadline=? WHERE group_name=? AND task=? AND anchor IS NULL")
                    .bind(now).bind(deadline).bind(group).bind(&id).execute(&mut **tx).await?;
                sqlx::query("UPDATE execution_tasks SET hard_due=min(hard_due,?) WHERE group_name=? AND task=?")
                    .bind(deadline).bind(group).bind(&id).execute(&mut **tx).await?;
            }
        }
        let held: Option<Attempt> = sqlx::query_as(
            "SELECT * FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
        )
        .bind(group)
        .bind(task)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(held) = held {
            let inputs: InputSnapshot = serde_json::from_str(&held.inputs)?;
            if !ready.causes.is_empty()
                || task_graph::validate_inputs_tx(tx, &inputs).await? != InputValidity::Current
            {
                stop_intent_tx(tx, &held, "model_invalidated", now).await?;
            }
        }
        sqlx::query(
            "UPDATE execution_tasks SET due_at=min(due_at,?) WHERE group_name=? AND task=?",
        )
        .bind(now)
        .bind(group)
        .bind(task)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) async fn guard_success_tx(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<()> {
    reserve(tx, group).await?;
    ensure!(
        execution(tx, group, task).await?.lifecycle_ready,
        "execution_lifecycle_unavailable"
    );
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
    )
    .bind(group)
    .bind(task)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(held == 0, "execution_cleanup_unresolved");
    Ok(())
}

pub(crate) async fn guard_model_change_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    proposed: &Budget,
    parent: Option<&str>,
    now: i64,
) -> Result<()> {
    reserve(tx, group).await?;
    let old = model(tx, group, task).await?;
    let a = account(tx, group, task).await?;
    ensure!(
        proposed.max_attempts > 0 && proposed.max_elapsed_seconds > 0,
        "finite_budget_required"
    );
    ensure!(
        i64::from(proposed.max_attempts)
            >= a.attempts_spent
                .checked_add(a.attempts_reserved)
                .context("budget_overflow")?,
        "budget_below_consumption"
    );
    let cost = proposed
        .max_cost
        .as_ref()
        .map(|c| i64::try_from(c.amount))
        .transpose()?;
    let unit = proposed.max_cost.as_ref().map(|c| c.unit.as_str());
    ensure!(
        unit == a.cost_unit.as_deref(),
        "execution_cost_unit_immutable"
    );
    if let Some(cap) = cost {
        ensure!(
            cap > 0
                && cap
                    >= a.cost_spent
                        .checked_add(a.cost_reserved)
                        .context("budget_overflow")?,
            "cost_below_consumption"
        );
    }
    if old.parent.as_deref() != parent {
        // Moving an already charged subtree needs explicit old/new ancestor
        // transfer accounting. Until that owner composition exists, fail closed.
        let started: i64 = sqlx::query_scalar("WITH RECURSIVE descendants(task) AS (SELECT ? UNION SELECT m.task FROM task_models m JOIN descendants d ON m.parent=d.task WHERE m.group_name=?) SELECT count(*) FROM execution_budgets b JOIN descendants d ON d.task=b.task WHERE b.group_name=? AND (anchor IS NOT NULL OR attempts_spent+attempts_reserved>0)")
            .bind(task).bind(group).bind(group).fetch_one(&mut **tx).await?;
        ensure!(started == 0, "execution_started_subtree_move_unavailable");
    }
    let elapsed = i64::try_from(proposed.max_elapsed_seconds)?;
    let deadline = a.anchor.map(|anchor| finite(anchor, elapsed)).transpose()?;
    let deadline = match (a.deadline, deadline) {
        (Some(old), Some(new)) => Some(old.min(new)),
        (_, new) => new,
    };
    sqlx::query("UPDATE execution_budgets SET max_attempts=?,elapsed_seconds=?,cost_limit=?,deadline=? WHERE group_name=? AND task=?")
        .bind(i64::from(proposed.max_attempts)).bind(elapsed).bind(cost).bind(deadline).bind(group).bind(task).execute(&mut **tx).await?;
    if let Some(deadline) = deadline {
        sqlx::query("UPDATE execution_tasks SET hard_due=min(hard_due,?),due_at=min(due_at,?) WHERE group_name=? AND task=?")
            .bind(deadline).bind(now).bind(group).bind(task).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Outcome of a guarded operation, separate from storage or contract errors.
///
/// The enclosing operation defines which observations a held result commits.
/// A staged atomic decision rolls back its entire transaction before returning
/// a hold. Neither variant supplies source or runtime authority by itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Checked<T> {
    /// The operation produced its typed result.
    Ready(T),
    /// Current guard reasons preventing the requested operation.
    Held(Vec<String>),
}

/// Exact permission evidence, selected and authenticated by the progress owner.
/// A missing historical ProgressEvidence is different from this explicit absent pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProgressBasis {
    judgment_record: Option<i64>,
    report_event: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressEvidence {
    basis: ProgressBasis,
    policy_record: Option<i64>,
    progress_revision: Option<i64>,
}
impl ProgressEvidence {
    fn from_boundary(boundary: &crate::progress::ProgressBoundary) -> Result<Self> {
        ensure!(
            boundary.judgment_record.is_some() == boundary.report_event.is_some(),
            "progress_basis_pair_missing"
        );
        ensure!(
            boundary.judgment_record.is_none_or(|id| id > 0)
                && boundary.report_event.is_none_or(|id| id > 0),
            "invalid_progress_basis_identity"
        );
        Ok(Self {
            basis: ProgressBasis {
                judgment_record: boundary.judgment_record,
                report_event: boundary.report_event,
            },
            policy_record: boundary.policy_record,
            progress_revision: boundary.progress_revision,
        })
    }
}
struct Readiness {
    holds: Vec<String>,
    progress: Option<ProgressEvidence>,
}

async fn readiness(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    now: i64,
    new_attempt: bool,
) -> Result<Vec<String>> {
    Ok(readiness_with_basis(tx, group, task, now, new_attempt)
        .await?
        .holds)
}

async fn readiness_with_basis(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    now: i64,
    new_attempt: bool,
) -> Result<Readiness> {
    scheduler_blocking_sources_tx(tx, group).await?;
    let e = execution(tx, group, task).await?;
    let row = model(tx, group, task).await?;
    let (generation, discontinuity) = observe_clock(tx, group, now).await?;
    let policy: ExecutionPolicy = serde_json::from_str(&e.policy)?;
    let mut codes = Vec::new();
    if !e.lifecycle_ready {
        codes.push("lifecycle_unavailable".to_owned());
    }
    let observed: i64 =
        sqlx::query_scalar("SELECT observed FROM execution_clock WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    if now < observed || (discontinuity && e.clock_ack < generation) {
        codes.push("clock_discontinuity".into());
    }
    let paused: bool = sqlx::query_scalar("SELECT paused FROM groups WHERE name=?")
        .bind(group)
        .fetch_one(&mut **tx)
        .await?;
    if paused {
        codes.push("group_paused".into());
    }
    let model_ready = task_graph::evaluate_task_tx(tx, group, task, Phase::Execute).await?;
    let model_current = model_ready.causes.is_empty();
    for reason in model_ready.causes {
        codes.push(format!("model:{}:{}", reason.code, reason.task));
    }
    for id in ancestors(tx, group, task).await? {
        let a = account(tx, group, &id).await?;
        if a.anchor.is_none() {
            codes.push(format!("unanchored:{id}"));
        }
        if a.deadline.is_some_and(|deadline| now >= deadline) {
            codes.push(format!("elapsed_exhausted:{id}"));
        }
        if new_attempt
            && a.attempts_spent
                .checked_add(a.attempts_reserved)
                .context("budget_overflow")?
                >= a.max_attempts
        {
            codes.push(format!("attempts_exhausted:{id}"));
        }
        if new_attempt
            && a.cost_limit
                .is_some_and(|limit| a.cost_spent.saturating_add(a.cost_reserved) >= limit)
        {
            codes.push(format!("cost_exhausted:{id}"));
        }
    }
    if row.deadline.is_some_and(|deadline| now >= deadline)
        || now >= effective_deadline(tx, group, task).await?
    {
        codes.push("hard_boundary".into());
    }
    let mut progress_observed = false;
    let mut progress_basis = None;
    if model_current {
        match current_progress_tx(tx, group, task, &policy, now).await {
            Ok((effective, boundary, facts)) => {
                let progress = ProgressEvidence::from_boundary(&boundary)?;
                progress_observed = true;
                if facts.clock_hold {
                    codes.push("clock_discontinuity".into());
                }
                if !facts.lifecycle_ready || facts.anchor_at.is_none() {
                    codes.push("lifecycle_unavailable".into());
                }
                let continued =
                    match continuation_allows_tx(tx, group, task, &facts, &progress, now).await {
                        Ok(continued) => continued,
                        Err(error) => {
                            cause(
                                tx,
                                group,
                                task,
                                "progress_boundary_unavailable",
                                &error.to_string(),
                                now,
                            )
                            .await?;
                            codes.push("progress_boundary_unavailable".into());
                            false
                        }
                    };
                if let (Some(anchor), Some(seconds)) =
                    (facts.anchor_at, effective.no_progress_seconds)
                {
                    if !continued && facts.effective_now >= finite(anchor, i64::try_from(seconds)?)?
                    {
                        codes.push("no_progress_elapsed".into());
                    }
                }
                if new_attempt
                    && !continued
                    && facts.closed_admitted_segments_since_anchor
                        >= i64::from(effective.closed_segment_limit)
                {
                    codes.push("strategy_decision_required".into());
                }
                if !codes
                    .iter()
                    .any(|code| code == "progress_boundary_unavailable")
                {
                    settle_cause(tx, group, task, "progress_boundary_unavailable", now).await?;
                }
                progress_basis = Some(progress);
            }
            Err(error) => {
                cause(
                    tx,
                    group,
                    task,
                    "progress_boundary_unavailable",
                    &error.to_string(),
                    now,
                )
                .await?;
                codes.push("progress_boundary_unavailable".into());
            }
        }
    }
    codes.sort();
    codes.dedup();
    for code in &codes {
        if code != "progress_boundary_unavailable" {
            cause(tx, group, task, code, code, now).await?;
        }
    }
    // Only predicate holds owned by this evaluation are settled here. Cleanup,
    // adapter observations, decisions and reports have separate source owners.
    let previous: Vec<String> = sqlx::query_scalar(
        "SELECT code FROM execution_causes WHERE group_name=? AND task=? AND settled=0",
    )
    .bind(group)
    .bind(task)
    .fetch_all(&mut **tx)
    .await?;
    for code in previous {
        let owned = code.starts_with("model:")
            || code.starts_with("unanchored:")
            || code.starts_with("elapsed_exhausted:")
            || code.starts_with("attempts_exhausted:")
            || code.starts_with("cost_exhausted:")
            || matches!(
                code.as_str(),
                "lifecycle_unavailable"
                    | "clock_discontinuity"
                    | "group_paused"
                    | "hard_boundary"
                    | "no_progress_elapsed"
                    | "strategy_decision_required"
            );
        // Running the finishing guard must not clear a new-attempt-only hold.
        let admission_only = code.starts_with("attempts_exhausted:")
            || code.starts_with("cost_exhausted:")
            || code == "strategy_decision_required";
        let progress_predicate = matches!(
            code.as_str(),
            "strategy_decision_required" | "no_progress_elapsed"
        );
        if owned
            && !codes.contains(&code)
            && (new_attempt || !admission_only)
            && (!progress_predicate || progress_observed)
        {
            settle_cause(tx, group, task, &code, now).await?;
        }
    }
    Ok(Readiness {
        holds: codes,
        progress: progress_basis,
    })
}

/// Actual progress owner selection and scheduler facts share this writer tx.
/// Persist only narrower limits so editing a policy cannot erase a crossed
/// boundary. Qualified evidence may advance the progress anchor; lifetime
/// anchors, charges and deadlines are never changed here.
async fn current_progress_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    ceiling: &ExecutionPolicy,
    now: i64,
) -> Result<(
    ExecutionPolicy,
    crate::progress::ProgressBoundary,
    ExecutionProgressFacts,
)> {
    let inputs = task_graph::capture_inputs_tx(tx, group, task, Phase::Execute).await?;
    let boundary = crate::progress::progress_boundary_tx(tx, group, task, &inputs).await?;
    ProgressEvidence::from_boundary(&boundary)?;
    ensure!(
        boundary.max_segments_without_milestone > 0
            && boundary.max_elapsed_without_milestone != Some(0),
        "invalid_progress_boundary"
    );
    let elapsed = match (
        ceiling.no_progress_seconds,
        boundary.max_elapsed_without_milestone,
    ) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let effective = ExecutionPolicy {
        closed_segment_limit: ceiling
            .closed_segment_limit
            .min(boundary.max_segments_without_milestone),
        no_progress_seconds: elapsed,
    };
    if effective != *ceiling {
        sqlx::query("UPDATE execution_tasks SET policy=? WHERE group_name=? AND task=?")
            .bind(canonical(&effective)?)
            .bind(group)
            .bind(task)
            .execute(&mut **tx)
            .await?;
        event(tx,group,task,None,"progress_policy_narrowed",json!({"policy":effective,"policy_record":boundary.policy_record,"progress_revision":boundary.progress_revision}),now).await?;
    }
    let facts = execution_progress_facts_tx(tx, group, task, boundary.report_event, now).await?;
    Ok((effective, boundary, facts))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClaimRequest {
    pub group: String,
    pub task: String,
    pub revision: i64,
    pub key: String,
}

/// Reservation, charge ancestry, runtime slot and immutable outbox are atomic.
/// RuntimeGate::target returns only a supported authenticated persistent target.
pub(crate) async fn claim_attempt_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    request: &ClaimRequest,
    now: i64,
) -> Result<Checked<Correlation>> {
    reserve(tx, &request.group).await?;
    let producer = format!("scheduler:{}", request.group);
    let bytes = canonical(request)?;
    if let Some(old) = prior(tx, &producer, &request.key, &bytes).await? {
        return Ok(Checked::Ready(old));
    }
    let group = &request.group;
    let task = &request.task;
    let row = execution(tx, group, task).await?;
    ensure!(
        row.revision == request.revision,
        "execution_revision_conflict"
    );
    let readiness = readiness_with_basis(tx, group, task, now, true).await?;
    let mut holds = readiness.holds;
    if now < row.due_at {
        holds.push("not_due".into());
    }
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
    )
    .bind(group)
    .bind(task)
    .fetch_one(&mut **tx)
    .await?;
    if held != 0 {
        holds.push("attempt_held".into());
    }
    if !holds.is_empty() {
        return Ok(Checked::Held(holds));
    }
    let progress = readiness.progress.context("progress_basis_unavailable")?;
    let m = model(tx, group, task).await?;
    let inputs = task_graph::capture_inputs_tx(tx, group, task, Phase::Execute).await?;
    let Some(target) = runtime
        .target(
            tx,
            group,
            task,
            &m.owner,
            inputs.owner_binding_generation,
            now,
        )
        .await?
    else {
        cause(
            tx,
            group,
            task,
            "runtime_unavailable",
            "No authenticated execution-capable runtime target",
            now,
        )
        .await?;
        return Ok(Checked::Held(vec!["runtime_unavailable".into()]));
    };
    ensure!(
        !target.identity.is_empty()
            && !target.concurrency_key.is_empty()
            && target.identity.len() <= 256
            && target.concurrency_key.len() <= 256
            && target.generation > 0,
        "invalid_runtime_target"
    );
    let occupied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_slots WHERE runtime_key=?)")
            .bind(&target.concurrency_key)
            .fetch_one(&mut **tx)
            .await?;
    if occupied {
        cause(
            tx,
            group,
            task,
            "runtime_slot_held",
            "Original runtime execution still owns the slot",
            now,
        )
        .await?;
        return Ok(Checked::Held(vec!["runtime_slot_held".into()]));
    }
    let mut charges = Vec::new();
    for id in ancestors(tx, group, task).await? {
        let a = account(tx, group, &id).await?;
        let cap = if let (Some(unit), Some(limit)) = (&a.cost_unit, a.cost_limit) {
            let Some(&cap) = target.cost_caps.get(unit) else {
                cause(tx, group, task, "cost_enforcement_unavailable", unit, now).await?;
                return Ok(Checked::Held(vec!["cost_enforcement_unavailable".into()]));
            };
            ensure!(cap >= 0, "invalid_runtime_cost_cap");
            if a.cost_spent
                .checked_add(a.cost_reserved)
                .and_then(|v| v.checked_add(cap))
                .context("budget_overflow")?
                > limit
            {
                cause(tx, group, task, "cost_reservation_exhausted", &id, now).await?;
                return Ok(Checked::Held(vec!["cost_reservation_exhausted".into()]));
            }
            Some(cap)
        } else {
            None
        };
        charges.push((id, cap, a.cost_unit));
    }
    let fence = row
        .fence
        .checked_add(1)
        .context("execution_fence_exhausted")?;
    let c = Correlation {
        group: group.clone(),
        task: task.clone(),
        attempt: uuid::Uuid::new_v4().to_string(),
        fence,
        dispatch_key: uuid::Uuid::new_v4().to_string(),
    };
    let predecessor: Option<String> = sqlx::query_scalar("SELECT id FROM execution_attempts WHERE group_name=? AND task=? ORDER BY fence DESC LIMIT 1")
        .bind(group).bind(task).fetch_optional(&mut **tx).await?;
    sqlx::query("UPDATE execution_tasks SET fence=?,revision=revision+1,due_at=? WHERE group_name=? AND task=? AND revision=?")
        .bind(fence).bind(finite(now, RECHECK)?).bind(group).bind(task).bind(row.revision).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO execution_attempts(id,group_name,task,fence,owner,owner_binding,inputs,runtime,runtime_key,dispatch_key,predecessor,state,created,observed,reconcile_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,'reserved',?,?,?)")
        .bind(&c.attempt).bind(group).bind(task).bind(fence).bind(&m.owner).bind(inputs.owner_binding_generation).bind(canonical(&inputs)?).bind(canonical(&target)?).bind(&target.concurrency_key).bind(&c.dispatch_key).bind(predecessor).bind(now).bind(now).bind(finite(now, RECHECK)?).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO execution_slots(runtime_key,attempt) VALUES(?,?)")
        .bind(&target.concurrency_key)
        .bind(&c.attempt)
        .execute(&mut **tx)
        .await?;
    let deadline = effective_deadline(tx, group, task).await?;
    let payload =
        canonical(&json!({"correlation":c,"inputs":inputs,"target":target,"deadline":deadline}))?;
    sqlx::query("INSERT INTO execution_dispatches(attempt,request,phase) VALUES(?,?,'prepared')")
        .bind(&c.attempt)
        .bind(payload)
        .execute(&mut **tx)
        .await?;
    for (id, cap, unit) in charges {
        let changed = sqlx::query("UPDATE execution_budgets SET attempts_reserved=attempts_reserved+1,cost_reserved=cost_reserved+? WHERE group_name=? AND task=? AND attempts_spent+attempts_reserved<max_attempts AND (cost_limit IS NULL OR cost_spent+cost_reserved+?<=cost_limit)")
            .bind(cap.unwrap_or(0)).bind(group).bind(&id).bind(cap.unwrap_or(0)).execute(&mut **tx).await?;
        ensure!(changed.rows_affected() == 1, "execution_budget_conflict");
        sqlx::query("INSERT INTO execution_charges(attempt,group_name,account,cost_cap,cost_unit) VALUES(?,?,?,?,?)")
            .bind(&c.attempt).bind(group).bind(id).bind(cap).bind(unit).execute(&mut **tx).await?;
    }
    for code in [
        "runtime_unavailable",
        "runtime_slot_held",
        "cost_enforcement_unavailable",
        "cost_reservation_exhausted",
    ] {
        settle_cause(tx, group, task, code, now).await?;
    }
    event(
        tx,
        group,
        task,
        Some(&c.attempt),
        "progress_claim_basis",
        serde_json::to_value(&progress)?,
        now,
    )
    .await?;
    event(tx, group, task, Some(&c.attempt), "claimed", json!(c), now).await?;
    receipt(tx, &producer, &request.key, &bytes, &c).await?;
    Ok(Checked::Ready(c))
}

/// Checks the original slot and all current input/authority/budget/runtime facts.
/// Publish is legal only for a running admitted attempt. Runtime additionally
/// checks model publication action/scope and its sealed effect-set protocol.
/// Caller must commit Held results to retain clock/cause observations.
pub(crate) async fn validate_current_attempt_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    c: &Correlation,
    purpose: CurrentUse,
    now: i64,
) -> Result<Checked<InputSnapshot>> {
    reserve(tx, &c.group).await?;
    let a = attempt(tx, c).await?;
    let inputs: InputSnapshot = serde_json::from_str(&a.inputs)?;
    let target: RuntimeTarget = serde_json::from_str(&a.runtime)?;
    let readiness = readiness_with_basis(tx, &c.group, &c.task, now, false).await?;
    let mut holds = readiness.holds;
    if matches!(purpose, CurrentUse::Dispatch | CurrentUse::Admit) {
        let original: Option<String> = sqlx::query_scalar("SELECT payload FROM execution_events WHERE attempt=? AND group_name=? AND task=? AND kind='progress_claim_basis' ORDER BY id LIMIT 1")
            .bind(&c.attempt).bind(&c.group).bind(&c.task).fetch_optional(&mut **tx).await?;
        let original = original
            .map(|raw| serde_json::from_str::<ProgressEvidence>(&raw))
            .transpose()?;
        match (original, readiness.progress) {
            (Some(original), Some(current)) if original.basis == current.basis => {}
            (None, _) => holds.push("progress_admission_unproven".into()),
            _ => holds.push("progress_admission_changed".into()),
        }
    }
    let slot: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_slots WHERE runtime_key=? AND attempt=?)",
    )
    .bind(&a.runtime_key)
    .bind(&a.id)
    .fetch_one(&mut **tx)
    .await?;
    let e = execution(tx, &c.group, &c.task).await?;
    let state_allowed = match purpose {
        CurrentUse::Dispatch | CurrentUse::Admit => {
            matches!(a.state.as_str(), "reserved" | "dispatching")
        }
        CurrentUse::Report | CurrentUse::Publish => a.state == "running" && a.admitted,
    };
    if !a.holds_slot || !slot || e.fence != a.fence || !state_allowed {
        holds.push("attempt_not_current".into());
    }
    if task_graph::validate_inputs_tx(tx, &inputs).await? != InputValidity::Current {
        holds.push("inputs_stale".into());
    }
    let m = model(tx, &c.group, &c.task).await?;
    if m.owner != a.owner {
        holds.push("owner_changed".into());
    }
    if !runtime.current(tx, c, &target, purpose, now).await? {
        holds.push("runtime_not_current".into());
    }
    if holds.is_empty() {
        Ok(Checked::Ready(inputs))
    } else {
        for code in &holds {
            cause(tx, &c.group, &c.task, code, code, now).await?;
        }
        Ok(Checked::Held(holds))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DispatchOffer {
    pub correlation: Correlation,
    pub request: String,
    pub revision: i64,
}

/// Exposure is committed before any send. Lost replies can only replay this key
/// where the authenticated adapter declares durable deduplication.
pub(crate) async fn expose_dispatch_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    c: &Correlation,
    dispatcher: &str,
    revision: i64,
    now: i64,
) -> Result<Checked<DispatchOffer>> {
    reserve(tx, &c.group).await?;
    ensure!(
        !dispatcher.is_empty() && dispatcher.len() <= 256,
        "invalid_dispatcher"
    );
    let a = attempt(tx, c).await?;
    let target: RuntimeTarget = serde_json::from_str(&a.runtime)?;
    let (request, phase, current_revision, transmissions, lease_until): (String,String,i64,i64,i64) = sqlx::query_as("SELECT request,phase,revision,transmissions,lease_until FROM execution_dispatches WHERE attempt=?")
        .bind(&c.attempt).fetch_one(&mut **tx).await?;
    ensure!(current_revision == revision, "dispatch_revision_conflict");
    if phase == "settled"
        || lease_until > now
        || transmissions >= 3
        || (phase == "exposed" && !target.durable_dedupe)
    {
        cause(
            tx,
            &c.group,
            &c.task,
            "dispatch_reconciliation",
            "Exposure cannot be inferred absent or automatically repeated",
            now,
        )
        .await?;
        return Ok(Checked::Held(vec!["dispatch_reconciliation".into()]));
    }
    if let Checked::Held(holds) =
        validate_current_attempt_tx(tx, runtime, c, CurrentUse::Dispatch, now).await?
    {
        stop_intent_tx(tx, &a, "dispatch_invalidated", now).await?;
        return Ok(Checked::Held(holds));
    }
    sqlx::query("UPDATE execution_dispatches SET phase='exposed',revision=revision+1,transmissions=transmissions+1,lease_owner=?,lease_until=? WHERE attempt=? AND revision=?")
        .bind(dispatcher).bind(finite(now, RECHECK)?).bind(&c.attempt).bind(revision).execute(&mut **tx).await?;
    sqlx::query("UPDATE execution_attempts SET state='dispatching',reconcile_at=? WHERE id=?")
        .bind(finite(now, RECHECK)?)
        .bind(&c.attempt)
        .execute(&mut **tx)
        .await?;
    event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "dispatch_exposed",
        json!({"revision": revision+1,"dispatcher":dispatcher}),
        now,
    )
    .await?;
    Ok(Checked::Ready(DispatchOffer {
        correlation: c.clone(),
        request,
        revision: revision + 1,
    }))
}

/// One-time authenticated admission. Historical replay returns the original
/// receipt even after closure; it grants no second start to the runtime owner.
pub(crate) async fn admit_execution_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    c: &Correlation,
    now: i64,
) -> Result<Checked<i64>> {
    reserve(tx, &c.group).await?;
    let bytes = canonical(c)?;
    if let Some(old) = prior(tx, "runtime-admission", &c.dispatch_key, &bytes).await? {
        return Ok(Checked::Ready(old));
    }
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM execution_dispatches WHERE attempt=?")
            .bind(&c.attempt)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(phase == "exposed", "dispatch_not_exposed");
    if let Checked::Held(holds) =
        validate_current_attempt_tx(tx, runtime, c, CurrentUse::Admit, now).await?
    {
        return Ok(Checked::Held(holds));
    }
    let changed = sqlx::query("UPDATE execution_attempts SET state='running',admitted=1,observed=?,reconcile_at=? WHERE id=? AND admitted=0 AND state='dispatching'")
        .bind(now).bind(finite(now, RECHECK)?).bind(&c.attempt).execute(&mut **tx).await?;
    ensure!(changed.rows_affected() == 1, "attempt_admission_conflict");
    let id = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "admitted",
        json!(c),
        now,
    )
    .await?;
    receipt(tx, "runtime-admission", &c.dispatch_key, &bytes, &id).await?;
    Ok(Checked::Ready(id))
}

/// Report evidence does not close an attempt or accept a task result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReport {
    /// Exact current attempt.
    pub correlation: Correlation,
    /// Stable append retry key.
    pub key: String,
    /// Report kind interpreted as evidence only.
    pub kind: ReportKind,
    /// Concise observed result or failure.
    pub summary: String,
    /// References retained without promoting them to verified effects.
    pub evidence: Vec<String>,
}
/// Worker-reported event category; none grants success or another attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportKind {
    /// A result is available for a separate model candidate/decision.
    Result,
    /// Work wants to continue after real runtime closure.
    Yield,
    /// Worker reports failure.
    Failure,
}

pub(crate) async fn record_report_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    actor: &Mailbox,
    report: &ExecutionReport,
    now: i64,
) -> Result<Checked<i64>> {
    Store::lock_actor(tx, actor).await?;
    let c = &report.correlation;
    ensure!(actor.group_name == c.group, "report_group_conflict");
    reserve(tx, &c.group).await?;
    ensure!(
        !report.summary.trim().is_empty()
            && report.summary.len() <= 4096
            && report.evidence.len() <= 32
            && report
                .evidence
                .iter()
                .all(|e| !e.is_empty() && e.len() <= 256),
        "invalid_execution_report"
    );
    let bytes = canonical(report)?;
    ensure!(bytes.len() <= 8192, "execution_report_too_large");
    let producer = format!("report:{}:{}", actor.id, actor.binding_version);
    if let Some(old) = prior(tx, &producer, &report.key, &bytes).await? {
        return Ok(Checked::Ready(old));
    }
    let a = attempt(tx, c).await?;
    ensure!(
        a.owner == actor.name && a.owner_binding == actor.binding_version,
        "attempt_report_authority"
    );
    if let Checked::Held(holds) =
        validate_current_attempt_tx(tx, runtime, c, CurrentUse::Report, now).await?
    {
        return Ok(Checked::Held(holds));
    }
    let id = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "reported",
        json!(report),
        now,
    )
    .await?;
    // No task-version or execution-version mutation for pure append.
    receipt(tx, &producer, &report.key, &bytes, &id).await?;
    Ok(Checked::Ready(id))
}

/// Prepared under the actual writer reservation, before accepted components.
/// This value cannot be supplied through runtime/native JSON.
#[derive(Debug)]
pub(crate) struct YieldReviewPlan {
    correlation: Correlation,
    report_bytes: String,
    original: crate::followup::CheckpointTaskBasis,
    actor: i64,
    actor_binding: i64,
    recorded_at: i64,
    requested_seconds: u32,
    review_at: i64,
}
impl YieldReviewPlan {
    pub(crate) fn recorded_at(&self) -> i64 {
        self.recorded_at
    }
    pub(crate) fn review_at(&self) -> i64 {
        self.review_at
    }
}

/// Protected immutable audit data. It is never a runtime permission proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct YieldReviewFact {
    schema_version: u16,
    correlation: Correlation,
    report_event: i64,
    checkpoint_history: i64,
    checkpoint_version: i64,
    original: crate::followup::CheckpointTaskBasis,
    actor: i64,
    actor_binding: i64,
    recorded_at: i64,
    requested_seconds: u32,
    review_at: i64,
}
#[derive(Debug, Serialize)]
pub(crate) struct YieldReviewReceipt {
    pub schema_version: u16,
    pub event: i64,
    pub correlation: Correlation,
    pub report_event: i64,
    pub checkpoint_history: i64,
    pub actor: i64,
    pub actor_binding: i64,
    pub recorded_at: i64,
    pub requested_seconds: u32,
    pub review_at: i64,
}
impl YieldReviewFact {
    fn receipt(&self, event: i64) -> YieldReviewReceipt {
        YieldReviewReceipt {
            schema_version: 1,
            event,
            correlation: self.correlation.clone(),
            report_event: self.report_event,
            checkpoint_history: self.checkpoint_history,
            actor: self.actor,
            actor_binding: self.actor_binding,
            recorded_at: self.recorded_at,
            requested_seconds: self.requested_seconds,
            review_at: self.review_at,
        }
    }
}

/// Fresh original-basis validation and the actual task/ancestor deadline clamp.
/// Initial Held precedes report/checkpoint/review writes; callers commit causes.
pub(crate) async fn prepare_yield_review_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    actor: &Mailbox,
    report: &ExecutionReport,
    original: &crate::followup::CheckpointTaskBasis,
    review_after_seconds: u32,
    now: i64,
) -> Result<Checked<YieldReviewPlan>> {
    Store::lock_actor(tx, actor).await?;
    let c = &report.correlation;
    ensure!(
        actor.group_name == c.group && matches!(report.kind, ReportKind::Yield),
        "yield_report_scope_or_kind"
    );
    ensure!(
        (1..=3600).contains(&review_after_seconds) && now >= 0,
        "invalid_yield_review_interval"
    );
    ensure!(
        !report.key.is_empty()
            && report.key.len() <= 256
            && !report.summary.trim().is_empty()
            && report.summary.len() <= 4096
            && report.evidence.len() <= 32
            && report
                .evidence
                .iter()
                .all(|e| !e.is_empty() && e.len() <= 256),
        "invalid_execution_report"
    );
    let report_bytes = canonical(report)?;
    ensure!(report_bytes.len() <= 8192, "execution_report_too_large");
    reserve(tx, &c.group).await?;
    let a = attempt(tx, c).await?;
    ensure!(
        a.owner == actor.name && a.owner_binding == actor.binding_version,
        "attempt_report_authority"
    );
    if let Checked::Held(codes) =
        validate_current_attempt_tx(tx, runtime, c, CurrentUse::Report, now).await?
    {
        return Ok(Checked::Held(codes));
    }
    let fresh = crate::followup::checkpoint_task_basis_tx(tx, actor, &c.task).await?;
    ensure!(
        &fresh == original
            && original.group_name == c.group
            && original.task == c.task
            && original.actor == actor.id
            && original.binding_version == actor.binding_version,
        "yield_admitted_checkpoint_basis_changed"
    );
    let review_at = now
        .checked_add(i64::from(review_after_seconds))
        .context("yield_review_time_overflow")?
        .min(original.escalate_at)
        .min(effective_deadline(tx, &c.group, &c.task).await?);
    if review_at <= now {
        cause(
            tx,
            &c.group,
            &c.task,
            "yield_review_boundary",
            "No future review remains within original bounds",
            now,
        )
        .await?;
        return Ok(Checked::Held(vec!["yield_review_boundary".into()]));
    }
    settle_cause(tx, &c.group, &c.task, "yield_review_boundary", now).await?;
    Ok(Checked::Ready(YieldReviewPlan {
        correlation: c.clone(),
        report_bytes,
        original: fresh,
        actor: actor.id,
        actor_binding: actor.binding_version,
        recorded_at: now,
        requested_seconds: review_after_seconds,
        review_at,
    }))
}

/// Read only authentic protected execution history, including after revocation.
/// Cleanup does not require today's checkpoint, actor or judgment permission.
async fn yield_review_fact_tx(
    tx: &mut Tx<'_>,
    c: &Correlation,
) -> Result<Option<(i64, YieldReviewFact)>> {
    let rows:Vec<(i64,String,i64)>=sqlx::query_as("SELECT id,payload,created FROM execution_events WHERE group_name=? AND task=? AND attempt=? AND kind='yield_review' ORDER BY id LIMIT 2")
        .bind(&c.group).bind(&c.task).bind(&c.attempt).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 1, "yield_review_history_ambiguous");
    let Some((id, bytes, created)) = rows.into_iter().next() else {
        return Ok(None);
    };
    let fact: YieldReviewFact = serde_json::from_str(&bytes)?;
    ensure!(
        fact.schema_version == 1
            && fact.correlation == *c
            && fact.report_event > 0
            && fact.checkpoint_history > 0
            && fact.actor > 0
            && fact.actor_binding > 0
            && fact.recorded_at >= 0
            && created == fact.recorded_at
            && fact.original.group_name == c.group
            && fact.original.task == c.task
            && fact.original.actor == fact.actor
            && fact.original.binding_version == fact.actor_binding
            && fact.original.followup > 0
            && fact.original.task_version > 0
            && fact.original.version >= 0
            && fact.original.opened <= fact.recorded_at
            && fact.checkpoint_version >= fact.original.version
            && (1..=3600).contains(&fact.requested_seconds)
            && fact.review_at > fact.recorded_at
            && fact.review_at
                <= fact
                    .recorded_at
                    .checked_add(i64::from(fact.requested_seconds))
                    .context("yield_review_time_overflow")?
            && fact.review_at <= fact.original.escalate_at
            && canonical(&serde_json::to_value(&fact)?)? == bytes,
        "yield_review_history_identity_conflict"
    );
    let report = execution_report_tx(tx, &c.group, &c.task, fact.report_event).await?;
    ensure!(
        report.report.correlation == *c
            && matches!(report.report.kind, ReportKind::Yield)
            && report.recorded_at == fact.recorded_at,
        "yield_review_report_history_conflict"
    );
    let producer = format!("report:{}:{}", fact.actor, fact.actor_binding);
    let receipt = prior::<i64>(
        tx,
        &producer,
        &report.report.key,
        &canonical(&report.report)?,
    )
    .await?;
    ensure!(
        receipt == Some(fact.report_event),
        "yield_review_report_receipt_missing"
    );
    Ok(Some((id, fact)))
}

/// Accepted-component phase: any refusal is an error and MUST roll back the
/// caller's complete report/checkpoint/review transaction. Never commit partial
/// acceptance as Held. The actual checkpoint history is validated after its
/// write, not compared against the now-mutated current followup row.
pub(crate) async fn record_yield_review_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    actor: &Mailbox,
    plan: &YieldReviewPlan,
    report_event: i64,
    checkpoint: &crate::followup::CheckpointWrite,
    now: i64,
) -> Result<YieldReviewReceipt> {
    Store::lock_actor(tx, actor).await?;
    let c = &plan.correlation;
    ensure!(
        actor.group_name == c.group
            && actor.id == plan.actor
            && actor.binding_version == plan.actor_binding,
        "yield_review_actor_changed"
    );
    reserve(tx, &c.group).await?;
    let report = execution_report_tx(tx, &c.group, &c.task, report_event).await?;
    ensure!(
        report.report.correlation == *c
            && canonical(&report.report)? == plan.report_bytes
            && report.report_bytes == canonical(&serde_json::to_value(&report.report)?)?
            && report.recorded_at == plan.recorded_at
            && matches!(report.report.kind, ReportKind::Yield),
        "yield_review_report_conflict"
    );
    let producer = format!("report:{}:{}", actor.id, actor.binding_version);
    ensure!(
        prior::<i64>(tx, &producer, &report.report.key, &plan.report_bytes).await?
            == Some(report_event),
        "yield_review_report_receipt_missing"
    );
    let proof = crate::followup::validate_checkpoint_write_tx(tx, actor, checkpoint).await?;
    let source_matches = matches!(proof.source(),crate::followup::Source::Task {id,version}
        if id==&c.task && *version==plan.original.task_version);
    let requested = proof.checkpoint();
    ensure!(
        source_matches
            && proof.actor() == actor.id
            && proof.followup() == plan.original.followup
            && proof.opened() == plan.original.opened
            && proof.escalate_at() == plan.original.escalate_at
            && proof.recorded_at() == plan.recorded_at
            && proof.next_check_at() == plan.review_at
            && proof.history_id() > 0
            && proof.version() >= plan.original.version
            && requested.version == plan.original.version
            && requested.next_check_at == plan.review_at
            && requested.waiting.is_none()
            && requested.extend_until.is_none()
            && requested.reason.is_none(),
        "yield_review_checkpoint_history_conflict"
    );
    let fact = YieldReviewFact {
        schema_version: 1,
        correlation: c.clone(),
        report_event,
        checkpoint_history: proof.history_id(),
        checkpoint_version: proof.version(),
        original: plan.original.clone(),
        actor: actor.id,
        actor_binding: actor.binding_version,
        recorded_at: plan.recorded_at,
        requested_seconds: plan.requested_seconds,
        review_at: plan.review_at,
    };
    if let Some((event, original)) = yield_review_fact_tx(tx, c).await? {
        ensure!(original == fact, "yield_review_retry_conflict");
        return Ok(original.receipt(event));
    }
    ensure!(
        now >= plan.recorded_at && now < plan.review_at,
        "yield_review_interval_elapsed"
    );
    let a = attempt(tx, c).await?;
    ensure!(
        a.owner == actor.name && a.owner_binding == actor.binding_version,
        "attempt_report_authority"
    );
    let Checked::Ready(_) =
        validate_current_attempt_tx(tx, runtime, c, CurrentUse::Report, now).await?
    else {
        anyhow::bail!("yield_review_current_report_held; roll back accepted components");
    };
    ensure!(
        plan.review_at <= effective_deadline(tx, &c.group, &c.task).await?,
        "yield_review_original_bound_changed"
    );
    let event = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "yield_review",
        serde_json::to_value(&fact)?,
        plan.recorded_at,
    )
    .await?;
    Ok(fact.receipt(event))
}

/// Immutable evidence, including the original attempt inputs. Historical reads
/// remain available after closure or invalidation; this is not a current grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionReportSnapshot {
    pub event: i64,
    pub recorded_at: i64,
    pub admitted: bool,
    pub report_bytes: String,
    pub report: ExecutionReport,
    pub inputs: InputSnapshot,
}

pub(crate) async fn execution_report_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    id: i64,
) -> Result<ExecutionReportSnapshot> {
    reserve(tx, group).await?;
    let (attempt_id,payload,recorded_at): (String,String,i64) = sqlx::query_as("SELECT attempt,payload,created FROM execution_events WHERE id=? AND group_name=? AND task=? AND kind='reported'")
        .bind(id).bind(group).bind(task).fetch_one(&mut **tx).await.context("execution_report_missing")?;
    let report: ExecutionReport = serde_json::from_str(&payload)?;
    ensure!(
        report.correlation.group == group
            && report.correlation.task == task
            && report.correlation.attempt == attempt_id,
        "execution_report_correlation_conflict"
    );
    let a = attempt(tx, &report.correlation).await?;
    ensure!(a.admitted, "execution_report_admission_missing");
    Ok(ExecutionReportSnapshot {
        event: id,
        recorded_at,
        admitted: a.admitted,
        report_bytes: payload,
        report,
        inputs: serde_json::from_str(&a.inputs)?,
    })
}

/// Ledger observations for the progress owner. Selecting evidence here does not
/// accept a milestone; the progress owner must validate its persisted judgment
/// and current authority in the same transaction before using these facts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionProgressFacts {
    pub task: String,
    pub task_version: i64,
    pub input_epoch: i64,
    pub first_business_eligible_at: Option<i64>,
    pub anchor_event: Option<i64>,
    pub anchor_at: Option<i64>,
    pub effective_now: i64,
    pub clock_hold: bool,
    pub lifecycle_ready: bool,
    pub closed_admitted_segments_since_anchor: i64,
    pub latest_execution_event: i64,
    pub current_attempt: Option<Correlation>,
    pub budgets: Vec<Account>,
}

/// A milestone report's own segment is the segment containing progress; only
/// later closed admitted segments count against its boundary. Late acceptance
/// retains the report timestamp/fence. Missing or stale evidence is an error,
/// never an invented zero count or fresh anchor. Currently supports report
/// evidence; other evidence kinds require their own concrete owner integration.
pub(crate) async fn execution_progress_facts_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    anchor_report: Option<i64>,
    now: i64,
) -> Result<ExecutionProgressFacts> {
    reserve(tx, group).await?;
    ensure!(now >= 0, "invalid_execution_time");
    let m = model(tx, group, task).await?;
    let e = execution(tx, group, task).await?;
    let mut budgets = Vec::new();
    for id in ancestors(tx, group, task).await? {
        budgets.push(account(tx, group, &id).await?);
    }
    let first = budgets
        .first()
        .context("execution_budget_unavailable")?
        .anchor;
    let (anchor_at, fence) = if let Some(id) = anchor_report {
        let report = execution_report_tx(tx, group, task, id).await?;
        ensure!(
            task_graph::validate_inputs_tx(tx, &report.inputs).await? == InputValidity::Current,
            "progress_evidence_inputs_stale"
        );
        ensure!(
            first.is_some_and(|first| report.recorded_at >= first),
            "progress_evidence_before_eligibility"
        );
        (Some(report.recorded_at), report.report.correlation.fence)
    } else {
        (first, 0)
    };
    let clock: Option<(i64, i64, bool)> = sqlx::query_as(
        "SELECT observed,generation,discontinuity FROM execution_clock WHERE group_name=?",
    )
    .bind(group)
    .fetch_optional(&mut **tx)
    .await?;
    let (observed, generation, discontinuity) = clock.unwrap_or((now, 0, false));
    let clock_hold = now < observed
        || (discontinuity && e.clock_ack < generation)
        || anchor_at.is_some_and(|at| now < at);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT id FROM execution_attempts WHERE group_name=? AND task=? AND state='closed' AND admitted=1 AND fence>? LIMIT 10001)")
        .bind(group).bind(task).bind(fence).fetch_one(&mut **tx).await?;
    ensure!(count <= 10_000, "progress_ledger_inventory_limit");
    let latest: i64 = sqlx::query_scalar(
        "SELECT coalesce(max(id),0) FROM execution_events WHERE group_name=? AND task=?",
    )
    .bind(group)
    .bind(task)
    .fetch_one(&mut **tx)
    .await?;
    let held: Option<Attempt> = sqlx::query_as(
        "SELECT * FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
    )
    .bind(group)
    .bind(task)
    .fetch_optional(&mut **tx)
    .await?;
    let input_epoch: i64 =
        sqlx::query_scalar("SELECT input_epoch FROM task_models WHERE group_name=? AND task=?")
            .bind(group)
            .bind(task)
            .fetch_one(&mut **tx)
            .await?;
    Ok(ExecutionProgressFacts {
        task: task.into(),
        task_version: m.version,
        input_epoch,
        first_business_eligible_at: first,
        anchor_event: anchor_report,
        anchor_at,
        effective_now: now.max(observed),
        clock_hold,
        lifecycle_ready: e.lifecycle_ready,
        closed_admitted_segments_since_anchor: count,
        latest_execution_event: latest,
        current_attempt: held.map(|a| a.correlation()),
        budgets,
    })
}

/// Writer request to stop one exact execution; stop intent is not closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopRequest {
    /// Exact attempt and fence.
    pub correlation: Correlation,
    /// Current business version.
    pub task_version: i64,
    /// Canonical retry identity.
    pub key: String,
    /// Audited writer reason.
    pub reason: String,
}

pub(crate) async fn request_stop_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    request: &StopRequest,
    now: i64,
) -> Result<i64> {
    Store::lock_actor(tx, actor).await?;
    let c = &request.correlation;
    ensure!(actor.group_name == c.group, "stop_group_conflict");
    reserve(tx, &c.group).await?;
    ensure!(
        !request.reason.trim().is_empty() && request.reason.len() <= 4096,
        "stop_reason_required"
    );
    let bytes = canonical(request)?;
    let producer = format!("stop:{}", actor.id);
    if let Some(old) = prior(tx, &producer, &request.key, &bytes).await? {
        return Ok(old);
    }
    let m = model(tx, &c.group, &c.task).await?;
    ensure!(
        m.writer == actor.name && m.version == request.task_version,
        "stop_writer_or_version_conflict"
    );
    let a = attempt(tx, c).await?;
    stop_intent_tx(tx, &a, &request.reason, now).await?;
    let id = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "writer_stop",
        json!(request),
        now,
    )
    .await?;
    receipt(tx, &producer, &request.key, &bytes, &id).await?;
    Ok(id)
}

/// A scheduling correction cannot replenish attempts or change authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRequest {
    /// Contracted task.
    pub task: String,
    /// Current business version.
    pub task_version: i64,
    /// Current execution revision.
    pub execution_revision: i64,
    /// Stable operation key.
    pub key: String,
    /// Audited explanation.
    pub reason: String,
    /// Next review/attempt time, within all original hard limits.
    pub due_at: i64,
}

/// Audited resolution of one task's clock hold; no deadline is changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockResolution {
    /// Task whose writer takes responsibility for the discontinuity.
    pub task: String,
    /// Current business version.
    pub task_version: i64,
    /// Current execution revision.
    pub execution_revision: i64,
    /// Exact observed clock discontinuity generation.
    pub generation: i64,
    /// Retry key.
    pub key: String,
    /// Audited explanation of the clock repair.
    pub reason: String,
}

async fn resolve_clock_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    request: &ClockResolution,
    now: i64,
) -> Result<i64> {
    Store::lock_actor(tx, actor).await?;
    reserve(tx, &actor.group_name).await?;
    ensure!(
        !request.reason.trim().is_empty() && request.reason.len() <= 4096,
        "clock_resolution_reason_required"
    );
    let bytes = canonical(request)?;
    let producer = format!("clock-resolution:{}", actor.id);
    if let Some(old) = prior(tx, &producer, &request.key, &bytes).await? {
        return Ok(old);
    }
    let m = model(tx, &actor.group_name, &request.task).await?;
    let e = execution(tx, &actor.group_name, &request.task).await?;
    ensure!(
        m.writer == actor.name
            && m.version == request.task_version
            && e.revision == request.execution_revision,
        "clock_resolution_writer_or_version_conflict"
    );
    let (observed, generation, discontinuity): (i64, i64, bool) = sqlx::query_as(
        "SELECT observed,generation,discontinuity FROM execution_clock WHERE group_name=?",
    )
    .bind(&actor.group_name)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        now >= observed
            && discontinuity
            && generation == request.generation
            && e.clock_ack < generation,
        "clock_resolution_not_current"
    );
    sqlx::query(
        "UPDATE execution_tasks SET clock_ack=?,revision=revision+1 WHERE group_name=? AND task=?",
    )
    .bind(generation)
    .bind(&actor.group_name)
    .bind(&request.task)
    .execute(&mut **tx)
    .await?;
    sqlx::query("UPDATE execution_clock SET observed=?,discontinuity=CASE WHEN EXISTS(SELECT 1 FROM execution_tasks WHERE group_name=? AND clock_ack<?) THEN 1 ELSE 0 END WHERE group_name=?")
        .bind(now).bind(&actor.group_name).bind(generation).bind(&actor.group_name).execute(&mut **tx).await?;
    settle_cause(
        tx,
        &actor.group_name,
        &request.task,
        "clock_discontinuity",
        now,
    )
    .await?;
    let id = event(
        tx,
        &actor.group_name,
        &request.task,
        None,
        "clock_resolved",
        json!(request),
        now,
    )
    .await?;
    receipt(tx, &producer, &request.key, &bytes, &id).await?;
    Ok(id)
}

async fn schedule_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    request: &ScheduleRequest,
    now: i64,
) -> Result<i64> {
    Store::lock_actor(tx, actor).await?;
    reserve(tx, &actor.group_name).await?;
    ensure!(
        !request.reason.trim().is_empty() && request.reason.len() <= 4096,
        "schedule_reason_required"
    );
    let bytes = canonical(request)?;
    let producer = format!("schedule:{}", actor.id);
    if let Some(old) = prior(tx, &producer, &request.key, &bytes).await? {
        return Ok(old);
    }
    let m = model(tx, &actor.group_name, &request.task).await?;
    let e = execution(tx, &actor.group_name, &request.task).await?;
    ensure!(
        m.writer == actor.name
            && m.version == request.task_version
            && e.revision == request.execution_revision,
        "schedule_writer_or_version_conflict"
    );
    ensure!(
        request.due_at >= now
            && request.due_at < effective_deadline(tx, &actor.group_name, &request.task).await?,
        "schedule_outside_boundary"
    );
    for id in ancestors(tx, &actor.group_name, &request.task).await? {
        if let Some(deadline) = account(tx, &actor.group_name, &id).await?.deadline {
            ensure!(request.due_at < deadline, "schedule_after_lifetime");
        }
    }
    sqlx::query(
        "UPDATE execution_tasks SET revision=revision+1,due_at=? WHERE group_name=? AND task=?",
    )
    .bind(request.due_at)
    .bind(&actor.group_name)
    .bind(&request.task)
    .execute(&mut **tx)
    .await?;
    event(
        tx,
        &actor.group_name,
        &request.task,
        None,
        "scheduled",
        json!(request),
        now,
    )
    .await?;
    let revision = e.revision + 1;
    receipt(tx, &producer, &request.key, &bytes, &revision).await?;
    Ok(revision)
}

/// Bounded scan receipt. A cursor advances only with all derived effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairPage {
    /// Tasks repaired from model events and the fair inventory scan.
    pub tasks: Vec<String>,
    /// Last model event fully accounted for.
    pub model_event: i64,
    /// Exact held correlations requiring runtime reconciliation outside SQLite.
    pub reconcile: Vec<Correlation>,
}

/// Timer repair uses the model inventory, including terminal tasks with cleanup.
/// It never infers safe closure or fabricates an earlier lifecycle anchor.
pub(crate) async fn reconcile_page_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    group: &str,
    now: i64,
) -> Result<RepairPage> {
    reserve(tx, group).await?;
    observe_clock(tx, group, now).await?;
    sqlx::query("INSERT OR IGNORE INTO execution_cursors(group_name) VALUES(?)")
        .bind(group)
        .execute(&mut **tx)
        .await?;
    let (cursor, scan): (i64, i64) =
        sqlx::query_as("SELECT model_event,last_scan FROM execution_cursors WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id,task FROM task_model_events WHERE group_name=? AND id>? ORDER BY id LIMIT ?",
    )
    .bind(group)
    .bind(cursor)
    .bind(PAGE)
    .fetch_all(&mut **tx)
    .await?;
    let mut tasks: BTreeSet<String> = events.iter().map(|(_, task)| task.clone()).collect();
    let fair: Vec<String> = sqlx::query_scalar("SELECT m.task FROM task_models m LEFT JOIN execution_tasks e ON e.group_name=m.group_name AND e.task=m.task WHERE m.group_name=? ORDER BY coalesce(e.scanned,0),m.task LIMIT ?")
        .bind(group).bind(PAGE).fetch_all(&mut **tx).await?;
    tasks.extend(fair);
    let next_scan = scan.checked_add(1).context("execution_scan_exhausted")?;
    let mut reconcile = Vec::new();
    for task in &tasks {
        for id in ancestors(tx, group, task).await? {
            initialize(tx, group, &id, now, false).await?;
        }
        let m = model(tx, group, task).await?;
        let held: Option<Attempt> = sqlx::query_as(
            "SELECT * FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
        )
        .bind(group)
        .bind(task)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(a) = held {
            let inputs: InputSnapshot = serde_json::from_str(&a.inputs)?;
            let current =
                task_graph::validate_inputs_tx(tx, &inputs).await? == InputValidity::Current;
            let holds = readiness(tx, group, task, now, false).await?;
            if terminal(&m.state) || !current || !holds.is_empty() {
                stop_intent_tx(tx, &a, "repair_invalidated", now).await?;
            }
            let current_attempt = attempt(tx, &a.correlation()).await?;
            if current_attempt.holds_slot && current_attempt.reconcile_at <= now {
                // A missing observation requests reconciliation, never a new turn.
                if now >= finite(current_attempt.observed, RECHECK)? {
                    cause(
                        tx,
                        group,
                        task,
                        "execution_uncertain",
                        "Runtime observation is due; original slot remains held",
                        now,
                    )
                    .await?;
                }
                reconcile.push(a.correlation());
                // Discovery does not reserve runtime I/O. The controller moves
                // reconcile_at only for the bounded attempts it will actually
                // inspect; otherwise a partial page could starve later work.
            }
        } else if !terminal(&m.state) {
            let holds = readiness(tx, group, task, now, true).await?;
            if holds.is_empty() {
                let inputs = task_graph::capture_inputs_tx(tx, group, task, Phase::Execute).await?;
                if runtime
                    .target(
                        tx,
                        group,
                        task,
                        &m.owner,
                        inputs.owner_binding_generation,
                        now,
                    )
                    .await?
                    .is_none()
                {
                    cause(
                        tx,
                        group,
                        task,
                        "runtime_unavailable",
                        "No authenticated execution-capable runtime target",
                        now,
                    )
                    .await?;
                } else {
                    settle_cause(tx, group, task, "runtime_unavailable", now).await?;
                }
            }
        }
        sqlx::query("UPDATE execution_causes SET escalated=1,review_at=hard_due WHERE group_name=? AND task=? AND settled=0 AND hard_due<=?")
            .bind(group).bind(task).bind(now).execute(&mut **tx).await?;
        sqlx::query("UPDATE execution_tasks SET scanned=? WHERE group_name=? AND task=?")
            .bind(next_scan)
            .bind(group)
            .bind(task)
            .execute(&mut **tx)
            .await?;
    }
    let model_event = events.last().map_or(cursor, |(id, _)| *id);
    sqlx::query("UPDATE execution_cursors SET model_event=?,last_scan=? WHERE group_name=?")
        .bind(model_event)
        .bind(next_scan)
        .bind(group)
        .execute(&mut **tx)
        .await?;
    Ok(RepairPage {
        tasks: tasks.into_iter().collect(),
        model_event,
        reconcile,
    })
}

async fn inspect_tx(tx: &mut Tx<'_>, group: &str, task: &str) -> Result<ExecutionView> {
    let state: String =
        sqlx::query_scalar("SELECT state FROM work_items WHERE group_name=? AND id=?")
            .bind(group)
            .bind(task)
            .fetch_one(&mut **tx)
            .await?;
    let tracked: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_tasks WHERE group_name=? AND task=?)",
    )
    .bind(group)
    .bind(task)
    .fetch_one(&mut **tx)
    .await?;
    let mut view = ExecutionView {
        task: task.into(),
        revision: None,
        business_state: state,
        attempt: None,
        attempt_state: None,
        causes: vec![],
        budgets: vec![],
        due_at: None,
        lifecycle_ready: false,
    };
    if !tracked {
        return Ok(view);
    }
    let e = execution(tx, group, task).await?;
    view.revision = Some(e.revision);
    view.due_at = Some(e.due_at);
    view.lifecycle_ready = e.lifecycle_ready;
    let held: Option<Attempt> = sqlx::query_as(
        "SELECT * FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1",
    )
    .bind(group)
    .bind(task)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(a) = held {
        view.attempt = Some(a.correlation());
        view.attempt_state = Some(a.state);
    }
    view.causes = sqlx::query_as("SELECT id,revision,code,detail,responsible,review_at,hard_due,escalated FROM execution_causes WHERE group_name=? AND task=? AND settled=0 ORDER BY code,id")
        .bind(group).bind(task).fetch_all(&mut **tx).await?;
    for id in ancestors(tx, group, task).await? {
        view.budgets.push(account(tx, group, &id).await?);
    }
    Ok(view)
}

/// Unsupported hosts retain responsibility without inventing runtime capability.
#[cfg(not(target_os = "linux"))]
pub(crate) async fn hold_missing_driver_tx(
    tx: &mut Tx<'_>,
    group: &str,
    tasks: &[String],
    now: i64,
) -> Result<()> {
    reserve(tx, group).await?;
    ensure!(tasks.len() <= 200, "invalid_driver_page");
    for task in tasks {
        let m = model(tx, group, task).await?;
        let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE group_name=? AND task=? AND holds_slot=1)")
            .bind(group).bind(task).fetch_one(&mut **tx).await?;
        if held || !terminal(&m.state) {
            cause(tx,group,task,"runtime_driver_unavailable","Managed Linux execution is unavailable on this host; original attempts remain held",now).await?;
        }
    }
    Ok(())
}

/// The actual managed driver is present on Linux. Capability checks remain the
/// runtime owner's responsibility; this only clears the prior missing-code hold.
#[cfg(target_os = "linux")]
pub(crate) async fn clear_missing_driver_tx(
    tx: &mut Tx<'_>,
    group: &str,
    tasks: &[String],
    now: i64,
) -> Result<()> {
    reserve(tx, group).await?;
    ensure!(tasks.len() <= 200, "invalid_driver_page");
    for task in tasks {
        settle_cause(tx, group, task, "runtime_driver_unavailable", now).await?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) async fn driver_failure_tx(
    tx: &mut Tx<'_>,
    c: &Correlation,
    detail: &str,
    now: i64,
) -> Result<()> {
    reserve(tx, &c.group).await?;
    let a = attempt(tx, c).await?;
    if a.holds_slot {
        let detail: String = detail.chars().take(2048).collect();
        cause(tx, &c.group, &c.task, "runtime_driver_error", &detail, now).await?;
    }
    Ok(())
}

/// Select only actual due slots and reserve a bounded reconciliation interval.
/// The caller fences its controller and commits this before external I/O.
#[cfg(target_os = "linux")]
pub(crate) async fn reserve_driver_reconciliation_tx(
    tx: &mut Tx<'_>,
    group: &str,
    now: i64,
    limit: u32,
) -> Result<Vec<(Correlation, bool)>> {
    reserve(tx, group).await?;
    ensure!((1..=4).contains(&limit), "invalid_driver_reconcile_limit");
    let rows: Vec<Attempt> = sqlx::query_as("SELECT * FROM execution_attempts WHERE group_name=? AND holds_slot=1 AND reconcile_at<=? ORDER BY reconcile_at,id LIMIT ?")
        .bind(group).bind(now).bind(i64::from(limit)).fetch_all(&mut **tx).await?;
    let mut result = Vec::with_capacity(rows.len());
    for a in rows {
        // Re-evaluate actual current inputs/limits before choosing stop. This
        // includes attempts not selected in the current model inventory page.
        let holds = readiness(tx, group, &a.task, now, false).await?;
        let inputs: InputSnapshot = serde_json::from_str(&a.inputs)?;
        let current = task_graph::validate_inputs_tx(tx, &inputs).await? == InputValidity::Current;
        if !current || !holds.is_empty() || terminal(&model(tx, group, &a.task).await?.state) {
            stop_intent_tx(tx, &a, "driver_invalidated", now).await?;
        }
        let current = attempt(tx, &a.correlation()).await?;
        if !current.holds_slot {
            continue;
        }
        sqlx::query("UPDATE execution_attempts SET reconcile_at=? WHERE id=? AND holds_slot=1")
            .bind(finite(now, RECHECK)?)
            .bind(&a.id)
            .execute(&mut **tx)
            .await?;
        result.push((a.correlation(), current.state == "stop_requested"));
    }
    Ok(result)
}

/// A committed page distinguishes held candidates from the end of this round.
/// `seen` is bounded by the controller's eleven-page round and never authority.
#[cfg(target_os = "linux")]
pub(crate) struct ClaimPage {
    pub offer: Option<DispatchOffer>,
    pub examined: Vec<String>,
    pub next_cursor: String,
    pub has_more: bool,
}
#[cfg(target_os = "linux")]
pub(crate) async fn next_driver_claim_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    group: &str,
    controller: &str,
    now: i64,
    seen: &BTreeSet<String>,
) -> Result<ClaimPage> {
    reserve(tx, group).await?;
    ensure!(seen.len() <= 220, "driver_claim_round_limit");
    sqlx::query("INSERT OR IGNORE INTO execution_cursors(group_name) VALUES(?)")
        .bind(group)
        .execute(&mut **tx)
        .await?;
    let after: String =
        sqlx::query_scalar("SELECT driver_task FROM execution_cursors WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut **tx)
            .await?;
    // One lookahead row says whether another page may exist. Repeated rows mark
    // a wrap of this round, rather than consuming its budget over a tiny set.
    let rows: Vec<(String,i64)> = sqlx::query_as("SELECT e.task,e.revision FROM execution_tasks e JOIN work_items w ON w.group_name=e.group_name AND w.id=e.task WHERE e.group_name=? AND e.due_at<=? AND w.state IN ('open','ready','active') AND NOT EXISTS(SELECT 1 FROM execution_attempts a WHERE a.group_name=e.group_name AND a.task=e.task AND a.holds_slot=1) ORDER BY CASE WHEN e.task>? THEN 0 ELSE 1 END,e.task LIMIT 21")
        .bind(group).bind(now).bind(&after).fetch_all(&mut **tx).await?;
    let available = rows.len();
    let mut page = ClaimPage {
        offer: None,
        examined: Vec::new(),
        next_cursor: after,
        has_more: false,
    };
    for (task, revision) in rows.iter().take(20) {
        if seen.contains(task) {
            return Ok(page);
        }
        sqlx::query("UPDATE execution_cursors SET driver_task=? WHERE group_name=?")
            .bind(task)
            .bind(group)
            .execute(&mut **tx)
            .await?;
        page.next_cursor = task.clone();
        page.examined.push(task.clone());
        let claim = ClaimRequest {
            group: group.into(),
            task: task.clone(),
            revision: *revision,
            key: format!("driver:{controller}:{task}:{revision}"),
        };
        let Checked::Ready(c) = claim_attempt_tx(tx, runtime, &claim, now).await? else {
            continue;
        };
        // Only the exact successful reservation can become an exposure. The
        // controller appends its private receipt in this same transaction.
        if let Checked::Ready(offer) =
            expose_dispatch_tx(tx, runtime, &c, controller, 1, now).await?
        {
            page.offer = Some(offer);
            page.has_more = available > page.examined.len();
            return Ok(page);
        }
    }
    page.has_more = available > page.examined.len()
        && rows
            .get(page.examined.len())
            .is_some_and(|(task, _)| !seen.contains(task));
    Ok(page)
}

/// Existing transport state only requests inspection of the original attempt.
/// No refreshed launch permission, new slot, Active observation or closure.
#[cfg(target_os = "linux")]
pub(crate) async fn defer_existing_dispatch_tx(
    tx: &mut Tx<'_>,
    c: &Correlation,
    now: i64,
) -> Result<()> {
    reserve(tx, &c.group).await?;
    let a = attempt(tx, c).await?;
    if a.holds_slot {
        sqlx::query("UPDATE execution_attempts SET reconcile_at=min(reconcile_at,?) WHERE id=? AND holds_slot=1")
            .bind(now).bind(&a.id).execute(&mut **tx).await?;
    }
    Ok(())
}

impl Store {
    /// Read current accounting without treating the projection as permission.
    /// # Errors
    /// Rejects stale identity, remote groups or a missing task.
    pub async fn execution_inspect(&self, actor: &Mailbox, task: &str) -> Result<ExecutionView> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        reserve(&mut tx, &actor.group_name).await?;
        let view = inspect_tx(&mut tx, &actor.group_name, task).await?;
        tx.commit().await?;
        Ok(view)
    }

    /// Repair one bounded inventory/event page; unavailable runtime remains held.
    /// The central dispatcher composes the internal API with its real gate.
    /// # Errors
    /// Rejects foreign groups, incomplete model graphs or invalid persistence.
    pub async fn execution_reconcile(&self, group: &str, now: i64) -> Result<RepairPage> {
        let mut tx = self.pool().begin().await?;
        let page = reconcile_page_tx(&mut tx, &UnavailableRuntime, group, now).await?;
        tx.commit().await?;
        Ok(page)
    }

    /// Request bounded cleanup of an exact attempt as its business writer.
    /// # Errors
    /// Rejects stale versions, changed retry bytes or unauthorized callers.
    pub async fn execution_stop(
        &self,
        actor: &Mailbox,
        request: &StopRequest,
        now: i64,
    ) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        let result = request_stop_tx(&mut tx, actor, request, now).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Set the next due time without changing limits, holds or attempt ownership.
    /// # Errors
    /// Rejects stale writer/version or a time outside an original hard bound.
    pub async fn execution_schedule(
        &self,
        actor: &Mailbox,
        request: &ScheduleRequest,
        now: i64,
    ) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        let result = schedule_tx(&mut tx, actor, request, now).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Resolve the exact persisted clock hold without resetting any lifetime.
    /// # Errors
    /// Rejects stale writer/version/generation and clocks below the high water mark.
    pub async fn execution_resolve_clock(
        &self,
        actor: &Mailbox,
        request: &ClockResolution,
        now: i64,
    ) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        let result = resolve_clock_tx(&mut tx, actor, request, now).await?;
        tx.commit().await?;
        Ok(result)
    }
}

async fn stop_intent_tx(tx: &mut Tx<'_>, a: &Attempt, reason: &str, now: i64) -> Result<()> {
    if !a.holds_slot {
        return Ok(());
    }
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM execution_dispatches WHERE attempt=?")
            .bind(&a.id)
            .fetch_one(&mut **tx)
            .await?;
    if phase == "prepared" && !a.admitted {
        finish_tx(
            tx,
            a,
            &json!({"kind":"never_exposed","reason":reason}),
            false,
            &BTreeMap::new(),
            now,
        )
        .await?;
    } else {
        sqlx::query("UPDATE execution_attempts SET state='stop_requested',reconcile_at=min(reconcile_at,?) WHERE id=? AND holds_slot=1")
            .bind(now).bind(&a.id).execute(&mut **tx).await?;
        cause(tx, &a.group_name, &a.task, "cleanup_required", reason, now).await?;
        if a.state != "stop_requested" {
            event(
                tx,
                &a.group_name,
                &a.task,
                Some(&a.id),
                "stop_requested",
                json!({"reason":reason}),
                now,
            )
            .await?;
        }
    }
    Ok(())
}

async fn finish_tx(
    tx: &mut Tx<'_>,
    a: &Attempt,
    closure: &Value,
    spent: bool,
    costs: &BTreeMap<String, i64>,
    now: i64,
) -> Result<()> {
    ensure!(a.holds_slot, "attempt_already_closed");
    let charges: Vec<(String, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT account,cost_cap,cost_unit FROM execution_charges WHERE attempt=? AND settled=0",
    )
    .bind(&a.id)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(!charges.is_empty(), "attempt_charges_missing");
    for (id, cap, unit) in charges {
        let actual = if spent {
            unit.as_ref().and_then(|unit| costs.get(unit).copied())
        } else {
            Some(0)
        };
        if let (Some(cap), Some(actual)) = (cap, actual) {
            ensure!(actual >= 0 && actual <= cap, "runtime_cost_cap_violated");
        }
        let unknown = cap.is_some() && actual.is_none();
        if unknown {
            cause(
                tx,
                &a.group_name,
                &a.task,
                "cost_unknown",
                "Final usage remains reserved at its enforced maximum",
                now,
            )
            .await?;
        }
        let release = if unknown { 0 } else { cap.unwrap_or(0) };
        let changed = sqlx::query("UPDATE execution_budgets SET attempts_reserved=attempts_reserved-1,attempts_spent=attempts_spent+?,cost_reserved=cost_reserved-?,cost_spent=cost_spent+?,unknown_cost=unknown_cost+? WHERE group_name=? AND task=? AND attempts_reserved>0 AND cost_reserved>=?")
            .bind(i64::from(spent)).bind(release).bind(actual.unwrap_or(0)).bind(i64::from(unknown)).bind(&a.group_name).bind(&id).bind(release).execute(&mut **tx).await?;
        ensure!(changed.rows_affected() == 1, "attempt_charge_conflict");
        sqlx::query("UPDATE execution_charges SET settled=1,actual_cost=? WHERE attempt=? AND group_name=? AND account=? AND settled=0")
            .bind(actual).bind(&a.id).bind(&a.group_name).bind(id).execute(&mut **tx).await?;
    }
    sqlx::query("UPDATE execution_attempts SET state='closed',holds_slot=0,closed_at=?,closure=? WHERE id=? AND holds_slot=1")
        .bind(now).bind(canonical(closure)?).bind(&a.id).execute(&mut **tx).await?;
    // Never release a replacement or an unrelated runtime's slot.
    let removed = sqlx::query("DELETE FROM execution_slots WHERE runtime_key=? AND attempt=?")
        .bind(&a.runtime_key)
        .bind(&a.id)
        .execute(&mut **tx)
        .await?;
    ensure!(removed.rows_affected() == 1, "attempt_slot_missing");
    sqlx::query("UPDATE execution_dispatches SET phase='settled',revision=revision+1,lease_owner=NULL,lease_until=0 WHERE attempt=?")
        .bind(&a.id).execute(&mut **tx).await?;
    let e = execution(tx, &a.group_name, &a.task).await?;
    let default_due = finite(now, RECHECK)?.min(e.hard_due);
    let due = match yield_review_fact_tx(tx, &a.correlation()).await? {
        Some((_, review)) => default_due.min(review.review_at),
        None => default_due,
    };
    sqlx::query(
        "UPDATE execution_tasks SET revision=revision+1,due_at=? WHERE group_name=? AND task=?",
    )
    .bind(due)
    .bind(&a.group_name)
    .bind(&a.task)
    .execute(&mut **tx)
    .await?;
    settle_cause(tx, &a.group_name, &a.task, "cleanup_required", now).await?;
    settle_cause(tx, &a.group_name, &a.task, "execution_uncertain", now).await?;
    for code in [
        "attempt_not_current",
        "inputs_stale",
        "owner_changed",
        "runtime_not_current",
        "dispatch_reconciliation",
        "observation_unavailable",
        "runtime_driver_error",
    ] {
        settle_cause(tx, &a.group_name, &a.task, code, now).await?;
    }
    event(
        tx,
        &a.group_name,
        &a.task,
        Some(&a.id),
        "closed",
        closure.clone(),
        now,
    )
    .await?;
    Ok(())
}

/// Historical authenticated closure has no current owner/input/clock/budget
/// permission requirement. It only settles this attempt's original allocation.
pub(crate) async fn close_attempt_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    c: &Correlation,
    closure_ref: &str,
    now: i64,
) -> Result<Checked<i64>> {
    reserve(tx, &c.group).await?;
    let bytes = canonical(&(c, closure_ref))?;
    let key = format!("{}:{}", c.attempt, closure_ref);
    if let Some(old) = prior(tx, "runtime-closure", &key, &bytes).await? {
        return Ok(Checked::Ready(old));
    }
    let a = attempt(tx, c).await?;
    let target: RuntimeTarget = serde_json::from_str(&a.runtime)?;
    let Some(closed) = runtime.closed(tx, c, &target, closure_ref).await? else {
        if a.holds_slot {
            sqlx::query("UPDATE execution_attempts SET state='uncertain',reconcile_at=? WHERE id=? AND holds_slot=1")
                .bind(finite(now, RECHECK)?).bind(&a.id).execute(&mut **tx).await?;
            cause(
                tx,
                &c.group,
                &c.task,
                "execution_uncertain",
                "Authenticated complete closure is unavailable",
                now,
            )
            .await?;
        }
        return Ok(Checked::Held(vec!["execution_uncertain".into()]));
    };
    ensure!(
        closed.receipt == closure_ref
            && !closed.effect_set.is_empty()
            && closed.admitted == a.admitted,
        "closure_correlation_conflict"
    );
    ensure!(a.holds_slot, "attempt_already_closed_with_other_receipt");
    // Even a never-admitted exposed dispatch consumes its attempt allowance;
    // only transactionally proven never-exposed cancellation refunds it.
    finish_tx(tx, &a, &json!(closed), true, &closed.costs, now).await?;
    let id = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "closure_receipt",
        json!({"receipt":closure_ref}),
        now,
    )
    .await?;
    receipt(tx, "runtime-closure", &key, &bytes, &id).await?;
    Ok(Checked::Ready(id))
}

/// Authenticated observations append facts; even exit is not complete closure.
pub(crate) async fn record_observation_tx<R: RuntimeGate>(
    tx: &mut Tx<'_>,
    runtime: &R,
    c: &Correlation,
    observation_ref: &str,
    now: i64,
) -> Result<Checked<i64>> {
    reserve(tx, &c.group).await?;
    let a = attempt(tx, c).await?;
    let target: RuntimeTarget = serde_json::from_str(&a.runtime)?;
    let Some(observation) = runtime.observation(tx, c, &target, observation_ref).await? else {
        cause(
            tx,
            &c.group,
            &c.task,
            "observation_unavailable",
            "No authenticated runtime observation",
            now,
        )
        .await?;
        return Ok(Checked::Held(vec!["observation_unavailable".into()]));
    };
    ensure!(
        observation.receipt == observation_ref
            && observation.sequence > 0
            && observation.observed_at >= a.created
            && observation.observed_at <= now,
        "invalid_runtime_observation"
    );
    let bytes = canonical(&(c, &observation))?;
    let key = format!("{}:{}", c.attempt, observation.sequence);
    if let Some(old) = prior(tx, "runtime-observation", &key, &bytes).await? {
        return Ok(Checked::Ready(old));
    }
    let previous: i64 = sqlx::query_scalar(
        "SELECT coalesce(max(sequence),0) FROM execution_observations WHERE attempt=?",
    )
    .bind(&a.id)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO execution_observations(attempt,sequence,receipt,payload) VALUES(?,?,?,?)",
    )
    .bind(&a.id)
    .bind(observation.sequence)
    .bind(observation_ref)
    .bind(canonical(&observation)?)
    .execute(&mut **tx)
    .await?;
    if a.holds_slot && observation.sequence > previous {
        match observation.status {
            ObservationStatus::Active => {
                // An observation cannot restore permission after stop/uncertainty.
                sqlx::query("UPDATE execution_attempts SET observed=max(observed,?),reconcile_at=? WHERE id=?")
                    .bind(observation.observed_at).bind(finite(now, RECHECK)?).bind(&a.id).execute(&mut **tx).await?;
            }
            ObservationStatus::Unknown | ObservationStatus::ExitObserved => {
                sqlx::query("UPDATE execution_attempts SET state=CASE WHEN state='stop_requested' THEN state ELSE 'uncertain' END,reconcile_at=? WHERE id=?")
                    .bind(finite(now, RECHECK)?).bind(&a.id).execute(&mut **tx).await?;
                cause(
                    tx,
                    &c.group,
                    &c.task,
                    "execution_uncertain",
                    "Exit or unknown status requires full lifecycle and effect closure",
                    now,
                )
                .await?;
            }
        }
    }
    let id = event(
        tx,
        &c.group,
        &c.task,
        Some(&c.attempt),
        "runtime_observed",
        json!(observation),
        now,
    )
    .await?;
    receipt(tx, "runtime-observation", &key, &bytes, &id).await?;
    Ok(Checked::Ready(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        states::TaskState,
        task_graph::{
            AuthoritySource, AuthorityState, Authorization, Completion, CostLimit, Criterion,
            TaskCreate, TaskDraft,
        },
        work::WorkDraft,
    };
    use anyhow::bail;

    // Fabricated runtime evidence exists only in this cfg(test) capability
    // fixture. It exercises scheduler guards, not physical runtime containment.
    struct RuntimeFixture {
        current: bool,
        closed: bool,
        cost: Option<i64>,
    }
    impl RuntimeFixture {
        fn healthy() -> Self {
            Self {
                current: true,
                closed: true,
                cost: Some(2),
            }
        }
    }
    impl RuntimeGate for RuntimeFixture {
        async fn target(
            &self,
            _: &mut Tx<'_>,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Option<RuntimeTarget>> {
            Ok(self.current.then(|| RuntimeTarget {
                identity: "fixture-only".into(),
                concurrency_key: "shared-fixture-runtime".into(),
                generation: 1,
                profile: "fixture-only".into(),
                durable_dedupe: true,
                cost_caps: BTreeMap::from([("tokens".into(), 5)]),
            }))
        }
        async fn current(
            &self,
            _: &mut Tx<'_>,
            _: &Correlation,
            _: &RuntimeTarget,
            _: CurrentUse,
            _: i64,
        ) -> Result<bool> {
            Ok(self.current)
        }
        async fn closed(
            &self,
            _: &mut Tx<'_>,
            _: &Correlation,
            _: &RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<ClosedRuntime>> {
            Ok(self.closed.then(|| ClosedRuntime {
                receipt: receipt.into(),
                effect_set: "fixture-complete-effects".into(),
                admitted: true,
                costs: self
                    .cost
                    .map(|cost| BTreeMap::from([("tokens".into(), cost)]))
                    .unwrap_or_default(),
            }))
        }
        async fn observation(
            &self,
            _: &mut Tx<'_>,
            _: &Correlation,
            _: &RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<RuntimeObservation>> {
            Ok(Some(RuntimeObservation {
                receipt: receipt.into(),
                sequence: 1,
                observed_at: 103,
                status: ObservationStatus::ExitObserved,
            }))
        }
    }
    struct Fixture {
        _temp: tempfile::TempDir,
        store: Store,
        writer: Mailbox,
        worker: Mailbox,
    }
    fn draft(id: &str, cost: bool) -> TaskCreate {
        TaskCreate {
            key: format!("create:{id}"),
            reason: "scheduler capability fixture".into(),
            expected_parent_versions: BTreeMap::new(),
            draft: TaskDraft {
                work: WorkDraft {
                    id: id.into(),
                    scope: "fixture artifact".into(),
                    owner: "worker".into(),
                    state: TaskState::Ready,
                    next_action: "produce artifact".into(),
                    deadline: None,
                    evidence: vec![],
                },
                contract: Contract {
                    deliverable: "artifact".into(),
                    criteria: vec![Criterion {
                        id: "artifact".into(),
                        description: "artifact exists".into(),
                    }],
                    allowed_scope: vec!["fixture artifact".into()],
                    completion: Completion::WriterAcceptance,
                    allow_delegation: true,
                    allow_input_invalidation: true,
                    budget: Budget {
                        max_attempts: 4,
                        max_elapsed_seconds: 600,
                        max_cost: cost.then(|| CostLimit {
                            amount: 20,
                            unit: "tokens".into(),
                        }),
                    },
                },
                authorization: Authorization {
                    state: AuthorityState::Authorized,
                    source: AuthoritySource::Direct {
                        authority_ref: "fixture-only".into(),
                    },
                    approved_scope: vec!["fixture artifact".into()],
                    reason: "test-only permission".into(),
                },
                requirements: vec![],
                parent: None,
            },
        }
    }
    impl Fixture {
        async fn new(cost: bool) -> Result<Self> {
            let temp = tempfile::tempdir()?;
            let store = Store::open(temp.path(), true).await?;
            store.enroll("g", None).await?;
            let credential = store.register("g", "writer", false).await?;
            let writer = store.authenticate("g", Some(&credential)).await?;
            let credential = store.register("g", "worker", false).await?;
            let worker = store.authenticate("g", Some(&credential)).await?;
            let fixture = Self {
                _temp: temp,
                store,
                writer,
                worker,
            };
            fixture.create("job", cost).await?;
            Ok(fixture)
        }
        async fn create(&self, id: &str, cost: bool) -> Result<()> {
            self.store
                .task_create(&self.writer, draft(id, cost), 100)
                .await?;
            // Explicit owner-composition simulation: actual model-side atomic
            // hook wiring is independently tested by the model owner.
            let mut tx = self.store.pool().begin().await?;
            sync_model_tx(&mut tx, "g", &[id.into()], 100).await?;
            tx.commit().await?;
            Ok(())
        }
        async fn claim(&self, task: &str, key: &str, now: i64) -> Result<Checked<Correlation>> {
            let mut tx = self.store.pool().begin().await?;
            reserve(&mut tx, "g").await?;
            let revision = execution(&mut tx, "g", task).await?.revision;
            let result = claim_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &ClaimRequest {
                    group: "g".into(),
                    task: task.into(),
                    revision,
                    key: key.into(),
                },
                now,
            )
            .await?;
            tx.commit().await?;
            Ok(result)
        }
        async fn start(&self) -> Result<Correlation> {
            let Checked::Ready(c) = self.claim("job", "first", 100).await? else {
                bail!("fixture claim held")
            };
            let mut tx = self.store.pool().begin().await?;
            assert!(matches!(
                expose_dispatch_tx(&mut tx, &RuntimeFixture::healthy(), &c, "d1", 1, 101).await?,
                Checked::Ready(_)
            ));
            tx.commit().await?;
            let mut tx = self.store.pool().begin().await?;
            assert!(matches!(
                admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 102).await?,
                Checked::Ready(_)
            ));
            tx.commit().await?;
            Ok(c)
        }
    }

    async fn deadline_control_decision(
        f: &Fixture,
        task: &str,
        deadline: i64,
        now: i64,
    ) -> Result<()> {
        use crate::task_graph::{Change, OutcomeChange, TaskDecision};
        let version = f.store.task_inspect(&f.writer, task).await?.work.version;
        f.store
            .task_decide(
                &f.writer,
                task,
                TaskDecision {
                    key: format!("deadline:{task}:{now}"),
                    version,
                    reason: "actual writer business deadline".into(),
                    work_patch: crate::work::WorkPatch {
                        deadline: Some(Some(deadline)),
                        ..Default::default()
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
                now,
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn original_deadline_clamps_task_and_ancestor_business_limits_without_extension()
    -> Result<()> {
        for ancestor in [false, true] {
            let f = Fixture::new(false).await?;
            let task = if ancestor {
                let mut child = draft("child", false);
                child.draft.parent = Some(crate::task_graph::ParentLink {
                    task: "job".into(),
                    required: true,
                    outcome: crate::task_graph::OutcomeKind::Accepted,
                    revision: None,
                });
                child.draft.authorization.source = AuthoritySource::Parent { task: "job".into() };
                child.expected_parent_versions.insert(
                    "job".into(),
                    f.store.task_inspect(&f.writer, "job").await?.work.version,
                );
                f.store.task_create(&f.writer, child, 100).await?;
                "child"
            } else {
                "job"
            };
            deadline_control_decision(&f, "job", 180, 101).await?;
            let Checked::Ready(c) = f.claim(task, "deadline-original", 102).await? else {
                bail!("genuine deadline claim held")
            };
            let request: String =
                sqlx::query_scalar("SELECT request FROM execution_dispatches WHERE attempt=?")
                    .bind(&c.attempt)
                    .fetch_one(f.store.pool())
                    .await?;
            for (current, expected, now) in [(180, 180, 103), (500, 180, 104), (140, 140, 105)] {
                if current != 180 {
                    deadline_control_decision(&f, "job", current, now).await?;
                }
                let mut tx = f.store.pool().begin().await?;
                assert_eq!(
                    original_attempt_deadline_tx(&mut tx, &c).await?.deadline(),
                    expected,
                    "ancestor={ancestor}"
                );
                // Budget-only Runtime queries miss the narrower business limit.
                assert_eq!(account(&mut tx, "g", task).await?.deadline, Some(700));
                tx.rollback().await?;
                let retained: String =
                    sqlx::query_scalar("SELECT request FROM execution_dispatches WHERE attempt=?")
                        .bind(&c.attempt)
                        .fetch_one(f.store.pool())
                        .await?;
                assert_eq!(
                    retained, request,
                    "current limits cannot rewrite original dispatch"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn original_deadline_requires_exact_attempt_correlation() -> Result<()> {
        let f = Fixture::new(false).await?;
        let Checked::Ready(c) = f.claim("job", "deadline-correlation", 100).await? else {
            bail!("claim held")
        };
        for field in 0..5 {
            let mut invalid = c.clone();
            match field {
                0 => invalid.group = "other".into(),
                1 => invalid.task = "other".into(),
                2 => invalid.attempt = "other".into(),
                3 => invalid.fence += 1,
                _ => invalid.dispatch_key = "other".into(),
            }
            let mut tx = f.store.pool().begin().await?;
            assert!(
                original_attempt_deadline_tx(&mut tx, &invalid)
                    .await
                    .is_err()
            );
            tx.rollback().await?;
        }
        let mut tx = f.store.pool().begin().await?;
        assert_eq!(
            original_attempt_deadline_tx(&mut tx, &c).await?.deadline(),
            700
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn original_deadline_rejects_corrupt_original_dispatch() -> Result<()> {
        let f = Fixture::new(false).await?;
        let Checked::Ready(c) = f.claim("job", "deadline-dispatch", 100).await? else {
            bail!("claim held")
        };
        for mutation in [
            "UPDATE execution_dispatches SET request=json_set(request,'$.correlation.dispatch_key','wrong')",
            "UPDATE execution_dispatches SET request=json_set(request,'$.inputs.unexpected',true)",
            "UPDATE execution_dispatches SET request=json_set(request,'$.target.identity','wrong')",
            "UPDATE execution_dispatches SET request=json_set(request,'$.deadline',0)",
        ] {
            let mut tx = f.store.pool().begin().await?;
            // Negative corruption only; rollback restores the actual immutable
            // dispatch and its guard before the next independent mutation.
            sqlx::query("DROP TRIGGER execution_dispatch_immutable")
                .execute(&mut *tx)
                .await?;
            sqlx::query(mutation).execute(&mut *tx).await?;
            assert!(original_attempt_deadline_tx(&mut tx, &c).await.is_err());
            tx.rollback().await?;
        }
        Ok(())
    }

    // These controls use the actual Progress checkpoint core and immutable
    // history. RuntimeFixture remains explicitly test-only physical evidence.
    async fn admitted_checkpoint_fixture()
    -> Result<(Fixture, Correlation, crate::followup::CheckpointTaskBasis)> {
        let f = Fixture::new(false).await?;
        let Checked::Ready(c) = f.claim("job", "yield-original", 100).await? else {
            bail!("claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                "yield-dispatch",
                1,
                101
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        let basis = crate::followup::checkpoint_task_basis_tx(&mut tx, &f.worker, "job").await?;
        assert!(matches!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 102).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        Ok((f, c, basis))
    }
    fn yield_report(c: &Correlation) -> ExecutionReport {
        ExecutionReport {
            correlation: c.clone(),
            key: "actual-yield-report".into(),
            kind: ReportKind::Yield,
            summary: "resume at the bounded original review".into(),
            evidence: vec!["fixture-artifact".into()],
        }
    }
    async fn yield_components(
        tx: &mut Tx<'_>,
        f: &Fixture,
        report: &ExecutionReport,
        plan: &YieldReviewPlan,
    ) -> Result<(i64, crate::followup::CheckpointWrite)> {
        let Checked::Ready(event) = record_report_tx(
            tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            report,
            plan.recorded_at(),
        )
        .await?
        else {
            bail!("report held")
        };
        let checkpoint = crate::followup::checkpoint_tx(
            tx,
            &f.worker,
            crate::followup::Source::Task {
                id: "job".into(),
                version: plan.original.task_version,
            },
            "actual-yield-checkpoint",
            crate::followup::Checkpoint {
                version: plan.original.version,
                next_step: "review original work".into(),
                next_check_at: plan.review_at(),
                waiting: None,
                evidence: vec!["fixture-artifact".into()],
                extend_until: None,
                reason: None,
            },
            plan.recorded_at(),
        )
        .await?;
        Ok((event, checkpoint))
    }
    async fn yield_state(store: &Store) -> Result<Value> {
        let mut tx = store.pool().begin().await?;
        let events: Vec<(i64, String, String, i64)> =
            sqlx::query_as("SELECT id,kind,payload,created FROM execution_events ORDER BY id")
                .fetch_all(&mut *tx)
                .await?;
        let receipts: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT producer,key,canonical,result FROM execution_receipts ORDER BY producer,key",
        )
        .fetch_all(&mut *tx)
        .await?;
        let histories:Vec<(i64,i64,i64,String,String,i64)>=sqlx::query_as("SELECT id,followup,version,canonical,snapshot,created FROM followup_history ORDER BY id").fetch_all(&mut *tx).await?;
        let plans: Vec<(i64, i64, i64, i64, Option<String>)> = sqlx::query_as(
            "SELECT id,version,next_check,escalate_at,checkpoint FROM followups ORDER BY id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let view = inspect_tx(&mut tx, "g", "job").await?;
        tx.rollback().await?;
        Ok(
            json!({"events":events,"receipts":receipts,"history":histories,"plans":plans,"execution":view}),
        )
    }

    #[tokio::test]
    async fn genuine_yield_history_advances_only_closed_original_due_and_replays() -> Result<()> {
        let (f, c, basis) = admitted_checkpoint_fixture().await?;
        let before = f.store.execution_inspect(&f.writer, "job").await?;
        let report = yield_report(&c);
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            30,
            103,
        )
        .await?
        else {
            bail!("prepare held")
        };
        assert_eq!((plan.recorded_at(), plan.review_at()), (103, 133));
        let (event, checkpoint) = yield_components(&mut tx, &f, &report, &plan).await?;
        // The genuine checkpoint changed the current row. Final validation must
        // authenticate immutable history, not reject this successful mutation.
        assert_ne!(
            crate::followup::checkpoint_task_basis_tx(&mut tx, &f.worker, "job").await?,
            basis
        );
        let receipt = record_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &plan,
            event,
            &checkpoint,
            103,
        )
        .await?;
        assert_eq!(receipt.checkpoint_history, checkpoint.history_id());
        assert_eq!(receipt.schema_version, 1);
        let replay = record_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &plan,
            event,
            &checkpoint,
            200,
        )
        .await?;
        assert_eq!(
            serde_json::to_value(&receipt)?,
            serde_json::to_value(replay)?
        );
        assert_eq!(
            yield_review_fact_tx(&mut tx, &c)
                .await?
                .context("actual fact missing")?
                .0,
            receipt.event
        );
        tx.commit().await?;
        let held = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(held.attempt, Some(c.clone()));
        assert_eq!(held.due_at, before.due_at);
        assert_eq!(
            serde_json::to_value(held.budgets)?,
            serde_json::to_value(before.budgets)?
        );
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &c, "yield-closed", 140).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let closed = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(closed.due_at, Some(133));
        assert!(closed.attempt.is_none());
        assert_eq!(
            (
                closed.budgets[0].anchor,
                closed.budgets[0].deadline,
                closed.budgets[0].attempts_spent,
                closed.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 1, 0)
        );
        let Checked::Ready(successor) = f.claim("job", "actual-yield-successor", 140).await? else {
            bail!("original review was not due")
        };
        let before_replay = yield_state(&f.store).await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &c, "yield-closed", 141).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert_eq!(yield_state(&f.store).await?, before_replay);
        assert_eq!(
            f.store.execution_inspect(&f.writer, "job").await?.attempt,
            Some(successor.clone())
        );
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &successor,
                "yield-second",
                1,
                142
            )
            .await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &successor, 143).await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &successor,
                "yield-second-closed",
                144
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","yield-is-not-continuation",175).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_yield_report_has_no_review_or_continuation_authority() -> Result<()> {
        let (f, c, _) = admitted_checkpoint_fixture().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            record_report_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &yield_report(&c),
                103
            )
            .await?,
            Checked::Ready(_)
        ));
        assert!(yield_review_fact_tx(&mut tx, &c).await?.is_none());
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                "raw-yield-closed",
                140
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert_eq!(
            f.store.execution_inspect(&f.writer, "job").await?.due_at,
            Some(170)
        );
        assert!(matches!(
            f.claim("job", "raw-yield-too-soon", 140).await?,
            Checked::Held(_)
        ));
        let Checked::Ready(second) = f.claim("job", "raw-yield-next", 171).await? else {
            bail!("ordinary second claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &second,
                "raw-second",
                1,
                172
            )
            .await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &second, 173).await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &second,
                "raw-second-closed",
                174
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","raw-yield-no-extra-segment",205).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn yield_late_refusal_rolls_back_genuine_components_and_saved_proof_is_invalid()
    -> Result<()> {
        let (f, c, basis) = admitted_checkpoint_fixture().await?;
        let report = yield_report(&c);
        let before = yield_state(&f.store).await?;
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            30,
            103,
        )
        .await?
        else {
            bail!("prepare held")
        };
        let (event, checkpoint) = yield_components(&mut tx, &f, &report, &plan).await?;
        assert!(
            record_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &plan,
                event,
                &checkpoint,
                133
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("yield_review_interval_elapsed")
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        let mut tx = f.store.pool().begin().await?;
        assert!(
            crate::followup::validate_checkpoint_write_tx(&mut tx, &f.worker, &checkpoint)
                .await
                .is_err()
        );
        assert!(
            record_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &plan,
                event,
                &checkpoint,
                104
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        // Actual stop intent between accepted components and the final guard
        // forces the enclosing writer to roll back every component and intent.
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            30,
            104,
        )
        .await?
        else {
            bail!("prepare held")
        };
        let (event, checkpoint) = yield_components(&mut tx, &f, &report, &plan).await?;
        let original = attempt(&mut tx, &c).await?;
        stop_intent_tx(&mut tx, &original, "actual-stop-before-yield-commit", 104).await?;
        assert!(
            record_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &plan,
                event,
                &checkpoint,
                104
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn yield_preparation_checks_original_basis_and_finite_bounds_before_writes() -> Result<()>
    {
        let (f, c, basis) = admitted_checkpoint_fixture().await?;
        let report = yield_report(&c);
        let before = yield_state(&f.store).await?;
        let mut tx = f.store.pool().begin().await?;
        for interval in [0, 3601] {
            assert!(
                prepare_yield_review_tx(
                    &mut tx,
                    &RuntimeFixture::healthy(),
                    &f.worker,
                    &report,
                    &basis,
                    interval,
                    103
                )
                .await
                .is_err()
            );
        }
        let Checked::Ready(minimum) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            1,
            103,
        )
        .await?
        else {
            bail!("minimum held")
        };
        assert_eq!(minimum.review_at(), 104);
        let Checked::Ready(clamped) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            3600,
            103,
        )
        .await?
        else {
            bail!("maximum held")
        };
        assert_eq!(clamped.review_at(), 700.min(basis.escalate_at));
        let unavailable = RuntimeFixture {
            current: false,
            closed: false,
            cost: None,
        };
        assert!(matches!(
            prepare_yield_review_tx(&mut tx, &unavailable, &f.worker, &report, &basis, 30, 103)
                .await?,
            Checked::Held(_)
        ));
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        // Change the genuine followup through its owner core; an old admitted
        // basis must not be refreshed silently even though task version matches.
        let mut tx = f.store.pool().begin().await?;
        crate::followup::checkpoint_tx(
            &mut tx,
            &f.worker,
            crate::followup::Source::Task {
                id: "job".into(),
                version: basis.task_version,
            },
            "intervening-checkpoint",
            crate::followup::Checkpoint {
                version: basis.version,
                next_step: "different genuine review".into(),
                next_check_at: 150,
                waiting: None,
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            103,
        )
        .await?;
        tx.commit().await?;
        let changed = yield_state(&f.store).await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            prepare_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &report,
                &basis,
                30,
                104
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("yield_admitted_checkpoint_basis_changed")
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, changed);
        Ok(())
    }

    #[tokio::test]
    async fn genuine_yield_cannot_delay_the_existing_closure_review() -> Result<()> {
        let (f, c, basis) = admitted_checkpoint_fixture().await?;
        let report = yield_report(&c);
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            3600,
            103,
        )
        .await?
        else {
            bail!("prepare held")
        };
        assert_eq!(plan.review_at(), 700.min(basis.escalate_at));
        let (event, checkpoint) = yield_components(&mut tx, &f, &report, &plan).await?;
        record_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &plan,
            event,
            &checkpoint,
            103,
        )
        .await?;
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        // Historical cleanup is based on the original actual fact and physical
        // closure, even when the current runtime capability is unavailable.
        let retired = RuntimeFixture {
            current: false,
            closed: true,
            cost: Some(2),
        };
        assert!(matches!(
            close_attempt_tx(&mut tx, &retired, &c, "early-closure", 104).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.due_at, Some(134));
        assert_eq!(
            (
                view.budgets[0].anchor,
                view.budgets[0].deadline,
                view.budgets[0].attempts_spent,
                view.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 1, 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn yield_rejects_genuine_wrong_report_or_checkpoint_and_rolls_back() -> Result<()> {
        let (f, c, basis) = admitted_checkpoint_fixture().await?;
        let report = yield_report(&c);
        let before = yield_state(&f.store).await?;
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            30,
            103,
        )
        .await?
        else {
            bail!("prepare held")
        };
        let (_, checkpoint) = yield_components(&mut tx, &f, &report, &plan).await?;
        let mut other = report.clone();
        other.key = "different-real-report".into();
        other.summary = "different immutable report".into();
        let Checked::Ready(wrong) =
            record_report_tx(&mut tx, &RuntimeFixture::healthy(), &f.worker, &other, 103).await?
        else {
            bail!("other report held")
        };
        assert!(
            record_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &plan,
                wrong,
                &checkpoint,
                103
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("yield_review_report_conflict")
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(plan) = prepare_yield_review_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &report,
            &basis,
            30,
            103,
        )
        .await?
        else {
            bail!("prepare held")
        };
        let Checked::Ready(event) =
            record_report_tx(&mut tx, &RuntimeFixture::healthy(), &f.worker, &report, 103).await?
        else {
            bail!("report held")
        };
        let wrong = crate::followup::checkpoint_tx(
            &mut tx,
            &f.worker,
            crate::followup::Source::Task {
                id: "job".into(),
                version: basis.task_version,
            },
            "genuine-different-checkpoint",
            crate::followup::Checkpoint {
                version: basis.version,
                next_step: "different review".into(),
                next_check_at: 134,
                waiting: None,
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            103,
        )
        .await?;
        assert!(
            record_yield_review_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &f.worker,
                &plan,
                event,
                &wrong,
                103
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("yield_review_checkpoint_history_conflict")
        );
        tx.rollback().await?;
        assert_eq!(yield_state(&f.store).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn policy_edit_cannot_remove_an_observed_elapsed_boundary() -> Result<()> {
        let f = Fixture::new(false).await?;
        let mut tx = f.store.pool().begin().await?;
        let version = model(&mut tx, "g", "job").await?.version;
        crate::progress::change_policy_tx(
            &mut tx,
            &f.writer,
            "job",
            &crate::progress::PolicyChange {
                key: "short-boundary".into(),
                task_version: version,
                expected_revision: None,
                reason: "explicit earlier boundary".into(),
                policy: crate::progress::ProgressPolicy {
                    max_segments_without_milestone: 2,
                    max_elapsed_without_milestone: Some(10),
                    milestones: vec![],
                },
            },
            100,
        )
        .await?;
        assert!(
            readiness(&mut tx, "g", "job", 110, true)
                .await?
                .contains(&"no_progress_elapsed".into())
        );
        let original: String = sqlx::query_scalar(
            "SELECT id FROM execution_causes WHERE code='no_progress_elapsed' AND settled=0",
        )
        .fetch_one(&mut *tx)
        .await?;
        crate::progress::change_policy_tx(
            &mut tx,
            &f.writer,
            "job",
            &crate::progress::PolicyChange {
                key: "weaker-policy".into(),
                task_version: version,
                expected_revision: Some(1),
                reason: "policy edit is not continuation".into(),
                policy: crate::progress::ProgressPolicy {
                    max_segments_without_milestone: 10,
                    max_elapsed_without_milestone: None,
                    milestones: vec![],
                },
            },
            111,
        )
        .await?;
        assert!(
            readiness(&mut tx, "g", "job", 112, true)
                .await?
                .contains(&"no_progress_elapsed".into())
        );
        let current: String = sqlx::query_scalar(
            "SELECT id FROM execution_causes WHERE code='no_progress_elapsed' AND settled=0",
        )
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(current, original);
        let budget = account(&mut tx, "g", "job").await?;
        assert_eq!(budget.anchor, Some(100));
        assert_eq!(budget.deadline, Some(700));
        assert_eq!(budget.attempts_spent + budget.attempts_reserved, 0);
        tx.rollback().await?;
        Ok(())
    }

    async fn progress_fixture(elapsed: Option<u64>) -> Result<(Fixture, i64, i64)> {
        let f = Fixture::new(false).await?;
        f.store
            .progress_policy(
                &f.writer,
                "job",
                &crate::progress::PolicyChange {
                    key: "milestone-policy".into(),
                    task_version: 1,
                    expected_revision: None,
                    reason: "actual milestone control".into(),
                    policy: crate::progress::ProgressPolicy {
                        max_segments_without_milestone: 2,
                        max_elapsed_without_milestone: elapsed,
                        milestones: vec![crate::progress::Milestone {
                            id: "artifact".into(),
                            criterion_ids: vec!["artifact".into()],
                            scope_units: vec!["fixture artifact".into()],
                        }],
                    },
                },
                100,
            )
            .await?;
        let first = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(one) = record_report_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &ExecutionReport {
                correlation: first.clone(),
                key: "first-progress-report".into(),
                kind: ReportKind::Result,
                summary: "first artifact".into(),
                evidence: vec!["artifact-one".into()],
            },
            103,
        )
        .await?
        else {
            bail!("first report held")
        };
        close_attempt_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &first,
            "first-close",
            104,
        )
        .await?;
        tx.commit().await?;
        let Checked::Ready(second) = f.claim("job", "second-progress-segment", 135).await? else {
            bail!("second segment held")
        };
        let mut tx = f.store.pool().begin().await?;
        expose_dispatch_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &second,
            "progress-control",
            1,
            135,
        )
        .await?;
        admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &second, 136).await?;
        let Checked::Ready(two) = record_report_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &f.worker,
            &ExecutionReport {
                correlation: second.clone(),
                key: "second-progress-report".into(),
                kind: ReportKind::Result,
                summary: "second artifact".into(),
                evidence: vec!["artifact-two".into()],
            },
            137,
        )
        .await?
        else {
            bail!("second report held")
        };
        close_attempt_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &second,
            "second-close",
            138,
        )
        .await?;
        tx.commit().await?;
        Ok((f, one, two))
    }

    fn qualify(
        key: &str,
        revision: i64,
        report: i64,
        supersedes: Option<i64>,
    ) -> crate::progress::JudgmentRequest {
        crate::progress::JudgmentRequest {
            key: key.into(),
            task_version: 1,
            progress_revision: revision,
            milestone: "artifact".into(),
            judge_grant: None,
            reason: "inspect actual immutable artifact evidence".into(),
            change: crate::progress::JudgmentChange::Qualify { report, supersedes },
        }
    }

    #[tokio::test]
    async fn qualified_progress_replay_supersession_and_revocation_change_real_claim_guard()
    -> Result<()> {
        let (f, one, two) = progress_fixture(None).await?;
        assert!(
            matches!(f.claim("job","unqualified",169).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        let mut tx = f.store.pool().begin().await?;
        let request = qualify("qualify-first", 1, one, None);
        assert!(
            crate::progress::judge_progress_tx(&mut tx, &f.worker, "job", &request, 170)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let approved =
            crate::progress::judge_progress_tx(&mut tx, &f.writer, "job", &request, 170).await?;
        let replay =
            crate::progress::judge_progress_tx(&mut tx, &f.writer, "job", &request, 171).await?;
        assert_eq!(
            (approved.record, approved.revision),
            (replay.record, replay.revision)
        );
        let policy = ExecutionPolicy::default();
        let (_, boundary, facts) = current_progress_tx(&mut tx, "g", "job", &policy, 171).await?;
        assert_eq!(boundary.judgment_record, Some(approved.record));
        assert_eq!(
            (
                facts.anchor_event,
                facts.anchor_at,
                facts.closed_admitted_segments_since_anchor
            ),
            (Some(one), Some(103), 1)
        );
        let revision = execution(&mut tx, "g", "job").await?.revision;
        assert!(matches!(
            claim_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &ClaimRequest {
                    group: "g".into(),
                    task: "job".into(),
                    revision,
                    key: "after-qualified".into()
                },
                171
            )
            .await?,
            Checked::Ready(_)
        ));
        // Roll back the actual successful claim and judgment together before
        // testing supersession on the same two immutable closed segments.
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let first =
            crate::progress::judge_progress_tx(&mut tx, &f.writer, "job", &request, 172).await?;
        let replacement = crate::progress::judge_progress_tx(
            &mut tx,
            &f.writer,
            "job",
            &qualify("qualify-second", first.revision, two, Some(first.record)),
            173,
        )
        .await?;
        let (_, _, facts) = current_progress_tx(&mut tx, "g", "job", &policy, 174).await?;
        assert_eq!(
            (
                facts.anchor_event,
                facts.anchor_at,
                facts.closed_admitted_segments_since_anchor
            ),
            (Some(two), Some(137), 0)
        );
        crate::progress::judge_progress_tx(
            &mut tx,
            &f.writer,
            "job",
            &crate::progress::JudgmentRequest {
                key: "revoke-second".into(),
                task_version: 1,
                progress_revision: replacement.revision,
                milestone: "artifact".into(),
                judge_grant: None,
                reason: "artifact qualification withdrawn".into(),
                change: crate::progress::JudgmentChange::Revoke {
                    judgment: replacement.record,
                },
            },
            175,
        )
        .await?;
        let revision = execution(&mut tx, "g", "job").await?.revision;
        assert!(
            matches!(claim_attempt_tx(&mut tx,&RuntimeFixture::healthy(),&ClaimRequest {group:"g".into(),task:"job".into(),revision,key:"after-revoked".into()},176).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        let (_, _, facts) = current_progress_tx(&mut tx, "g", "job", &policy, 176).await?;
        assert_eq!(
            (
                facts.anchor_event,
                facts.anchor_at,
                facts.closed_admitted_segments_since_anchor
            ),
            (None, Some(100), 2)
        );
        assert_eq!(
            (
                facts.budgets[0].anchor,
                facts.budgets[0].deadline,
                facts.budgets[0].attempts_spent,
                facts.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 2, 0)
        );
        assert_eq!(
            execution_report_tx(&mut tx, "g", "job", one)
                .await?
                .recorded_at,
            103
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn qualified_progress_revalidates_actual_judge_grant_before_claim() -> Result<()> {
        let (f, one, _) = progress_fixture(None).await?;
        let mut grant = crate::task_graph::JudgeGrantDecision {
            key: "judge-grant".into(),
            task_version: 1,
            expected_revision: None,
            reason: "one milestone authority".into(),
            grant: crate::task_graph::JudgeGrant {
                id: "artifact-judge".into(),
                decider: "worker".into(),
                milestone: "artifact".into(),
                criterion_ids: vec!["artifact".into()],
                authority_ref: "test-writer-explicit-grant".into(),
                revoked: false,
            },
        };
        let granted = f
            .store
            .task_judge_grant(&f.writer, "job", grant.clone(), 169)
            .await?;
        let mut request = qualify("qualified-by-decider", 1, one, None);
        request.judge_grant = Some(crate::task_graph::JudgeGrantRef {
            id: grant.grant.id.clone(),
            revision: granted.revision,
        });
        f.store
            .progress_judge(&f.worker, "job", &request, 170)
            .await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(readiness(&mut tx, "g", "job", 171, true).await?.is_empty());
        tx.commit().await?;
        grant.key = "revoke-judge-grant".into();
        grant.expected_revision = Some(granted.revision);
        grant.grant.revoked = true;
        f.store
            .task_judge_grant(&f.writer, "job", grant, 172)
            .await?;
        assert!(
            matches!(f.claim("job","after-judge-revoked",173).await?,Checked::Held(codes) if codes.contains(&"progress_boundary_unavailable".into()))
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(
            (
                view.budgets[0].anchor,
                view.budgets[0].deadline,
                view.budgets[0].attempts_spent,
                view.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 2, 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn late_actual_qualification_does_not_restart_elapsed_boundary() -> Result<()> {
        let (f, one, _) = progress_fixture(Some(70)).await?;
        f.store
            .progress_judge(
                &f.writer,
                "job",
                &qualify("late-qualification", 1, one, None),
                180,
            )
            .await?;
        assert!(
            matches!(f.claim("job","late-claim",181).await?,Checked::Held(codes) if codes.contains(&"no_progress_elapsed".into()))
        );
        let mut tx = f.store.pool().begin().await?;
        let policy: ExecutionPolicy =
            serde_json::from_str(&execution(&mut tx, "g", "job").await?.policy)?;
        let (_, _, facts) = current_progress_tx(&mut tx, "g", "job", &policy, 181).await?;
        assert_eq!(
            (facts.anchor_event, facts.anchor_at),
            (Some(one), Some(103))
        );
        assert_eq!(
            (
                facts.budgets[0].anchor,
                facts.budgets[0].deadline,
                facts.budgets[0].attempts_spent
            ),
            (Some(100), Some(700), 2)
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn actual_authority_change_invalidates_qualified_progress_without_resetting_usage()
    -> Result<()> {
        use crate::task_graph::{Change, OutcomeChange, TaskDecision};
        let (f, one, _) = progress_fixture(None).await?;
        f.store
            .progress_judge(
                &f.writer,
                "job",
                &qualify("before-authority-change", 1, one, None),
                169,
            )
            .await?;
        let before = f.store.task_inspect(&f.writer, "job").await?;
        let mut authority = before.model.context("model missing")?.authorization;
        authority.state = AuthorityState::Held;
        authority.reason = "source writer withdrew scope pending decision".into();
        f.store
            .task_decide(
                &f.writer,
                "job",
                TaskDecision {
                    key: "hold-real-authority".into(),
                    version: before.work.version,
                    reason: "revoke original input authority".into(),
                    work_patch: Default::default(),
                    scope: Change::Keep,
                    contract: Change::Keep,
                    authorization: Change::Set(authority),
                    requirements: Change::Keep,
                    parent: Change::Keep,
                    expected_parent_versions: BTreeMap::new(),
                    clear_invalidation: false,
                    outcome: OutcomeChange::Keep,
                    resolve_message: None,
                },
                170,
            )
            .await?;
        assert!(
            matches!(f.claim("job","after-authority-change",171).await?,Checked::Held(codes) if codes.iter().any(|code| code.starts_with("model:")))
        );
        let mut tx = f.store.pool().begin().await?;
        let original = execution_report_tx(&mut tx, "g", "job", one).await?;
        assert_ne!(
            task_graph::validate_inputs_tx(&mut tx, &original.inputs).await?,
            InputValidity::Current
        );
        assert_eq!(original.recorded_at, 103);
        let budget = account(&mut tx, "g", "job").await?;
        assert_eq!(
            (
                budget.anchor,
                budget.deadline,
                budget.attempts_spent,
                budget.attempts_reserved
            ),
            (Some(100), Some(700), 2, 0)
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn controller_revocation_fences_exposed_worker_without_releasing_its_slot() -> Result<()>
    {
        use crate::execution_driver::{Controller, validate_dispatch_controller_tx};
        let f = Fixture::new(false).await?;
        let controller = Controller::acquire(&f.store, 100).await?;
        let Checked::Ready(c) = f.claim("job", "controller-claim", 100).await? else {
            bail!("claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(offer) = expose_dispatch_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            "controller",
            1,
            101,
        )
        .await?
        else {
            bail!("offer held")
        };
        let permit = controller
            .authorize_dispatch_tx(&mut tx, &offer, 101)
            .await?;
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        validate_dispatch_controller_tx(&mut tx, &c, permit.receipt(), 102).await?;
        tx.commit().await?;
        // The worker has not admitted. Controller shutdown/restart cannot turn
        // the exposed offer into proof that no delayed worker can exist.
        controller
            .finish(&f.store, 103, "revoke-before-physical-exposure")
            .await?;
        drop(controller);
        let successor = Controller::acquire(&f.store, 104).await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            validate_dispatch_controller_tx(&mut tx, &c, permit.receipt(), 105)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.attempt, Some(c));
        assert_eq!(view.budgets[0].attempts_reserved, 1);
        assert_eq!(view.budgets[0].attempts_spent, 0);
        successor.finish(&f.store, 106, "joined").await?;
        Ok(())
    }

    #[tokio::test]
    async fn controller_receipt_expiry_does_not_release_or_refresh_attempt() -> Result<()> {
        use crate::execution_driver::{Controller, validate_dispatch_controller_tx};
        let f = Fixture::new(false).await?;
        let controller = Controller::acquire(&f.store, 100).await?;
        let Checked::Ready(c) = f.claim("job", "expiring-claim", 100).await? else {
            bail!("claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        let Checked::Ready(offer) = expose_dispatch_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            "controller",
            1,
            101,
        )
        .await?
        else {
            bail!("offer held")
        };
        let permit = controller
            .authorize_dispatch_tx(&mut tx, &offer, 101)
            .await?;
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            validate_dispatch_controller_tx(&mut tx, &c, permit.receipt(), 111)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            controller
                .authorize_dispatch_tx(&mut tx, &offer, 112)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.attempt, Some(c));
        assert_eq!(view.budgets[0].attempts_reserved, 1);
        controller.finish(&f.store, 113, "joined").await?;
        Ok(())
    }

    #[tokio::test]
    async fn controller_inventory_cursor_survives_restart_and_preserves_paused_choice() -> Result<()>
    {
        use crate::execution_driver::Controller;
        let f = Fixture::new(false).await?;
        f.store.enroll("z", None).await?;
        let credential = f.store.register("z", "writer", false).await?;
        let writer = f.store.authenticate("z", Some(&credential)).await?;
        f.store.register("z", "worker", false).await?;
        f.store
            .task_create(&writer, draft("later", false), 100)
            .await?;
        sqlx::query("UPDATE groups SET paused=1,auto_prompt=0 WHERE name='g'")
            .execute(f.store.pool())
            .await?;
        let first = Controller::acquire(&f.store, 100).await?;
        let page = first.tick(&f.store, 100).await?;
        assert_eq!(page.group.as_deref(), Some("g"));
        assert_eq!(page.tasks, 1);
        assert!(page.error.is_none());
        first.finish(&f.store, 101, "restart").await?;
        drop(first);
        let next = Controller::acquire(&f.store, 102).await?;
        let page = next.tick(&f.store, 103).await?;
        assert_eq!(page.group.as_deref(), Some("z"));
        assert_eq!(page.tasks, 1);
        assert!(page.error.is_none());
        let choice: (i64, i64) =
            sqlx::query_as("SELECT paused,auto_prompt FROM groups WHERE name='g'")
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!(choice, (1, 0));
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert!(view.causes.iter().any(|c| c.code == "group_paused"));
        assert_eq!(
            (
                view.budgets[0].anchor,
                view.budgets[0].deadline,
                view.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 0)
        );
        next.finish(&f.store, 104, "joined").await?;
        Ok(())
    }

    #[tokio::test]
    async fn controller_timer_repairs_without_a_mail_scan_or_new_mail() -> Result<()> {
        use crate::execution_driver::Controller;
        let f = Fixture::new(false).await?;
        let controller = Controller::acquire(&f.store, crate::now()?).await?;
        let (status, mut observed) = tokio::sync::watch::channel(Value::Null);
        let running = controller.clone();
        let store = f.store.clone();
        let job = tokio::spawn(async move {
            running
                .run(&store, crate::execution_driver::JobKind::Repair, status)
                .await
        });
        let updated =
            tokio::time::timeout(std::time::Duration::from_secs(15), observed.changed()).await;
        job.abort();
        let _joined = job.await;
        controller
            .finish(&f.store, crate::now()?, "timer-control-joined")
            .await?;
        updated??;
        let report = observed.borrow().clone();
        assert_eq!(report["repair"]["group"], "g");
        assert_eq!(report["repair"]["tasks"], 1);
        assert!(report["repair"]["error"].is_null());
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert!(view.attempt.is_none());
        assert_eq!(view.budgets[0].attempts_reserved, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn driver_open_contract_activates_after_real_authorization_and_preserves_ledger()
    -> Result<()> {
        use crate::task_graph::{Change, OutcomeChange, TaskDecision};
        let f = Fixture::new(false).await?;
        // All transitions go through the actual writer CAS API. RuntimeFixture
        // supplies selector capability only: this control never admits a worker
        // or supplies a physical closure/observation.
        async fn decide(
            f: &Fixture,
            task: &str,
            state: TaskState,
            authority: Option<Authorization>,
            now: i64,
        ) -> Result<()> {
            let before = f.store.task_inspect(&f.writer, task).await?;
            let version = before.work.version;
            f.store
                .task_decide(
                    &f.writer,
                    task,
                    TaskDecision {
                        key: format!("open-transition:{task}:{now}"),
                        version,
                        reason: "actual Open activation control".into(),
                        work_patch: crate::work::WorkPatch {
                            state: (before.work.state != state).then_some(state),
                            ..Default::default()
                        },
                        scope: Change::Keep,
                        contract: Change::Keep,
                        authorization: authority.map_or(Change::Keep, Change::Set),
                        requirements: Change::Keep,
                        parent: Change::Keep,
                        expected_parent_versions: BTreeMap::new(),
                        clear_invalidation: false,
                        outcome: OutcomeChange::Keep,
                        resolve_message: None,
                    },
                    now,
                )
                .await?;
            Ok(())
        }
        decide(&f, "job", TaskState::Blocked, None, 100).await?;
        let mut creation = draft("default-open", false);
        creation.draft.work.state = TaskState::Open;
        creation.draft.authorization.state = AuthorityState::Held;
        creation.draft.authorization.reason = "real initial authority hold".into();
        f.store.task_create(&f.writer, creation, 100).await?;
        let controller = crate::execution_driver::Controller::acquire(&f.store, 100).await?;
        let mut tx = f.store.pool().begin().await?;
        assert_eq!(
            crate::execution_driver::Controller::oldest_claim_due_for_test(&mut tx, "g").await?,
            Some(100)
        );
        let held = next_driver_claim_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            "g",
            "open-control",
            100,
            &BTreeSet::new(),
        )
        .await?;
        assert_eq!(held.examined, vec!["default-open"]);
        assert!(
            held.offer.is_none(),
            "Open must not override held authority"
        );
        let initial = account(&mut tx, "g", "default-open").await?;
        assert_eq!(
            (
                initial.anchor,
                initial.deadline,
                initial.attempts_spent,
                initial.attempts_reserved
            ),
            (None, None, 0, 0)
        );
        tx.commit().await?;
        let original = f.store.task_inspect(&f.writer, "default-open").await?;
        let mut authority = original.model.context("contract")?.authorization;
        authority.state = AuthorityState::Authorized;
        authority.reason = "writer clears original authority hold".into();
        // No redundant Ready/Active consumer update: the task stays Open.
        decide(&f, "default-open", TaskState::Open, Some(authority), 101).await?;
        assert_eq!(
            f.store
                .task_inspect(&f.writer, "default-open")
                .await?
                .work
                .state,
            TaskState::Open
        );
        let mut tx = f.store.pool().begin().await?;
        let page = next_driver_claim_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            "g",
            "open-control",
            101,
            &BTreeSet::new(),
        )
        .await?;
        let offer = page
            .offer
            .context("authorized Open task was not selected")?;
        assert_eq!(offer.correlation.task, "default-open");
        controller
            .authorize_dispatch_tx(&mut tx, &offer, 101)
            .await?;
        let ledger = account(&mut tx, "g", "default-open").await?;
        let original = (
            ledger.anchor,
            ledger.deadline,
            ledger.attempts_spent,
            ledger.attempts_reserved,
        );
        assert_eq!(original, (Some(101), Some(701), 0, 1));
        tx.commit().await?;
        for (index, state) in [
            TaskState::Ready,
            TaskState::Active,
            TaskState::Blocked,
            TaskState::Review,
            TaskState::Open,
        ]
        .into_iter()
        .enumerate()
        {
            let now = 102 + i64::try_from(index)?;
            decide(&f, "default-open", state, None, now).await?;
            let mut tx = f.store.pool().begin().await?;
            let ledger = account(&mut tx, "g", "default-open").await?;
            assert_eq!(
                (
                    ledger.anchor,
                    ledger.deadline,
                    ledger.attempts_spent,
                    ledger.attempts_reserved
                ),
                original
            );
            let page = next_driver_claim_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                "g",
                "open-control",
                now,
                &BTreeSet::new(),
            )
            .await?;
            assert!(
                page.offer.is_none() && page.examined.is_empty(),
                "original unresolved slot prevents overlap after any state transition"
            );
            assert!(attempt(&mut tx, &offer.correlation).await?.holds_slot);
            tx.commit().await?;
        }
        controller
            .finish(&f.store, 108, "open selector control joined")
            .await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn driver_claim_scan_passes_a_held_candidate_and_commits_the_real_offer() -> Result<()> {
        let f = Fixture::new(false).await?;
        let mut blocked = draft("a-held", false);
        blocked.draft.authorization.state = AuthorityState::Held;
        blocked.draft.authorization.reason = "actual writer holds this candidate".into();
        f.store.task_create(&f.writer, blocked, 100).await?;
        let controller = crate::execution_driver::Controller::acquire(&f.store, 100).await?;
        let mut tx = f.store.pool().begin().await?;
        reconcile_page_tx(&mut tx, &RuntimeFixture::healthy(), "g", 100).await?;
        let offer = next_driver_claim_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            "g",
            "fixture-controller",
            100,
            &BTreeSet::new(),
        )
        .await?
        .offer
        .context("later ready task was hidden")?;
        assert_eq!(offer.correlation.task, "job");
        let authority = controller
            .authorize_dispatch_tx(&mut tx, &offer, 100)
            .await?;
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        crate::execution_driver::validate_dispatch_controller_tx(
            &mut tx,
            &offer.correlation,
            authority.receipt(),
            101,
        )
        .await?;
        let phase: String =
            sqlx::query_scalar("SELECT phase FROM execution_dispatches WHERE attempt=?")
                .bind(&offer.correlation.attempt)
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(phase, "exposed");
        assert_eq!(account(&mut tx, "g", "a-held").await?.attempts_reserved, 0);
        assert_eq!(account(&mut tx, "g", "job").await?.attempts_reserved, 1);
        tx.rollback().await?;
        controller
            .finish(&f.store, 102, "claim-control-joined")
            .await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bounded_runtime_reservation_does_not_postpone_unselected_due_attempts() -> Result<()> {
        // Separate test-only runtime slots make three simultaneously held
        // attempts. All business, admission and ledger writes are actual APIs.
        struct SeparateSlots;
        impl RuntimeGate for SeparateSlots {
            async fn target(
                &self,
                tx: &mut Tx<'_>,
                group: &str,
                task: &str,
                owner: &str,
                binding: i64,
                now: i64,
            ) -> Result<Option<RuntimeTarget>> {
                let mut target = RuntimeFixture::healthy()
                    .target(tx, group, task, owner, binding, now)
                    .await?;
                if let Some(target) = &mut target {
                    target.concurrency_key = format!("fixture-slot:{task}");
                }
                Ok(target)
            }
            async fn current(
                &self,
                tx: &mut Tx<'_>,
                c: &Correlation,
                target: &RuntimeTarget,
                purpose: CurrentUse,
                now: i64,
            ) -> Result<bool> {
                RuntimeFixture::healthy()
                    .current(tx, c, target, purpose, now)
                    .await
            }
            async fn closed(
                &self,
                tx: &mut Tx<'_>,
                c: &Correlation,
                target: &RuntimeTarget,
                receipt: &str,
            ) -> Result<Option<ClosedRuntime>> {
                RuntimeFixture::healthy()
                    .closed(tx, c, target, receipt)
                    .await
            }
            async fn observation(
                &self,
                tx: &mut Tx<'_>,
                c: &Correlation,
                target: &RuntimeTarget,
                receipt: &str,
            ) -> Result<Option<RuntimeObservation>> {
                RuntimeFixture::healthy()
                    .observation(tx, c, target, receipt)
                    .await
            }
        }
        let f = Fixture::new(false).await?;
        f.create("a", false).await?;
        f.create("b", false).await?;
        let mut tx = f.store.pool().begin().await?;
        for (index, task) in ["a", "b", "job"].into_iter().enumerate() {
            let now = 100 + i64::try_from(index)? * 3;
            let revision = execution(&mut tx, "g", task).await?.revision;
            let Checked::Ready(c) = claim_attempt_tx(
                &mut tx,
                &SeparateSlots,
                &ClaimRequest {
                    group: "g".into(),
                    task: task.into(),
                    revision,
                    key: format!("due:{task}"),
                },
                now,
            )
            .await?
            else {
                bail!("due fixture held")
            };
            expose_dispatch_tx(
                &mut tx,
                &SeparateSlots,
                &c,
                "bounded-controller",
                1,
                now + 1,
            )
            .await?;
            admit_execution_tx(&mut tx, &SeparateSlots, &c, now + 2).await?;
        }
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        let page = reconcile_page_tx(&mut tx, &SeparateSlots, "g", 150).await?;
        assert_eq!(page.reconcile.len(), 3);
        let mut selected = BTreeSet::new();
        for remaining in [2_i64, 1, 0] {
            let reserved = reserve_driver_reconciliation_tx(&mut tx, "g", 150, 1).await?;
            assert_eq!(reserved.len(), 1);
            assert!(selected.insert(reserved[0].0.attempt.clone()));
            let due: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM execution_attempts WHERE holds_slot=1 AND reconcile_at<=150",
            )
            .fetch_one(&mut *tx)
            .await?;
            assert_eq!(due, remaining);
        }
        assert_eq!(selected.len(), 3);
        assert!(
            reserve_driver_reconciliation_tx(&mut tx, "g", 150, 1)
                .await?
                .is_empty()
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn real_recovery_ack_preserves_cause_guard_and_rolls_back_with_case() -> Result<()> {
        let f = Fixture::new(false).await?;
        let mut tx = f.store.pool().begin().await?;
        let revision = execution(&mut tx, "g", "job").await?.revision;
        let result = claim_attempt_tx(
            &mut tx,
            &UnavailableRuntime,
            &ClaimRequest {
                group: "g".into(),
                task: "job".into(),
                revision,
                key: "held-for-real-case".into(),
            },
            100,
        )
        .await?;
        assert!(matches!(result, Checked::Held(_)));
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        let reference = list_execution_causes_tx(&mut tx, "g", "", 100)
            .await?
            .into_iter()
            .next()
            .context("runtime cause missing")?;
        let ExecutionCauseState::Current(before) =
            inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            bail!("cause not current")
        };
        assert!(execution_case_ack_tx(&mut tx, &reference).await?.is_none());
        let case =
            crate::decision_recovery::ensure_execution_case_tx(&mut tx, &reference, 101).await?;
        let proof = crate::decision_recovery::validate_execution_case_tx(
            &mut tx,
            "g",
            case.id,
            case.version,
            &before.guard,
        )
        .await?;
        let ack = ack_execution_decision_tx(&mut tx, &proof, 102).await?;
        assert_eq!(
            ack_execution_decision_tx(&mut tx, &proof, 103)
                .await?
                .ledger_event,
            ack.ledger_event
        );
        let historical = execution_case_ack_tx(&mut tx, &reference)
            .await?
            .context("missing actual ACK history")?;
        assert_eq!(canonical(&historical)?, canonical(&ack)?);
        let ExecutionCauseState::Current(after) =
            inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            bail!("ACK settled cause")
        };
        assert_eq!(before.guard, after.guard);
        assert_eq!(after.handoff_case, Some(case.id.to_string()));
        assert_eq!(
            serde_json::to_value(&before.budgets)?,
            serde_json::to_value(&after.budgets)?
        );
        let links: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_events WHERE kind='decision_case_linked'",
        )
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(links, 1);
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let ExecutionCauseState::Current(rolled_back) =
            inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            bail!("rollback lost source cause")
        };
        assert!(rolled_back.handoff_case.is_none());
        assert!(execution_case_ack_tx(&mut tx, &reference).await?.is_none());
        // Deliberate negative partial-link fixture; no repair is authorized.
        sqlx::query("UPDATE execution_causes SET case_ref='404' WHERE id=?")
            .bind(&reference.cause_generation)
            .execute(&mut *tx)
            .await?;
        assert!(
            execution_case_ack_tx(&mut tx, &reference)
                .await
                .unwrap_err()
                .to_string()
                .contains("execution_case_ack_partial_link")
        );
        assert_eq!(rolled_back.guard, before.guard);
        let cases: i64 =
            sqlx::query_scalar("SELECT count(*) FROM decision_cases WHERE group_name='g'")
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(cases, 0);
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn late_milestone_observation_keeps_original_report_anchor_and_ledger() -> Result<()> {
        let f = Fixture::new(false).await?;
        let c = f.start().await?;
        let runtime = RuntimeFixture::healthy();
        let mut tx = f.store.pool().begin().await?;
        let report = ExecutionReport {
            correlation: c.clone(),
            key: "progress-evidence".into(),
            kind: ReportKind::Result,
            summary: "criterion evidence".into(),
            evidence: vec!["artifact:fixture".into()],
        };
        let Checked::Ready(id) =
            record_report_tx(&mut tx, &runtime, &f.worker, &report, 104).await?
        else {
            bail!("report held")
        };
        assert!(matches!(
            close_attempt_tx(&mut tx, &runtime, &c, "progress-closed", 105).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        let whole = execution_progress_facts_tx(&mut tx, "g", "job", None, 600).await?;
        let selected = execution_progress_facts_tx(&mut tx, "g", "job", Some(id), 600).await?;
        assert_eq!(whole.closed_admitted_segments_since_anchor, 1);
        assert_eq!(selected.closed_admitted_segments_since_anchor, 0);
        assert_eq!(selected.first_business_eligible_at, Some(100));
        assert_eq!(selected.anchor_at, Some(104));
        assert_eq!(selected.budgets[0].attempts_spent, 1);
        assert_eq!(selected.budgets[0].deadline, Some(700));
        assert_eq!(
            execution_report_tx(&mut tx, "g", "job", id)
                .await?
                .report
                .correlation,
            c
        );
        assert!(
            execution_report_tx(&mut tx, "g", "another-task", id)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        sqlx::query(
            "UPDATE task_models SET input_epoch=input_epoch+1 WHERE group_name='g' AND task='job'",
        )
        .execute(&mut *tx)
        .await?;
        assert!(
            execution_progress_facts_tx(&mut tx, "g", "job", Some(id), 601)
                .await
                .is_err()
        );
        // A stale judgment can still show its original historical report.
        assert_eq!(
            execution_report_tx(&mut tx, "g", "job", id)
                .await?
                .recorded_at,
            104
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_concurrent_claims_keep_one_slot_and_one_reservation() -> Result<()> {
        let f = Fixture::new(false).await?;
        let (a, b) = tokio::join!(f.claim("job", "a", 100), f.claim("job", "b", 100));
        let results = [a?, b?];
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Checked::Ready(_)))
                .count(),
            1
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.budgets[0].attempts_reserved, 1);
        assert_eq!(view.budgets[0].attempts_spent, 0);
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_dispatches")
            .fetch_one(f.store.pool())
            .await?;
        assert_eq!(rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_shared_runtime_slot_blocks_other_task() -> Result<()> {
        let f = Fixture::new(false).await?;
        f.create("other", false).await?;
        assert!(matches!(f.claim("job", "a", 100).await?, Checked::Ready(_)));
        assert!(
            matches!(f.claim("other","b",100).await?,Checked::Held(holds) if holds.contains(&"runtime_slot_held".into()))
        );
        let view = f.store.execution_inspect(&f.writer, "other").await?;
        assert_eq!(view.budgets[0].attempts_reserved, 0);
        assert!(
            view.causes
                .iter()
                .any(|c| c.code == "runtime_slot_held" && c.responsible == "writer")
        );
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_prepared_stop_refunds_but_exposed_stop_retains_slot() -> Result<()> {
        let f = Fixture::new(true).await?;
        let Checked::Ready(c) = f.claim("job", "first", 100).await? else {
            bail!("held")
        };
        let request = StopRequest {
            correlation: c.clone(),
            task_version: 1,
            key: "stop".into(),
            reason: "fixture cancellation".into(),
        };
        let first = f.store.execution_stop(&f.writer, &request, 101).await?;
        assert_eq!(
            f.store.execution_stop(&f.writer, &request, 102).await?,
            first
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert!(view.attempt.is_none());
        assert_eq!(
            (
                view.budgets[0].attempts_spent,
                view.budgets[0].attempts_reserved,
                view.budgets[0].cost_reserved
            ),
            (0, 0, 0)
        );
        let Checked::Ready(c) = f.claim("job", "second", 132).await? else {
            bail!("held")
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                "dispatcher",
                1,
                133
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        f.store
            .execution_stop(
                &f.writer,
                &StopRequest {
                    correlation: c.clone(),
                    task_version: 1,
                    key: "stop-exposed".into(),
                    reason: "fixture cancellation".into(),
                },
                134,
            )
            .await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.attempt.as_ref(), Some(&c));
        assert_eq!(view.attempt_state.as_deref(), Some("stop_requested"));
        assert_eq!(view.budgets[0].cost_reserved, 5);
        assert!(matches!(
            f.claim("job", "cannot-retry", 135).await?,
            Checked::Held(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_old_closure_is_valid_after_rebind_but_current_output_is_not() -> Result<()> {
        let f = Fixture::new(true).await?;
        let c = f.start().await?;
        // Test-only binding transition to isolate historical cleanup from
        // current-output guards. This is not a native recovery witness.
        sqlx::query("UPDATE mailboxes SET binding_version=binding_version+1 WHERE group_name='g' AND name='worker'").execute(f.store.pool()).await?;
        let runtime = RuntimeFixture {
            current: false,
            closed: true,
            cost: Some(2),
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            validate_current_attempt_tx(&mut tx, &runtime, &c, CurrentUse::Publish, 103).await?,
            Checked::Held(_)
        ));
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        let closed = close_attempt_tx(&mut tx, &runtime, &c, "closed-one", 104).await?;
        assert!(matches!(closed, Checked::Ready(_)));
        tx.commit().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert!(view.attempt.is_none());
        assert_eq!(
            (
                view.budgets[0].attempts_spent,
                view.budgets[0].attempts_reserved,
                view.budgets[0].cost_spent,
                view.budgets[0].cost_reserved
            ),
            (1, 0, 2, 0)
        );
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            close_attempt_tx(&mut tx, &runtime, &c, "closed-one", 105).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert_eq!(
            f.store.execution_inspect(&f.writer, "job").await?.budgets[0].attempts_spent,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_unknown_effects_or_exit_never_release_slot() -> Result<()> {
        let f = Fixture::new(false).await?;
        let c = f.start().await?;
        let runtime = RuntimeFixture {
            current: true,
            closed: false,
            cost: None,
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            record_observation_tx(&mut tx, &runtime, &c, "exit", 103).await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            close_attempt_tx(&mut tx, &runtime, &c, "unknown", 104).await?,
            Checked::Held(_)
        ));
        tx.commit().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.attempt, Some(c));
        assert_eq!(view.attempt_state.as_deref(), Some("uncertain"));
        assert!(matches!(
            f.claim("job", "retry", 140).await?,
            Checked::Held(_)
        ));
        let mut tx = f.store.pool().begin().await?;
        assert!(guard_success_tx(&mut tx, "g", "job").await.is_err());
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_unknown_cost_stays_reserved_after_complete_effect_closure() -> Result<()> {
        let f = Fixture::new(true).await?;
        let c = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture {
                    current: true,
                    closed: true,
                    cost: None
                },
                &c,
                "closure",
                104
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert!(view.attempt.is_none());
        assert_eq!(view.budgets[0].cost_reserved, 5);
        assert_eq!(view.budgets[0].unknown_cost, 1);
        assert_eq!(view.budgets[0].cost_spent, 0);
        assert!(view.causes.iter().any(|c| c.code == "cost_unknown"));
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_clock_regression_holds_even_after_time_recovers() -> Result<()> {
        let f = Fixture::new(false).await?;
        f.store.execution_reconcile("g", 110).await?;
        assert!(
            matches!(f.claim("job","regressed",109).await?,Checked::Held(holds) if holds.contains(&"clock_discontinuity".into()))
        );
        assert!(
            matches!(f.claim("job","recovered-wall-time",111).await?,Checked::Held(holds) if holds.contains(&"clock_discontinuity".into()))
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.budgets[0].anchor, Some(100));
        assert_eq!(view.budgets[0].deadline, Some(700));
        assert_eq!(view.budgets[0].attempts_reserved, 0);
        f.store
            .execution_resolve_clock(
                &f.writer,
                &ClockResolution {
                    task: "job".into(),
                    task_version: 1,
                    execution_revision: view.revision.unwrap(),
                    generation: 1,
                    key: "resolve-clock".into(),
                    reason: "fixture clock source repaired".into(),
                },
                112,
            )
            .await?;
        assert!(matches!(
            f.claim("job", "resolved", 113).await?,
            Checked::Ready(_)
        ));
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(
            (view.budgets[0].anchor, view.budgets[0].deadline),
            (Some(100), Some(700))
        );
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_final_reserved_attempt_can_report_and_publish() -> Result<()> {
        let f = Fixture::new(false).await?;
        sqlx::query(
            "UPDATE execution_budgets SET max_attempts=1 WHERE group_name='g' AND task='job'",
        )
        .execute(f.store.pool())
        .await?;
        let c = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            validate_current_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                CurrentUse::Publish,
                103
            )
            .await?,
            Checked::Ready(_)
        ));
        let report = ExecutionReport {
            correlation: c,
            key: "report".into(),
            kind: ReportKind::Result,
            summary: "candidate available".into(),
            evidence: vec!["fixture-artifact".into()],
        };
        let first =
            record_report_tx(&mut tx, &RuntimeFixture::healthy(), &f.worker, &report, 103).await?;
        assert_eq!(
            first,
            record_report_tx(&mut tx, &RuntimeFixture::healthy(), &f.worker, &report, 104).await?
        );
        tx.commit().await?;
        assert_eq!(
            f.store.task_inspect(&f.writer, "job").await?.work.version,
            1
        );
        assert_eq!(
            f.store.execution_inspect(&f.writer, "job").await?.budgets[0].attempts_reserved,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_two_closed_segments_require_strategy_without_reset() -> Result<()> {
        let f = Fixture::new(false).await?;
        let c = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &c, "one", 103).await?;
        tx.commit().await?;
        let Checked::Ready(c) = f.claim("job", "second", 134).await? else {
            bail!("held second")
        };
        let mut tx = f.store.pool().begin().await?;
        expose_dispatch_tx(&mut tx, &RuntimeFixture::healthy(), &c, "d", 1, 134).await?;
        admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 135).await?;
        close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &c, "two", 136).await?;
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","third",167).await?,Checked::Held(holds) if holds.contains(&"strategy_decision_required".into()))
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.budgets[0].attempts_spent, 2);
        assert_eq!(view.budgets[0].anchor, Some(100));
        Ok(())
    }

    #[tokio::test]
    async fn real_strategy_grant_is_finite_replayable_and_atomic_with_its_case() -> Result<()> {
        let f = Fixture::new(false).await?;
        let first = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &first, "one", 103).await?;
        tx.commit().await?;
        let Checked::Ready(second) = f.claim("job", "second", 134).await? else {
            bail!("held second")
        };
        let mut tx = f.store.pool().begin().await?;
        expose_dispatch_tx(&mut tx, &RuntimeFixture::healthy(), &second, "d", 1, 134).await?;
        admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &second, 135).await?;
        close_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &second, "two", 136).await?;
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","third",167).await?,Checked::Held(holds) if holds.contains(&"strategy_decision_required".into()))
        );
        let mut tx = f.store.pool().begin().await?;
        let generation: String = sqlx::query_scalar("SELECT id FROM execution_causes WHERE group_name='g' AND task='job' AND code='strategy_decision_required' AND settled=0").fetch_one(&mut *tx).await?;
        let reference = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: generation,
        };
        let ExecutionCauseState::Current(source) =
            inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            bail!("missing source")
        };
        let case =
            crate::decision_recovery::ensure_execution_case_tx(&mut tx, &reference, 168).await?;
        task_graph::refresh_recovery_projection_tx(
            &mut tx,
            "g",
            &BTreeMap::from([(case.id, case.version)]),
        )
        .await?;
        let authority = crate::decision_recovery::validate_case_authority_tx(
            &mut tx,
            &f.writer,
            case.id,
            case.version,
            &source.guard,
        )
        .await?;
        let revision = execution(&mut tx, "g", "job").await?.revision;
        let request = ContinueStrategy {
            key: "one-finite-segment".into(),
            reason: "source writer approves one revised strategy segment".into(),
            execution_revision: revision,
            additional_segments: 1,
            expires_at: 300,
        };
        let Checked::Ready(applied) =
            apply_continue_strategy_tx(&mut tx, &authority, &request, 169).await?
        else {
            bail!("finite grant held")
        };
        validate_applied_disposition_tx(&mut tx, &applied).await?;
        let mut altered = applied.record.envelope.clone();
        altered.allowance.additional_segments = 2;
        assert_eq!(
            validate_strategy_envelope_tx(&mut tx, "g", "job", &altered)
                .await
                .unwrap_err()
                .to_string(),
            "strategy_envelope_receipt_conflict"
        );
        let mut altered = applied.record.envelope.clone();
        altered.allowance.actor_id += 1;
        assert_eq!(
            validate_strategy_envelope_tx(&mut tx, "g", "job", &altered)
                .await
                .unwrap_err()
                .to_string(),
            "strategy_envelope_receipt_conflict"
        );
        let Checked::Ready(replayed) =
            apply_continue_strategy_tx(&mut tx, &authority, &request, 170).await?
        else {
            bail!("grant replay held")
        };
        assert_eq!(applied.execution_event(), replayed.execution_event());
        assert_eq!(applied.record.envelope.allowance.additional_segments, 1);
        let (_, boundary, facts) =
            current_progress_tx(&mut tx, "g", "job", &ExecutionPolicy::default(), 170).await?;
        let progress = ProgressEvidence::from_boundary(&boundary)?;
        assert!(continuation_allows_tx(&mut tx, "g", "job", &facts, &progress, 170).await?);
        assert!(!continuation_allows_tx(&mut tx, "g", "job", &facts, &progress, 300).await?);
        let Checked::Ready(expired_replay) =
            apply_continue_strategy_tx(&mut tx, &authority, &request, 301).await?
        else {
            bail!("historical strategy replay held")
        };
        assert_eq!(expired_replay.execution_event(), applied.execution_event());
        assert_eq!(expired_replay.record.envelope.allowance.expires_at, 300);
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_events WHERE group_name='g' AND task='job' AND kind='strategy_continuation'").fetch_one(&mut *tx).await?;
        assert_eq!(events, 1);
        let after = execution_progress_facts_tx(&mut tx, "g", "job", None, 301).await?;
        assert_eq!(
            (
                after.budgets[0].attempts_spent,
                after.budgets[0].attempts_reserved
            ),
            (2, 0)
        );
        assert!(!continuation_allows_tx(&mut tx, "g", "job", &after, &progress, 301).await?);
        assert_eq!(
            (
                facts.budgets[0].anchor,
                facts.budgets[0].deadline,
                facts.budgets[0].attempts_spent
            ),
            (Some(100), Some(700), 2)
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT continuation FROM execution_tasks WHERE group_name='g' AND task='job'",
        )
        .fetch_one(&mut *tx)
        .await?;
        assert!(raw.is_none());
        assert!(matches!(
            inspect_execution_cause_tx(&mut tx, &reference).await?,
            ExecutionCauseState::Current(_)
        ));
        let cases: i64 =
            sqlx::query_scalar("SELECT count(*) FROM decision_cases WHERE group_name='g'")
                .fetch_one(&mut *tx)
                .await?;
        assert_eq!(cases, 0);
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn actual_recovery_finalizer_grants_only_one_additional_closed_segment() -> Result<()> {
        use crate::task_graph::{
            ActionNode, CandidateDraft, CandidateRequest, Change, CriterionEvidence,
            DecisionAction, DecisionContinuation, DecisionMaterialization,
            DecisionMaterializationRequest, DecisionPolicy, DecisionPolicyDecision,
            DecisionSourceExpectation, OutcomeChange, OutcomeKind, TaskAction, TaskDecision,
        };
        // Actual durable state on both sides of a public transaction failure.
        async fn durable_state(store: &Store) -> Result<BTreeMap<String, String>> {
            let mut result = BTreeMap::new();
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
                "execution_charges",
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
                let query = format!(
                    "SELECT json_group_array(json(row_json)) FROM (SELECT json_object({fields}) AS row_json FROM {table} ORDER BY {order})"
                );
                result.insert(
                    table.into(),
                    sqlx::query_scalar(&query).fetch_one(store.pool()).await?,
                );
            }
            Ok(result)
        }
        let (f, _, _) = progress_fixture(None).await?;
        assert!(
            matches!(f.claim("job","needs-finite-strategy",169).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        let before = f.store.execution_inspect(&f.writer, "job").await?;
        let mut tx = f.store.pool().begin().await?;
        let generation: String = sqlx::query_scalar("SELECT id FROM execution_causes WHERE group_name='g' AND task='job' AND code='strategy_decision_required' AND settled=0")
            .fetch_one(&mut *tx).await?;
        let reference = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: generation,
        };
        let case =
            crate::decision_recovery::ensure_execution_case_tx(&mut tx, &reference, 170).await?;
        task_graph::refresh_recovery_projection_tx(
            &mut tx,
            "g",
            &BTreeMap::from([(case.id, case.version)]),
        )
        .await?;
        tx.commit().await?;
        let source = DecisionSourceExpectation {
            source: case.current_source.source.clone(),
            input_epoch: case.current_source.input_epoch,
            candidate: case.current_source.candidate.clone(),
            outcome: case.current_source.outcome.clone(),
        };
        let mut contract = f
            .store
            .task_inspect(&f.writer, "job")
            .await?
            .model
            .context("actual source contract")?
            .contract;
        contract.allow_delegation = false;
        f.store
            .decision_policy(
                &f.writer,
                DecisionPolicyDecision {
                    key: "finite-segment-policy".into(),
                    expected_revision: None,
                    source: source.clone(),
                    reason: "one actual finite strategy review".into(),
                    policy: DecisionPolicy {
                        id: "finite-segment-review".into(),
                        contract,
                        actions: vec![DecisionAction::ContinueStrategy],
                        reviewer: Some(f.worker.name.clone()),
                        allow_writer_fallback: true,
                        deadline: case.hard_due,
                        authority_ref: "actual source writer consent".into(),
                        revoked: false,
                    },
                },
                170,
            )
            .await?;
        let mut tx = f.store.pool().begin().await?;
        let DecisionMaterialization::Materialized(materialized) =
            task_graph::materialize_decision_task_tx(
                &mut tx,
                "g",
                &DecisionMaterializationRequest {
                    policy: "finite-segment-review".into(),
                    policy_revision: 1,
                    case_id: case.id,
                    case_version: case.version,
                    source,
                },
                170,
            )
            .await?
        else {
            bail!("actual materialization refused")
        };
        tx.commit().await?;
        let decision = materialized.task;
        let blockers:Vec<(String,Option<String>)>=sqlx::query_as("SELECT selector,waiting FROM decision_blockers WHERE group_name='g' AND case_id=? ORDER BY ordinal")
            .bind(case.id).fetch_all(f.store.pool()).await?;
        assert_eq!(blockers.len(), 1);
        assert_eq!(
            serde_json::from_str::<ActionNode>(&blockers[0].0)?,
            ActionNode {
                task: "job".into(),
                action: TaskAction::Execute
            }
        );
        assert_eq!(
            serde_json::from_str::<ActionNode>(blockers[0].1.as_deref().context("actual wait")?)?,
            ActionNode {
                task: decision.clone(),
                action: TaskAction::AcceptResult
            }
        );
        let review = f.store.task_inspect(&f.writer, &decision).await?;
        let candidate = f
            .store
            .task_candidate(
                &f.worker,
                &decision,
                CandidateRequest {
                    version: review.work.version,
                    key: "finite-segment-candidate".into(),
                    candidate: CandidateDraft {
                        revision: "finite-review-v1".into(),
                        summary: "one bounded source segment".into(),
                        criterion_evidence: review
                            .model
                            .context("actual decision contract")?
                            .contract
                            .criteria
                            .iter()
                            .map(|c| CriterionEvidence {
                                criterion_id: c.id.clone(),
                                references: vec!["actual-finite-review".into()],
                            })
                            .collect(),
                        inputs: f
                            .store
                            .task_capture_inputs(
                                &f.worker,
                                &decision,
                                review.work.version,
                                Phase::Accept,
                            )
                            .await?,
                    },
                },
                171,
            )
            .await?;
        let current = f.store.decision_case(&f.writer, case.id).await?;
        let request = DecisionContinuation {
            key: "public-finite-segment".into(),
            case_version: current.version,
            policy_revision: 1,
            continuation: ContinueStrategy {
                key: "case-one-segment".into(),
                reason: "writer commits one bounded strategy change".into(),
                execution_revision: f
                    .store
                    .execution_inspect(&f.writer, "job")
                    .await?
                    .revision
                    .context("source revision")?,
                additional_segments: 1,
                expires_at: 300,
            },
            decision: TaskDecision {
                key: "finite-segment-outcome".into(),
                version: f
                    .store
                    .task_inspect(&f.writer, &decision)
                    .await?
                    .work
                    .version,
                reason: "accept actual finite reviewer candidate".into(),
                work_patch: crate::work::WorkPatch::default(),
                scope: Change::Keep,
                contract: Change::Keep,
                authorization: Change::Keep,
                requirements: Change::Keep,
                parent: Change::Keep,
                expected_parent_versions: BTreeMap::new(),
                clear_invalidation: false,
                outcome: OutcomeChange::Success {
                    kind: OutcomeKind::Accepted,
                    candidate: candidate.id,
                },
                resolve_message: None,
            },
        };
        let unchanged = durable_state(&f.store).await?;
        assert!(
            f.store
                .decision_continue_strategy(&f.worker, &decision, request.clone(), 171)
                .await
                .is_err()
        );
        assert_eq!(durable_state(&f.store).await?, unchanged);
        sqlx::query("CREATE TRIGGER scheduler_reject_case_completion BEFORE UPDATE ON decision_cases WHEN NEW.state='handled' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM execution_events WHERE kind='strategy_continuation') THEN RAISE(ABORT,'scheduler_failure_after_real_continuation') ELSE RAISE(ABORT,'source_not_applied') END; END")
            .execute(f.store.pool()).await?;
        let error = f
            .store
            .decision_continue_strategy(&f.writer, &decision, request.clone(), 171)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("scheduler_failure_after_real_continuation"),
            "{error:#}"
        );
        assert_eq!(durable_state(&f.store).await?, unchanged);
        sqlx::query("DROP TRIGGER scheduler_reject_case_completion")
            .execute(f.store.pool())
            .await?;
        let Checked::Ready(applied) = f
            .store
            .decision_continue_strategy(&f.writer, &decision, request.clone(), 171)
            .await?
        else {
            bail!("public finite grant held")
        };
        assert_eq!(applied.case.state, "handled");
        assert_eq!(applied.case.original_due, case.original_due);
        assert_eq!(applied.case.hard_due, case.hard_due);
        assert_eq!(applied.case.original_source, case.original_source);
        assert_eq!(applied.decision.work.state, TaskState::Accepted);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM operator_obligations WHERE case_id=?"
            )
            .bind(case.id)
            .fetch_one(f.store.pool())
            .await?,
            "handled"
        );
        assert_eq!(
            serde_json::to_value(f.store.execution_inspect(&f.writer, "job").await?.budgets)?,
            serde_json::to_value(before.budgets)?
        );
        // Keep all original clocks and actual successor accounting unchanged.
        let Checked::Ready(third) = f.claim("job", "finite-successor", 172).await? else {
            bail!("finalized strategy did not permit successor")
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &third,
                "finite-control",
                1,
                173
            )
            .await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &third, 174).await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &third,
                "finite-complete",
                175
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","past-finite-allowance",206).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(
            (
                view.budgets[0].anchor,
                view.budgets[0].deadline,
                view.budgets[0].attempts_spent,
                view.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 3, 0)
        );
        assert_eq!(view.business_state, "ready");
        let committed = durable_state(&f.store).await?;
        let replay = f
            .store
            .decision_continue_strategy(&f.writer, &decision, request.clone(), 301)
            .await?;
        assert_eq!(
            serde_json::to_value(replay)?,
            serde_json::to_value(Checked::Ready(applied))?
        );
        assert_eq!(durable_state(&f.store).await?, committed);
        let mut conflicting = request;
        conflicting.continuation.expires_at = 301;
        let error = f
            .store
            .decision_continue_strategy(&f.writer, &decision, conflicting, 301)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("decision_key_conflict"),
            "{error:#}"
        );
        assert_eq!(durable_state(&f.store).await?, committed);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_budget_change_cannot_extend_anchored_deadline_or_erase_usage() -> Result<()>
    {
        let f = Fixture::new(true).await?;
        f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        guard_model_change_tx(
            &mut tx,
            "g",
            "job",
            &Budget {
                max_attempts: 8,
                max_elapsed_seconds: 1000,
                max_cost: Some(CostLimit {
                    amount: 30,
                    unit: "tokens".into(),
                }),
            },
            None,
            103,
        )
        .await?;
        let a = account(&mut tx, "g", "job").await?;
        assert_eq!(
            (a.anchor, a.deadline, a.attempts_reserved, a.cost_reserved),
            (Some(100), Some(700), 1, 5)
        );
        assert!(guard_success_tx(&mut tx, "g", "job").await.is_err());
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_old_closure_replay_cannot_free_successor_slot() -> Result<()> {
        let f = Fixture::new(false).await?;
        let old = f.start().await?;
        let mut tx = f.store.pool().begin().await?;
        close_attempt_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &old,
            "old-closure",
            103,
        )
        .await?;
        tx.commit().await?;
        let Checked::Ready(new) = f.claim("job", "new-attempt", 134).await? else {
            bail!("held")
        };
        assert!(new.fence > old.fence);
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &old,
                "old-closure",
                135
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.attempt, Some(new));
        assert_eq!(view.budgets[0].attempts_reserved, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_repair_keeps_terminal_cleanup_and_rolls_back_cursor_with_effects()
    -> Result<()> {
        let f = Fixture::new(false).await?;
        let c = f.start().await?;
        // Deliberate persisted negative-state fixture models an interrupted
        // outer owner integration. No successful outcome is fabricated.
        sqlx::query(
            "UPDATE work_items SET state='cancelled',open=0 WHERE group_name='g' AND id='job'",
        )
        .execute(f.store.pool())
        .await?;
        let mut tx = f.store.pool().begin().await?;
        let page = reconcile_page_tx(&mut tx, &RuntimeFixture::healthy(), "g", 150).await?;
        assert!(page.reconcile.contains(&c));
        tx.rollback().await?;
        let cursors: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_cursors")
            .fetch_one(f.store.pool())
            .await?;
        assert_eq!(cursors, 0);
        let page = f.store.execution_reconcile("g", 151).await?;
        assert!(page.reconcile.contains(&c));
        let view = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(view.business_state, "cancelled");
        assert_eq!(view.attempt, Some(c));
        assert_eq!(view.attempt_state.as_deref(), Some("stop_requested"));
        assert!(
            view.causes
                .iter()
                .any(|cause| cause.code == "cleanup_required")
        );
        let controller = crate::execution_driver::Controller::acquire(&f.store, 182).await?;
        let page = controller.tick(&f.store, 183).await?;
        assert_eq!(page.runtime_reconciliation_due, 1);
        controller
            .finish(&f.store, 184, "cleanup-controller-joined")
            .await?;
        let after = f.store.execution_inspect(&f.writer, "job").await?;
        assert_eq!(after.attempt, view.attempt);
        assert_eq!(after.budgets[0].attempts_reserved, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_eligible_child_anchors_and_charges_ineligible_parent() -> Result<()> {
        use crate::task_graph::{OutcomeKind, ParentLink};
        let f = Fixture::new(false).await?;
        let mut parent = draft("parent", true);
        parent.draft.work.state = TaskState::Blocked;
        f.store.task_create(&f.writer, parent, 100).await?;
        let mut tx = f.store.pool().begin().await?;
        sync_model_tx(&mut tx, "g", &["parent".into()], 100).await?;
        assert!(account(&mut tx, "g", "parent").await?.anchor.is_none());
        tx.commit().await?;
        let mut child = draft("child", true);
        child.draft.parent = Some(ParentLink {
            task: "parent".into(),
            required: true,
            outcome: OutcomeKind::Accepted,
            revision: None,
        });
        child.expected_parent_versions.insert("parent".into(), 1);
        f.store.task_create(&f.writer, child, 120).await?;
        let mut tx = f.store.pool().begin().await?;
        sync_model_tx(&mut tx, "g", &["parent".into(), "child".into()], 120).await?;
        assert_eq!(account(&mut tx, "g", "parent").await?.anchor, Some(120));
        tx.commit().await?;
        assert!(matches!(
            f.claim("child", "child-attempt", 120).await?,
            Checked::Ready(_)
        ));
        let view = f.store.execution_inspect(&f.writer, "child").await?;
        assert_eq!(view.budgets.len(), 2);
        for account in view.budgets {
            assert_eq!(account.anchor, Some(120));
            assert_eq!(account.attempts_reserved, 1);
            assert_eq!(account.cost_reserved, 5);
        }
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_projection_reads_source_even_when_no_edges_exist() -> Result<()> {
        let f = Fixture::new(false).await?;
        let mut tx = f.store.pool().begin().await?;
        let sources = scheduler_blocking_sources_tx(&mut tx, "g").await?;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source, "continuation:job");
        assert!(sources[0].edges.is_empty());
        tx.commit().await?;
        // Persist an unsupported wait with deliberately absent projection.
        // Neither graph integration nor admission may infer that it is harmless.
        sqlx::query("UPDATE execution_tasks SET continuation='{\"wait_for\":\"other\"}' WHERE group_name='g' AND task='job'").execute(f.store.pool()).await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(scheduler_blocking_sources_tx(&mut tx, "g").await.is_err());
        tx.rollback().await?;
        assert!(f.claim("job", "cannot-ignore-source", 110).await.is_err());
        Ok(())
    }
    // Same body is shipped for the frozen pre-fix source: it must fail on real
    // admission, not on a missing new symbol or compilation/setup failure.
    #[tokio::test]
    async fn replacement_judgment_cannot_admit_the_original_reservation() -> Result<()> {
        let (f, report, _) = progress_fixture(None).await?;
        let first = f
            .store
            .progress_judge(
                &f.writer,
                "job",
                &qualify("basis-first", 1, report, None),
                170,
            )
            .await?;
        let Checked::Ready(c) = f.claim("job", "basis-reservation", 171).await? else {
            bail!("actual qualified claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        assert!(matches!(
            expose_dispatch_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                "basis-control",
                1,
                172
            )
            .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let replacement = f
            .store
            .progress_judge(
                &f.writer,
                "job",
                &qualify(
                    "basis-replacement",
                    first.revision,
                    report,
                    Some(first.record),
                ),
                173,
            )
            .await?;
        assert_ne!(first.record, replacement.record);
        let mut tx = f.store.pool().begin().await?;
        let result = admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 174).await?;
        assert!(
            matches!(result,Checked::Held(ref codes) if codes.contains(&"progress_admission_changed".into())),
            "requalification must not reuse the old reservation: {result:?}"
        );
        let original = attempt(&mut tx, &c).await?;
        assert!(original.holds_slot && !original.admitted);
        let budget = account(&mut tx, "g", "job").await?;
        assert_eq!(
            (
                budget.anchor,
                budget.deadline,
                budget.attempts_spent,
                budget.attempts_reserved
            ),
            (Some(100), Some(700), 2, 1)
        );
        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_slots WHERE attempt=?")
            .bind(&c.attempt)
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(slots, 1);
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn basis_replay_and_revocation_preserve_admitted_cleanup() -> Result<()> {
        let (f, report, _) = progress_fixture(None).await?;
        let judgment = f
            .store
            .progress_judge(
                &f.writer,
                "job",
                &qualify("admitted-basis", 1, report, None),
                170,
            )
            .await?;
        let Checked::Ready(c) = f.claim("job", "admitted-basis-reservation", 171).await? else {
            bail!("qualified claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        expose_dispatch_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            "basis-admit",
            1,
            172,
        )
        .await?;
        let Checked::Ready(admission) =
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 173).await?
        else {
            bail!("genuine current basis held")
        };
        tx.commit().await?;
        f.store
            .progress_judge(
                &f.writer,
                "job",
                &crate::progress::JudgmentRequest {
                    key: "revoke-after-admission".into(),
                    task_version: 1,
                    progress_revision: judgment.revision,
                    milestone: "artifact".into(),
                    judge_grant: None,
                    reason: "qualification withdrawn".into(),
                    change: crate::progress::JudgmentChange::Revoke {
                        judgment: judgment.record,
                    },
                },
                174,
            )
            .await?;
        let mut tx = f.store.pool().begin().await?;
        assert_eq!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 175).await?,
            Checked::Ready(admission)
        );
        assert_eq!(account(&mut tx, "g", "job").await?.attempts_reserved, 1);
        // Current authority is not required for original physical closure.
        assert!(matches!(
            close_attempt_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                &c,
                "basis-original-closure",
                176
            )
            .await?,
            Checked::Ready(_)
        ));
        assert!(!attempt(&mut tx, &c).await?.holds_slot);
        let basis_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_events WHERE attempt=? AND kind='progress_claim_basis'",
        )
        .bind(&c.attempt)
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(basis_count, 1);
        let budget = account(&mut tx, "g", "job").await?;
        assert_eq!(
            (
                budget.anchor,
                budget.deadline,
                budget.attempts_spent,
                budget.attempts_reserved
            ),
            (Some(100), Some(700), 3, 0)
        );
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn missing_legacy_basis_never_becomes_validated_absence() -> Result<()> {
        let f = Fixture::new(false).await?;
        let Checked::Ready(c) = f.claim("job", "legacy-reservation", 100).await? else {
            bail!("claim held")
        };
        let mut tx = f.store.pool().begin().await?;
        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM execution_events WHERE attempt=? AND kind='progress_claim_basis'",
        )
        .bind(&c.attempt)
        .fetch_one(&mut *tx)
        .await?;
        let evidence: ProgressEvidence = serde_json::from_str(&payload)?;
        assert_eq!(
            evidence.basis,
            ProgressBasis {
                judgment_record: None,
                report_event: None
            }
        );
        tx.rollback().await?;
        // Construct only the negative historical-shape fixture. This cannot
        // create positive authority and is never a production correction path.
        sqlx::query("DROP TRIGGER execution_event_immutable_delete")
            .execute(f.store.pool())
            .await?;
        sqlx::query("DELETE FROM execution_events WHERE attempt=? AND kind='progress_claim_basis'")
            .bind(&c.attempt)
            .execute(f.store.pool())
            .await?;
        let mut tx = f.store.pool().begin().await?;
        let held = validate_current_attempt_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            CurrentUse::Dispatch,
            101,
        )
        .await?;
        assert!(
            matches!(held,Checked::Held(codes) if codes.contains(&"progress_admission_unproven".into()))
        );
        // Exact historical claim replay remains the old correlation, not a new
        // basis or a second charge. Fresh physical admission remains held above.
        let receipt:String=sqlx::query_scalar("SELECT canonical FROM execution_receipts WHERE producer='scheduler:g' AND key='legacy-reservation'").fetch_one(&mut *tx).await?;
        let original_request: ClaimRequest = serde_json::from_str(&receipt)?;
        assert_eq!(
            claim_attempt_tx(&mut tx, &RuntimeFixture::healthy(), &original_request, 102).await?,
            Checked::Ready(c.clone())
        );
        let basis_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_events WHERE attempt=? AND kind='progress_claim_basis'",
        )
        .bind(&c.attempt)
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(basis_count, 0);
        assert!(attempt(&mut tx, &c).await?.holds_slot);
        assert_eq!(account(&mut tx, "g", "job").await?.attempts_reserved, 1);
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn same_report_requalification_does_not_transfer_finite_allowance() -> Result<()> {
        let (f, report, _) = progress_fixture(None).await?;
        let first = f
            .store
            .progress_judge(
                &f.writer,
                "job",
                &qualify("finite-basis-first", 1, report, None),
                170,
            )
            .await?;
        let Checked::Ready(c) = f.claim("job", "third-for-finite-basis", 171).await? else {
            bail!("qualified third segment held")
        };
        let mut tx = f.store.pool().begin().await?;
        expose_dispatch_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            "finite-basis",
            1,
            172,
        )
        .await?;
        admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &c, 173).await?;
        close_attempt_tx(
            &mut tx,
            &RuntimeFixture::healthy(),
            &c,
            "finite-basis-close",
            174,
        )
        .await?;
        tx.commit().await?;
        assert!(
            matches!(f.claim("job","finite-basis-boundary",205).await?,Checked::Held(codes) if codes.contains(&"strategy_decision_required".into()))
        );
        let mut tx = f.store.pool().begin().await?;
        let generation:String=sqlx::query_scalar("SELECT id FROM execution_causes WHERE group_name='g' AND task='job' AND code='strategy_decision_required' AND settled=0").fetch_one(&mut *tx).await?;
        let reference = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: generation,
        };
        let ExecutionCauseState::Current(source) =
            inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            bail!("cause missing")
        };
        let case =
            crate::decision_recovery::ensure_execution_case_tx(&mut tx, &reference, 206).await?;
        task_graph::refresh_recovery_projection_tx(
            &mut tx,
            "g",
            &BTreeMap::from([(case.id, case.version)]),
        )
        .await?;
        let authority = crate::decision_recovery::validate_case_authority_tx(
            &mut tx,
            &f.writer,
            case.id,
            case.version,
            &source.guard,
        )
        .await?;
        let request = ContinueStrategy {
            key: "basis-finite-grant".into(),
            reason: "one genuine revised strategy".into(),
            execution_revision: execution(&mut tx, "g", "job").await?.revision,
            additional_segments: 1,
            expires_at: 400,
        };
        let Checked::Ready(applied) =
            apply_continue_strategy_tx(&mut tx, &authority, &request, 207).await?
        else {
            bail!("actual finite source grant held")
        };
        let before = applied
            .record
            .envelope
            .allowance
            .progress_basis
            .clone()
            .context("new grant basis missing")?;
        assert_eq!(before.basis.judgment_record, Some(first.record));
        let next = crate::progress::judge_progress_tx(
            &mut tx,
            &f.writer,
            "job",
            &qualify(
                "finite-basis-replacement",
                first.revision,
                report,
                Some(first.record),
            ),
            208,
        )
        .await?;
        let (_, boundary, facts) =
            current_progress_tx(&mut tx, "g", "job", &ExecutionPolicy::default(), 209).await?;
        let current = ProgressEvidence::from_boundary(&boundary)?;
        assert_eq!(current.basis.report_event, before.basis.report_event);
        assert_eq!(current.basis.judgment_record, Some(next.record));
        assert!(!continuation_allows_tx(&mut tx, "g", "job", &facts, &current, 209).await?);
        assert!(
            readiness(&mut tx, "g", "job", 209, true)
                .await?
                .contains(&"strategy_decision_required".into())
        );
        assert_eq!(
            (
                facts.budgets[0].anchor,
                facts.budgets[0].deadline,
                facts.budgets[0].attempts_spent,
                facts.budgets[0].attempts_reserved
            ),
            (Some(100), Some(700), 3, 0)
        );
        // Exact historical envelope bytes with the optional field absent remain
        // decodable/re-encodable. They supply no new continuation authority.
        let mut old = serde_json::to_value(&applied.record.envelope.allowance)?;
        old.as_object_mut()
            .context("allowance object")?
            .remove("progress_basis");
        let old_bytes = canonical(&old)?;
        let old_allowance: StrategyAllowance = serde_json::from_str(&old_bytes)?;
        assert!(old_allowance.progress_basis.is_none());
        assert_eq!(
            canonical(&serde_json::to_value(&old_allowance)?)?,
            old_bytes
        );
        tx.rollback().await?;
        Ok(())
    }

    // These controls substitute explicitly labeled transport/capability fixtures.
    // All claims, admissions, receipts, slots and charges are actual owner APIs.
    // They never qualify a native target or prove physical closure.
    #[cfg(target_os = "linux")]
    struct IndependentFixtureSlots;
    #[cfg(target_os = "linux")]
    impl RuntimeGate for IndependentFixtureSlots {
        async fn target(
            &self,
            tx: &mut Tx<'_>,
            group: &str,
            task: &str,
            owner: &str,
            binding: i64,
            now: i64,
        ) -> Result<Option<RuntimeTarget>> {
            let mut target = RuntimeFixture::healthy()
                .target(tx, group, task, owner, binding, now)
                .await?;
            if let Some(target) = &mut target {
                target.concurrency_key = format!("test-only:{group}:{task}");
            }
            Ok(target)
        }
        async fn current(
            &self,
            tx: &mut Tx<'_>,
            c: &Correlation,
            target: &RuntimeTarget,
            purpose: CurrentUse,
            now: i64,
        ) -> Result<bool> {
            RuntimeFixture::healthy()
                .current(tx, c, target, purpose, now)
                .await
        }
        async fn closed(
            &self,
            tx: &mut Tx<'_>,
            c: &Correlation,
            target: &RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<ClosedRuntime>> {
            RuntimeFixture::healthy()
                .closed(tx, c, target, receipt)
                .await
        }
        async fn observation(
            &self,
            tx: &mut Tx<'_>,
            c: &Correlation,
            target: &RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<RuntimeObservation>> {
            RuntimeFixture::healthy()
                .observation(tx, c, target, receipt)
                .await
        }
    }
    #[cfg(target_os = "linux")]
    enum ControlMode {
        Admit,
        Existing,
        Pending,
    }
    #[cfg(target_os = "linux")]
    struct ControlIo {
        mode: ControlMode,
        entered: tokio::sync::Notify,
        dispatches: std::sync::Mutex<Vec<Correlation>>,
        reconciliations: std::sync::Mutex<Vec<Correlation>>,
    }
    #[cfg(target_os = "linux")]
    impl ControlIo {
        fn new(mode: ControlMode) -> Self {
            Self {
                mode,
                entered: tokio::sync::Notify::new(),
                dispatches: Default::default(),
                reconciliations: Default::default(),
            }
        }
    }
    #[cfg(target_os = "linux")]
    type ControlFuture<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Checked<T>>> + Send + 'a>>;
    #[cfg(target_os = "linux")]
    impl crate::execution_driver::RuntimeIo for ControlIo {
        fn dispatch<'a>(
            &'a self,
            store: &'a Store,
            offer: &'a DispatchOffer,
            authority: &'a crate::execution_driver::DispatchAuthority,
        ) -> ControlFuture<'a, crate::managed_runtime::ManagedDispatch> {
            Box::pin(async move {
                self.dispatches
                    .lock()
                    .expect("control lock")
                    .push(offer.correlation.clone());
                self.entered.notify_one();
                match self.mode {
                    ControlMode::Pending => std::future::pending().await,
                    ControlMode::Existing => Ok(Checked::Ready(
                        crate::managed_runtime::ManagedDispatch::Existing,
                    )),
                    ControlMode::Admit => {
                        let mut tx = store.pool().begin().await?;
                        let now = crate::now()?;
                        crate::execution_driver::validate_dispatch_controller_tx(
                            &mut tx,
                            &offer.correlation,
                            authority.receipt(),
                            now,
                        )
                        .await?;
                        let admitted = admit_execution_tx(
                            &mut tx,
                            &IndependentFixtureSlots,
                            &offer.correlation,
                            now,
                        )
                        .await?;
                        tx.commit().await?;
                        match admitted {
                            Checked::Ready(_) => Ok(Checked::Ready(
                                crate::managed_runtime::ManagedDispatch::WorkerExposed,
                            )),
                            Checked::Held(codes) => Ok(Checked::Held(codes)),
                        }
                    }
                }
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: &'a Store,
            correlation: &'a Correlation,
            _: bool,
            _: i64,
        ) -> ControlFuture<'a, crate::managed_runtime::ManagedReconciliation> {
            Box::pin(async move {
                self.reconciliations
                    .lock()
                    .expect("control lock")
                    .push(correlation.clone());
                Ok(Checked::Held(vec![
                    "test_transport_has_no_physical_proof".into(),
                ]))
            })
        }
    }
    #[cfg(target_os = "linux")]
    async fn live_controller_store() -> Result<(tempfile::TempDir, Store)> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        Ok((temp, store))
    }
    #[cfg(target_os = "linux")]
    async fn live_group(store: &Store, group: &str) -> Result<Mailbox> {
        store.enroll(group, None).await?;
        let credential = store.register(group, "writer", false).await?;
        store.register(group, "worker", false).await?;
        store.authenticate(group, Some(&credential)).await
    }
    #[cfg(target_os = "linux")]
    async fn live_task(store: &Store, writer: &Mailbox, id: &str, held: bool) -> Result<()> {
        let mut request = draft(id, false);
        if held {
            request.draft.authorization.state = AuthorityState::Held;
            request.draft.authorization.reason =
                "real writer withholding this fixture scope".into();
        }
        store.task_create(writer, request, crate::now()?).await?;
        // Production model hooks initialize the actual scheduler ledger.
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn controller_201_candidates_reach_the_actual_ready_admission() -> Result<()> {
        let (_temp, store) = live_controller_store().await?;
        let writer = live_group(&store, "g").await?;
        for i in 0..201 {
            live_task(&store, &writer, &format!("t{i:03}"), i < 200).await?;
        }
        let controller =
            crate::execution_driver::Controller::acquire(&store, crate::now()?).await?;
        let io = ControlIo::new(ControlMode::Admit);
        let start = std::time::Instant::now();
        let wall = crate::now()?;
        let page = controller
            .claim_round_with(&store, &IndependentFixtureSlots, &io)
            .await?;
        let elapsed = start.elapsed();
        println!(
            "{}",
            json!({"control":"201_candidate_bookkeeping","wall":wall,"elapsed_ms":elapsed.as_millis(),"page":page,"native_qualified":false})
        );
        assert!(page.error.is_none(), "{:?}", page.error);
        assert_eq!(page.pages, 11);
        assert_eq!(page.candidates.len(), 201);
        assert_eq!(page.dispatches, 1);
        assert!(
            elapsed < std::time::Duration::from_secs(25),
            "original 201-record fairness target exceeded: {elapsed:?}"
        );
        let admitted: (String, i64, i64) =
            sqlx::query_as("SELECT task,admitted,holds_slot FROM execution_attempts")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(admitted, ("t200".into(), 1, 1));
        let charged: i64 =
            sqlx::query_scalar("SELECT sum(attempts_reserved) FROM execution_budgets")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(charged, 1);
        let expired: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_budgets WHERE anchor IS NOT NULL AND deadline<=?",
        )
        .bind(crate::now()?)
        .fetch_one(store.pool())
        .await?;
        assert_eq!(expired, 0, "expiry is not successful fairness");
        controller
            .finish(&store, crate::now()?, "control-joined")
            .await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn controller_three_groups_restart_and_inserts_keep_shared_page_budget() -> Result<()> {
        let (_temp, store) = live_controller_store().await?;
        let mut writers = Vec::new();
        for group in ["a", "b", "c"] {
            let writer = live_group(&store, group).await?;
            for i in 0..67 {
                live_task(&store, &writer, &format!("t{i:03}"), i < 66).await?;
            }
            writers.push(writer);
        }
        let controller =
            crate::execution_driver::Controller::acquire(&store, crate::now()?).await?;
        let io = ControlIo::new(ControlMode::Admit);
        let start = std::time::Instant::now();
        let first = controller
            .claim_round_with(&store, &IndependentFixtureSlots, &io)
            .await?;
        assert_eq!(first.pages, 11);
        assert_eq!(first.dispatches, 2);
        assert_eq!(
            first.groups_attempted, 11,
            "page allowance is shared across groups"
        );
        assert_eq!(
            first
                .candidates
                .iter()
                .map(|(g, _)| g.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["a", "b", "c"])
        );
        let cursors: Vec<(String, String)> = sqlx::query_as(
            "SELECT group_name,driver_task FROM execution_cursors ORDER BY group_name",
        )
        .fetch_all(store.pool())
        .await?;
        controller
            .finish(&store, crate::now()?, "restart-control")
            .await?;
        drop(controller);
        let next = crate::execution_driver::Controller::acquire(&store, crate::now()?).await?;
        let retained: Vec<(String, String)> = sqlx::query_as(
            "SELECT group_name,driver_task FROM execution_cursors ORDER BY group_name",
        )
        .fetch_all(store.pool())
        .await?;
        assert_eq!(cursors, retained);
        let second = next
            .claim_round_with(&store, &IndependentFixtureSlots, &io)
            .await?;
        assert_eq!(
            second.candidates.first().map(|(g, _)| g.as_str()),
            Some("c")
        );
        let admitted: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE admitted=1")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(admitted, 3);
        assert!(
            start.elapsed() < std::time::Duration::from_secs(25),
            "three-group original fairness target exceeded"
        );
        live_task(&store, &writers[0], "a-before", false).await?;
        live_task(&store, &writers[2], "z-after", false).await?;
        let mut traces = vec![
            serde_json::to_value(&first)?,
            serde_json::to_value(&second)?,
        ];
        for _ in 0..3 {
            let page = next
                .claim_round_with(&store, &IndependentFixtureSlots, &io)
                .await?;
            assert!(page.pages <= 11 && page.candidates.len() <= 220);
            traces.push(serde_json::to_value(page)?);
            let inserted:i64=sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE task IN ('a-before','z-after') AND admitted=1").fetch_one(store.pool()).await?;
            if inserted == 2 {
                break;
            }
        }
        let inserted:i64=sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE task IN ('a-before','z-after') AND admitted=1").fetch_one(store.pool()).await?;
        assert_eq!(inserted, 2);
        println!(
            "{}",
            json!({"control":"three_group_restart_inserts","elapsed_ms":start.elapsed().as_millis(),"traces":traces,"native_qualified":false})
        );
        next.finish(&store, crate::now()?, "control-joined").await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn controller_existing_defers_original_cleanup_without_nested_io() -> Result<()> {
        let (_temp, store) = live_controller_store().await?;
        let writer = live_group(&store, "g").await?;
        live_task(&store, &writer, "job", false).await?;
        let controller =
            crate::execution_driver::Controller::acquire(&store, crate::now()?).await?;
        let io = ControlIo::new(ControlMode::Existing);
        let page = controller
            .claim_round_with(&store, &IndependentFixtureSlots, &io)
            .await?;
        assert_eq!((page.existing, page.dispatches), (1, 0));
        assert!(io.reconciliations.lock().expect("control lock").is_empty());
        let original = io.dispatches.lock().expect("control lock")[0].clone();
        let mut tx = store.pool().begin().await?;
        let a = attempt(&mut tx, &original).await?;
        assert!(a.holds_slot && !a.admitted && a.reconcile_at <= crate::now()?);
        let before = account(&mut tx, "g", "job").await?;
        stop_intent_tx(&mut tx, &a, "control-stop-before-reconcile", crate::now()?).await?;
        tx.commit().await?;
        let page = controller.reconcile_round_with(&store, &io).await?;
        assert_eq!(page.closed, 0);
        assert_eq!(
            io.reconciliations.lock().expect("control lock").as_slice(),
            std::slice::from_ref(&original)
        );
        let mut tx = store.pool().begin().await?;
        let after = attempt(&mut tx, &original).await?;
        assert!(after.holds_slot && !after.admitted);
        assert_eq!(after.state, "stop_requested");
        assert_eq!(
            serde_json::to_value(account(&mut tx, "g", "job").await?)?,
            serde_json::to_value(before)?
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM execution_attempts")
                .fetch_one(&mut *tx)
                .await?,
            1
        );
        tx.rollback().await?;
        controller
            .finish(&store, crate::now()?, "control-joined")
            .await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn controller_slow_dispatch_leaves_repair_supervision_and_next_target_live() -> Result<()>
    {
        let (_temp, store) = live_controller_store().await?;
        let writer = live_group(&store, "g").await?;
        live_task(&store, &writer, "a-slow", false).await?;
        live_task(&store, &writer, "b-healthy", false).await?;
        let controller =
            crate::execution_driver::Controller::acquire(&store, crate::now()?).await?;
        let slow = ControlIo::new(ControlMode::Pending);
        let start = std::time::Instant::now();
        let independent = async {
            slow.entered.notified().await;
            let started = std::time::Instant::now();
            let repair = controller.tick(&store, crate::now()?).await?;
            let supervision = controller
                .tick_job(&store, crate::execution_driver::JobKind::Supervise)
                .await?;
            assert_eq!(repair.pages, 1, "{repair:?}");
            assert_eq!(supervision.pages, 1, "{supervision:?}");
            assert!(started.elapsed() < std::time::Duration::from_secs(10));
            let blocked = controller
                .claim_round_with(&store, &IndependentFixtureSlots, &slow)
                .await?;
            assert_eq!(
                blocked.pages, 0,
                "dispatch capacity cannot queue expiring receipts"
            );
            Ok::<_, anyhow::Error>((repair, supervision))
        };
        let (claim, maintenance) = tokio::join!(
            controller.claim_round_with(&store, &IndependentFixtureSlots, &slow),
            independent
        );
        let claim = claim?;
        let (repair, supervision) = maintenance?;
        assert!(
            start.elapsed() >= std::time::Duration::from_secs(15),
            "actual timer, never a fake advanced clock"
        );
        assert!(claim.runtime_errors.iter().any(|e| e.contains("timed out")));
        let healthy = ControlIo::new(ControlMode::Admit);
        let later = controller
            .claim_round_with(&store, &IndependentFixtureSlots, &healthy)
            .await?;
        assert_eq!(later.dispatches, 1, "{later:?}");
        let held: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE holds_slot=1")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(held, 2, "timed-out original keeps its slot");
        println!(
            "{}",
            json!({"control":"slow_dispatch_independent_jobs","elapsed_ms":start.elapsed().as_millis(),"claim":claim,"repair":repair,"supervision":supervision,"healthy":later,"native_qualified":false})
        );
        controller
            .finish(&store, crate::now()?, "control-joined")
            .await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reconciliation_201_original_slots_remain_fair_and_charged() -> Result<()> {
        // Deterministic ledger control. Times stay inside original lifetime;
        // this does not claim real-time cleanup of 201 physical processes.
        let f = Fixture::new(false).await?;
        for i in 0..200 {
            f.create(&format!("a{i:03}"), false).await?;
        }
        let mut tx = f.store.pool().begin().await?;
        let tasks: Vec<String> =
            sqlx::query_scalar("SELECT task FROM execution_tasks ORDER BY task")
                .fetch_all(&mut *tx)
                .await?;
        let mut originals = BTreeSet::new();
        for task in &tasks {
            let revision = execution(&mut tx, "g", task).await?.revision;
            let Checked::Ready(c) = claim_attempt_tx(
                &mut tx,
                &IndependentFixtureSlots,
                &ClaimRequest {
                    group: "g".into(),
                    task: task.clone(),
                    revision,
                    key: format!("held:{task}"),
                },
                100,
            )
            .await?
            else {
                bail!("fixture claim held")
            };
            assert!(matches!(
                expose_dispatch_tx(
                    &mut tx,
                    &IndependentFixtureSlots,
                    &c,
                    "held-control",
                    1,
                    100
                )
                .await?,
                Checked::Ready(_)
            ));
            assert!(matches!(
                admit_execution_tx(&mut tx, &IndependentFixtureSlots, &c, 100).await?,
                Checked::Ready(_)
            ));
            originals.insert(c.attempt);
        }
        tx.commit().await?;
        // Negative interrupted-state fixtures, never fabricated positive proof.
        sqlx::query("UPDATE execution_attempts SET state='uncertain' WHERE task='a000'")
            .execute(f.store.pool())
            .await?;
        sqlx::query(
            "UPDATE work_items SET state='cancelled',open=0 WHERE group_name='g' AND id='a001'",
        )
        .execute(f.store.pool())
        .await?;
        let mut selected = BTreeSet::new();
        for remaining in (0..201_i64).rev() {
            let mut tx = f.store.pool().begin().await?;
            let next = reserve_driver_reconciliation_tx(&mut tx, "g", 150, 1).await?;
            assert_eq!(next.len(), 1);
            assert!(
                selected.insert(next[0].0.attempt.clone()),
                "same due original repeated before backlog drained"
            );
            let due: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM execution_attempts WHERE holds_slot=1 AND reconcile_at<=150",
            )
            .fetch_one(&mut *tx)
            .await?;
            assert_eq!(due, remaining, "unselected correlations must remain due");
            tx.commit().await?;
        }
        assert_eq!(selected, originals);
        let held: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE holds_slot=1")
                .fetch_one(f.store.pool())
                .await?;
        let reserved: i64 =
            sqlx::query_scalar("SELECT sum(attempts_reserved) FROM execution_budgets")
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!((held, reserved), (201, 201));
        let terminal: String =
            sqlx::query_scalar("SELECT state FROM execution_attempts WHERE task='a001'")
                .fetch_one(f.store.pool())
                .await?;
        assert_eq!(terminal, "stop_requested");
        println!(
            "{}",
            json!({"control":"201_held_original_bookkeeping","selected":selected,"remaining_slots":held,"native_qualified":false,"elapsed_sla_proven":false})
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn fairness_201_round_regression_adapter() -> Result<()> {
        let f = Fixture::new(false).await?;
        for i in 0..200 {
            let mut held = draft(&format!("a{i:03}"), false);
            held.draft.authorization.state = AuthorityState::Held;
            held.draft.authorization.reason = "actual writer holds candidate".into();
            f.store.task_create(&f.writer, held, 100).await?;
        }
        let mut tx = f.store.pool().begin().await?;
        let mut selected = None;
        let mut seen = BTreeSet::new();
        for _ in 0..11 {
            let page = next_driver_claim_tx(
                &mut tx,
                &RuntimeFixture::healthy(),
                "g",
                "round-control",
                100,
                &seen,
            )
            .await?;
            seen.extend(page.examined);
            if page.offer.is_some() {
                selected = page.offer;
                break;
            }
            if !page.has_more {
                break;
            }
        }
        assert_eq!(
            selected
                .as_ref()
                .map(|offer| offer.correlation.task.as_str()),
            Some("job"),
            "actual ready candidate hidden by held pages"
        );
        let offer = selected.context("actual ready offer")?;
        assert!(matches!(
            admit_execution_tx(&mut tx, &RuntimeFixture::healthy(), &offer.correlation, 100)
                .await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        Ok(())
    }
}
