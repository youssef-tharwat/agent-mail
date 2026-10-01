//! Source-authorized recovery and finite decision responsibility.
//!
//! Source inspection never receipts an inbox. Case identity excludes plan/task
//! revisions and runtime bindings. Model, execution and notifier owners supply
//! their own transaction guards; absence of those guards is a visible hold.
use crate::{
    bounded,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};

/// An exact original obligation, independent of attention occurrences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Obligation {
    /// A task at an observed business revision.
    Task {
        /// Same-group task identifier.
        id: String,
        /// Business revision, distinct from the followup plan revision.
        version: i64,
    },
    /// One original delivery, even when other recipients have settled.
    Delivery {
        /// Original message identifier.
        message: i64,
        /// Exact recipient name in the same group.
        recipient: String,
    },
}

/// The existing followup plan; reading this never records retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ObligationPlan {
    /// Stable plan identity.
    pub id: i64,
    /// Plan CAS revision.
    pub version: i64,
    /// Original opening time.
    pub opened: i64,
    /// Effective next assessment time.
    pub next_check: i64,
    /// Effective hard boundary.
    pub escalate_at: i64,
}

/// Current authoritative source facts, without an execution permission claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObligationView {
    /// The exact current source selector.
    pub source: Obligation,
    /// Stable identity that excludes task and plan versions.
    pub source_key: String,
    /// Original immutable writer or sender.
    pub authority: String,
    /// Original authority mailbox identity.
    pub authority_id: i64,
    /// Source authority registration; absence never erases responsibility.
    pub authority_registered: bool,
    /// Current task owner or selected original delivery recipient.
    pub recipient: String,
    /// Recipient mailbox identity, including retired registrations.
    pub recipient_id: i64,
    /// Registration only; this is not runtime liveness or delivery proof.
    pub recipient_registered: bool,
    /// Whether the task is still open or the delivery pending.
    pub unresolved: bool,
    /// Original Mail due or current writer-controlled task business deadline.
    /// Attention correction cannot modify it.
    pub business_deadline: Option<i64>,
    /// Model semantic epoch when the source is contracted.
    pub input_epoch: Option<i64>,
    /// Exact immutable candidate identity when present.
    pub candidate: Option<String>,
    /// Exact current outcome identity when present.
    pub outcome: Option<String>,
    /// Existing metadata; absence never creates a plan on read.
    pub plan: Option<ObligationPlan>,
}

/// One persistent case and its separate business responsibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionCase {
    /// Durable case identifier.
    pub id: i64,
    /// Home group.
    pub group: String,
    /// Case CAS revision.
    pub version: i64,
    /// Canonical source, excluding observation revisions.
    pub source_key: String,
    /// Stable episode assigned by its source owner.
    pub episode: String,
    /// Immutable writer/sender responsibility.
    pub authority: String,
    /// Original source evidence, retained after source changes.
    pub original_source: ObligationView,
    /// Most recently reconciled source evidence.
    pub current_source: ObligationView,
    /// Referenced scheduler cause identity; serialized evidence is not authority.
    pub execution_source: Option<Value>,
    /// Original scheduler source guard, retained across later corrections.
    pub execution_original_guard: Option<Value>,
    /// Last explicitly accepted scheduler guard; use the real scheduler to revalidate.
    pub execution_guard: Option<Value>,
    /// Original causal boundary, never replenished by checkpoints.
    pub original_due: i64,
    /// Current finite review time.
    pub review_at: i64,
    /// Current finite hard boundary.
    pub hard_due: i64,
    /// Durable business state; transport acceptance cannot settle it.
    pub state: String,
    /// Ordinary model-owned decision task, when actually materialized.
    pub decision_task: Option<String>,
    /// Missing actual capability that prevents further application.
    pub capability_hold: Option<String>,
    /// A real model source change invalidated the case's previous action authority.
    pub requires_reassessment: bool,
    /// The one terminal operator responsibility for this case.
    pub operator_obligation: i64,
}

/// Expected plan state. Missing plans require a deliberate repair request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExpectedPlan {
    /// Repair only if source truth still has no plan.
    Absent,
    /// Correct the exact inspected plan version.
    Present {
        /// Stable plan identifier.
        id: i64,
        /// Plan CAS version.
        version: i64,
    },
}

/// A direct source-authorized metadata correction, not a grant or disposition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCorrection {
    /// Exact retry identity; changed content conflicts.
    pub key: String,
    /// Original source and, for tasks, observed business revision.
    pub source: Obligation,
    /// Observed plan or explicit missing-plan expectation.
    pub expected_plan: ExpectedPlan,
    /// Exact versions of every unresolved case for this canonical source.
    /// Empty is valid only when no such case exists.
    #[serde(default)]
    pub case_versions: std::collections::BTreeMap<i64, i64>,
    /// Concrete correction reason.
    pub reason: String,
    /// Bounded evidence references.
    pub evidence: Vec<String>,
    /// Concrete next action under existing authority.
    pub next_step: String,
    /// Future reassessment time.
    pub next_check_at: i64,
    /// Future hard boundary; an explicit correction may shorten it.
    pub escalation_at: i64,
}

pub(crate) fn validate_correction_shape(request: &SourceCorrection) -> Result<()> {
    required(&request.key, 128, "source correction key")?;
    required(&request.reason, 512, "source correction reason")?;
    required(&request.next_step, 512, "next step")?;
    evidence_valid(&request.evidence)?;
    ensure!(
        serde_json::to_vec(request)?.len() <= 16384,
        "source correction exceeds 16 KiB"
    );
    if let ExpectedPlan::Present { id, version } = &request.expected_plan {
        ensure!(
            *id > 0 && *version >= 0,
            "invalid expected plan identity/version"
        );
    }
    ensure!(
        request.case_versions.len() <= 1000
            && request
                .case_versions
                .iter()
                .all(|(id, version)| *id > 0 && *version >= 0),
        "invalid case expectations"
    );
    Ok(())
}

/// Correct an explicit live source and its current cases atomically. Exact
/// historical retry is checked before current timestamps, source and plan CAS.
/// This never alters business deadlines, scope, attempts or transport budgets.
pub(crate) async fn correct_obligation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    request: &SourceCorrection,
    now: i64,
) -> Result<Value> {
    validate_correction_shape(request)?;
    authenticate_tx(tx, actor).await?;
    let canonical = json!({"operation":"source_correction","request":request});
    if let Some(result) = replay_tx(tx, actor, &request.key, &canonical).await? {
        return Ok(result);
    }
    validate_correction_time(request, now)?;
    let source = inspect_source_tx(tx, &actor.group_name, &request.source).await?;
    ensure!(
        source.source == request.source && source.unresolved,
        "source changed or settled"
    );
    ensure!(
        source.authority_id == actor.id && source.authority == actor.name,
        "only original source authority may correct"
    );
    let rows = sqlx::query("SELECT id,version FROM decision_cases WHERE group_name=? AND source_key=? AND state NOT IN ('handled','superseded') ORDER BY id LIMIT 1001")
        .bind(&actor.group_name).bind(&source.source_key).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 1000, "source case bound exceeded");
    let versions: std::collections::BTreeMap<i64, i64> = rows
        .iter()
        .map(|r| (r.get("id"), r.get("version")))
        .collect();
    ensure!(
        versions == request.case_versions,
        "current source case identity/version conflict"
    );
    let target = match &request.source {
        Obligation::Task { id, version } => crate::followup::CorrectionSource::Task {
            id: id.clone(),
            version: *version,
        },
        Obligation::Delivery { message, .. } => crate::followup::CorrectionSource::Delivery {
            message: *message,
            recipient: source.recipient_id,
        },
    };
    let expected = match request.expected_plan {
        ExpectedPlan::Absent => None,
        ExpectedPlan::Present { id, version } => Some((id, version)),
    };
    let report = crate::followup::Checkpoint {
        version: expected.map_or(0, |(_, version)| version),
        next_step: request.next_step.clone(),
        next_check_at: request.next_check_at,
        waiting: None,
        evidence: request.evidence.clone(),
        extend_until: Some(request.escalation_at),
        reason: Some(request.reason.clone()),
    };
    let plan =
        crate::followup::correct_obligation_plan_tx(tx, actor, &target, expected, &report, now)
            .await?;
    let corrected = inspect_source_tx(tx, &actor.group_name, &request.source).await?;
    let mut changed = std::collections::BTreeMap::new();
    let mut cases = Vec::new();
    for (id, version) in versions {
        let before = load_case_tx(tx, &actor.group_name, id).await?;
        ensure!(
            before.authority == actor.name,
            "case authority differs from original source"
        );
        version.checked_add(1).context("case revision overflow")?;
        let legacy = before.execution_source.is_none()
            && before.decision_task.is_none()
            && corrected.input_epoch.is_none();
        // Metadata correction can reschedule a legacy review. Execution/decision
        // deadlines and cleanup remain exactly where their owner left them.
        sqlx::query("UPDATE decision_cases SET version=version+1,current_source=?,review_at=?,hard_due=?,state=?,last_scan=0 WHERE group_name=? AND id=? AND version=?")
            .bind(serde_json::to_string(&corrected)?).bind(if legacy { request.next_check_at } else { before.review_at })
            .bind(if legacy { request.escalation_at } else { before.hard_due }).bind(if legacy { "held" } else { &before.state })
            .bind(&actor.group_name).bind(id).bind(version).execute(&mut **tx).await?;
        sqlx::query("UPDATE operator_obligations SET version=version+1,hard_due=?,state=CASE WHEN ? THEN 'pending' ELSE state END,reason=?,evidence=?,last_scan=0 WHERE group_name=? AND case_id=?")
            .bind(if legacy { request.escalation_at } else { before.hard_due }).bind(legacy).bind(&request.reason)
            .bind(serde_json::to_string(&request.evidence)?).bind(&actor.group_name).bind(id).execute(&mut **tx).await?;
        let after = load_case_tx(tx, &actor.group_name, id).await?;
        record_audit_tx(
            tx,
            AuditRecord {
                group: &actor.group_name,
                case: Some(id),
                actor: None,
                key: &request.key,
                operation: "source_plan_case_corrected",
                canonical: &json!({"before":before,"request":request}),
                result: &serde_json::to_value(&after)?,
                now,
            },
        )
        .await?;
        changed.insert(id, after.version);
        cases.push(after);
    }
    if !changed.is_empty() {
        crate::task_graph::refresh_recovery_projection_tx(tx, &actor.group_name, &changed).await?;
    }
    let result = json!({"source_before":source,"source_after":corrected,"plan":plan,"cases":cases});
    record_audit_tx(
        tx,
        AuditRecord {
            group: &actor.group_name,
            case: None,
            actor: Some(actor.id),
            key: &request.key,
            operation: "source_correction",
            canonical: &canonical,
            result: &result,
            now,
        },
    )
    .await?;
    Ok(result)
}

impl Store {
    /// Apply an explicit source-authorized correction and return its durable receipt.
    pub async fn correct_obligation(
        &self,
        actor: &Mailbox,
        request: SourceCorrection,
        now: i64,
    ) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        let result = correct_obligation_tx(&mut tx, actor, &request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
}

// Call only AFTER exact replay lookup so a historical successful correction can
// still be retried after its original future timestamps have passed.
pub(crate) fn validate_correction_time(request: &SourceCorrection, now: i64) -> Result<()> {
    ensure!(
        now < request.next_check_at && request.next_check_at <= request.escalation_at,
        "require now < next_check_at <= escalation_at"
    );
    Ok(())
}

pub(crate) fn required(value: &str, limit: usize, label: &str) -> Result<()> {
    bounded(value, limit, label)?;
    ensure!(!value.trim().is_empty(), "{label} is empty");
    Ok(())
}

pub(crate) fn evidence_valid(evidence: &[String]) -> Result<()> {
    ensure!(evidence.len() <= 16, "too many evidence references");
    for item in evidence {
        required(item, 256, "evidence")?;
    }
    Ok(())
}

pub(crate) async fn reserve_home_tx(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<()> {
    // Reserve the SQLite writer before source/graph/cursor reads. No runtime I/O.
    let result = sqlx::query(
        "UPDATE groups SET paused=paused WHERE name=? AND home_machine IN (SELECT id FROM node)",
    )
    .bind(group)
    .execute(&mut **tx)
    .await?;
    ensure!(
        result.rows_affected() == 1,
        "recovery requires the group's home store"
    );
    Ok(())
}

pub(crate) async fn authenticate_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
) -> Result<()> {
    Store::lock_actor(tx, actor).await?;
    reserve_home_tx(tx, &actor.group_name).await?;
    let valid: i64 =
        sqlx::query_scalar("SELECT count(*) FROM mailboxes WHERE id=? AND group_name=? AND name=?")
            .bind(actor.id)
            .bind(&actor.group_name)
            .bind(&actor.name)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(valid == 1, "recovery actor identity mismatch");
    Ok(())
}

/// Internal inspection shared by the supervisor and source-correction transaction.
/// It includes unavailable owners and closed source truth; callers choose whether
/// a mutation requires unresolved state. It performs no actor impersonation.
pub(crate) async fn inspect_source_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    source: &Obligation,
) -> Result<ObligationView> {
    match source {
        Obligation::Task { id, version } => {
            required(id, 128, "task")?;
            ensure!(*version > 0, "task revision must be positive");
            let row = sqlx::query("SELECT w.version,w.open,w.owner,w.writer,w.deadline,b.id AS recipient_id,b.agent_state,b.remote_machine,a.id AS authority_id,a.agent_state AS authority_state,a.remote_machine AS authority_remote,m.input_epoch,m.current_candidate,m.current_outcome FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer LEFT JOIN task_models m ON m.group_name=w.group_name AND m.task=w.id WHERE w.group_name=? AND w.id=?")
                .bind(group).bind(id).fetch_optional(&mut **tx).await?.context("task source missing")?;
            ensure!(
                row.get::<Option<String>, _>("remote_machine").is_none()
                    && row.get::<Option<String>, _>("authority_remote").is_none(),
                "remote task recovery unsupported"
            );
            let current_version: i64 = row.get("version");
            let plan = sqlx::query_as::<_, ObligationPlan>("SELECT id,version,opened,next_check,escalate_at FROM followups WHERE group_name=? AND task=?")
                .bind(group).bind(id).fetch_optional(&mut **tx).await?;
            Ok(ObligationView {
                source: Obligation::Task {
                    id: id.clone(),
                    version: current_version,
                },
                source_key: serde_json::to_string(&json!(["task", id]))?,
                authority: row.get("writer"),
                authority_id: row.get("authority_id"),
                authority_registered: row.get::<String, _>("authority_state") == "registered",
                recipient: row.get("owner"),
                recipient_id: row.get("recipient_id"),
                recipient_registered: row.get::<String, _>("agent_state") == "registered",
                unresolved: row.get::<i64, _>("open") == 1,
                business_deadline: row.get("deadline"),
                input_epoch: row.get("input_epoch"),
                candidate: row.get("current_candidate"),
                outcome: row.get("current_outcome"),
                plan,
            })
        }
        Obligation::Delivery { message, recipient } => {
            ensure!(*message > 0, "message identifier must be positive");
            required(recipient, 128, "recipient")?;
            let row = sqlx::query("SELECT b.id AS recipient_id,b.agent_state,b.remote_machine,a.id AS authority_id,a.name AS authority,a.agent_state AS authority_state,a.remote_machine AS authority_remote,d.state,m.due FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN mailboxes a ON a.id=m.sender WHERE d.message=? AND b.group_name=? AND b.name=? AND a.group_name=b.group_name")
                .bind(message).bind(group).bind(recipient).fetch_optional(&mut **tx).await?.context("original same-group delivery missing")?;
            ensure!(
                row.get::<Option<String>, _>("remote_machine").is_none()
                    && row.get::<Option<String>, _>("authority_remote").is_none(),
                "remote delivery recovery unsupported"
            );
            let recipient_id: i64 = row.get("recipient_id");
            let plan = sqlx::query_as::<_, ObligationPlan>("SELECT id,version,opened,next_check,escalate_at FROM followups WHERE group_name=? AND message=? AND recipient=?")
                .bind(group).bind(message).bind(recipient_id).fetch_optional(&mut **tx).await?;
            Ok(ObligationView {
                source: source.clone(),
                source_key: serde_json::to_string(&json!(["delivery", message, recipient_id]))?,
                authority: row.get("authority"),
                authority_id: row.get("authority_id"),
                authority_registered: row.get::<String, _>("authority_state") == "registered",
                recipient: recipient.clone(),
                recipient_id,
                recipient_registered: row.get::<String, _>("agent_state") == "registered",
                unresolved: row.get::<String, _>("state") == "pending",
                business_deadline: row.get("due"),
                input_epoch: None,
                candidate: None,
                outcome: None,
                plan,
            })
        }
    }
}

pub(crate) async fn load_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    id: i64,
) -> Result<DecisionCase> {
    let row = sqlx::query("SELECT c.*,o.id AS obligation_id FROM decision_cases c JOIN operator_obligations o ON o.group_name=c.group_name AND o.case_id=c.id WHERE c.group_name=? AND c.id=?")
        .bind(group).bind(id).fetch_optional(&mut **tx).await?.context("decision case or operator responsibility missing")?;
    Ok(DecisionCase {
        id: row.get("id"),
        group: row.get("group_name"),
        version: row.get("version"),
        source_key: row.get("source_key"),
        episode: row.get("episode"),
        authority: row.get("authority"),
        original_source: serde_json::from_str(&row.get::<String, _>("original_source"))?,
        current_source: serde_json::from_str(&row.get::<String, _>("current_source"))?,
        execution_source: row
            .get::<Option<String>, _>("execution_source")
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        execution_original_guard: row
            .get::<Option<String>, _>("execution_original_guard")
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        execution_guard: row
            .get::<Option<String>, _>("execution_guard")
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        original_due: row.get("original_due"),
        review_at: row.get("review_at"),
        hard_due: row.get("hard_due"),
        state: row.get("state"),
        decision_task: row.get("decision_task"),
        capability_hold: row.get("capability_hold"),
        requires_reassessment: row.get::<i64, _>("requires_reassessment") == 1,
        operator_obligation: row.get("obligation_id"),
    })
}

pub(crate) struct AuditRecord<'a> {
    pub(crate) group: &'a str,
    pub(crate) case: Option<i64>,
    pub(crate) actor: Option<i64>,
    pub(crate) key: &'a str,
    pub(crate) operation: &'a str,
    pub(crate) canonical: &'a Value,
    pub(crate) result: &'a Value,
    pub(crate) now: i64,
}

pub(crate) async fn record_audit_tx(
    tx: &mut Transaction<'_, Sqlite>,
    record: AuditRecord<'_>,
) -> Result<()> {
    let AuditRecord {
        group,
        case,
        actor,
        key,
        operation,
        canonical,
        result,
        now,
    } = record;
    sqlx::query("INSERT INTO decision_audit(group_name,case_id,actor,key,operation,canonical,result,created) VALUES(?,?,?,?,?,?,?,?)")
        .bind(group).bind(case).bind(actor).bind(key).bind(operation).bind(serde_json::to_string(canonical)?)
        .bind(serde_json::to_string(result)?).bind(now).execute(&mut **tx).await?;
    Ok(())
}

pub(crate) async fn replay_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    key: &str,
    canonical: &Value,
) -> Result<Option<Value>> {
    if let Some(row) =
        sqlx::query("SELECT canonical,result FROM decision_audit WHERE actor=? AND key=?")
            .bind(actor.id)
            .bind(key)
            .fetch_optional(&mut **tx)
            .await?
    {
        ensure!(
            serde_json::from_str::<Value>(&row.get::<String, _>("canonical"))? == *canonical,
            "recovery key reused with different content"
        );
        return Ok(Some(serde_json::from_str(&row.get::<String, _>("result"))?));
    }
    Ok(None)
}

/// Create responsibility for a *nonexecution* expired obligation. The episode is
/// derived here, never supplied by a caller, and survives task/plan revisions.
/// Execution causes must enter through the scheduler's actual guarded handoff.
pub(crate) async fn ensure_obligation_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    source: &ObligationView,
    now: i64,
) -> Result<DecisionCase> {
    ensure!(source.unresolved, "source is already settled");
    if let Obligation::Task { id, .. } = &source.source {
        // A decision task's own failure stays in its original case.
        if let Some(original) = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM decision_cases WHERE group_name=? AND decision_task=?",
        )
        .bind(group)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        {
            return load_case_tx(tx, group, original).await;
        }
        let materialized: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_materializations WHERE group_name=? AND decision_task=?",
        )
        .bind(group)
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            materialized == 0,
            "orphan_decision_materialization: recovery mapping required; no recursive decision"
        );
        ensure!(
            source.input_epoch.is_none(),
            "scheduler_cause_handoff_unavailable: contracted task requires its validated execution episode"
        );
    }
    let missing_plan = source.plan.is_none();
    let unavailable = !source.authority_registered || !source.recipient_registered;
    let original_due = source.plan.as_ref().map_or(now, |plan| plan.escalate_at);
    ensure!(
        missing_plan || unavailable || original_due <= now,
        "obligation hard boundary has not expired"
    );
    // Missing metadata needs immediate responsible inspection, not an invented
    // grace period or a plan created by an observer.
    let hard_due = original_due.min(now);
    let episode = "obligation";
    if let Some(id) = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM decision_cases WHERE group_name=? AND source_key=? AND episode=?",
    )
    .bind(group)
    .bind(&source.source_key)
    .bind(episode)
    .fetch_optional(&mut **tx)
    .await?
    {
        return load_case_tx(tx, group, id).await;
    }
    let (kind, task, message, recipient) = match &source.source {
        Obligation::Task { id, .. } => ("task", Some(id.as_str()), None, None),
        Obligation::Delivery { message, .. } => {
            ("delivery", None, Some(*message), Some(source.recipient_id))
        }
    };
    let snapshot = serde_json::to_string(source)?;
    let hold = "decision_materialization_and_shared_notifier_unavailable";
    let id = sqlx::query("INSERT INTO decision_cases(group_name,source_kind,task,message,recipient,source_key,episode,authority,original_source,current_source,opened,original_due,review_at,hard_due,state,capability_hold) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'operator_required',?)")
        .bind(group).bind(kind).bind(task).bind(message).bind(recipient).bind(&source.source_key).bind(episode)
        .bind(&source.authority).bind(&snapshot).bind(&snapshot).bind(now).bind(original_due).bind(hard_due).bind(hard_due).bind(hold)
        .execute(&mut **tx).await?.last_insert_rowid();
    sqlx::query("INSERT INTO operator_obligations(group_name,case_id,authority,opened,hard_due,state,reason,evidence) VALUES(?,?,?,?,?,'escalated',?,?)")
        .bind(group).bind(id).bind(&source.authority).bind(now).bind(hard_due).bind(if missing_plan { "source plan missing; explicit authority repair required" } else if unavailable { "source authority or recipient retired; original authority remains responsible" } else { "original obligation expired; authorized decision required" })
        .bind(serde_json::to_string(&vec![source.plan.as_ref().map_or_else(|| "source:missing-plan".to_owned(), |plan| format!("followup:{}", plan.id))])?).execute(&mut **tx).await?;
    let result = load_case_tx(tx, group, id).await?;
    record_audit_tx(
        tx,
        AuditRecord {
            group,
            case: Some(id),
            actor: None,
            key: "ensure-obligation",
            operation: "open_case",
            canonical: &json!({"source":source,"episode":episode}),
            result: &serde_json::to_value(&result)?,
            now,
        },
    )
    .await?;
    Ok(result)
}

impl Store {
    /// Inspect an exact obligation as its source authority or current recipient.
    /// Reads leave source dispositions, retrieval and plans untouched.
    pub async fn inspect_obligation(
        &self,
        actor: &Mailbox,
        source: Obligation,
    ) -> Result<ObligationView> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let home: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM groups WHERE name=? AND home_machine IN (SELECT id FROM node)",
        )
        .bind(&actor.group_name)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(home == 1, "recovery requires the group's home store");
        let view = inspect_source_tx(&mut tx, &actor.group_name, &source).await?;
        ensure!(
            actor.id == view.authority_id || actor.id == view.recipient_id,
            "obligation is not visible to this actor"
        );
        tx.commit().await?;
        Ok(view)
    }

    /// Persist the one finite responsibility for an expired legacy obligation.
    /// This never allocates a decision budget or changes the business source.
    pub async fn recover_expired_obligation(
        &self,
        actor: &Mailbox,
        key: &str,
        source: Obligation,
        now: i64,
    ) -> Result<DecisionCase> {
        required(key, 128, "recovery key")?;
        let canonical = json!({"operation":"recover_expired_obligation","source":source});
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, key, &canonical).await? {
            tx.commit().await?;
            return Ok(serde_json::from_value(old)?);
        }
        let current = inspect_source_tx(&mut tx, &actor.group_name, &source).await?;
        ensure!(
            current.source == source,
            "source revision changed; inspect and reconsider"
        );
        ensure!(
            current.authority_id == actor.id,
            "only the original source authority can request recovery"
        );
        let case = ensure_obligation_case_tx(&mut tx, &actor.group_name, &current, now).await?;
        record_audit_tx(
            &mut tx,
            AuditRecord {
                group: &actor.group_name,
                case: Some(case.id),
                actor: Some(actor.id),
                key,
                operation: "recover_expired_obligation",
                canonical: &canonical,
                result: &serde_json::to_value(&case)?,
                now,
            },
        )
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(case)
    }

    /// Read a current case without changing operator or source responsibility.
    pub async fn decision_case(&self, actor: &Mailbox, id: i64) -> Result<DecisionCase> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let case = load_case_tx(&mut tx, &actor.group_name, id).await?;
        tx.commit().await?;
        Ok(case)
    }
}

/// Nonserializable proof minted only after revalidating a persisted case and its
/// current source under the SQLite writer reservation. There is no public nonce,
/// policy payload or `Mailbox` constructor that can stand in for this proof.
#[derive(Debug)]
pub(crate) struct ValidatedDecisionEpisode {
    case: DecisionCase,
    source: ObligationView,
}

/// Historical identity only. A model receipt lookup may use this after source
/// settlement or grant revocation; possession does not authorize a new mutation.
#[derive(Debug)]
pub(crate) struct DecisionCaseIdentity {
    pub(crate) id: i64,
    pub(crate) group: String,
    pub(crate) source_key: String,
    pub(crate) episode: String,
    pub(crate) authority: String,
    pub(crate) original_source: ObligationView,
}

/// Read immutable identity for the model's exact historical replay lookup.
/// No eligibility/current-version/source-state predicate belongs on this read.
/// Callers authenticate their entrypoint, compare complete original canonical
/// request bytes and return the saved receipt without relinking the case. A new
/// mutation still requires `validate_decision_episode_tx` in that transaction.
pub(crate) async fn read_case_identity_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case_id: i64,
) -> Result<DecisionCaseIdentity> {
    let row = sqlx::query("SELECT id,group_name,source_key,episode,authority,original_source FROM decision_cases WHERE group_name=? AND id=?")
        .bind(group).bind(case_id).fetch_optional(&mut **tx).await?.context("decision case identity missing")?;
    Ok(DecisionCaseIdentity {
        id: row.get("id"),
        group: row.get("group_name"),
        source_key: row.get("source_key"),
        episode: row.get("episode"),
        authority: row.get("authority"),
        original_source: serde_json::from_str(&row.get::<String, _>("original_source"))?,
    })
}
impl ValidatedDecisionEpisode {
    pub(crate) fn group(&self) -> &str {
        &self.case.group
    }
    pub(crate) fn case_id(&self) -> i64 {
        self.case.id
    }
    pub(crate) fn case_version(&self) -> i64 {
        self.case.version
    }
    pub(crate) fn source_key(&self) -> &str {
        &self.case.source_key
    }
    pub(crate) fn episode(&self) -> &str {
        &self.case.episode
    }
    pub(crate) fn authority(&self) -> &str {
        &self.case.authority
    }
    pub(crate) fn hard_due(&self) -> i64 {
        self.case.hard_due
    }
    pub(crate) fn source(&self) -> &ObligationView {
        &self.source
    }
}

/// Revalidation, not deserialization, creates the model's materialization input.
/// The model must call this again in its own transaction before using a retained
/// proof, and load its persisted finite grant itself. Execution episodes require
/// the scheduler's real causal guard in this same transaction.
pub(crate) async fn validate_decision_episode_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case_id: i64,
    expected_version: i64,
    now: i64,
) -> Result<ValidatedDecisionEpisode> {
    reserve_home_tx(tx, group).await?;
    let case = load_case_tx(tx, group, case_id).await?;
    ensure!(
        case.version == expected_version,
        "decision case revision conflict"
    );
    ensure!(!case.requires_reassessment, "source_reassessment_required");
    ensure!(
        matches!(case.state.as_str(), "held" | "decision_pending"),
        "case is not eligible for decision materialization"
    );
    ensure!(
        now < case.hard_due,
        "case hard boundary expired; terminal operator responsibility remains"
    );
    let source = inspect_source_tx(tx, group, &case.current_source.source).await?;
    ensure!(source.unresolved, "source is settled");
    ensure!(
        source.source == case.current_source.source
            && source.input_epoch == case.current_source.input_epoch
            && source.candidate == case.current_source.candidate
            && source.outcome == case.current_source.outcome,
        "decision source changed; explicit case reassessment required"
    );
    ensure!(
        source.authority == case.authority,
        "source authority changed"
    );
    if let Some(guard) = &case.execution_guard {
        let guard =
            serde_json::from_value::<crate::execution::ExecutionSourceGuard>(guard.clone())?;
        validate_execution_case_tx(tx, group, case.id, case.version, &guard).await?;
    } else {
        ensure!(
            case.episode == "obligation" && source.input_epoch.is_none(),
            "validated scheduler episode required"
        );
    }
    Ok(ValidatedDecisionEpisode { case, source })
}

/// Bind the actual model materialization receipt and case atomically. This
/// helper cannot create a task or accept a caller's arbitrary task draft.
pub(crate) async fn link_materialized_decision_tx(
    tx: &mut Transaction<'_, Sqlite>,
    episode: &ValidatedDecisionEpisode,
    task: &str,
    now: i64,
) -> Result<DecisionCase> {
    let current = validate_decision_episode_tx(
        tx,
        episode.group(),
        episode.case_id(),
        episode.case_version(),
        now,
    )
    .await?;
    ensure!(
        current.source == episode.source,
        "decision source changed during materialization"
    );
    let found: i64 = sqlx::query_scalar("SELECT count(*) FROM task_materializations m JOIN work_items w ON w.group_name=m.group_name AND w.id=m.decision_task WHERE m.group_name=? AND m.source=? AND m.episode=? AND m.decision_task=? AND w.writer=?")
        .bind(episode.group()).bind(episode.source_key()).bind(episode.episode()).bind(task).bind(episode.authority())
        .fetch_one(&mut **tx).await?;
    ensure!(found == 1, "matching model materialization receipt missing");
    ensure!(
        current
            .case
            .decision_task
            .as_deref()
            .is_none_or(|old| old == task),
        "episode already materialized a different task"
    );
    if current.case.decision_task.is_none() {
        // Linking preserves the validated source and existing scope. Carry only
        // blockers that belong to that exact prior revision; never bless stale
        // rows by assigning the new case revision indiscriminately.
        let versions: Vec<i64> = sqlx::query_scalar("SELECT case_version FROM decision_blockers WHERE group_name=? AND case_id=? ORDER BY ordinal")
            .bind(episode.group()).bind(episode.case_id()).fetch_all(&mut **tx).await?;
        ensure!(
            versions.len() <= 32
                && versions
                    .iter()
                    .all(|version| *version == episode.case_version()),
            "graph_validation_incomplete: stale recovery blocker before decision linkage"
        );
        let next_version = episode
            .case_version()
            .checked_add(1)
            .context("case revision overflow")?;
        let changed = sqlx::query("UPDATE decision_cases SET decision_task=?,state='decision_pending',version=?,capability_hold='shared_notifier_unavailable' WHERE group_name=? AND id=? AND version=? AND decision_task IS NULL")
            .bind(task).bind(next_version).bind(episode.group()).bind(episode.case_id()).bind(episode.case_version()).execute(&mut **tx).await?;
        ensure!(
            changed.rows_affected() == 1,
            "decision linkage case revision conflict"
        );
        let changed = sqlx::query("UPDATE decision_blockers SET case_version=? WHERE group_name=? AND case_id=? AND case_version=?")
            .bind(next_version).bind(episode.group()).bind(episode.case_id()).bind(episode.case_version()).execute(&mut **tx).await?;
        ensure!(
            changed.rows_affected() == u64::try_from(versions.len())?,
            "decision linkage blocker revision conflict"
        );
        initialize_linked_work_blocker_tx(tx, &current, next_version, task, now).await?;
        record_audit_tx(tx, AuditRecord {
            group: episode.group(),
            case: Some(episode.case_id()),
            actor: None,
            key: "materialize",
            operation: "decision_linked",
            canonical: &json!({"case_version":episode.case_version(),"source":episode.source_key(),"episode":episode.episode(),"carried_blockers":versions.len()}),
            result: &json!({"decision_task":task,"case_version":next_version}),
            now,
        }).await?;
    }
    load_case_tx(tx, episode.group(), episode.case_id()).await
}

/// The actual model materializer calls this linkage under its validated standing
/// policy. Only a contracted Work cause creates an initial Execute dependency;
/// legacy Mail/bare-task materialization cannot acquire a Work source edge.
async fn initialize_linked_work_blocker_tx(
    tx: &mut Transaction<'_, Sqlite>,
    episode: &ValidatedDecisionEpisode,
    version: i64,
    task: &str,
    now: i64,
) -> Result<()> {
    use crate::task_graph::{ActionNode, TaskAction};
    if episode.source.input_epoch.is_none() {
        return Ok(());
    }
    let Obligation::Task { id: source, .. } = &episode.source.source else {
        anyhow::bail!("contracted Work linkage source required");
    };
    let guard: crate::execution::ExecutionSourceGuard = serde_json::from_value(
        episode
            .case
            .execution_guard
            .clone()
            .context("actual Work execution guard required")?,
    )?;
    let current =
        validate_execution_case_tx(tx, episode.group(), episode.case_id(), version, &guard).await?;
    ensure!(
        matches!(
            &current.cause.source_kind,
            crate::execution::ExecutionSourceKind::Work
        ),
        "initial blocker requires original Work cause"
    );
    ensure!(
        current.case.decision_task.as_deref() == Some(task),
        "actual linked decision required"
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM decision_blockers WHERE group_name=? AND case_id=?",
    )
    .bind(episode.group())
    .bind(episode.case_id())
    .fetch_one(&mut **tx)
    .await?;
    ensure!(count == 0, "initial Work linkage has preexisting blockers");
    let responsible: String = sqlx::query_scalar("SELECT w.owner FROM task_materializations m JOIN work_items w ON w.group_name=m.group_name AND w.id=m.decision_task WHERE m.group_name=? AND m.source=? AND m.episode=? AND m.decision_task=? AND w.writer=?")
        .bind(episode.group()).bind(episode.source_key()).bind(episode.episode()).bind(task)
        .bind(episode.authority()).fetch_one(&mut **tx).await?;
    let blocker = ScopedBlocker {
        selector: ActionNode {
            task: source.clone(),
            action: TaskAction::Execute,
        },
        waiting: Some(ActionNode {
            task: task.into(),
            action: TaskAction::AcceptResult,
        }),
        responsible,
        reason: "original Work execution waits for its finite decision outcome".into(),
        evidence: vec![
            format!("materialized-decision:{task}"),
            format!("execution-cause:{}", guard.cause_ref.cause_generation),
        ],
    };
    sqlx::query("INSERT INTO decision_blockers(group_name,case_id,case_version,ordinal,selector,waiting,responsible,reason,evidence) VALUES(?,?,?,0,?,?,?,?,?)")
        .bind(episode.group()).bind(episode.case_id()).bind(version)
        .bind(serde_json::to_string(&blocker.selector)?)
        .bind(blocker.waiting.as_ref().map(serde_json::to_string).transpose()?)
        .bind(&blocker.responsible).bind(&blocker.reason).bind(serde_json::to_string(&blocker.evidence)?)
        .execute(&mut **tx).await?;
    crate::task_graph::refresh_recovery_projection_tx(
        tx,
        episode.group(),
        &std::collections::BTreeMap::from([(episode.case_id(), version)]),
    )
    .await?;
    record_audit_tx(tx, AuditRecord {group:episode.group(),case:Some(episode.case_id()),actor:None,
        key:"materialize-initial-execute",operation:"decision_initial_work_blocker",
        canonical:&json!({"original_case":episode.case,"source":episode.source,"guard":guard}),
        result:&json!({"case_version":version,"decision_task":task,"ordinal":0,"blocker":blocker}),now}).await?;
    Ok(())
}

/// Source-owned specification consumed by the model's complete typed graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedBlocker {
    /// Exact declared action being held.
    pub selector: crate::task_graph::ActionNode,
    /// Exact action that must complete first. External conditions remain
    /// explicitly unproven and need a responsible review; they are not DAG proof.
    pub waiting: Option<crate::task_graph::ActionNode>,
    /// Existing responsible authority or reviewer named by the finite policy.
    pub responsible: String,
    /// Concrete causal reason.
    pub reason: String,
    /// Bounded evidence references.
    pub evidence: Vec<String>,
}

/// Owner-tagged source projection, before model-wide scope and cycle validation.
#[derive(Debug)]
pub(crate) struct RecoveryBlockingEdge {
    pub(crate) source: String,
    pub(crate) case_version: i64,
    pub(crate) edge: crate::task_graph::BlockingEdge,
}

/// Enumerate authoritative recovery edges for the model's complete graph check.
/// This validates recovery source meaning and case CAS only. The model still
/// validates declared scope aliases, the full graph and projection equality.
/// A partial/stale/unmapped recovery projection is an error, never an empty DAG.
pub(crate) async fn recovery_blocking_edges_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
) -> Result<Vec<RecoveryBlockingEdge>> {
    reserve_home_tx(tx, group).await?;
    let rows = sqlx::query("SELECT b.*,c.version AS current_version FROM decision_blockers b JOIN decision_cases c ON c.group_name=b.group_name AND c.id=b.case_id WHERE b.group_name=? ORDER BY b.case_id,b.ordinal LIMIT 10001")
        .bind(group).fetch_all(&mut **tx).await?;
    ensure!(
        rows.len() <= 10000,
        "graph_validation_incomplete: recovery edge bound"
    );
    let mut edges = Vec::with_capacity(rows.len());
    for row in rows {
        ensure!(
            row.get::<i64, _>("case_version") == row.get::<i64, _>("current_version"),
            "graph_validation_incomplete: stale recovery blocker"
        );
        let case = load_case_tx(tx, group, row.get("case_id")).await?;
        ensure!(
            matches!(
                case.state.as_str(),
                "held" | "decision_pending" | "operator_required"
            ),
            "graph_validation_incomplete: settled case retains blockers"
        );
        let source = inspect_source_tx(tx, group, &case.current_source.source).await?;
        ensure!(
            source.unresolved
                && source.source == case.current_source.source
                && source.input_epoch == case.current_source.input_epoch
                && source.candidate == case.current_source.candidate
                && source.outcome == case.current_source.outcome,
            "graph_validation_incomplete: recovery source changed"
        );
        let selector: crate::task_graph::ActionNode =
            serde_json::from_str(&row.get::<String, _>("selector"))?;
        let source_task = match &source.source {
            Obligation::Task { id, .. } => Some(id.as_str()),
            Obligation::Delivery { .. } => None,
        };
        ensure!(
            Some(selector.task.as_str()) == source_task
                || Some(selector.task.as_str()) == case.decision_task.as_deref(),
            "graph_validation_incomplete: blocker consumer is outside this case"
        );
        let responsible: String = row.get("responsible");
        required(&responsible, 128, "blocker responsible")?;
        if responsible != case.authority {
            let authorized_reviewer: i64 = sqlx::query_scalar("SELECT count(*) FROM work_items WHERE group_name=? AND id=? AND owner=? AND writer=?")
                .bind(group).bind(&case.decision_task).bind(&responsible).bind(&case.authority).fetch_one(&mut **tx).await?;
            ensure!(
                authorized_reviewer == 1,
                "graph_validation_incomplete: blocker responsibility is outside this decision"
            );
        }
        required(&row.get::<String, _>("reason"), 512, "blocker reason")?;
        evidence_valid(&serde_json::from_str::<Vec<String>>(
            &row.get::<String, _>("evidence"),
        )?)?;
        let waiting = row
            .get::<Option<String>, _>("waiting")
            .context("graph_validation_incomplete: external wait has no typed action mapping")?;
        let prerequisite: crate::task_graph::ActionNode = serde_json::from_str(&waiting)?;
        // A source action may only be held on its actual decision task. An
        // ordinary decision may wait on a declared source/dependency action;
        // model traversal detects the resulting mixed cycle atomically.
        if Some(selector.task.as_str()) == source_task {
            ensure!(
                Some(prerequisite.task.as_str()) == case.decision_task.as_deref(),
                "graph_validation_incomplete: source blocker is not its decision task"
            );
        }
        for task in [&selector.task, &prerequisite.task] {
            let exists: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM task_models WHERE group_name=? AND task=?",
            )
            .bind(group)
            .bind(task)
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                exists == 1,
                "graph_validation_incomplete: typed action task missing"
            );
        }
        edges.push(RecoveryBlockingEdge {
            source: format!("case:{}", case.id),
            case_version: case.version,
            edge: crate::task_graph::BlockingEdge {
                consumer: selector,
                prerequisite,
                kind: "decision".into(),
            },
        });
    }
    Ok(edges)
}

/// Explicit metadata correction for an unmaterialized nonexecution case.
/// Scope, policies and execution budgets require their owners' separate guards.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseCorrection {
    /// Exact retry key.
    pub key: String,
    /// Existing canonical case.
    pub case_id: i64,
    /// Observed case revision.
    pub version: i64,
    /// Complete inspected source guard, including concrete model result IDs.
    pub source: ObligationView,
    /// Audited reason for changing the finite case schedule.
    pub reason: String,
    /// Evidence references.
    pub evidence: Vec<String>,
    /// New future review time.
    pub review_at: i64,
    /// Explicit future hard boundary; original boundary stays in the audit.
    pub hard_due: i64,
}

impl Store {
    /// Correct a finite unmaterialized legacy case under its original authority.
    /// Neither source metadata nor a scheduler allocation changes. Exact replay
    /// precedes current time/CAS checks and returns its historical result.
    pub async fn correct_decision_case(
        &self,
        actor: &Mailbox,
        request: CaseCorrection,
        now: i64,
    ) -> Result<DecisionCase> {
        required(&request.key, 128, "case correction key")?;
        required(&request.reason, 512, "case correction reason")?;
        evidence_valid(&request.evidence)?;
        let canonical = json!({"operation":"correct_decision_case","request":request});
        ensure!(
            serde_json::to_vec(&canonical)?.len() <= 16384,
            "case correction exceeds 16 KiB"
        );
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            tx.commit().await?;
            return Ok(serde_json::from_value(old)?);
        }
        let case = load_case_tx(&mut tx, &actor.group_name, request.case_id).await?;
        ensure!(case.version == request.version, "case revision conflict");
        ensure!(
            case.episode == "obligation" && case.decision_task.is_none(),
            "model_scheduler_case_correction_guard_unavailable"
        );
        let source =
            inspect_source_tx(&mut tx, &actor.group_name, &case.current_source.source).await?;
        ensure!(
            source == request.source,
            "source changed; inspect and reconsider"
        );
        ensure!(source.unresolved, "source already settled");
        ensure!(
            source.authority_id == actor.id && source.authority == case.authority,
            "only the original source authority may correct a case"
        );
        ensure!(
            source.input_epoch.is_none(),
            "model_scheduler_case_correction_guard_unavailable"
        );
        ensure!(
            now < request.review_at && request.review_at <= request.hard_due,
            "require now < review_at <= hard_due"
        );
        // Existing blockers need the complete model graph delta; do not make
        // their captured case revision silently stale or implicitly remove them.
        let blockers: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM decision_blockers WHERE group_name=? AND case_id=?",
        )
        .bind(&actor.group_name)
        .bind(case.id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(blockers == 0, "model_blocker_correction_guard_unavailable");
        sqlx::query("UPDATE decision_cases SET current_source=?,version=version+1,review_at=?,hard_due=?,state='held',last_scan=0 WHERE group_name=? AND id=? AND version=?")
            .bind(serde_json::to_string(&source)?).bind(request.review_at).bind(request.hard_due).bind(&actor.group_name).bind(case.id).bind(case.version)
            .execute(&mut *tx).await?;
        sqlx::query("UPDATE operator_obligations SET version=version+1,hard_due=?,state='pending',reason=?,evidence=?,last_scan=0 WHERE group_name=? AND case_id=?")
            .bind(request.hard_due).bind(&request.reason).bind(serde_json::to_string(&request.evidence)?).bind(&actor.group_name).bind(case.id).execute(&mut *tx).await?;
        let corrected = load_case_tx(&mut tx, &actor.group_name, case.id).await?;
        record_audit_tx(
            &mut tx,
            AuditRecord {
                group: &actor.group_name,
                case: Some(case.id),
                actor: Some(actor.id),
                key: &request.key,
                operation: "correct_decision_case",
                canonical: &canonical,
                result: &serde_json::to_value(&corrected)?,
                now,
            },
        )
        .await?;
        // Keep a separate immutable before/after record as well as retry bytes.
        record_audit_tx(&mut tx, AuditRecord {
            group: &actor.group_name,
            case: Some(case.id),
            actor: None,
            key: "case-correction-evidence",
            operation: "case_schedule_corrected",
            canonical: &json!({"actor":actor.id,"binding":actor.binding_version,"before":case,"reason":request.reason,"evidence":request.evidence}),
            result: &serde_json::to_value(&corrected)?,
            now,
        }).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(corrected)
    }
}

/// Revalidated relationship between a real scheduler cause and a persisted case.
/// Private fields prevent caller-created case strings from becoming authority.
#[derive(Debug)]
pub(crate) struct ValidatedExecutionCase {
    case: DecisionCase,
    cause: crate::execution::ExecutionCauseSnapshot,
}
impl ValidatedExecutionCase {
    pub(crate) fn case_id(&self) -> i64 {
        self.case.id
    }
    pub(crate) fn case_version(&self) -> i64 {
        self.case.version
    }
    pub(crate) fn group(&self) -> &str {
        &self.case.group
    }
    pub(crate) fn authority(&self) -> &str {
        &self.case.authority
    }
    pub(crate) fn decision_task(&self) -> Option<&str> {
        self.case.decision_task.as_deref()
    }
    pub(crate) fn guard(&self) -> &crate::execution::ExecutionSourceGuard {
        &self.cause.guard
    }
}

/// A current original-writer authorization, distinct from runtime closure.
#[derive(Debug)]
pub(crate) struct ValidatedCaseAuthority {
    case: ValidatedExecutionCase,
    actor: Mailbox,
}
impl ValidatedCaseAuthority {
    pub(crate) fn case(&self) -> &ValidatedExecutionCase {
        &self.case
    }
    pub(crate) fn actor(&self) -> &Mailbox {
        &self.actor
    }
}

async fn current_execution_cause_tx(
    tx: &mut Transaction<'_, Sqlite>,
    reference: &crate::execution::ExecutionCauseRef,
) -> Result<crate::execution::ExecutionCauseSnapshot> {
    use crate::execution::ExecutionCauseState;
    match crate::execution::inspect_execution_cause_tx(tx, reference).await? {
        ExecutionCauseState::Current(cause) => Ok(*cause),
        ExecutionCauseState::Superseded(_) => {
            anyhow::bail!("execution cause superseded; retain historical case")
        }
        ExecutionCauseState::Missing => {
            anyhow::bail!("execution cause missing; not supersession evidence")
        }
    }
}

/// Derive one case from an actual persisted scheduler cause. The scheduler owns
/// the acknowledgement in this same caller transaction. Its cause remains
/// independently current if materialization/ack composition is unavailable.
pub(crate) async fn ensure_execution_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    reference: &crate::execution::ExecutionCauseRef,
    now: i64,
) -> Result<DecisionCase> {
    use crate::execution::ExecutionSourceKind;
    reserve_home_tx(tx, &reference.group).await?;
    let cause = current_execution_cause_tx(tx, reference).await?;
    if let ExecutionSourceKind::MaterializedDecision { source, episode } = &cause.source_kind {
        let id = sqlx::query_scalar::<_, i64>("SELECT id FROM decision_cases WHERE group_name=? AND source_key=? AND episode=? AND decision_task=?")
            .bind(&reference.group).bind(source).bind(episode).bind(&reference.source_task)
            .fetch_optional(&mut **tx).await?.context("original decision case mapping missing; no recursive case permitted")?;
        let case = load_case_tx(tx, &reference.group, id).await?;
        ensure!(
            cause
                .handoff_case
                .as_deref()
                .is_none_or(|linked| linked == id.to_string()),
            "execution cause linked to another case"
        );
        escalate_execution_operator_tx(tx, &case, &cause, now).await?;
        return Ok(case);
    }
    let source = inspect_source_tx(
        tx,
        &reference.group,
        &Obligation::Task {
            id: reference.source_task.clone(),
            version: cause.guard.task_version,
        },
    )
    .await?;
    ensure!(
        source.input_epoch == Some(cause.guard.input_epoch)
            && source.candidate == cause.guard.candidate_id,
        "execution cause source guard mismatch"
    );
    let episode = format!("execution:{}", reference.cause_generation);
    if let Some(id) = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM decision_cases WHERE group_name=? AND source_key=? AND episode=?",
    )
    .bind(&reference.group)
    .bind(&source.source_key)
    .bind(&episode)
    .fetch_optional(&mut **tx)
    .await?
    {
        ensure!(
            cause
                .handoff_case
                .as_deref()
                .is_none_or(|linked| linked == id.to_string()),
            "execution cause linked to another case"
        );
        let case = load_case_tx(tx, &reference.group, id).await?;
        escalate_execution_operator_tx(tx, &case, &cause, now).await?;
        return Ok(case);
    }
    ensure!(
        cause.handoff_case.is_none(),
        "execution handoff references a missing recovery case"
    );
    let escalated = cause.cause.escalated
        || now >= cause.cause.hard_due
        || !source.authority_registered
        || !source.recipient_registered;
    let state = if escalated {
        "operator_required"
    } else {
        "held"
    };
    let guard = serde_json::to_string(&cause.guard)?;
    let snapshot = serde_json::to_string(&source)?;
    let id = sqlx::query("INSERT INTO decision_cases(group_name,source_kind,task,source_key,episode,authority,original_source,current_source,execution_source,execution_original_guard,execution_guard,opened,original_due,review_at,hard_due,state,capability_hold) VALUES(?,'task',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(&reference.group).bind(&reference.source_task).bind(&source.source_key).bind(&episode).bind(&source.authority)
        .bind(&snapshot).bind(&snapshot).bind(serde_json::to_string(reference)?).bind(&guard).bind(&guard).bind(now)
        .bind(cause.cause.hard_due).bind(cause.cause.review_at.min(cause.cause.hard_due)).bind(cause.cause.hard_due).bind(state)
        .bind("model_materialization_and_shared_notifier_unavailable")
        .execute(&mut **tx).await?.last_insert_rowid();
    sqlx::query("INSERT INTO operator_obligations(group_name,case_id,authority,opened,hard_due,state,reason,evidence) VALUES(?,?,?,?,?,?,?,?)")
        .bind(&reference.group).bind(id).bind(&source.authority).bind(now).bind(cause.cause.hard_due)
        .bind(if escalated { "escalated" } else { "pending" }).bind(&cause.cause.detail)
        .bind(serde_json::to_string(&vec![format!("execution-cause:{}", reference.cause_generation)])?)
        .execute(&mut **tx).await?;
    let case = load_case_tx(tx, &reference.group, id).await?;
    record_audit_tx(
        tx,
        AuditRecord {
            group: &reference.group,
            case: Some(id),
            actor: None,
            key: "execution-case",
            operation: "execution_case_opened",
            canonical: &json!({"guard":cause.guard,"code":cause.cause.code}),
            result: &serde_json::to_value(&case)?,
            now,
        },
    )
    .await?;
    Ok(case)
}

async fn escalate_execution_operator_tx(
    tx: &mut Transaction<'_, Sqlite>,
    case: &DecisionCase,
    cause: &crate::execution::ExecutionCauseSnapshot,
    now: i64,
) -> Result<()> {
    if !cause.cause.escalated && now < cause.cause.hard_due {
        return Ok(());
    }
    // Escalation does not revise source guards, graph edges or lifetime budgets.
    // The original operator obligation is reused for a decision's own cause.
    let result = sqlx::query("UPDATE operator_obligations SET state='escalated',version=version+1,hard_due=MIN(hard_due,?),reason=?,last_scan=? WHERE group_name=? AND case_id=? AND (state<>'escalated' OR hard_due>?)")
        .bind(cause.cause.hard_due).bind(&cause.cause.detail).bind(now).bind(&case.group).bind(case.id).bind(cause.cause.hard_due)
        .execute(&mut **tx).await?;
    if result.rows_affected() == 1 {
        record_audit_tx(
            tx,
            AuditRecord {
                group: &case.group,
                case: Some(case.id),
                actor: None,
                key: "execution-escalation",
                operation: "operator_escalated",
                canonical: &json!({"cause":cause.guard,"code":cause.cause.code}),
                result: &json!({"operator_obligation":case.operator_obligation}),
                now,
            },
        )
        .await?;
    }
    Ok(())
}

/// Revalidate actual cause/case/source linkage before scheduler acknowledgement
/// or disposition. A current proof is obtained from source rows, never from a
/// caller-supplied case string or serialized authority boolean.
pub(crate) async fn validate_execution_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case_id: i64,
    expected_version: i64,
    expected: &crate::execution::ExecutionSourceGuard,
) -> Result<ValidatedExecutionCase> {
    use crate::execution::ExecutionSourceKind;
    reserve_home_tx(tx, group).await?;
    ensure!(
        expected.cause_ref.group == group,
        "execution source group mismatch"
    );
    let case = load_case_tx(tx, group, case_id).await?;
    ensure!(
        case.version == expected_version,
        "decision case revision conflict"
    );
    ensure!(
        matches!(
            case.state.as_str(),
            "held" | "decision_pending" | "operator_required"
        ),
        "case no longer unresolved"
    );
    ensure!(!case.requires_reassessment, "source_reassessment_required");
    let cause = current_execution_cause_tx(tx, &expected.cause_ref).await?;
    ensure!(
        cause.guard == *expected,
        "execution source changed; inspect and reconsider"
    );
    ensure!(
        cause
            .handoff_case
            .as_deref()
            .is_none_or(|linked| linked == case_id.to_string()),
        "execution cause belongs to another case"
    );
    match &cause.source_kind {
        ExecutionSourceKind::Work => {
            ensure!(
                case.execution_source.as_ref() == Some(&serde_json::to_value(&expected.cause_ref)?),
                "case execution source mismatch"
            );
            ensure!(
                case.execution_guard.as_ref() == Some(&serde_json::to_value(expected)?),
                "case source guard requires explicit reassessment"
            );
            let source = inspect_source_tx(tx, group, &case.current_source.source).await?;
            ensure!(
                source.source == case.current_source.source
                    && source.input_epoch == case.current_source.input_epoch
                    && source.candidate == case.current_source.candidate
                    && source.outcome == case.current_source.outcome,
                "case original source changed; explicit reassessment required"
            );
            ensure!(
                source.authority == case.authority,
                "original authority changed"
            );
        }
        ExecutionSourceKind::MaterializedDecision { source, episode } => {
            ensure!(
                case.source_key == *source
                    && case.episode == *episode
                    && case.decision_task.as_deref()
                        == Some(expected.cause_ref.source_task.as_str()),
                "decision cause must map to its original case"
            );
        }
    }
    Ok(ValidatedExecutionCase { case, cause })
}

/// Authenticate source business authority independently from source/own-attempt
/// closure. Scheduler still validates the particular disposition and budgets.
pub(crate) async fn validate_case_authority_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    case_id: i64,
    expected_version: i64,
    expected: &crate::execution::ExecutionSourceGuard,
) -> Result<ValidatedCaseAuthority> {
    authenticate_tx(tx, actor).await?;
    let case =
        validate_execution_case_tx(tx, &actor.group_name, case_id, expected_version, expected)
            .await?;
    ensure!(
        case.authority() == actor.name,
        "only the original source authority may apply a decision"
    );
    // A decision task's own current cause proves responsibility, not that the
    // original business source/candidate is still current. Recheck it on EVERY
    // authorization path, including MaterializedDecision. Model still owns
    // immutable candidate-input validity and operation-specific authority.
    let original =
        inspect_source_tx(tx, &actor.group_name, &case.case.current_source.source).await?;
    ensure!(
        original.authority_id == actor.id && original.authority == case.case.authority,
        "original source authority changed"
    );
    ensure!(
        original.source == case.case.current_source.source
            && original.input_epoch == case.case.current_source.input_epoch
            && original.candidate == case.case.current_source.candidate
            && original.outcome == case.case.current_source.outcome,
        "original source or candidate changed; historical case mapping grants no disposition authority"
    );
    if let Some(stored) = &case.case.execution_guard {
        let original_guard: crate::execution::ExecutionSourceGuard =
            serde_json::from_value(stored.clone())?;
        let original_cause = current_execution_cause_tx(tx, &original_guard.cause_ref).await?;
        ensure!(
            original_cause.guard == original_guard,
            "original execution source changed; reassessment required"
        );
    }
    Ok(ValidatedCaseAuthority {
        case,
        actor: actor.clone(),
    })
}

/// Before any successful case/decision outcome, recheck the case and decision's
/// OWN execution closure. The scheduler also guards source success separately.
/// Negative source cancellation may use authority alone while cleanup remains;
/// it must not mark this case/decision successfully handled via that path.
pub(crate) async fn guard_case_completion_tx(
    tx: &mut Transaction<'_, Sqlite>,
    authority: &ValidatedCaseAuthority,
) -> Result<()> {
    let current = validate_case_authority_tx(
        tx,
        authority.actor(),
        authority.case().case_id(),
        authority.case().case_version(),
        authority.case().guard(),
    )
    .await?;
    if let Some(task) = current.case().decision_task() {
        crate::execution::guard_success_tx(tx, current.case().group(), task).await?;
    }
    Ok(())
}

/// Model-owned mutations use this pre-state proof around mandatory reconciliation.
/// It validates recovery facts only; policy, actor and candidate authority remain
/// with the actual model mutation. No caller can deserialize or mint this proof.
#[derive(Debug)]
pub(crate) struct ValidatedDecisionChange {
    case: DecisionCase,
    decision_work: crate::work::WorkItem,
    decision_model: crate::task_graph::TaskModel,
    blockers: Vec<DecisionBlockerRow>,
    kind: DecisionChangeKind,
    operator_escalated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecisionChangeKind {
    Candidate,
    WriterFallback,
    Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
struct DecisionBlockerRow {
    case_version: i64,
    ordinal: i64,
    selector: String,
    waiting: Option<String>,
    responsible: String,
    reason: String,
    evidence: String,
}

/// Must run before the model mutation and its event, in the same writer tx.
/// Existing reassessment holds require their own source-authorized correction;
/// this cannot retroactively bless an unwrapped historical candidate or phase.
pub(crate) async fn prepare_decision_change_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    case_id: i64,
    version: i64,
    task: &str,
    kind: DecisionChangeKind,
    now: i64,
) -> Result<ValidatedDecisionChange> {
    reserve_home_tx(tx, group).await?;
    let case = load_case_tx(tx, group, case_id).await?;
    ensure!(
        case.version == version && case.decision_task.as_deref() == Some(task),
        "decision change case/task revision conflict"
    );
    ensure!(
        matches!(
            case.state.as_str(),
            "held" | "decision_pending" | "operator_required"
        ) && !case.requires_reassessment,
        "source_reassessment_required"
    );
    if kind != DecisionChangeKind::Outcome {
        ensure!(
            now < case.hard_due && case.state != "operator_required",
            "decision phase boundary expired"
        );
    }
    let source = inspect_source_tx(tx, group, &case.current_source.source).await?;
    ensure!(
        source == case.current_source
            && source.unresolved
            && source.authority_registered
            && source.authority == case.authority,
        "original decision source changed or unavailable"
    );
    if let Some(guard) = &case.execution_guard {
        let guard: crate::execution::ExecutionSourceGuard = serde_json::from_value(guard.clone())?;
        validate_execution_case_tx(tx, group, case_id, version, &guard).await?;
    } else {
        ensure!(
            case.episode == "obligation" && source.input_epoch.is_none(),
            "actual scheduler episode required"
        );
    }
    let mapping: i64 = sqlx::query_scalar("SELECT count(*) FROM task_materializations WHERE group_name=? AND source=? AND episode=? AND decision_task=?")
        .bind(group).bind(&case.source_key).bind(&case.episode).bind(task).fetch_one(&mut **tx).await?;
    ensure!(
        mapping == 1,
        "actual decision materialization mapping missing"
    );
    let observed = inspect_source_tx(
        tx,
        group,
        &Obligation::Task {
            id: task.into(),
            version: 1,
        },
    )
    .await?;
    let Obligation::Task {
        version: decision_version,
        ..
    } = observed.source
    else {
        unreachable!()
    };
    let expected = crate::task_graph::TaskSourceExpectation {
        group: group.into(),
        task: task.into(),
        task_version: decision_version,
        input_epoch: observed.input_epoch.context("decision contract missing")?,
        current_candidate: observed.candidate,
        current_outcome: observed.outcome,
    };
    let decision = crate::task_graph::validate_task_source_tx(tx, &expected).await?;
    ensure!(
        decision.work.writer == case.authority && decision.work.state.is_open(),
        "decision task is not open under original writer"
    );
    if kind == DecisionChangeKind::WriterFallback {
        let advanced: i64 = sqlx::query_scalar(
            "SELECT reviewer_advanced FROM decision_cases WHERE group_name=? AND id=?",
        )
        .bind(group)
        .bind(case_id)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            advanced == 0 && decision.work.owner != case.authority,
            "decision writer fallback already used"
        );
    }
    // Validate genuine current typed sources before the hook removes their rows.
    recovery_blocking_edges_tx(tx, group).await?;
    let blockers = sqlx::query_as::<_, DecisionBlockerRow>("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name=? AND case_id=? ORDER BY ordinal")
        .bind(group).bind(case_id).fetch_all(&mut **tx).await?;
    ensure!(
        blockers.len() <= 32 && blockers.iter().all(|row| row.case_version == version),
        "stale decision blockers before model mutation"
    );
    let operator_state: String = sqlx::query_scalar(
        "SELECT state FROM operator_obligations WHERE group_name=? AND id=? AND case_id=?",
    )
    .bind(group)
    .bind(case.operator_obligation)
    .bind(case_id)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        matches!(operator_state.as_str(), "pending" | "escalated"),
        "operator responsibility already settled"
    );
    Ok(ValidatedDecisionChange {
        case,
        decision_work: decision.work,
        decision_model: decision.model.context("decision model missing")?,
        blockers,
        kind,
        operator_escalated: operator_state == "escalated",
    })
}

/// Validate the real event and hook output against the captured pre-state.
/// The returned version is read from the model witness, never inferred as +1.
async fn decision_change_after_tx(
    tx: &mut Transaction<'_, Sqlite>,
    before: &ValidatedDecisionChange,
    applied: &crate::task_graph::AppliedModelDecision,
) -> Result<(DecisionCase, crate::task_graph::TaskView, i64)> {
    crate::task_graph::validate_applied_model_decision_tx(tx, applied).await?;
    ensure!(
        applied.group() == before.case.group && applied.task() == before.decision_work.id,
        "model receipt belongs to another decision"
    );
    let (event, task_version) = applied
        .root_event()
        .context("decision root event missing")?;
    let (case_version, case_event) = applied
        .case_after(before.case.id)
        .context("decision reconciliation receipt missing")?;
    ensure!(
        case_event == event,
        "decision receipt includes another source change"
    );
    let row = sqlx::query("SELECT snapshot,previous_snapshot FROM task_model_events WHERE group_name=? AND task=? AND id=? AND operation=?")
        .bind(&before.case.group).bind(&before.decision_work.id).bind(event).bind(applied.operation())
        .fetch_one(&mut **tx).await?;
    let prior: (crate::work::WorkItem, crate::task_graph::TaskModel) = serde_json::from_str(
        &row.get::<Option<String>, _>("previous_snapshot")
            .context("decision previous model missing")?,
    )?;
    ensure!(
        serde_json::to_value(&prior)?
            == serde_json::to_value((&before.decision_work, &before.decision_model))?,
        "model decision pre-state changed"
    );
    let after: crate::task_graph::TaskView =
        serde_json::from_str(&row.get::<String, _>("snapshot"))?;
    ensure!(
        after.work.version == task_version,
        "decision event version mismatch"
    );
    let audits: Vec<String> = sqlx::query_scalar("SELECT canonical FROM decision_audit WHERE group_name=? AND case_id=? AND operation='model_source_reconciled' AND json_extract(canonical,'$.model_event')=?")
        .bind(&before.case.group).bind(before.case.id).bind(event).fetch_all(&mut **tx).await?;
    ensure!(
        audits.len() == 1,
        "exact recovery reconciliation audit missing"
    );
    let audit: Value = serde_json::from_str(&audits[0])?;
    ensure!(
        audit["before"] == serde_json::to_value(&before.case)?
            && audit["removed_blockers"] == serde_json::to_value(&before.blockers)?,
        "recovery pre-state or scoped blockers changed before model event"
    );
    let current = load_case_tx(tx, &before.case.group, before.case.id).await?;
    ensure!(
        current.version == case_version
            && current.requires_reassessment
            && current.decision_task == before.case.decision_task
            && current.execution_source == before.case.execution_source
            && current.execution_guard == before.case.execution_guard,
        "reconciled decision case changed"
    );
    let original =
        inspect_source_tx(tx, &before.case.group, &before.case.current_source.source).await?;
    ensure!(
        original == before.case.current_source && current.current_source == original,
        "original source changed during decision mutation"
    );
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM decision_blockers WHERE group_name=? AND case_id=?",
    )
    .bind(&before.case.group)
    .bind(before.case.id)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        remaining == 0,
        "unexpected blockers after model reconciliation"
    );
    Ok((current, after, event))
}

/// Reconcile a model-authorized candidate or single reviewer-to-writer change.
/// Call model scheduler synchronization again after this and the full graph
/// refresh, before committing. Old attempts/slots remain scheduler-owned.
pub(crate) async fn finalize_decision_change_tx(
    tx: &mut Transaction<'_, Sqlite>,
    before: &ValidatedDecisionChange,
    applied: &crate::task_graph::AppliedModelDecision,
    now: i64,
) -> Result<DecisionCase> {
    ensure!(
        before.kind != DecisionChangeKind::Outcome,
        "outcome needs applied source disposition"
    );
    ensure!(
        now < before.case.hard_due,
        "decision phase boundary expired"
    );
    let (current, after, event) = decision_change_after_tx(tx, before, applied).await?;
    let model = after
        .model
        .as_ref()
        .context("decision after-model missing")?;
    ensure!(
        after.work.state.is_open()
            && applied.outcome().is_none()
            && after.work.writer == before.decision_work.writer
            && after.work.scope == before.decision_work.scope
            && after.work.deadline == before.decision_work.deadline
            && model.contract == before.decision_model.contract
            && model.authorization == before.decision_model.authorization
            && model.requirements == before.decision_model.requirements
            && model.parent == before.decision_model.parent,
        "decision phase changed source authority, scope, result or finite limits"
    );
    match before.kind {
        DecisionChangeKind::Candidate => ensure!(
            after.work.owner == before.decision_work.owner
                && model.input_epoch == before.decision_model.input_epoch
                && model.current_candidate.is_some()
                && model.current_candidate != before.decision_model.current_candidate,
            "candidate transition mismatch"
        ),
        DecisionChangeKind::WriterFallback => ensure!(
            after.work.owner == before.case.authority
                && before.decision_work.owner != before.case.authority
                && model.input_epoch > before.decision_model.input_epoch,
            "writer fallback transition mismatch"
        ),
        DecisionChangeKind::Outcome => unreachable!(),
    }
    // A phase never changes the original cause; unlike a source disposition,
    // it must still have the real original scheduler guard after the model event.
    if let Some(guard) = &before.case.execution_guard {
        let guard: crate::execution::ExecutionSourceGuard = serde_json::from_value(guard.clone())?;
        ensure!(
            current_execution_cause_tx(tx, &guard.cause_ref)
                .await?
                .guard
                == guard,
            "original execution source changed during decision phase"
        );
    }
    let next = current
        .version
        .checked_add(1)
        .context("case revision overflow")?;
    let changed = sqlx::query("UPDATE decision_cases SET version=?,requires_reassessment=0,state=?,capability_hold=?,reviewer_advanced=MAX(reviewer_advanced,?),last_scan=0 WHERE group_name=? AND id=? AND version=? AND reassessment_event=? AND requires_reassessment=1")
        .bind(next).bind(&before.case.state).bind(&before.case.capability_hold)
        .bind(i64::from(before.kind == DecisionChangeKind::WriterFallback))
        .bind(&before.case.group).bind(before.case.id).bind(current.version).bind(event).execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == 1,
        "decision phase case CAS conflict"
    );
    for blocker in &before.blockers {
        let responsible = if before.kind == DecisionChangeKind::WriterFallback
            && blocker.responsible == before.decision_work.owner
        {
            &before.case.authority
        } else {
            &blocker.responsible
        };
        sqlx::query("INSERT INTO decision_blockers(group_name,case_id,case_version,ordinal,selector,waiting,responsible,reason,evidence) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(&before.case.group).bind(before.case.id).bind(next).bind(blocker.ordinal)
            .bind(&blocker.selector).bind(&blocker.waiting).bind(responsible).bind(&blocker.reason).bind(&blocker.evidence)
            .execute(&mut **tx).await?;
    }
    let changed = sqlx::query("UPDATE operator_obligations SET version=version+1,state=CASE WHEN state='escalated' OR ? THEN 'escalated' ELSE 'pending' END,reason=?,evidence=?,last_scan=0 WHERE group_name=? AND id=? AND case_id=? AND state IN ('pending','escalated')")
        .bind(before.operator_escalated).bind("decision phase changed under actual model authority; original source responsibility remains")
        .bind(serde_json::to_string(&vec![format!("model-event:{event}")])?).bind(&before.case.group)
        .bind(before.case.operator_obligation).bind(before.case.id).execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == 1,
        "decision phase operator responsibility conflict"
    );
    crate::task_graph::refresh_recovery_projection_tx(
        tx,
        &before.case.group,
        &std::collections::BTreeMap::from([(before.case.id, next)]),
    )
    .await?;
    let result = load_case_tx(tx, &before.case.group, before.case.id).await?;
    // Keep the actual restored partition beside the genuine model event. In
    // particular, writer fallback transfers only the previous owner's rows;
    // neither an inferred after-state nor a case-only result proves that detail.
    let blockers_after = sqlx::query_as::<_, DecisionBlockerRow>("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name=? AND case_id=? ORDER BY ordinal")
        .bind(&before.case.group).bind(before.case.id).fetch_all(&mut **tx).await?;
    record_audit_tx(tx, AuditRecord {
        group: &before.case.group,
        case: Some(before.case.id),
        actor: None,
        key: applied.operation(),
        operation: "decision_phase_reconciled",
        canonical: &json!({"before":before.case,"model_event":event,"model_actor":applied.actor(),
            "kind":format!("{:?}",before.kind),"blockers":before.blockers,
            "owner_before":before.decision_work.owner,"owner_after":after.work.owner,
            "blockers_after":blockers_after}),
        result: &serde_json::to_value(&result)?,
        now,
    }).await?;
    Ok(result)
}

/// Complete only a Work cause after both actual source disposition and ordinary
/// decision outcome. Validate both private after-state witnesses BEFORE changing
/// their referenced case. Historical outer retries must use the owning receipt.
pub(crate) async fn finalize_applied_decision_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    before: &ValidatedDecisionChange,
    execution: &crate::execution::AppliedDisposition,
    model: &crate::task_graph::AppliedModelDecision,
    now: i64,
) -> Result<DecisionCase> {
    authenticate_tx(tx, actor).await?;
    ensure!(
        before.kind == DecisionChangeKind::Outcome
            && actor.group_name == before.case.group
            && actor.name == before.case.authority
            && actor.id == before.case.original_source.authority_id,
        "original source authority required for decision completion"
    );
    crate::execution::validate_applied_disposition_tx(tx, execution).await?;
    ensure!(
        matches!(
            execution.source_kind(),
            crate::execution::ExecutionSourceKind::Work
        ) && execution.group() == before.case.group
            && execution.actor_id() == actor.id
            && execution.case_id() == before.case.id
            && execution.case_version() == before.case.version
            && before.case.execution_guard.as_ref()
                == Some(&serde_json::to_value(execution.source_before())?),
        "applied source does not handle this original Work case"
    );
    let (current, after, event) = decision_change_after_tx(tx, before, model).await?;
    let (outcome, kind) = model.outcome().context("actual decision outcome missing")?;
    ensure!(
        model.actor() == actor.name
            && matches!(
                kind,
                crate::task_graph::OutcomeKind::Accepted
                    | crate::task_graph::OutcomeKind::Completed
            )
            && !after.work.state.is_open()
            && Some(outcome)
                == after
                    .model
                    .as_ref()
                    .and_then(|m| m.current_outcome.as_deref()),
        "ordinary decision successful outcome required"
    );
    crate::execution::guard_success_tx(tx, &before.case.group, &before.decision_work.id).await?;
    crate::execution::guard_success_tx(
        tx,
        &before.case.group,
        &execution.source_before().cause_ref.source_task,
    )
    .await?;
    let next = current
        .version
        .checked_add(1)
        .context("case revision overflow")?;
    let changed = sqlx::query("UPDATE decision_cases SET version=?,state='handled',requires_reassessment=0,capability_hold=NULL,last_scan=? WHERE group_name=? AND id=? AND version=? AND reassessment_event=? AND requires_reassessment=1")
        .bind(next).bind(now).bind(&before.case.group).bind(before.case.id).bind(current.version).bind(event).execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == 1,
        "decision outcome case CAS conflict"
    );
    let changed = sqlx::query("UPDATE operator_obligations SET version=version+1,state='handled',reason=?,evidence=?,last_scan=? WHERE group_name=? AND id=? AND case_id=? AND state IN ('pending','escalated')")
        .bind("original writer committed source disposition and actual decision outcome")
        .bind(serde_json::to_string(&vec![format!("model-event:{event}"),format!("execution-event:{}",execution.execution_event())])?)
        .bind(now).bind(&before.case.group).bind(before.case.operator_obligation).bind(before.case.id).execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == 1,
        "decision outcome operator responsibility conflict"
    );
    crate::task_graph::refresh_recovery_projection_tx(
        tx,
        &before.case.group,
        &std::collections::BTreeMap::from([(before.case.id, next)]),
    )
    .await?;
    let result = load_case_tx(tx, &before.case.group, before.case.id).await?;
    record_audit_tx(tx, AuditRecord {
        group: &before.case.group,
        case: Some(before.case.id),
        actor: None,
        key: model.operation(),
        operation: "decision_source_and_outcome_applied",
        canonical: &json!({"before":before.case,"actor":actor.id,"binding":actor.binding_version,
            "execution":execution.audit()?,"model_event":event,"model_outcome":outcome}),
        result: &serde_json::to_value(&result)?,
        now,
    }).await?;
    Ok(result)
}

/// Exact source-authorized replacement of this decision case's typed waits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionBlockersRequest {
    /// Immutable actor-scoped retry identity.
    pub key: String,
    /// Existing materialized recovery case.
    pub case_id: i64,
    /// Exact observed recovery revision.
    pub case_version: i64,
    /// Complete bounded set; order supplies stable ordinals.
    pub blockers: Vec<ScopedBlocker>,
    /// Source authority's explanation.
    pub reason: String,
}

async fn advance_case_version_tx(
    tx: &mut Transaction<'_, Sqlite>,
    case: &DecisionCase,
) -> Result<i64> {
    sqlx::query_scalar("UPDATE decision_cases SET version=version+1 WHERE group_name=? AND id=? AND version=? AND version<9223372036854775807 RETURNING version")
        .bind(&case.group).bind(case.id).bind(case.version)
        .fetch_optional(&mut **tx).await?.context("decision case revision conflict or overflow")
}

impl Store {
    /// Persist real typed blockers under the original source authority.
    ///
    /// The full replacement, case CAS, named projection and audit commit together.
    /// No source/decision lifetime, operator responsibility or budget is changed.
    ///
    /// # Errors
    /// Rejects stale or unauthorized sources, expired cases, untyped or undeclared
    /// actions, unrelated responsibility, graph cycles and conflicting retries.
    pub async fn set_decision_blockers(
        &self,
        actor: &Mailbox,
        request: DecisionBlockersRequest,
        now: i64,
    ) -> Result<DecisionCase> {
        required(&request.key, 128, "blocker key")?;
        required(&request.reason, 4096, "blocker reason")?;
        ensure!(request.blockers.len() <= 32, "case blocker bound exceeded");
        let canonical = json!({"operation":"set_decision_blockers","request":request});
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        if let Some(old) = replay_tx(&mut tx, actor, &request.key, &canonical).await? {
            return Ok(serde_json::from_value(old)?);
        }
        let case = load_case_tx(&mut tx, &actor.group_name, request.case_id).await?;
        ensure!(
            case.version == request.case_version && now >= 0 && now < case.hard_due,
            "blocker case revision or finite boundary conflict"
        );
        ensure!(
            actor.id == case.original_source.authority_id && actor.name == case.authority,
            "original source authority required for blockers"
        );
        ensure!(
            matches!(&case.current_source.source, Obligation::Task { .. })
                && case.current_source.input_epoch.is_some(),
            "typed blocker writer supports contracted Work only"
        );
        let guard: crate::execution::ExecutionSourceGuard = serde_json::from_value(
            case.execution_guard
                .clone()
                .context("actual Work execution guard required")?,
        )?;
        let authority =
            validate_case_authority_tx(&mut tx, actor, case.id, case.version, &guard).await?;
        ensure!(
            matches!(
                &authority.case.cause.source_kind,
                crate::execution::ExecutionSourceKind::Work
            ),
            "typed blocker writer requires original Work cause"
        );
        let task = case
            .decision_task
            .as_deref()
            .context("actual materialized decision required")?;
        let before = prepare_decision_change_tx(
            &mut tx,
            &actor.group_name,
            case.id,
            case.version,
            task,
            DecisionChangeKind::Outcome,
            now,
        )
        .await?;
        // Validate each full typed specification before writing; the actual owner
        // enumerator and model graph below validate its source/alias semantics.
        for blocker in &request.blockers {
            ensure!(
                blocker.waiting.is_some(),
                "typed blocker prerequisite required"
            );
            required(&blocker.responsible, 128, "blocker responsible")?;
            required(&blocker.reason, 512, "blocker reason")?;
            evidence_valid(&blocker.evidence)?;
        }
        let version = advance_case_version_tx(&mut tx, &case).await?;
        let removed = sqlx::query(
            "DELETE FROM decision_blockers WHERE group_name=? AND case_id=? AND case_version=?",
        )
        .bind(&actor.group_name)
        .bind(case.id)
        .bind(case.version)
        .execute(&mut *tx)
        .await?;
        ensure!(
            removed.rows_affected() == u64::try_from(before.blockers.len())?,
            "blocker replacement source rows changed"
        );
        for (ordinal, blocker) in request.blockers.iter().enumerate() {
            sqlx::query("INSERT INTO decision_blockers(group_name,case_id,case_version,ordinal,selector,waiting,responsible,reason,evidence) VALUES(?,?,?,?,?,?,?,?,?)")
                .bind(&actor.group_name).bind(case.id).bind(version).bind(i64::try_from(ordinal)?)
                .bind(serde_json::to_string(&blocker.selector)?)
                .bind(blocker.waiting.as_ref().map(serde_json::to_string).transpose()?)
                .bind(&blocker.responsible).bind(&blocker.reason)
                .bind(serde_json::to_string(&blocker.evidence)?).execute(&mut *tx).await?;
        }
        crate::task_graph::refresh_recovery_projection_tx(
            &mut tx,
            &actor.group_name,
            &std::collections::BTreeMap::from([(case.id, version)]),
        )
        .await?;
        // Source semantics restrict every blocker consumer to this source or
        // its materialized decision. Synchronize those actual consumers; the
        // scheduler owns ancestor accounting, stop intent and slot retention.
        let mut consumers = std::collections::BTreeSet::from([task.to_owned()]);
        if case.current_source.input_epoch.is_some() {
            if let Obligation::Task { id, .. } = &case.current_source.source {
                consumers.insert(id.clone());
            }
        }
        crate::execution::sync_model_tx(
            &mut tx,
            &actor.group_name,
            &consumers.into_iter().collect::<Vec<_>>(),
            now,
        )
        .await?;
        let result = load_case_tx(&mut tx, &actor.group_name, case.id).await?;
        record_audit_tx(
            &mut tx,
            AuditRecord {
                group: &actor.group_name,
                case: Some(case.id),
                actor: None,
                key: &request.key,
                operation: "decision_blockers_replaced",
                canonical: &json!({"actor":actor.id,"binding":actor.binding_version,"before":case,
                "original_blockers":before.blockers,"request":request}),
                result: &serde_json::to_value(&result)?,
                now,
            },
        )
        .await?;
        record_audit_tx(
            &mut tx,
            AuditRecord {
                group: &actor.group_name,
                case: Some(case.id),
                actor: Some(actor.id),
                key: &request.key,
                operation: "set_decision_blockers",
                canonical: &canonical,
                result: &serde_json::to_value(&result)?,
                now,
            },
        )
        .await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
}

/// Private to a single uncommitted model continuation transaction.
/// On Held the caller MUST roll back the entire transaction, including observations.
#[derive(Debug)]
pub(crate) struct StagedDecisionContinuation {
    original: ValidatedDecisionChange,
    intermediate: ValidatedDecisionChange,
    authority: ValidatedCaseAuthority,
    removed: Vec<DecisionBlockerRow>,
    audit_id: i64,
    audit_canonical: Value,
    audit_result: Value,
}

impl StagedDecisionContinuation {
    pub(crate) fn authority(&self) -> &ValidatedCaseAuthority {
        &self.authority
    }
}

fn same_decision_change(a: &ValidatedDecisionChange, b: &ValidatedDecisionChange) -> Result<bool> {
    Ok(a.case == b.case
        && a.blockers == b.blockers
        && a.kind == b.kind
        && a.operator_escalated == b.operator_escalated
        && serde_json::to_value((&a.decision_work, &a.decision_model))?
            == serde_json::to_value((&b.decision_work, &b.decision_model))?)
}

/// Stage only original Work Execute -> its actual decision AcceptResult rows.
/// The caller owns policy/outcome authorization and must retain all before-component
/// task IDs plus original source as final graph/scheduler synchronization seeds.
/// No caller may commit a stage without the actual outcome and staged finalizer.
pub(crate) async fn stage_decision_continuation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    original: ValidatedDecisionChange,
    authority: ValidatedCaseAuthority,
    now: i64,
) -> Result<StagedDecisionContinuation> {
    use crate::task_graph::{ActionNode, TaskAction};
    authenticate_tx(tx, actor).await?;
    ensure!(
        original.kind == DecisionChangeKind::Outcome && now >= 0 && now < original.case.hard_due,
        "finite Outcome staging proof required"
    );
    ensure!(
        authority.actor.id == actor.id
            && authority.actor.binding_version == actor.binding_version
            && authority.actor.group_name == actor.group_name
            && authority.actor.name == actor.name
            && authority.case.case == original.case
            && matches!(
                &authority.case.cause.source_kind,
                crate::execution::ExecutionSourceKind::Work
            ),
        "staging requires original Work case authority"
    );
    let current = prepare_decision_change_tx(
        tx,
        &actor.group_name,
        original.case.id,
        original.case.version,
        &original.decision_work.id,
        DecisionChangeKind::Outcome,
        now,
    )
    .await?;
    ensure!(
        same_decision_change(&original, &current)?,
        "staging original proof changed"
    );
    let guard = authority.case.guard().clone();
    validate_case_authority_tx(tx, actor, original.case.id, original.case.version, &guard).await?;
    let Obligation::Task { id: source, .. } = &original.case.current_source.source else {
        anyhow::bail!("staging requires original Work task");
    };
    let mut removed = Vec::new();
    let mut retained = Vec::new();
    for row in &original.blockers {
        let selector: ActionNode = serde_json::from_str(&row.selector)?;
        let waiting: ActionNode = serde_json::from_str(
            row.waiting
                .as_deref()
                .context("staging requires typed blocker prerequisite")?,
        )?;
        if selector.task == *source
            && selector.action == TaskAction::Execute
            && waiting.task == original.decision_work.id
            && waiting.action == TaskAction::AcceptResult
        {
            removed.push(row.clone());
        } else {
            retained.push(row.clone());
        }
    }
    let version = advance_case_version_tx(tx, &original.case).await?;
    for row in &removed {
        let changed = sqlx::query("DELETE FROM decision_blockers WHERE group_name=? AND case_id=? AND case_version=? AND ordinal=?")
            .bind(&actor.group_name).bind(original.case.id).bind(original.case.version)
            .bind(row.ordinal).execute(&mut **tx).await?;
        ensure!(changed.rows_affected() == 1, "staged blocker row changed");
    }
    let changed = sqlx::query("UPDATE decision_blockers SET case_version=? WHERE group_name=? AND case_id=? AND case_version=?")
        .bind(version).bind(&actor.group_name).bind(original.case.id).bind(original.case.version)
        .execute(&mut **tx).await?;
    ensure!(
        changed.rows_affected() == u64::try_from(retained.len())?,
        "retained staged blockers changed"
    );
    for row in &mut retained {
        row.case_version = version;
    }
    crate::task_graph::refresh_recovery_projection_tx(
        tx,
        &actor.group_name,
        &std::collections::BTreeMap::from([(original.case.id, version)]),
    )
    .await?;
    let intermediate = prepare_decision_change_tx(
        tx,
        &actor.group_name,
        original.case.id,
        version,
        &original.decision_work.id,
        DecisionChangeKind::Outcome,
        now,
    )
    .await?;
    ensure!(
        intermediate.blockers == retained,
        "intermediate blocker partition changed"
    );
    let authority =
        validate_case_authority_tx(tx, actor, original.case.id, version, &guard).await?;
    let audit_canonical = json!({"actor":actor.id,"binding":actor.binding_version,
        "before":original.case,"decision_work":original.decision_work,
        "decision_model":original.decision_model,"original_blockers":original.blockers});
    let audit_result =
        json!({"intermediate":intermediate.case,"removed":removed,"retained":retained});
    let audit_id: i64 = sqlx::query_scalar("INSERT INTO decision_audit(group_name,case_id,actor,key,operation,canonical,result,created) VALUES(?,?,NULL,'continuation-stage','decision_continuation_staged',?,?,?) RETURNING id")
        .bind(&actor.group_name).bind(original.case.id).bind(serde_json::to_string(&audit_canonical)?)
        .bind(serde_json::to_string(&audit_result)?).bind(now).fetch_one(&mut **tx).await?;
    Ok(StagedDecisionContinuation {
        original,
        intermediate,
        authority,
        removed,
        audit_id,
        audit_canonical,
        audit_result,
    })
}

/// Consume one actual stage and genuine scheduler/model after-state receipts.
/// The unchanged model hook must audit intermediate case + retained blocker rows.
/// Caller reloads/synchronizes every before-component task and original source,
/// saves the final outer receipt, and commits only after this succeeds.
pub(crate) async fn finalize_staged_decision_continuation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    stage: StagedDecisionContinuation,
    execution: &crate::execution::AppliedDisposition,
    model: &crate::task_graph::AppliedModelDecision,
    now: i64,
) -> Result<DecisionCase> {
    authenticate_tx(tx, actor).await?;
    ensure!(
        stage.authority.actor.id == actor.id
            && stage.authority.actor.binding_version == actor.binding_version
            && stage.authority.actor.group_name == actor.group_name
            && stage.authority.actor.name == actor.name,
        "staging actor changed"
    );
    let row = sqlx::query("SELECT canonical,result FROM decision_audit WHERE id=? AND group_name=? AND case_id=? AND operation='decision_continuation_staged'")
        .bind(stage.audit_id).bind(&actor.group_name).bind(stage.original.case.id)
        .fetch_one(&mut **tx).await?;
    ensure!(
        serde_json::from_str::<Value>(&row.get::<String, _>("canonical"))? == stage.audit_canonical
            && serde_json::from_str::<Value>(&row.get::<String, _>("result"))?
                == stage.audit_result,
        "staging immutable audit mismatch"
    );
    let mut reconstructed = stage.removed.clone();
    for row in &stage.intermediate.blockers {
        let mut prior = row.clone();
        ensure!(
            prior.case_version == stage.intermediate.case.version,
            "staging retained revision changed"
        );
        prior.case_version = stage.original.case.version;
        reconstructed.push(prior);
    }
    reconstructed.sort_by_key(|row| row.ordinal);
    ensure!(
        reconstructed == stage.original.blockers,
        "staging original blocker partition mismatch"
    );
    let mut restored_case = stage.intermediate.case.clone();
    restored_case.version = stage.original.case.version;
    ensure!(
        restored_case == stage.original.case
            && stage.authority.case.case == stage.intermediate.case
            && serde_json::to_value((
                &stage.original.decision_work,
                &stage.original.decision_model
            ))? == serde_json::to_value((
                &stage.intermediate.decision_work,
                &stage.intermediate.decision_model
            ))?,
        "staging changed source/model responsibility"
    );
    let result =
        finalize_applied_decision_tx(tx, actor, &stage.intermediate, execution, model, now).await?;
    record_audit_tx(tx, AuditRecord {group:&actor.group_name,case:Some(result.id),actor:None,
        key:model.operation(),operation:"decision_continuation_stage_completed",
        canonical:&json!({"stage_audit":stage.audit_id,"original_case_version":stage.original.case.version,
            "intermediate_case_version":stage.intermediate.case.version,"model_event":model.root_event(),
            "execution_event":execution.execution_event()}),result:&serde_json::to_value(&result)?,now}).await?;
    Ok(result)
}

/// One recovery projection source changed by an immutable model event.
#[derive(Debug)]
pub(crate) struct RecoverySourceChange {
    pub(crate) source: String,
    pub(crate) case_id: i64,
    pub(crate) case_version: i64,
    pub(crate) model_event: i64,
}

/// A source-change hold is an admission constraint, not a dependency edge. It
/// cannot introduce a graph cycle that prevents a negative source correction.
#[derive(Debug)]
pub(crate) struct RecoveryAdmissionHold {
    pub(crate) case_id: i64,
    pub(crate) case_version: i64,
    pub(crate) responsible: String,
    pub(crate) model_event: i64,
    pub(crate) reason: String,
}

/// Reconcile the complete affected model operation from real immutable events.
///
/// Model ordering: persist work/model rows AND task_model_events; call this hook;
/// refresh only returned recovery sources plus changed model sources; validate
/// full graph; run scheduler sync; commit all together. This hook never writes
/// task authority, results, scheduler slots or budgets. It cannot make a stale
/// case current merely from caller-supplied before/after snapshots.
pub(crate) async fn reconcile_model_changes_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    operation: &str,
    now: i64,
) -> Result<Vec<RecoverySourceChange>> {
    required(operation, 128, "model operation")?;
    reserve_home_tx(tx, group).await?;
    let events = sqlx::query("SELECT id,task,task_version,input_epoch,snapshot,previous_snapshot,reason,actor,origin FROM task_model_events WHERE group_name=? AND operation=? ORDER BY id LIMIT 1001")
        .bind(group).bind(operation).fetch_all(&mut **tx).await?;
    ensure!(
        !events.is_empty() && events.len() <= 1000,
        "model operation missing or affected component exceeds recovery bound"
    );
    let mut changes = std::collections::BTreeMap::new();
    for event in events {
        let event_id: i64 = event.get("id");
        let task: String = event.get("task");
        let after: crate::task_graph::TaskView =
            serde_json::from_str(&event.get::<String, _>("snapshot"))?;
        let model = after
            .model
            .as_ref()
            .context("model event has no contracted source")?;
        let current = inspect_source_tx(
            tx,
            group,
            &Obligation::Task {
                id: task.clone(),
                version: event.get("task_version"),
            },
        )
        .await?;
        ensure!(
            after.work.group_name == group
                && after.work.id == task
                && after.work.version == event.get::<i64, _>("task_version")
                && model.input_epoch == event.get::<i64, _>("input_epoch")
                && current.source
                    == (Obligation::Task {
                        id: task.clone(),
                        version: after.work.version
                    })
                && current.input_epoch == Some(model.input_epoch)
                && current.candidate == model.current_candidate
                && current.outcome == model.current_outcome
                && current.unresolved == after.work.state.is_open(),
            "model event no longer describes current authoritative source"
        );
        let rows = sqlx::query("SELECT id,requires_reassessment,reassessment_event FROM decision_cases WHERE group_name=? AND (task=? OR decision_task=?) ORDER BY id LIMIT 1001")
            .bind(group).bind(&task).bind(&task).fetch_all(&mut **tx).await?;
        ensure!(rows.len() <= 1000, "affected recovery case bound exceeded");
        for row in rows {
            let id: i64 = row.get("id");
            if row.get::<Option<i64>, _>("reassessment_event") == Some(event_id) {
                continue;
            }
            let replayed: i64 = sqlx::query_scalar("SELECT count(*) FROM decision_audit WHERE group_name=? AND case_id=? AND operation='model_source_reconciled' AND json_extract(canonical,'$.model_event')=?")
                .bind(group).bind(id).bind(event_id).fetch_one(&mut **tx).await?;
            if replayed != 0 {
                continue;
            }
            let before = load_case_tx(tx, group, id).await?;
            let source_changed =
                matches!(&before.current_source.source, Obligation::Task { id, .. } if id == &task);
            // Historical model-event replay with an already-current case cannot
            // introduce a new hold or rewrite its newer explicit authority.
            if source_changed
                && before.current_source.source == current.source
                && before.current_source.input_epoch == current.input_epoch
                && before.current_source.candidate == current.candidate
                && before.current_source.outcome == current.outcome
            {
                continue;
            }
            let blockers = sqlx::query("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name=? AND case_id=? ORDER BY ordinal")
                .bind(group).bind(id).fetch_all(&mut **tx).await?;
            ensure!(blockers.len() <= 32, "case blocker bound exceeded");
            let removed: Vec<Value> = blockers.iter().map(|r| json!({
                "case_version":r.get::<i64,_>("case_version"),"ordinal":r.get::<i64,_>("ordinal"),
                "selector":r.get::<String,_>("selector"),"waiting":r.get::<Option<String>,_>("waiting"),
                "responsible":r.get::<String,_>("responsible"),"reason":r.get::<String,_>("reason"),"evidence":r.get::<String,_>("evidence")
            })).collect();
            // Obsolete waits cannot block cancellation or retain aliases that
            // the corrected contract no longer declares. Replace their effect
            // with an explicit whole-task admission hold, not permission.
            sqlx::query("DELETE FROM decision_blockers WHERE group_name=? AND case_id=?")
                .bind(group)
                .bind(id)
                .execute(&mut **tx)
                .await?;
            let observed = if source_changed {
                current.clone()
            } else {
                inspect_source_tx(tx, group, &before.current_source.source).await?
            };
            let terminal_without_execution = !observed.unresolved
                && before.execution_source.is_none()
                && before.decision_task.is_none();
            sqlx::query("UPDATE decision_cases SET current_source=?,version=version+1,requires_reassessment=?,reassessment_event=?,state=?,capability_hold=?,last_scan=0 WHERE group_name=? AND id=? AND version=?")
                .bind(serde_json::to_string(&observed)?).bind(i64::from(!terminal_without_execution)).bind(event_id)
                .bind(if terminal_without_execution { "superseded" } else { "held" })
                .bind(if terminal_without_execution { "source_settled" } else { "source_changed_requires_authority_reassessment" })
                .bind(group).bind(id).bind(before.version).execute(&mut **tx).await?;
            sqlx::query("UPDATE operator_obligations SET version=version+1,state=?,reason=?,evidence=?,last_scan=0 WHERE group_name=? AND case_id=?")
                .bind(if terminal_without_execution { "superseded" } else if now >= before.hard_due { "escalated" } else { "pending" })
                .bind(if terminal_without_execution { "source settled under its actual authority" } else { "model source changed; original authority must reassess; execution cleanup remains independently guarded" })
                .bind(serde_json::to_string(&vec![format!("model-event:{event_id}")])?).bind(group).bind(id).execute(&mut **tx).await?;
            let case = load_case_tx(tx, group, id).await?;
            record_audit_tx(tx, AuditRecord {
                group,
                case: Some(id),
                actor: None,
                key: operation,
                operation: "model_source_reconciled",
                canonical: &json!({"model_event":event_id,"origin":event.get::<String,_>("origin"),"actor":event.get::<String,_>("actor"),"reason":event.get::<String,_>("reason"),"before":before,"removed_blockers":removed}),
                result: &serde_json::to_value(&case)?,
                now,
            }).await?;
            changes.insert(
                id,
                RecoverySourceChange {
                    source: format!("case:{id}"),
                    case_id: id,
                    case_version: case.version,
                    model_event: event_id,
                },
            );
        }
    }
    Ok(changes.into_values().collect())
}

/// Mandatory admission/readiness companion to the model-change hook. Every
/// affected source action (including existing scope units) is conservatively
/// held until explicit case reassessment restores a source-validated scope.
/// This does not affect the model's ability to record negative corrections.
pub(crate) async fn recovery_admission_holds_tx(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    task: &str,
) -> Result<Vec<RecoveryAdmissionHold>> {
    reserve_home_tx(tx, group).await?;
    let rows = sqlx::query("SELECT id,version,authority,reassessment_event FROM decision_cases WHERE group_name=? AND requires_reassessment=1 AND state NOT IN ('handled','superseded') AND (task=? OR decision_task=?) ORDER BY id LIMIT 1001")
        .bind(group).bind(task).bind(task).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 1000, "recovery admission hold bound exceeded");
    rows.into_iter()
        .map(|row| {
            Ok(RecoveryAdmissionHold {
                case_id: row.get("id"),
                case_version: row.get("version"),
                responsible: row.get("authority"),
                model_event: row
                    .get::<Option<i64>, _>("reassessment_event")
                    .context("recovery hold has no model-event evidence")?,
                reason: "Source changed; original authority must reassess the existing finite case"
                    .into(),
            })
        })
        .collect()
}

#[cfg(test)]
mod strategy_tests {
    use super::*;
    use crate::{
        execution::{self, Checked, ContinueStrategy, ExecutionCauseRef, ExecutionCauseState},
        progress::{PolicyChange, ProgressPolicy},
        states::TaskState,
        task_graph::{
            AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract,
            Criterion, TaskCreate, TaskDraft,
        },
        work::WorkDraft,
    };
    use std::collections::BTreeMap;

    // Actual model/progress/scheduler writers create this elapsed-time cause.
    // No runtime is launched and no source cause or closure witness is injected.
    async fn elapsed_case() -> Result<(
        tempfile::TempDir,
        Store,
        Mailbox,
        Mailbox,
        DecisionCase,
        execution::ExecutionSourceGuard,
        ContinueStrategy,
    )> {
        let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
        ensure!(
            root == "/tmp/agent-mail-durable-execution/decision-recovery",
            "isolated recovery state required"
        );
        let dir = tempfile::Builder::new()
            .prefix("strategy-case-")
            .tempdir_in(root)?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        let credential = store.register("g", "writer", false).await?;
        let writer = store.authenticate("g", Some(&credential)).await?;
        let credential = store.register("g", "worker", false).await?;
        let worker = store.authenticate("g", Some(&credential)).await?;
        store
            .task_create(
                &writer,
                TaskCreate {
                    key: "create-job".into(),
                    reason: "elapsed decision control".into(),
                    expected_parent_versions: BTreeMap::new(),
                    draft: TaskDraft {
                        work: WorkDraft {
                            id: "job".into(),
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
                                max_cost: None,
                            },
                        },
                        authorization: Authorization {
                            state: AuthorityState::Authorized,
                            source: AuthoritySource::Direct {
                                authority_ref: "test source writer".into(),
                            },
                            approved_scope: vec!["fixture artifact".into()],
                            reason: "bounded control".into(),
                        },
                        requirements: vec![],
                        parent: None,
                    },
                },
                100,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        crate::progress::change_policy_tx(
            &mut tx,
            &writer,
            "job",
            &PolicyChange {
                key: "elapsed-boundary".into(),
                task_version: 1,
                expected_revision: None,
                reason: "finite earlier progress check".into(),
                policy: ProgressPolicy {
                    max_segments_without_milestone: 2,
                    max_elapsed_without_milestone: Some(10),
                    milestones: vec![],
                },
            },
            100,
        )
        .await?;
        tx.commit().await?;
        store.execution_reconcile("g", 111).await?;
        let view = store.execution_inspect(&writer, "job").await?;
        let cause = view
            .causes
            .iter()
            .find(|cause| cause.code == "no_progress_elapsed")
            .context("actual elapsed cause missing")?;
        let reference = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: cause.id.clone(),
        };
        let mut tx = store.pool().begin().await?;
        let ExecutionCauseState::Current(current) =
            execution::inspect_execution_cause_tx(&mut tx, &reference).await?
        else {
            anyhow::bail!("elapsed cause not current")
        };
        let case = ensure_execution_case_tx(&mut tx, &reference, 112).await?;
        crate::task_graph::refresh_recovery_projection_tx(
            &mut tx,
            "g",
            &BTreeMap::from([(case.id, case.version)]),
        )
        .await?;
        tx.commit().await?;
        let request = ContinueStrategy {
            key: "bounded-continuation".into(),
            reason: "writer approves one revised segment".into(),
            execution_revision: view.revision.context("execution revision missing")?,
            additional_segments: 1,
            expires_at: 200,
        };
        Ok((dir, store, writer, worker, case, current.guard, request))
    }

    #[tokio::test]
    async fn actual_model_noop_receipt_cannot_clear_a_candidate_phase_hold() -> Result<()> {
        use crate::task_graph::{
            self, Change, DecisionAction, DecisionMaterialization, DecisionMaterializationRequest,
            DecisionPolicy, DecisionPolicyDecision, DecisionSourceExpectation, OutcomeChange,
            TaskDecision,
        };
        let (_dir, store, writer, _worker, case, _guard, _request) = elapsed_case().await?;
        let mut contract = store
            .task_inspect(&writer, "job")
            .await?
            .model
            .context("source contract missing")?
            .contract;
        contract.allow_delegation = false;
        let source = DecisionSourceExpectation {
            source: case.current_source.source.clone(),
            input_epoch: case.current_source.input_epoch,
            candidate: case.current_source.candidate.clone(),
            outcome: case.current_source.outcome.clone(),
        };
        store
            .decision_policy(
                &writer,
                DecisionPolicyDecision {
                    key: "finite-policy".into(),
                    expected_revision: None,
                    source: source.clone(),
                    reason: "actual phase receipt guard control".into(),
                    policy: DecisionPolicy {
                        id: "one-decision".into(),
                        contract,
                        actions: vec![DecisionAction::Recommend],
                        reviewer: Some("worker".into()),
                        allow_writer_fallback: true,
                        deadline: case.hard_due,
                        authority_ref: "actual source writer consent".into(),
                        revoked: false,
                    },
                },
                113,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Materialized(receipt) =
            task_graph::materialize_decision_task_tx(
                &mut tx,
                "g",
                &DecisionMaterializationRequest {
                    policy: "one-decision".into(),
                    policy_revision: 1,
                    case_id: case.id,
                    case_version: case.version,
                    source,
                },
                114,
            )
            .await?
        else {
            anyhow::bail!("actual decision materialization refused")
        };
        tx.commit().await?;
        let before = store.decision_case(&writer, case.id).await?;
        let task = store.task_inspect(&writer, &receipt.task).await?;
        let mut tx = store.pool().begin().await?;
        let proof = prepare_decision_change_tx(
            &mut tx,
            "g",
            case.id,
            before.version,
            &receipt.task,
            DecisionChangeKind::Candidate,
            115,
        )
        .await?;
        let application = Store::task_decide_with_reconciliation_tx(
            &mut tx,
            &writer,
            &receipt.task,
            TaskDecision {
                key: "actual-noop".into(),
                version: task.work.version,
                reason: "no candidate supplied".into(),
                work_patch: crate::work::WorkPatch::default(),
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
            115,
        )
        .await?;
        let applied = application
            .applied
            .context("actual model event receipt missing")?;
        assert!(
            load_case_tx(&mut tx, "g", case.id)
                .await?
                .requires_reassessment
        );
        let error = finalize_decision_change_tx(&mut tx, &proof, &applied, 115)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("candidate transition mismatch"),
            "expected candidate transition guard; actual error: {error:#}"
        );
        tx.rollback().await?;
        assert_eq!(store.decision_case(&writer, case.id).await?, before);
        assert_eq!(
            store
                .task_inspect(&writer, &receipt.task)
                .await?
                .work
                .version,
            task.work.version
        );
        Ok(())
    }

    #[tokio::test]
    async fn real_blocker_writer_stages_only_execute_and_rollback_restores_all_rows() -> Result<()>
    {
        use crate::task_graph::{
            self, ActionNode, DecisionAction, DecisionMaterialization,
            DecisionMaterializationRequest, DecisionPolicy, DecisionPolicyDecision,
            DecisionSourceExpectation, TaskAction,
        };
        let (_dir, store, writer, worker, case, guard, _request) = elapsed_case().await?;
        let mut contract = store
            .task_inspect(&writer, "job")
            .await?
            .model
            .context("source contract missing")?
            .contract;
        contract.allow_delegation = false;
        let source = DecisionSourceExpectation {
            source: case.current_source.source.clone(),
            input_epoch: case.current_source.input_epoch,
            candidate: case.current_source.candidate.clone(),
            outcome: case.current_source.outcome.clone(),
        };
        store
            .decision_policy(
                &writer,
                DecisionPolicyDecision {
                    key: "blocker-policy".into(),
                    expected_revision: None,
                    source: source.clone(),
                    reason: "actual typed blocker control".into(),
                    policy: DecisionPolicy {
                        id: "blocker-decision".into(),
                        contract,
                        actions: vec![DecisionAction::Recommend],
                        reviewer: Some("worker".into()),
                        allow_writer_fallback: true,
                        deadline: case.hard_due,
                        authority_ref: "original source consent".into(),
                        revoked: false,
                    },
                },
                113,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Materialized(receipt) =
            task_graph::materialize_decision_task_tx(
                &mut tx,
                "g",
                &DecisionMaterializationRequest {
                    policy: "blocker-decision".into(),
                    policy_revision: 1,
                    case_id: case.id,
                    case_version: case.version,
                    source,
                },
                114,
            )
            .await?
        else {
            anyhow::bail!("actual decision materialization refused")
        };
        tx.commit().await?;
        let linked = store.decision_case(&writer, case.id).await?;
        let initial: Vec<DecisionBlockerRow> = sqlx::query_as("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name='g' AND case_id=? ORDER BY ordinal")
            .bind(case.id).fetch_all(store.pool()).await?;
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].case_version, linked.version);
        assert_eq!(
            serde_json::from_str::<ActionNode>(&initial[0].selector)?,
            ActionNode {
                task: "job".into(),
                action: TaskAction::Execute
            }
        );
        assert_eq!(
            serde_json::from_str::<ActionNode>(
                initial[0]
                    .waiting
                    .as_deref()
                    .context("actual wait missing")?
            )?,
            ActionNode {
                task: receipt.task.clone(),
                action: TaskAction::AcceptResult
            }
        );
        assert_eq!(initial[0].responsible, worker.name);
        let request = DecisionBlockersRequest {
            key: "install-real-blockers".into(),
            case_id: case.id,
            case_version: linked.version,
            reason: "wait for this finite decision".into(),
            blockers: vec![TaskAction::Execute, TaskAction::AcceptResult]
                .into_iter()
                .map(|action| ScopedBlocker {
                    selector: ActionNode {
                        task: "job".into(),
                        action,
                    },
                    waiting: Some(ActionNode {
                        task: receipt.task.clone(),
                        action: TaskAction::AcceptResult,
                    }),
                    responsible: writer.name.clone(),
                    reason: "actual original-case dependency".into(),
                    evidence: vec!["actual materialization".into()],
                })
                .collect(),
        };
        assert!(
            store
                .set_decision_blockers(&worker, request.clone(), 115)
                .await
                .is_err()
        );
        let installed = store
            .set_decision_blockers(&writer, request.clone(), 115)
            .await?;
        assert_eq!(
            store
                .set_decision_blockers(&writer, request, case.hard_due + 1)
                .await?,
            installed
        );
        let projection_sql = "SELECT owner,source,source_version,consumer,prerequisite,kind FROM task_blocking_edges WHERE group_name='g' ORDER BY owner,source,consumer,prerequisite,kind";
        let projection: Vec<(String, String, Option<i64>, String, String, String)> =
            sqlx::query_as(projection_sql)
                .fetch_all(store.pool())
                .await?;
        let mut tx = store.pool().begin().await?;
        let original = prepare_decision_change_tx(
            &mut tx,
            "g",
            case.id,
            installed.version,
            &receipt.task,
            DecisionChangeKind::Outcome,
            116,
        )
        .await?;
        let original_rows = original.blockers.clone();
        assert_eq!(original_rows.len(), 2);
        let authority =
            validate_case_authority_tx(&mut tx, &writer, case.id, installed.version, &guard)
                .await?;
        let stage =
            stage_decision_continuation_tx(&mut tx, &writer, original, authority, 116).await?;
        assert_eq!(stage.removed.len(), 1);
        assert_eq!(stage.intermediate.blockers.len(), 1);
        let retained = &stage.intermediate.blockers[0];
        assert_eq!(retained.ordinal, 1);
        assert_eq!(
            retained.case_version,
            stage.authority().case().case_version()
        );
        let mut original_retained = retained.clone();
        original_retained.case_version = installed.version;
        assert_eq!(original_retained, original_rows[1]);
        let mut restored = stage.intermediate.case.clone();
        restored.version = installed.version;
        assert_eq!(restored, installed);
        // Same rollback used by the model's Held/error branch. No source grant or
        // ordinary outcome is claimed by this producer/staging control.
        tx.rollback().await?;
        assert_eq!(store.decision_case(&writer, case.id).await?, installed);
        let rows: Vec<DecisionBlockerRow> = sqlx::query_as("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name='g' AND case_id=? ORDER BY ordinal")
            .bind(case.id).fetch_all(store.pool()).await?;
        assert_eq!(rows, original_rows);
        let after: Vec<(String, String, Option<i64>, String, String, String)> =
            sqlx::query_as(projection_sql)
                .fetch_all(store.pool())
                .await?;
        assert_eq!(after, projection);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM decision_audit WHERE operation='decision_continuation_staged'"
            )
            .fetch_one(store.pool())
            .await?,
            0
        );
        Ok(())
    }

    async fn actual_ack_history(
        store: &Store,
        guard: &execution::ExecutionSourceGuard,
    ) -> Result<Value> {
        let receipt: (String, String) = sqlx::query_as(
            "SELECT canonical,result FROM execution_receipts WHERE producer=? AND key=?",
        )
        .bind(format!("execution-case-ack:{}", guard.cause_ref.group))
        .bind(&guard.cause_ref.cause_generation)
        .fetch_one(store.pool())
        .await?;
        let result: Value = serde_json::from_str(&receipt.1)?;
        let event: (String, String, Option<String>, String, String, i64) = sqlx::query_as(
            "SELECT group_name,task,attempt,kind,payload,created FROM execution_events WHERE id=?",
        )
        .bind(
            result["ledger_event"]
                .as_i64()
                .context("ACK event missing")?,
        )
        .fetch_one(store.pool())
        .await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_events WHERE group_name=? AND task=? AND kind='decision_case_linked' AND json_extract(payload,'$.source_guard.cause_ref.cause_generation')=?")
            .bind(&guard.cause_ref.group).bind(&guard.cause_ref.source_task)
            .bind(&guard.cause_ref.cause_generation).fetch_one(store.pool()).await?;
        Ok(json!({"receipt":receipt,"event":event,"count":count}))
    }

    #[tokio::test]
    async fn supervisor_uses_actual_consent_and_rolls_back_materialization_with_its_page()
    -> Result<()> {
        use crate::decision_supervisor::supervise_recovery_page;
        use crate::task_graph::{
            ActionNode, CandidateDraft, CandidateRequest, CriterionEvidence, DecisionAction,
            DecisionPolicy, DecisionPolicyDecision, DecisionSourceExpectation, Phase, TaskAction,
        };

        async fn snapshot(store: &Store, writer: &Mailbox, case_id: i64) -> Result<Value> {
            let supervision: String = sqlx::query_scalar("SELECT json_object('source_cursor',source_cursor,'case_cursor',case_cursor,'execution_cursor',execution_cursor,'missing_task_cursor',missing_task_cursor,'missing_mail_cursor',missing_mail_cursor,'missing_recipient_cursor',missing_recipient_cursor,'heartbeat',heartbeat,'source_passes',source_passes,'case_passes',case_passes,'execution_passes',execution_passes,'missing_task_passes',missing_task_passes,'missing_mail_passes',missing_mail_passes,'completed_scans',completed_scans,'last_full_scan',last_full_scan,'unresolved',unresolved,'capability_hold',capability_hold) FROM decision_supervision WHERE group_name='g'")
                .fetch_one(store.pool()).await?;
            let operator: String = sqlx::query_scalar("SELECT json_object('id',id,'version',version,'state',state,'reason',reason,'evidence',evidence,'hard_due',hard_due,'last_scan',last_scan) FROM operator_obligations WHERE group_name='g' AND case_id=?")
                .bind(case_id).fetch_one(store.pool()).await?;
            let projection: Vec<(String,String,Option<i64>,String,String,String)> = sqlx::query_as("SELECT owner,source,source_version,consumer,prerequisite,kind FROM task_blocking_edges WHERE group_name='g' ORDER BY owner,source,consumer,prerequisite,kind")
                .fetch_all(store.pool()).await?;
            let mut counts = BTreeMap::new();
            for table in [
                "work_items",
                "task_models",
                "task_model_events",
                "task_materializations",
                "decision_cases",
                "decision_blockers",
                "decision_audit",
                "execution_events",
            ] {
                let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(store.pool())
                    .await?;
                counts.insert(table, count);
            }
            Ok(
                json!({"supervision":serde_json::from_str::<Value>(&supervision)?,
                "operator":serde_json::from_str::<Value>(&operator)?,"projection":projection,"counts":counts,
                "source":store.task_inspect(writer,"job").await?,
                "execution":store.execution_inspect(writer,"job").await?,
                "case":store.decision_case(writer,case_id).await?}),
            )
        }

        let (_dir, store, writer, worker, case, guard, _request) = elapsed_case().await?;
        let original_budgets =
            serde_json::to_value(&store.execution_inspect(&writer, "job").await?.budgets)?;
        let before_ack = recovery_durable_state(&store).await?;
        sqlx::query("CREATE TRIGGER reject_page_after_ack BEFORE UPDATE ON decision_supervision BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM execution_events WHERE kind='decision_case_linked') AND EXISTS(SELECT 1 FROM execution_causes WHERE case_ref IS NOT NULL) THEN RAISE(ABORT,'forced_page_failure_after_ack') ELSE RAISE(ABORT,'authentic_ack_missing') END; END")
            .execute(store.pool()).await?;
        let error = supervise_recovery_page(&store, "g", 113, 1)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_page_failure_after_ack"),
            "{error:#}"
        );
        assert_eq!(recovery_durable_state(&store).await?, before_ack);
        sqlx::query("DROP TRIGGER reject_page_after_ack")
            .execute(store.pool())
            .await?;
        let missing = supervise_recovery_page(&store, "g", 113, 1).await?;
        let original_ack = actual_ack_history(&store, &guard).await?;
        assert_eq!(original_ack["count"], 1);
        let mut tx = store.pool().begin().await?;
        let ExecutionCauseState::Current(acknowledged) =
            execution::inspect_execution_cause_tx(&mut tx, &guard.cause_ref).await?
        else {
            anyhow::bail!("ACK must leave original cause unresolved")
        };
        assert_eq!(acknowledged.guard, guard);
        assert_eq!(acknowledged.handoff_case, Some(case.id.to_string()));
        tx.rollback().await?;
        assert_eq!(store.decision_case(&writer, case.id).await?, case);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM operator_obligations WHERE case_id=?"
            )
            .bind(case.id)
            .fetch_one(store.pool())
            .await?,
            "pending"
        );

        assert_eq!(missing.materialization_attempts, 1);
        assert_eq!(missing.materializations_refused, 1);
        assert_eq!(missing.decisions_materialized, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM work_items")
                .fetch_one(store.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_materializations")
                .fetch_one(store.pool())
                .await?,
            0
        );
        assert_eq!(
            serde_json::to_value(&store.execution_inspect(&writer, "job").await?.budgets)?,
            original_budgets
        );
        let refused: String = sqlx::query_scalar(
            "SELECT result FROM decision_audit WHERE operation='supervisor_materialization'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(
            serde_json::from_str::<Value>(&refused)?,
            json!({"state":"refused","reason":"policy_missing"})
        );
        let repeated = supervise_recovery_page(&store, "g", 114, 1).await?;
        assert_eq!(repeated.materializations_refused, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM decision_audit WHERE operation='supervisor_materialization'"
            )
            .fetch_one(store.pool())
            .await?,
            1
        );
        let current = store.decision_case(&writer, case.id).await?;
        let mut contract = store
            .task_inspect(&writer, "job")
            .await?
            .model
            .context("source contract missing")?
            .contract;
        contract.allow_delegation = false;
        store
            .decision_policy(
                &writer,
                DecisionPolicyDecision {
                    key: "supervisor-policy".into(),
                    expected_revision: None,
                    reason: "actual supervised source consent".into(),
                    source: DecisionSourceExpectation {
                        source: current.current_source.source.clone(),
                        input_epoch: current.current_source.input_epoch,
                        candidate: current.current_source.candidate.clone(),
                        outcome: current.current_source.outcome.clone(),
                    },
                    policy: DecisionPolicy {
                        id: "supervised-decision".into(),
                        contract,
                        actions: vec![DecisionAction::Recommend],
                        reviewer: Some(worker.name.clone()),
                        allow_writer_fallback: true,
                        deadline: case.hard_due,
                        authority_ref: "actual writer consent for supervision".into(),
                        revoked: false,
                    },
                },
                115,
            )
            .await?;
        let before = snapshot(&store, &writer, case.id).await?;
        let durable_before = recovery_durable_state(&store).await?;
        sqlx::query("CREATE TRIGGER reject_supervisor_materialization BEFORE INSERT ON decision_audit WHEN NEW.operation='supervisor_materialization' AND json_extract(NEW.result,'$.state')='materialized' BEGIN SELECT RAISE(ABORT,'forced_supervisor_materialization_audit'); END")
            .execute(store.pool()).await?;
        let error = supervise_recovery_page(&store, "g", 116, 100)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_supervisor_materialization_audit"),
            "expected actual post-materialization audit failure; actual error: {error:#}"
        );
        assert_eq!(snapshot(&store, &writer, case.id).await?, before);
        assert_eq!(recovery_durable_state(&store).await?, durable_before);
        sqlx::query("DROP TRIGGER reject_supervisor_materialization")
            .execute(store.pool())
            .await?;
        let applied = supervise_recovery_page(&store, "g", 117, 100).await?;
        assert_eq!(applied.decisions_materialized, 1);
        assert_eq!(applied.materializations_refused, 0);
        let linked = store.decision_case(&writer, case.id).await?;
        let task = linked
            .decision_task
            .as_deref()
            .context("supervised decision missing")?;
        assert_eq!(
            store.task_inspect(&writer, task).await?.work.owner,
            worker.name
        );
        assert_eq!(linked.hard_due, case.hard_due);
        assert_eq!(linked.original_due, case.original_due);
        let blockers: Vec<DecisionBlockerRow> = sqlx::query_as("SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name='g' AND case_id=? ORDER BY ordinal")
            .bind(case.id).fetch_all(store.pool()).await?;
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].responsible, worker.name);
        assert_eq!(blockers[0].case_version, linked.version);
        assert_eq!(
            serde_json::from_str::<ActionNode>(&blockers[0].selector)?,
            ActionNode {
                task: "job".into(),
                action: TaskAction::Execute
            }
        );
        assert_eq!(
            serde_json::from_str::<ActionNode>(
                blockers[0]
                    .waiting
                    .as_deref()
                    .context("actual wait missing")?
            )?,
            ActionNode {
                task: task.into(),
                action: TaskAction::AcceptResult
            }
        );
        let again = supervise_recovery_page(&store, "g", 118, 100).await?;
        assert_eq!(again.materialization_attempts, 0);
        assert_eq!(
            store.decision_case(&writer, case.id).await?.decision_task,
            linked.decision_task
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_materializations")
                .fetch_one(store.pool())
                .await?,
            1
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM operator_obligations WHERE case_id=? AND state IN ('pending','escalated')")
            .bind(case.id).fetch_one(store.pool()).await?,1);
        assert!(linked.version > case.version);
        assert_eq!(actual_ack_history(&store, &guard).await?, original_ack);
        let decision = store.task_inspect(&writer, task).await?;
        store
            .task_candidate(
                &worker,
                task,
                CandidateRequest {
                    version: decision.work.version,
                    key: "supervised-real-candidate".into(),
                    candidate: CandidateDraft {
                        revision: "review-v1".into(),
                        summary: "Actual reviewer phase change".into(),
                        criterion_evidence: decision
                            .model
                            .as_ref()
                            .context("decision contract missing")?
                            .contract
                            .criteria
                            .iter()
                            .map(|criterion| CriterionEvidence {
                                criterion_id: criterion.id.clone(),
                                references: vec!["real-review".into()],
                            })
                            .collect(),
                        inputs: store
                            .task_capture_inputs(
                                &worker,
                                task,
                                decision.work.version,
                                Phase::Accept,
                            )
                            .await?,
                    },
                },
                119,
            )
            .await?;
        let phase = store.decision_case(&writer, case.id).await?;
        assert!(phase.version > linked.version);
        supervise_recovery_page(&store, "g", 120, 100).await?;
        assert_eq!(actual_ack_history(&store, &guard).await?, original_ack);
        assert_eq!(store.decision_case(&writer, case.id).await?, phase);
        assert_eq!(
            serde_json::to_value(store.execution_inspect(&writer, "job").await?.budgets)?,
            original_budgets
        );
        // The current schema prevents this negative corruption fixture at the
        // write boundary. Keep that guard intact and assert the rejected attack
        // leaves all real history/effects unchanged before genuine page replay.
        let immutable_before = recovery_durable_state(&store).await?;
        let rejected = sqlx::query("DELETE FROM execution_receipts WHERE producer=? AND key=?")
            .bind("execution-case-ack:g")
            .bind(&guard.cause_ref.cause_generation)
            .execute(store.pool())
            .await
            .unwrap_err();
        assert!(
            format!("{rejected:#}").contains("immutable execution receipt"),
            "{rejected:#}"
        );
        assert_eq!(recovery_durable_state(&store).await?, immutable_before);
        assert_eq!(actual_ack_history(&store, &guard).await?, original_ack);
        supervise_recovery_page(&store, "g", 121, 100).await?;
        assert_eq!(actual_ack_history(&store, &guard).await?, original_ack);
        assert_eq!(store.decision_case(&writer, case.id).await?, phase);
        assert_eq!(
            serde_json::to_value(store.execution_inspect(&writer, "job").await?.budgets)?,
            original_budgets
        );
        Ok(())
    }

    #[tokio::test]
    async fn superseded_execution_case_refuses_without_poisoning_later_case() -> Result<()> {
        use crate::decision_supervisor::supervise_recovery_page;
        use crate::task_graph::{
            DecisionAction, DecisionPolicy, DecisionPolicyDecision, DecisionSourceExpectation,
        };

        async fn create_task(store: &Store, writer: &Mailbox, id: &str, now: i64) -> Result<()> {
            store
                .task_create(
                    writer,
                    TaskCreate {
                        key: format!("create-{id}"),
                        reason: "real superseded-cause regression".into(),
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
                                    max_cost: None,
                                },
                            },
                            authorization: Authorization {
                                state: AuthorityState::Authorized,
                                source: AuthoritySource::Direct {
                                    authority_ref: "original source writer".into(),
                                },
                                approved_scope: vec!["fixture artifact".into()],
                                reason: "bounded regression".into(),
                            },
                            requirements: vec![],
                            parent: None,
                        },
                    },
                    now,
                )
                .await?;
            Ok(())
        }

        let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
        ensure!(
            root == "/tmp/agent-mail-durable-execution/decision-recovery",
            "isolated recovery state required"
        );
        let dir = tempfile::Builder::new()
            .prefix("superseded-case-")
            .tempdir_in(root)?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        let writer_key = store.register("g", "writer", false).await?;
        let writer = store.authenticate("g", Some(&writer_key)).await?;
        store.register("g", "worker", false).await?;
        create_task(&store, &writer, "job", 100).await?;
        store.pause("g", true).await?;
        store.execution_reconcile("g", 101).await?;
        let view = store.execution_inspect(&writer, "job").await?;
        let cause = view
            .causes
            .iter()
            .find(|cause| cause.code == "group_paused")
            .context("genuine paused-group cause missing")?;
        let reference = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: cause.id.clone(),
        };
        let first = supervise_recovery_page(&store, "g", 102, 100).await?;
        assert_eq!(first.decisions_materialized, 0);
        let mut tx = store.pool().begin().await?;
        let ack = execution::execution_case_ack_tx(&mut tx, &reference)
            .await?
            .context("actual initial case acknowledgement missing")?;
        let original = load_case_tx(&mut tx, "g", ack.case_id).await?;
        tx.rollback().await?;
        assert!(original.decision_task.is_none());
        let original_ack = actual_ack_history(&store, &ack.source_guard).await?;
        let original_task = store.task_inspect(&writer, "job").await?;

        store.pause("g", false).await?;
        store.execution_reconcile("g", 103).await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            execution::inspect_execution_cause_tx(&mut tx, &reference).await?,
            ExecutionCauseState::Superseded(_)
        ));
        tx.rollback().await?;
        let unchanged_task = store.task_inspect(&writer, "job").await?;
        assert_eq!(unchanged_task.work.version, original_task.work.version);
        assert_eq!(
            serde_json::to_value(&unchanged_task.model)?,
            serde_json::to_value(&original_task.model)?
        );
        assert_eq!(unchanged_task.work.state, TaskState::Ready);

        // Repair has no runtime target, so unpausing replaces the old hold with
        // a distinct current cause for the same unchanged source task.
        let runtime_view = store.execution_inspect(&writer, "job").await?;
        let runtime_cause = runtime_view
            .causes
            .iter()
            .find(|cause| cause.code == "runtime_unavailable")
            .context("genuine current runtime-unavailable cause missing")?;
        let runtime_ref = ExecutionCauseRef {
            group: "g".into(),
            source_task: "job".into(),
            cause_generation: runtime_cause.id.clone(),
        };
        assert_ne!(runtime_ref.cause_generation, reference.cause_generation);

        // A genuine later, still-current elapsed-time cause is scanned in the
        // same page. All cases/ACKs come from the production supervisor path.
        create_task(&store, &writer, "later", 104).await?;
        let mut tx = store.pool().begin().await?;
        crate::progress::change_policy_tx(
            &mut tx,
            &writer,
            "later",
            &PolicyChange {
                key: "later-progress".into(),
                task_version: 1,
                expected_revision: None,
                reason: "actual finite later cause".into(),
                policy: ProgressPolicy {
                    max_segments_without_milestone: 2,
                    max_elapsed_without_milestone: Some(10),
                    milestones: vec![],
                },
            },
            104,
        )
        .await?;
        tx.commit().await?;
        store.execution_reconcile("g", 115).await?;
        supervise_recovery_page(&store, "g", 116, 100).await?;
        let later_view = store.execution_inspect(&writer, "later").await?;
        let later_cause = later_view
            .causes
            .iter()
            .find(|cause| cause.code == "no_progress_elapsed")
            .context("genuine later elapsed cause missing")?;
        let later_ref = ExecutionCauseRef {
            group: "g".into(),
            source_task: "later".into(),
            cause_generation: later_cause.id.clone(),
        };
        let mut tx = store.pool().begin().await?;
        for current in [&runtime_ref, &later_ref] {
            assert!(matches!(
                execution::inspect_execution_cause_tx(&mut tx, current).await?,
                ExecutionCauseState::Current(_)
            ));
        }
        let runtime_ack = execution::execution_case_ack_tx(&mut tx, &runtime_ref)
            .await?
            .context("actual current runtime case acknowledgement missing")?;
        let runtime = load_case_tx(&mut tx, "g", runtime_ack.case_id).await?;
        let later_ack = execution::execution_case_ack_tx(&mut tx, &later_ref)
            .await?
            .context("actual later case acknowledgement missing")?;
        let later = load_case_tx(&mut tx, "g", later_ack.case_id).await?;
        tx.rollback().await?;
        assert!(runtime.id > original.id);
        assert!(later.id > original.id);
        assert_ne!(runtime.id, later.id);

        for (task, case) in [("job", &original), ("later", &later)] {
            let mut contract = store
                .task_inspect(&writer, task)
                .await?
                .model
                .context("source contract missing")?
                .contract;
            contract.allow_delegation = false;
            store
                .decision_policy(
                    &writer,
                    DecisionPolicyDecision {
                        key: format!("policy-{task}"),
                        expected_revision: None,
                        reason: "original writer installs genuine finite consent".into(),
                        source: DecisionSourceExpectation {
                            source: case.current_source.source.clone(),
                            input_epoch: case.current_source.input_epoch,
                            candidate: case.current_source.candidate.clone(),
                            outcome: case.current_source.outcome.clone(),
                        },
                        policy: DecisionPolicy {
                            id: format!("decision-{task}"),
                            contract,
                            actions: vec![DecisionAction::Recommend],
                            reviewer: Some("worker".into()),
                            allow_writer_fallback: true,
                            deadline: case.hard_due,
                            authority_ref: "actual original writer".into(),
                            revoked: false,
                        },
                    },
                    117,
                )
                .await?;
        }
        let before = recovery_durable_state(&store).await?;
        let original_budgets =
            serde_json::to_value(store.execution_inspect(&writer, "job").await?.budgets)?;
        let later_budgets =
            serde_json::to_value(store.execution_inspect(&writer, "later").await?.budgets)?;
        let operator_before: (i64, String, String, i64) = sqlx::query_as(
            "SELECT version,state,reason,hard_due FROM operator_obligations WHERE case_id=?",
        )
        .bind(original.id)
        .fetch_one(store.pool())
        .await?;

        // This fault follows the real later materialization. Refusal cannot
        // swallow storage errors or commit only the early case/cursor effects.
        sqlx::query("CREATE TRIGGER reject_superseded_page BEFORE UPDATE ON decision_supervision BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM task_materializations) THEN RAISE(ABORT,'forced_after_real_later_materialization') ELSE RAISE(ABORT,'later_materialization_missing') END; END")
            .execute(store.pool()).await?;
        let error = supervise_recovery_page(&store, "g", 118, 100)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_after_real_later_materialization"),
            "{error:#}"
        );
        assert_eq!(recovery_durable_state(&store).await?, before);
        sqlx::query("DROP TRIGGER reject_superseded_page")
            .execute(store.pool())
            .await?;

        let page = supervise_recovery_page(&store, "g", 118, 100).await?;
        assert!(page.cases_scanned >= 2);
        assert_eq!(page.decisions_materialized, 2);
        assert!(page.materializations_refused >= 1);
        assert_eq!(page.case_cursor, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT heartbeat FROM decision_supervision WHERE group_name='g'"
            )
            .fetch_one(store.pool())
            .await?,
            118
        );
        assert_eq!(store.decision_case(&writer, original.id).await?, original);
        let linked_runtime = store.decision_case(&writer, runtime.id).await?;
        let linked_later = store.decision_case(&writer, later.id).await?;
        let runtime_decision = linked_runtime
            .decision_task
            .clone()
            .context("current runtime case decision missing")?;
        let later_decision = linked_later
            .decision_task
            .clone()
            .context("current elapsed case decision missing")?;
        assert_ne!(runtime_decision, later_decision);
        let mapping_query = "SELECT c.id,m.episode,m.decision_task FROM task_materializations m JOIN decision_cases c ON c.group_name=m.group_name AND c.source_key=m.source AND c.episode=m.episode WHERE m.group_name='g' ORDER BY c.id";
        let mappings: Vec<(i64, String, String)> = sqlx::query_as(mapping_query)
            .fetch_all(store.pool())
            .await?;
        let mut expected_mappings = vec![
            (
                runtime.id,
                format!("execution:{}", runtime_ref.cause_generation),
                runtime_decision,
            ),
            (
                later.id,
                format!("execution:{}", later_ref.cause_generation),
                later_decision,
            ),
        ];
        expected_mappings.sort_by_key(|mapping| mapping.0);
        assert_eq!(mappings, expected_mappings);
        assert!(mappings.iter().all(|mapping| mapping.0 != original.id));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_materializations")
                .fetch_one(store.pool())
                .await?,
            2
        );
        assert_eq!(
            actual_ack_history(&store, &ack.source_guard).await?,
            original_ack
        );
        let operator_after: (i64, String, String, i64) = sqlx::query_as(
            "SELECT version,state,reason,hard_due FROM operator_obligations WHERE case_id=?",
        )
        .bind(original.id)
        .fetch_one(store.pool())
        .await?;
        assert_eq!(operator_after, operator_before);
        assert!(matches!(operator_after.1.as_str(), "pending" | "escalated"));
        let refused: String = sqlx::query_scalar("SELECT result FROM decision_audit WHERE case_id=? AND operation='supervisor_materialization' ORDER BY id DESC LIMIT 1")
            .bind(original.id).fetch_one(store.pool()).await?;
        assert_eq!(
            serde_json::from_str::<Value>(&refused)?,
            json!({"state":"refused","reason":"case_unavailable"})
        );
        let after = recovery_durable_state(&store).await?;
        for table in [
            "execution_slots",
            "execution_charges",
            "execution_attempts",
            "execution_receipts",
        ] {
            assert_eq!(after[table], before[table], "original {table} changed");
        }
        assert_eq!(
            serde_json::to_value(store.execution_inspect(&writer, "job").await?.budgets)?,
            original_budgets
        );
        assert_eq!(
            serde_json::to_value(store.execution_inspect(&writer, "later").await?.budgets)?,
            later_budgets
        );
        let audit_count: i64 = sqlx::query_scalar("SELECT count(*) FROM decision_audit WHERE case_id=? AND operation='supervisor_materialization'")
            .bind(original.id).fetch_one(store.pool()).await?;
        let repeated = supervise_recovery_page(&store, "g", 119, 100).await?;
        assert_eq!(repeated.decisions_materialized, 0);
        assert_eq!(store.decision_case(&writer, original.id).await?, original);
        assert_eq!(
            store
                .decision_case(&writer, runtime.id)
                .await?
                .decision_task,
            linked_runtime.decision_task
        );
        assert_eq!(
            store.decision_case(&writer, later.id).await?.decision_task,
            linked_later.decision_task
        );
        let repeated_mappings: Vec<(i64, String, String)> = sqlx::query_as(mapping_query)
            .fetch_all(store.pool())
            .await?;
        assert_eq!(repeated_mappings, mappings);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_materializations")
                .fetch_one(store.pool())
                .await?,
            2
        );
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_audit WHERE case_id=? AND operation='supervisor_materialization'")
            .bind(original.id).fetch_one(store.pool()).await?, audit_count);
        Ok(())
    }

    // Compare actual durable rows, including audits and receipts, across failures.
    async fn recovery_durable_state(store: &Store) -> Result<BTreeMap<String, String>> {
        let mut snapshot = BTreeMap::new();
        for table in [
            "decision_supervision",
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
            let sql = format!(
                "SELECT json_group_array(json(row_json)) FROM (SELECT json_object({fields}) AS row_json FROM {table} ORDER BY {order})"
            );
            let rows: String = sqlx::query_scalar(&sql).fetch_one(store.pool()).await?;
            snapshot.insert(table.into(), rows);
        }
        Ok(snapshot)
    }

    #[tokio::test]
    async fn continuation_case_failure_rolls_back_real_source_and_success_replays_after_expiry()
    -> Result<()> {
        use crate::task_graph::{
            self, ActionNode, CandidateDraft, CandidateRequest, Change, CriterionEvidence,
            DecisionAction, DecisionContinuation, DecisionMaterialization,
            DecisionMaterializationRequest, DecisionPolicy, DecisionPolicyDecision,
            DecisionSourceExpectation, OutcomeChange, OutcomeKind, Phase, TaskAction, TaskDecision,
        };
        let (_dir, store, writer, worker, case, guard, mut continuation) = elapsed_case().await?;
        crate::decision_supervisor::supervise_recovery_page(&store, "g", 113, 100).await?;
        let original_ack = actual_ack_history(&store, &guard).await?;
        let before = store.execution_inspect(&writer, "job").await?;
        let budgets = serde_json::to_value(&before.budgets)?;
        let before_source = store.task_inspect(&writer, "job").await?;
        let mut contract = before_source
            .model
            .as_ref()
            .context("source contract missing")?
            .contract
            .clone();
        contract.allow_delegation = false;
        let source = DecisionSourceExpectation {
            source: case.current_source.source.clone(),
            input_epoch: case.current_source.input_epoch,
            candidate: case.current_source.candidate.clone(),
            outcome: case.current_source.outcome.clone(),
        };
        store
            .decision_policy(
                &writer,
                DecisionPolicyDecision {
                    key: "continuation-policy".into(),
                    expected_revision: None,
                    source: source.clone(),
                    reason: "Original writer authorizes one finite continuation decision".into(),
                    policy: DecisionPolicy {
                        id: "continuation-review".into(),
                        contract,
                        actions: vec![DecisionAction::ContinueStrategy],
                        reviewer: Some(worker.name.clone()),
                        allow_writer_fallback: true,
                        deadline: case.hard_due,
                        authority_ref: "actual source writer consent".into(),
                        revoked: false,
                    },
                },
                113,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        let DecisionMaterialization::Materialized(materialized) =
            task_graph::materialize_decision_task_tx(
                &mut tx,
                "g",
                &DecisionMaterializationRequest {
                    policy: "continuation-review".into(),
                    policy_revision: 1,
                    case_id: case.id,
                    case_version: case.version,
                    source,
                },
                114,
            )
            .await?
        else {
            anyhow::bail!("actual continuation decision materialization refused")
        };
        tx.commit().await?;
        let task = materialized.task;
        let linked = store.decision_case(&writer, case.id).await?;
        let initial: Vec<DecisionBlockerRow> = sqlx::query_as(
            "SELECT case_version,ordinal,selector,waiting,responsible,reason,evidence FROM decision_blockers WHERE group_name='g' AND case_id=? ORDER BY ordinal")
            .bind(case.id).fetch_all(store.pool()).await?;
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].case_version, linked.version);
        assert_eq!(
            serde_json::from_str::<ActionNode>(&initial[0].selector)?,
            ActionNode {
                task: "job".into(),
                action: TaskAction::Execute
            }
        );
        assert_eq!(
            serde_json::from_str::<ActionNode>(
                initial[0]
                    .waiting
                    .as_deref()
                    .context("initial wait missing")?
            )?,
            ActionNode {
                task: task.clone(),
                action: TaskAction::AcceptResult
            }
        );
        assert_eq!(initial[0].responsible, worker.name);
        let decision_before = store.task_inspect(&writer, &task).await?;
        let candidate = store
            .task_candidate(
                &worker,
                &task,
                CandidateRequest {
                    version: decision_before.work.version,
                    key: "actual-continuation-review".into(),
                    candidate: CandidateDraft {
                        revision: "continuation-review-v1".into(),
                        summary: "One bounded source segment".into(),
                        criterion_evidence: decision_before
                            .model
                            .as_ref()
                            .context("decision model missing")?
                            .contract
                            .criteria
                            .iter()
                            .map(|criterion| CriterionEvidence {
                                criterion_id: criterion.id.clone(),
                                references: vec!["actual-review-evidence".into()],
                            })
                            .collect(),
                        inputs: store
                            .task_capture_inputs(
                                &worker,
                                &task,
                                decision_before.work.version,
                                Phase::Accept,
                            )
                            .await?,
                    },
                },
                115,
            )
            .await?;
        let current = store.decision_case(&writer, case.id).await?;
        continuation.execution_revision = store
            .execution_inspect(&writer, "job")
            .await?
            .revision
            .context("current source execution revision missing")?;
        assert_eq!(continuation.expires_at, 200);
        assert_eq!(continuation.additional_segments, 1);
        let request = DecisionContinuation {
            key: "atomic-case-continuation".into(),
            case_version: current.version,
            policy_revision: 1,
            continuation,
            decision: TaskDecision {
                key: "ordinary-continuation-outcome".into(),
                version: store.task_inspect(&writer, &task).await?.work.version,
                reason: "Accept actual finite reviewer candidate".into(),
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
        let decision_budgets =
            serde_json::to_value(store.execution_inspect(&writer, &task).await?.budgets)?;
        let durable_before = recovery_durable_state(&store).await?;
        assert!(
            store
                .decision_continue_strategy(&worker, &task, request.clone(), 117)
                .await
                .is_err()
        );
        assert_eq!(recovery_durable_state(&store).await?, durable_before);

        // This storage error is reachable only after the real scheduler mutation.
        // The public transaction must restore source, model, case, projections,
        // responsibility, budgets, audits and receipts together.
        sqlx::query("CREATE TRIGGER reject_case_completion BEFORE UPDATE ON decision_cases WHEN NEW.state='handled' BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM execution_events WHERE kind='strategy_continuation') THEN RAISE(ABORT,'forced_case_failure_after_source') ELSE RAISE(ABORT,'source_not_applied') END; END")
            .execute(store.pool()).await?;
        let error = store
            .decision_continue_strategy(&writer, &task, request.clone(), 117)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("forced_case_failure_after_source"),
            "expected case failure after real source mutation: {error:#}"
        );
        assert_eq!(recovery_durable_state(&store).await?, durable_before);
        sqlx::query("DROP TRIGGER reject_case_completion")
            .execute(store.pool())
            .await?;
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            execution::inspect_execution_cause_tx(&mut tx, &guard.cause_ref).await?,
            ExecutionCauseState::Current(_)
        ));
        assert_eq!(load_case_tx(&mut tx, "g", case.id).await?, current);
        tx.rollback().await?;

        let Checked::Ready(first) = store
            .decision_continue_strategy(&writer, &task, request.clone(), 117)
            .await?
        else {
            anyhow::bail!("actual public continuation held")
        };
        assert_eq!(first.case.state, "handled");
        assert_eq!(first.case.original_due, case.original_due);
        assert_eq!(first.case.hard_due, case.hard_due);
        assert_eq!(first.case.original_source, case.original_source);
        assert_eq!(first.decision.work.state, TaskState::Accepted);
        let mut tx = store.pool().begin().await?;
        let unfinished = execution::list_execution_causes_tx(&mut tx, "g", "", 100).await?;
        assert!(!unfinished.contains(&guard.cause_ref));
        let historical = execution::execution_case_ack_tx(&mut tx, &guard.cause_ref)
            .await?
            .context("settled source must retain its authentic ACK history")?;
        assert_eq!(historical.case_id, case.id);
        assert_eq!(historical.case_version, case.version);
        assert_eq!(historical.source_guard, guard);
        tx.rollback().await?;
        assert_eq!(actual_ack_history(&store, &guard).await?, original_ack);
        let state: String =
            sqlx::query_scalar("SELECT state FROM operator_obligations WHERE case_id=?")
                .bind(case.id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(state, "handled");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM decision_blockers WHERE case_id=?")
                .bind(case.id)
                .fetch_one(store.pool())
                .await?,
            0
        );
        let after_source = store.task_inspect(&writer, "job").await?;
        assert_eq!(after_source.work.version, before_source.work.version);
        assert_eq!(
            serde_json::to_value(after_source.model)?,
            serde_json::to_value(before_source.model)?
        );
        let committed = recovery_durable_state(&store).await?;
        let replay = store
            .decision_continue_strategy(&writer, &task, request.clone(), 201)
            .await?;
        assert_eq!(
            serde_json::to_value(replay)?,
            serde_json::to_value(Checked::Ready(first))?
        );
        assert_eq!(recovery_durable_state(&store).await?, committed);
        let mut conflicting = request;
        conflicting.continuation.expires_at = 201;
        let error = store
            .decision_continue_strategy(&writer, &task, conflicting, 201)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("decision_key_conflict"),
            "{error:#}"
        );
        assert_eq!(recovery_durable_state(&store).await?, committed);
        let after = store.execution_inspect(&writer, "job").await?;
        assert_eq!(serde_json::to_value(&after.budgets)?, budgets);
        assert_eq!(after.business_state, before.business_state);
        assert_eq!(
            serde_json::to_value(store.execution_inspect(&writer, &task).await?.budgets)?,
            decision_budgets
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_events WHERE kind='strategy_continuation'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(count, 1);
        Ok(())
    }
}
