//! Immutable milestone policies and judgments, separate from task disposition.
//!
//! Scheduler owns all attempts, charges, elapsed anchors and guard comparison.
//! Reports are evidence, never qualified progress until an authorized judgment.
//! All transaction functions use real owner reads and do not commit or do I/O.
use crate::{
    execution,
    store::{Mailbox, Store},
    task_graph::{self, Contract, InputSnapshot, InputValidity, JudgeGrantRef, Phase},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};
use std::collections::BTreeSet;

type Tx<'a> = Transaction<'a, Sqlite>;

/// A stable milestone maps to existing criteria and literal scope units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Milestone {
    /// Stable task-local identifier; reuse cannot change its meaning.
    pub id: String,
    /// Nonempty subset of the actual model criteria.
    pub criterion_ids: Vec<String>,
    /// Nonempty subset of the actual contract's literal scope units.
    pub scope_units: Vec<String>,
}

/// Visible progress boundary, never additional execution authority or budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressPolicy {
    /// Closed admitted segments allowed without a qualified milestone.
    pub max_segments_without_milestone: u32,
    /// Explicit earlier elapsed boundary; None uses existing finite deadlines.
    pub max_elapsed_without_milestone: Option<u64>,
    /// At most 32 stable, independently judged milestones.
    pub milestones: Vec<Milestone>,
}
impl Default for ProgressPolicy {
    fn default() -> Self {
        Self {
            max_segments_without_milestone: 2,
            max_elapsed_without_milestone: None,
            milestones: Vec::new(),
        }
    }
}

/// Writer-only policy edit with independent progress CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyChange {
    /// Canonical retry identity within the actor's progress journal.
    pub key: String,
    /// Exact observed business version; it is not advanced by this edit.
    pub task_version: i64,
    /// None explicitly selects an absent policy; otherwise exact progress revision.
    pub expected_revision: Option<i64>,
    /// Required audited explanation.
    pub reason: String,
    /// Complete visible policy.
    pub policy: ProgressPolicy,
}

/// A rejection is a valid business judgment, not an authentication failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JudgmentChange {
    /// Qualify one immutable admitted report; replacement names the prior judgment.
    Qualify {
        /// Immutable scheduler report event.
        report: i64,
        /// Prior current qualification, if replacing it.
        supersedes: Option<i64>,
    },
    /// Record why an immutable report does not meet this milestone.
    Reject {
        /// Immutable scheduler report event.
        report: i64,
    },
    /// Withdraw exactly one judgment while retaining its evidence and spending.
    Revoke {
        /// Immutable judgment being withdrawn.
        judgment: i64,
    },
}

/// An authenticated judgment request; original inputs come from the report owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JudgmentRequest {
    /// Stable retry key; changed content with this key is rejected.
    pub key: String,
    /// Exact current task version.
    pub task_version: i64,
    /// Exact current progress revision.
    pub progress_revision: i64,
    /// Stable milestone in the current policy.
    pub milestone: String,
    /// Optional exact persisted narrow model grant; source writers omit it.
    pub judge_grant: Option<JudgeGrantRef>,
    /// Audited reason for qualification, rejection or revocation.
    pub reason: String,
    /// Operation and immutable source references.
    pub change: JudgmentChange,
}

/// Immutable operation receipt. Historical retries never authorize another effect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressReceipt {
    /// Immutable progress journal event.
    pub record: i64,
    /// Resulting progress revision; business task version is unchanged.
    pub revision: i64,
    /// Stored policy or judgment category.
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredJudgment {
    milestone: Milestone,
    inputs: InputSnapshot,
    report: Option<i64>,
    report_at: Option<i64>,
    report_fence: Option<i64>,
    judge_id: i64,
    judge_name: String,
    judge_grant: Option<JudgeGrantRef>,
    supersedes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordPayload {
    progress_revision: i64,
    reason: String,
    policy: Option<ProgressPolicy>,
    policy_record: Option<i64>,
    judgment: Option<StoredJudgment>,
}

async fn home_tx(tx: &mut Tx<'_>, group: &str) -> Result<()> {
    let changed = sqlx::query(
        "UPDATE groups SET paused=paused WHERE name=? AND home_machine IN (SELECT id FROM node)",
    )
    .bind(group)
    .execute(&mut **tx)
    .await?;
    ensure!(
        changed.rows_affected() == 1,
        "progress requires the home store"
    );
    Ok(())
}

async fn authenticate_tx(tx: &mut Tx<'_>, actor: &Mailbox) -> Result<()> {
    Store::lock_actor(tx, actor).await?;
    home_tx(tx, &actor.group_name).await?;
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM mailboxes WHERE id=? AND name=? AND group_name=? AND remote_machine IS NULL")
        .bind(actor.id).bind(&actor.name).bind(&actor.group_name).fetch_one(&mut **tx).await?;
    ensure!(actual == 1, "progress actor identity mismatch");
    Ok(())
}

fn request_valid(key: &str, reason: &str) -> Result<()> {
    crate::name(key)?;
    crate::bounded(reason, 4096, "progress reason")?;
    ensure!(!reason.trim().is_empty(), "progress reason required");
    Ok(())
}

async fn replay_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    key: &str,
    canonical: &str,
) -> Result<Option<ProgressReceipt>> {
    let row = sqlx::query(
        "SELECT id,canonical,kind,payload FROM progress_records WHERE actor=? AND request_key=?",
    )
    .bind(actor.id)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|r| {
        ensure!(
            r.get::<String, _>("canonical") == canonical,
            "progress retry key conflict"
        );
        let payload: RecordPayload = serde_json::from_str(&r.get::<String, _>("payload"))?;
        Ok(ProgressReceipt {
            record: r.get("id"),
            revision: payload.progress_revision,
            kind: r.get("kind"),
        })
    })
    .transpose()
}

struct RecordToAppend<'a> {
    task: &'a str,
    key: &'a str,
    canonical: &'a str,
    kind: &'a str,
    payload: &'a RecordPayload,
}

async fn append_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    record: RecordToAppend<'_>,
    now: i64,
) -> Result<ProgressReceipt> {
    let RecordToAppend {
        task,
        key,
        canonical,
        kind,
        payload,
    } = record;
    let bytes = serde_json::to_string(payload)?;
    crate::bounded(&bytes, 65_536, "progress record")?;
    let id = sqlx::query("INSERT INTO progress_records(group_name,task,kind,actor,actor_binding,request_key,canonical,payload,created) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(&actor.group_name).bind(task).bind(kind).bind(actor.id).bind(actor.binding_version).bind(key).bind(canonical).bind(bytes).bind(now).execute(&mut **tx).await?.last_insert_rowid();
    Ok(ProgressReceipt {
        record: id,
        revision: payload.progress_revision,
        kind: kind.into(),
    })
}

async fn policy_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
) -> Result<Option<(i64, i64, ProgressPolicy)>> {
    let row = sqlx::query("SELECT p.revision,p.policy_record,r.payload FROM task_progress p JOIN progress_records r ON r.id=p.policy_record WHERE p.group_name=? AND p.task=?")
        .bind(group).bind(task).fetch_optional(&mut **tx).await?;
    row.map(|r| {
        let p: RecordPayload = serde_json::from_str(&r.get::<String, _>("payload"))?;
        Ok((
            r.get("revision"),
            r.get("policy_record"),
            p.policy.context("policy record missing policy")?,
        ))
    })
    .transpose()
}

fn normalize_policy(policy: &mut ProgressPolicy, contract: &Contract) -> Result<()> {
    ensure!(
        (1..=10_000).contains(&policy.max_segments_without_milestone),
        "progress segment threshold must be 1..10000"
    );
    if let Some(seconds) = policy.max_elapsed_without_milestone {
        ensure!(
            seconds > 0 && i64::try_from(seconds).is_ok(),
            "positive representable elapsed threshold required"
        );
    }
    ensure!(policy.milestones.len() <= 32, "at most32 milestones");
    policy.milestones.sort_by(|a, b| a.id.cmp(&b.id));
    let mut ids = BTreeSet::new();
    for m in &mut policy.milestones {
        crate::name(&m.id)?;
        ensure!(ids.insert(m.id.clone()), "duplicate milestone");
        m.criterion_ids.sort();
        m.scope_units.sort();
        ensure!(
            !m.criterion_ids.is_empty()
                && m.criterion_ids.len() <= 32
                && !m.criterion_ids.windows(2).any(|p| p[0] == p[1]),
            "distinct milestone criteria required"
        );
        ensure!(
            !m.scope_units.is_empty()
                && m.scope_units.len() <= 32
                && !m.scope_units.windows(2).any(|p| p[0] == p[1]),
            "distinct milestone scope units required"
        );
        ensure!(
            m.criterion_ids
                .iter()
                .all(|id| contract.criteria.iter().any(|c| &c.id == id)),
            "milestone criterion missing from model"
        );
        ensure!(
            m.scope_units
                .iter()
                .all(|s| contract.allowed_scope.contains(s)),
            "milestone scope outside contract"
        );
    }
    Ok(())
}

/// Actual source writer policy transaction; no execution anchor or counter writes.
pub(crate) async fn change_policy_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    task: &str,
    request: &PolicyChange,
    now: i64,
) -> Result<ProgressReceipt> {
    request_valid(&request.key, &request.reason)?;
    crate::name(task)?;
    authenticate_tx(tx, actor).await?;
    // Canonical retries are checked before current business CAS/authority changes.
    let canonical = serde_json::to_string(&json!(["policy", task, request]))?;
    if let Some(old) = replay_tx(tx, actor, &request.key, &canonical).await? {
        return Ok(old);
    }
    let row = sqlx::query("SELECT w.writer,w.version,m.contract FROM work_items w JOIN task_models m ON m.group_name=w.group_name AND m.task=w.id WHERE w.group_name=? AND w.id=?")
        .bind(&actor.group_name).bind(task).fetch_one(&mut **tx).await?;
    ensure!(
        row.get::<String, _>("writer") == actor.name,
        "source writer required for progress policy"
    );
    ensure!(
        row.get::<i64, _>("version") == request.task_version,
        "task version conflict"
    );
    let contract: Contract = serde_json::from_str(&row.get::<String, _>("contract"))?;
    let mut policy = request.policy.clone();
    normalize_policy(&mut policy, &contract)?;
    let current = policy_tx(tx, &actor.group_name, task).await?;
    ensure!(
        current.as_ref().map(|(r, _, _)| *r) == request.expected_revision,
        "progress revision conflict"
    );
    let inputs = task_graph::capture_inputs_tx(tx, &actor.group_name, task, Phase::Execute).await?;
    // The model performs current source/input validation, including held tasks.
    for m in &policy.milestones {
        task_graph::validate_progress_judge_tx(tx, actor, &inputs, &m.id, None).await?;
        let definition = serde_json::to_string(m)?;
        if let Some(old)=sqlx::query_scalar::<_,String>("SELECT definition FROM progress_milestones WHERE group_name=? AND task=? AND milestone=?")
            .bind(&actor.group_name).bind(task).bind(&m.id).fetch_optional(&mut **tx).await? {
            ensure!(old==definition,"milestone identity cannot change meaning");
        } else {
            sqlx::query("INSERT INTO progress_milestones(group_name,task,milestone,definition) VALUES(?,?,?,?)")
                .bind(&actor.group_name).bind(task).bind(&m.id).bind(definition).execute(&mut **tx).await?;
        }
    }
    let revision = request
        .expected_revision
        .unwrap_or(0)
        .checked_add(1)
        .context("progress revision overflow")?;
    let result = append_tx(
        tx,
        actor,
        RecordToAppend {
            task,
            key: &request.key,
            canonical: &canonical,
            kind: "policy",
            payload: &RecordPayload {
                progress_revision: revision,
                reason: request.reason.clone(),
                policy: Some(policy),
                policy_record: None,
                judgment: None,
            },
        },
        now,
    )
    .await?;
    sqlx::query("INSERT INTO task_progress(group_name,task,revision,policy_record) VALUES(?,?,?,?) ON CONFLICT(group_name,task) DO UPDATE SET revision=excluded.revision,policy_record=excluded.policy_record")
        .bind(&actor.group_name).bind(task).bind(revision).bind(result.record).execute(&mut **tx).await?;
    Ok(result)
}

/// Append a real authorized judgment against immutable scheduler evidence.
pub(crate) async fn judge_progress_tx(
    tx: &mut Tx<'_>,
    actor: &Mailbox,
    task: &str,
    request: &JudgmentRequest,
    now: i64,
) -> Result<ProgressReceipt> {
    request_valid(&request.key, &request.reason)?;
    crate::name(task)?;
    crate::name(&request.milestone)?;
    authenticate_tx(tx, actor).await?;
    let canonical = serde_json::to_string(&json!(["judgment", task, request]))?;
    if let Some(old) = replay_tx(tx, actor, &request.key, &canonical).await? {
        return Ok(old);
    }
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM work_items WHERE group_name=? AND id=?")
            .bind(&actor.group_name)
            .bind(task)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(version == request.task_version, "task version conflict");
    let (prior_revision, policy_record, policy) = policy_tx(tx, &actor.group_name, task)
        .await?
        .context("progress policy required")?;
    ensure!(
        prior_revision == request.progress_revision,
        "progress revision conflict"
    );
    let milestone = policy
        .milestones
        .iter()
        .find(|m| m.id == request.milestone)
        .context("milestone not in current policy")?
        .clone();
    let (kind, inputs, report, report_at, report_fence, supersedes) = match &request.change {
        JudgmentChange::Qualify { report, .. } | JudgmentChange::Reject { report } => {
            let snapshot =
                execution::execution_report_tx(tx, &actor.group_name, task, *report).await?;
            ensure!(snapshot.admitted, "progress report was not admitted");
            let supersedes = match &request.change {
                JudgmentChange::Qualify { supersedes, .. } => *supersedes,
                _ => None,
            };
            (
                if matches!(&request.change, JudgmentChange::Qualify { .. }) {
                    "qualified"
                } else {
                    "rejected"
                },
                snapshot.inputs,
                Some(snapshot.event),
                Some(snapshot.recorded_at),
                Some(snapshot.report.correlation.fence),
                supersedes,
            )
        }
        JudgmentChange::Revoke { judgment } => {
            let row=sqlx::query("SELECT payload FROM progress_records WHERE id=? AND group_name=? AND task=? AND kind='qualified'")
                .bind(judgment).bind(&actor.group_name).bind(task).fetch_one(&mut **tx).await?;
            let old: RecordPayload = serde_json::from_str(&row.get::<String, _>("payload"))?;
            let old = old.judgment.context("judgment payload missing")?;
            ensure!(
                old.milestone.id == request.milestone,
                "revocation milestone mismatch"
            );
            // Current authority validates revocation; the target retains its own
            // historical inputs rather than inventing a new report.
            let authority_inputs =
                task_graph::capture_inputs_tx(tx, &actor.group_name, task, Phase::Execute).await?;
            let criteria = task_graph::validate_progress_judge_tx(
                tx,
                actor,
                &authority_inputs,
                &request.milestone,
                request.judge_grant.as_ref(),
            )
            .await?;
            ensure!(
                milestone
                    .criterion_ids
                    .iter()
                    .all(|id| criteria.contains(id)),
                "revocation exceeds model criterion authority"
            );
            ("revoked", old.inputs, None, None, None, Some(*judgment))
        }
    };
    if kind != "revoked" {
        let criteria = task_graph::validate_progress_judge_tx(
            tx,
            actor,
            &inputs,
            &request.milestone,
            request.judge_grant.as_ref(),
        )
        .await?;
        ensure!(
            milestone
                .criterion_ids
                .iter()
                .all(|id| criteria.contains(id)),
            "judgment exceeds model criterion authority"
        );
    }
    let current:Option<i64>=sqlx::query_scalar("SELECT judgment FROM progress_current WHERE group_name=? AND task=? AND milestone=? AND input_epoch=?")
        .bind(&actor.group_name).bind(task).bind(&request.milestone).bind(inputs.input_epoch).fetch_optional(&mut **tx).await?;
    if kind == "qualified" {
        ensure!(
            current == supersedes,
            "qualification replacement must name current judgment"
        );
    }
    if kind == "revoked" {
        ensure!(current == supersedes, "revocation target is not current");
    }
    let revision = prior_revision
        .checked_add(1)
        .context("progress revision overflow")?;
    let payload = RecordPayload {
        progress_revision: revision,
        reason: request.reason.clone(),
        policy: None,
        policy_record: Some(policy_record),
        judgment: Some(StoredJudgment {
            milestone,
            inputs: inputs.clone(),
            report,
            report_at,
            report_fence,
            judge_id: actor.id,
            judge_name: actor.name.clone(),
            judge_grant: request.judge_grant.clone(),
            supersedes,
        }),
    };
    let result = append_tx(
        tx,
        actor,
        RecordToAppend {
            task,
            key: &request.key,
            canonical: &canonical,
            kind,
            payload: &payload,
        },
        now,
    )
    .await?;
    if kind == "qualified" {
        sqlx::query("INSERT INTO progress_current(group_name,task,milestone,input_epoch,judgment) VALUES(?,?,?,?,?) ON CONFLICT(group_name,task,milestone,input_epoch) DO UPDATE SET judgment=excluded.judgment")
            .bind(&actor.group_name).bind(task).bind(&request.milestone).bind(inputs.input_epoch).bind(result.record).execute(&mut **tx).await?;
    } else if kind == "revoked" {
        sqlx::query("DELETE FROM progress_current WHERE group_name=? AND task=? AND milestone=? AND input_epoch=? AND judgment=?")
            .bind(&actor.group_name).bind(task).bind(&request.milestone).bind(inputs.input_epoch).bind(supersedes).execute(&mut **tx).await?;
    }
    sqlx::query("UPDATE task_progress SET revision=? WHERE group_name=? AND task=? AND revision=?")
        .bind(revision)
        .bind(&actor.group_name)
        .bind(task)
        .bind(prior_revision)
        .execute(&mut **tx)
        .await?;
    Ok(result)
}

/// Source pointers and policy, never an execution permission or copied ledger.
#[derive(Debug, Clone)]
pub(crate) struct ProgressBoundary {
    pub policy_record: Option<i64>,
    pub progress_revision: Option<i64>,
    pub max_segments_without_milestone: u32,
    pub max_elapsed_without_milestone: Option<u64>,
    pub judgment_record: Option<i64>,
    pub report_event: Option<i64>,
}

/// Read policy/current evidence in the scheduler's same writer transaction.
/// Uses the actual model-owned stored-provenance validator from Mail1344.
/// Its pinned composition is required; no historical Mailbox is constructed.
pub(crate) async fn progress_boundary_tx(
    tx: &mut Tx<'_>,
    group: &str,
    task: &str,
    inputs: &InputSnapshot,
) -> Result<ProgressBoundary> {
    home_tx(tx, group).await?;
    ensure!(
        inputs.group == group && inputs.task == task,
        "progress input identity mismatch"
    );
    ensure!(
        task_graph::validate_inputs_tx(tx, inputs).await? == InputValidity::Current,
        "progress inputs stale"
    );
    let (revision, record, policy) = match policy_tx(tx, group, task).await? {
        Some((revision, record, policy)) => (Some(revision), Some(record), policy),
        None => (None, None, ProgressPolicy::default()),
    };
    let rows=sqlx::query("SELECT r.id,r.payload FROM progress_current c JOIN progress_records r ON r.id=c.judgment WHERE c.group_name=? AND c.task=? AND c.input_epoch=? ORDER BY r.id LIMIT 33")
        .bind(group).bind(task).bind(inputs.input_epoch).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 32, "progress milestone inventory limit");
    let mut selected: Option<(i64, i64, i64, i64)> = None;
    for row in rows {
        let payload: RecordPayload = serde_json::from_str(&row.get::<String, _>("payload"))?;
        let judgment = payload.judgment.context("qualified judgment missing")?;
        if !policy.milestones.contains(&judgment.milestone) {
            continue;
        }
        if task_graph::validate_inputs_tx(tx, &judgment.inputs).await? != InputValidity::Current {
            continue;
        }
        let criteria = task_graph::validate_stored_progress_judge_tx(
            tx,
            judgment.judge_id,
            &judgment.judge_name,
            &judgment.inputs,
            &judgment.milestone.id,
            judgment.judge_grant.as_ref(),
        )
        .await?;
        ensure!(
            judgment
                .milestone
                .criterion_ids
                .iter()
                .all(|id| criteria.contains(id)),
            "stored judgment criterion authority changed"
        );
        let report = execution::execution_report_tx(
            tx,
            group,
            task,
            judgment.report.context("qualification report missing")?,
        )
        .await?;
        ensure!(
            report.admitted
                && report.inputs == judgment.inputs
                && Some(report.recorded_at) == judgment.report_at
                && Some(report.report.correlation.fence) == judgment.report_fence,
            "stored judgment report provenance mismatch"
        );
        // Select by the original segment/time, never the approval's later time.
        let candidate = (
            report.report.correlation.fence,
            report.recorded_at,
            report.event,
            row.get::<i64, _>("id"),
        );
        if selected.as_ref().is_none_or(|prior| candidate > *prior) {
            selected = Some(candidate);
        }
    }
    Ok(ProgressBoundary {
        policy_record: record,
        progress_revision: revision,
        max_segments_without_milestone: policy.max_segments_without_milestone,
        max_elapsed_without_milestone: policy.max_elapsed_without_milestone,
        judgment_record: selected.map(|s| s.3),
        report_event: selected.map(|s| s.2),
    })
}

impl Store {
    /// Set policy without changing task versions, execution capacity or elapsed anchors.
    pub async fn progress_policy(
        &self,
        actor: &Mailbox,
        task: &str,
        request: &PolicyChange,
        now: i64,
    ) -> Result<ProgressReceipt> {
        let mut tx = self.pool().begin().await?;
        let result = change_policy_tx(&mut tx, actor, task, request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
    /// Append an authorized qualification, rejection or explicit revocation.
    pub async fn progress_judge(
        &self,
        actor: &Mailbox,
        task: &str,
        request: &JudgmentRequest,
        now: i64,
    ) -> Result<ProgressReceipt> {
        let mut tx = self.pool().begin().await?;
        let result = judge_progress_tx(&mut tx, actor, task, request, now).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
    }
    /// Read immutable progress history without claiming delivery or retrieval.
    pub async fn progress_history(
        &self,
        actor: &Mailbox,
        task: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        ensure!(
            after >= 0 && (1..=100).contains(&limit),
            "invalid progress history page"
        );
        let mut tx = self.pool().begin().await?;
        authenticate_tx(&mut tx, actor).await?;
        let rows=sqlx::query("SELECT id,kind,payload,created FROM progress_records WHERE group_name=? AND task=? AND id>? ORDER BY id LIMIT ?")
            .bind(&actor.group_name).bind(task).bind(after).bind(limit as i64).fetch_all(&mut *tx).await?;
        let result=rows.into_iter().map(|r| Ok(json!({"id":r.get::<i64,_>("id"),"kind":r.get::<String,_>("kind"),"payload":serde_json::from_str::<Value>(&r.get::<String,_>("payload"))?,"created":r.get::<i64,_>("created")}))).collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(result)
    }
}
