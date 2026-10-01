//! Durable attention attached to authoritative tasks and recipient deliveries.
//! Checkpoints report intent. Only ordinary task/mail operations settle work.
use crate::{
    bounded,
    states::{EventKind, MessageState, TaskState},
    store::{Mailbox, Message, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};

/// Source observed before recording a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    /// A current occurrence addressed to this agent, including escalation to a sender.
    Attention {
        /// Attention occurrence identifier, not a source-plan identifier.
        id: i64,
    },
    /// A locally authoritative assignment at a specific revision.
    Task {
        /// Task identifier.
        id: String,
        /// Observed task revision.
        version: i64,
    },
    /// A pending inbox delivery, or the sender's sole original local delivery.
    /// Original self-recipient selection takes precedence.
    Mail {
        /// Message identifier.
        id: i64,
    },
}
/// Condition to reassess; satisfaction never grants business authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitFor {
    /// Reassess a same-group task when it reaches a listed state.
    Task {
        /// Dependency identifier.
        id: String,
        /// Explicitly selected qualifying states.
        states: Vec<TaskState>,
    },
    /// Reassess an outgoing request when a reply arrives or all deliveries settle.
    Mail {
        /// Outgoing message identifier.
        id: i64,
    },
    /// An external condition needs a person or role to review it.
    External {
        /// Person or role responsible for the decision.
        responsible: String,
        /// Concrete condition preventing progress.
        reason: String,
    },
}
/// Reported next step for unfinished work. Times are UTC Unix seconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    /// Observed attention metadata version (zero for an initial plan).
    pub version: i64,
    /// Concrete intended next action, not a task-state mutation.
    pub next_step: String,
    /// Next time to reassess this report.
    pub next_check_at: i64,
    /// Optional dependency; time alone suffices for active work.
    #[serde(default)]
    pub waiting: Option<WaitFor>,
    /// Reported evidence references, never inferred verification.
    #[serde(default)]
    pub evidence: Vec<String>,
    /// Writer-only explicit extension of the escalation boundary.
    #[serde(default)]
    pub extend_until: Option<i64>,
    /// Required explanation for an explicit extension.
    #[serde(default)]
    pub reason: Option<String>,
}
/// Whether follow-through is observed or actively dispatched.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Show due work without generating reminders.
    Observe,
    /// Dispatch bounded reminders under existing runtime policy.
    Enabled,
}
impl Mode {
    fn text(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Enabled => "enabled",
        }
    }
}
/// Operator-configured group policy; independent of task deadlines.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Dispatch mode; new groups default to enabled, saved policies are preserved.
    pub mode: Mode,
    /// Interval between the two follow-up opportunities.
    pub interval_seconds: i64,
    /// Maximum unattended time, including an unavailable runtime.
    pub max_seconds: i64,
    /// Absolute executable and arguments; receives bounded JSON on stdin.
    #[serde(default)]
    pub notifier: Option<Vec<String>>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            mode: Mode::Enabled,
            interval_seconds: 900,
            max_seconds: 3600,
            notifier: None,
        }
    }
}
/// Partial operator update. Omitted fields retain their saved values.
#[derive(Debug, Default)]
pub struct PolicyPatch {
    /// Dispatch mode.
    pub mode: Option<Mode>,
    /// Recovery reminder interval in seconds.
    pub interval_seconds: Option<i64>,
    /// Maximum unattended interval in seconds.
    pub max_seconds: Option<i64>,
    /// None preserves the route; Some(None) clears it.
    pub notifier: Option<Option<Vec<String>>>,
}
impl From<Policy> for PolicyPatch {
    fn from(policy: Policy) -> Self {
        Self {
            mode: Some(policy.mode),
            interval_seconds: Some(policy.interval_seconds),
            max_seconds: Some(policy.max_seconds),
            notifier: Some(policy.notifier),
        }
    }
}
fn policy_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Policy> {
    Ok(Policy {
        mode: serde_json::from_value(json!(row.get::<String, _>("mode")))?,
        interval_seconds: row.get("interval_seconds"),
        max_seconds: row.get("max_seconds"),
        notifier: row
            .get::<Option<String>, _>("notifier")
            .map(|v| serde_json::from_str(&v))
            .transpose()?,
    })
}
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
struct Plan {
    id: i64,
    group_name: String,
    task: Option<String>,
    task_version: i64,
    message: Option<i64>,
    recipient: i64,
    authority: i64,
    version: i64,
    opened: i64,
    retrieved_at: Option<i64>,
    retrieved_binding: Option<i64>,
    checkpoint: Option<String>,
    dependency_ready_at: Option<i64>,
    next_check: i64,
    escalate_at: i64,
    stage: i64,
    scanned: i64,
}
impl Plan {
    fn report(&self) -> Result<Option<Checkpoint>> {
        self.checkpoint
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("invalid stored checkpoint")
    }
    fn value(&self) -> Result<Value> {
        let mut value = serde_json::to_value(self)?;
        value["checkpoint"] = serde_json::to_value(self.report()?)?;
        Ok(value)
    }
}

/// An explicit original source selected by the recovery transaction.
#[derive(Debug)]
pub(crate) enum CorrectionSource {
    Task { id: String, version: i64 },
    Delivery { message: i64, recipient: i64 },
}

/// Apply attention metadata only. The caller owns exact retry/case CAS and must
/// append the returned before/after facts to its immutable audit in this same
/// transaction before commit. This helper independently checks source authority;
/// an ID or observed plan supplied by the caller is never authorization.
pub(crate) async fn correct_obligation_plan_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    source: &CorrectionSource,
    expected: Option<(i64, i64)>,
    report: &Checkpoint,
    time: i64,
) -> Result<Value> {
    Store::lock_actor(tx, actor).await?;
    let home = sqlx::query(
        "UPDATE groups SET paused=paused WHERE name=? AND home_machine IN (SELECT id FROM node)",
    )
    .bind(&actor.group_name)
    .execute(&mut **tx)
    .await?;
    ensure!(
        home.rows_affected() == 1,
        "source correction requires the home store"
    );
    bounded(&report.next_step, 512, "next step")?;
    ensure!(
        !report.next_step.trim().is_empty(),
        "concrete next step required"
    );
    ensure!(
        report.waiting.is_none(),
        "source correction cannot add a dependency"
    );
    let reason = report.reason.as_deref().unwrap_or_default();
    bounded(reason, 512, "correction reason")?;
    ensure!(
        !reason.trim().is_empty(),
        "source correction requires a reason"
    );
    ensure!(report.evidence.len() <= 16, "too many evidence references");
    for item in &report.evidence {
        bounded(item, 256, "evidence")?;
        ensure!(!item.trim().is_empty(), "empty evidence");
    }
    ensure!(
        report.version == expected.map_or(0, |(_, version)| version),
        "correction report version differs from plan expectation"
    );
    let boundary = report
        .extend_until
        .context("explicit correction boundary required")?;
    ensure!(
        time < report.next_check_at && report.next_check_at <= boundary,
        "require now < review <= hard boundary"
    );
    let (task, task_version, message, recipient, opened, source_facts) = match source {
        CorrectionSource::Task { id, version } => {
            let row = sqlx::query("SELECT w.version,w.open,w.deadline,w.writer,a.id AS authority,a.remote_machine AS authority_remote,b.id AS recipient,b.remote_machine,(SELECT MIN(changed) FROM work_changes h WHERE h.group_name=w.group_name AND h.work_id=w.id) AS opened FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer WHERE w.group_name=? AND w.id=?")
                .bind(&actor.group_name).bind(id).fetch_optional(&mut **tx).await?.context("task source missing")?;
            ensure!(
                row.get::<String, _>("writer") == actor.name
                    && row.get::<i64, _>("authority") == actor.id
                    && row.get::<Option<String>, _>("authority_remote").is_none(),
                "only original local task writer may correct its plan"
            );
            ensure!(
                row.get::<i64, _>("version") == *version && row.get::<i64, _>("open") == 1,
                "task changed or settled"
            );
            ensure!(
                row.get::<Option<String>, _>("remote_machine").is_none(),
                "remote task correction unsupported"
            );
            let opened = row
                .get::<Option<i64>, _>("opened")
                .context("original task opening evidence missing")?;
            (
                Some(id.clone()),
                *version,
                None,
                row.get::<i64, _>("recipient"),
                opened,
                json!({"task":id,"version":version,"writer":actor.name,"business_deadline":row.get::<Option<i64>,_>("deadline"),"opened":opened}),
            )
        }
        CorrectionSource::Delivery { message, recipient } => {
            let row = sqlx::query("SELECT m.sender,m.created,m.due,d.state,b.remote_machine FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN mailboxes a ON a.id=m.sender WHERE d.message=? AND d.recipient=? AND b.group_name=? AND a.group_name=b.group_name AND a.remote_machine IS NULL")
                .bind(message).bind(recipient).bind(&actor.group_name).fetch_optional(&mut **tx).await?.context("original local delivery missing")?;
            ensure!(
                row.get::<i64, _>("sender") == actor.id,
                "only original sender may correct delivery plan"
            );
            ensure!(
                row.get::<String, _>("state") == "pending",
                "delivery is no longer pending"
            );
            ensure!(
                row.get::<Option<String>, _>("remote_machine").is_none(),
                "remote delivery correction unsupported"
            );
            (
                None,
                0,
                Some(*message),
                *recipient,
                row.get::<i64, _>("created"),
                json!({"message":message,"recipient":recipient,"sender":actor.id,"original_due":row.get::<i64,_>("due"),"opened":row.get::<i64,_>("created")}),
            )
        }
    };
    let before = sqlx::query_as::<_, Plan>(
        "SELECT * FROM followups WHERE group_name=? AND (task=? OR (message=? AND recipient=?))",
    )
    .bind(&actor.group_name)
    .bind(&task)
    .bind(message)
    .bind(recipient)
    .fetch_optional(&mut **tx)
    .await?;
    ensure!(
        before.as_ref().map(|p| (p.id, p.version)) == expected,
        "source plan identity/version conflict"
    );
    if let Some(plan) = &before {
        ensure!(
            plan.authority == actor.id
                && plan.recipient == recipient
                && plan.task_version == task_version,
            "source plan authority or revision is corrupt"
        );
        plan.version
            .checked_add(1)
            .context("source plan revision overflow")?;
        let result = sqlx::query("UPDATE followups SET version=version+1,checkpoint=?,next_check=?,escalate_at=?,stage=0,scanned=0,dependency_ready_at=NULL WHERE id=? AND version=?")
            .bind(serde_json::to_string(report)?).bind(report.next_check_at).bind(boundary).bind(plan.id).bind(plan.version).execute(&mut **tx).await?;
        ensure!(result.rows_affected() == 1, "source plan changed");
    } else {
        sqlx::query("INSERT INTO followups(group_name,task,task_version,message,recipient,authority,opened,checkpoint,next_check,escalate_at) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(&actor.group_name).bind(&task).bind(task_version).bind(message).bind(recipient).bind(actor.id)
            .bind(opened).bind(serde_json::to_string(report)?).bind(report.next_check_at).bind(boundary).execute(&mut **tx).await?;
    }
    let after = sqlx::query_as::<_, Plan>(
        "SELECT * FROM followups WHERE group_name=? AND (task=? OR (message=? AND recipient=?))",
    )
    .bind(&actor.group_name)
    .bind(&task)
    .bind(message)
    .bind(recipient)
    .fetch_one(&mut **tx)
    .await?;
    Ok(
        json!({"source":source_facts,"before":before.map(|p|p.value()).transpose()?,"after":after.value()?,"actor_binding":actor.binding_version}),
    )
}

/// Captured admission data, never authority supplied by a runtime/native DTO.
/// Consumers compare protected original storage with a fresh authenticated read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointTaskBasis {
    pub(crate) group_name: String,
    pub(crate) task: String,
    pub(crate) task_version: i64,
    pub(crate) followup: i64,
    pub(crate) version: i64,
    pub(crate) recipient: i64,
    pub(crate) authority: i64,
    pub(crate) opened: i64,
    pub(crate) escalate_at: i64,
    pub(crate) actor: i64,
    pub(crate) binding_version: i64,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct CheckpointHistoryRecord {
    id: i64,
    followup: i64,
    version: i64,
    actor: i64,
    key: String,
    canonical: String,
    snapshot: String,
    created: i64,
}

/// Opaque result of the actual history write/read; it does not commit the caller.
#[derive(Debug)]
pub(crate) struct CheckpointWrite {
    history: CheckpointHistoryRecord,
    snapshot: Value,
    replay: bool,
}

impl CheckpointWrite {
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> &Value {
        &self.snapshot
    }
    pub(crate) fn into_snapshot(self) -> Value {
        self.snapshot
    }
    #[cfg(test)]
    pub(crate) fn history_id(&self) -> i64 {
        self.history.id
    }
    #[cfg(test)]
    pub(crate) fn recorded_at(&self) -> i64 {
        self.history.created
    }
    pub(crate) fn is_replay(&self) -> bool {
        self.replay
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointCanonical {
    source: Source,
    checkpoint: Checkpoint,
}

// Read only the actual fields needed from the immutable public snapshot. Other
// existing public metadata remains in CheckpointWrite::snapshot unchanged.
#[derive(Deserialize)]
struct CheckpointHistorySnapshot {
    id: i64,
    group_name: String,
    task: Option<String>,
    task_version: i64,
    message: Option<i64>,
    recipient: i64,
    authority: i64,
    version: i64,
    opened: i64,
    checkpoint: Option<Checkpoint>,
    next_check: i64,
    escalate_at: i64,
}

/// History authenticated in the consuming transaction, not fresh admission.
/// Scheduler still validates the actual report, original basis and current gates.
#[derive(Debug)]
pub(crate) struct CheckpointHistoryProof {
    history_id: i64,
    actor: i64,
    source: Source,
    checkpoint: Checkpoint,
    followup: i64,
    version: i64,
    recorded_at: i64,
    opened: i64,
    escalate_at: i64,
    next_check_at: i64,
}

impl CheckpointHistoryProof {
    pub(crate) fn history_id(&self) -> i64 {
        self.history_id
    }
    pub(crate) fn actor(&self) -> i64 {
        self.actor
    }
    pub(crate) fn source(&self) -> &Source {
        &self.source
    }
    pub(crate) fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub(crate) fn followup(&self) -> i64 {
        self.followup
    }
    pub(crate) fn version(&self) -> i64 {
        self.version
    }
    pub(crate) fn recorded_at(&self) -> i64 {
        self.recorded_at
    }
    pub(crate) fn opened(&self) -> i64 {
        self.opened
    }
    pub(crate) fn escalate_at(&self) -> i64 {
        self.escalate_at
    }
    pub(crate) fn next_check_at(&self) -> i64 {
        self.next_check_at
    }
}

async fn checkpoint_history_record_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: i64,
    key: &str,
) -> Result<Option<CheckpointHistoryRecord>> {
    Ok(sqlx::query_as::<_, CheckpointHistoryRecord>(
        "SELECT id,followup,version,actor,key,canonical,snapshot,created FROM followup_history WHERE actor=? AND key=?",
    )
    .bind(actor).bind(key).fetch_optional(&mut **tx).await?)
}

async fn checkpoint_task_plan_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    task: &str,
) -> Result<Plan> {
    let plan =
        sqlx::query_as::<_, Plan>("SELECT * FROM active_followups WHERE group_name=? AND task=?")
            .bind(&actor.group_name)
            .bind(task)
            .fetch_optional(&mut **tx)
            .await?
            .context("no active local task; remote follow-up unsupported")?;
    ensure!(
        plan.recipient == actor.id || plan.authority == actor.id,
        "only the task owner or writer may checkpoint"
    );
    Ok(plan)
}

/// Capture the real task/followup basis under the admission writer reservation.
pub(crate) async fn checkpoint_task_basis_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    task: &str,
) -> Result<CheckpointTaskBasis> {
    Store::lock_actor(tx, actor).await?;
    let plan = checkpoint_task_plan_tx(tx, actor, task).await?;
    Ok(CheckpointTaskBasis {
        group_name: plan.group_name,
        task: plan.task.context("task checkpoint source missing task")?,
        task_version: plan.task_version,
        followup: plan.id,
        version: plan.version,
        recipient: plan.recipient,
        authority: plan.authority,
        opened: plan.opened,
        escalate_at: plan.escalate_at,
        actor: actor.id,
        binding_version: actor.binding_version,
    })
}

/// Resolve the real immutable history again in the scheduler's transaction.
/// A rolled-back write or copied DTO cannot supply this opaque proof.
pub(crate) async fn validate_checkpoint_write_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    write: &CheckpointWrite,
) -> Result<CheckpointHistoryProof> {
    Store::lock_actor(tx, actor).await?;
    ensure!(
        write.history.actor == actor.id,
        "checkpoint history actor mismatch"
    );
    let history = checkpoint_history_record_tx(tx, actor.id, &write.history.key)
        .await?
        .context("checkpoint history is not persisted in this transaction")?;
    ensure!(
        history == write.history,
        "checkpoint history identity or bytes changed"
    );
    let canonical: CheckpointCanonical = serde_json::from_str(&history.canonical)?;
    let snapshot: CheckpointHistorySnapshot = serde_json::from_str(&history.snapshot)?;
    ensure!(
        snapshot.id == history.followup
            && snapshot.version == history.version
            && snapshot.group_name == actor.group_name
            && (snapshot.recipient == actor.id || snapshot.authority == actor.id),
        "checkpoint history source identity mismatch"
    );
    match &canonical.source {
        Source::Task { id, version } => ensure!(
            snapshot.task.as_ref() == Some(id) && snapshot.task_version == *version,
            "checkpoint history task basis mismatch"
        ),
        Source::Mail { id } => ensure!(
            snapshot.message == Some(*id),
            "checkpoint history mail source mismatch"
        ),
        Source::Attention { .. } => {}
    }
    let mut stored = snapshot
        .checkpoint
        .context("checkpoint history snapshot missing report")?;
    let mut requested = canonical.checkpoint.clone();
    // An identical checkpoint can save a new retry key without rewriting the
    // plan's prior report.version. This is existing public no-op behavior.
    stored.version = 0;
    requested.version = 0;
    ensure!(
        stored == requested,
        "checkpoint history report differs from canonical request"
    );
    Ok(CheckpointHistoryProof {
        history_id: history.id,
        actor: history.actor,
        source: canonical.source,
        checkpoint: canonical.checkpoint,
        followup: history.followup,
        version: history.version,
        recorded_at: history.created,
        opened: snapshot.opened,
        escalate_at: snapshot.escalate_at,
        next_check_at: snapshot.next_check,
    })
}

/// Record the actual checkpoint in the caller's writer transaction.
/// The caller commits before hinting; this result alone is not a commit receipt.
pub(crate) async fn checkpoint_tx(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    source: Source,
    key: &str,
    report: Checkpoint,
    time: i64,
) -> Result<CheckpointWrite> {
    bounded(key, 128, "checkpoint key")?;
    ensure!(!key.is_empty(), "checkpoint key is empty");
    bounded(&report.next_step, 512, "next step")?;
    ensure!(
        !report.next_step.trim().is_empty(),
        "concrete next_step is required"
    );
    ensure!(report.evidence.len() <= 16, "too many evidence references");
    for item in &report.evidence {
        bounded(item, 256, "evidence")?;
        ensure!(!item.trim().is_empty(), "empty evidence");
    }
    if let Some(reason) = &report.reason {
        bounded(reason, 512, "extension reason")?;
    }
    let canonical = serde_json::to_string(&json!({"source":source,"checkpoint":report}))?;
    Store::lock_actor(tx, actor).await?;
    if let Some(history) = checkpoint_history_record_tx(tx, actor.id, key).await? {
        ensure!(
            history.canonical == canonical,
            "checkpoint key already used with different content"
        );
        let snapshot = serde_json::from_str(&history.snapshot)?;
        return Ok(CheckpointWrite {
            history,
            snapshot,
            replay: true,
        });
    }
    let plan = match &source {
        Source::Attention{id}=>sqlx::query_as::<_,Plan>("SELECT f.* FROM active_followups f JOIN active_attention o ON o.followup=f.id WHERE o.id=? AND o.recipient=? AND (f.recipient=? OR f.authority=?)")
            .bind(id).bind(actor.id).bind(actor.id).bind(actor.id).fetch_optional(&mut **tx).await?.context("attention occurrence is stale or not addressed to this agent")?,
        Source::Task { id, version } => {
            let plan = checkpoint_task_plan_tx(tx, actor, id).await?;
            ensure!(
                plan.task_version == *version,
                "task revision changed; run task show {id} and reconsider (current revision {})",
                plan.task_version
            );
            plan
        }
        Source::Mail { id } => {
            // Preserve an original self-recipient's inbox semantics, even
            // after that delivery settles. Never select a different target.
            let self_delivery: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE message=? AND recipient=?")
                .bind(id).bind(actor.id).fetch_one(&mut **tx).await?;
            if self_delivery != 0 {
                sqlx::query_as::<_, Plan>("SELECT * FROM active_followups WHERE message=? AND recipient=?")
                    .bind(id).bind(actor.id).fetch_optional(&mut **tx).await?
                    .context("message is not pending in this inbox")?
            } else {
                // Count ALL original recipients, including settled/remote
                // ones. One remaining pending row does not remove ambiguity.
                let originals: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE message=?")
                    .bind(id).fetch_one(&mut **tx).await?;
                ensure!(originals == 1, "sender checkpoint requires exactly one original recipient; use explicit delivery correction");
                sqlx::query_as::<_, Plan>("SELECT f.* FROM followups f JOIN deliveries d ON d.message=f.message AND d.recipient=f.recipient JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN mailboxes a ON a.id=m.sender JOIN groups g ON g.name=b.group_name WHERE m.id=? AND m.sender=? AND a.group_name=? AND b.group_name=a.group_name AND b.remote_machine IS NULL AND a.remote_machine IS NULL AND d.state='pending' AND f.authority=m.sender AND f.group_name=b.group_name AND g.home_machine IN (SELECT id FROM node)")
                    .bind(id).bind(actor.id).bind(&actor.group_name).fetch_optional(&mut **tx).await?
                    .context("sender has no single pending home-local delivery plan")?
            }
        },
    };
    ensure!(
        plan.version == report.version,
        "checkpoint version conflict; current attention metadata: {}",
        plan.value()?
    );
    ensure!(
        report.next_check_at > time,
        "next_check_at must be in the future"
    );
    let mut boundary = plan.escalate_at;
    if let Some(until) = report.extend_until {
        ensure!(
            actor.id == plan.authority,
            "only the task writer/request sender can extend escalation; recipients must report the blocker to that authority"
        );
        ensure!(
            until > time && until >= boundary,
            "extension must preserve the existing boundary and be in the future"
        );
        let reason = report.reason.as_deref().unwrap_or_default();
        bounded(reason, 512, "extension reason")?;
        ensure!(
            !reason.trim().is_empty(),
            "extension requires an audited reason"
        );
        boundary = until;
    }
    ensure!(
        report.next_check_at <= boundary,
        "checkpoint exceeds escalation boundary {boundary}; request an explicit authority decision"
    );
    if let Some(wait) = &report.waiting {
        validate_wait(tx, actor, &plan, wait).await?;
    }
    let mut normalized = report.clone();
    normalized.version = 0;
    let unchanged = plan.report()?.is_some_and(|mut old| {
        old.version = 0;
        old == normalized
    });
    if !unchanged {
        sqlx::query("UPDATE followups SET version=version+1,checkpoint=?,next_check=?,escalate_at=?,stage=0,scanned=0,dependency_ready_at=NULL WHERE id=? AND version=?")
            .bind(serde_json::to_string(&report)?).bind(report.next_check_at).bind(boundary).bind(plan.id).bind(plan.version).execute(&mut **tx).await?;
    }
    let current = sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE id=?")
        .bind(plan.id)
        .fetch_one(&mut **tx)
        .await?;
    // Recheck before committing the checkpoint and its exact retry response.
    if report.waiting.is_some() && waiting_satisfied(tx, &current).await? {
        sqlx::query("UPDATE followups SET next_check=MIN(next_check,?) WHERE id=?")
            .bind(time)
            .bind(plan.id)
            .execute(&mut **tx)
            .await?;
    }
    let current = sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE id=?")
        .bind(plan.id)
        .fetch_one(&mut **tx)
        .await?;
    let snapshot = current.value()?;
    let snapshot_json = serde_json::to_string(&snapshot)?;
    let inserted = sqlx::query("INSERT INTO followup_history(followup,version,actor,key,canonical,snapshot,created) VALUES(?,?,?,?,?,?,?)")
        .bind(plan.id).bind(current.version).bind(actor.id).bind(key).bind(&canonical)
        .bind(&snapshot_json).bind(time).execute(&mut **tx).await?;
    Ok(CheckpointWrite {
        history: CheckpointHistoryRecord {
            id: inserted.last_insert_rowid(),
            followup: plan.id,
            version: current.version,
            actor: actor.id,
            key: key.to_owned(),
            canonical,
            snapshot: snapshot_json,
            created: time,
        },
        snapshot,
        replay: false,
    })
}

impl Store {
    /// Configure bounded follow-through. This is an operator action.
    pub async fn configure_followups(&self, group: &str, policy: &Policy, time: i64) -> Result<()> {
        self.patch_followups(group, &policy.clone().into(), time)
            .await?;
        Ok(())
    }
    /// Read the effective group policy without changing it.
    pub async fn followup_policy(&self, group: &str) -> Result<Policy> {
        policy_from_row(
            &sqlx::query("SELECT * FROM followup_policy WHERE group_name=?")
                .bind(group)
                .fetch_one(self.pool())
                .await?,
        )
    }
    /// Atomically merge and validate settings, preserving unspecified values.
    pub async fn patch_followups(
        &self,
        group: &str,
        patch: &PolicyPatch,
        time: i64,
    ) -> Result<Policy> {
        if patch.mode.is_none()
            && patch.interval_seconds.is_none()
            && patch.max_seconds.is_none()
            && patch.notifier.is_none()
        {
            return self.followup_policy(group).await;
        }
        let mut tx = self.pool().begin().await?;
        sqlx::query("UPDATE followup_policy SET updated=updated WHERE group_name=?")
            .bind(group)
            .execute(&mut *tx)
            .await?;
        let previous = sqlx::query("SELECT * FROM followup_policy WHERE group_name=?")
            .bind(group)
            .fetch_one(&mut *tx)
            .await?;
        let mut policy = policy_from_row(&previous)?;
        let previous_notifier = policy
            .notifier
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        if let Some(mode) = patch.mode {
            policy.mode = mode;
        }
        if let Some(interval) = patch.interval_seconds {
            policy.interval_seconds = interval;
        }
        if let Some(max) = patch.max_seconds {
            policy.max_seconds = max;
        }
        if let Some(notifier) = &patch.notifier {
            policy.notifier = notifier.clone();
        }
        ensure!(
            (60..=86400).contains(&policy.interval_seconds),
            "interval_seconds must be 60..86400"
        );
        ensure!(
            (policy.interval_seconds * 4..=604800).contains(&policy.max_seconds),
            "max_seconds must be at least four intervals and at most seven days"
        );
        if let Some(args) = &policy.notifier {
            ensure!(
                !args.is_empty()
                    && args.len() <= 16
                    && std::path::Path::new(&args[0]).is_absolute(),
                "notifier requires an absolute executable and at most 15 arguments"
            );
            for arg in args {
                bounded(arg, 1024, "notifier argument")?;
            }
        }
        let notifier = policy
            .notifier
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let route_change = previous_notifier != notifier;
        let route_generation = if route_change {
            Some(crate::operator_notices::route_generation_tx(&mut tx, group).await?)
        } else {
            None
        };
        sqlx::query("UPDATE followup_policy SET mode=?,interval_seconds=?,max_seconds=?,notifier=?,updated=? WHERE group_name=?")
            .bind(policy.mode.text()).bind(policy.interval_seconds).bind(policy.max_seconds).bind(&notifier).bind(time).bind(group).execute(&mut *tx).await?;
        if previous.get::<String, _>("mode") == "observe" && policy.mode == Mode::Enabled {
            // Explicit activation gives unplanned historical records a grace period.
            sqlx::query("UPDATE followups SET next_check=MAX(next_check,?),escalate_at=MAX(escalate_at,?) WHERE group_name=? AND version=0 AND stage=0")
                .bind(time.checked_add(policy.interval_seconds).context("clock overflow")?).bind(time.checked_add(policy.max_seconds).context("clock overflow")?).bind(group).execute(&mut *tx).await?;
        }
        if let Some(generation) = route_generation {
            let key = crate::operator_notices::route_repair_key(group, generation, &notifier);
            crate::operator_notices::repair_operator_route_tx(
                &mut tx,
                group,
                generation,
                &key,
                "Trusted operator changed notifier configuration",
                time,
            )
            .await?;
        }
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(policy)
    }
    /// Record an authenticated progress report without changing business state.
    pub async fn checkpoint(
        &self,
        actor: &Mailbox,
        source: Source,
        key: &str,
        report: Checkpoint,
        time: i64,
    ) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        let write = checkpoint_tx(&mut tx, actor, source, key, report, time).await?;
        let should_hint = !write.is_replay();
        tx.commit().await?;
        if should_hint {
            #[cfg(test)]
            checkpoint_public_controls::before_hint(self.root(), actor, key).await;
            crate::stream::hint(self.root()).await;
        }
        Ok(write.into_snapshot())
    }
    /// Read current metadata for a source; administrative reads do not receipt it.
    pub async fn source_followup(
        &self,
        actor: &Mailbox,
        task: Option<&str>,
        message: Option<i64>,
    ) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let row=sqlx::query_as::<_,Plan>("SELECT * FROM followups WHERE group_name=? AND ((task=? AND (recipient=? OR authority=?)) OR (message=? AND recipient=?))")
            .bind(&actor.group_name).bind(task).bind(actor.id).bind(actor.id).bind(message).bind(actor.id).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        row.map_or(Ok(json!({"supported":false,"reason":"no local follow-up visible; remote follow-up unsupported"})),|p|p.value())
    }
    /// List checkpoint history visible to the current responsible actor.
    pub async fn checkpoint_history(
        &self,
        actor: &Mailbox,
        task: Option<&str>,
        message: Option<i64>,
    ) -> Result<Vec<Value>> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows=sqlx::query("SELECT h.snapshot,h.created,b.name AS author FROM followup_history h JOIN followups f ON f.id=h.followup JOIN mailboxes b ON b.id=h.actor WHERE f.group_name=? AND (f.recipient=? OR f.authority=?) AND (f.task=? OR f.message=?) ORDER BY h.id DESC LIMIT 20")
            .bind(&actor.group_name).bind(actor.id).bind(actor.id).bind(task).bind(message).fetch_all(&mut *tx).await?;
        let values=rows.into_iter().map(|r|Ok(json!({"checkpoint":serde_json::from_str::<Value>(&r.get::<String,_>("snapshot"))?,"created":r.get::<i64,_>("created"),"author":r.get::<String,_>("author")}))).collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(values)
    }
    /// Read a bounded page of attention occurrences. This read does not receipt hidden details.
    pub async fn attention_list(&self, actor: &Mailbox, after: i64) -> Result<Value> {
        ensure!(after >= 0, "attention cursor must be nonnegative");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows=sqlx::query("SELECT o.id,o.stage,o.reason,o.created,f.task,f.task_version,f.message,f.version FROM active_attention o JOIN followups f ON f.id=o.followup WHERE o.recipient=? AND o.id>? ORDER BY o.id LIMIT 6")
            .bind(actor.id).bind(after).fetch_all(&mut *tx).await?;
        let more = rows.len() > 5;
        let items:Vec<Value>=rows.iter().take(5).map(|r|json!({"id":r.get::<i64,_>("id"),"stage":r.get::<i64,_>("stage"),"reason":r.get::<String,_>("reason"),"task":r.get::<Option<String>,_>("task"),"task_version":r.get::<i64,_>("task_version"),"message":r.get::<Option<i64>,_>("message"),"version":r.get::<i64,_>("version")})).collect();
        let next = items.last().and_then(|v| v["id"].as_i64()).unwrap_or(after);
        tx.commit().await?;
        Ok(json!({"items":items,"more":more,"next_after":next}))
    }
    /// Retrieve exactly one addressed occurrence; this does not settle its source.
    pub async fn attention_show(&self, actor: &Mailbox, id: i64) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let row=sqlx::query("SELECT followup,plan_version,stage,reason FROM attention_occurrences WHERE id=? AND recipient=?").bind(id).bind(actor.id).fetch_optional(&mut *tx).await?.context("attention occurrence is not addressed to this agent")?;
        let plan = sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE id=?")
            .bind(row.get::<i64, _>("followup"))
            .fetch_one(&mut *tx)
            .await?;
        // Escalations go to the original sender, whose inbox cannot read the
        // recipient's delivery. Expose this one authorized source, preserving
        // its recipient's disposition and retrieval identity.
        let mail = if let Some(message) = plan.message {
            ensure!(
                plan.group_name == actor.group_name
                    && (actor.id == plan.recipient || actor.id == plan.authority),
                "source is not addressed to this agent"
            );
            let item = sqlx::query_as!(Message,
                "SELECT m.id,b.name AS sender,m.summary,m.body,m.created,m.deadline AS due,d.state AS 'state: MessageState',d.reply_id,m.work_id FROM messages m JOIN deliveries d ON d.message=m.id JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND d.recipient=?",
                message, plan.recipient).fetch_one(&mut *tx).await?;
            if actor.id == plan.recipient {
                Self::retrieve_tx(
                    &mut tx,
                    actor,
                    EventKind::MailPending,
                    &message.to_string(),
                    0,
                )
                .await?;
            }
            Some(item)
        } else {
            None
        };
        let active: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM active_attention WHERE id=?)")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        Self::retrieve_tx(
            &mut tx,
            actor,
            EventKind::AttentionDue,
            &id.to_string(),
            row.get("plan_version"),
        )
        .await?;
        tx.commit().await?;
        Ok(
            json!({"id":id,"current":active,"stage":row.get::<i64,_>("stage"),"reason":row.get::<String,_>("reason"),"followup":plan.value()?,"mail":mail,"instruction":"Read the included mail or fetch the current task. Act within its authority or record a checkpoint/blocker. Retrieval does not settle work."}),
        )
    }
    /// Record source retrieval for all runtimes, separately from transport receipts.
    pub(crate) async fn retrieve_followup_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        kind: EventKind,
        subject: &str,
        version: i64,
        time: i64,
    ) -> Result<()> {
        if kind == EventKind::AttentionDue {
            sqlx::query("UPDATE attention_occurrences SET retrieved_at=COALESCE(retrieved_at,?) WHERE CAST(id AS TEXT)=? AND recipient=? AND plan_version=?").bind(time).bind(subject).bind(actor.id).bind(version).execute(&mut **tx).await?;
        } else {
            let task = if kind == EventKind::WorkChanged {
                Some(subject)
            } else {
                None
            };
            let message = if kind == EventKind::MailPending {
                subject.parse::<i64>().ok()
            } else {
                None
            };
            sqlx::query("UPDATE followups SET retrieved_at=?,retrieved_binding=?,next_check=CASE WHEN checkpoint IS NULL THEN MIN(escalate_at,?+(SELECT interval_seconds FROM followup_policy WHERE group_name=followups.group_name)) ELSE next_check END WHERE recipient=? AND ((task=? AND task_version=?) OR message=?) AND (retrieved_at IS NULL OR retrieved_binding<>?)")
                .bind(time).bind(actor.binding_version).bind(time).bind(actor.id).bind(task).bind(version).bind(message).bind(actor.binding_version).execute(&mut **tx).await?;
        }
        Ok(())
    }
}

async fn validate_wait(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    plan: &Plan,
    wait: &WaitFor,
) -> Result<()> {
    match wait {
        WaitFor::External {
            responsible,
            reason,
        } => {
            for value in [responsible, reason] {
                bounded(value, 256, "external condition")?;
                ensure!(
                    !value.trim().is_empty(),
                    "external condition needs a responsible person/role and reason"
                );
            }
        }
        WaitFor::Mail { id } => {
            let valid: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages WHERE id=? AND sender=?)")
                    .bind(id)
                    .bind(actor.id)
                    .fetch_one(&mut **tx)
                    .await?;
            ensure!(
                valid && plan.message != Some(*id),
                "wait-mail must name your outgoing request, not this delivery"
            );
            let remote:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM deliveries d JOIN mailboxes b ON b.id=d.recipient WHERE d.message=? AND b.remote_machine IS NOT NULL)")
                .bind(id).fetch_one(&mut **tx).await?;
            ensure!(!remote, "cross-machine follow-up waits are unsupported");
        }
        WaitFor::Task { id, states } => {
            ensure!(
                !states.is_empty() && states.len() <= 8,
                "dependency needs explicit task states"
            );
            ensure!(
                plan.task.as_deref() != Some(id),
                "task cannot wait on itself"
            );
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=?)",
            )
            .bind(&actor.group_name)
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
            ensure!(
                exists,
                "dependency must be a locally authoritative task in this group"
            );
            let mut next = Some(id.clone());
            let mut visited = std::collections::HashSet::new();
            while let Some(id) = next {
                ensure!(
                    visited.insert(id.clone()) && plan.task.as_deref() != Some(&id),
                    "task dependency cycle"
                );
                let checkpoint: Option<String> = sqlx::query_scalar(
                    "SELECT checkpoint FROM active_followups WHERE group_name=? AND task=?",
                )
                .bind(&actor.group_name)
                .bind(id)
                .fetch_optional(&mut **tx)
                .await?
                .flatten();
                next = match checkpoint
                    .map(|s| serde_json::from_str::<Checkpoint>(&s))
                    .transpose()?
                    .and_then(|r| r.waiting)
                {
                    Some(WaitFor::Task { id, .. }) => Some(id),
                    _ => None,
                };
                ensure!(
                    visited.len() <= 1000,
                    "dependency chain exceeds supported bound"
                );
            }
        }
    }
    Ok(())
}
async fn waiting_satisfied(tx: &mut Transaction<'_, Sqlite>, plan: &Plan) -> Result<bool> {
    match plan.report()?.and_then(|r|r.waiting) {
        Some(WaitFor::Task{id,states})=>{
            let state:Option<String>=sqlx::query_scalar("SELECT state FROM work_items WHERE group_name=? AND id=?").bind(&plan.group_name).bind(id).fetch_optional(&mut **tx).await?;
            Ok(state.is_some_and(|s|states.iter().any(|state|state.as_str()==s)))
        },
        Some(WaitFor::Mail{id})=>Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages WHERE reply_to=?) OR NOT EXISTS(SELECT 1 FROM deliveries WHERE message=? AND state='pending')").bind(id).bind(id).fetch_one(&mut **tx).await?),
        _=>Ok(false),
    }
}

async fn publish_dependency_ready(
    tx: &mut Transaction<'_, Sqlite>,
    plan: &Plan,
    time: i64,
) -> Result<()> {
    let result=sqlx::query("INSERT OR IGNORE INTO attention_occurrences(followup,plan_version,stage,reason,recipient,created) VALUES(?,?,1,'dependency_ready',?,?)")
        .bind(plan.id).bind(plan.version).bind(plan.recipient).bind(time).execute(&mut **tx).await?;
    if result.rows_affected() == 1 {
        sqlx::query("INSERT INTO coordination_events(recipient,kind,subject,version,created) VALUES(?,'attention_due',?,?,?)")
            .bind(plan.recipient).bind(result.last_insert_rowid().to_string()).bind(plan.version).bind(time).execute(&mut **tx).await?;
    }
    Ok(())
}

/// A successful runtime turn ended with a specific offered plan still outstanding.
/// Version/stage guards coalesce duplicate receipts and overlapping hook/queue offers.
pub(crate) async fn turn_completed(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    id: i64,
    version: i64,
    offered_stage: i64,
    time: i64,
) -> Result<()> {
    let Some(p) = sqlx::query_as::<_, Plan>("SELECT f.* FROM active_followups f JOIN followup_policy p ON p.group_name=f.group_name JOIN groups g ON g.name=f.group_name WHERE f.id=? AND f.version=? AND f.stage=? AND p.mode='enabled' AND g.paused=0")
        .bind(id).bind(version).bind(offered_stage).fetch_optional(&mut **tx).await? else {return Ok(());};
    let report = p.report()?;
    let satisfied = waiting_satisfied(tx, &p).await?;
    if time < p.escalate_at && report.as_ref().is_some_and(|r| r.next_check_at > time) && !satisfied
    {
        return Ok(());
    }
    // Approval and review holds do not authorize implementation or corrective wakes.
    let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=? AND state IN ('blocked','review'))")
        .bind(&p.group_name).bind(&p.task).fetch_one(&mut **tx).await?;
    if p.stage == 3 {
        if actor.id == p.authority {
            sqlx::query("UPDATE attention_occurrences SET operator_after=MIN(operator_after,?) WHERE followup=? AND plan_version=? AND stage=3")
                .bind(time).bind(p.id).bind(p.version).execute(&mut **tx).await?;
        }
        return Ok(());
    }
    if actor.id != p.recipient {
        return Ok(());
    }
    if authority_review_due(&p, report.as_ref(), satisfied, held, time) {
        return advance_attention(tx, &p, 3, time).await;
    }
    if held {
        return Ok(());
    }
    let stage = if p.stage == 0 { 1 } else { 3 };
    advance_attention(tx, &p, stage, time).await
}

fn authority_review_due(
    p: &Plan,
    report: Option<&Checkpoint>,
    satisfied: bool,
    held: bool,
    time: i64,
) -> bool {
    time >= p.escalate_at
        || (time >= p.next_check
            && ((report.is_some_and(|r| r.waiting.is_some()) && !satisfied)
                || (held && report.is_none())))
}

async fn advance_attention(
    tx: &mut Transaction<'_, Sqlite>,
    p: &Plan,
    stage: i64,
    time: i64,
) -> Result<()> {
    let recipient = if stage == 3 { p.authority } else { p.recipient };
    let operator_after = (stage == 3).then_some(if recipient == p.recipient {
        time
    } else {
        time.saturating_add(300)
    });
    let result = sqlx::query("INSERT OR IGNORE INTO attention_occurrences(followup,plan_version,stage,reason,recipient,created,operator_after) VALUES(?,?,?,?,?,?,?)")
        .bind(p.id).bind(p.version).bind(stage).bind(if stage==3 {"escalation"} else {"reminder"}).bind(recipient).bind(time).bind(operator_after).execute(&mut **tx).await?;
    if result.rows_affected() == 1 && (stage != 3 || recipient != p.recipient) {
        sqlx::query("INSERT INTO coordination_events(recipient,kind,subject,version,created) VALUES(?,'attention_due',?,?,?)")
            .bind(recipient).bind(result.last_insert_rowid().to_string()).bind(p.version).bind(time).execute(&mut **tx).await?;
    }
    // Preserve the hard boundary and the separate recovery interval.
    sqlx::query("UPDATE followups SET stage=?,next_check=MIN(escalate_at,?+(SELECT interval_seconds FROM followup_policy WHERE group_name=?)) WHERE id=?")
        .bind(stage).bind(time).bind(&p.group_name).bind(p.id).execute(&mut **tx).await?;
    Ok(())
}

/// Reconcile a fair bounded page; the five-second service scan recovers missed hints.
pub async fn reconcile(store: &Store, time: i64) -> Result<()> {
    let mut tx = store.pool().begin().await?;
    // Take SQLite's writer reservation before reading decisions.
    sqlx::query("UPDATE followup_policy SET updated=updated WHERE 0")
        .execute(&mut *tx)
        .await?;
    // Bounded repair also covers registrations that became local after assignment.
    sqlx::query("INSERT OR IGNORE INTO followups(group_name,message,recipient,authority,opened,next_check,escalate_at) SELECT b.group_name,d.message,b.id,m.sender,m.created,?+p.max_seconds,?+p.max_seconds FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN followup_policy p ON p.group_name=b.group_name WHERE d.state='pending' AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.message=d.message AND f.recipient=b.id) ORDER BY m.id,b.id LIMIT 100")
        .bind(time).bind(time).execute(&mut *tx).await?;
    sqlx::query("INSERT OR IGNORE INTO followups(group_name,task,task_version,recipient,authority,opened,next_check,escalate_at) SELECT w.group_name,w.id,w.version,b.id,a.id,w.updated,?+p.max_seconds,?+p.max_seconds FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer JOIN followup_policy p ON p.group_name=w.group_name WHERE w.open=1 AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.group_name=w.group_name AND f.task=w.id) ORDER BY w.group_name,w.id LIMIT 100")
        .bind(time).bind(time).execute(&mut *tx).await?;
    let plans = sqlx::query_as::<_, Plan>(
        "SELECT * FROM active_followups WHERE stage<3 OR (dependency_ready_at IS NULL AND json_extract(checkpoint,'$.waiting.kind') IN ('task','mail')) ORDER BY scanned,id LIMIT 100",
    )
    .fetch_all(&mut *tx)
    .await?;
    for p in plans {
        sqlx::query("UPDATE followups SET scanned=? WHERE id=?")
            .bind(time)
            .bind(p.id)
            .execute(&mut *tx)
            .await?;
        let policy=sqlx::query("SELECT p.mode,p.interval_seconds,g.paused FROM followup_policy p JOIN groups g ON g.name=p.group_name WHERE p.group_name=?").bind(&p.group_name).fetch_one(&mut *tx).await?;
        if policy.get::<String, _>("mode") != "enabled" || policy.get::<i64, _>("paused") != 0 {
            continue;
        }
        let binding: i64 = sqlx::query_scalar("SELECT binding_version FROM mailboxes WHERE id=?")
            .bind(p.recipient)
            .fetch_one(&mut *tx)
            .await?;
        let retrieved = p.retrieved_at.is_some() && p.retrieved_binding == Some(binding);
        let exhausted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient WHERE e.recipient=? AND ((e.kind='mail_pending' AND e.subject=CAST(? AS TEXT)) OR (e.kind='work_changed' AND e.subject=? AND e.version=?)) AND ((b.attempts>=3 AND b.wake_attempted>=e.id) OR EXISTS(SELECT 1 FROM runtime_wakes n WHERE n.recipient=b.id AND n.binding_version=b.binding_version AND n.attempts>=3 AND n.attempted>=e.id)))")
            .bind(p.recipient).bind(p.message).bind(&p.task).bind(p.task_version).fetch_one(&mut *tx).await?;
        let report = p.report()?;
        let unread = !retrieved && report.is_none();
        let waiting = report.as_ref().is_some_and(|r| r.waiting.is_some());
        let satisfied = waiting && waiting_satisfied(&mut tx, &p).await?;
        let newly_satisfied = satisfied && p.dependency_ready_at.is_none();
        if newly_satisfied {
            sqlx::query("UPDATE followups SET dependency_ready_at=? WHERE id=?")
                .bind(time)
                .bind(p.id)
                .execute(&mut *tx)
                .await?;
        }
        if p.stage == 3 {
            if newly_satisfied {
                // A hold may become ready after escalation. Wake its owner once while
                // retaining the authority's escalation and the original hard boundary.
                publish_dependency_ready(&mut tx, &p, time).await?;
            }
            continue;
        }
        let hard_due = time >= p.escalate_at;
        let due = time >= p.next_check || (satisfied && p.stage == 0);
        if !(hard_due || due || unread && exhausted) {
            continue;
        }
        if unread && !exhausted && !hard_due && !satisfied {
            continue;
        }
        let held:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=? AND state IN ('blocked','review'))").bind(&p.group_name).bind(&p.task).fetch_one(&mut *tx).await?;
        let stage = if authority_review_due(&p, report.as_ref(), satisfied, held, time)
            || (unread && exhausted)
            || p.stage >= 2
        {
            3
        } else {
            p.stage + 1
        };
        // Escalation is attention metadata, never a recursive mail obligation.
        advance_attention(&mut tx, &p, stage, time).await?;
        if stage == 3 && newly_satisfied {
            publish_dependency_ready(&mut tx, &p, time).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

impl Store {
    /// Bounded operator view. This never records retrieval or extends deadlines.
    pub async fn followup_status(&self, group: Option<&str>, time: i64) -> Result<Value> {
        self.followup_status_for(group, time, None).await
    }

    pub(crate) async fn followup_status_for(
        &self,
        group: Option<&str>,
        time: i64,
        actor: Option<i64>,
    ) -> Result<Value> {
        let totals=sqlx::query("SELECT COUNT(*) AS pending,COALESCE(SUM(next_check<=?),0) AS due,COALESCE(SUM(stage=3),0) AS escalated FROM active_followups WHERE (? IS NULL OR group_name=?) AND (? IS NULL OR recipient=? OR authority=?)")
            .bind(time).bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_one(self.pool()).await?;
        let rows=sqlx::query("SELECT f.*,b.name AS owner,b.binding_version AS current_binding,a.name AS decision_owner,p.mode FROM active_followups f JOIN mailboxes b ON b.id=f.recipient JOIN mailboxes a ON a.id=f.authority JOIN followup_policy p ON p.group_name=f.group_name WHERE (? IS NULL OR f.group_name=?) AND (? IS NULL OR f.recipient=? OR f.authority=?) ORDER BY f.escalate_at,f.id LIMIT 101").bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let more = rows.len() > 100;
        let mut items = Vec::new();
        for r in rows.iter().take(100) {
            let checkpoint = r
                .get::<Option<String>, _>("checkpoint")
                .map(|s| serde_json::from_str::<Checkpoint>(&s))
                .transpose()?;
            items.push(json!({"id":r.get::<i64,_>("id"),"group":r.get::<String,_>("group_name"),"task":r.get::<Option<String>,_>("task"),"task_version":r.get::<i64,_>("task_version"),"message":r.get::<Option<i64>,_>("message"),"version":r.get::<i64,_>("version"),"owner":r.get::<String,_>("owner"),"decision_owner":r.get::<String,_>("decision_owner"),"retrieved_at":r.get::<Option<i64>,_>("retrieved_at"),"retrieval_current":r.get::<Option<i64>,_>("retrieved_binding")==Some(r.get::<i64,_>("current_binding")),"checkpoint":checkpoint,"next_check_at":r.get::<i64,_>("next_check"),"escalate_at":r.get::<i64,_>("escalate_at"),"escalated":r.get::<i64,_>("stage")==3,"due":r.get::<i64,_>("next_check")<=time,"mode":r.get::<String,_>("mode")}));
        }
        let notifications=sqlx::query("SELECT l.occurrence AS id,f.group_name,l.state AS operator_state,l.detail AS operator_detail,l.attempts AS operator_attempts,l.next_at AS operator_next FROM operator_notice_legacy l JOIN attention_occurrences o ON o.id=l.occurrence JOIN followups f ON f.id=o.followup WHERE (? IS NULL OR f.group_name=?) AND (? IS NULL OR f.recipient=? OR f.authority=?) ORDER BY l.occurrence DESC LIMIT 101").bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let legacy_alerts:Vec<Value>=notifications.iter().take(100).map(|r|json!({"id":r.get::<i64,_>("id"),"group":r.get::<String,_>("group_name"),"state":r.get::<String,_>("operator_state"),"detail":r.get::<Option<String>,_>("operator_detail"),"attempts":r.get::<i64,_>("operator_attempts"),"next_attempt":r.get::<i64,_>("operator_next")})).collect();
        let mut notice_tx = self.pool().begin().await?;
        let (alerts, notices_more) =
            crate::operator_notices::status_notices_tx(&mut notice_tx, group, actor, time).await?;
        notice_tx.commit().await?;
        let turns = sqlx::query("SELECT o.id,b.name,o.runtime,o.session,o.turn,o.state,o.created,o.completed_at,(SELECT COUNT(*) FROM turn_offer_items i WHERE i.offer=o.id) AS records FROM turn_offers o JOIN mailboxes b ON b.id=o.recipient WHERE (? IS NULL OR b.group_name=?) AND (? IS NULL OR o.recipient=?) AND o.binding_version=b.binding_version ORDER BY o.created DESC,o.id LIMIT 51")
            .bind(group).bind(group).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let turn_receipts: Vec<Value> = turns.iter().take(50).map(|r| json!({"offer":r.get::<String,_>("id"),"agent":r.get::<String,_>("name"),"runtime":r.get::<String,_>("runtime"),"session":r.get::<String,_>("session"),"turn":r.get::<Option<String>,_>("turn"),"state":r.get::<String,_>("state"),"records":r.get::<i64,_>("records"),"created":r.get::<i64,_>("created"),"completed_at":r.get::<Option<i64>,_>("completed_at")})).collect();
        Ok(
            json!({"totals":{"pending":totals.get::<i64,_>("pending"),"due":totals.get::<i64,_>("due"),"escalated":totals.get::<i64,_>("escalated")},"items":items,"more":more || notices_more || notifications.len()>100,"operator_notifications":alerts,"legacy_operator_notification_history":legacy_alerts,"legacy_operator_notification_history_note":"Earlier notification records; these do not confirm acceptance by the current route","turn_receipts":turn_receipts,"turn_receipts_more":turns.len()>50,"remote_followup":"unsupported"}),
        )
    }
}

/// Deliver bounded escalation summaries outside the coordinator's wake path.
pub async fn notify_operators(store: &Store, time: i64) -> Result<()> {
    crate::operator_notices::dispatch_notices(store, time).await
}

#[cfg(test)]
mod correction_tests {
    use super::*;
    use crate::store::Publish;

    async fn fixture() -> Result<(tempfile::TempDir, Store, Mailbox, i64)> {
        const {
            assert!(cfg!(debug_assertions));
        }
        let root = std::env::var("AGENT_MAIL_STATE_DIR")?;
        assert_eq!(root, "/tmp/agent-mail-durable-execution/decision-recovery");
        let dir = tempfile::Builder::new()
            .prefix("plan-control-")
            .tempdir_in(root)?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        for name in ["sender", "one", "two"] {
            store.register("g", name, false).await?;
        }
        let sender = store.mailbox("g", "sender").await?;
        Ok((dir, store, sender, 1_700_000_000))
    }
    fn report(now: i64) -> Checkpoint {
        Checkpoint {
            version: 0,
            next_step: "Review source evidence".into(),
            next_check_at: now + 4001,
            waiting: None,
            evidence: vec!["control:source".into()],
            extend_until: Some(now + 5000),
            reason: Some("Explicit source correction".into()),
        }
    }
    async fn message(
        store: &Store,
        sender: &Mailbox,
        recipients: Vec<String>,
        now: i64,
    ) -> Result<i64> {
        store
            .publish(
                sender,
                Publish {
                    recipients,
                    key: "m".into(),
                    summary: "Review".into(),
                    body: "Evidence".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                now,
            )
            .await
    }
    #[tokio::test]
    async fn missing_task_plan_repair_checks_actual_writer_and_original_opening() -> Result<()> {
        let (_dir, store, sender, now) = fixture().await?;
        store
            .work_create(
                &sender,
                crate::work::WorkDraft {
                    id: "repair-task".into(),
                    scope: "Review".into(),
                    owner: "one".into(),
                    state: TaskState::Active,
                    next_action: "Review".into(),
                    deadline: Some(now + 3600),
                    evidence: vec![],
                },
                now,
            )
            .await?;
        sqlx::query("DELETE FROM followups WHERE task='repair-task'")
            .execute(store.pool())
            .await?;
        let one = store.mailbox("g", "one").await?;
        let target = CorrectionSource::Task {
            id: "repair-task".into(),
            version: 1,
        };
        let mut tx = store.pool().begin().await?;
        assert!(
            correct_obligation_plan_tx(&mut tx, &one, &target, None, &report(now), now + 4000)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = store.pool().begin().await?;
        let receipt =
            correct_obligation_plan_tx(&mut tx, &sender, &target, None, &report(now), now + 4000)
                .await?;
        assert_eq!(receipt["before"], Value::Null);
        assert_eq!(receipt["after"]["opened"], now);
        assert_eq!(receipt["after"]["retrieved_at"], Value::Null);
        assert_eq!(receipt["source"]["business_deadline"], now + 3600);
        tx.commit().await?;
        let work = store.work_show(&sender, "repair-task").await?;
        assert_eq!(work.version, 1);
        assert_eq!(work.deadline, Some(now + 3600));
        Ok(())
    }
    #[tokio::test]
    async fn explicit_plan_correction_isolates_recipient_and_guards_authority_cas() -> Result<()> {
        let (_dir, store, sender, now) = fixture().await?;
        let id = message(&store, &sender, vec!["one".into(), "two".into()], now).await?;
        let one = store.mailbox("g", "one").await?;
        let plan =
            sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE message=? AND recipient=?")
                .bind(id)
                .bind(one.id)
                .fetch_one(store.pool())
                .await?;
        let other_before: String=sqlx::query_scalar("SELECT json_object('version',version,'next_check',next_check,'escalate_at',escalate_at) FROM followups WHERE message=? AND recipient<>?").bind(id).bind(one.id).fetch_one(store.pool()).await?;
        let target = CorrectionSource::Delivery {
            message: id,
            recipient: one.id,
        };
        let mut tx = store.pool().begin().await?;
        assert!(
            correct_obligation_plan_tx(
                &mut tx,
                &one,
                &target,
                Some((plan.id, plan.version)),
                &report(now),
                now + 4000
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        let mut tx = store.pool().begin().await?;
        assert!(
            correct_obligation_plan_tx(
                &mut tx,
                &sender,
                &target,
                Some((plan.id, plan.version + 1)),
                &report(now),
                now + 4000
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        let mut tx = store.pool().begin().await?;
        let receipt = correct_obligation_plan_tx(
            &mut tx,
            &sender,
            &target,
            Some((plan.id, plan.version)),
            &report(now),
            now + 4000,
        )
        .await?;
        assert_eq!(receipt["before"]["opened"], receipt["after"]["opened"]);
        assert_eq!(receipt["after"]["version"], plan.version + 1);
        assert_eq!(receipt["after"]["retrieved_at"], Value::Null);
        tx.commit().await?;
        let mut shorter = report(now);
        shorter.version = 1;
        shorter.extend_until = Some(now + 4100);
        let mut tx = store.pool().begin().await?;
        let changed = correct_obligation_plan_tx(
            &mut tx,
            &sender,
            &target,
            Some((plan.id, plan.version + 1)),
            &shorter,
            now + 4000,
        )
        .await?;
        assert_eq!(changed["after"]["escalate_at"], now + 4100);
        tx.commit().await?;
        let other_after: String=sqlx::query_scalar("SELECT json_object('version',version,'next_check',next_check,'escalate_at',escalate_at) FROM followups WHERE message=? AND recipient<>?").bind(id).bind(one.id).fetch_one(store.pool()).await?;
        assert_eq!(other_before, other_after);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM deliveries WHERE message=? AND state='pending'"
            )
            .bind(id)
            .fetch_one(store.pool())
            .await?,
            2
        );
        Ok(())
    }
    #[tokio::test]
    async fn sender_convenience_counts_settled_originals_and_preserves_self_precedence()
    -> Result<()> {
        let (_dir, store, sender, now) = fixture().await?;
        let id = message(&store, &sender, vec!["one".into(), "two".into()], now).await?;
        let one = store.mailbox("g", "one").await?;
        sqlx::query("UPDATE deliveries SET state='resolved' WHERE message=? AND recipient=?")
            .bind(id)
            .bind(one.id)
            .execute(store.pool())
            .await?;
        assert!(
            store
                .checkpoint(
                    &sender,
                    Source::Mail { id },
                    "ambiguous",
                    report(now),
                    now + 4000
                )
                .await
                .is_err()
        );
        let (_self_dir, self_store, self_sender, now) = fixture().await?;
        let self_id = message(
            &self_store,
            &self_sender,
            vec!["sender".into(), "two".into()],
            now,
        )
        .await?;
        let result = self_store
            .checkpoint(
                &self_sender,
                Source::Mail { id: self_id },
                "self",
                report(now),
                now + 4000,
            )
            .await?;
        assert_eq!(result["recipient"], self_sender.id);
        sqlx::query("UPDATE deliveries SET state='resolved' WHERE message=? AND recipient=?")
            .bind(self_id)
            .bind(self_sender.id)
            .execute(self_store.pool())
            .await?;
        assert!(
            self_store
                .checkpoint(
                    &self_sender,
                    Source::Mail { id: self_id },
                    "settled-self",
                    report(now),
                    now + 4000
                )
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM followups WHERE message=? AND recipient<>?"
            )
            .bind(self_id)
            .bind(self_sender.id)
            .fetch_one(self_store.pool())
            .await?,
            0
        );
        Ok(())
    }
}

#[cfg(test)]
mod checkpoint_public_controls {
    use super::*;
    use crate::work::WorkDraft;
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock},
        time::Duration,
    };
    use tokio::sync::oneshot;

    type GateKey = (PathBuf, i64, String);
    struct HintGate {
        committed: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }
    fn gates() -> &'static Mutex<HashMap<GateKey, HintGate>> {
        static GATES: OnceLock<Mutex<HashMap<GateKey, HintGate>>> = OnceLock::new();
        GATES.get_or_init(|| Mutex::new(HashMap::new()))
    }
    struct GateGuard(GateKey);
    impl Drop for GateGuard {
        fn drop(&mut self) {
            gates()
                .lock()
                .expect("checkpoint test gate lock")
                .remove(&self.0);
        }
    }
    fn gate(
        root: &Path,
        actor: &Mailbox,
        key: &str,
    ) -> (GateGuard, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let identity = (root.to_owned(), actor.id, key.to_owned());
        let (committed, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let old = gates().lock().expect("checkpoint test gate lock").insert(
            identity.clone(),
            HintGate {
                committed,
                release: released,
            },
        );
        assert!(old.is_none(), "duplicate checkpoint test gate");
        (GateGuard(identity), observed, release)
    }
    pub(super) async fn before_hint(root: &Path, actor: &Mailbox, key: &str) {
        let gate = {
            gates().lock().expect("checkpoint test gate lock").remove(&(
                root.to_owned(),
                actor.id,
                key.to_owned(),
            ))
        };
        // No mutex guard or SQLite transaction survives across this barrier.
        if let Some(gate) = gate {
            let _ = gate.committed.send(());
            let _ = gate.release.await;
        }
    }

    pub(super) struct Fixture {
        pub(super) dir: tempfile::TempDir,
        pub(super) store: Store,
        pub(super) writer: Mailbox,
        pub(super) owner: Mailbox,
        pub(super) other: Mailbox,
        pub(super) time: i64,
    }
    impl Fixture {
        pub(super) async fn new() -> Result<Self> {
            const {
                assert!(cfg!(debug_assertions));
            }
            let dir = tempfile::Builder::new()
                .prefix("am-checkpoint-core-")
                .tempdir_in("/tmp")?;
            let store = Store::open(dir.path(), true).await?;
            store.enroll("g", None).await?;
            for name in ["writer", "owner", "other"] {
                store.register("g", name, false).await?;
            }
            let writer = store.mailbox("g", "writer").await?;
            let owner = store.mailbox("g", "owner").await?;
            let other = store.mailbox("g", "other").await?;
            let time = crate::now()?;
            store
                .configure_followups(
                    "g",
                    &Policy {
                        mode: Mode::Enabled,
                        interval_seconds: 60,
                        max_seconds: 240,
                        notifier: None,
                    },
                    time,
                )
                .await?;
            store.close().await;
            let store = Store::open(dir.path(), false).await?;
            let result = Self {
                dir,
                store,
                writer,
                owner,
                other,
                time,
            };
            result.task("work").await?;
            Ok(result)
        }
        pub(super) async fn task(&self, id: &str) -> Result<()> {
            self.store
                .work_create(
                    &self.writer,
                    WorkDraft {
                        id: id.into(),
                        scope: "Review actual evidence".into(),
                        owner: "owner".into(),
                        state: TaskState::Active,
                        next_action: "Review actual evidence".into(),
                        deadline: None,
                        evidence: vec![],
                    },
                    self.time,
                )
                .await?;
            Ok(())
        }
        pub(super) fn source(&self) -> Source {
            Source::Task {
                id: "work".into(),
                version: 1,
            }
        }
        pub(super) fn report(&self) -> Checkpoint {
            Checkpoint {
                version: 0,
                next_step: "Review remaining evidence".into(),
                next_check_at: self.time + 90,
                waiting: None,
                evidence: vec![],
                extend_until: None,
                reason: None,
            }
        }
        pub(super) async fn history_count(&self) -> Result<i64> {
            Ok(sqlx::query_scalar("SELECT COUNT(*) FROM followup_history")
                .fetch_one(self.store.pool())
                .await?)
        }
    }

    #[tokio::test]
    async fn public_checkpoint_survives_actual_commit_before_hint_loss_and_replays_without_hint()
    -> Result<()> {
        let f = Fixture::new().await?;
        let (_guard, committed, _release) = gate(f.store.root(), &f.owner, "hint-loss");
        let store = f.store.clone();
        let owner = f.owner.clone();
        let source = f.source();
        let report = f.report();
        let time = f.time;
        let operation = tokio::spawn(async move {
            store
                .checkpoint(&owner, source, "hint-loss", report, time)
                .await
        });
        let observed = tokio::time::timeout(Duration::from_secs(3), committed).await;
        // Always join the public operation, including a failed barrier wait.
        operation.abort();
        let ended = operation.await;
        observed.context("actual checkpoint commit barrier timed out")??;
        assert!(ended.is_err_and(|error| error.is_cancelled()));
        let retry_source = f.source();
        let retry_report = f.report();
        f.store.close().await;
        let store = Store::open(f.dir.path(), false).await?;
        let original = store.source_followup(&f.owner, Some("work"), None).await?;
        assert_eq!(original["version"], 1);
        assert_eq!(original["checkpoint"]["next_check_at"], f.time + 90);
        let history = store
            .checkpoint_history(&f.owner, Some("work"), None)
            .await?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["checkpoint"], original);
        assert_eq!(history[0]["created"], f.time);
        let (_retry_guard, mut hint_seen, _retry_release) =
            gate(store.root(), &f.owner, "hint-loss");
        let replay = tokio::time::timeout(
            Duration::from_secs(3),
            store.checkpoint(
                &f.owner,
                retry_source,
                "hint-loss",
                retry_report,
                f.time + 5000,
            ),
        )
        .await
        .context("exact retry tried to wait for a stream hint")??;
        assert_eq!(replay, original);
        assert!(matches!(
            hint_seen.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            store
                .checkpoint_history(&f.owner, Some("work"), None)
                .await?
                .len(),
            1
        );
        assert_eq!(store.work_show(&f.owner, "work").await?.version, 1);
        Ok(())
    }
}

#[cfg(test)]
mod checkpoint_transaction_tests {
    use super::checkpoint_public_controls::Fixture;
    use super::*;
    use crate::work::{WorkPatch, WorkUpdate};

    impl Fixture {
        async fn basis(&self) -> Result<CheckpointTaskBasis> {
            let mut tx = self.store.pool().begin().await?;
            let basis = checkpoint_task_basis_tx(&mut tx, &self.owner, "work").await?;
            tx.commit().await?;
            Ok(basis)
        }
    }

    #[tokio::test]
    async fn checkpoint_write_and_history_proof_obey_outer_rollback() -> Result<()> {
        let f = Fixture::new().await?;
        let original = f.basis().await?;
        let mut tx = f.store.pool().begin().await?;
        let write = checkpoint_tx(
            &mut tx,
            &f.owner,
            f.source(),
            "rollback",
            f.report(),
            f.time,
        )
        .await?;
        let proof = validate_checkpoint_write_tx(&mut tx, &f.owner, &write).await?;
        assert_eq!(proof.history_id(), write.history_id());
        assert_eq!(proof.actor(), f.owner.id);
        assert_eq!(proof.followup(), original.followup);
        assert_eq!(proof.version(), 1);
        assert_eq!(proof.recorded_at(), f.time);
        assert_eq!(proof.opened(), original.opened);
        assert_eq!(proof.escalate_at(), original.escalate_at);
        assert_eq!(proof.next_check_at(), f.time + 90);
        assert_eq!(proof.checkpoint(), &f.report());
        assert!(matches!(proof.source(), Source::Task { id, version: 1 } if id == "work"));
        assert_eq!(write.snapshot()["version"], 1);
        // A real subsequent refusal is in the same outer transaction.
        let mut conflicting = f.report();
        conflicting.next_step = "Different request".into();
        assert!(
            checkpoint_tx(
                &mut tx,
                &f.owner,
                f.source(),
                "rollback",
                conflicting,
                f.time
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(f.basis().await?, original);
        assert_eq!(f.history_count().await?, 0);
        let mut tx = f.store.pool().begin().await?;
        assert!(
            validate_checkpoint_write_tx(&mut tx, &f.owner, &write)
                .await
                .is_err()
        );
        tx.rollback().await?;
        assert_eq!(f.store.work_show(&f.owner, "work").await?.version, 1);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_exact_replay_keeps_original_time_after_task_change_and_expiry() -> Result<()>
    {
        let f = Fixture::new().await?;
        let original = f
            .store
            .checkpoint(&f.owner, f.source(), "original", f.report(), f.time)
            .await?;
        f.store
            .update_work(
                &f.writer,
                "work",
                WorkUpdate {
                    version: 1,
                    reason: "Actual writer changes the next action".into(),
                    patch: WorkPatch {
                        next_action: Some("Review updated inputs".into()),
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                f.time + 1,
            )
            .await?;
        let after_change = f.basis().await?;
        assert_eq!(after_change.task_version, 2);
        let mut tx = f.store.pool().begin().await?;
        let replay = checkpoint_tx(
            &mut tx,
            &f.owner,
            f.source(),
            "original",
            f.report(),
            f.time + 5000,
        )
        .await?;
        assert!(replay.is_replay());
        assert_eq!(replay.snapshot(), &original);
        assert_eq!(replay.recorded_at(), f.time);
        let proof = validate_checkpoint_write_tx(&mut tx, &f.owner, &replay).await?;
        assert_eq!(proof.recorded_at(), f.time);
        assert_eq!(proof.next_check_at(), f.time + 90);
        tx.commit().await?;
        assert_eq!(f.basis().await?, after_change);
        assert_eq!(f.history_count().await?, 1);
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_tx(
                &mut tx,
                &f.owner,
                f.source(),
                "fresh-stale",
                f.report(),
                f.time + 2
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let mut changed = f.report();
        changed.next_step = "Changed retry".into();
        assert!(
            checkpoint_tx(
                &mut tx,
                &f.owner,
                f.source(),
                "original",
                changed,
                f.time + 2
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(f.history_count().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_basis_and_write_refuse_wrong_actor_stale_plan_and_real_rebind() -> Result<()>
    {
        let f = Fixture::new().await?;
        let original = f.basis().await?;
        let decoded: CheckpointTaskBasis =
            serde_json::from_value(serde_json::to_value(&original)?)?;
        assert_eq!(decoded, original);
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_task_basis_tx(&mut tx, &f.other, "work")
                .await
                .is_err()
        );
        assert!(
            checkpoint_tx(&mut tx, &f.other, f.source(), "foreign", f.report(), f.time)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let first =
            checkpoint_tx(&mut tx, &f.owner, f.source(), "first", f.report(), f.time).await?;
        tx.commit().await?;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_tx(
                &mut tx,
                &f.owner,
                f.source(),
                "stale-plan",
                f.report(),
                f.time
            )
            .await
            .is_err()
        );
        assert!(
            validate_checkpoint_write_tx(&mut tx, &f.other, &first)
                .await
                .is_err()
        );
        tx.rollback().await?;
        // Rotate through the real registration API; never construct an actor.
        f.store.register("g", "owner", true).await?;
        let current = f.store.mailbox("g", "owner").await?;
        assert_ne!(current.binding_version, f.owner.binding_version);
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_task_basis_tx(&mut tx, &f.owner, "work")
                .await
                .is_err()
        );
        assert!(
            checkpoint_tx(&mut tx, &f.owner, f.source(), "first", f.report(), f.time)
                .await
                .is_err()
        );
        assert!(
            validate_checkpoint_write_tx(&mut tx, &f.owner, &first)
                .await
                .is_err()
        );
        tx.rollback().await?;
        let mut tx = f.store.pool().begin().await?;
        let new_basis = checkpoint_task_basis_tx(&mut tx, &current, "work").await?;
        assert_ne!(new_basis, original);
        assert_eq!(new_basis.opened, original.opened);
        assert_eq!(new_basis.escalate_at, original.escalate_at);
        tx.commit().await?;
        assert_eq!(f.history_count().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_history_retains_wait_recheck_and_identical_report_semantics() -> Result<()>
    {
        let f = Fixture::new().await?;
        f.task("dependency").await?;
        let mut report = f.report();
        report.waiting = Some(WaitFor::Task {
            id: "dependency".into(),
            states: vec![TaskState::Active],
        });
        let mut tx = f.store.pool().begin().await?;
        let first = checkpoint_tx(
            &mut tx,
            &f.owner,
            f.source(),
            "wait",
            report.clone(),
            f.time,
        )
        .await?;
        let proof = validate_checkpoint_write_tx(&mut tx, &f.owner, &first).await?;
        assert_eq!(proof.next_check_at(), f.time);
        assert_eq!(proof.checkpoint().next_check_at, f.time + 90);
        tx.commit().await?;
        let original = f.basis().await?;
        report.version = original.version;
        let mut tx = f.store.pool().begin().await?;
        let unchanged = checkpoint_tx(
            &mut tx,
            &f.owner,
            f.source(),
            "same-content",
            report,
            f.time + 1,
        )
        .await?;
        let proof = validate_checkpoint_write_tx(&mut tx, &f.owner, &unchanged).await?;
        assert_eq!(proof.version(), original.version);
        assert_eq!(proof.next_check_at(), f.time);
        assert_eq!(unchanged.snapshot(), first.snapshot());
        tx.commit().await?;
        assert_eq!(f.basis().await?, original);
        assert_eq!(f.history_count().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_extension_still_requires_real_writer_and_a_future_bound() -> Result<()> {
        let f = Fixture::new().await?;
        let original = f.basis().await?;
        let mut report = f.report();
        report.next_check_at = original.escalate_at + 10;
        report.extend_until = Some(original.escalate_at + 30);
        report.reason = Some("Writer grants a finite coordination extension".into());
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_tx(
                &mut tx,
                &f.owner,
                f.source(),
                "unauthorized-extension",
                report.clone(),
                f.time
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        assert_eq!(f.basis().await?, original);
        assert_eq!(f.history_count().await?, 0);
        let mut tx = f.store.pool().begin().await?;
        let write = checkpoint_tx(
            &mut tx,
            &f.writer,
            f.source(),
            "writer-extension",
            report,
            f.time,
        )
        .await?;
        let proof = validate_checkpoint_write_tx(&mut tx, &f.writer, &write).await?;
        assert_eq!(proof.escalate_at(), original.escalate_at + 30);
        assert_eq!(proof.opened(), original.opened);
        tx.commit().await?;
        let mut expired = f.report();
        expired.version = 1;
        expired.next_check_at = f.time;
        let mut tx = f.store.pool().begin().await?;
        assert!(
            checkpoint_tx(&mut tx, &f.writer, f.source(), "expired", expired, f.time)
                .await
                .is_err()
        );
        tx.rollback().await?;
        assert_eq!(f.history_count().await?, 1);
        assert_eq!(f.store.work_show(&f.owner, "work").await?.version, 1);
        Ok(())
    }
}
