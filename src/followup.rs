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
    /// A pending delivery in the caller's inbox.
    Mail {
        /// Message identifier.
        id: i64,
    },
}
/// Combination semantics for a persisted prerequisite set.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case")]
pub enum PrerequisiteMode {
    /// Every condition must qualify.
    All,
    /// At least one condition must qualify.
    Any,
}
/// Exact task state and optional revision prerequisite.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskPrerequisite {
    /// Same-group task identifier.
    pub id: String,
    /// Explicit qualifying states; cancellation counts only when selected.
    pub states: Vec<TaskState>,
    /// Require this exact accepted revision when provided.
    #[serde(default)]
    pub accepted_revision: Option<String>,
}
/// Condition to reassess; satisfaction never grants business authority.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum MailPredicate {
    /// A reply arrives or every recipient records a business disposition.
    #[default]
    FirstReplyOrAllSettled,
    /// At least one recipient published a final reply.
    FirstReply,
    /// At least one recipient recorded a business disposition.
    AnySettled,
    /// Every recipient recorded a business disposition.
    AllSettled,
}
impl MailPredicate {
    /// Omit the default from canonical checkpoint JSON.
    pub fn is_default(&self) -> bool {
        *self == Self::FirstReplyOrAllSettled
    }
    /// Evaluate only business outcomes, never transport receipts.
    pub fn satisfied(self, total: i64, pending: i64, replies: i64) -> bool {
        match self {
            Self::FirstReplyOrAllSettled => replies > 0 || (total > 0 && pending == 0),
            Self::FirstReply => replies > 0,
            Self::AnySettled => pending < total,
            Self::AllSettled => total > 0 && pending == 0,
        }
    }
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
    /// Persist multiple task prerequisites with explicit all/any semantics.
    Tasks {
        /// Combination semantics.
        mode: PrerequisiteMode,
        /// One to thirty-two same-group prerequisites.
        tasks: Vec<TaskPrerequisite>,
    },
    /// Reassess an outgoing request when a reply arrives or all deliveries settle.
    Mail {
        /// Outgoing message identifier.
        id: i64,
        /// Explicit qualifying outcome; omission means first reply or all settled.
        #[serde(default, skip_serializing_if = "MailPredicate::is_default")]
        predicate: MailPredicate,
    },
    /// Reassess only after GitHub confirms the merge of an exact expected head.
    PullRequest {
        /// GitHub owner/repository, observed through the authenticated gh adapter.
        repository: crate::external::GitHubRepository,
        /// Positive GitHub pull-request number.
        number: i64,
        /// Full expected head; another revision cannot satisfy this condition.
        head: crate::external::CommitId,
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

/// Authority responsible for a source's reminder and escalation schedule.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Schedule {
    /// This task or request has an independent schedule.
    Source,
    /// A retrieved request shares its linked task's current schedule.
    Task {
        /// Same-group task identifier.
        id: crate::names::TaskId,
        /// Current task revision governing the schedule.
        version: i64,
        /// Follow-up metadata identifier for that task.
        followup: i64,
    },
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
        value["schedule"] = serde_json::to_value(Schedule::Source)?;
        Ok(value)
    }
}

impl Store {
    async fn scheduled_plan_value(tx: &mut Transaction<'_, Sqlite>, plan: &Plan) -> Result<Value> {
        let mut value = plan.value()?;
        if plan.message.is_some() {
            let task = sqlx::query("SELECT f.id,f.task,f.task_version FROM task_request_schedules s JOIN followups f ON f.id=s.task_followup WHERE s.followup=?")
                .bind(plan.id).fetch_optional(&mut **tx).await?;
            if let Some(task) = task {
                value["schedule"] = serde_json::to_value(Schedule::Task {
                    id: task.get::<String, _>("task").parse()?,
                    version: task.get("task_version"),
                    followup: task.get("id"),
                })?;
            }
        }
        Ok(value)
    }

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
        sqlx::query("UPDATE followup_policy SET mode=?,interval_seconds=?,max_seconds=?,notifier=?,updated=? WHERE group_name=?")
            .bind(policy.mode.text()).bind(policy.interval_seconds).bind(policy.max_seconds).bind(&notifier).bind(time).bind(group).execute(&mut *tx).await?;
        if previous.get::<String, _>("mode") == "observe" && policy.mode == Mode::Enabled {
            // Explicit activation gives unplanned historical records a grace period.
            sqlx::query("UPDATE followups SET next_check=MAX(next_check,?),escalate_at=MAX(escalate_at,?) WHERE group_name=? AND version=0 AND stage=0")
                .bind(time.checked_add(policy.interval_seconds).context("clock overflow")?).bind(time.checked_add(policy.max_seconds).context("clock overflow")?).bind(group).execute(&mut *tx).await?;
        }
        if notifier.is_some() && previous.get::<Option<String>, _>("notifier") != notifier {
            // Explicit route repair rearms only failed operator alerts, never business delivery.
            sqlx::query("UPDATE attention_occurrences SET operator_attempts=0,operator_next=?,operator_state='pending',operator_detail='Operator notifier configuration changed' WHERE operator_state IN ('failed','unconfigured','uncertain') AND id IN (SELECT o.id FROM active_attention o JOIN followups f ON f.id=o.followup WHERE f.group_name=? AND o.stage=3)")
                .bind(time).bind(group).execute(&mut *tx).await?;
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
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if let Some(row) =
            sqlx::query("SELECT canonical,snapshot FROM followup_history WHERE actor=? AND key=?")
                .bind(actor.id)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?
        {
            ensure!(
                row.get::<String, _>("canonical") == canonical,
                "checkpoint key already used with different content"
            );
            let result = serde_json::from_str(&row.get::<String, _>("snapshot"))?;
            tx.commit().await?;
            return Ok(result);
        }
        let plan = match &source {
            Source::Attention{id}=>sqlx::query_as::<_,Plan>("SELECT f.* FROM active_followups f JOIN active_attention o ON o.followup=f.id WHERE o.id=? AND o.recipient=? AND (f.recipient=? OR f.authority=?)")
                .bind(id).bind(actor.id).bind(actor.id).bind(actor.id).fetch_optional(&mut *tx).await?.context("attention occurrence is stale or not addressed to this agent")?,
            Source::Task { id, version } => {
                let plan = sqlx::query_as::<_, Plan>(
                    "SELECT * FROM active_followups WHERE group_name=? AND task=?",
                )
                .bind(&actor.group_name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .context("no active local task; remote follow-up unsupported")?;
                ensure!(
                    plan.recipient == actor.id || plan.authority == actor.id,
                    "only the task owner or writer may checkpoint"
                );
                ensure!(
                    plan.task_version == *version,
                    "task revision changed; run task show {id} and reconsider (current revision {})",
                    plan.task_version
                );
                plan
            }
            Source::Mail { id } => sqlx::query_as::<_, Plan>(
                "SELECT * FROM active_followups WHERE message=? AND recipient=?",
            )
            .bind(id)
            .bind(actor.id)
            .fetch_optional(&mut *tx)
            .await?
            .context("message is not pending in this inbox")?,
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
            validate_wait(&mut tx, actor, &plan, wait).await?;
        }
        let mut normalized = report.clone();
        normalized.version = 0;
        let unchanged = plan.report()?.is_some_and(|mut old| {
            old.version = 0;
            old == normalized
        });
        if !unchanged {
            sqlx::query("UPDATE followups SET version=version+1,checkpoint=?,next_check=?,escalate_at=?,stage=0,scanned=0,dependency_ready_at=NULL WHERE id=? AND version=?")
                .bind(serde_json::to_string(&report)?).bind(report.next_check_at).bind(boundary).bind(plan.id).bind(plan.version).execute(&mut *tx).await?;
        }
        let current = sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE id=?")
            .bind(plan.id)
            .fetch_one(&mut *tx)
            .await?;
        // Recheck before committing the checkpoint and its exact retry response.
        if report.waiting.is_some() && waiting_satisfied(&mut tx, &current).await? {
            sqlx::query("UPDATE followups SET next_check=MIN(next_check,?) WHERE id=?")
                .bind(time)
                .bind(plan.id)
                .execute(&mut *tx)
                .await?;
        }
        let current = sqlx::query_as::<_, Plan>("SELECT * FROM followups WHERE id=?")
            .bind(plan.id)
            .fetch_one(&mut *tx)
            .await?;
        match &source {
            Source::Task { id, version } => {
                Self::retrieve_tx(&mut tx, actor, EventKind::WorkChanged, id, *version).await?
            }
            Source::Mail { id } => {
                Self::retrieve_tx(&mut tx, actor, EventKind::MailPending, &id.to_string(), 0)
                    .await?
            }
            Source::Attention { id } => {
                Self::retrieve_tx(
                    &mut tx,
                    actor,
                    EventKind::AttentionDue,
                    &id.to_string(),
                    plan.version,
                )
                .await?
            }
        }
        let result = current.value()?;
        sqlx::query("INSERT INTO followup_history(followup,version,actor,key,canonical,snapshot,created) VALUES(?,?,?,?,?,?,?)")
            .bind(plan.id).bind(current.version).bind(actor.id).bind(key).bind(canonical).bind(serde_json::to_string(&result)?).bind(time).execute(&mut *tx).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(result)
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
        let value = if let Some(plan) = row {
            Self::scheduled_plan_value(&mut tx, &plan).await?
        } else {
            json!({"supported":false,"reason":"no local follow-up visible; remote follow-up unsupported"})
        };
        tx.commit().await?;
        Ok(value)
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
                "SELECT m.intent AS 'intent: crate::states::MessageIntent',m.id,b.name AS sender,m.summary,m.body,m.created,m.deadline AS due,d.state AS 'state: MessageState',d.reply_id,m.work_id,m.context AS 'context!: crate::mail_context::MessageContext',m.reply_to,CASE WHEN m.parent_global_id IS NOT NULL THEN json_object('global_id',m.parent_global_id,'local_id',m.reply_to) END AS 'parent?: crate::mail_context::ParentMessage' FROM messages m JOIN deliveries d ON d.message=m.id JOIN mailboxes b ON b.id=m.sender WHERE m.id=? AND d.recipient=?",
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
        let followup = Self::scheduled_plan_value(&mut tx, &plan).await?;
        tx.commit().await?;
        Ok(
            json!({"id":id,"current":active,"stage":row.get::<i64,_>("stage"),"reason":row.get::<String,_>("reason"),"followup":followup,"mail":mail,"instruction":"Read the included mail or fetch the current task. Act within its authority or record a checkpoint/blocker. Retrieval does not settle work."}),
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
        WaitFor::PullRequest { number, .. } => {
            ensure!(*number > 0, "pull-request number must be positive")
        }
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
        WaitFor::Mail { id, .. } => {
            let valid: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages WHERE id=? AND sender=? AND intent='request')")
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
        WaitFor::Tasks { tasks, .. } => {
            ensure!(
                !tasks.is_empty() && tasks.len() <= 32,
                "prerequisite set must contain 1..32 tasks"
            );
            let mut ids = Vec::new();
            for task in tasks {
                crate::name(&task.id)?;
                ensure!(
                    !task.states.is_empty() && task.states.len() <= 8,
                    "prerequisite needs explicit states"
                );
                ensure!(!ids.contains(&task.id), "duplicate prerequisite task");
                if let Some(revision) = &task.accepted_revision {
                    bounded(revision, 128, "prerequisite revision")?;
                    ensure!(
                        !revision.trim().is_empty(),
                        "prerequisite revision required"
                    );
                }
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=?)",
                )
                .bind(&actor.group_name)
                .bind(&task.id)
                .fetch_one(&mut **tx)
                .await?;
                ensure!(exists, "prerequisite task missing in this group");
                ids.push(task.id.clone());
            }
            validate_dependency_graph(tx, &actor.group_name, plan.task.as_deref(), &ids).await?;
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
            validate_dependency_graph(
                tx,
                &actor.group_name,
                plan.task.as_deref(),
                std::slice::from_ref(id),
            )
            .await?;
        }
    }
    Ok(())
}
/// Validate the union of explicit dependency facts and persisted checkpoint waits.
pub(crate) async fn validate_dependency_graph(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    source: Option<&str>,
    targets: &[String],
) -> Result<()> {
    let Some(source) = source else { return Ok(()) };
    let mut pending = targets.to_vec();
    let mut visited = std::collections::HashSet::new();
    while let Some(id) = pending.pop() {
        ensure!(id != source, "task dependency cycle");
        if !visited.insert(id.clone()) {
            continue;
        }
        ensure!(
            visited.len() <= 1000,
            "dependency graph exceeds supported bound"
        );
        let checkpoint: Option<String> = sqlx::query_scalar(
            "SELECT checkpoint FROM active_followups WHERE group_name=? AND task=?",
        )
        .bind(group)
        .bind(&id)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        if let Some(wait) = checkpoint
            .map(|s| serde_json::from_str::<Checkpoint>(&s))
            .transpose()?
            .and_then(|c| c.waiting)
        {
            match wait {
                WaitFor::Task { id, .. } => pending.push(id),
                WaitFor::Tasks { tasks, .. } => pending.extend(tasks.into_iter().map(|t| t.id)),
                _ => {}
            }
        }
        let links: Vec<String> = sqlx::query_scalar(
            "SELECT target FROM task_dependency_edges WHERE group_name=? AND source=?",
        )
        .bind(group)
        .bind(id)
        .fetch_all(&mut **tx)
        .await?;
        pending.extend(links);
    }
    Ok(())
}
async fn waiting_satisfied(tx: &mut Transaction<'_, Sqlite>, plan: &Plan) -> Result<bool> {
    match plan.report()?.and_then(|r| r.waiting) {
        Some(WaitFor::Task { id, states }) => {
            let state: Option<String> =
                sqlx::query_scalar("SELECT state FROM work_items WHERE group_name=? AND id=?")
                    .bind(&plan.group_name)
                    .bind(id)
                    .fetch_optional(&mut **tx)
                    .await?;
            Ok(state.is_some_and(|s| states.iter().any(|state| state.as_str() == s)))
        }
        Some(WaitFor::Tasks { mode, tasks }) => {
            let mut qualified = 0;
            for task in &tasks {
                let row = sqlx::query(
                    "SELECT state,accepted_revision FROM work_items WHERE group_name=? AND id=?",
                )
                .bind(&plan.group_name)
                .bind(&task.id)
                .fetch_optional(&mut **tx)
                .await?;
                if let Some(row) = row {
                    let state: String = row.get("state");
                    let revision: Option<String> = row.get("accepted_revision");
                    if task.states.iter().any(|s| s.as_str() == state)
                        && task
                            .accepted_revision
                            .as_ref()
                            .is_none_or(|r| revision.as_ref() == Some(r))
                    {
                        qualified += 1;
                    }
                }
            }
            Ok(match mode {
                PrerequisiteMode::All => qualified == tasks.len(),
                PrerequisiteMode::Any => qualified > 0,
            })
        }
        Some(WaitFor::Mail { id, predicate }) => {
            let row=sqlx::query("SELECT COUNT(*) AS total,COALESCE(SUM(state='pending'),0) AS pending,COALESCE(SUM(reply_id IS NOT NULL),0) AS replies FROM deliveries WHERE message=?").bind(id).fetch_one(&mut **tx).await?;
            Ok(predicate.satisfied(row.get("total"), row.get("pending"), row.get("replies")))
        }
        Some(WaitFor::PullRequest { repository,number,head }) => {
            Ok(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM external_pr_facts WHERE repository=? AND number=? AND state='merged' AND head=? AND merge_commit IS NOT NULL AND error IS NULL)")
                .bind(repository.as_str()).bind(number).bind(head.as_str()).fetch_one(&mut **tx).await?)
        }
        _ => Ok(false),
    }
}

pub(crate) async fn occurrence_current(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    subject: &str,
) -> Result<bool> {
    let plan=sqlx::query_as::<_,Plan>("SELECT f.* FROM active_followups f JOIN active_attention o ON o.followup=f.id WHERE CAST(o.id AS TEXT)=? AND o.recipient=?")
        .bind(subject).bind(actor.id).fetch_optional(&mut **tx).await?;
    let Some(plan) = plan else {
        return Ok(false);
    };
    // Escalation remains due even while dependencies block the owner's work.
    let escalation: bool =
        sqlx::query_scalar("SELECT stage=3 FROM attention_occurrences WHERE CAST(id AS TEXT)=?")
            .bind(subject)
            .fetch_one(&mut **tx)
            .await?;
    if escalation {
        return Ok(true);
    }
    if crate::task_graph::readiness_tx(tx, &plan.group_name, plan.task.as_deref())
        .await?
        .is_some_and(|r| !r.ready)
    {
        return Ok(false);
    }
    if plan
        .report()?
        .is_some_and(|report| report.waiting.is_some())
    {
        waiting_satisfied(tx, &plan).await
    } else {
        Ok(true)
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

async fn advance_attention(
    tx: &mut Transaction<'_, Sqlite>,
    p: &Plan,
    stage: i64,
    time: i64,
    dependency_ready: bool,
) -> Result<()> {
    let recipient = if stage == 3 { p.authority } else { p.recipient };
    let operator_after = (stage == 3).then_some(if recipient == p.recipient {
        time
    } else {
        time.saturating_add(300)
    });
    let result = sqlx::query("INSERT OR IGNORE INTO attention_occurrences(followup,plan_version,stage,reason,recipient,created,operator_after) VALUES(?,?,?,?,?,?,?)")
        .bind(p.id).bind(p.version).bind(stage).bind(if stage==3 {"escalation"} else if dependency_ready {"dependency_ready"} else {"reminder"}).bind(recipient).bind(time).bind(operator_after).execute(&mut **tx).await?;
    if result.rows_affected() == 1 && (stage != 3 || recipient != p.recipient) {
        sqlx::query("INSERT INTO coordination_events(recipient,kind,subject,version,created) VALUES(?,'attention_due',?,?,?)")
            .bind(recipient).bind(result.last_insert_rowid().to_string()).bind(p.version).bind(time).execute(&mut **tx).await?;
    }
    // Preserve the hard boundary and the separate recovery interval.
    sqlx::query("UPDATE followups SET stage=?,next_check=MIN(escalate_at,?+(SELECT interval_seconds FROM followup_policy WHERE group_name=?)) WHERE id=?")
        .bind(stage).bind(time).bind(&p.group_name).bind(p.id).execute(&mut **tx).await?;
    Ok(())
}

/// Return the next future attention deadline for the local worker.
/// Elapsed deadlines are left to bounded reconciliation so unread or held work
/// cannot make the worker spin. Disabled and paused groups do not schedule wakes.
/// # Errors
/// Reading the persisted attention schedule fails.
pub async fn next_deadline(store: &Store, time: i64) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "WITH enabled AS (
            SELECT f.* FROM scheduled_followups f
            JOIN followup_policy p ON p.group_name=f.group_name
            JOIN groups g ON g.name=f.group_name
            WHERE p.mode='enabled' AND g.paused=0
        ), deadlines AS (
            SELECT next_check AS deadline FROM enabled WHERE stage<3
            UNION ALL SELECT escalate_at FROM enabled WHERE stage<3
            UNION ALL SELECT MAX(o.operator_after,o.operator_next)
                FROM active_attention o JOIN enabled f ON f.id=o.followup
                WHERE o.stage=3 AND o.operator_attempts<3 AND o.operator_state<>'accepted'
        ) SELECT MIN(deadline) FROM deadlines WHERE deadline>?",
    )
    .bind(time)
    .fetch_one(store.pool())
    .await?)
}

/// Reconcile a fair bounded page; periodic service scans recover missed hints.
pub async fn reconcile(store: &Store, time: i64) -> Result<()> {
    let mut tx = store.pool().begin().await?;
    // Take SQLite's writer reservation before reading decisions.
    sqlx::query("UPDATE followup_policy SET updated=updated WHERE 0")
        .execute(&mut *tx)
        .await?;
    let before: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM coordination_events")
        .fetch_one(&mut *tx)
        .await?;
    // Bounded repair also covers registrations that became local after assignment.
    sqlx::query("INSERT OR IGNORE INTO followups(group_name,message,recipient,authority,opened,next_check,escalate_at) SELECT b.group_name,d.message,b.id,m.sender,m.created,?+p.max_seconds,?+p.max_seconds FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient JOIN followup_policy p ON p.group_name=b.group_name WHERE m.intent='request' AND d.state='pending' AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.message=d.message AND f.recipient=b.id) ORDER BY m.id,b.id LIMIT 100")
        .bind(time).bind(time).execute(&mut *tx).await?;
    sqlx::query("INSERT OR IGNORE INTO followups(group_name,task,task_version,recipient,authority,opened,next_check,escalate_at) SELECT w.group_name,w.id,w.version,b.id,a.id,w.updated,?+p.max_seconds,?+p.max_seconds FROM work_items w JOIN mailboxes b ON b.group_name=w.group_name AND b.name=w.owner JOIN mailboxes a ON a.group_name=w.group_name AND a.name=w.writer JOIN followup_policy p ON p.group_name=w.group_name WHERE w.open=1 AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM followups f WHERE f.group_name=w.group_name AND f.task=w.id) ORDER BY w.group_name,w.id LIMIT 100")
        .bind(time).bind(time).execute(&mut *tx).await?;
    let plans = sqlx::query_as::<_, Plan>(
        "SELECT * FROM scheduled_followups f WHERE stage<3 OR (dependency_ready_at IS NULL AND (json_extract(checkpoint,'$.waiting.kind') IN ('task','tasks','mail','pull_request') OR EXISTS(SELECT 1 FROM task_dependency_edges e WHERE e.group_name=f.group_name AND e.source=f.task))) ORDER BY scanned,id LIMIT 100",
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
        let exhausted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM coordination_events e JOIN mailboxes b ON b.id=e.recipient JOIN attention_attempts a ON a.recipient=e.recipient AND a.binding_version=b.binding_version AND a.event=e.id WHERE e.recipient=? AND ((e.kind='mail_pending' AND e.subject=CAST(? AS TEXT)) OR (e.kind='work_changed' AND e.subject=? AND e.version=?)) AND a.attempts>=3 AND a.next_attempt<=? AND NOT EXISTS(SELECT 1 FROM event_receipts r WHERE r.recipient=e.recipient AND r.binding_version=b.binding_version AND r.event=e.id))")
            .bind(p.recipient).bind(p.message).bind(&p.task).bind(p.task_version).bind(time).fetch_one(&mut *tx).await?;
        let report = p.report()?;
        let unread = !retrieved && report.is_none();
        let readiness =
            crate::task_graph::readiness_tx(&mut tx, &p.group_name, p.task.as_deref()).await?;
        let dependencies = readiness.as_ref().is_some_and(|r| r.total > 0);
        let checkpoint_wait = report.as_ref().is_some_and(|r| r.waiting.is_some());
        let waiting = dependencies || checkpoint_wait;
        let satisfied = waiting
            && readiness.as_ref().is_none_or(|r| r.ready)
            && (!checkpoint_wait || waiting_satisfied(&mut tx, &p).await?);
        let newly_satisfied = satisfied && p.dependency_ready_at.is_none();
        if newly_satisfied {
            sqlx::query("UPDATE followups SET dependency_ready_at=? WHERE id=?")
                .bind(time)
                .bind(p.id)
                .execute(&mut *tx)
                .await?;
        }
        let held:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=? AND state IN ('blocked','review'))").bind(&p.group_name).bind(&p.task).fetch_one(&mut *tx).await?;
        if p.stage == 3 {
            // A changed prerequisite creates a new generation without extending
            // the boundary or dropping the writer's outstanding escalation.
            let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM attention_occurrences WHERE followup=? AND plan_version=? AND stage=3)")
                .bind(p.id).bind(p.version).fetch_one(&mut *tx).await?;
            if !current {
                advance_attention(&mut tx, &p, 3, time, false).await?;
                // This is still the same overdue obligation. Keep operator
                // delivery history and its deadline rather than restarting it
                // each time a prerequisite changes truth.
                sqlx::query("UPDATE attention_occurrences SET (operator_after,operator_attempts,operator_next,operator_state,operator_detail)=(SELECT operator_after,operator_attempts,operator_next,operator_state,operator_detail FROM attention_occurrences old WHERE old.followup=? AND old.stage=3 AND old.plan_version<? ORDER BY old.id DESC LIMIT 1) WHERE followup=? AND plan_version=? AND stage=3 AND EXISTS(SELECT 1 FROM attention_occurrences old WHERE old.followup=? AND old.stage=3 AND old.plan_version<?)")
                    .bind(p.id).bind(p.version).bind(p.id).bind(p.version).bind(p.id).bind(p.version).execute(&mut *tx).await?;
            }
            if newly_satisfied && !held {
                // A dependency may become ready after escalation. Wake an unheld owner
                // while retaining the authority's escalation and hard boundary.
                publish_dependency_ready(&mut tx, &p, time).await?;
            }
            continue;
        }
        let hard_due = time >= p.escalate_at;
        // Persistent prerequisites suppress owner reminders, but never prevent
        // supervision at the original hard boundary.
        if dependencies && !satisfied && !checkpoint_wait && !hard_due {
            continue;
        }
        let due = time >= p.next_check || newly_satisfied;
        if !(hard_due || due || unread && exhausted) {
            continue;
        }
        if unread && !exhausted && !hard_due && !satisfied {
            continue;
        }
        let stage =
            if hard_due || (unread && exhausted) || (waiting && !satisfied) || held || p.stage >= 2
            {
                3
            } else {
                p.stage + 1
            };
        // Escalation is attention metadata, never a recursive mail obligation.
        advance_attention(&mut tx, &p, stage, time, newly_satisfied).await?;
        if stage == 3 && newly_satisfied && !held {
            publish_dependency_ready(&mut tx, &p, time).await?;
        }
    }
    let after: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id),0) FROM coordination_events")
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    if after != before {
        crate::stream::hint(store.root()).await;
    }
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
        let totals=sqlx::query("SELECT COUNT(*) AS pending,COALESCE(SUM(s.followup IS NULL),0) AS scheduled,COALESCE(SUM(s.followup IS NOT NULL),0) AS shared_requests,COALESCE(SUM(s.followup IS NULL AND f.next_check<=?),0) AS due,COALESCE(SUM(s.followup IS NULL AND f.stage=3),0) AS escalated FROM active_followups f LEFT JOIN task_request_schedules s ON s.followup=f.id WHERE (? IS NULL OR f.group_name=?) AND (? IS NULL OR f.recipient=? OR f.authority=?)")
            .bind(time).bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_one(self.pool()).await?;
        let rows=sqlx::query("SELECT f.*,b.name AS owner,b.binding_version AS current_binding,a.name AS decision_owner,p.mode FROM scheduled_followups f JOIN mailboxes b ON b.id=f.recipient JOIN mailboxes a ON a.id=f.authority JOIN followup_policy p ON p.group_name=f.group_name WHERE (? IS NULL OR f.group_name=?) AND (? IS NULL OR f.recipient=? OR f.authority=?) ORDER BY f.escalate_at,f.id LIMIT 101").bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let more = rows.len() > 100;
        let mut items = Vec::new();
        for r in rows.iter().take(100) {
            let checkpoint = r
                .get::<Option<String>, _>("checkpoint")
                .map(|s| serde_json::from_str::<Checkpoint>(&s))
                .transpose()?;
            items.push(json!({"id":r.get::<i64,_>("id"),"group":r.get::<String,_>("group_name"),"task":r.get::<Option<String>,_>("task"),"task_version":r.get::<i64,_>("task_version"),"message":r.get::<Option<i64>,_>("message"),"version":r.get::<i64,_>("version"),"owner":r.get::<String,_>("owner"),"decision_owner":r.get::<String,_>("decision_owner"),"retrieved_at":r.get::<Option<i64>,_>("retrieved_at"),"retrieval_current":r.get::<Option<i64>,_>("retrieved_binding")==Some(r.get::<i64,_>("current_binding")),"checkpoint":checkpoint,"next_check_at":r.get::<i64,_>("next_check"),"escalate_at":r.get::<i64,_>("escalate_at"),"escalated":r.get::<i64,_>("stage")==3,"due":r.get::<i64,_>("next_check")<=time,"mode":r.get::<String,_>("mode")}));
        }
        for item in &mut items {
            if !item["checkpoint"].is_null() {
                let report: Checkpoint = serde_json::from_value(item["checkpoint"].clone())?;
                if let Some(wait) = report.waiting {
                    item["external_condition"] =
                        crate::external::condition_status(self, &wait).await?;
                }
            }
        }
        let delivery=sqlx::query("SELECT e.id,e.kind,e.subject,e.recipient,b.name,b.binding_version,a.attempts,a.next_attempt FROM attention_events e JOIN mailboxes b ON b.id=e.recipient JOIN attention_attempts a ON a.recipient=e.recipient AND a.binding_version=b.binding_version AND a.event=e.id WHERE (? IS NULL OR b.group_name=?) AND (? IS NULL OR e.recipient=?) ORDER BY a.next_attempt,e.id LIMIT 101")
            .bind(group).bind(group).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let attempts:Vec<Value>=delivery.iter().take(100).map(|r|json!({"event":r.get::<i64,_>("id"),"kind":r.get::<String,_>("kind"),"subject":r.get::<String,_>("subject"),"participant":r.get::<String,_>("name"),"binding_version":r.get::<i64,_>("binding_version"),"attempts":r.get::<i64,_>("attempts"),"next_attempt":r.get::<i64,_>("next_attempt"),"exhausted":r.get::<i64,_>("attempts")>=3 && r.get::<i64,_>("next_attempt")<=time})).collect();
        let notifications=sqlx::query("SELECT o.id,f.group_name,o.operator_state,o.operator_detail,o.operator_attempts,o.operator_next FROM active_attention o JOIN followups f ON f.id=o.followup WHERE o.stage=3 AND (? IS NULL OR f.group_name=?) AND (? IS NULL OR f.recipient=? OR f.authority=?) ORDER BY o.id DESC LIMIT 101").bind(group).bind(group).bind(actor).bind(actor).bind(actor).fetch_all(self.pool()).await?;
        let alerts:Vec<Value>=notifications.iter().take(100).map(|r|json!({"id":r.get::<i64,_>("id"),"group":r.get::<String,_>("group_name"),"state":r.get::<String,_>("operator_state"),"detail":r.get::<Option<String>,_>("operator_detail"),"attempts":r.get::<i64,_>("operator_attempts"),"next_attempt":r.get::<i64,_>("operator_next")})).collect();
        Ok(
            json!({"totals":{"pending":totals.get::<i64,_>("pending"),"scheduled":totals.get::<i64,_>("scheduled"),"shared_requests":totals.get::<i64,_>("shared_requests"),"due":totals.get::<i64,_>("due"),"escalated":totals.get::<i64,_>("escalated")},"items":items,"more":more || notifications.len()>100 || delivery.len()>100,"delivery_attempts":attempts,"operator_notifications":alerts,"remote_followup":"unsupported"}),
        )
    }
}

/// Deliver bounded escalation summaries outside the coordinator's wake path.
pub async fn notify_operators(store: &Store, time: i64) -> Result<()> {
    // A crash after a reservation leaves uncertain evidence, never permanent in-flight state.
    sqlx::query("UPDATE attention_occurrences SET operator_state='uncertain',operator_detail='Previous attempt has no confirmed result' WHERE operator_state='attempting' AND operator_next<=?").bind(time).execute(store.pool()).await?;
    let rows=sqlx::query("SELECT o.id,f.group_name,g.socket,p.notifier FROM active_attention o JOIN followups f ON f.id=o.followup JOIN groups g ON g.name=f.group_name JOIN followup_policy p ON p.group_name=f.group_name WHERE o.stage=3 AND o.operator_after<=? AND o.operator_next<=? AND o.operator_attempts<3 AND o.operator_state<>'accepted' AND p.mode='enabled' AND g.paused=0 ORDER BY o.operator_next,o.id LIMIT 10").bind(time).bind(time).fetch_all(store.pool()).await?;
    // One external call per group per scan; all included sources keep durable receipts.
    let mut grouped =
        std::collections::BTreeMap::<String, (Option<String>, Option<String>, Vec<i64>)>::new();
    for row in rows {
        let entry = grouped
            .entry(row.get("group_name"))
            .or_insert_with(|| (row.get("socket"), row.get("notifier"), Vec::new()));
        entry.2.push(row.get("id"));
    }
    for (group, (socket, notifier, ids)) in grouped {
        let mut reserved = Vec::new();
        for id in ids {
            let changed=sqlx::query("UPDATE attention_occurrences SET operator_attempts=operator_attempts+1,operator_next=?,operator_state='attempting' WHERE id=? AND operator_attempts<3 AND operator_next<=? AND EXISTS(SELECT 1 FROM active_attention o JOIN followups f ON f.id=o.followup JOIN followup_policy p ON p.group_name=f.group_name JOIN groups g ON g.name=f.group_name WHERE o.id=? AND p.mode='enabled' AND g.paused=0)")
                .bind(time.saturating_add(300)).bind(id).bind(time).bind(id).execute(store.pool()).await?;
            if changed.rows_affected() == 1 {
                reserved.push(id);
            }
        }
        if reserved.is_empty() {
            continue;
        }
        let payload = json!({"group":group,"attention_ids":reserved,"instruction":"Unresolved Agent Mail escalation. Inspect agent-mail status --json for this group."});
        let result = if let Some(notifier) = notifier {
            run_notifier(&notifier, &payload).await.map(|()| "accepted")
        } else if let Some(socket) = socket.filter(|s| !s.is_empty()) {
            crate::herdr::notify(std::path::Path::new(&socket), &group, reserved.len())
                .await
                .map(|()| "accepted")
        } else {
            Ok("unconfigured")
        };
        let (state, detail) = match result {
            Ok("unconfigured") => (
                "unconfigured",
                Some("No operator push route configured; visible in status only".into()),
            ),
            Ok(_) => ("accepted", None),
            Err(error) => ("failed", Some(format!("{error:#}"))),
        };
        for id in reserved {
            sqlx::query(
                "UPDATE attention_occurrences SET operator_state=?,operator_detail=? WHERE id=?",
            )
            .bind(state)
            .bind(detail.as_deref())
            .bind(id)
            .execute(store.pool())
            .await?;
        }
    }
    Ok(())
}
async fn run_notifier(config: &str, payload: &Value) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let args: Vec<String> = serde_json::from_str(config)?;
    ensure!(!args.is_empty(), "empty notifier");
    let mut child = tokio::process::Command::new(&args[0])
        .args(&args[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        if let Some(mut input) = child.stdin.take() {
            input.write_all(&serde_json::to_vec(payload)?).await?;
            input.shutdown().await?;
        }
        ensure!(
            child.wait().await?.success(),
            "operator notifier exited unsuccessfully"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("operator notifier timed out")??;
    Ok(())
}
