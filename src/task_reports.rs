//! Immutable owner results backed by mandatory, transactionally published requests.
//!
//! Reporting never changes task state or acceptance authority. Ordinary mail
//! remains independently private; submitting a task report explicitly makes its
//! evidence available to the task's writer, including an authorized successor.

use crate::{
    mail_context::{ContextSource, TaskVersion},
    states::MessageIntent,
    store::{Mailbox, Publish, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Sqlite, Transaction};

/// An owner's result requiring a disposition from the task writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReport {
    /// Current task version observed by the owner.
    pub version: TaskVersion,
    /// Stable logical submission key; identical retries return the original result.
    pub key: String,
    /// Result summary and the decision needed from the writer.
    pub summary: String,
    /// Immutable source revision being submitted for disposition.
    pub revision: String,
    /// Accessible evidence references, without claiming they have been verified.
    pub evidence: Vec<String>,
    /// Supporting explanation; large material belongs in referenced artifacts.
    pub body: String,
}

impl Store {
    /// Submit a current owner's result and publish its writer obligation atomically.
    ///
    /// Identical retries remain valid after later task decisions. New submissions
    /// require the exact current version and owner, and an unfinished local task.
    /// # Errors
    /// The actor or task is stale, authority or payload is invalid, a key conflicts,
    /// the task is only a remote snapshot, or persistence fails.
    pub async fn report_task(
        &self,
        actor: &Mailbox,
        id: &str,
        report: TaskReport,
        now: i64,
    ) -> Result<Value> {
        crate::name(id)?;
        crate::bounded(&report.key, 96, "report key")?;
        ensure!(!report.key.is_empty(), "report key is required");
        crate::bounded(
            &report.revision,
            crate::work::REVISION_LIMIT,
            "reported revision",
        )?;
        ensure!(
            !report.revision.trim().is_empty(),
            "reported revision is required"
        );
        crate::work::validate_evidence(&report.evidence)?;
        let canonical = serde_json::to_string(&(id, &report))?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if let Some(old) = sqlx::query!(
            "SELECT message,canonical FROM task_reports WHERE reporter=? AND report_key=?",
            actor.id,
            report.key
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            ensure!(
                old.canonical == canonical,
                "report key already exists with different content"
            );
            let value = Self::task_report_value_tx(&mut tx, actor, old.message).await?;
            tx.commit().await?;
            return Ok(value);
        }
        let send_key = format!("task-report:{}", report.key);
        ensure!(sqlx::query_scalar!("SELECT NOT EXISTS(SELECT 1 FROM messages WHERE sender=? AND dedup_key=?) AS 'unused!: bool'",actor.id,send_key).fetch_one(&mut *tx).await?, "report publication key conflicts with ordinary mail");
        let task = sqlx::query!(
            "SELECT owner,writer,version,open FROM work_items WHERE group_name=? AND id=?",
            actor.group_name, id
        ).fetch_optional(&mut *tx).await?.context("task reports require the task's home store; a remote snapshot cannot establish current reporting authority")?;
        ensure!(
            task.version == report.version.get(),
            "task version conflict; read the current task before reporting"
        );
        ensure!(
            task.owner == actor.name,
            "only the current task owner may submit a result"
        );
        ensure!(task.open == 1, "terminal tasks cannot receive a new result");
        // The body carries typed evidence over the existing contextual mail path.
        // Follow-ups, retries and disposition all use its existing request ledger.
        let body = serde_json::to_string(
            &json!({"task":id,"version":report.version,"revision":report.revision,"evidence":report.evidence,"body":report.body}),
        )?;
        let mut publication = Publish {
            intent: MessageIntent::Request,
            recipients: vec![task.writer],
            key: send_key,
            summary: report.summary,
            body,
            due_after: None,
            context: ContextSource::Task {
                id: id.parse()?,
                version: report.version,
            },
        };
        let message = Self::publish_tx(&mut tx, actor, &mut publication, now).await?;
        // An unrelated mail send cannot masquerade as a typed submission by
        // occupying its reserved publication key before the report transaction.
        ensure!(
            sqlx::query_scalar!(
                "SELECT NOT EXISTS(SELECT 1 FROM task_reports WHERE message=?) AS 'unused!: bool'",
                message
            )
            .fetch_one(&mut *tx)
            .await?,
            "message is already a task report"
        );
        let evidence = serde_json::to_string(&report.evidence)?;
        let authority_version = report.version.get();
        sqlx::query!("INSERT INTO task_reports(message,group_name,work_id,reporter,report_key,canonical,revision,evidence,authority_version,decision_message) VALUES(?,?,?,?,?,?,?,?,?,?)",message,actor.group_name,id,actor.id,report.key,canonical,report.revision,evidence,authority_version,message).execute(&mut *tx).await?;
        Self::retrieve_tx(
            &mut tx,
            actor,
            crate::states::EventKind::WorkChanged,
            id,
            authority_version,
        )
        .await?;
        let value = Self::task_report_value_tx(&mut tx, actor, message).await?;
        tx.commit().await?;
        crate::stream::hint(self.root()).await;
        Ok(value)
    }

    /// Read a typed result as its author or the task's current writer.
    /// # Errors
    /// The actor is stale, the result is not authorized, or persistence fails.
    pub async fn task_report_value(&self, actor: &Mailbox, id: i64) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let value = Self::task_report_value_tx(&mut tx, actor, id).await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn task_report_value_tx(
        tx: &mut Transaction<'_, Sqlite>,
        actor: &Mailbox,
        id: i64,
    ) -> Result<Value> {
        let row = sqlx::query!("SELECT r.message,r.work_id,r.canonical,r.decision_message,m.created FROM task_reports r JOIN work_items w ON w.group_name=r.group_name AND w.id=r.work_id JOIN messages m ON m.id=r.message WHERE r.message=? AND r.group_name=? AND (r.reporter=? OR w.writer=?)",id,actor.group_name,actor.id,actor.name).fetch_optional(&mut **tx).await?.context("task report is not available to this agent in this group")?;
        let (_, report): (String, TaskReport) = serde_json::from_str(&row.canonical)?;
        let state: String =
            sqlx::query_scalar("SELECT state FROM deliveries WHERE message=? LIMIT 1")
                .bind(row.decision_message)
                .fetch_one(&mut **tx)
                .await?;
        let addressed = sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM deliveries WHERE message=? AND recipient=?) AS 'addressed!: bool'",row.decision_message,actor.id).fetch_one(&mut **tx).await?;
        if addressed {
            Self::retrieve_tx(
                tx,
                actor,
                crate::states::EventKind::MailPending,
                &row.decision_message.to_string(),
                0,
            )
            .await?;
            sqlx::query!("INSERT OR IGNORE INTO message_observations(message,recipient,binding_version) VALUES(?,?,?)",row.decision_message,actor.id,actor.binding_version).execute(&mut **tx).await?;
        }
        Ok(
            json!({"id":row.message,"task":row.work_id,"report":report,"created":row.created,"message":row.decision_message,"disposition":state,"persisted":true}),
        )
    }

    /// Inspect bounded report metadata without exposing another mailbox's body.
    /// # Errors
    /// The actor is stale or storage fails.
    pub async fn task_reports(&self, actor: &Mailbox, task: &str, after: i64) -> Result<Value> {
        crate::name(task)?;
        ensure!(after >= 0, "report cursor must be nonnegative");
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let rows = sqlx::query!("SELECT r.message,r.revision,r.decision_message,m.created,b.name AS reporter,d.state FROM task_reports r JOIN messages m ON m.id=r.message JOIN mailboxes b ON b.id=r.reporter JOIN deliveries d ON d.message=r.decision_message WHERE r.group_name=? AND r.work_id=? AND r.message>? ORDER BY r.message LIMIT 21",actor.group_name,task,after).fetch_all(&mut *tx).await?;
        let more = rows.len() > 20;
        let items:Vec<_> = rows.into_iter().take(20).map(|r|json!({"id":r.message,"revision":r.revision,"message":r.decision_message,"reporter":r.reporter,"created":r.created,"disposition":r.state})).collect();
        let next_after = items.last().and_then(|v| v["id"].as_i64()).unwrap_or(after);
        tx.commit().await?;
        Ok(json!({"items":items,"more":more,"next_after":next_after}))
    }

    pub(crate) async fn transfer_reports_tx(
        tx: &mut Transaction<'_, Sqlite>,
        writer: &Mailbox,
        task: &str,
        next_writer: &str,
        next_version: i64,
        now: i64,
    ) -> Result<()> {
        let prior_version = next_version
            .checked_sub(1)
            .context("task version underflow")?;
        let rows = sqlx::query!("SELECT r.message,r.decision_message,b.name AS reporter,m.summary,m.body,m.context AS 'context!: crate::mail_context::MessageContext' FROM task_reports r JOIN mailboxes b ON b.id=r.reporter JOIN messages m ON m.id=r.message JOIN deliveries d ON d.message=r.decision_message WHERE r.group_name=? AND r.work_id=? AND d.state='pending'",writer.group_name,task).fetch_all(&mut **tx).await?;
        for row in rows {
            // The task writer authorizes rerouting this typed result; its
            // reporting owner remains the logical requester for replies/waits.
            let reporter = Self::mailbox_tx(tx, &writer.group_name, &row.reporter).await?;
            let crate::mail_context::MessageContext::Task { id, version } = row.context else {
                anyhow::bail!("task report has invalid context")
            };
            // The task transfer ledger owns retry idempotency. This forward
            // must create a fresh obligation that ordinary mail cannot occupy
            // in advance or supply in an already-settled state.
            let key = format!("report-transfer:{}", uuid::Uuid::new_v4());
            ensure!(
                sqlx::query_scalar!("SELECT NOT EXISTS(SELECT 1 FROM messages WHERE sender=? AND dedup_key=?) AS 'unused!: bool'",reporter.id,key)
                    .fetch_one(&mut **tx).await?,
                "fresh report transfer key already exists"
            );
            let mut publication = Publish {
                intent: MessageIntent::Request,
                recipients: vec![next_writer.into()],
                key,
                summary: row.summary,
                body: row.body,
                due_after: None,
                context: ContextSource::Task { id, version },
            };
            let next = Self::publish_tx(tx, &reporter, &mut publication, now).await?;
            sqlx::query!("UPDATE followups SET authority=(SELECT reporter FROM task_reports WHERE message=?),opened=(SELECT opened FROM followups WHERE message=?),next_check=(SELECT next_check FROM followups WHERE message=?),escalate_at=(SELECT escalate_at FROM followups WHERE message=?) WHERE message=? AND EXISTS(SELECT 1 FROM followups WHERE message=?)",row.message,row.decision_message,row.decision_message,row.decision_message,next,row.decision_message).execute(&mut **tx).await?;
            sqlx::query!("UPDATE deliveries SET state='withdrawn',resolution='Task report handed to the new writer' WHERE message=? AND state='pending'",row.decision_message).execute(&mut **tx).await?;
            sqlx::query!(
                "UPDATE task_reports SET decision_message=?,authority_version=CASE WHEN authority_version=? THEN ? ELSE authority_version END WHERE message=?",
                next,
                prior_version,
                next_version,
                row.message
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}
