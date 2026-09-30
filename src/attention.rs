//! Observable work progress and delivery problems without inferred completion.
//!
//! [`Store::attention`] accepts Unix seconds and returns bounded work and issue
//! lists. Its `more` flag signals truncation. Delivery receipts, retry exhaustion,
//! and missing endpoints describe transport health independently of business state.

use crate::store::Store;
use anyhow::Result;
use serde::Serialize;

/// A transport or business obligation that may require operator attention.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    /// An open work assignment has passed its deadline.
    WorkOverdue,
    /// An unresolved delivery has passed its deadline.
    MailOverdue,
    /// Actionable work exists without a current native wake endpoint.
    MissingEndpoint,
    /// The persisted delivery retry budget has been consumed.
    DeliveryExhausted,
    /// A persisted attempt lacks a runtime or retrieval receipt.
    DeliveryUnconfirmed,
}
/// An actionable diagnostic for a group participant.
#[derive(Debug, Serialize)]
pub struct AttentionItem {
    /// Enrolled group containing the referenced participant or record.
    pub group: String,
    /// Name of the participant addressed by this result.
    pub participant: String,
    /// Event or diagnostic category.
    pub kind: AttentionKind,
    /// Identifier of the message, work record, or other changed subject.
    pub subject: Option<String>,
    /// Evidence or explanation associated with this diagnostic.
    pub detail: String,
}
/// Business progress reported independently of transport receipt.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressState {
    /// The assignment remains open regardless of transport status.
    Open,
}
/// An open assignment’s owner, revision, and deadline.
#[derive(Debug, Serialize)]
pub struct WorkProgress {
    /// Enrolled group containing the referenced participant or record.
    pub group: String,
    /// Persistent identifier for this record.
    pub id: String,
    /// Participant responsible for the assignment.
    pub owner: String,
    /// Record or protocol version used to validate this operation.
    pub version: i64,
    /// Deadline in Unix seconds; in patches, Some(None) explicitly clears it.
    pub deadline: Option<i64>,
    /// Stored business state; it does not imply transport delivery.
    pub state: ProgressState,
}
/// Bounded work progress and diagnostics with a truncation indicator.
#[derive(Debug, Serialize)]
pub struct AttentionReport {
    /// Bounded list of open work records.
    pub work: Vec<WorkProgress>,
    /// Bounded list of attention diagnostics.
    pub items: Vec<AttentionItem>,
    /// Whether additional records were omitted from this bounded report.
    pub more: bool,
}

impl Store {
    /// Report overdue obligations and delivery problems at a supplied timestamp.
    ///
    /// # Errors
    /// The database queries fail.
    pub async fn attention(&self, now: i64) -> Result<AttentionReport> {
        self.attention_for(None, now).await
    }
    /// Report attention for one group, or the installation when omitted.
    /// # Errors
    /// Database queries fail.
    pub async fn attention_for(&self, group: Option<&str>, now: i64) -> Result<AttentionReport> {
        let work = sqlx::query!("SELECT group_name,id,owner,version,deadline FROM work_items WHERE open=1 AND (? IS NULL OR group_name=?) ORDER BY deadline IS NULL,deadline,group_name,id LIMIT 101",group,group).fetch_all(self.pool()).await?;
        let mut more = work.len() > 100;
        let mut items = Vec::new();
        let mut progress = Vec::new();
        for row in work.into_iter().take(100) {
            let state = ProgressState::Open;
            if row.deadline.is_some_and(|d| d <= now) {
                items.push(AttentionItem {
                    group: row.group_name.clone(),
                    participant: row.owner.clone(),
                    kind: AttentionKind::WorkOverdue,
                    subject: Some(row.id.clone()),
                    detail:
                        "Deadline passed; work remains open and delivery does not prove progress"
                            .into(),
                });
            }
            progress.push(WorkProgress {
                group: row.group_name,
                id: row.id,
                owner: row.owner,
                version: row.version,
                deadline: row.deadline,
                state,
            });
        }
        let overdue = sqlx::query!("SELECT b.group_name,b.name,m.id FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient WHERE d.state='pending' AND m.deadline<=? AND (? IS NULL OR b.group_name=?) ORDER BY m.deadline LIMIT 101",now,group,group).fetch_all(self.pool()).await?;
        more |= overdue.len() > 100;
        for row in overdue.into_iter().take(100) {
            items.push(AttentionItem {
                group: row.group_name,
                participant: row.name,
                kind: AttentionKind::MailOverdue,
                subject: Some(row.id.to_string()),
                detail: "Request remains unresolved after its deadline".into(),
            });
        }
        let missing=sqlx::query!("SELECT b.group_name,b.name FROM mailboxes b WHERE (? IS NULL OR b.group_name=?) AND b.pane IS NULL AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM runtime_wakes c WHERE c.recipient=b.id AND c.binding_version=b.binding_version) AND (EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=b.group_name AND w.owner=b.name AND w.open=1) OR EXISTS(SELECT 1 FROM deliveries d WHERE d.recipient=b.id AND d.state='pending')) LIMIT 101",group,group).fetch_all(self.pool()).await?;
        more |= missing.len() > 100;
        for row in missing.into_iter().take(100) {
            items.push(AttentionItem{group:row.group_name,participant:row.name,kind:AttentionKind::MissingEndpoint,subject:None,detail:"No current idle-wake endpoint; hooks alone cannot wake an idle client. Run status --check for setup guidance".into()});
        }
        let exhausted=sqlx::query!("SELECT b.group_name,b.name,c.attempts FROM runtime_wakes c JOIN mailboxes b ON b.id=c.recipient AND b.binding_version=c.binding_version WHERE (? IS NULL OR b.group_name=?) AND c.attempts>0 AND EXISTS(SELECT 1 FROM wake_events e WHERE e.recipient=b.id AND e.id>c.scanned) AND NOT EXISTS(SELECT 1 FROM wake_events e WHERE e.recipient=b.id AND e.id>c.attempted) UNION ALL SELECT b.group_name,b.name,b.attempts FROM mailboxes b WHERE (? IS NULL OR b.group_name=?) AND b.agent_state='registered' AND b.pane IS NOT NULL AND b.attempts>0 AND b.wake_attempted>=(SELECT MAX(id) FROM herdr_wake_events WHERE recipient=b.id) ORDER BY group_name,name LIMIT 101",group,group,group,group).fetch_all(self.pool()).await?;
        more |= exhausted.len() > 100;
        for row in exhausted.into_iter().take(100) {
            items.push(AttentionItem {
                group: row.group_name,
                participant: row.name,
                kind: if row.attempts >= 3 { AttentionKind::DeliveryExhausted } else { AttentionKind::DeliveryUnconfirmed },
                subject: None,
                detail: if row.attempts >= 3 { "Delivery attempts exhausted; inspect endpoint before explicitly rearming" } else { "Delivery attempt persisted without a runtime or retrieval receipt; delivery may be in progress or uncertain" }.into(),
            });
        }
        Ok(AttentionReport {
            work: progress,
            items,
            more,
        })
    }
}
