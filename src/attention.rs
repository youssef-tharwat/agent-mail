//! Observable progress and delivery reporting, independent of delivery receipts.
use crate::store::Store;
use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    WorkOverdue,
    MailOverdue,
    MissingEndpoint,
    DeliveryExhausted,
    DeliveryUnconfirmed,
}
#[derive(Debug, Serialize)]
pub struct AttentionItem {
    pub group: String,
    pub participant: String,
    pub kind: AttentionKind,
    pub subject: Option<String>,
    pub detail: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressState {
    Open,
}
#[derive(Debug, Serialize)]
pub struct WorkProgress {
    pub group: String,
    pub id: String,
    pub owner: String,
    pub version: i64,
    pub deadline: Option<i64>,
    pub state: ProgressState,
}
#[derive(Debug, Serialize)]
pub struct AttentionReport {
    pub work: Vec<WorkProgress>,
    pub items: Vec<AttentionItem>,
    pub more: bool,
}

impl Store {
    /// Report overdue obligations and delivery problems without inferring completion.
    pub async fn attention(&self, now: i64) -> Result<AttentionReport> {
        let work = sqlx::query!("SELECT group_name,id,owner,version,deadline FROM work_items WHERE open=1 ORDER BY deadline IS NULL,deadline,group_name,id LIMIT 101").fetch_all(&self.pool).await?;
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
        let overdue = sqlx::query!("SELECT b.group_name,b.name,m.id FROM deliveries d JOIN messages m ON m.id=d.message JOIN mailboxes b ON b.id=d.recipient WHERE d.state='pending' AND m.due<=? ORDER BY m.due LIMIT 101",now).fetch_all(&self.pool).await?;
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
        let missing=sqlx::query!("SELECT b.group_name,b.name FROM mailboxes b WHERE b.pane IS NULL AND b.remote_machine IS NULL AND NOT EXISTS(SELECT 1 FROM codex_wakes c WHERE c.recipient=b.id AND c.binding_version=b.binding_version) AND (EXISTS(SELECT 1 FROM work_items w WHERE w.group_name=b.group_name AND w.owner=b.name AND w.open=1) OR EXISTS(SELECT 1 FROM deliveries d WHERE d.recipient=b.id AND d.state='pending')) LIMIT 101").fetch_all(&self.pool).await?;
        more |= missing.len() > 100;
        for row in missing.into_iter().take(100) {
            items.push(AttentionItem{group:row.group_name,participant:row.name,kind:AttentionKind::MissingEndpoint,subject:None,detail:"No current idle-wake endpoint; hooks alone cannot wake an idle client. Run doctor for setup guidance".into()});
        }
        let exhausted=sqlx::query!("SELECT b.group_name,b.name,c.attempts FROM codex_wakes c JOIN mailboxes b ON b.id=c.recipient AND b.binding_version=c.binding_version WHERE c.attempts>0 AND EXISTS(SELECT 1 FROM wake_events e WHERE e.recipient=b.id AND e.id>c.scanned) LIMIT 101").fetch_all(&self.pool).await?;
        more |= exhausted.len() > 100;
        for row in exhausted.into_iter().take(100) {
            items.push(AttentionItem {
                group: row.group_name,
                participant: row.name,
                kind: if row.attempts >= 3 { AttentionKind::DeliveryExhausted } else { AttentionKind::DeliveryUnconfirmed },
                subject: None,
                detail: if row.attempts >= 3 { "Delivery attempts exhausted; inspect endpoint before explicitly rearming" } else { "Delivery attempt persisted without a confirmed receipt; delivery may be in progress or uncertain" }.into(),
            });
        }
        Ok(AttentionReport {
            work: progress,
            items,
            more,
        })
    }
}
