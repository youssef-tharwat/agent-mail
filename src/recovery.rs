//! Bounded recovery views shared by the CLI and automatic hooks.
//!
//! The view combines current work, unresolved mail, relay health, and continuation
//! cursors. Payload budgets may truncate either collection; callers use the returned
//! cursors to fetch more. Visible records receive transport retrieval receipts; business state is unchanged.

use crate::store::{Mailbox, Store};
use anyhow::Result;
use serde_json::{Value, json};

/// A bounded view and the exact source records whose details it contains.
pub(crate) struct RecoveryView {
    pub value: Value,
    pub mail: Vec<i64>,
    pub work: Vec<(String, i64)>,
}

impl Store {
    /// Read bounded recovery state and continuation cursors for a participant.
    ///
    /// # Errors
    /// The actor is stale or database reads or JSON encoding fail.
    pub async fn context_value(
        &self,
        actor: &Mailbox,
        work_after: String,
        mail_after: i64,
    ) -> Result<Value> {
        let view = self.recovery_view(actor, work_after, mail_after).await?;
        self.retrieved(actor, &view.mail, &view.work).await?;
        Ok(view.value)
    }

    /// Assemble context without claiming that its sources were returned.
    pub(crate) async fn recovery_view(
        &self,
        actor: &Mailbox,
        work_after: String,
        mail_after: i64,
    ) -> Result<RecoveryView> {
        let group = &actor.group_name;
        let work = self.work_list(actor, &work_after).await?;
        let mail = self.inbox(actor, mail_after).await?;
        let mut works = Vec::new();
        let mut mails = Vec::new();
        let mut next_work = work_after.clone();
        let mut next_mail = mail_after;
        for item in work.iter().take(5) {
            works.push(item);
            let candidate = json!({"group":group,"work":works,"mail":mails,"work_more":true,"mail_more":true,"next_work_after":item.id,"next_mail_after":next_mail});
            if serde_json::to_vec(&candidate)?.len() > 3400 {
                works.pop();
                break;
            }
            next_work = item.id.clone();
        }
        for item in mail.iter().take(5) {
            mails.push(item);
            let candidate = json!({"group":group,"work":works,"mail":mails,"work_more":true,"mail_more":true,"next_work_after":next_work,"next_mail_after":item.id});
            if serde_json::to_vec(&candidate)?.len() > 3400 {
                mails.pop();
                break;
            }
            next_mail = item.id;
        }
        let peers = self.peers_status().await?;
        let stale_peers: Vec<_> = peers
            .iter()
            .filter(|p| p.queued > 0 || p.last_error.is_some())
            .take(3)
            .map(|p| json!({"machine_id":p.machine_id,"queued":p.queued,"last_sync":p.last_sync}))
            .collect();
        let (outbox_pending, outbox_oldest) = self.outbox_status().await?;
        let followups = self.attention_list(actor, 0).await?;
        let mut checkpoints = Vec::new();
        for item in &works {
            let plan = self.source_followup(actor, Some(&item.id), None).await?;
            checkpoints.push(json!({"task":item.id,"version":plan["version"],"next_check_at":plan["next_check"],"checkpoint_recorded":!plan["checkpoint"].is_null(),"details":"task show"}));
        }
        for item in &mails {
            let plan = self.source_followup(actor, None, Some(item.id)).await?;
            checkpoints.push(json!({"mail":item.id,"version":plan["version"],"next_check_at":plan["next_check"],"checkpoint_recorded":!plan["checkpoint"].is_null(),"details":"mail show"}));
        }
        let mut value = json!({"group":group,"followups":followups,"checkpoints":checkpoints,"checkpoints_more":false,"checkpoint_help":"Use task/mail checkpoint when yielding with unfinished work; show fetches full metadata; attention list pages pending follow-ups", "work":works,"mail":mails,"work_more":work.len()>works.len(),"mail_more":mail.len()>mails.len(),"next_work_after":next_work,"next_mail_after":next_mail,"stale_peers":stale_peers,"outbox_pending":outbox_pending,"outbox_oldest":outbox_oldest});
        // Account for metadata too. Do not receipt any record trimmed from the response.
        while serde_json::to_vec(&value)?.len() > 4096 {
            let attention = value["followups"]["items"]
                .as_array_mut()
                .expect("attention array");
            if attention.len() > 1 {
                attention.pop();
                let last = attention.last().expect("remaining attention")["id"].clone();
                value["followups"]["next_after"] = last;
                value["followups"]["more"] = json!(true);
                continue;
            }
            if value["checkpoints"]
                .as_array_mut()
                .expect("checkpoint array")
                .pop()
                .is_some()
            {
                value["checkpoints_more"] = json!(true);
                continue;
            }
            let mail_size = mails
                .last()
                .map(serde_json::to_vec)
                .transpose()?
                .map_or(0, |v| v.len());
            let work_size = works
                .last()
                .map(serde_json::to_vec)
                .transpose()?
                .map_or(0, |v| v.len());
            anyhow::ensure!(
                mail_size + work_size > 0,
                "recovery metadata exceeds byte budget"
            );
            if mail_size >= work_size {
                mails.pop();
                value["mail"] = json!(mails);
                value["mail_more"] = json!(true);
                value["next_mail_after"] = json!(mails.last().map_or(mail_after, |m| m.id));
            } else {
                works.pop();
                value["work"] = json!(works);
                value["work_more"] = json!(true);
                value["next_work_after"] =
                    json!(works.last().map_or(work_after.as_str(), |w| w.id.as_str()));
            }
        }
        Ok(RecoveryView {
            value,
            mail: mails.iter().map(|m| m.id).collect(),
            work: works.iter().map(|w| (w.id.clone(), w.version)).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        states::TaskState,
        work::{WorkDraft, WorkPatch, WorkUpdate},
    };

    #[tokio::test]
    async fn deferred_context_receipts_preserve_newer_task_revisions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "worker", false).await?;
        let writer = store.mailbox("g", "writer").await?;
        let actor = store.mailbox("g", "worker").await?;
        let now = crate::now()?;
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: "revision".into(),
                    scope: "Review".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "First action".into(),
                    deadline: None,
                    evidence: vec![],
                },
                now,
            )
            .await?;
        let view = store.recovery_view(&actor, String::new(), 0).await?;
        assert_eq!(view.value["work"][0]["version"], 1);
        store
            .update_work(
                &writer,
                "revision",
                WorkUpdate {
                    version: 1,
                    patch: WorkPatch {
                        next_action: Some("Second action".into()),
                        ..Default::default()
                    },
                    reason: "New evidence".into(),
                    resolve_message: None,
                },
                now + 1,
            )
            .await?;
        let mut tx = store.pool().begin().await?;
        Store::lock_actor(&mut tx, &actor).await?;
        Store::retrieved_tx(&mut tx, &actor, &view.mail, &view.work).await?;
        tx.commit().await?;
        let current = store
            .source_followup(&actor, Some("revision"), None)
            .await?;
        assert!(
            current["retrieved_at"].is_null(),
            "old rendered revision cannot receipt current work"
        );
        let current_view = store.context_value(&actor, String::new(), 0).await?;
        assert_eq!(current_view["work"][0]["version"], 2);
        assert!(
            !store
                .source_followup(&actor, Some("revision"), None)
                .await?["retrieved_at"]
                .is_null()
        );
        Ok(())
    }
}
