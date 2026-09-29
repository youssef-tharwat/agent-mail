//! Bounded recovery view shared by CLI and automatic client hooks.
use crate::store::{Mailbox, Store};
use anyhow::Result;
use serde_json::{Value, json};
impl Store {
    pub async fn context_value(
        &self,
        actor: &Mailbox,
        work_after: String,
        mail_after: i64,
    ) -> Result<Value> {
        let group = &actor.group_name;
        let work = self.work_list(actor, &work_after).await?;
        let mail = self.inbox(actor, mail_after).await?;
        let mut works = Vec::new();
        let mut mails = Vec::new();
        let mut next_work = work_after;
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
        Ok(
            json!({"group":group,"work":works,"mail":mails,"work_more":work.len()>works.len(),"mail_more":mail.len()>mails.len(),"next_work_after":next_work,"next_mail_after":next_mail,"stale_peers":stale_peers,"outbox_pending":outbox_pending,"outbox_oldest":outbox_oldest}),
        )
    }
}
