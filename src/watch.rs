//! Bounded change batches, identity-scoped cursors and event-driven request waits.
use crate::{
    states::{EventKind, MessageState},
    store::{Mailbox, Store},
    stream::{self, Frame},
};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::{collections::BTreeMap, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};

/// A record identifier and the latest event revision in a batch.
#[derive(Debug, Serialize)]
pub struct Change {
    /// Mail or task identifier; details are fetched separately.
    pub id: String,
    /// Revision carried by the durable event (mail uses a delivery identifier).
    pub revision: Option<i64>,
}
/// Compact notifications grouped by business record type.
#[derive(Debug, Default, Serialize)]
pub struct Changes {
    /// Newly received requests.
    pub new_mail: Vec<Change>,
    /// Replies, resolutions or withdrawals on existing requests.
    pub mail_updates: Vec<Change>,
    /// Scheduled follow-through occurrences; fetch with attention show.
    pub followups: Vec<Change>,
    /// Changed assignments.
    pub tasks: Vec<Change>,
}
impl Changes {
    pub(crate) fn collect(events: impl IntoIterator<Item = (EventKind, String, i64)>) -> Self {
        let mut new_mail = BTreeMap::new();
        let mut mail_updates = BTreeMap::new();
        let mut tasks = BTreeMap::new();
        let mut followups = BTreeMap::new();
        for (kind, id, revision) in events {
            match kind {
                EventKind::AttentionDue => {
                    followups.insert(id, revision);
                }
                EventKind::WorkChanged => {
                    tasks.insert(id, revision);
                }
                EventKind::MailPending => {
                    new_mail.insert(id, ());
                }
                EventKind::MailChanged => {
                    mail_updates.insert(id, ());
                }
            }
        }
        Self {
            followups: followups
                .into_iter()
                .map(|(id, revision)| Change {
                    id,
                    revision: Some(revision),
                })
                .collect(),
            new_mail: new_mail
                .into_iter()
                .map(|(id, ())| Change { id, revision: None })
                .collect(),
            mail_updates: mail_updates
                .into_iter()
                .map(|(id, ())| Change { id, revision: None })
                .collect(),
            tasks: tasks
                .into_iter()
                .map(|(id, revision)| Change {
                    id,
                    revision: Some(revision),
                })
                .collect(),
        }
    }
}
/// One bounded batch. Save its cursor after handling the batch to resume safely.
#[derive(Debug, Serialize)]
pub struct Batch {
    /// Identity-scoped exclusive replay cursor; not a credential.
    pub cursor: String,
    /// Changed IDs only, never request bodies or task scopes.
    pub changes: Changes,
}
/// A private subscription with bounded buffering and automatic reconnect.
#[derive(Debug)]
pub struct Watch {
    receiver: mpsc::Receiver<Result<Frame>>,
    task: JoinHandle<()>,
    prefix: String,
    after: i64,
    buffered: Vec<Frame>,
    error: Option<anyhow::Error>,
}
impl Drop for Watch {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Store {
    /// Capture a race-safe starting cursor for this agent's committed events.
    /// # Errors
    /// Identity is stale or the database query fails.
    pub async fn change_position(&self, actor: &Mailbox) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let row = sqlx::query!("SELECT COALESCE(MAX(id),0) AS 'position!: i64' FROM coordination_events WHERE recipient=?",actor.id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(row.position)
    }
    /// Subscribe to changes, optionally replaying from a previously emitted cursor.
    /// A fresh subscription starts now; use context first for existing obligations.
    /// # Errors
    /// The cursor belongs to another store, agent or generation, or the worker is unavailable.
    pub async fn watch(&self, actor: &Mailbox, cursor: Option<&str>) -> Result<Watch> {
        let prefix = format!(
            "am1:{}:{}:{}:",
            self.machine_id().await?,
            actor.id,
            actor.binding_version
        );
        let after = match cursor {
            Some(cursor) => {
                let after = cursor.strip_prefix(&prefix).context("cursor belongs to another store, agent or binding; recover context and start a fresh watch")?.parse::<i64>().context("invalid watch cursor")?;
                ensure!(
                    after >= 0 && after <= self.change_position(actor).await?,
                    "watch cursor is outside this agent's event history"
                );
                after
            }
            None => self.change_position(actor).await?,
        };
        Watch::start(self.clone(), actor.clone(), prefix, after).await
    }
}
impl Watch {
    async fn start(store: Store, actor: Mailbox, prefix: String, after: i64) -> Result<Self> {
        let mut reader = stream::connect(&store, &actor, after).await?;
        match stream::next(&mut reader).await? {
            Frame::Ready {
                version: 2,
                participant,
                binding_version,
            } if participant == actor.name && binding_version == actor.binding_version => {}
            Frame::Ready { .. } => {
                anyhow::bail!("event stream identity did not match the current agent")
            }
            Frame::Error { message } => anyhow::bail!("{message}"),
            _ => anyhow::bail!("event stream did not authenticate"),
        }
        let (sender, receiver) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut position = after;
            loop {
                match stream::next(&mut reader).await {
                    Ok(Frame::Event {
                        id,
                        version,
                        participant,
                        binding_version,
                        kind,
                        subject,
                        revision,
                    }) => {
                        position = id;
                        if sender
                            .send(Ok(Frame::Event {
                                id,
                                version,
                                participant,
                                binding_version,
                                kind,
                                subject,
                                revision,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Frame::Ready {
                        version: 2,
                        participant,
                        binding_version,
                    }) if participant == actor.name && binding_version == actor.binding_version => {
                    }
                    Ok(Frame::Ready { .. }) => {
                        let _ = sender.send(Err(anyhow::anyhow!("event stream identity changed; recover context before watching again"))).await;
                        return;
                    }
                    Ok(Frame::Error { message }) => {
                        let _ = sender.send(Err(anyhow::anyhow!(message))).await;
                        return;
                    }
                    Err(_) => {
                        // Retry transport loss with bounded backoff; never poll agent state in the client.
                        let mut delay = Duration::from_millis(250);
                        loop {
                            if sender.is_closed() {
                                return;
                            }
                            tokio::time::sleep(delay).await;
                            if let Ok(connection) = stream::connect(&store, &actor, position).await
                            {
                                reader = connection;
                                break;
                            }
                            delay = (delay * 2).min(Duration::from_secs(5));
                        }
                    }
                }
            }
        });
        Ok(Self {
            receiver,
            task,
            prefix,
            after,
            buffered: Vec::new(),
            error: None,
        })
    }
    /// Current resume cursor. A fresh watch emits this before its first change.
    pub fn cursor(&self) -> String {
        format!("{}{}", self.prefix, self.after)
    }
    /// Wait for at most 32 events, coalesced within a 75ms window.
    /// # Errors
    /// The identity changed, the protocol failed, or the reader terminated.
    pub async fn next(&mut self) -> Result<Batch> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        if self.buffered.is_empty() {
            self.buffered.push(
                self.receiver
                    .recv()
                    .await
                    .context("watch reader stopped")??,
            );
        }
        let end = tokio::time::Instant::now() + Duration::from_millis(75);
        while self.buffered.len() < 32 {
            match tokio::time::timeout_at(end, self.receiver.recv()).await {
                Ok(Some(Ok(frame))) => self.buffered.push(frame),
                Ok(Some(Err(error))) => {
                    self.error = Some(error);
                    break;
                }
                Ok(None) | Err(_) => break,
            }
        }
        let events = std::mem::take(&mut self.buffered);
        let mut changes = Vec::new();
        for frame in events {
            if let Frame::Event {
                id,
                kind,
                subject,
                revision,
                ..
            } = frame
            {
                self.after = id;
                changes.push((kind.parse()?, subject, revision));
            }
        }
        Ok(Batch {
            cursor: self.cursor(),
            changes: Changes::collect(changes),
        })
    }
}
/// Per-recipient disposition of a request sent by the caller.
#[derive(Debug, Serialize)]
pub struct RecipientOutcome {
    /// Address within the caller's group.
    pub agent: String,
    /// Business disposition, independent of stream delivery.
    pub state: MessageState,
    /// Reply to fetch with mail show; absent for resolution without a reply.
    pub reply_id: Option<i64>,
}
/// Why a request wait ended.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitOutcome {
    /// At least one recipient sent a reply.
    Reply,
    /// Every recipient resolved or withdrew without sending a reply.
    Settled,
    /// Business deadline or caller timeout reached; request remains unchanged.
    Deadline,
}
/// Bounded outcome returned by mail wait.
#[derive(Debug, Serialize)]
pub struct WaitResult {
    /// Original outgoing request ID.
    pub id: i64,
    /// Wait completion reason, never a business mutation.
    pub outcome: WaitOutcome,
    /// Total recipient count.
    pub recipients: i64,
    /// Recipients still owing a disposition.
    pub pending: i64,
    /// Number of recipients that sent a reply.
    pub replies: i64,
    /// First 32 recipient dispositions in address order.
    pub items: Vec<RecipientOutcome>,
    /// Additional dispositions can be inspected with mail wait after completion.
    pub more: bool,
}
impl Store {
    async fn request_outcome(&self, actor: &Mailbox, id: i64) -> Result<(WaitResult, Option<i64>)> {
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let request = sqlx::query!(
            "SELECT deadline AS \"deadline?\" FROM messages WHERE id=? AND sender=?",
            id,
            actor.id
        )
        .fetch_optional(&mut *tx)
        .await?
        .context("request not found among this agent's sent messages")?;
        let counts = sqlx::query!("SELECT COUNT(*) AS 'recipients!: i64',COALESCE(SUM(state='pending'),0) AS 'pending!: i64',COALESCE(SUM(reply_id IS NOT NULL),0) AS 'replies!: i64' FROM deliveries WHERE message=?",id).fetch_one(&mut *tx).await?;
        let items = sqlx::query_as!(RecipientOutcome,"SELECT b.name AS agent,d.state AS 'state: MessageState',d.reply_id FROM deliveries d JOIN mailboxes b ON b.id=d.recipient WHERE d.message=? ORDER BY b.name LIMIT 32",id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok((
            WaitResult {
                id,
                outcome: if counts.replies > 0 {
                    WaitOutcome::Reply
                } else {
                    WaitOutcome::Settled
                },
                recipients: counts.recipients,
                pending: counts.pending,
                replies: counts.replies,
                more: counts.recipients > 32,
                items,
            },
            request.deadline,
        ))
    }
    /// Wait for the first reply, all non-reply dispositions, or the earlier deadline/timeout.
    /// No client database polling; committed events drive rechecks and reconnect replay.
    /// # Errors
    /// Caller does not own the sent request, identity changes or worker is unavailable while pending.
    pub async fn wait_mail(
        &self,
        actor: &Mailbox,
        id: i64,
        timeout: Option<Duration>,
    ) -> Result<WaitResult> {
        let position = self.change_position(actor).await?;
        let (result, due) = self.request_outcome(actor, id).await?;
        if result.pending == 0 || result.replies > 0 {
            return Ok(result);
        }
        let business_deadline = match due {
            Some(due) => Some(Duration::from_secs(
                u64::try_from(due.saturating_sub(crate::now()?)).unwrap_or(0),
            )),
            None => None,
        };
        let duration = match (timeout, business_deadline) {
            (Some(limit), Some(deadline)) => Some(limit.min(deadline)),
            (Some(limit), None) => Some(limit),
            (None, deadline) => deadline,
        };
        if duration.is_some_and(|duration| duration.is_zero()) {
            return Ok(WaitResult {
                outcome: WaitOutcome::Deadline,
                ..result
            });
        }
        let end = duration.map(|duration| tokio::time::Instant::now() + duration);
        let prefix = format!(
            "am1:{}:{}:{}:",
            self.machine_id().await?,
            actor.id,
            actor.binding_version
        );
        let mut watch = Watch::start(self.clone(), actor.clone(), prefix, position).await?;
        // Recheck after subscribing: a racing resolution is visible even before its event arrives.
        loop {
            let (result, _) = self.request_outcome(actor, id).await?;
            if result.pending == 0 || result.replies > 0 {
                return Ok(result);
            }
            tokio::select! {
                batch = watch.next() => { batch?; }
                _ = async { match end { Some(end) => tokio::time::sleep_until(end).await, None => std::future::pending().await } } => {
                    let (mut result, _) = self.request_outcome(actor, id).await?;
                    if result.pending != 0 { result.outcome = WaitOutcome::Deadline; }
                    return Ok(result);
                }
            }
        }
    }
}
