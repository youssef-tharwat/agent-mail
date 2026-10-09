//! Shared native wake routing with durable retry and receipt policy.
//!
//! Codex and Claude attachments belong to a standalone binding generation. A wake
//! reserves its retry budget before I/O and records confirmed queue acceptance in a
//! transaction. Mail and work remain unresolved until an explicit business decision.
//! Endpoint replacement invalidates stale delivery attempts.

use crate::states::DeliveryState;
use crate::{
    diagnostics::{Operation, Phase},
    identity::Binding,
    service::Observation,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

use crate::states::NativeRuntime;
enum State {
    Idle,
    Active,
    Unavailable,
}
enum Peer {
    ClaudeInbox {
        socket: PathBuf,
        endpoint: crate::claude_inbox::Endpoint,
    },
    Codex {
        client: Box<crate::codex::Client>,
        thread: Uuid,
        turn: Option<String>,
    },
    Claude {
        socket: PathBuf,
        status: crate::claude::Status,
    },
}
impl Peer {
    async fn connect(kind: NativeRuntime, socket: &Path, thread: Uuid) -> Result<Self> {
        Ok(match kind {
            NativeRuntime::Codex => Self::Codex {
                client: Box::new(crate::codex::Client::connect(socket).await?),
                thread,
                turn: None,
            },
            NativeRuntime::Claude => Self::Claude {
                socket: socket.into(),
                status: crate::claude::probe(socket, thread).await?,
            },
        })
    }
    async fn state(&mut self) -> Result<State> {
        Ok(match self {
            Self::ClaudeInbox { endpoint, .. } => match endpoint.activity {
                crate::states::InboxActivity::Active => State::Active,
                crate::states::InboxActivity::Idle => State::Idle,
                crate::states::InboxActivity::Ended => State::Unavailable,
            },
            Self::Codex { client, thread, .. } => match client.thread(*thread).await?.status {
                crate::codex::ThreadStatus::Idle => State::Idle,
                crate::codex::ThreadStatus::Active => State::Active,
                _ => State::Unavailable,
            },
            Self::Claude { status, .. } => {
                if !status.ready {
                    State::Unavailable
                } else if status.active {
                    State::Active
                } else {
                    State::Idle
                }
            }
        })
    }
    async fn prepare(&mut self, active: bool) -> Result<()> {
        if active {
            if let Self::Codex {
                client,
                thread,
                turn,
            } = self
            {
                let result = client
                    .call(
                        "thread/read",
                        json!({"threadId":thread,"includeTurns":true}),
                    )
                    .await?;
                *turn = Some(
                    result["thread"]["turns"]
                        .as_array()
                        .context("active turn unavailable")?
                        .iter()
                        .rev()
                        .find(|t| t["status"] == "inProgress")
                        .and_then(|t| t["id"].as_str())
                        .context("active turn changed")?
                        .into(),
                );
            }
        }
        Ok(())
    }
    async fn send(
        &mut self,
        thread: Uuid,
        text: String,
        active: bool,
        message_id: String,
    ) -> Result<()> {
        match self {
            Self::Codex { client, turn, .. } => {
                if let Some(turn) = turn {
                    client.call("turn/steer",json!({"threadId":thread,"expectedTurnId":turn,"input":[{"type":"text","text":text}]})).await?;
                } else {
                    let receipt = client
                        .call(
                            "thread/queue/add",
                            json!({"threadId":thread,"clientUserMessageId":message_id,"input":[{"type":"text","text":text}]}),
                        )
                        .await?;
                    ensure!(
                        receipt["queuedSubmission"]["id"].as_str().is_some(),
                        "native queue receipt missing"
                    );
                }
                Ok(())
            }
            Self::ClaudeInbox { socket, endpoint } => {
                crate::claude_inbox::send(socket, thread, &endpoint.token, &message_id, active)
                    .await
            }
            Self::Claude { socket, status } => {
                ensure!(status.active == active, "Claude state changed");
                crate::claude::deliver(socket, thread, status, text).await
            }
        }
    }
}
/// Probe a configured runtime endpoint without starting a turn.
///
/// # Errors
/// The runtime is unavailable, its identity is invalid, or protocol checks fail.
pub async fn probe(kind: NativeRuntime, socket: &Path, thread: Uuid) -> Result<Value> {
    match kind {
        NativeRuntime::Codex => crate::codex::probe(socket, thread).await,
        NativeRuntime::Claude => {
            let status = crate::claude::probe(socket, thread).await?;
            Ok(
                json!({"client":status.client,"ready":status.ready,"persistent":true,"state":if !status.ready{"not_loaded"}else if status.active{"active"}else{"idle"},"safe_queue":status.ready,"receipt":"queue_accepted"}),
            )
        }
    }
}
impl Store {
    /// Attach a verified Codex thread to a standalone binding generation.
    ///
    /// Errors if the endpoint is unavailable, already assigned, or not persistent.
    ///
    /// # Errors
    /// The binding, endpoint, thread, or assignment is invalid, or probing or persistence fails.
    pub async fn attach_codex(&self, actor: &Mailbox, socket: &Path, thread: Uuid) -> Result<()> {
        self.attach_native(actor, socket, thread, NativeRuntime::Codex, None)
            .await
    }
    /// Attach a verified Claude session to a standalone binding generation.
    ///
    /// # Errors
    /// The binding, endpoint, session, or assignment is invalid, or probing or persistence fails.
    pub async fn attach_claude(&self, actor: &Mailbox, socket: &Path, thread: Uuid) -> Result<()> {
        self.attach_native(actor, socket, thread, NativeRuntime::Claude, None)
            .await
    }
    /// Attach the current launcher session without overriding a persisted delivery pause.
    pub async fn attach_codex_from_hook(
        &self,
        actor: &Mailbox,
        socket: &Path,
        thread: Uuid,
        launch: &str,
    ) -> Result<()> {
        self.attach_native(actor, socket, thread, NativeRuntime::Codex, Some(launch))
            .await
    }
    async fn attach_native(
        &self,
        actor: &Mailbox,
        socket: &Path,
        thread: Uuid,
        kind: NativeRuntime,
        launch: Option<&str>,
    ) -> Result<()> {
        ensure!(
            matches!(actor.binding, Binding::Standalone { .. }),
            "Runtime wake requires a standalone participant"
        );
        ensure!(socket.is_absolute(), "socket path must be absolute");
        let socket = socket.to_str().context("socket must be UTF-8")?;
        if launch.is_some() {
            let thread_text = thread.to_string();
            let same = sqlx::query!("SELECT recipient FROM runtime_wakes WHERE recipient=? AND binding_version=? AND runtime='codex' AND socket=? AND thread=?",actor.id,actor.binding_version,socket,thread_text).fetch_optional(self.pool()).await?;
            // Existing attachment needs no network roundtrip at every tool boundary.
            // Delivery and status diagnostics independently probe endpoint health.
            if same.is_some() {
                return Ok(());
            }
        }
        let info = probe(kind, Path::new(socket), thread).await?;
        ensure!(
            info["ready"] != false,
            "runtime is not ready; initialize its native session first"
        );
        let runtime = kind.as_str();
        let thread = thread.to_string();
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if let Some(launch) = launch {
            let current = sqlx::query!("SELECT recipient FROM runtime_readiness WHERE recipient=? AND binding_version=? AND launch=? AND client_session=? AND NOT EXISTS(SELECT 1 FROM runtime_policy WHERE recipient=? AND binding_version=? AND enabled=0)",actor.id,actor.binding_version,launch,thread,actor.id,actor.binding_version).fetch_optional(&mut *tx).await?;
            if current.is_none() {
                tx.commit().await?;
                return Ok(());
            }
        } else {
            sqlx::query!("DELETE FROM runtime_readiness WHERE recipient=?", actor.id)
                .execute(&mut *tx)
                .await?;
            sqlx::query!("INSERT INTO runtime_policy(recipient,binding_version,enabled) VALUES(?,?,1) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,enabled=1",actor.id,actor.binding_version).execute(&mut *tx).await?;
        }
        // Repeating identical attachment preserves its cursor and retry budget.
        sqlx::query!("INSERT INTO runtime_wakes(recipient,binding_version,socket,thread,runtime) VALUES (?,?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,socket=excluded.socket,thread=excluded.thread,runtime=excluded.runtime,scanned=0,delivered=0,attempted=0,attempts=0,next_attempt=0 WHERE runtime_wakes.binding_version<>excluded.binding_version OR runtime_wakes.socket<>excluded.socket OR runtime_wakes.thread<>excluded.thread OR runtime_wakes.runtime<>excluded.runtime",
            actor.id, actor.binding_version, socket, thread, runtime).execute(&mut *tx).await?;
        sqlx::query!("DELETE FROM claude_inboxes WHERE recipient=?", actor.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    /// Persist an operator's delivery preference for the current binding.
    ///
    /// # Errors
    /// The actor is stale or storage fails.
    pub async fn set_runtime_enabled(&self, actor: &Mailbox, enabled: bool) -> Result<()> {
        ensure!(
            !matches!(actor.binding, Binding::Remote { .. }),
            "delivery preferences require a local participant"
        );
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("INSERT INTO runtime_policy(recipient,binding_version,enabled) VALUES(?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,enabled=excluded.enabled",actor.id,actor.binding_version,enabled).execute(&mut *tx).await?;
        if !enabled {
            sqlx::query!("DELETE FROM runtime_wakes WHERE recipient=?", actor.id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    /// Whether automatic attachment and delivery are permitted for this binding.
    ///
    /// # Errors
    /// Reading the persisted preference fails.
    pub async fn runtime_enabled(&self, actor: &Mailbox) -> Result<bool> {
        Ok(sqlx::query!(
            "SELECT CASE WHEN m.agent_state='retired' THEN 0 ELSE COALESCE(p.enabled,1) END AS 'enabled!: i64' FROM mailboxes m LEFT JOIN runtime_policy p ON p.recipient=m.id AND p.binding_version=m.binding_version WHERE m.id=? AND m.binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(self.pool())
        .await?
        .context("agent binding changed; recover the current identity")?.enabled != 0)
    }
    /// Report explicit delivery preferences without exposing credentials.
    ///
    /// # Errors
    /// Querying current binding preferences fails.
    pub async fn runtime_policy_status(&self) -> Result<Value> {
        let rows=sqlx::query!("SELECT b.group_name,b.name,p.enabled FROM runtime_policy p JOIN mailboxes b ON b.id=p.recipient AND b.binding_version=p.binding_version ORDER BY b.group_name,b.name").fetch_all(self.pool()).await?;
        Ok(Value::Array(
            rows.into_iter()
                .map(
                    |r| json!({"group":r.group_name,"participant":r.name,"enabled":r.enabled != 0}),
                )
                .collect(),
        ))
    }
    /// Report configured native endpoints and their durable retry budgets.
    ///
    /// # Errors
    /// The database query fails.
    pub async fn native_status(&self) -> Result<Value> {
        let rows = sqlx::query!("SELECT m.group_name,m.name,c.runtime,c.binding_version,m.binding_version AS current_version,c.socket,c.thread,c.scanned,c.delivered,c.attempted,c.attempts,c.next_attempt,i.activity AS inbox_activity FROM runtime_wakes c JOIN mailboxes m ON m.id=c.recipient LEFT JOIN claude_inboxes i ON i.recipient=c.recipient").fetch_all(self.pool()).await?;
        Ok(Value::Array(rows.into_iter().map(|r| json!({"group":r.group_name,"participant":r.name,"runtime":r.runtime,"transport":if r.inbox_activity.is_some(){"inbox"}else{"native"},"activity":r.inbox_activity,"socket":r.socket,"thread":r.thread,"binding_current":r.binding_version==r.current_version,"delivered_through":r.delivered,"attempted_through":r.attempted,"attempts":r.attempts,"next_attempt":r.next_attempt,"delivery_unconfirmed":r.attempted>r.scanned && r.attempts>0,"delivery_detail":if r.attempted>r.scanned && r.attempts>0 {Some("Reserved wake has no confirmed receipt; execution may have started")}else{None}})).collect()))
    }
    pub(crate) async fn has_native(&self, actor: &Mailbox) -> Result<bool> {
        Ok(sqlx::query!(
            "SELECT recipient FROM runtime_wakes WHERE recipient=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(self.pool())
        .await?
        .is_some())
    }
}

pub(crate) async fn tick(store: &Store, time: i64) -> Result<Vec<Observation>> {
    let targets = sqlx::query!("SELECT m.group_name,m.name FROM runtime_wakes c JOIN mailboxes m ON m.id=c.recipient AND m.binding_version=c.binding_version").fetch_all(store.pool()).await?;
    let mut observations = Vec::new();
    for target in targets {
        let actor = store.mailbox(&target.group_name, &target.name).await?;
        let (state, detail) = match tokio::time::timeout(
            Duration::from_secs(5),
            deliver(store, &actor, time),
        )
        .await
        {
            Ok(Ok(state)) => (state, None),
            Ok(Err(error)) => (DeliveryState::Uncertain, Some(format!("{error:#}"))),
            Err(_) => (DeliveryState::TimedOut, None),
        };
        observations.push(Observation {
            group: target.group_name,
            participant: target.name,
            state,
            detail,
        });
    }
    Ok(observations)
}

async fn deliver(store: &Store, actor: &Mailbox, time: i64) -> Result<DeliveryState> {
    let Some(_wake_lock) = crate::service::wake_lock(store.root(), actor.id)? else {
        return Ok(DeliveryState::Ineligible);
    };
    let group = store.group(&actor.group_name).await?;
    if group.paused != 0 {
        return Ok(DeliveryState::Paused);
    }
    let Some(endpoint) = sqlx::query!("SELECT runtime,socket,thread,scanned,delivered,attempted,attempts,next_attempt FROM runtime_wakes WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).fetch_optional(store.pool()).await? else { return Ok(DeliveryState::BindingChanged); };
    let latest = sqlx::query!(
        "SELECT COALESCE(MAX(id),0) AS \"id!: i64\" FROM coordination_events WHERE recipient=?",
        actor.id
    )
    .fetch_one(store.pool())
    .await?
    .id;
    let snapshot = store.attention_snapshot(actor).await?;
    if latest <= endpoint.scanned && snapshot.items.is_empty() {
        return Ok(DeliveryState::Settled);
    }
    let candidates = store.attention_candidates(actor, time).await?;
    let actionable = candidates
        .iter()
        .any(|i| i.reason != crate::states::AttentionReason::StopWork);
    let cancellation = candidates
        .iter()
        .any(|i| i.reason == crate::states::AttentionReason::StopWork);
    if !actionable && !cancellation {
        if !snapshot.items.is_empty() {
            return Ok(DeliveryState::Waiting);
        }
        store.scan_passive(actor, latest).await?;
        return Ok(DeliveryState::Passive);
    }
    // A new event must not bypass a reserved or ambiguous attempt.
    if endpoint.attempts > 0 && endpoint.next_attempt > time && !cancellation {
        return Ok(DeliveryState::Waiting);
    }
    let thread = Uuid::parse_str(&endpoint.thread)?;
    let kind = endpoint.runtime.parse::<NativeRuntime>()?;
    let mut peer = if let Some(inbox) = store.claude_inbox(actor).await? {
        crate::claude_inbox::verify(store, Path::new(&endpoint.socket), &inbox)?;
        Peer::ClaudeInbox {
            socket: endpoint.socket.clone().into(),
            endpoint: inbox,
        }
    } else {
        Peer::connect(kind, Path::new(&endpoint.socket), thread).await?
    };
    let inbox = matches!(peer, Peer::ClaudeInbox { .. });
    let active = match peer.state().await? {
        State::Active if cancellation => true,
        State::Idle if actionable => false,
        State::Idle => {
            store.scan_passive(actor, latest).await?;
            return Ok(DeliveryState::Passive);
        }
        _ => return Ok(DeliveryState::Busy),
    };
    peer.prepare(active).await?;
    let Some(batch) = store
        .claim_attention(actor, crate::names::DeliveryConsumer::Native, time)
        .await?
    else {
        return Ok(DeliveryState::Ineligible);
    };
    // Queue identity belongs to this exact reservation, including its receipt token.
    // A cursor can remain unchanged across distinct pages and retry reservations.
    let message_id = if inbox {
        batch.token.clone()
    } else {
        format!(
            "agent-mail-{}-{}-{}-{}",
            actor.id, actor.binding_version, endpoint.thread, batch.token
        )
    };
    let latest = batch
        .attention
        .items
        .iter()
        .map(|i| i.event)
        .max()
        .context("empty attention batch")?;
    // Persist the attempt before I/O. A crash or lost response consumes its budget.
    let mut tx = store.pool().begin().await?;
    Store::lock_actor(&mut tx, actor).await?;
    let next = batch.expires;
    let reserved = sqlx::query!("UPDATE runtime_wakes SET attempts=CASE WHEN attempted=? THEN attempts+1 ELSE 1 END, attempted=?,next_attempt=? WHERE recipient=? AND binding_version=? AND socket=? AND thread=? AND EXISTS(SELECT 1 FROM groups WHERE name=? AND paused=0) AND EXISTS(SELECT 1 FROM attention_dispatch WHERE recipient=? AND binding_version=? AND token=?)",latest,latest,next,actor.id,actor.binding_version,endpoint.socket,endpoint.thread,actor.group_name,actor.id,actor.binding_version,batch.token).execute(&mut *tx).await?.rows_affected();
    if reserved != 0 && inbox {
        sqlx::query!(
            "UPDATE claude_inboxes SET pending_id=?,pending_event=?,pending_attention=? WHERE recipient=?",
            message_id,
            latest,
            batch.token,
            actor.id
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    if reserved == 0 {
        return Ok(DeliveryState::Ineligible);
    }
    let challenge = match store.reserve_delivery_challenge(actor, time).await {
        Ok(challenge) => challenge,
        Err(error) => {
            eprintln!(
                "agent-mail: could not attach delivery check to {}/{} notification: {error:#}",
                actor.group_name, actor.name
            );
            None
        }
    };
    let mut text = Store::attention_text(&batch, true)?;
    if let Some(nonce) = challenge.as_deref() {
        text.push('\n');
        text.push_str(&crate::verification::challenge(actor, nonce));
    }
    // Hold the binding lock during the bounded send: replacement/detachment cannot race it.
    let operation = Operation::NativeDelivery;
    let mut tx = store.delivery_transaction(actor, operation).await?;
    let still_attached = sqlx::query!("SELECT recipient FROM runtime_wakes WHERE recipient=? AND binding_version=? AND socket=? AND thread=? AND EXISTS(SELECT 1 FROM groups WHERE name=? AND paused=0) AND EXISTS(SELECT 1 FROM attention_dispatch WHERE recipient=? AND token=?)",actor.id,actor.binding_version,endpoint.socket,endpoint.thread,actor.group_name,actor.id,batch.token).fetch_optional(&mut **tx).await?.is_some();
    ensure!(still_attached, "Runtime endpoint changed before delivery");
    if !Store::validate_attention_tx(&mut tx, actor, &batch).await? {
        tx.commit().await?;
        return Ok(DeliveryState::StateChanged);
    }
    if let Peer::ClaudeInbox { socket, endpoint } = &peer {
        crate::claude_inbox::verify(store, socket, endpoint)?;
    }
    store
        .diagnostics()
        .measure(
            operation,
            Phase::Transport,
            peer.send(thread, text, active, message_id),
        )
        .await?;
    if let Some(nonce) = challenge.as_deref() {
        sqlx::query!(
            "UPDATE delivery_probes SET transport_accepted_at=COALESCE(transport_accepted_at,?),healthy_at=?,failed=0 WHERE recipient=? AND binding_version=? AND nonce=?",
            time,
            time,
            actor.id,
            actor.binding_version,
            nonce
        )
        .execute(&mut **tx)
        .await?;
    }
    if inbox {
        tx.commit().await?;
        return Ok(DeliveryState::AwaitingReceipt);
    }
    sqlx::query!(
        "UPDATE runtime_wakes SET delivered=? WHERE recipient=?",
        latest,
        actor.id
    )
    .execute(&mut **tx)
    .await?;
    // Queue acceptance is not ingestion. Keep the reservation and bounded retry
    // state until explicit batch acknowledgement or record retrieval.
    tx.commit().await?;
    Ok(DeliveryState::Queued)
}

/// Dispatch a verification challenge through the exact native wake transport.
pub(crate) async fn send_verification(
    store: &Store,
    actor: &Mailbox,
    nonce: &str,
    time: i64,
) -> Result<()> {
    let Some(_wake_lock) = crate::service::wake_lock(store.root(), actor.id)? else {
        return Ok(());
    };
    let endpoint = sqlx::query!(
        "SELECT runtime,socket,thread FROM runtime_wakes WHERE recipient=? AND binding_version=?",
        actor.id,
        actor.binding_version
    )
    .fetch_one(store.pool())
    .await?;
    let thread = Uuid::parse_str(&endpoint.thread)?;
    let mut peer = if let Some(inbox) = store.claude_inbox(actor).await? {
        crate::claude_inbox::verify(store, Path::new(&endpoint.socket), &inbox)?;
        Peer::ClaudeInbox {
            socket: endpoint.socket.clone().into(),
            endpoint: inbox,
        }
    } else {
        Peer::connect(
            endpoint.runtime.parse()?,
            Path::new(&endpoint.socket),
            thread,
        )
        .await?
    };
    if !matches!(peer.state().await?, State::Idle) {
        return Ok(());
    }
    if !store.reserve_probe(actor, nonce, time).await? {
        return Ok(());
    }
    let operation = Operation::NativeVerification;
    let mut tx = store.delivery_transaction(actor, operation).await?;
    ensure!(
        Store::probe_current(&mut tx, actor, nonce, time).await?,
        "verification connection changed"
    );
    if let Peer::ClaudeInbox { socket, endpoint } = &peer {
        crate::claude_inbox::verify(store, socket, endpoint)?;
    }
    store
        .diagnostics()
        .measure(
            operation,
            Phase::Transport,
            peer.send(
                thread,
                crate::verification::challenge(actor, nonce),
                false,
                nonce.into(),
            ),
        )
        .await?;
    sqlx::query!("UPDATE delivery_probes SET transport_accepted_at=COALESCE(transport_accepted_at,?),healthy_at=?,failed=0 WHERE recipient=? AND binding_version=? AND nonce=?",time,time,actor.id,actor.binding_version,nonce).execute(&mut **tx).await?;
    tx.commit().await?;
    Ok(())
}
