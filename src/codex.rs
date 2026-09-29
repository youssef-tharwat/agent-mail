//! Opt-in local Codex queue delivery; transport receipts never complete work.
use crate::{
    identity::Binding,
    service::Observation,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::net::UnixStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use uuid::Uuid;

struct Client {
    stream: WebSocketStream<UnixStream>,
    sequence: u64,
    server_version: String,
}
impl Client {
    async fn connect(socket: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket).await?;
        let config = WebSocketConfig::default().max_message_size(Some(1024 * 1024));
        let (stream, _) =
            tokio_tungstenite::client_async_with_config("ws://localhost/", stream, Some(config))
                .await?;
        let mut client = Self {
            stream,
            sequence: 0,
            server_version: String::new(),
        };
        let initialized = client.call("initialize", json!({"clientInfo":{"name":"agent_mail","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
        client.server_version = initialized["userAgent"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        client
            .stream
            .send(Message::Text(
                json!({"method":"initialized"}).to_string().into(),
            ))
            .await?;
        Ok(client)
    }
    async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        self.sequence += 1;
        self.stream
            .send(Message::Text(
                json!({"id":self.sequence,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await?;
        while let Some(frame) = self.stream.next().await {
            if let Message::Text(text) = frame? {
                let response: Value = serde_json::from_str(&text)?;
                if response.get("method").is_none() && response["id"] == self.sequence {
                    ensure!(
                        response.get("error").is_none(),
                        "Codex {method}: {}",
                        response["error"]
                    );
                    return response
                        .get("result")
                        .cloned()
                        .context("Codex response missing result");
                }
                // This adapter never answers agent approval or tool requests.
            }
        }
        anyhow::bail!("Codex connection closed before receipt")
    }
    async fn thread(&mut self, id: Uuid) -> Result<Thread> {
        #[derive(Deserialize)]
        struct Response {
            thread: Thread,
        }
        let response: Response = serde_json::from_value(
            self.call("thread/read", json!({"threadId":id,"includeTurns":false}))
                .await?,
        )?;
        ensure!(
            response.thread.id == id && !response.thread.ephemeral,
            "expected persistent Codex thread"
        );
        Ok(response.thread)
    }
}
#[derive(Deserialize)]
struct Thread {
    id: Uuid,
    ephemeral: bool,
    status: ThreadStatus,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ThreadStatus {
    Idle,
    Active,
    NotLoaded,
    SystemError,
}

impl Store {
    /// Attach a verified local thread to the current standalone binding generation.
    ///
    /// Errors if the endpoint is unavailable, already assigned, or not persistent.
    pub async fn attach_codex(&self, actor: &Mailbox, socket: &Path, thread: Uuid) -> Result<()> {
        ensure!(
            matches!(actor.binding, Binding::Standalone { .. }),
            "Codex wake requires a standalone participant"
        );
        ensure!(socket.is_absolute(), "socket path must be absolute");
        let socket = socket.to_str().context("socket must be UTF-8")?;
        tokio::time::timeout(Duration::from_secs(5), async {
            Client::connect(Path::new(socket))
                .await?
                .thread(thread)
                .await
        })
        .await
        .context("Codex verification timed out")??;
        let thread = thread.to_string();
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        // Repeating identical attachment preserves its cursor and retry budget.
        sqlx::query!("INSERT INTO codex_wakes(recipient,binding_version,socket,thread) VALUES (?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET binding_version=excluded.binding_version,socket=excluded.socket,thread=excluded.thread,scanned=0,delivered=0,attempted=0,attempts=0,next_attempt=0 WHERE codex_wakes.binding_version<>excluded.binding_version OR codex_wakes.socket<>excluded.socket OR codex_wakes.thread<>excluded.thread",
            actor.id, actor.binding_version, socket, thread).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Disable automatic Codex delivery without changing mail or work state.
    pub async fn detach_codex(&self, actor: &Mailbox) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        sqlx::query!("DELETE FROM codex_wakes WHERE recipient=?", actor.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    /// Report endpoints and persisted delivery budgets without participant credentials.
    pub async fn codex_status(&self) -> Result<Value> {
        let rows = sqlx::query!("SELECT m.group_name,m.name,c.binding_version,m.binding_version AS current_version,c.socket,c.thread,c.delivered,c.attempted,c.attempts,c.next_attempt FROM codex_wakes c JOIN mailboxes m ON m.id=c.recipient").fetch_all(&self.pool).await?;
        Ok(Value::Array(rows.into_iter().map(|r| json!({"group":r.group_name,"participant":r.name,"socket":r.socket,"thread":r.thread,"binding_current":r.binding_version==r.current_version,"delivered_through":r.delivered,"attempted_through":r.attempted,"attempts":r.attempts,"next_attempt":r.next_attempt})).collect()))
    }
    pub(crate) async fn has_codex(&self, actor: &Mailbox) -> Result<bool> {
        Ok(sqlx::query!(
            "SELECT recipient FROM codex_wakes WHERE recipient=? AND binding_version=?",
            actor.id,
            actor.binding_version
        )
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }
}

pub(crate) async fn tick(store: &Store, time: i64) -> Result<Vec<Observation>> {
    let targets = sqlx::query!("SELECT m.group_name,m.name FROM codex_wakes c JOIN mailboxes m ON m.id=c.recipient AND m.binding_version=c.binding_version").fetch_all(&store.pool).await?;
    let mut observations = Vec::new();
    for target in targets {
        let actor = store.mailbox(&target.group_name, &target.name).await?;
        let state = match tokio::time::timeout(Duration::from_secs(5), deliver(store, &actor, time))
            .await
        {
            Ok(Ok(state)) => state.to_string(),
            Ok(Err(error)) => format!("Codex wake uncertain: {error:#}"),
            Err(_) => "Codex wake timed out; receipt unknown".into(),
        };
        observations.push(Observation {
            group: target.group_name,
            participant: target.name,
            state,
        });
    }
    Ok(observations)
}

async fn deliver(store: &Store, actor: &Mailbox, time: i64) -> Result<&'static str> {
    let group = store.group(&actor.group_name).await?;
    if group.paused != 0 {
        return Ok("Codex wake disabled or group paused");
    }
    let Some(endpoint) = sqlx::query!("SELECT socket,thread,scanned,delivered,attempted,attempts,next_attempt FROM codex_wakes WHERE recipient=? AND binding_version=?",actor.id,actor.binding_version).fetch_optional(&store.pool).await? else { return Ok("binding changed; attach explicitly"); };
    let latest = sqlx::query!(
        "SELECT COALESCE(MAX(id),0) AS \"id!: i64\" FROM coordination_events WHERE recipient=?",
        actor.id
    )
    .fetch_one(&store.pool)
    .await?
    .id;
    if latest <= endpoint.scanned {
        return Ok("Codex notifications settled; work progress reported separately");
    }
    let actionable = store.needs_wake(actor, endpoint.scanned).await?;
    let cancellation = store.needs_cancellation(actor, endpoint.scanned).await?;
    if !actionable && !cancellation {
        store.scan_passive(actor, latest).await?;
        return Ok("Codex passive changes retained for recovery; no turn needed");
    }
    if latest == endpoint.attempted && endpoint.attempts >= 3 {
        return Ok("Codex delivery attempts exhausted; inspect status");
    }
    if latest == endpoint.attempted && endpoint.next_attempt > time {
        return Ok("Codex waiting for delivery deadline");
    }
    let thread = Uuid::parse_str(&endpoint.thread)?;
    let mut client = Client::connect(Path::new(&endpoint.socket)).await?;
    let live = client.thread(thread).await?;
    let steer = if matches!(live.status, ThreadStatus::Active) && cancellation {
        let result = client
            .call(
                "thread/read",
                json!({"threadId":thread,"includeTurns":true}),
            )
            .await?;
        let turns = result["thread"]["turns"]
            .as_array()
            .context("active turn unavailable")?;
        Some(
            turns
                .iter()
                .rev()
                .find(|t| t["status"] == "inProgress")
                .and_then(|t| t["id"].as_str())
                .context("active turn changed; retry later")?
                .to_string(),
        )
    } else if matches!(live.status, ThreadStatus::Idle) {
        if !actionable {
            store.scan_passive(actor, latest).await?;
            return Ok("Codex idle cancellation retained for recovery; no turn needed");
        }
        None
    } else {
        return Ok("Codex not idle; changes retained");
    };
    let text = store.delivery_text(actor).await?;
    // Persist the attempt before I/O. A crash or lost response consumes its budget.
    let mut tx = store.pool.begin().await?;
    Store::lock_actor(&mut tx, actor).await?;
    let next = time + 300;
    let reserved = sqlx::query!("UPDATE codex_wakes SET attempts=CASE WHEN attempted=? THEN attempts+1 ELSE 1 END, attempted=?,next_attempt=? WHERE recipient=? AND binding_version=? AND socket=? AND thread=? AND scanned<? AND (attempted<>? OR next_attempt<=?) AND (attempted<>? OR attempts<3) AND EXISTS(SELECT 1 FROM groups WHERE name=? AND paused=0)",latest,latest,next,actor.id,actor.binding_version,endpoint.socket,endpoint.thread,latest,latest,time,latest,actor.group_name).execute(&mut *tx).await?.rows_affected();
    tx.commit().await?;
    if reserved == 0 {
        return Ok("Codex reservation no longer eligible");
    }
    // Hold the binding lock during the bounded send: replacement/detachment cannot race it.
    let mut tx = store.pool.begin().await?;
    Store::lock_actor(&mut tx, actor).await?;
    let still_attached = sqlx::query!("SELECT recipient FROM codex_wakes WHERE recipient=? AND binding_version=? AND socket=? AND thread=? AND EXISTS(SELECT 1 FROM groups WHERE name=? AND paused=0) AND NOT EXISTS(SELECT 1 FROM coordination_events WHERE recipient=? AND id>?)",actor.id,actor.binding_version,endpoint.socket,endpoint.thread,actor.group_name,actor.id,latest).fetch_optional(&mut *tx).await?.is_some();
    ensure!(still_attached, "Codex endpoint changed before delivery");
    if let Some(turn) = steer {
        client.call("turn/steer",json!({"threadId":thread,"expectedTurnId":turn,"input":[{"type":"text","text":text}]})).await?;
    } else {
        let receipt=client.call("thread/queue/add",json!({"threadId":thread,"clientUserMessageId":format!("agent-mail-{}-{}-{latest}",actor.id,actor.binding_version),"input":[{"type":"text","text":text}]})).await?;
        ensure!(
            receipt["queuedSubmission"]["id"].as_str().is_some(),
            "Codex receipt missing submission ID"
        );
    }
    sqlx::query!(
        "UPDATE codex_wakes SET delivered=?,scanned=?,attempts=0,next_attempt=0 WHERE recipient=?",
        latest,
        latest,
        actor.id
    )
    .execute(&mut *tx)
    .await?;
    // Queue acceptance acknowledges transport only. Business deliveries remain pending.
    sqlx::query!("INSERT OR IGNORE INTO event_receipts(recipient,binding_version,event) SELECT recipient,?,id FROM coordination_events WHERE recipient=? AND id<=?",actor.binding_version,actor.id,latest).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok("Codex update queued; work and mail resolution unchanged")
}

/// Inspect the configured runtime without starting a turn.
pub async fn probe(socket: &Path, thread: Uuid) -> Result<serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(5),async {
        let mut client=Client::connect(socket).await?;
        let live=client.thread(thread).await?;
        client.call("thread/queue/list",json!({"threadId":thread})).await?;
        Ok(json!({"client":client.server_version,"persistent":true,"state":match live.status {ThreadStatus::Idle=>"idle",ThreadStatus::Active=>"active",ThreadStatus::NotLoaded=>"not_loaded",ThreadStatus::SystemError=>"system_error"},"safe_queue":true,"receipt":"queue_accepted"}))
    }).await.context("Codex probe timed out")?
}
