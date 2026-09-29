//! Opt-in delivery through a local Codex runtime queue.
//!
//! [`probe`] checks a persistent thread and queue support with a deadline. Runtime
//! receipts acknowledge transport only; they never complete work. This adapter never
//! answers approval or tool requests on behalf of the operator.

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

pub(crate) struct Client {
    stream: WebSocketStream<UnixStream>,
    sequence: u64,
    server_version: String,
}
impl Client {
    pub(crate) async fn connect(socket: &Path) -> Result<Self> {
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
    pub(crate) async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
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
    pub(crate) async fn thread(&mut self, id: Uuid) -> Result<Thread> {
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
pub(crate) struct Thread {
    id: Uuid,
    ephemeral: bool,
    pub(crate) status: ThreadStatus,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum ThreadStatus {
    Idle,
    Active,
    NotLoaded,
    SystemError,
}

/// Verify a persistent Codex thread and native queue support.
///
/// # Errors
/// The probe times out, identity or persistence checks fail, or RPC or decoding fails.
pub async fn probe(socket: &Path, thread: Uuid) -> Result<serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(5),async {
        let mut client=Client::connect(socket).await?;
        let live=client.thread(thread).await?;
        client.call("thread/queue/list",json!({"threadId":thread})).await?;
        Ok(json!({"client":client.server_version,"persistent":true,"state":match live.status {ThreadStatus::Idle=>"idle",ThreadStatus::Active=>"active",ThreadStatus::NotLoaded=>"not_loaded",ThreadStatus::SystemError=>"system_error"},"safe_queue":true,"receipt":"queue_accepted"}))
    }).await.context("Codex probe timed out")?
}

/// Find the sole loaded thread on a launcher-owned private server.
/// Multiple loaded threads are ambiguous and are never guessed.
pub async fn sole_loaded_thread(socket: &Path) -> Result<Option<Uuid>> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut client = Client::connect(socket).await?;
        let result = client
            .call("thread/loaded/list", json!({"limit":2}))
            .await?;
        let ids = result["data"]
            .as_array()
            .context("loaded thread list missing data")?;
        if ids.len() != 1 || !result["nextCursor"].is_null() {
            return Ok(None);
        }
        Ok(Some(Uuid::parse_str(
            ids[0].as_str().context("loaded thread ID")?,
        )?))
    })
    .await
    .context("loaded thread lookup timed out")?
}
