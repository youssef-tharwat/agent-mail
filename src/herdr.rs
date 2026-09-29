use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

use crate::{
    PLUGIN_ID,
    identity::Binding,
    store::{Mailbox, Store},
};

/// A verified Herdr session bound to a durable Mail participant.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HerdrBinding {
    pub pane: String,
    pub terminal: String,
    pub agent: String,
    pub session_kind: String,
    pub session_value: String,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Session {
    pub agent: String,
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Agent {
    pub pane_id: String,
    pub terminal_id: String,
    pub agent: Option<String>,
    pub agent_session: Option<Session>,
    pub agent_status: String,
    #[serde(default)]
    pub interactive_ready: bool,
    #[serde(default)]
    pub launch_pending: bool,
    pub cwd: Option<String>,
}

impl Agent {
    pub fn identity(&self) -> Result<&Session> {
        let session = self.agent_session.as_ref().context(
            "native agent identity is unavailable; enable the agent's Herdr integration",
        )?;
        ensure!(
            !session.value.is_empty() && !self.terminal_id.is_empty(),
            "agent identity is incomplete"
        );
        ensure!(
            self.agent.as_deref() == Some(session.agent.as_str()),
            "agent and native session identity disagree"
        );
        ensure!(
            matches!(session.kind.as_str(), "id" | "path"),
            "unsupported agent identity kind"
        );
        Ok(session)
    }

    pub fn matches(&self, mailbox: &Mailbox) -> bool {
        let Some(binding) = mailbox.binding.herdr() else {
            return false;
        };
        self.identity().is_ok_and(|s| {
            self.pane_id == binding.pane
                && self.terminal_id == binding.terminal
                && s.agent == binding.agent
                && s.kind == binding.session_kind
                && s.value == binding.session_value
        })
    }

    pub fn ready(&self) -> bool {
        self.interactive_ready
            && !self.launch_pending
            && matches!(self.agent_status.as_str(), "idle" | "done")
    }
}

/// The socket is always explicit; never use Herdr's UI-focused session as a fallback.
pub async fn call(socket: &Path, method: &str, params: Value) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = UnixStream::connect(socket)
            .await
            .context("connect to Herdr")?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut request =
            serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))?;
        request.push(b'\n');
        stream.write_all(&request).await?;
        let limit = 4 * 1024 * 1024;
        let mut reader = BufReader::new(stream.take(limit + 1));
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        ensure!(
            line.len() as u64 <= limit && line.ends_with('\n'),
            "invalid or oversized Herdr response"
        );
        let response: Value = serde_json::from_str(&line).context("decode Herdr response")?;
        ensure!(response["id"] == id, "Herdr response ID mismatch");
        if let Some(error) = response.get("error") {
            bail!("Herdr {method}: {error}");
        }
        response
            .get("result")
            .cloned()
            .context("Herdr response has no result")
    })
    .await
    .context("Herdr request timed out; delivery may have occurred")?
}

pub async fn agent(socket: &Path, target: &str) -> Result<Agent> {
    let result = call(socket, "agent.get", json!({"target": target})).await?;
    serde_json::from_value(result["agent"].clone()).context("decode Herdr agent")
}

pub async fn agents(socket: &Path) -> Result<Vec<Agent>> {
    let result = call(socket, "agent.list", json!({})).await?;
    serde_json::from_value(result["agents"].clone()).context("decode Herdr agents")
}

pub async fn plugin_enabled(socket: &Path) -> Result<bool> {
    let result = call(socket, "plugin.list", json!({"plugin_id": PLUGIN_ID})).await?;
    let plugins = result["plugins"]
        .as_array()
        .context("decode Herdr plugins")?;
    Ok(plugins
        .iter()
        .any(|p| p["plugin_id"] == PLUGIN_ID && p["enabled"] == true))
}

pub async fn notify(socket: &Path, group: &str, count: usize) -> Result<()> {
    call(
        socket,
        "notification.show",
        json!({
            "title": "Agent Mail needs attention",
            "body": format!("{count} inbox(es) in {group} need attention. Run agent-mail status.")
        }),
    )
    .await?;
    Ok(())
}

impl Store {
    /// Bind a mailbox to a native Herdr identity, preserving its durable address.
    pub async fn bind(
        &self,
        group: &str,
        participant: &str,
        agent: &Agent,
        replace: bool,
    ) -> Result<()> {
        ensure!(
            self.group(group).await?.socket.is_some(),
            "configure a Herdr socket before binding"
        );
        let session = agent.identity()?;
        let binding = Binding::Herdr(HerdrBinding {
            pane: agent.pane_id.clone(),
            terminal: agent.terminal_id.clone(),
            agent: session.agent.clone(),
            session_kind: session.kind.clone(),
            session_value: session.value.clone(),
            cwd: agent.cwd.clone(),
        });
        self.set_binding(group, participant, &binding, replace)
            .await
    }

    pub(crate) async fn authenticate_herdr(&self, group: &str) -> Result<Mailbox> {
        ensure!(
            std::env::var("HERDR_ENV").as_deref() == Ok("1"),
            "no Herdr session or standalone credential"
        );
        let pane = std::env::var("HERDR_PANE_ID").context("missing caller pane identity")?;
        let group = self.group(group).await?;
        let socket = std::env::var("HERDR_SOCKET_PATH").context("missing caller Herdr socket")?;
        let configured_socket = group
            .socket
            .as_deref()
            .context("group has no Herdr socket")?;
        ensure!(
            Path::new(&socket) == Path::new(configured_socket),
            "caller belongs to a different Herdr session"
        );
        let binding = self.caller(&group.name, &pane).await?;
        let live = agent(Path::new(configured_socket), &pane).await?;
        ensure!(
            live.matches(&binding),
            "participant binding no longer matches this agent; operator rebinding is required"
        );
        Ok(binding)
    }
}
