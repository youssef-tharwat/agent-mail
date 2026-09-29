//! Explicit Herdr socket transport and verified participant bindings.
//!
//! Calls have bounded response sizes and deadlines. Binding compares native session,
//! pane, and terminal identities; process environment is consulted only for caller
//! authentication. No operation silently switches to Herdr's currently focused session.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
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
    /// Herdr pane identifier associated with the registration, when applicable.
    pub pane: String,
    /// Terminal identity bound alongside the pane.
    pub terminal: String,
    /// Native agent name reported by Herdr.
    pub agent: String,
    /// Native identity format, either id or path.
    pub session_kind: String,
    /// Native identity value in the format specified by session_kind.
    pub session_value: String,
    /// Working directory reported by Herdr, if available.
    pub cwd: Option<PathBuf>,
}

/// A native session identity advertised by a Herdr agent.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Session {
    /// Native agent name reported by Herdr.
    pub agent: String,
    /// Native identity format, either id or path.
    pub kind: String,
    /// Native identity value in the advertised format.
    pub value: String,
}

/// A live Herdr agent snapshot used to verify identity and readiness.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Agent {
    /// Herdr pane identifier reported by the live agent.
    pub pane_id: String,
    /// Live terminal identity used to detect pane reuse.
    pub terminal_id: String,
    /// Native agent name reported by Herdr.
    pub agent: Option<String>,
    /// Native session details, if Herdr has observed them.
    pub agent_session: Option<Session>,
    /// Runtime status advertised by Herdr.
    pub agent_status: String,
    /// Whether the live agent is ready for interactive input.
    #[serde(default)]
    pub interactive_ready: bool,
    /// Whether agent launch has not yet completed.
    #[serde(default)]
    pub launch_pending: bool,
    /// Working directory reported by Herdr, if available.
    pub cwd: Option<PathBuf>,
}

impl Agent {
    /// Validate the native session identity advertised by a Herdr agent.
    ///
    /// # Errors
    /// Session details are absent, inconsistent, or use an unsupported identity kind.
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

    /// Check whether a live Herdr identity matches a mailbox snapshot.
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

    /// Check whether Herdr reports an idle, interactive, fully launched agent.
    pub fn ready(&self) -> bool {
        self.interactive_ready
            && !self.launch_pending
            && matches!(self.agent_status.as_str(), "idle" | "done")
    }
}

/// Send one bounded request to an explicitly selected Herdr socket.
///
/// # Errors
/// Connection, timeout, framing, decoding, or remote RPC failure occurs.
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

/// Read one live Herdr agent by pane target.
///
/// # Errors
/// The RPC fails or the response does not describe an agent.
pub async fn agent(socket: &Path, target: &str) -> Result<Agent> {
    let result = call(socket, "agent.get", json!({"target": target})).await?;
    serde_json::from_value(result["agent"].clone()).context("decode Herdr agent")
}

/// List agents from an explicitly selected Herdr session.
///
/// # Errors
/// The RPC fails or the agent list cannot be decoded.
pub async fn agents(socket: &Path) -> Result<Vec<Agent>> {
    let result = call(socket, "agent.list", json!({})).await?;
    serde_json::from_value(result["agents"].clone()).context("decode Herdr agents")
}

/// Check whether the Mail plugin is linked and enabled in Herdr.
///
/// # Errors
/// The RPC fails or its response cannot be inspected.
pub async fn plugin_enabled(socket: &Path) -> Result<bool> {
    let result = call(socket, "plugin.list", json!({"plugin_id": PLUGIN_ID})).await?;
    let plugins = result["plugins"]
        .as_array()
        .context("decode Herdr plugins")?;
    Ok(plugins
        .iter()
        .any(|p| p["plugin_id"] == PLUGIN_ID && p["enabled"] == true))
}

/// Notify the operator about a group’s outstanding obligations.
///
/// # Errors
/// Herdr connection, timeout, or notification delivery fails.
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
    /// Bind a participant to a verified live Herdr identity.
    ///
    /// # Errors
    /// The group lacks a socket, identity validation fails, replacement conflicts, or persistence fails.
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
        let socket =
            std::env::var_os("HERDR_SOCKET_PATH").context("missing caller Herdr socket")?;
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
