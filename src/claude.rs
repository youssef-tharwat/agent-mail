//! Native Claude streaming bridge owned by the operator's client.
//!
//! [`run`] launches the CLI with bounded streaming frames and a private bridge socket.
//! The caller retains control of arguments and permission requests. Queue lifecycle
//! receipts confirm transport; model output never accepts or completes business work.
//! The child is terminated on cancellation and the socket is removed on exit.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CommandState {
    Queued,
    Started,
    Completed,
    Cancelled,
    Failed,
    #[serde(other)]
    Unknown,
}

const FRAME_LIMIT: usize = 4 * 1024 * 1024;
/// Observed Claude bridge identity, capabilities, and activity generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    /// Bridge protocol version checked during probing.
    pub protocol: u32,
    /// Native session identity, once initialized.
    pub session: Option<Uuid>,
    /// Runtime client version advertised by the bridge.
    pub client: String,
    /// Whether the required queue lifecycle capability is available.
    pub ready: bool,
    /// Whether native inputs remain active or queued.
    pub active: bool,
    /// Activity generation used to reject stale deliveries.
    pub epoch: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Status {
        version: u32,
    },
    Deliver {
        version: u32,
        session: Uuid,
        epoch: u64,
        active: bool,
        text: String,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    Status(Status),
    Accepted { uuid: Uuid },
    Error { message: String },
}
struct Call {
    request: Request,
    reply: oneshot::Sender<Response>,
}

async fn read_frame<R: AsyncBufRead + Unpin>(read: &mut R, limit: usize) -> Result<Option<Value>> {
    let mut bytes = Vec::new();
    read.take((limit + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    ensure!(
        bytes.len() <= limit && bytes.last() == Some(&b'\n'),
        "oversized or incomplete native frame"
    );
    Ok(Some(serde_json::from_slice(&bytes)?))
}
async fn write_frame<W: AsyncWrite + Unpin>(write: &mut W, frame: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(frame)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(3), async {
        write.write_all(&bytes).await?;
        write.flush().await
    })
    .await
    .context("native transport write timed out")??;
    Ok(())
}

/// One request per private connection, with bounded buffers and deadline.
async fn call(path: &Path, request: Request) -> Result<Response> {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut io = BufReader::new(UnixStream::connect(path).await?);
        write_frame(io.get_mut(), &serde_json::to_value(request)?).await?;
        serde_json::from_value(
            read_frame(&mut io, 8192)
                .await?
                .context("Claude bridge disconnected")?,
        )
        .context("invalid Claude bridge response")
    })
    .await
    .context("Claude bridge timed out")?
}
/// Read a bridge’s status and verify the expected Claude session.
///
/// # Errors
/// The bridge times out, its protocol or session differs, or transport or decoding fails.
pub async fn probe(path: &Path, session: Uuid) -> Result<Status> {
    match call(path, Request::Status { version: 1 }).await? {
        Response::Status(status) => {
            ensure!(
                status.protocol == 1 && status.session == Some(session),
                "Claude session identity mismatch"
            );
            Ok(status)
        }
        Response::Error { message } => anyhow::bail!("{message}"),
        _ => anyhow::bail!("unexpected Claude status response"),
    }
}
pub(crate) async fn deliver(
    path: &Path,
    session: Uuid,
    status: &Status,
    text: String,
) -> Result<()> {
    match call(
        path,
        Request::Deliver {
            version: 1,
            session,
            epoch: status.epoch,
            active: status.active,
            text,
        },
    )
    .await?
    {
        Response::Accepted { .. } => Ok(()),
        Response::Error { message } => anyhow::bail!("{message}"),
        _ => anyhow::bail!("Claude queue receipt missing"),
    }
}

struct SocketGuard {
    path: PathBuf,
    _lock: std::fs::File,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Default)]
struct RuntimeState {
    session: Option<Uuid>,
    client: String,
    ready: bool,
    active: BTreeSet<String>,
    epoch: u64,
}
impl RuntimeState {
    fn status(&self) -> Status {
        Status {
            protocol: 1,
            session: self.session,
            client: self.client.clone(),
            ready: self.ready,
            active: !self.active.is_empty(),
            epoch: self.epoch,
        }
    }
    fn submitted(&mut self, uuid: String) {
        self.active.insert(uuid);
        self.epoch += 1;
    }
    fn observe(&mut self, value: &Value) -> Result<Option<Uuid>> {
        if !value["parent_tool_use_id"].is_null() {
            return Ok(None);
        }
        if value["type"] == "system" && value["subtype"] == "init" {
            let session = Uuid::parse_str(
                value["session_id"]
                    .as_str()
                    .context("Claude init has no session")?,
            )?;
            ensure!(
                self.session.is_none_or(|old| old == session),
                "Claude session changed; reattach explicitly"
            );
            self.session = Some(session);
            self.client = value["claude_code_version"]
                .as_str()
                .unwrap_or("unknown")
                .into();
            self.ready = value["capabilities"]
                .as_array()
                .is_some_and(|caps| caps.iter().any(|v| v == "msg_lifecycle_v1"));
        }
        if value["type"] == "command_lifecycle" {
            if let Some(session) = self.session {
                ensure!(
                    value["session_id"]
                        .as_str()
                        .and_then(|id| Uuid::parse_str(id).ok())
                        == Some(session),
                    "Claude lifecycle session identity mismatch"
                );
            }
            ensure!(self.active.len() < 64, "too many native Claude commands");
            let id = value["command_uuid"]
                .as_str()
                .context("Claude lifecycle has no command UUID")?;
            match serde_json::from_value::<CommandState>(value["state"].clone())
                .unwrap_or(CommandState::Unknown)
            {
                CommandState::Queued => {
                    self.active.insert(id.into());
                    return Ok(Uuid::parse_str(id).ok());
                }
                CommandState::Started => {
                    self.active.insert(id.into());
                }
                CommandState::Completed | CommandState::Cancelled | CommandState::Failed => {
                    self.active.remove(id);
                    self.epoch += 1;
                }
                CommandState::Unknown => {
                    self.ready = false;
                }
            }
        }
        Ok(None)
    }
}

/// Bridge the operator’s stdio client to a native Claude subprocess.
/// permission requests remain under that client's control; no Mail credential is injected.
///
/// # Errors
/// Arguments or socket ownership are invalid, launching or transport fails, or native frames are invalid.
pub async fn run(socket: &Path, args: Vec<String>) -> Result<()> {
    ensure!(
        socket.is_absolute(),
        "Claude bridge socket must be absolute"
    );
    ensure!(
        !args.iter().any(|s| s == "--input-format"
            || s == "--output-format"
            || s.starts_with("--input-format=")
            || s.starts_with("--output-format=")),
        "bridge sets native stream formats"
    );
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(socket.with_extension("lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("Claude bridge already running")?;
    if let Ok(meta) = std::fs::symlink_metadata(socket) {
        ensure!(meta.file_type().is_socket(), "bridge path is not a socket");
        std::fs::remove_file(socket)?;
    }
    let listener = UnixListener::bind(socket)?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let _socket = SocketGuard {
        path: socket.to_path_buf(),
        _lock: lock,
    };
    let mut child = tokio::process::Command::new("claude")
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--replay-user-messages",
            "--permission-prompt-tool",
            "stdio",
        ])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("start native Claude CLI")?;
    let mut input = child.stdin.take().context("Claude stdin missing")?;
    let output = child.stdout.take().context("Claude stdout missing")?;
    let (native_tx, mut native_rx) = mpsc::channel(8);
    let (client_tx, mut client_rx) = mpsc::channel(8);
    let (requests, mut calls) = mpsc::channel::<Call>(8);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let mut read = BufReader::new(output);
        loop {
            let item = read_frame(&mut read, FRAME_LIMIT).await;
            let end = !matches!(&item, Ok(Some(_)));
            if native_tx.send(item).await.is_err() || end {
                break;
            }
        }
    });
    tasks.spawn(async move {
        let mut read = BufReader::new(tokio::io::stdin());
        loop {
            let item = read_frame(&mut read, FRAME_LIMIT).await;
            let end = !matches!(&item, Ok(Some(_)));
            if client_tx.send(item).await.is_err() || end {
                break;
            }
        }
    });
    tasks.spawn(async move {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
        let mut peers = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((io, _)) = accepted else { break; };
                    let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                    let tx = requests.clone();
                    peers.spawn(async move {
                        let _permit = permit;
                        let _ = tokio::time::timeout(Duration::from_secs(5), async {
                            let mut io = BufReader::new(io);
                            let frame = read_frame(&mut io, 8192).await?
                                .context("empty bridge request")?;
                            let request = serde_json::from_value(frame)?;
                            let (reply, rx) = oneshot::channel();
                            tx.send(Call { request, reply }).await?;
                            write_frame(io.get_mut(), &serde_json::to_value(rx.await?)?).await
                        }).await;
                    });
                }
                _ = peers.join_next(), if !peers.is_empty() => {}
            }
        }
    });
    let mut runtime = RuntimeState::default();
    let mut pending: HashMap<Uuid, oneshot::Sender<Response>> = HashMap::new();
    let mut stdout = tokio::io::stdout();
    let result: Result<()> = async {
        loop {
            tokio::select! {
                value = native_rx.recv() => {
                    let Some(value) = value.transpose()?.flatten() else {
                        anyhow::bail!("Claude process ended");
                    };
                    if let Some(id) = runtime.observe(&value)? {
                        if let Some(reply) = pending.remove(&id) {
                            let _ = reply.send(Response::Accepted { uuid: id });
                        }
                    }
                    write_frame(&mut stdout, &value).await?;
                }
                value = client_rx.recv() => {
                    let Some(mut value) = value.transpose()?.flatten() else { break; };
                    if value["type"] == "user" {
                        ensure!(runtime.active.len() < 32, "too many queued Claude inputs");
                        let id = value["uuid"].as_str().map(str::to_owned)
                            .unwrap_or_else(|| Uuid::new_v4().to_string());
                        value["uuid"] = json!(id);
                        if value["shouldQuery"] != false {
                            runtime.submitted(id);
                        }
                    }
                    write_frame(&mut input, &value).await?;
                }
                call = calls.recv() => {
                    let Some(call) = call else { break; };
                    pending.retain(|_, reply| !reply.is_closed());
                    match call.request {
                        Request::Status { version: 1 } => {
                            let _ = call.reply.send(Response::Status(runtime.status()));
                        }
                        Request::Deliver { version: 1, session, epoch, active, text }
                            if runtime.ready && runtime.session == Some(session)
                                && epoch == runtime.epoch && active != runtime.active.is_empty()
                                && pending.len() < 2 && text.len() <= 6000 => {
                            let id = Uuid::new_v4();
                            runtime.submitted(id.to_string());
                            let frame = json!({
                                "type": "user", "session_id": session, "uuid": id,
                                "parent_tool_use_id": null,
                                "priority": if active { "now" } else { "next" },
                                "message": { "role": "user", "content": text }
                            });
                            write_frame(&mut input, &frame).await?;
                            pending.insert(id, call.reply);
                        }
                        _ => {
                            let _ = call.reply.send(Response::Error {
                                message: "Claude state or session changed, unsupported capability, or invalid request".into()
                            });
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
        Ok(())
    }.await;
    tasks.abort_all();
    drop(input);
    if tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_receipts_are_not_model_results_and_stale_turns_change_epoch() -> Result<()> {
        let session = Uuid::new_v4();
        let command = Uuid::new_v4();
        let mut state = RuntimeState::default();
        state.observe(&json!({"type":"system","subtype":"init","session_id":session,"claude_code_version":"2.1.284","capabilities":["msg_lifecycle_v1"]}))?;
        assert!(state.status().ready);
        assert!(!state.status().active);
        state.submitted(command.to_string());
        let epoch = state.epoch;
        assert_eq!(
            state.observe(
                &json!({"type":"command_lifecycle","state":"queued","session_id":session,"command_uuid":command})
            )?,
            Some(command)
        );
        state.observe(&json!({"type":"result","subtype":"success"}))?;
        assert!(
            state.status().active,
            "result alone does not establish idle state"
        );
        state.observe(
            &json!({"type":"command_lifecycle","state":"completed","session_id":session,"command_uuid":command}),
        )?;
        assert!(!state.status().active);
        assert!(state.epoch > epoch);
        assert!(
            state
                .observe(&json!({"type":"system","subtype":"init","session_id":Uuid::new_v4()}))
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn missing_capabilities_and_foreign_lifecycle_cannot_acknowledge_delivery() -> Result<()> {
        let session = Uuid::new_v4();
        let command = Uuid::new_v4();
        let mut state = RuntimeState::default();
        state.observe(&json!({"type":"system","subtype":"init","session_id":session}))?;
        assert!(!state.status().ready);
        state.submitted(command.to_string());
        assert!(
            state
                .observe(&json!({"type":"command_lifecycle","state":"queued",
            "session_id":Uuid::new_v4(),"command_uuid":command}))
                .is_err()
        );
        assert!(state.status().active);
        assert_eq!(
            state.observe(&json!({"type":"command_lifecycle","state":"completed",
            "parent_tool_use_id":"child","session_id":session,"command_uuid":command}))?,
            None
        );
        assert!(
            state.status().active,
            "subagent lifecycle cannot finish the parent"
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_and_oversized_frames_are_rejected() {
        let mut read = std::io::Cursor::new(b"{\"too_large\":true}\n");
        assert!(read_frame(&mut read, 8).await.is_err());
        let mut read = std::io::Cursor::new(b"{}\n");
        assert_eq!(read_frame(&mut read, 8).await.unwrap(), Some(json!({})));
    }
}
