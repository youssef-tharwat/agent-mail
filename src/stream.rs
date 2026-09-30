//! Private, resumable streams of committed coordination events.
//!
//! The server owns the worker lock and removes its socket before releasing that lock.
//! Subscriptions authenticate the current binding generation, then replay from a
//! cursor. Hints only accelerate replay; reconnecting never acknowledges business work.
//! Use [`Server::shutdown`] to await client cleanup before starting a replacement.

use crate::{identity::Binding, store::Store};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Notify, Semaphore, broadcast, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    WorkerStatus {
        version: u32,
    },
    PrepareUpgrade {
        version: u32,
        target_version: String,
    },
    Changed {
        version: u32,
    },
    Subscribe {
        version: u32,
        group: String,
        participant: String,
        binding: Box<Binding>,
        binding_version: i64,
        after: i64,
    },
}
/// Identity of the running local worker, returned over its private socket.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WorkerInfo {
    pub version: String,
    pub schema: i64,
    pub executable: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlReply {
    Worker { info: WorkerInfo },
    UpgradeAccepted,
    Error { message: String },
}

async fn control(root: &Path, request: Request) -> Result<ControlReply> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut socket = UnixStream::connect(socket(root)).await?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        socket.write_all(&bytes).await?;
        let mut reader = BufReader::new(socket);
        let response = line(&mut reader, 4096).await?;
        let reply: ControlReply = serde_json::from_slice(&response)?;
        match reply {
            ControlReply::Error { message } => anyhow::bail!("{message}"),
            other => Ok(other),
        }
    })
    .await
    .context("worker control timed out")?
}

pub(crate) async fn worker_info(root: &Path) -> Result<WorkerInfo> {
    match control(root, Request::WorkerStatus { version: 1 }).await? {
        ControlReply::Worker { info } => Ok(info),
        _ => anyhow::bail!("unexpected worker control response"),
    }
}

pub(crate) async fn prepare_upgrade(root: &Path) -> Result<()> {
    ensure!(
        matches!(
            control(
                root,
                Request::PrepareUpgrade {
                    version: 1,
                    target_version: env!("CARGO_PKG_VERSION").into()
                }
            )
            .await?,
            ControlReply::UpgradeAccepted
        ),
        "worker did not accept upgrade handoff"
    );
    Ok(())
}

/// A versioned subscription response, committed event, or protocol error.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// The subscription has authenticated and is ready to replay.
    Ready {
        /// Event-stream protocol version.
        version: u32,
        /// Authenticated participant receiving this frame.
        participant: String,
        /// Credential generation used for this subscription.
        binding_version: i64,
    },
    /// One committed coordination event for the subscriber.
    Event {
        /// Event-stream protocol version.
        version: u32,
        /// Authenticated participant receiving this frame.
        participant: String,
        /// Credential generation used for this subscription.
        binding_version: i64,
        /// Committed coordination-event cursor.
        id: i64,
        /// Category of the committed change.
        kind: String,
        /// Identifier of the changed business record.
        subject: String,
        /// Version of the subject when this event was committed.
        revision: i64,
    },
    /// A protocol or subscription failure requiring caller handling.
    Error {
        /// Message UUID or protocol failure description.
        message: String,
    },
}

/// Return the private event socket path beneath a state directory.
pub fn socket(root: &Path) -> PathBuf {
    root.join("events.sock")
}

/// Send a best-effort acceleration hint after committing a state change.
pub async fn hint(root: &Path) {
    let _ = tokio::time::timeout(Duration::from_millis(100), async {
        let mut stream = UnixStream::connect(socket(root)).await?;
        stream
            .write_all(b"{\"method\":\"changed\",\"version\":1}\n")
            .await
    })
    .await;
}

/// Event listener whose tasks share ownership of the worker lock.
#[derive(Debug)]
pub struct Server {
    changed: Arc<Notify>,
    upgrade: Arc<Notify>,
    task: JoinHandle<()>,
    stop: Option<oneshot::Sender<()>>,
}

#[derive(Debug)]
struct Endpoint {
    path: PathBuf,
    _lock: crate::service::WorkerLock,
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        // Remove our socket before releasing the worker lock. Client tasks also
        // retain this guard, including while task cancellation is being polled.
        let _ = std::fs::remove_file(&self.path);
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    /// Start the event server while exclusively owning the installation's worker lock.
    ///
    /// # Errors
    /// Returns an error if another worker owns the lock, the socket path is not
    /// a socket, or binding and setting private permissions fails.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime with I/O enabled.
    pub fn start(store: Store) -> Result<Self> {
        let lock = crate::service::WorkerLock::acquire(store.root())?;
        let path = socket(store.root());
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            ensure!(meta.file_type().is_socket(), "stream path is not a socket");
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(&path)?;
        let endpoint = Arc::new(Endpoint {
            path: path.clone(),
            _lock: lock,
        });
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let changed = Arc::new(Notify::new());
        let notify = changed.clone();
        let upgrade = Arc::new(Notify::new());
        let upgrading = upgrade.clone();
        let (upgrade_signal, _) = watch::channel(false);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (signals, _) = broadcast::channel::<()>(1);
            let slots = Arc::new(Semaphore::new(32));
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted=listener.accept()=>{
                        let Ok((stream,_))=accepted else {break;};
                        let Ok(permit)=slots.clone().try_acquire_owned() else {continue;};
                        let store=store.clone();let signals=signals.clone();let notify=notify.clone();let endpoint=Arc::clone(&endpoint);let upgrading=upgrading.clone();let upgrade_signal=upgrade_signal.clone();
                        clients.spawn(async move {let _endpoint=endpoint;let _permit=permit;let _=serve(stream,&store,&signals,&notify,&upgrading,&upgrade_signal).await;});
                    }
                    _=clients.join_next(),if !clients.is_empty()=>{}
                }
            }
            clients.abort_all();
            while clients.join_next().await.is_some() {}
            drop(listener);
            drop(endpoint);
        });
        Ok(Self {
            changed,
            upgrade,
            task,
            stop: Some(stop),
        })
    }

    /// Wait for a committed-change hint; durable replay remains authoritative.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    pub(crate) async fn upgrade_requested(&self) {
        self.upgrade.notified().await;
    }

    /// Stop the listener and all client tasks before releasing the worker lock.
    ///
    /// # Errors
    /// Returns an error if the server task panicked or was externally cancelled.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.task).await.context("event server task failed")?;
        Ok(())
    }
}
async fn send(stream: &mut tokio::net::unix::OwnedWriteHalf, frame: &Frame) -> Result<()> {
    let mut bytes = serde_json::to_vec(frame)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), stream.write_all(&bytes))
        .await
        .context("subscriber is too slow; reconnect from cursor")??;
    Ok(())
}
async fn line<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R, limit: usize) -> Result<Vec<u8>> {
    // take bounds allocation before read_until, including malicious oversized handshakes.
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await?;
    ensure!(
        bytes.len() <= limit && bytes.last() == Some(&b'\n'),
        "invalid or oversized stream frame"
    );
    Ok(bytes)
}
async fn serve(
    stream: UnixStream,
    store: &Store,
    signals: &broadcast::Sender<()>,
    notify: &Notify,
    upgrading: &Notify,
    upgrade_signal: &watch::Sender<bool>,
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let result:Result<()>=async {
        let bytes=tokio::time::timeout(Duration::from_secs(3),line(&mut read,4096)).await??;
        let request:Request=serde_json::from_slice(&bytes).context("invalid stream request")?;
        match &request {
            Request::WorkerStatus {version:1} => {
                let response=ControlReply::Worker {info:WorkerInfo {version:env!("CARGO_PKG_VERSION").into(),schema:crate::store::SCHEMA_VERSION,executable:std::env::current_exe()?}};
                let mut bytes=serde_json::to_vec(&response)?; bytes.push(b'\n'); write.write_all(&bytes).await?; return Ok(());
            }
            Request::PrepareUpgrade {version:1,target_version} => {
                ensure!(semver::Version::parse(target_version)? >= semver::Version::parse(env!("CARGO_PKG_VERSION"))?,"worker is newer than the requested executable; automatic downgrade is refused");
                write.write_all(b"{\"type\":\"upgrade_accepted\"}\n").await?;
                upgrade_signal.send_replace(true);
                // Allow subscribers to receive their terminal upgrade frame before the worker drains.
                tokio::time::sleep(Duration::from_millis(100)).await;
                upgrading.notify_one();return Ok(());
            }
            _=>{}
        }
        let Request::Subscribe{version,group,participant,binding,binding_version,mut after}=request else {
            ensure!(matches!(request,Request::Changed{version:1}),"unsupported stream version");
            let _=signals.send(());notify.notify_one();return Ok(());
        };
        ensure!(version==2 && after>=0,"unsupported stream version; upgrade client for attention events");
        let actor=store.mailbox(&group,&participant).await?;
        ensure!(actor.binding==*binding && actor.binding_version==binding_version,"binding changed or credential invalid; reconnect with current identity");
        if let Binding::Herdr(bound)=&*binding {
            let group=store.group(&group).await?;
            let live=crate::herdr::agent(Path::new(group.socket.as_deref().context("Herdr socket missing")?),&bound.pane).await?;
            ensure!(live.matches(&actor),"Herdr identity mismatch");
        }
        ensure!(!matches!(*binding,Binding::Remote{..}),"remote routes cannot subscribe locally");
        // Subscribe before replay. A racing commit is either in replay or its pending signal.
        let mut receiver=signals.subscribe();
        let mut upgrade=upgrade_signal.subscribe();
        send(&mut write,&Frame::Ready{version:2,participant:participant.clone(),binding_version}).await?;
        loop {
            ensure!(!*upgrade.borrow(),"store upgrade in progress; resume with the new binary and saved cursor");
            let mut tx=store.pool().begin().await?;
            Store::check_actor(&mut tx,&actor).await?;
            let rows=sqlx::query!("SELECT id,kind,subject,version FROM coordination_events WHERE recipient=? AND id>? ORDER BY id LIMIT 32",actor.id,after).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            let full=rows.len()==32;
            for row in rows {
                send(&mut write,&Frame::Event{version:2,participant:participant.clone(),binding_version,id:row.id,kind:row.kind,subject:row.subject,revision:row.version}).await?;
                after=row.id;
            }
            if full {continue;}
            tokio::select! {
                _=receiver.recv()=>{},
                _=upgrade.changed()=>{},
                _=tokio::time::sleep(Duration::from_secs(5))=>{},
                ended=read.fill_buf()=>{ensure!(!ended?.is_empty(),"subscriber disconnected");anyhow::bail!("unexpected subscriber input");}
            }
        }
    }.await;
    if let Err(error) = result {
        let _ = send(
            &mut write,
            &Frame::Error {
                message: format!("{error:#}"),
            },
        )
        .await;
    }
    Ok(())
}

/// Subscribe using a mailbox credential and an exclusive replay cursor.
///
/// # Errors
/// The cursor is negative, socket connection fails, or the handshake cannot be written.
pub async fn connect(
    store: &Store,
    actor: &crate::store::Mailbox,
    after: i64,
) -> Result<BufReader<UnixStream>> {
    ensure!(after >= 0, "cursor must be nonnegative");
    let mut stream = UnixStream::connect(socket(store.root()))
        .await
        .context("Mail worker unavailable; start agent-mail service run")?;
    let request = Request::Subscribe {
        version: 2,
        group: actor.group_name.clone(),
        participant: actor.name.clone(),
        binding: Box::new(actor.binding.clone()),
        binding_version: actor.binding_version,
        after,
    };
    let mut bytes = serde_json::to_vec(&request)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    Ok(BufReader::new(stream))
}
/// Read and decode one bounded event frame.
///
/// # Errors
/// The stream closes, the frame is incomplete or oversized, or decoding fails.
pub async fn next(reader: &mut BufReader<UnixStream>) -> Result<Frame> {
    serde_json::from_slice(&line(reader, 8192).await?).context("invalid event frame")
}
