//! Private, resumable event stream. Socket hints never substitute for committed events.
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
    sync::{Notify, Semaphore, broadcast},
    task::{JoinHandle, JoinSet},
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
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
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Ready {
        version: u32,
        participant: String,
        binding_version: i64,
    },
    Event {
        version: u32,
        participant: String,
        binding_version: i64,
        id: i64,
        kind: String,
        subject: String,
        revision: i64,
    },
    Error {
        message: String,
    },
}

pub fn socket(root: &Path) -> PathBuf {
    root.join("events.sock")
}

/// Best-effort post-commit acceleration; an absent worker cannot fail a committed operation.
pub async fn hint(root: &Path) {
    let _ = tokio::time::timeout(Duration::from_millis(100), async {
        let mut stream = UnixStream::connect(socket(root)).await?;
        stream
            .write_all(b"{\"method\":\"changed\",\"version\":1}\n")
            .await
    })
    .await;
}

pub struct Server {
    pub changed: Arc<Notify>,
    task: JoinHandle<()>,
    path: PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}
impl Server {
    /// Start only while holding the installation's worker lock.
    pub fn start(store: Store) -> Result<Self> {
        let path = socket(&store.root);
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            ensure!(meta.file_type().is_socket(), "stream path is not a socket");
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let changed = Arc::new(Notify::new());
        let notify = changed.clone();
        let task = tokio::spawn(async move {
            let (signals, _) = broadcast::channel::<()>(1);
            let slots = Arc::new(Semaphore::new(32));
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let Ok((stream,_))=accepted else {break;};
                        let Ok(permit)=slots.clone().try_acquire_owned() else {continue;};
                        let store=store.clone();let signals=signals.clone();let notify=notify.clone();
                        clients.spawn(async move {let _permit=permit;let _=serve(stream,&store,&signals,&notify).await;});
                    }
                    _=clients.join_next(),if !clients.is_empty()=>{}
                }
            }
        });
        Ok(Self {
            changed,
            task,
            path,
        })
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
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let result:Result<()>=async {
        let bytes=tokio::time::timeout(Duration::from_secs(3),line(&mut read,4096)).await??;
        let request:Request=serde_json::from_slice(&bytes).context("invalid stream request")?;
        let Request::Subscribe{version,group,participant,binding,binding_version,mut after}=request else {
            ensure!(matches!(request,Request::Changed{version:1}),"unsupported stream version");
            let _=signals.send(());notify.notify_one();return Ok(());
        };
        ensure!(version==1 && after>=0,"unsupported version or cursor");
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
        send(&mut write,&Frame::Ready{version:1,participant:participant.clone(),binding_version}).await?;
        loop {
            let mut tx=store.pool.begin().await?;
            Store::check_actor(&mut tx,&actor).await?;
            let rows=sqlx::query!("SELECT id,kind,subject,version FROM coordination_events WHERE recipient=? AND id>? ORDER BY id LIMIT 32",actor.id,after).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            let full=rows.len()==32;
            for row in rows {
                send(&mut write,&Frame::Event{version:1,participant:participant.clone(),binding_version,id:row.id,kind:row.kind,subject:row.subject,revision:row.version}).await?;
                after=row.id;
            }
            if full {continue;}
            tokio::select! {
                _=receiver.recv()=>{},
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

/// Connect using the already authenticated actor; streaming never acknowledges events.
pub async fn connect(
    store: &Store,
    actor: &crate::store::Mailbox,
    after: i64,
) -> Result<BufReader<UnixStream>> {
    ensure!(after >= 0, "cursor must be nonnegative");
    let mut stream = UnixStream::connect(socket(&store.root))
        .await
        .context("Mail worker unavailable; start agent-mail service run")?;
    let request = Request::Subscribe {
        version: 1,
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
pub async fn next(reader: &mut BufReader<UnixStream>) -> Result<Frame> {
    serde_json::from_slice(&line(reader, 8192).await?).context("invalid event frame")
}
