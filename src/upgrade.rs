//! Automatic upgrades of existing stores, with serialized backup and worker handoff.

use anyhow::{Context, Result, ensure};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(crate) async fn backup(
    connection: &mut SqliteConnection,
    root: &Path,
    version: i64,
) -> Result<PathBuf> {
    let directory = root.join("backups");
    std::fs::create_dir_all(&directory)?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let path = directory.join(format!("schema-{version}-{}.db", uuid::Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    let destination = path.to_str().context("backup path is not UTF-8")?;
    sqlx::query!("VACUUM INTO ?", destination)
        .execute(connection)
        .await?;
    let mut check = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(&path).read_only(true),
    )
    .await?;
    let integrity = sqlx::query!("PRAGMA quick_check")
        .fetch_all(&mut check)
        .await?;
    ensure!(
        integrity.len() == 1 && integrity[0].quick_check.as_deref() == Some("ok"),
        "backup integrity check failed"
    );
    let saved = sqlx::query!("PRAGMA user_version")
        .fetch_one(&mut check)
        .await?;
    ensure!(
        saved.user_version == Some(version),
        "backup schema verification failed"
    );
    check.close().await?;
    file.sync_all()?;
    std::fs::File::open(&directory)?.sync_all()?;
    Ok(path)
}

use crate::{
    service,
    store::{DatabaseGuard, SCHEMA_VERSION, Store},
    stream, supervision,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{fs::File, time::Duration};
use tokio::{io::AsyncWriteExt, net::UnixListener, task::JoinHandle};

/// Store opening intent; only initialization may create a missing store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Ordinary operation on an existing store.
    Existing,
    /// Explicit first-time initialization or group enrollment.
    Initialize,
    /// Delivery worker startup; it will own its own replacement lifecycle.
    Worker,
}

struct Gate(File);
impl Gate {
    fn shared(self) -> Result<Self> {
        FileExt::lock_shared(&self.0)?;
        Ok(self)
    }
    async fn acquire(root: &Path, exclusive: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(root.join("upgrade.lock"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let result = if exclusive {
                file.try_lock_exclusive()
            } else {
                FileExt::try_lock_shared(&file)
            };
            match result {
                Ok(()) => return Ok(Self(file)),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "another upgrade is still running; retry this command shortly"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

async fn schema(root: &Path) -> Result<i64> {
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(root.join("mail.db"))
            .read_only(true)
            .create_if_missing(false),
    )
    .await?;
    let version = sqlx::query!("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await?
        .user_version
        .context("SQLite schema version missing")?;
    connection.close().await?;
    ensure!(
        version <= SCHEMA_VERSION,
        "store schema {version} is newer than this binary; automatic downgrade is refused"
    );
    Ok(version)
}

#[derive(Debug, Serialize, Deserialize)]
struct SavedWorker {
    rollback: PathBuf,
    managed: bool,
}
fn intent(root: &Path) -> PathBuf {
    root.join("upgrade.json")
}

// A temporary private endpoint terminates old watches so they release schema handles.
// It owns the worker lock while the actual delivery worker is absent.
struct DrainEndpoint {
    socket: PathBuf,
    _worker: service::WorkerLock,
}
impl Drop for DrainEndpoint {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
    }
}
struct Drain {
    task: JoinHandle<()>,
    _endpoint: std::sync::Arc<DrainEndpoint>,
}
impl Drain {
    fn start(root: &Path) -> Result<Self> {
        use std::os::unix::fs::FileTypeExt;
        let worker = service::WorkerLock::acquire(root)?;
        let socket = stream::socket(root);
        if let Ok(metadata) = std::fs::symlink_metadata(&socket) {
            ensure!(
                metadata.file_type().is_socket(),
                "event path is not a socket"
            );
            std::fs::remove_file(&socket)?;
        }
        let listener = UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        let endpoint = std::sync::Arc::new(DrainEndpoint {
            socket,
            _worker: worker,
        });
        let keepalive = endpoint.clone();
        let task = tokio::spawn(async move {
            let _endpoint = keepalive;
            while let Ok((mut client, _)) = listener.accept().await {
                let _=tokio::time::timeout(Duration::from_millis(100),client.write_all(b"{\"type\":\"error\",\"message\":\"store upgrade in progress; resume with the upgraded binary and saved cursor\"}\n")).await;
            }
        });
        Ok(Self {
            task,
            _endpoint: endpoint,
        })
    }
    async fn close(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
impl Drop for Drain {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn legacy_worker(root: &Path) -> Result<(u32, PathBuf)> {
    let output = tokio::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .env("LC_ALL", "C")
        .output()
        .await?;
    ensure!(output.status.success(), "cannot inspect the legacy worker");
    let suffix = format!(" --state-dir {} service run", root.display());
    let mut matches = Vec::new();
    for line in String::from_utf8(output.stdout)?.lines() {
        let line = line.trim();
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let Some(executable) = command.trim_start().strip_suffix(&suffix) else {
            continue;
        };
        let executable = PathBuf::from(executable);
        if executable
            .file_name()
            .is_some_and(|name| name == "agent-mail")
        {
            let pid = pid.parse::<u32>()?;
            if pid != std::process::id() {
                matches.push((pid, executable));
            }
        }
    }
    ensure!(
        matches.len() == 1,
        "legacy worker cannot be uniquely identified for this store; stop its supervisor once, then retry with the upgraded binary"
    );
    Ok(matches.remove(0))
}

async fn wait_stopped(root: &Path) -> Result<()> {
    for _ in 0..100 {
        if !service::running(root) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("worker did not release this store for upgrade")
}

async fn stop(root: &Path) -> Result<SavedWorker> {
    let info = stream::worker_info(root).await.ok();
    if let Some(info) = &info {
        ensure!(
            semver::Version::parse(&info.version)?
                <= semver::Version::parse(env!("CARGO_PKG_VERSION"))?,
            "worker is newer than this binary; automatic downgrade is refused"
        );
    }
    let legacy = if info.is_none() {
        Some(legacy_worker(root).await?)
    } else {
        None
    };
    let executable = info
        .as_ref()
        .map(|i| i.executable.clone())
        .or_else(|| legacy.as_ref().map(|(_, p)| p.clone()))
        .context("worker executable missing")?;
    let directory = root
        .join("backups")
        .join(format!("worker-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory)?;
    std::fs::set_permissions(root.join("backups"), std::fs::Permissions::from_mode(0o700))?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let rollback = directory.join("agent-mail");
    std::fs::copy(executable, &rollback)
        .context("cannot preserve the running worker executable")?;
    std::fs::set_permissions(&rollback, std::fs::Permissions::from_mode(0o700))?;
    File::open(&rollback)?.sync_all()?;
    let saved = SavedWorker {
        rollback,
        managed: supervision::owned_running(root)?,
    };
    // Persist restart intent before stopping any worker.
    write_intent(root, &saved)?;
    let controlled = info.is_some() && stream::prepare_upgrade(root).await.is_ok();
    if saved.managed {
        ensure!(
            supervision::suspend_owned(root)?,
            "owned service changed during upgrade"
        );
    }
    if !controlled && !saved.managed {
        let (pid, _) = legacy.context("worker rejected a verified upgrade handoff")?;
        let status = tokio::process::Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()
            .await?;
        ensure!(
            status.success(),
            "cannot interrupt the verified legacy worker"
        );
    }
    wait_stopped(root).await?;
    Ok(saved)
}

fn write_intent(root: &Path, saved: &SavedWorker) -> Result<()> {
    use std::io::Write;
    let temporary = root.join("upgrade.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(saved)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, intent(root))?;
    File::open(root)?.sync_all()?;
    Ok(())
}

fn resume(root: &Path, saved: &SavedWorker, executable: &Path) -> Result<()> {
    if saved.managed {
        supervision::resume_owned(root, executable)
    } else {
        supervision::spawn_worker(root, executable).map(|_| ())
    }
}

async fn schema_guard(root: &Path) -> Result<DatabaseGuard> {
    for _ in 0..200 {
        if let Ok(guard) = DatabaseGuard::acquire(root, true) {
            return Ok(guard);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!(
        "active commands still hold this store; no migration was applied; close the old commands and retry"
    )
}

async fn wait_started(root: &Path, current: bool) -> Result<()> {
    for _ in 0..160 {
        if service::running(root)
            && (!current
                || stream::worker_info(root)
                    .await
                    .is_ok_and(|i| i.version == env!("CARGO_PKG_VERSION")))
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!(
        "replacement worker did not become ready; upgrade intent retained; inspect service.log and retry the command"
    )
}

/// Open existing state, automatically migrating and replacing an older local worker.
///
/// Missing stores still require explicit initialization. Read-only diagnostics do
/// not call this entry point. Concurrent callers serialize a single upgrade; schema
/// failures roll back before the original worker is restored.
///
/// # Errors
/// Backup, migration, worker ownership, shutdown, restart or filesystem checks fail.
pub async fn open(root: &Path, mode: OpenMode) -> Result<Store> {
    if !root.join("mail.db").exists() {
        ensure!(
            mode == OpenMode::Initialize,
            "database missing; run agent-mail init GROUP first"
        );
        std::fs::create_dir_all(root)?;
        let _gate = Gate::acquire(root, true).await?;
        let store = Store::open(root, true).await?;
        store.close().await;
        return Store::open(root, false).await;
    }
    let root = root.canonicalize()?;
    let shared = Gate::acquire(&root, false).await?;
    let version = schema(&root).await?;
    let info = if mode != OpenMode::Worker && service::running(&root) {
        stream::worker_info(&root).await.ok()
    } else {
        None
    };
    let replace = mode != OpenMode::Worker
        && service::running(&root)
        && info.is_none_or(|i| i.version != env!("CARGO_PKG_VERSION"));
    if version == SCHEMA_VERSION
        && !replace
        && (mode == OpenMode::Worker || !intent(&root).exists())
    {
        return Store::open(&root, false).await;
    }
    ensure!(
        version > 0 || mode == OpenMode::Initialize,
        "database is not initialized; run agent-mail init GROUP first"
    );
    drop(shared);
    let gate = Gate::acquire(&root, true).await?;
    let version = schema(&root).await?;
    let mut saved = if intent(&root).exists() {
        Some(serde_json::from_slice::<SavedWorker>(&std::fs::read(
            intent(&root),
        )?)?)
    } else {
        None
    };
    let running = service::running(&root);
    let replace = if running && mode != OpenMode::Worker {
        stream::worker_info(&root)
            .await
            .ok()
            .is_none_or(|i| i.version != env!("CARGO_PKG_VERSION"))
    } else {
        false
    };
    // Another caller may have completed the migration while we waited for the
    // exclusive gate. Its returned store already holds a shared schema lock.
    if version == SCHEMA_VERSION && !replace && saved.is_none() {
        return Store::open(&root, false).await;
    }
    if running {
        saved = Some(stop(&root).await?);
    }
    let drain = Drain::start(&root)?;
    let migrated = async {
        let guard = schema_guard(&root).await?;
        if version < SCHEMA_VERSION {
            Store::open_guarded(&root, true, guard).await?.close().await;
        } else {
            drop(guard);
        }
        Store::open(&root, false).await
    }
    .await;
    drain.close().await;
    let _shared = gate.shared()?;
    match migrated {
        Ok(store) => {
            if let Some(saved) = &saved {
                if !matches!(mode, OpenMode::Worker) {
                    resume(&root, saved, &std::env::current_exe()?)?;
                    wait_started(&root, true).await?;
                }
                std::fs::remove_file(intent(&root))?;
                let _ = std::fs::remove_file(&saved.rollback);
                if let Some(parent) = saved.rollback.parent() {
                    let _ = std::fs::remove_dir(parent);
                }
            }
            Ok(store)
        }
        Err(error) => {
            if let Some(saved) = &saved {
                resume(&root, saved, &saved.rollback).context(
                    "migration failed; original worker could not restart; upgrade intent retained",
                )?;
                wait_started(&root, false).await?;
                std::fs::remove_file(intent(&root))?;
            }
            Err(error)
        }
    }
}
