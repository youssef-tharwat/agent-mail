//! Protected original capture custody. Stored output is never business authority.
use crate::{
    execution::Correlation, managed_runtime::RuntimeDirectory, runtime_effects::ContentDigest,
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use rustix::fs::{FileType, Mode, OFlags, fstat, fsync, openat};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::fd::AsFd,
};

/// Maximum retained raw native stdout bytes for one original attempt.
pub const STDOUT_LIMIT: u64 = 8 * 1024 * 1024;
/// Maximum retained raw native stderr bytes for one original attempt.
pub const STDERR_LIMIT: u64 = 1024 * 1024;
/// Permanent conservative spool, journal and control reservation per attempt.
pub const RESERVED_BYTES: i64 = 26 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectIdentity {
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
}

fn identity(fd: &impl AsFd, directory: bool) -> Result<ObjectIdentity> {
    let s = fstat(fd)?;
    ensure!(
        FileType::from_raw_mode(s.st_mode)
            == if directory {
                FileType::Directory
            } else {
                FileType::RegularFile
            }
            && s.st_uid == rustix::process::geteuid().as_raw()
            && s.st_mode & 0o077 == 0
            && (directory || s.st_nlink == 1),
        "capture object ownership/type conflict"
    );
    Ok(ObjectIdentity {
        device: s.st_dev as u64,
        inode: s.st_ino as u64,
        uid: s.st_uid as u32,
        mode: s.st_mode as u32,
    })
}

pub(crate) fn object_name(key: &str, kind: &str) -> Result<String> {
    ensure!(
        !key.is_empty()
            && key.len() <= 100
            && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid original capture key"
    );
    Ok(match kind {
        "launch" => format!("lock-{key}"),
        "journal" => key.to_owned(),
        "scratch" => format!("{key}.scratch"),
        "custody" | "effects" | "stdout" | "stderr" => format!("{kind}-{key}"),
        _ => anyhow::bail!("unknown capture object kind"),
    })
}

pub(crate) async fn prepare_intent_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    c: &Correlation,
    key: &str,
    now: i64,
) -> Result<()> {
    object_name(key, "custody")?;
    sqlx::query("INSERT INTO runtime_capture_intents(attempt,correlation,object_key,reserved_bytes,created,node,boot_id) VALUES(?,?,?,?,?,(SELECT id FROM node),?)")
        .bind(&c.attempt).bind(serde_json::to_string(c)?).bind(key).bind(RESERVED_BYTES).bind(now)
        .bind(current_boot()?)
        .execute(&mut **tx).await?;
    Ok(())
}

pub(crate) async fn original_key(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<Option<String>> {
    let row: Option<(String, String, String, bool)> = sqlx::query_as(
        "SELECT correlation,object_key,boot_id,node=(SELECT id FROM node) FROM runtime_capture_intents WHERE attempt=?",
    )
    .bind(&c.attempt)
    .fetch_optional(store.pool())
    .await?;
    row.map(|(canonical, key, boot, home)| {
        ensure!(
            home && boot == current_boot()?,
            "original capture installation or boot changed"
        );
        ensure!(
            serde_json::from_str::<Correlation>(&canonical)? == *c,
            "capture intent correlation conflict"
        );
        Ok(key)
    })
    .transpose()
}

/// Files are created only by the original dispatcher before worker exposure.
pub(crate) async fn create_objects(
    store: &crate::store::Store,
    c: &Correlation,
    directory: &RuntimeDirectory,
    key: &str,
) -> Result<()> {
    ensure!(
        original_key(store, c).await?.as_deref() == Some(key),
        "capture creation intent missing"
    );
    for kind in ["custody", "effects", "stdout", "stderr"] {
        let name = object_name(key, kind)?;
        let fd = openat(
            &directory.fd,
            name.as_str(),
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?;
        fsync(&fd)?;
    }
    fsync(&directory.fd)?;
    let mut tx = store.pool().begin().await?;
    for kind in [
        "launch", "custody", "effects", "stdout", "stderr", "journal", "scratch",
    ] {
        let name = object_name(key, kind)?;
        let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        if kind == "scratch" {
            flags |= OFlags::DIRECTORY;
        }
        let fd = openat(&directory.fd, name.as_str(), flags, Mode::empty())?;
        let object = identity(&fd, kind == "scratch")?;
        sqlx::query(
            "INSERT INTO runtime_capture_objects(attempt,kind,name,identity) VALUES(?,?,?,?)",
        )
        .bind(&c.attempt)
        .bind(kind)
        .bind(name)
        .bind(serde_json::to_string(&object)?)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn open_original(
    store: &crate::store::Store,
    c: &Correlation,
    kind: &str,
    writable: bool,
) -> Result<File> {
    let key = original_key(store, c)
        .await?
        .context("original capture intent absent")?;
    let (name, expected): (String, String) = sqlx::query_as(
        "SELECT name,identity FROM runtime_capture_objects WHERE attempt=? AND kind=?",
    )
    .bind(&c.attempt)
    .bind(kind)
    .fetch_one(store.pool())
    .await?;
    ensure!(
        name == object_name(&key, kind)?,
        "capture object name changed"
    );
    let directory = RuntimeDirectory::open(&store.root().join("managed-runtime"))?;
    let fd = openat(
        &directory.fd,
        name.as_str(),
        (if writable {
            OFlags::RDWR | OFlags::APPEND
        } else {
            OFlags::RDONLY
        }) | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    ensure!(
        identity(&fd, false)? == serde_json::from_str::<ObjectIdentity>(&expected)?,
        "original capture object replaced"
    );
    Ok(File::from(fd))
}

/// Lock ownership follows the open file description; no runtime client inherits it.
pub(crate) struct CustodyLock {
    file: File,
}
impl Drop for CustodyLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub(crate) async fn custody_lock(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<CustodyLock> {
    let file = open_original(store, c, "custody", true).await?;
    file.try_lock_exclusive()
        .context("original capture custodian still active")?;
    Ok(CustodyLock { file })
}

/// None denotes an original legacy segment. New segments never recreate missing locks.
pub(crate) async fn effect_lock(
    store: &crate::store::Store,
    c: &Correlation,
    exclusive: bool,
) -> Result<Option<CustodyLock>> {
    if original_key(store, c).await?.is_none() {
        return Ok(None);
    }
    let file = open_original(store, c, "effects", true).await?;
    if exclusive {
        file.try_lock_exclusive()?;
    } else {
        FileExt::try_lock_shared(&file)?;
    }
    Ok(Some(CustodyLock { file }))
}

pub(crate) async fn verify_launch_object(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<()> {
    if original_key(store, c).await?.is_some() {
        drop(open_original(store, c, "launch", false).await?);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContainmentCreation {
    pub(crate) root: std::path::PathBuf,
    pub(crate) key: String,
    parent: ObjectIdentity,
}

pub(crate) async fn record_containment_creation(
    store: &crate::store::Store,
    c: &Correlation,
    root: &std::path::Path,
    key: &str,
) -> Result<()> {
    let parent = RuntimeDirectory::open(root)?;
    let creation = ContainmentCreation {
        root: root.into(),
        key: key.into(),
        parent: identity(&parent.fd, true)?,
    };
    let body = serde_json::to_string(&creation)?;
    let changed=sqlx::query("UPDATE runtime_capture_intents SET containment_creation=? WHERE attempt=? AND object_key=? AND containment_creation IS NULL AND exposed=0")
        .bind(body).bind(&c.attempt).bind(key).execute(store.pool()).await?.rows_affected();
    ensure!(
        changed == 1,
        "original containment creation already consumed"
    );
    Ok(())
}

pub(crate) async fn original_creation(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<ContainmentCreation> {
    let key = original_key(store, c)
        .await?
        .context("capture creation intent absent")?;
    let body: String = sqlx::query_scalar(
        "SELECT containment_creation FROM runtime_capture_intents WHERE attempt=? AND exposed=0",
    )
    .bind(&c.attempt)
    .fetch_one(store.pool())
    .await?;
    let creation: ContainmentCreation = serde_json::from_str(&body)?;
    ensure!(creation.key == key, "original creation key changed");
    let parent = RuntimeDirectory::open(&creation.root)?;
    ensure!(
        identity(&parent.fd, true)? == creation.parent,
        "original containment parent replaced"
    );
    Ok(creation)
}

pub(crate) struct CaptureFiles {
    _lock: CustodyLock,
    pub(crate) stdout: File,
    pub(crate) stderr: File,
    journal: File,
}
impl CaptureFiles {
    pub(crate) async fn open(store: &crate::store::Store, c: &Correlation) -> Result<Self> {
        let lock = custody_lock(store, c).await?;
        Ok(Self {
            _lock: lock,
            stdout: open_original(store, c, "stdout", true).await?,
            stderr: open_original(store, c, "stderr", true).await?,
            journal: open_original(store, c, "journal", true).await?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SpoolSeal {
    identity: ObjectIdentity,
    bytes: u64,
    digest: ContentDigest,
}
fn seal_file(file: &mut File, limit: u64) -> Result<SpoolSeal> {
    file.sync_all()?;
    let original = identity(file, false)?;
    ensure!(
        file.metadata()?.len() <= limit,
        "capture spool exceeds reserved limit"
    );
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    (&mut *file).take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit && bytes.len() as u64 == file.metadata()?.len(),
        "capture spool changed during seal"
    );
    Ok(SpoolSeal {
        identity: original,
        bytes: bytes.len() as u64,
        digest: ContentDigest::of_bytes(&bytes),
    })
}

/// Classification records what was retained, never a qualification or source outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureDisposition {
    /// All trusted stream/input/wait facts and strict terminal were observed.
    Complete,
    /// Native execution or capture I/O failed; no positive terminal is authorized.
    Failed,
    /// The bounded bytes violated framing or output limits.
    Invalid,
    /// Custody was lost; only the retained original prefix is known.
    Interrupted,
}
impl CaptureDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Invalid => "invalid",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureEvidence {
    pub(crate) correlation: Correlation,
    pub(crate) disposition: CaptureDisposition,
    stdout: SpoolSeal,
    stderr: SpoolSeal,
    journal: SpoolSeal,
    pub(crate) journal_sequence: Option<u64>,
    pub(crate) terminal: Option<String>,
    pub(crate) terminal_digest: Option<ContentDigest>,
    pub(crate) control: Option<serde_json::Value>,
}

/// Outcome fields supplied by capture collection or recovery.
pub(crate) struct CaptureOutcome {
    pub(crate) disposition: CaptureDisposition,
    pub(crate) journal_sequence: Option<u64>,
    pub(crate) terminal: Option<String>,
    pub(crate) control: Option<serde_json::Value>,
}

pub(crate) async fn seal_capture(
    store: &crate::store::Store,
    c: &Correlation,
    files: &mut CaptureFiles,
    outcome: CaptureOutcome,
    now: i64,
) -> Result<String> {
    let CaptureOutcome {
        disposition,
        journal_sequence,
        terminal,
        control,
    } = outcome;
    ensure!(
        terminal.as_ref().is_none_or(|s| s.len() <= 24 * 1024),
        "capture terminal too large"
    );
    ensure!(
        disposition == CaptureDisposition::Complete || terminal.is_none(),
        "noncomplete capture cannot retain a positive terminal"
    );
    ensure!(
        disposition != CaptureDisposition::Complete
            || (journal_sequence.is_some() && terminal.is_some() && control.is_some()),
        "complete capture lacks original terminal/control evidence"
    );
    let terminal_digest = terminal
        .as_ref()
        .map(|text| ContentDigest::of_bytes(text.as_bytes()));
    let evidence = CaptureEvidence {
        correlation: c.clone(),
        disposition,
        stdout: seal_file(&mut files.stdout, STDOUT_LIMIT)?,
        stderr: seal_file(&mut files.stderr, STDERR_LIMIT)?,
        journal: seal_file(&mut files.journal, crate::managed_runtime::JOURNAL_LIMIT)?,
        journal_sequence,
        terminal,
        terminal_digest,
        control,
    };
    let encoded = serde_json::to_string(&evidence)?;
    ensure!(
        encoded.len() <= 1024 * 1024,
        "capture evidence exceeds reservation"
    );
    let mut tx = store.pool().begin().await?;
    let prior: Option<(String, String)> =
        sqlx::query_as("SELECT id,evidence FROM runtime_capture_seals WHERE attempt=?")
            .bind(&c.attempt)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some((id, prior)) = prior {
        ensure!(prior == encoded, "capture seal conflict");
        return Ok(id);
    }
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO runtime_capture_seals(attempt,id,disposition,evidence,created) VALUES(?,?,?,?,?)")
        .bind(&c.attempt).bind(&id).bind(disposition.as_str()).bind(encoded).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(id)
}

pub(crate) async fn read_seal_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    c: &Correlation,
) -> Result<(String, CaptureEvidence)> {
    let (id, disposition, body): (String, String, String) =
        sqlx::query_as("SELECT id,disposition,evidence FROM runtime_capture_seals WHERE attempt=?")
            .bind(&c.attempt)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(body.len() <= 1024 * 1024, "capture evidence exceeds bounds");
    let evidence: CaptureEvidence = serde_json::from_str(&body)?;
    ensure!(
        evidence.correlation == *c && evidence.disposition.as_str() == disposition,
        "capture seal identity conflict"
    );
    ensure!(
        evidence.disposition == CaptureDisposition::Complete || evidence.terminal.is_none(),
        "invalid positive capture evidence"
    );
    ensure!(
        evidence.terminal_digest
            == evidence
                .terminal
                .as_ref()
                .map(|text| ContentDigest::of_bytes(text.as_bytes())),
        "capture terminal digest conflict"
    );
    ensure!(
        evidence.disposition != CaptureDisposition::Complete
            || (evidence.journal_sequence.is_some()
                && evidence.terminal.is_some()
                && evidence.control.is_some()),
        "complete capture evidence incomplete"
    );
    Ok((id, evidence))
}

/// Verify original retained bytes after all writers are fenced; do not trust SQL hashes alone.
pub(crate) async fn verify_sealed_files(
    store: &crate::store::Store,
    c: &Correlation,
    files: &mut CaptureFiles,
) -> Result<String> {
    let mut tx = store.pool().begin().await?;
    let (id, evidence) = read_seal_tx(&mut tx, c).await?;
    tx.commit().await?;
    let stdout = seal_file(&mut files.stdout, STDOUT_LIMIT)?;
    let stderr = seal_file(&mut files.stderr, STDERR_LIMIT)?;
    let journal = seal_file(&mut files.journal, crate::managed_runtime::JOURNAL_LIMIT)?;
    ensure!(
        serde_json::to_value(stdout)? == serde_json::to_value(evidence.stdout)?
            && serde_json::to_value(stderr)? == serde_json::to_value(evidence.stderr)?
            && serde_json::to_value(journal)? == serde_json::to_value(evidence.journal)?,
        "retained original capture bytes changed"
    );
    Ok(id)
}

pub(crate) fn read_spool(file: &mut File, limit: u64) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    (&mut *file).take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "capture spool too large");
    Ok(bytes)
}

#[derive(Debug)]
struct StreamFailure {
    disposition: CaptureDisposition,
    detail: String,
}
impl std::fmt::Display for StreamFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.detail)
    }
}
impl std::error::Error for StreamFailure {}

pub(crate) fn invalid_capture(detail: impl Into<String>) -> anyhow::Error {
    StreamFailure {
        disposition: CaptureDisposition::Invalid,
        detail: detail.into(),
    }
    .into()
}

fn failed_io(error: std::io::Error) -> anyhow::Error {
    StreamFailure {
        disposition: CaptureDisposition::Failed,
        detail: error.to_string(),
    }
    .into()
}

/// Persist actual collection failure before stopping; EOF and a positive terminal stay absent.
pub(crate) async fn record_failure_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    c: &Correlation,
    error: &anyhow::Error,
    now: i64,
) -> Result<()> {
    let failure = error.downcast_ref::<StreamFailure>();
    let disposition = failure.map_or(CaptureDisposition::Failed, |e| e.disposition);
    let detail: String = error.to_string().chars().take(1024).collect();
    sqlx::query(
        "INSERT INTO runtime_capture_failures(attempt,disposition,detail,created) VALUES(?,?,?,?)",
    )
    .bind(&c.attempt)
    .bind(disposition.as_str())
    .bind(detail)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn retained_failure(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<Option<CaptureDisposition>> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT disposition FROM runtime_capture_failures WHERE attempt=?")
            .bind(&c.attempt)
            .fetch_optional(store.pool())
            .await?;
    value
        .map(|value| match value.as_str() {
            "failed" => Ok(CaptureDisposition::Failed),
            "invalid" => Ok(CaptureDisposition::Invalid),
            _ => anyhow::bail!("unknown protected capture failure"),
        })
        .transpose()
}

#[cfg(target_os = "linux")]
pub(crate) async fn drain<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    file: &mut File,
    limit: u64,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    ensure!(
        file.metadata()?.len() == 0,
        "native capture cannot append another run"
    );
    let mut total = 0u64;
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader.read(&mut chunk).await.map_err(failed_io)?;
        if n == 0 {
            file.sync_all().map_err(failed_io)?;
            return Ok(());
        }
        if total + (n as u64) > limit {
            return Err(StreamFailure {
                disposition: CaptureDisposition::Invalid,
                detail: "native capture overflow".into(),
            }
            .into());
        }
        file.write_all(&chunk[..n]).map_err(failed_io)?;
        file.sync_data().map_err(failed_io)?;
        total += n as u64;
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessIdentity {
    boot: String,
    pid: u32,
    start: u64,
}
#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Result<ProcessIdentity> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    ensure!(
        stat.len() <= 16384 && boot.len() <= 128,
        "process identity too large"
    );
    let start = stat
        .rsplit_once(')')
        .context("invalid process stat")?
        .1
        .split_whitespace()
        .nth(19)
        .context("missing process start")?
        .parse()?;
    Ok(ProcessIdentity {
        boot: boot.trim().into(),
        pid,
        start,
    })
}
#[cfg(target_os = "linux")]
pub(crate) async fn record_process(
    store: &crate::store::Store,
    c: &Correlation,
    role: &str,
    pid: u32,
) -> Result<()> {
    let mut tx = store.pool().begin().await?;
    record_process_tx(&mut tx, c, role, pid).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) async fn record_process_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    c: &Correlation,
    role: &str,
    pid: u32,
) -> Result<()> {
    let body = serde_json::to_string(&process_identity(pid)?)?;
    let old: Option<String> = sqlx::query_scalar(
        "SELECT identity FROM runtime_capture_processes WHERE attempt=? AND role=?",
    )
    .bind(&c.attempt)
    .bind(role)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(old) = old {
        ensure!(old == body, "original capture process changed");
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO runtime_capture_processes(attempt,role,identity,created) VALUES(?,?,?,?)",
    )
    .bind(&c.attempt)
    .bind(role)
    .bind(body)
    .bind(crate::now()?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) async fn authenticate_process(
    store: &crate::store::Store,
    c: &Correlation,
    role: &str,
    pid: u32,
) -> Result<()> {
    let body: String = sqlx::query_scalar(
        "SELECT identity FROM runtime_capture_processes WHERE attempt=? AND role=?",
    )
    .bind(&c.attempt)
    .bind(role)
    .fetch_one(store.pool())
    .await?;
    ensure!(
        serde_json::from_str::<ProcessIdentity>(&body)? == process_identity(pid)?,
        "unregistered original capture process"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) async fn stop_original_custodian(
    store: &crate::store::Store,
    c: &Correlation,
) -> Result<()> {
    let body: Option<String> = sqlx::query_scalar(
        "SELECT identity FROM runtime_capture_processes WHERE attempt=? AND role='custodian'",
    )
    .bind(&c.attempt)
    .fetch_optional(store.pool())
    .await?;
    let Some(body) = body else {
        return Ok(());
    };
    let original: ProcessIdentity = serde_json::from_str(&body)?;
    let pid = rustix::process::Pid::from_raw(i32::try_from(original.pid)?)
        .context("invalid custodian pid")?;
    let fd = match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::SRCH) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        process_identity(original.pid)? == original,
        "custodian pid or boot identity changed"
    );
    match rustix::process::pidfd_send_signal(&fd, rustix::process::Signal::KILL) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(error) => return Err(error.into()),
    }
    let death = tokio::io::unix::AsyncFd::new(fd)?;
    let _ready = tokio::time::timeout(std::time::Duration::from_secs(10), death.readable())
        .await
        .context("original custodian death was not observed")??;
    Ok(())
}

fn current_boot() -> Result<String> {
    #[cfg(target_os = "linux")]
    {
        let value = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        ensure!(
            !value.trim().is_empty() && value.len() <= 128,
            "invalid runtime boot identity"
        );
        Ok(value.trim().into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok("unsupported-host".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::OpenOptionsExt;

    fn private_file(path: &std::path::Path) -> File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap()
    }

    #[test]
    fn retained_spool_rejects_hardlink_alias_and_preserves_original_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("stdout");
        let mut file = private_file(&path);
        file.write_all(b"original prefix").unwrap();
        let before = seal_file(&mut file, STDOUT_LIMIT).unwrap();
        std::fs::hard_link(&path, temp.path().join("alias")).unwrap();
        assert!(seal_file(&mut file, STDOUT_LIMIT).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"original prefix");
        std::fs::remove_file(temp.path().join("alias")).unwrap();
        let after = seal_file(&mut file, STDOUT_LIMIT).unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
    }

    #[test]
    fn original_object_identity_detects_path_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("spool");
        let file = private_file(&path);
        let original = identity(&file, false).unwrap();
        std::fs::rename(&path, temp.path().join("retained")).unwrap();
        let replacement = private_file(&path);
        assert_ne!(identity(&replacement, false).unwrap(), original);
        assert_eq!(identity(&file, false).unwrap(), original);
    }

    #[test]
    fn shared_effect_writers_exclude_cleanup_until_all_release() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("effects");
        let a = private_file(&path);
        let b = File::options().read(true).write(true).open(&path).unwrap();
        let cleanup = File::options().read(true).write(true).open(&path).unwrap();
        FileExt::try_lock_shared(&a).unwrap();
        FileExt::try_lock_shared(&b).unwrap();
        assert!(cleanup.try_lock_exclusive().is_err());
        drop(CustodyLock { file: a });
        assert!(cleanup.try_lock_exclusive().is_err());
        drop(CustodyLock { file: b });
        cleanup.try_lock_exclusive().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn actual_pipe_eof_retains_exact_binary_bytes_and_write_failure_is_failed() {
        use tokio::io::AsyncWriteExt;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("stdout");
        let mut file = private_file(&path);
        let bytes = [0, 0xff, b'\n', b'x'];
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(&bytes).await.unwrap();
        drop(writer);
        drain(reader, &mut file, 32).await.unwrap();
        assert_eq!(read_spool(&mut file, 32).unwrap(), bytes);
        let seal = seal_file(&mut file, 32).unwrap();
        assert_eq!(seal.bytes, 4);
        assert_eq!(seal.digest, ContentDigest::of_bytes(&bytes));
        let failed_path = temp.path().join("failed");
        drop(private_file(&failed_path));
        let mut readonly = File::open(&failed_path).unwrap();
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(&bytes).await.unwrap();
        drop(writer);
        let error = drain(reader, &mut readonly, 32).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<StreamFailure>().unwrap().disposition,
            CaptureDisposition::Failed
        );
        assert!(std::fs::read(&failed_path).unwrap().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn overflow_retains_a_bounded_failed_prefix_without_rewriting_it() {
        use tokio::io::AsyncWriteExt;
        let temp = tempfile::tempdir().unwrap();
        let mut file = private_file(&temp.path().join("stdout"));
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(&[b'x'; 33]).await.unwrap();
        drop(writer);
        let error = drain(reader, &mut file, 32).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<StreamFailure>().unwrap().disposition,
            CaptureDisposition::Invalid
        );
        assert!(file.metadata().unwrap().len() <= 32);
        let sealed = seal_file(&mut file, 32).unwrap();
        assert!(sealed.bytes <= 32);
    }
}

pub(crate) async fn validate_closure_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    c: &Correlation,
    receipt: &str,
) -> Result<()> {
    let (body, effects): (String, String) =
        sqlx::query_as("SELECT evidence,effect_set FROM runtime_closures WHERE id=? AND attempt=?")
            .bind(receipt)
            .bind(&c.attempt)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(body.len() <= 1024 * 1024, "closure evidence too large");
    let value: serde_json::Value = serde_json::from_str(&body)?;
    if value.get("schema_version").is_none() {
        return Ok(());
    } // Authentic legacy reader retained.
    ensure!(
        value["schema_version"] == 2
            && value["correlation"] == serde_json::to_value(c)?
            && value["effect_set"].as_str() == Some(effects.as_str()),
        "capture closure provenance conflict"
    );
    let (seal, _) = read_seal_tx(tx, c).await?;
    ensure!(
        value["capture_seal"].as_str() == Some(seal.as_str()),
        "closure capture seal conflict"
    );
    let (containment,exposed,launched,admitted):(Option<String>,bool,bool,bool)=sqlx::query_as("SELECT s.containment,i.exposed,s.launch_committed,a.admitted FROM runtime_segments s JOIN runtime_capture_intents i USING(attempt) JOIN execution_attempts a ON a.id=s.attempt WHERE s.attempt=? AND s.tombstoned=1 AND s.state='quiescent'")
        .bind(&c.attempt).fetch_one(&mut **tx).await?;
    match containment {
        Some(body) => ensure!(
            value["quiescent_containment"]["identity"]
                == serde_json::from_str::<serde_json::Value>(&body)?,
            "original quiescent identity conflict"
        ),
        None => ensure!(
            !exposed && !launched && !admitted && value["quiescent_containment"].is_null(),
            "missing original closure identity"
        ),
    }
    Ok(())
}

/// One selected original inspection opportunity. Scheduler settlement and physical
/// removal remain separate owner checks; this value grants no native start.
#[derive(Debug)]
pub(crate) struct ReclamationReservation {
    correlation: Correlation,
}

impl ReclamationReservation {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.correlation
    }

    pub(crate) fn original_attempt_id(&self) -> &str {
        &self.correlation().attempt
    }

    pub(crate) fn into_correlation(self) -> Correlation {
        self.correlation
    }
}

#[derive(sqlx::FromRow)]
struct PendingReclamation {
    attempt: String,
    correlation: String,
    group_name: String,
    task: String,
    fence: i64,
    dispatch_key: String,
    original_home: bool,
    closure: String,
    effect_set: String,
    evidence: String,
}

async fn pending_reclamation_after_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group: &str,
    after: Option<&str>,
) -> Result<Option<PendingReclamation>> {
    // The permanent installation capture reservation bounds this entire inventory
    // to nine originals. Keep this SQL in its Runtime owner, not the controller.
    Ok(sqlx::query_as("SELECT i.attempt,i.correlation,s.group_name,s.task,s.fence,s.dispatch_key,i.node=(SELECT id FROM node) AS original_home,c.id AS closure,c.effect_set,c.evidence FROM runtime_capture_intents i JOIN runtime_closures c USING(attempt) JOIN runtime_segments s USING(attempt) JOIN groups g ON g.name=s.group_name WHERE g.name=? AND g.home_machine=(SELECT id FROM node) AND NOT EXISTS(SELECT 1 FROM runtime_capture_reclamations r WHERE r.attempt=i.attempt) AND (? IS NULL OR i.attempt COLLATE BINARY > ?) ORDER BY i.attempt COLLATE BINARY LIMIT 1")
        .bind(group).bind(after).bind(after).fetch_optional(&mut **tx).await?)
}

/// Reserve at most one historical original using Scheduler's durable ID cursor.
/// Caller owns the controller/home writer fence and commits this reservation with
/// the returned original ID before I/O. Errors never become empty or skip a row.
/// This function neither commits nor changes any slot, charge, closure or deadline.
pub(crate) async fn reserve_reclamation_after_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group: &str,
    after_attempt: Option<&str>,
    now: i64,
) -> Result<Option<ReclamationReservation>> {
    ensure!(now >= 0, "invalid reclamation observation time");
    ensure!(
        after_attempt.is_none_or(|id| !id.is_empty() && id.len() <= 128),
        "invalid original reclamation cursor"
    );
    let mut pending = pending_reclamation_after_tx(tx, group, after_attempt).await?;
    if pending.is_none() && after_attempt.is_some() {
        // Only a genuine empty first probe may wrap, exactly once.
        pending = pending_reclamation_after_tx(tx, group, None).await?;
    }
    let Some(row) = pending else {
        return Ok(None);
    };
    ensure!(
        row.original_home
            && !row.attempt.is_empty()
            && row.attempt.len() <= 128
            && row.correlation.len() <= 4096,
        "invalid original reclamation identity"
    );
    let c: Correlation = serde_json::from_str(&row.correlation)?;
    ensure!(
        c.attempt == row.attempt
            && c.group == group
            && c.group == row.group_name
            && c.task == row.task
            && c.fence == row.fence
            && c.dispatch_key == row.dispatch_key,
        "original reclamation correlation conflict"
    );
    ensure!(
        row.evidence.len() <= 1024 * 1024,
        "closure evidence too large"
    );
    crate::runtime_effects::reject_duplicate_json_keys(row.evidence.as_bytes())?;
    let evidence: serde_json::Value = serde_json::from_str(&row.evidence)?;
    // A capture intent is an R9 original; it cannot take the legacy closure branch.
    // Genuine pre-R9 historical readers elsewhere remain unchanged.
    ensure!(
        evidence["schema_version"] == 2,
        "capture reclamation requires its original closure format"
    );
    validate_closure_tx(tx, &c, &row.closure).await?;
    ensure!(
        crate::runtime_effects::effect_set_reconciled_tx(tx, &c.attempt, &row.effect_set).await?,
        "original reclamation effect set is not reconciled"
    );
    let changed = sqlx::query(
        "UPDATE runtime_capture_intents SET reclaim_checked=max(reclaim_checked,?) WHERE attempt=?",
    )
    .bind(now)
    .bind(&c.attempt)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    ensure!(changed == 1, "original reclamation reservation disappeared");
    Ok(Some(ReclamationReservation { correlation: c }))
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod reclamation_tests {
    use super::*;
    use crate::{
        execution::{self, Checked, RuntimeGate, RuntimeTarget},
        managed_runtime::{LaunchLock, NativeJournal},
        states::TaskState,
        store::Store,
        task_graph::{
            AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract,
            Criterion, TaskCreate, TaskDraft,
        },
        work::WorkDraft,
    };
    use std::{collections::BTreeMap, os::unix::fs::DirBuilderExt};

    // Bookkeeping fixture only. Real model/claim/dispatch APIs create the original
    // attempt. No managed target is enabled and no native/cgroup proof is minted.
    struct ClaimFixture(RuntimeTarget);
    impl RuntimeGate for ClaimFixture {
        async fn target(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Option<RuntimeTarget>> {
            Ok(Some(self.0.clone()))
        }
        async fn current(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &Correlation,
            _: &RuntimeTarget,
            _: execution::CurrentUse,
            _: i64,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn closed(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &Correlation,
            _: &RuntimeTarget,
            _: &str,
        ) -> Result<Option<execution::ClosedRuntime>> {
            Ok(None)
        }
        async fn observation(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &Correlation,
            _: &RuntimeTarget,
            _: &str,
        ) -> Result<Option<execution::RuntimeObservation>> {
            Ok(None)
        }
    }

    struct Original {
        correlation: Correlation,
    }

    async fn fixture_store() -> Result<(tempfile::TempDir, Store)> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(&temp.path().canonicalize()?, true).await?;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(store.root().join("managed-runtime"))?;
        Ok((temp, store))
    }

    async fn prepare_original(store: &Store, group: &str, task: &str) -> Result<Correlation> {
        let storage_path = store.root().join("managed-runtime");
        if !storage_path.exists() {
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&storage_path)?;
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM groups WHERE name=?)")
            .bind(group)
            .fetch_one(store.pool())
            .await?;
        if !exists {
            store.enroll(group, None).await?;
        }
        for actor in ["reclaim-writer", "reclaim-worker"] {
            if store.mailbox(group, actor).await.is_err() {
                store.register(group, actor, false).await?;
            }
        }
        let writer = store.mailbox(group, "reclaim-writer").await?;
        let worker = store.mailbox(group, "reclaim-worker").await?;
        store
            .task_create(
                &writer,
                TaskCreate {
                    key: format!("create-{task}"),
                    reason: "reclamation transaction fixture".into(),
                    expected_parent_versions: BTreeMap::new(),
                    draft: TaskDraft {
                        work: WorkDraft {
                            id: task.into(),
                            scope: "artifact".into(),
                            owner: "reclaim-worker".into(),
                            state: TaskState::Ready,
                            next_action: "produce artifact".into(),
                            deadline: None,
                            evidence: vec![],
                        },
                        contract: Contract {
                            deliverable: "artifact".into(),
                            criteria: vec![Criterion {
                                id: "artifact".into(),
                                description: "artifact exists".into(),
                            }],
                            allowed_scope: vec!["artifact".into()],
                            completion: Completion::WriterAcceptance,
                            allow_delegation: false,
                            allow_input_invalidation: true,
                            budget: Budget {
                                max_attempts: 1,
                                max_elapsed_seconds: 600,
                                max_cost: None,
                            },
                        },
                        authorization: Authorization {
                            state: AuthorityState::Authorized,
                            source: AuthoritySource::Direct {
                                authority_ref: "fixture-only".into(),
                            },
                            approved_scope: vec!["artifact".into()],
                            reason: "test-only permission".into(),
                        },
                        requirements: vec![],
                        parent: None,
                    },
                },
                100,
            )
            .await?;
        let target = RuntimeTarget {
            identity: format!("reclaim-{group}-{task}"),
            concurrency_key: format!("reclaim-{group}-{task}"),
            generation: 1,
            profile: "read_only".into(),
            durable_dedupe: true,
            cost_caps: BTreeMap::new(),
        };
        let runtime = ClaimFixture(target.clone());
        let mut tx = store.pool().begin().await?;
        execution::sync_model_tx(&mut tx, group, &[task.into()], 100).await?;
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM execution_tasks WHERE group_name=? AND task=?",
        )
        .bind(group)
        .bind(task)
        .fetch_one(&mut *tx)
        .await?;
        let c = match execution::claim_attempt_tx(
            &mut tx,
            &runtime,
            &execution::ClaimRequest {
                group: group.into(),
                task: task.into(),
                revision,
                key: format!("capture-fixture:{task}:{revision}"),
            },
            100,
        )
        .await?
        {
            Checked::Ready(c) => c,
            Checked::Held(holds) => anyhow::bail!(
                "fixture original claim held: group={group} task={task} revision={revision} now=100 holds={holds:?}"
            ),
        };
        // All originals share setup time 100. Advancing the group clock here
        // would make the next original's claim a backward clock observation.
        if let Checked::Held(holds) =
            execution::expose_dispatch_tx(&mut tx, &runtime, &c, "fixture", 1, 100).await?
        {
            anyhow::bail!(
                "fixture original exposure held: group={group} task={task} revision={revision} now=100 holds={holds:?}"
            );
        }
        sqlx::query("INSERT INTO runtime_targets(id,group_name,name,current_generation,enabled) VALUES(?,?,?,1,0)")
            .bind(&target.identity).bind(group).bind(task).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO runtime_target_versions(target,generation,owner,owner_binding,client,profile,concurrency_key,specification,policy_digest,executable_digest,created) VALUES(?,1,'reclaim-worker',?,'codex','read_only',?,'{}',?,?,100)")
            .bind(&target.identity).bind(worker.binding_version).bind(&target.concurrency_key)
            .bind("0".repeat(64)).bind("0".repeat(64)).execute(&mut *tx).await?;
        let key = format!("fixture-{}", c.attempt);
        sqlx::query("INSERT INTO runtime_segments(attempt,group_name,task,fence,dispatch_key,target,target_generation,canonical_request,journal_key,state,created) SELECT a.id,a.group_name,a.task,a.fence,a.dispatch_key,?,1,d.request,?,'starting',100 FROM execution_attempts a JOIN execution_dispatches d ON d.attempt=a.id WHERE a.id=?")
            .bind(&target.identity).bind(&key).bind(&c.attempt).execute(&mut *tx).await?;
        prepare_intent_tx(&mut tx, &c, &key, 100).await?;
        tx.commit().await?;
        let storage = RuntimeDirectory::open(&store.root().join("managed-runtime"))?;
        let launch = LaunchLock::create(&storage, &object_name(&key, "launch")?, &c)?;
        drop(NativeJournal::create(&storage, &key)?);
        std::fs::DirBuilder::new().mode(0o700).create(
            store
                .root()
                .join("managed-runtime")
                .join(object_name(&key, "scratch")?),
        )?;
        create_objects(store, &c, &storage, &key).await?;
        drop(launch);
        Ok(c)
    }

    /// Actual never-exposed Runtime recovery produces the closure. The only
    /// injected outcome is a negative database write fault after real settlement.
    /// The caller supplies an original private cgroup-v2 parent; no child is made.
    pub(crate) async fn pending_original(
        store: &Store,
        group: &str,
        task: &str,
        cgroup_root: &std::path::Path,
    ) -> Result<Correlation> {
        let c = prepare_original(store, group, task).await?;
        let key = original_key(store, &c)
            .await?
            .context("original capture missing")?;
        record_containment_creation(store, &c, cgroup_root, &key).await?;
        sqlx::query("CREATE TRIGGER IF NOT EXISTS test_hold_reclamation BEFORE INSERT ON runtime_capture_reclamations BEGIN SELECT RAISE(ABORT,'test reclamation write fault'); END")
            .execute(store.pool()).await?;
        // Reconciliation may report the injected error through its uncertainty
        // path. Assert actual retained outcomes, never turn that error into proof.
        // Keep recovery at the shared setup time too; subsequent originals in
        // this group must pass the real Scheduler clock guard without a reset.
        let result = crate::managed_runtime::reconcile_managed(store, &c, true, 100).await;
        let closure: Option<String> =
            sqlx::query_scalar("SELECT id FROM runtime_closures WHERE attempt=?")
                .bind(&c.attempt)
                .fetch_optional(store.pool())
                .await?;
        let closure =
            closure.with_context(|| format!("genuine recovery did not close: {result:?}"))?;
        let mut tx = store.pool().begin().await?;
        validate_closure_tx(&mut tx, &c, &closure).await?;
        let pending: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS(SELECT 1 FROM runtime_capture_reclamations WHERE attempt=?)",
        )
        .bind(&c.attempt)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            pending,
            "negative reclamation fault did not retain original"
        );
        let settled_unexposed: bool = sqlx::query_scalar("SELECT a.state='closed' AND a.holds_slot=0 AND a.admitted=0 AND i.exposed=0 AND s.launch_committed=0 AND s.containment IS NULL FROM execution_attempts a JOIN runtime_segments s ON s.attempt=a.id JOIN runtime_capture_intents i ON i.attempt=a.id WHERE a.id=?")
            .bind(&c.attempt).fetch_one(&mut *tx).await?;
        ensure!(
            settled_unexposed,
            "actual unexposed closure did not settle its original allocation"
        );
        tx.rollback().await?;
        Ok(c)
    }

    /// Remove only the test fault; genuine Runtime replay writes reclamation.
    pub(crate) async fn release_reclamation_fault(store: &Store) -> Result<()> {
        sqlx::query("DROP TRIGGER test_hold_reclamation")
            .execute(store.pool())
            .await?;
        Ok(())
    }

    async fn original(
        store: &Store,
        group: &str,
        task: &str,
        with_closure: bool,
    ) -> Result<Original> {
        let c = if with_closure {
            let parent = std::env::var_os("AGENT_MAIL_TEST_CGROUP_ROOT")
                .context("root must provide a private original cgroup-v2 parent")?;
            pending_original(store, group, task, std::path::Path::new(&parent)).await?
        } else {
            prepare_original(store, group, task).await?
        };
        Ok(Original { correlation: c })
    }

    async fn selected(
        store: &Store,
        group: &str,
        after: Option<&str>,
        now: i64,
    ) -> Result<Option<Correlation>> {
        let mut tx = store.pool().begin().await?;
        let selection = reserve_reclamation_after_tx(&mut tx, group, after, now).await?;
        let result = selection.map(|selected| {
            assert_eq!(
                selected.original_attempt_id(),
                selected.correlation().attempt
            );
            selected.into_correlation()
        });
        tx.commit().await?;
        Ok(result)
    }

    async fn observed(store: &Store, c: &Correlation) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT reclaim_checked FROM runtime_capture_intents WHERE attempt=?",
        )
        .bind(&c.attempt)
        .fetch_one(store.pool())
        .await?)
    }

    async fn protected_state(store: &Store) -> Result<Vec<String>> {
        let mut values = Vec::new();
        for query in [
            "SELECT json_object('id',id,'state',state,'slot',holds_slot,'admitted',admitted,'closure',closure,'closed',closed_at) FROM execution_attempts ORDER BY id",
            "SELECT json_object('group',group_name,'task',task,'spent',attempts_spent,'reserved',attempts_reserved,'cost',cost_spent,'cost_reserved',cost_reserved,'unknown',unknown_cost,'anchor',anchor,'deadline',deadline) FROM execution_budgets ORDER BY group_name,task",
            "SELECT json_object('id',id,'attempt',attempt,'effect_set',effect_set,'evidence',evidence,'costs',costs) FROM runtime_closures ORDER BY id",
            "SELECT json_object('witnesses',(SELECT count(*) FROM runtime_capability_witnesses),'enabled',(SELECT coalesce(sum(enabled),0) FROM runtime_targets),'slots',(SELECT count(*) FROM execution_slots),'events',(SELECT count(*) FROM execution_events))",
        ] {
            values.extend(
                sqlx::query_scalar::<_, String>(query)
                    .fetch_all(store.pool())
                    .await?,
            );
        }
        Ok(values)
    }

    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent"]
    async fn original_ids_rotate_at_same_and_backward_time_without_settlement() -> Result<()> {
        let (_temp, store) = fixture_store().await?;
        let mut originals = Vec::new();
        for task in ["one", "two", "three"] {
            originals.push(original(&store, "g", task, true).await?);
        }
        originals.sort_by(|a, b| a.correlation.attempt.cmp(&b.correlation.attempt));
        let before = protected_state(&store).await?;
        let mut cursor: Option<String> = None;
        for round in 0..3 {
            for original in &originals {
                let c = selected(
                    &store,
                    "g",
                    cursor.as_deref(),
                    if round < 2 { 1000 } else { 999 },
                )
                .await?
                .unwrap();
                assert_eq!(c, original.correlation);
                cursor = Some(c.attempt.clone());
                assert_eq!(observed(&store, &c).await?, 1000);
            }
        }
        // Actual recovered originals remain unchanged, with disabled targets and
        // exhausted fixture elapsed time. Historical selection adds no current gate.
        assert_eq!(protected_state(&store).await?, before);
        store.close().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent"]
    async fn keyset_wraps_once_past_reclaimed_cursor_and_filters_group_and_missing_closure()
    -> Result<()> {
        let (_temp, store) = fixture_store().await?;
        let mut items = [
            original(&store, "g", "one", true).await?,
            original(&store, "g", "two", true).await?,
        ];
        items.sort_by(|a, b| a.correlation.attempt.cmp(&b.correlation.attempt));
        let other = original(&store, "other", "one", true).await?;
        let incomplete = original(&store, "g", "incomplete", false).await?;
        release_reclamation_fault(&store).await?;
        assert!(matches!(
            crate::managed_runtime::reconcile_managed(&store, &items[0].correlation, true, 103,)
                .await?,
            Checked::Ready(_)
        ));
        let reclaimed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM runtime_capture_reclamations WHERE attempt=?)",
        )
        .bind(&items[0].correlation.attempt)
        .fetch_one(store.pool())
        .await?;
        assert!(reclaimed);
        for cursor in [
            None,
            Some(items[0].correlation.attempt.as_str()),
            Some(items[1].correlation.attempt.as_str()),
            Some("zzzz"),
        ] {
            assert_eq!(
                selected(&store, "g", cursor, 104).await?.unwrap(),
                items[1].correlation
            );
        }
        assert_eq!(
            selected(&store, "other", None, 104).await?.unwrap(),
            other.correlation
        );
        assert!(
            selected(&store, "absent", Some("zzzz"), 104)
                .await?
                .is_none()
        );
        assert_eq!(observed(&store, &incomplete.correlation).await?, 0);
        assert_eq!(observed(&store, &items[0].correlation).await?, 0);
        // The original group changing home removes local inspection authority.
        sqlx::query("UPDATE groups SET home_machine='foreign-fixture-home' WHERE name='g'")
            .execute(store.pool())
            .await?;
        assert!(selected(&store, "g", None, 105).await?.is_none());
        sqlx::query("UPDATE groups SET home_machine=(SELECT id FROM node) WHERE name='g'")
            .execute(store.pool())
            .await?;
        assert!(matches!(
            crate::managed_runtime::reconcile_managed(&store, &items[1].correlation, true, 106,)
                .await?,
            Checked::Ready(_)
        ));
        for cursor in [
            None,
            Some(items[1].correlation.attempt.as_str()),
            Some("zzzz"),
        ] {
            assert!(selected(&store, "g", cursor, 107).await?.is_none());
        }
        store.close().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent"]
    async fn reservation_rollback_and_store_reopen_preserve_original_selection_and_high_water()
    -> Result<()> {
        let (temp, store) = fixture_store().await?;
        let a = original(&store, "g", "one", true).await?;
        let before = protected_state(&store).await?;
        let mut tx = store.pool().begin().await?;
        let reservation = reserve_reclamation_after_tx(&mut tx, "g", None, 500)
            .await?
            .unwrap();
        assert_eq!(reservation.correlation(), &a.correlation);
        tx.rollback().await?;
        assert_eq!(observed(&store, &a.correlation).await?, 0);
        assert_eq!(
            selected(&store, "g", None, 200).await?.unwrap(),
            a.correlation
        );
        assert_eq!(protected_state(&store).await?, before);
        store.close().await;
        let store = Store::open(&temp.path().canonicalize()?, false).await?;
        assert_eq!(observed(&store, &a.correlation).await?, 200);
        assert_eq!(
            selected(&store, "g", Some(&a.correlation.attempt), 100)
                .await?
                .unwrap(),
            a.correlation
        );
        assert_eq!(observed(&store, &a.correlation).await?, 200);
        assert_eq!(protected_state(&store).await?, before);
        store.close().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires root-supplied private cgroup-v2 parent"]
    async fn malformed_original_history_errors_without_skipping_or_updating_reservation()
    -> Result<()> {
        for fault in [
            "correlation",
            "closure",
            "legacy_closure",
            "capture",
            "effects",
            "installation",
        ] {
            let (_temp, store) = fixture_store().await?;
            let mut items = vec![
                original(&store, "g", "one", true).await?,
                original(&store, "g", "two", true).await?,
            ];
            items.sort_by(|a, b| a.correlation.attempt.cmp(&b.correlation.attempt));
            let first = &items[0].correlation;
            // Deliberate corruption in this isolated test database; production
            // immutable triggers are retained and never relaxed by the selector.
            match fault {
                "correlation" | "installation" => {
                    sqlx::query("DROP TRIGGER runtime_capture_intent_identity")
                        .execute(store.pool())
                        .await?;
                    let query = if fault == "correlation" {
                        "UPDATE runtime_capture_intents SET correlation=json_set(correlation,'$.task','different') WHERE attempt=?"
                    } else {
                        "UPDATE runtime_capture_intents SET node='foreign-fixture-node' WHERE attempt=?"
                    };
                    sqlx::query(query)
                        .bind(&first.attempt)
                        .execute(store.pool())
                        .await?;
                }
                "closure" | "legacy_closure" => {
                    sqlx::query("DROP TRIGGER runtime_closure_no_update")
                        .execute(store.pool())
                        .await?;
                    let query = if fault == "closure" {
                        "UPDATE runtime_closures SET evidence=json_set(evidence,'$.correlation.task','different') WHERE attempt=?"
                    } else {
                        "UPDATE runtime_closures SET evidence='{}' WHERE attempt=?"
                    };
                    sqlx::query(query)
                        .bind(&first.attempt)
                        .execute(store.pool())
                        .await?;
                }
                "capture" => {
                    sqlx::query("DROP TRIGGER runtime_capture_seal_no_update")
                        .execute(store.pool())
                        .await?;
                    sqlx::query("UPDATE runtime_capture_seals SET evidence=json_set(evidence,'$.correlation.dispatch_key','different') WHERE attempt=?")
                        .bind(&first.attempt).execute(store.pool()).await?;
                }
                "effects" => {
                    sqlx::query("DROP TRIGGER runtime_effect_set_no_update")
                        .execute(store.pool())
                        .await?;
                    sqlx::query(
                        "UPDATE runtime_effect_sets SET effects='[\"missing\"]' WHERE attempt=?",
                    )
                    .bind(&first.attempt)
                    .execute(store.pool())
                    .await?;
                }
                _ => unreachable!(),
            }
            let before = protected_state(&store).await?;
            for cursor in [None, Some("zzzz")] {
                assert!(selected(&store, "g", cursor, 200).await.is_err(), "{fault}");
            }
            for item in &items {
                assert_eq!(observed(&store, &item.correlation).await?, 0);
            }
            assert_eq!(protected_state(&store).await?, before);
            store.close().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_cursor_or_time_is_rejected_even_when_inventory_is_empty() -> Result<()> {
        let (_temp, store) = fixture_store().await?;
        let oversized = "é".repeat(65);
        for (cursor, now) in [(Some(""), 0), (Some(oversized.as_str()), 0), (None, -1)] {
            assert!(selected(&store, "absent", cursor, now).await.is_err());
        }
        let boundary = "é".repeat(64);
        assert!(
            selected(&store, "absent", Some(&boundary), 0)
                .await?
                .is_none()
        );
        store.close().await;
        Ok(())
    }
}
