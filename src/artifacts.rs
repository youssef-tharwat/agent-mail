//! Typed evidence references and bounded, immutable local content storage.
//! Metadata never implies acceptance. Managed bytes are group scoped and hashed
//! before publication; one cross-process lock serializes readers and maintenance.
#![allow(missing_docs)]
use crate::store::{Mailbox, Store};
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceLocation {
    Managed,
    Repository {
        repository: String,
        revision: String,
        path: String,
    },
    External {
        uri: String,
    },
    Legacy {
        reference: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDraft {
    pub id: String,
    pub location: ResourceLocation,
    pub digest: Option<String>,
    pub media_type: Option<String>,
    pub size: Option<u64>,
    pub provenance: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub group_name: String,
    pub resource: ArtifactDraft,
    pub producer: String,
    pub created: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactLink {
    Task { id: String },
    Message { id: i64 },
    RecordRevision { id: String, version: i64 },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireArtifactLink {
    Task { id: String },
    MessageGlobal { id: uuid::Uuid },
    RecordRevision { id: String, version: i64 },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactLimits {
    pub decoded_bytes: u64,
    pub stored_bytes: u64,
    pub quota_bytes: u64,
    pub temporary_bytes: u64,
}
impl Default for ArtifactLimits {
    fn default() -> Self {
        Self {
            decoded_bytes: 256 * 1024 * 1024,
            stored_bytes: 256 * 1024 * 1024,
            quota_bytes: 1024 * 1024 * 1024,
            temporary_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ArtifactAccess {
    Verified { digest: String, size: u64 },
    Unavailable { reason: String },
    Unsupported { reason: String },
    IntegrityFailure { reason: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactStats {
    pub logical_bytes: u64,
    pub unique_original_bytes: u64,
    pub physical_stored_bytes: u64,
    pub temporary_bytes: u64,
    pub reclaimable_bytes: u64,
    pub quota_usage_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PruneResult {
    pub dry_run: bool,
    pub digests: Vec<String>,
    pub orphan_digests: Vec<String>,
    pub temporary_files: Vec<String>,
    pub reclaimed_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSnapshot {
    pub artifact: Artifact,
    pub links: Vec<WireArtifactLink>,
    pub pinned: bool,
    pub revision: i64,
}

fn digest_valid(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}
fn validate(draft: &ArtifactDraft) -> Result<()> {
    crate::bounded(&draft.id, 128, "artifact ID")?;
    ensure!(!draft.id.is_empty(), "artifact ID is empty");
    ensure!(
        serde_json::to_vec(draft)?.len() <= 8192,
        "artifact metadata exceeds 8192 bytes"
    );
    if let Some(d) = &draft.digest {
        ensure!(
            digest_valid(d),
            "unsupported digest; expected sha256: and 64 lowercase hexadecimal characters"
        );
    }
    if let Some(s) = draft.size {
        ensure!(
            s <= i64::MAX as u64,
            "artifact size exceeds supported range"
        );
    }
    match &draft.location {
        ResourceLocation::Repository {
            repository,
            revision,
            path,
        } => {
            ensure!(
                !repository.is_empty() && !revision.is_empty() && !path.is_empty(),
                "repository references require repository, revision, and path"
            );
            ensure!(
                !Path::new(path).is_absolute() && !path.split('/').any(|s| s == ".."),
                "repository path must be relative without parent traversal"
            );
            validate_uri(repository)?;
        }
        ResourceLocation::External { uri } => validate_uri(uri)?,
        ResourceLocation::Legacy { reference } => {
            ensure!(!reference.is_empty(), "legacy reference is empty")
        }
        ResourceLocation::Managed => {}
    }
    Ok(())
}
fn validate_uri(uri: &str) -> Result<()> {
    ensure!(
        uri.contains("://") && !uri.starts_with("file://"),
        "resource location must be an explicit remote URI; local paths are legacy references"
    );
    let authority = uri
        .split("://")
        .nth(1)
        .unwrap_or("")
        .split('/')
        .next()
        .unwrap_or("");
    ensure!(
        !authority.is_empty()
            && !authority.contains('@')
            && !uri.contains('?')
            && !uri.contains('#'),
        "resource URI must not contain embedded credentials, query strings, or fragments"
    );
    Ok(())
}
fn group_key(group: &str) -> String {
    format!("{:x}", Sha256::digest(group.as_bytes()))
}
fn object_path(root: &Path, group: &str, digest: &str) -> Result<PathBuf> {
    ensure!(digest_valid(digest), "invalid blob digest");
    Ok(root
        .join("artifacts/objects")
        .join(group_key(group))
        .join(&digest[7..]))
}
async fn lock(root: &Path) -> Result<File> {
    let directory = root.join("artifacts");
    fs::create_dir_all(&directory)?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(directory.join("store.lock"))?;
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
            Err(e) => return Err(e.into()),
        }
    }
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
fn bounded_copy(
    reader: &mut dyn Read,
    writer: &mut dyn Write,
    limit: u64,
) -> Result<(String, u64)> {
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .context("artifact size overflow")?;
        ensure!(
            count <= limit,
            "decoded artifact exceeds size limit {limit}"
        );
        hash.update(&buffer[..n]);
        writer.write_all(&buffer[..n])?;
    }
    Ok((format!("sha256:{:x}", hash.finalize()), count))
}
async fn audit(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    id: Option<&str>,
    action: &str,
    actor: &str,
    details: &str,
    now: i64,
) -> Result<()> {
    sqlx::query("INSERT INTO artifact_audit(group_name,artifact_id,action,actor,details,changed) VALUES(?,?,?,?,?,?)").bind(group).bind(id).bind(action).bind(actor).bind(details).bind(now).execute(&mut **tx).await?;
    Ok(())
}
impl Store {
    pub async fn artifact_register(
        &self,
        actor: &Mailbox,
        draft: ArtifactDraft,
        now: i64,
    ) -> Result<Artifact> {
        ensure!(
            !matches!(draft.location, ResourceLocation::Managed),
            "managed references must be created through artifact ingest"
        );
        let _guard = lock(self.root()).await?;
        self.artifact_insert(actor, draft, now).await
    }
    async fn artifact_insert(
        &self,
        actor: &Mailbox,
        draft: ArtifactDraft,
        now: i64,
    ) -> Result<Artifact> {
        validate(&draft)?;
        let canonical = serde_json::to_string(&draft)?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        if let Some(row) =
            sqlx::query("SELECT canonical,snapshot FROM artifacts WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(&draft.id)
                .fetch_optional(&mut *tx)
                .await?
        {
            ensure!(
                row.get::<String, _>("canonical") == canonical,
                "artifact ID already exists with different metadata"
            );
            let old: Artifact = serde_json::from_str(&row.get::<String, _>("snapshot"))?;
            ensure!(
                old.producer == actor.name,
                "artifact retry producer changed"
            );
            return Ok(old);
        }
        require_home(&mut tx, &actor.group_name).await?;
        let item = Artifact {
            group_name: actor.group_name.clone(),
            resource: draft,
            producer: actor.name.clone(),
            created: now,
        };
        sqlx::query("INSERT INTO artifacts(group_name,id,snapshot,canonical,created,digest) VALUES(?,?,?,?,?,?)").bind(&item.group_name).bind(&item.resource.id).bind(serde_json::to_string(&item)?).bind(canonical).bind(now).bind(&item.resource.digest).execute(&mut *tx).await?;
        audit(
            &mut tx,
            &actor.group_name,
            Some(&item.resource.id),
            "created",
            &actor.name,
            "metadata only; no review or acceptance",
            now,
        )
        .await?;
        enqueue_artifact(&mut tx, &item, now).await?;
        failure_boundary("reference-before-commit");
        tx.commit().await?;
        Ok(item)
    }
    pub async fn artifact_ingest<R: Read>(
        &self,
        actor: &Mailbox,
        mut draft: ArtifactDraft,
        mut reader: R,
        limits: &ArtifactLimits,
        now: i64,
    ) -> Result<Artifact> {
        ensure!(
            matches!(draft.location, ResourceLocation::Managed),
            "ingest requires managed location"
        );
        validate(&draft)?;
        let _guard = lock(self.root()).await?;
        // Validate authority before spending disk or processing untrusted bytes.
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        tx.commit().await?;
        let tmp_dir = self.root().join("artifacts/tmp");
        fs::create_dir_all(&tmp_dir)?;
        let temporary_available = limits
            .temporary_bytes
            .saturating_sub(directory_bytes(&tmp_dir)?);
        let mut raw = tempfile::NamedTempFile::new_in(&tmp_dir)?;
        let (digest, original) = bounded_copy(
            &mut reader,
            raw.as_file_mut(),
            limits.decoded_bytes.min(temporary_available / 2),
        )?;
        if let Some(expected) = &draft.digest {
            ensure!(expected == &digest, "artifact digest mismatch");
        }
        if let Some(expected) = draft.size {
            ensure!(expected == original, "artifact original length mismatch");
        }
        draft.digest = Some(digest.clone());
        draft.size = Some(original);
        let mut encoded = tempfile::NamedTempFile::new_in(&tmp_dir)?;
        let mut codec = "identity-v1";
        let mut stored = original;
        if original >= 4096 {
            raw.as_file_mut().seek(SeekFrom::Start(0))?;
            let mut output = LimitedWriter {
                file: encoded.as_file_mut(),
                remaining: temporary_available.saturating_sub(original),
                exceeded: false,
            };
            let compression = (|| -> std::io::Result<()> {
                let mut encoder = zstd::stream::write::Encoder::new(&mut output, 3)?;
                std::io::copy(raw.as_file_mut(), &mut encoder)?;
                encoder.finish()?;
                Ok(())
            })();
            let capped = output.exceeded;
            if let Err(error) = compression {
                ensure!(capped, "artifact compression failed: {error}");
            }
            let compressed = encoded.as_file().metadata()?.len();
            if !capped && compressed <= original.saturating_mul(9) / 10 {
                codec = "zstd-v1";
                stored = compressed;
            }
        }
        ensure!(
            stored <= limits.stored_bytes,
            "stored artifact exceeds size limit"
        );
        let path = object_path(self.root(), &actor.group_name, &digest)?;
        let existing=sqlx::query("SELECT codec,format_version,original_size,stored_size FROM artifact_blobs WHERE group_name=? AND digest=?").bind(&actor.group_name).bind(&digest).fetch_optional(self.pool()).await?;
        if let Some(row) = existing.as_ref() {
            ensure!(
                row.get::<i64, _>("format_version") == 1,
                "unsupported codec format version"
            );
            let prior_codec: String = row.get("codec");
            let prior_size: i64 = row.get("original_size");
            let prior_stored: i64 = row.get("stored_size");
            verify_path(
                &path,
                &prior_codec,
                &digest,
                prior_size as u64,
                prior_stored as u64,
                limits,
                &mut std::io::sink(),
            )?;
        } else {
            let total: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(stored_size),0) FROM artifact_blobs WHERE group_name=?",
            )
            .bind(&actor.group_name)
            .fetch_one(self.pool())
            .await?;
            let actual_disk = directory_bytes(
                &self
                    .root()
                    .join("artifacts/objects")
                    .join(group_key(&actor.group_name)),
            )?;
            let replaceable = path.metadata().map(|m| m.len()).unwrap_or(0);
            ensure!(
                (total as u64)
                    .max(actual_disk.saturating_sub(replaceable))
                    .saturating_add(stored)
                    <= limits.quota_bytes,
                "artifact quota exceeded; protected evidence cannot be evicted; prune unreferenced objects explicitly"
            );
            let parent = path.parent().context("blob parent missing")?;
            fs::create_dir_all(parent)?;
            sync_directory(parent.parent().context("blob scope parent missing")?)?;
            sync_directory(
                parent
                    .parent()
                    .and_then(Path::parent)
                    .context("object store parent missing")?,
            )?;
            sync_directory(self.root())?;
            let published = if codec == "zstd-v1" { encoded } else { raw };
            published.as_file().sync_all()?;
            // A crash between durable bytes and metadata leaves an orphan, never a partial reference.
            published.persist(&path).map_err(|e| e.error)?;
            sync_directory(parent)?;
            failure_boundary("published");
            sqlx::query("INSERT INTO artifact_blobs(group_name,digest,codec,original_size,stored_size,created) VALUES(?,?,?,?,?,?)").bind(&actor.group_name).bind(&digest).bind(codec).bind(original as i64).bind(stored as i64).bind(now).execute(self.pool()).await?;
        }
        if let Ok(existing) = self.artifact_show(actor, &draft.id).await {
            ensure!(
                existing.resource == draft,
                "artifact ID already exists with different metadata"
            );
            return Ok(existing);
        }
        failure_boundary("blob-metadata");
        self.artifact_insert(actor, draft, now).await
    }
    pub async fn artifact_show(&self, actor: &Mailbox, id: &str) -> Result<Artifact> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let data: String =
            sqlx::query_scalar("SELECT snapshot FROM artifacts WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .context("artifact not found in group")?;
        Ok(serde_json::from_str(&data)?)
    }
    pub async fn artifact_list(
        &self,
        actor: &Mailbox,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Artifact>> {
        ensure!(
            (1..=100).contains(&limit),
            "artifact page limit must be 1..100"
        );
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT snapshot FROM artifacts WHERE group_name=? AND id>? ORDER BY id LIMIT ?",
        )
        .bind(&actor.group_name)
        .bind(after.unwrap_or(""))
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await?;
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
    pub async fn artifact_fetch<W: Write>(
        &self,
        actor: &Mailbox,
        id: &str,
        mut writer: W,
        limits: &ArtifactLimits,
    ) -> Result<()> {
        let _guard = lock(self.root()).await?;
        let item = self.artifact_show(actor, id).await?;
        ensure!(
            matches!(item.resource.location, ResourceLocation::Managed),
            "unsupported retrieval: external and repository references require their own authorized transport; legacy paths are sender local"
        );
        let digest = item
            .resource
            .digest
            .as_deref()
            .context("managed artifact digest missing")?;
        let row=sqlx::query("SELECT codec,format_version,original_size,stored_size FROM artifact_blobs WHERE group_name=? AND digest=?").bind(&actor.group_name).bind(digest).fetch_optional(self.pool()).await?.context("artifact unavailable on this machine; metadata sync does not transfer managed bytes")?;
        ensure!(
            row.get::<i64, _>("format_version") == 1,
            "unsupported codec format version"
        );
        let path = object_path(self.root(), &actor.group_name, digest)?;
        let tmp_dir = self.root().join("artifacts/tmp");
        fs::create_dir_all(&tmp_dir)?;
        let temporary_available = limits
            .temporary_bytes
            .saturating_sub(directory_bytes(&tmp_dir)?);
        ensure!(
            row.get::<i64, _>("original_size") as u64 <= temporary_available,
            "artifact verification spool exceeds temporary storage limit"
        );
        ensure!(
            item.resource.size == Some(row.get::<i64, _>("original_size") as u64),
            "artifact metadata original length mismatch"
        );
        // Verify into a bounded disk spool first: corrupt content is never returned as success.
        let mut verified = tempfile::NamedTempFile::new_in(&tmp_dir)?;
        verify_path(
            &path,
            &row.get::<String, _>("codec"),
            digest,
            row.get::<i64, _>("original_size") as u64,
            row.get::<i64, _>("stored_size") as u64,
            limits,
            verified.as_file_mut(),
        )?;
        verified.as_file_mut().seek(SeekFrom::Start(0))?;
        std::io::copy(verified.as_file_mut(), &mut writer)?;
        Ok(())
    }
    pub async fn artifact_check(
        &self,
        actor: &Mailbox,
        id: &str,
        limits: &ArtifactLimits,
    ) -> Result<ArtifactAccess> {
        let item = self.artifact_show(actor, id).await?;
        match item.resource.location {
            ResourceLocation::Managed => match self.artifact_fetch(actor,id,std::io::sink(),limits).await {
                Ok(())=>Ok(ArtifactAccess::Verified { digest:item.resource.digest.context("missing digest")?,size:item.resource.size.context("missing size")? }),
                Err(e)=>{let reason=format!("{e:#}"); if reason.contains("unavailable") || reason.contains("No such file") {Ok(ArtifactAccess::Unavailable {reason})} else if reason.contains("unsupported codec") {Ok(ArtifactAccess::Unsupported {reason})} else {Ok(ArtifactAccess::IntegrityFailure {reason})}}
            },
            ResourceLocation::Legacy {..} => Ok(ArtifactAccess::Unsupported {reason:"legacy evidence remains unverified; sender-local paths are not remotely accessible; explicit import required".into()}),
            _ => Ok(ArtifactAccess::Unavailable {reason:"external/repository reference identified; no authorized transport configured; remote durability and integrity are unverified".into()}),
        }
    }
    pub async fn artifact_link(
        &self,
        actor: &Mailbox,
        id: &str,
        target: ArtifactLink,
        now: i64,
    ) -> Result<()> {
        let _guard = lock(self.root()).await?;
        let item = self.artifact_show(actor, id).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        require_home(&mut tx, &actor.group_name).await?;
        if matches!(item.resource.location, ResourceLocation::Managed) {
            let digest = item.resource.digest.as_deref().context("missing digest")?;
            let present: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM artifact_blobs WHERE group_name=? AND digest=?)",
            )
            .bind(&actor.group_name)
            .bind(digest)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                present && object_path(self.root(), &actor.group_name, digest)?.is_file(),
                "artifact unavailable; reingest bytes before linking"
            );
        }
        ensure!(
            serde_json::to_vec(&target)?.len() <= 1024,
            "artifact link exceeds 1024 bytes"
        );
        validate_target(&mut tx, &actor.group_name, &target).await?;
        target_authority(&mut tx, actor, &target, false).await?;
        let encoded = serde_json::to_string(&target)?;
        let inserted=sqlx::query("INSERT OR IGNORE INTO artifact_links(group_name,artifact_id,target,created) VALUES(?,?,?,?)").bind(&actor.group_name).bind(id).bind(&encoded).bind(now).execute(&mut *tx).await?;
        if inserted.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(());
        }
        audit(
            &mut tx,
            &actor.group_name,
            Some(id),
            "linked",
            &actor.name,
            &encoded,
            now,
        )
        .await?;
        sqlx::query("UPDATE artifacts SET revision=revision+1 WHERE group_name=? AND id=?")
            .bind(&actor.group_name)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        enqueue_existing(&mut tx, &actor.group_name, id, now).await?;
        failure_boundary("link-before-commit");
        tx.commit().await?;
        failure_boundary("link-after-commit");
        Ok(())
    }
    pub async fn artifact_unlink(
        &self,
        actor: &Mailbox,
        id: &str,
        target: ArtifactLink,
        now: i64,
    ) -> Result<()> {
        let _guard = lock(self.root()).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        require_home(&mut tx, &actor.group_name).await?;
        target_authority(&mut tx, actor, &target, true).await?;
        let deleted = sqlx::query(
            "DELETE FROM artifact_links WHERE group_name=? AND artifact_id=? AND target=?",
        )
        .bind(&actor.group_name)
        .bind(id)
        .bind(serde_json::to_string(&target)?)
        .execute(&mut *tx)
        .await?;
        if deleted.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("UPDATE artifact_blobs SET created=? WHERE group_name=? AND digest=(SELECT digest FROM artifacts WHERE group_name=? AND id=?)").bind(now).bind(&actor.group_name).bind(&actor.group_name).bind(id).execute(&mut *tx).await?;
        audit(
            &mut tx,
            &actor.group_name,
            Some(id),
            "unlinked",
            &actor.name,
            &serde_json::to_string(&target)?,
            now,
        )
        .await?;
        sqlx::query("UPDATE artifacts SET revision=revision+1 WHERE group_name=? AND id=?")
            .bind(&actor.group_name)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        enqueue_existing(&mut tx, &actor.group_name, id, now).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn artifact_pin(
        &self,
        actor: &Mailbox,
        id: &str,
        pinned: bool,
        now: i64,
    ) -> Result<()> {
        let _guard = lock(self.root()).await?;
        self.artifact_show(actor, id).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        require_home(&mut tx, &actor.group_name).await?;
        let encoded: String =
            sqlx::query_scalar("SELECT snapshot FROM artifacts WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        let item: Artifact = serde_json::from_str(&encoded)?;
        ensure!(
            item.producer == actor.name,
            "artifact retention pins are controlled by their producer"
        );
        let current: bool =
            sqlx::query_scalar("SELECT pinned FROM artifacts WHERE group_name=? AND id=?")
                .bind(&actor.group_name)
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        if current == pinned {
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("UPDATE artifacts SET pinned=? WHERE group_name=? AND id=?")
            .bind(pinned)
            .bind(&actor.group_name)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        audit(
            &mut tx,
            &actor.group_name,
            Some(id),
            "pin",
            &actor.name,
            if pinned { "retained" } else { "released" },
            now,
        )
        .await?;
        sqlx::query("UPDATE artifacts SET revision=revision+1 WHERE group_name=? AND id=?")
            .bind(&actor.group_name)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE artifact_blobs SET created=? WHERE group_name=? AND digest=(SELECT digest FROM artifacts WHERE group_name=? AND id=?)").bind(now).bind(&actor.group_name).bind(&actor.group_name).bind(id).execute(&mut *tx).await?;
        enqueue_existing(&mut tx, &actor.group_name, id, now).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn artifact_stats(&self, actor: &Mailbox) -> Result<ArtifactStats> {
        let _guard = lock(self.root()).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let (unique,_physical):(i64,i64)=sqlx::query_as("SELECT COALESCE(SUM(original_size),0),COALESCE(SUM(stored_size),0) FROM artifact_blobs WHERE group_name=?").bind(&actor.group_name).fetch_one(&mut *tx).await?;
        let logical:i64=sqlx::query_scalar("SELECT COALESCE(SUM(b.original_size),0) FROM artifacts a JOIN artifact_blobs b ON a.group_name=b.group_name AND a.digest=b.digest WHERE a.group_name=?").bind(&actor.group_name).fetch_one(&mut *tx).await?;
        let reclaimable_digests:Vec<String>=sqlx::query_scalar("SELECT digest FROM artifact_blobs b WHERE group_name=? AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.group_name=b.group_name AND a.digest=b.digest AND (a.pinned=1 OR EXISTS(SELECT 1 FROM artifact_links l WHERE l.group_name=a.group_name AND l.artifact_id=a.id)))").bind(&actor.group_name).fetch_all(&mut *tx).await?;
        let mut reclaimable = 0u64;
        for digest in reclaimable_digests {
            reclaimable = reclaimable.saturating_add(file_bytes(&object_path(
                self.root(),
                &actor.group_name,
                &digest,
            )?)?);
        }
        let orphan_candidates = self
            .artifact_orphan_candidates(&mut tx, &actor.group_name, 0, crate::now()?)
            .await?;
        reclaimable = reclaimable.saturating_add(orphan_candidates.bytes());
        let temporary = directory_bytes(&self.root().join("artifacts/tmp"))?;
        let disk = directory_bytes(
            &self
                .root()
                .join("artifacts/objects")
                .join(group_key(&actor.group_name)),
        )?;
        Ok(ArtifactStats {
            logical_bytes: logical as u64,
            unique_original_bytes: unique as u64,
            physical_stored_bytes: disk,
            temporary_bytes: temporary,
            reclaimable_bytes: reclaimable,
            quota_usage_bytes: disk.saturating_add(temporary),
        })
    }
    pub async fn artifact_prune(
        &self,
        actor: &Mailbox,
        grace: i64,
        dry_run: bool,
        now: i64,
    ) -> Result<PruneResult> {
        ensure!(grace >= 0, "grace must be nonnegative");
        let _guard = lock(self.root()).await?;
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        require_home(&mut tx, &actor.group_name).await?;
        let rows=sqlx::query("SELECT digest,stored_size FROM artifact_blobs b WHERE group_name=? AND created<=? AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.group_name=b.group_name AND a.digest=b.digest AND (a.pinned=1 OR EXISTS(SELECT 1 FROM artifact_links l WHERE l.group_name=a.group_name AND l.artifact_id=a.id))) ORDER BY digest").bind(&actor.group_name).bind(now.saturating_sub(grace)).fetch_all(&mut *tx).await?;
        let orphans = self
            .artifact_orphan_candidates(&mut tx, &actor.group_name, grace, now)
            .await?;
        let mut result = PruneResult {
            dry_run,
            orphan_digests: orphans
                .objects
                .iter()
                .map(|(digest, _)| digest.clone())
                .collect(),
            temporary_files: orphans
                .temporary
                .iter()
                .map(|(name, _)| name.clone())
                .collect(),
            reclaimed_bytes: orphans.bytes(),
            ..Default::default()
        };
        for row in rows {
            let digest: String = row.get("digest");
            result.reclaimed_bytes =
                result
                    .reclaimed_bytes
                    .saturating_add(file_bytes(&object_path(
                        self.root(),
                        &actor.group_name,
                        &digest,
                    )?)?);
            result.digests.push(digest.clone());
            if !dry_run {
                // Metadata first: interruption leaves reclaimable orphan bytes, never a readable partial blob.
                sqlx::query("DELETE FROM artifact_blobs WHERE group_name=? AND digest=?")
                    .bind(&actor.group_name)
                    .bind(&digest)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        audit(
            &mut tx,
            &actor.group_name,
            None,
            if dry_run {
                "prune-dry-run"
            } else {
                "prune-planned"
            },
            &actor.name,
            &serde_json::to_string(&result)?,
            now,
        )
        .await?;
        tx.commit().await?;
        failure_boundary("prune-after-metadata");
        if !dry_run {
            for digest in result.digests.iter().chain(&result.orphan_digests) {
                let path = object_path(self.root(), &actor.group_name, digest)?;
                if path.exists() {
                    fs::remove_file(&path)?;
                    failure_boundary("prune-after-delete");
                    sync_directory(path.parent().unwrap())?;
                }
            }
            let temporary = self.root().join("artifacts/tmp");
            for name in &result.temporary_files {
                fs::remove_file(temporary.join(name))?;
            }
            if !result.temporary_files.is_empty() {
                sync_directory(&temporary)?;
            }
            let mut tx = self.pool().begin().await?;
            audit(
                &mut tx,
                &actor.group_name,
                None,
                "prune-completed",
                &actor.name,
                &serde_json::to_string(&result)?,
                now,
            )
            .await?;
            tx.commit().await?;
        }
        Ok(result)
    }
    async fn artifact_orphan_candidates(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        group: &str,
        grace: i64,
        now: i64,
    ) -> Result<OrphanCandidates> {
        let mut candidates = OrphanCandidates::default();
        let directory = self.root().join("artifacts/objects").join(group_key(group));
        if directory.exists() {
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let digest = format!("sha256:{}", entry.file_name().to_string_lossy());
                if !digest_valid(&digest) {
                    continue;
                }
                let live: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM artifact_blobs WHERE group_name=? AND digest=?)",
                )
                .bind(group)
                .bind(&digest)
                .fetch_one(&mut **tx)
                .await?;
                let metadata = entry.metadata()?;
                if !live && expired(&metadata, grace, now) {
                    candidates.objects.push((digest, metadata.len()));
                }
            }
        }
        let temporary = self.root().join("artifacts/tmp");
        if temporary.exists() {
            for entry in fs::read_dir(temporary)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let metadata = entry.metadata()?;
                if expired(&metadata, grace, now) {
                    let name = entry
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("temporary artifact filename is not UTF-8"))?;
                    candidates.temporary.push((name, metadata.len()));
                }
            }
        }
        candidates.objects.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.temporary.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(candidates)
    }
}
fn expired(metadata: &fs::Metadata, grace: i64, now: i64) -> bool {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .is_some_and(|t| (t.as_secs() as i64) <= now.saturating_sub(grace))
}
fn directory_bytes(directory: &Path) -> Result<u64> {
    if !directory.exists() {
        return Ok(0);
    }
    let mut total = 0;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}
fn verify_path(
    path: &Path,
    codec: &str,
    digest: &str,
    original: u64,
    stored: u64,
    limits: &ArtifactLimits,
    writer: &mut dyn Write,
) -> Result<()> {
    ensure!(
        original <= limits.decoded_bytes && stored <= limits.stored_bytes,
        "artifact exceeds configured size limits"
    );
    let file = File::open(path).context("artifact unavailable: managed object missing")?;
    ensure!(
        file.metadata()?.len() == stored,
        "artifact stored length mismatch"
    );
    let (actual, length) = match codec {
        "identity-v1" => bounded_copy(
            &mut std::io::BufReader::new(file),
            writer,
            limits.decoded_bytes.min(original),
        )?,
        "zstd-v1" => {
            let mut decoder = zstd::stream::read::Decoder::new(file)?;
            decoder.window_log_max(23)?;
            bounded_copy(&mut decoder, writer, limits.decoded_bytes.min(original))?
        }
        _ => bail!("unsupported codec {codec}"),
    };
    ensure!(actual == digest, "artifact digest mismatch");
    ensure!(length == original, "artifact decoded length mismatch");
    Ok(())
}
async fn validate_target(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    target: &ArtifactLink,
) -> Result<()> {
    let present:bool=match target {
        ArtifactLink::Task {id}=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM work_items WHERE group_name=? AND id=?)").bind(group).bind(id).fetch_one(&mut **tx).await?,
        ArtifactLink::Message {id}=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages m JOIN mailboxes a ON a.id=m.sender WHERE a.group_name=? AND m.id=?)").bind(group).bind(id).fetch_one(&mut **tx).await?,
        ArtifactLink::RecordRevision {id,version}=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM record_revisions WHERE group_name=? AND record_id=? AND revision=? UNION ALL SELECT 1 FROM record_snapshots WHERE group_name=? AND record_id=? AND revision=?)").bind(group).bind(id).bind(version).bind(group).bind(id).bind(version).fetch_one(&mut **tx).await?,
    };
    ensure!(present, "artifact link target not found in group");
    Ok(())
}
async fn enqueue_existing(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    id: &str,
    now: i64,
) -> Result<()> {
    let encoded: String =
        sqlx::query_scalar("SELECT snapshot FROM artifacts WHERE group_name=? AND id=?")
            .bind(group)
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
    enqueue_artifact(tx, &serde_json::from_str(&encoded)?, now).await
}
async fn enqueue_artifact(
    tx: &mut Transaction<'_, Sqlite>,
    item: &Artifact,
    now: i64,
) -> Result<()> {
    let (pinned, revision): (bool, i64) =
        sqlx::query_as("SELECT pinned,revision FROM artifacts WHERE group_name=? AND id=?")
            .bind(&item.group_name)
            .bind(&item.resource.id)
            .fetch_one(&mut **tx)
            .await?;
    let rows:Vec<String>=sqlx::query_scalar("SELECT target FROM artifact_links WHERE group_name=? AND artifact_id=? ORDER BY target LIMIT 1001").bind(&item.group_name).bind(&item.resource.id).fetch_all(&mut **tx).await?;
    ensure!(rows.len() <= 1000, "artifact exceeds maximum 1000 links");
    let mut links = Vec::new();
    for encoded in rows {
        links.push(portable_target(tx, &item.group_name, &encoded).await?);
    }
    let snapshot = ArtifactSnapshot {
        artifact: item.clone(),
        links,
        pinned,
        revision,
    };
    ensure!(
        serde_json::to_vec(&snapshot)?.len() <= 192 * 1024,
        "artifact snapshot exceeds 192KiB synchronization limit"
    );
    crate::relay::enqueue_artifact_snapshot(tx, &snapshot, now).await
}
pub(crate) async fn apply_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &ArtifactSnapshot,
) -> Result<()> {
    ensure!(
        serde_json::to_vec(snapshot)?.len() <= 192 * 1024,
        "artifact snapshot exceeds synchronization limit"
    );
    let item = &snapshot.artifact;
    validate(&item.resource)?;
    ensure!(
        snapshot.links.len() <= 1000,
        "artifact snapshot link limit exceeded"
    );
    ensure!(snapshot.revision > 0, "invalid artifact snapshot revision");
    if matches!(item.resource.location, ResourceLocation::Managed) {
        ensure!(
            item.resource.digest.is_some() && item.resource.size.is_some(),
            "managed snapshot lacks digest or size"
        );
    }
    ensure!(
        snapshot
            .links
            .iter()
            .all(|target| serde_json::to_vec(target).is_ok_and(|v| v.len() <= 1024)),
        "artifact snapshot link exceeds limit"
    );
    let canonical = serde_json::to_string(&item.resource)?;
    let old: Option<(String, i64)> =
        sqlx::query_as("SELECT canonical,revision FROM artifacts WHERE group_name=? AND id=?")
            .bind(&item.group_name)
            .bind(&item.resource.id)
            .fetch_optional(&mut **tx)
            .await?;
    if let Some((old, revision)) = old {
        ensure!(
            old == canonical,
            "synced artifact immutable metadata conflict"
        );
        if revision >= snapshot.revision {
            return Ok(());
        }
    }
    sqlx::query("INSERT INTO artifacts(group_name,id,snapshot,canonical,created,digest,pinned,revision) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(group_name,id) DO UPDATE SET pinned=excluded.pinned,revision=excluded.revision").bind(&item.group_name).bind(&item.resource.id).bind(serde_json::to_string(item)?).bind(canonical).bind(item.created).bind(&item.resource.digest).bind(snapshot.pinned).bind(snapshot.revision).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM artifact_links WHERE group_name=? AND artifact_id=?")
        .bind(&item.group_name)
        .bind(&item.resource.id)
        .execute(&mut **tx)
        .await?;
    for target in &snapshot.links {
        // Snapshot targets may arrive before their task/record/message. They still retain bytes.
        sqlx::query(
            "INSERT INTO artifact_links(group_name,artifact_id,target,created) VALUES(?,?,?,?)",
        )
        .bind(&item.group_name)
        .bind(&item.resource.id)
        .bind(serde_json::to_string(target)?)
        .bind(item.created)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}
impl Store {
    /// Operator backup: one consistent SQLite snapshot and every locally stored
    /// managed blob. A manifest explicitly marks this as a complete local backup;
    /// remote references remain external and are not promised durable.
    pub async fn artifact_backup(&self, destination: &Path, limits: &ArtifactLimits) -> Result<()> {
        let _guard = lock(self.root()).await?;
        ensure!(!destination.exists(), "backup destination already exists");
        let parent = destination
            .parent()
            .context("backup destination needs parent")?;
        fs::create_dir_all(parent)?;
        let staging = tempfile::tempdir_in(parent)?;
        let database = staging.path().join("mail.db");
        sqlx::query("VACUUM INTO ?")
            .bind(database.to_str().context("backup path must be UTF-8")?)
            .execute(self.pool())
            .await?;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&database)
            .read_only(true);
        let backup = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let rows=sqlx::query("SELECT group_name,digest,codec,format_version,original_size,stored_size FROM artifact_blobs ORDER BY group_name,digest").fetch_all(&backup).await?;
        ensure_required_blobs(&backup).await?;
        let mut count = 0;
        for row in rows {
            ensure!(
                row.get::<i64, _>("format_version") == 1,
                "unsupported codec format version"
            );
            let group: String = row.get("group_name");
            let digest: String = row.get("digest");
            let source = object_path(self.root(), &group, &digest)?;
            verify_path(
                &source,
                &row.get::<String, _>("codec"),
                &digest,
                row.get::<i64, _>("original_size") as u64,
                row.get::<i64, _>("stored_size") as u64,
                limits,
                &mut std::io::sink(),
            )?;
            let target = object_path(staging.path(), &group, &digest)?;
            fs::create_dir_all(target.parent().unwrap())?;
            fs::copy(source, &target)?;
            File::open(&target)?.sync_all()?;
            count += 1;
        }
        backup.close().await;
        File::open(&database)?.sync_all()?;
        let manifest = serde_json::json!({"format":"agent-mail-local-backup-v1","managed_blobs":count,"external_references":"metadata only; external bytes and durability excluded"});
        fs::write(
            staging.path().join("artifact-backup.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        File::open(staging.path().join("artifact-backup.json"))?.sync_all()?;
        let staging_path = staging.keep();
        sync_tree_directories(&staging_path)?;
        fs::rename(&staging_path, destination)?;
        sync_directory(parent)?;
        Ok(())
    }
    /// Restore a complete local backup into a new directory. SQLite-only copies
    /// fail because they lack the explicit artifact backup manifest.
    pub async fn artifact_restore(
        source: &Path,
        destination: &Path,
        limits: &ArtifactLimits,
    ) -> Result<()> {
        ensure!(!destination.exists(), "restore destination already exists");
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(source.join("artifact-backup.json"))
                .context("incomplete backup: SQLite alone does not include artifact payloads")?,
        )?;
        ensure!(
            manifest["format"] == "agent-mail-local-backup-v1",
            "unsupported artifact backup format"
        );
        let parent = destination
            .parent()
            .context("restore destination needs parent")?;
        fs::create_dir_all(parent)?;
        let staging = tempfile::tempdir_in(parent)?;
        fs::copy(source.join("mail.db"), staging.path().join("mail.db"))?;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(staging.path().join("mail.db"))
            .read_only(true);
        let backup = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(&backup)
            .await?;
        ensure!(integrity == "ok", "backup SQLite integrity check failed");
        let rows=sqlx::query("SELECT group_name,digest,codec,format_version,original_size,stored_size FROM artifact_blobs ORDER BY group_name,digest").fetch_all(&backup).await?;
        ensure!(
            manifest["managed_blobs"].as_u64() == Some(rows.len() as u64),
            "backup manifest blob count mismatch"
        );
        ensure_required_blobs(&backup).await?;
        let mut group_totals = std::collections::HashMap::<String, u64>::new();
        for row in rows {
            ensure!(
                row.get::<i64, _>("format_version") == 1,
                "unsupported codec format version"
            );
            let group: String = row.get("group_name");
            let digest: String = row.get("digest");
            let source_path = object_path(source, &group, &digest)?;
            let stored = row.get::<i64, _>("stored_size") as u64;
            let total = group_totals.entry(group.clone()).or_default();
            *total = total.checked_add(stored).context("backup size overflow")?;
            ensure!(
                *total <= limits.quota_bytes,
                "restore exceeds configured storage quota"
            );
            verify_path(
                &source_path,
                &row.get::<String, _>("codec"),
                &digest,
                row.get::<i64, _>("original_size") as u64,
                stored,
                limits,
                &mut std::io::sink(),
            )?;
            let target = object_path(staging.path(), &group, &digest)?;
            fs::create_dir_all(target.parent().unwrap())?;
            fs::copy(source_path, &target)?;
            File::open(&target)?.sync_all()?;
        }
        backup.close().await;
        File::open(staging.path().join("mail.db"))?.sync_all()?;
        sync_tree_directories(staging.path())?;
        fs::rename(staging.keep(), destination)?;
        sync_directory(parent)?;
        Ok(())
    }
}
pub(crate) async fn enqueue_artifacts_for_route(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    machine: uuid::Uuid,
    now: i64,
) -> Result<()> {
    let _ = machine; // relay helper routes current snapshots to all current group peers.
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT snapshot FROM artifacts WHERE group_name=? ORDER BY id")
            .bind(group)
            .fetch_all(&mut **tx)
            .await?;
    for encoded in rows {
        enqueue_artifact(tx, &serde_json::from_str(&encoded)?, now).await?;
    }
    Ok(())
}
impl Store {
    pub async fn artifact_links_for_task(
        &self,
        actor: &Mailbox,
        id: &str,
    ) -> Result<Vec<Artifact>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let target = serde_json::to_string(&ArtifactLink::Task { id: id.into() })?;
        let rows:Vec<String>=sqlx::query_scalar("SELECT a.snapshot FROM artifacts a JOIN artifact_links l ON l.group_name=a.group_name AND l.artifact_id=a.id WHERE l.group_name=? AND l.target=? ORDER BY a.id LIMIT 16").bind(&actor.group_name).bind(target).fetch_all(&mut *tx).await?;
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
}
async fn require_home(tx: &mut Transaction<'_, Sqlite>, group: &str) -> Result<()> {
    let home: String = sqlx::query_scalar("SELECT home_machine FROM groups WHERE name=?")
        .bind(group)
        .fetch_one(&mut **tx)
        .await?;
    let node: String = sqlx::query_scalar("SELECT id FROM node LIMIT 1")
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        home == node,
        "artifact metadata writable only on home machine; replicated bytes may be explicitly imported"
    );
    Ok(())
}
fn sync_tree_directories(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree_directories(&entry.path())?;
        }
    }
    sync_directory(root)
}
async fn target_authority(
    tx: &mut Transaction<'_, Sqlite>,
    actor: &Mailbox,
    target: &ArtifactLink,
    removing: bool,
) -> Result<()> {
    match target {
        ArtifactLink::Task { id } => {
            let (writer, state): (String, String) =
                sqlx::query_as("SELECT writer,state FROM work_items WHERE group_name=? AND id=?")
                    .bind(&actor.group_name)
                    .bind(id)
                    .fetch_one(&mut **tx)
                    .await?;
            ensure!(
                writer == actor.name,
                "task artifact links require current task writer authority"
            );
            if removing {
                let accepted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM work_changes WHERE group_name=? AND work_id=? AND json_extract(snapshot,'$.state')='accepted')").bind(&actor.group_name).bind(id).fetch_one(&mut **tx).await?;
                ensure!(
                    state != "accepted" && !accepted,
                    "accepted task evidence remains retained"
                );
            }
        }
        ArtifactLink::Message { id } => {
            let sender:i64=sqlx::query_scalar("SELECT m.sender FROM messages m JOIN mailboxes a ON a.id=m.sender WHERE a.group_name=? AND m.id=?").bind(&actor.group_name).bind(id).fetch_one(&mut **tx).await?;
            ensure!(
                sender == actor.id,
                "message artifact links require sender authority"
            );
        }
        ArtifactLink::RecordRevision { id, .. } => {
            let writer: String =
                sqlx::query_scalar("SELECT writer FROM shared_records WHERE group_name=? AND id=?")
                    .bind(&actor.group_name)
                    .bind(id)
                    .fetch_one(&mut **tx)
                    .await?;
            ensure!(
                writer == actor.name,
                "record artifact links require designated record writer authority"
            );
            ensure!(
                !removing,
                "retained record revision evidence links are immutable"
            );
        }
    }
    Ok(())
}
impl Store {
    pub async fn artifact_targets(
        &self,
        actor: &Mailbox,
        id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value> {
        ensure!(
            (1..=100).contains(&limit),
            "artifact links page limit must be 1..100"
        );
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifacts WHERE group_name=? AND id=?)",
        )
        .bind(&actor.group_name)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(present, "artifact not found in group");
        let position = if let Some(after) = after {
            crate::bounded(after, 4096, "artifact links cursor")?;
            let cursor: ArtifactCursor =
                serde_json::from_str(after).context("invalid artifact links cursor")?;
            ensure!(
                cursor.format == 1
                    && cursor.group == actor.group_name
                    && cursor.artifact == id
                    && cursor.actor == actor.id
                    && cursor.generation == actor.binding_version,
                "artifact links cursor belongs to another scope or generation"
            );
            cursor.target
        } else {
            String::new()
        };
        let mut rows:Vec<String>=sqlx::query_scalar("SELECT target FROM artifact_links WHERE group_name=? AND artifact_id=? AND target>? ORDER BY target LIMIT ?").bind(&actor.group_name).bind(id).bind(position).bind(limit as i64+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        let cursor = if more {
            Some(serde_json::to_string(&ArtifactCursor {
                format: 1,
                group: actor.group_name.clone(),
                artifact: id.into(),
                actor: actor.id,
                generation: actor.binding_version,
                target: rows
                    .last()
                    .context("artifact links cursor target missing")?
                    .clone(),
            })?)
        } else {
            None
        };
        let mut links = Vec::new();
        for encoded in rows {
            links.push(portable_target(&mut tx, &actor.group_name, &encoded).await?);
        }
        Ok(
            serde_json::json!({"group_name":actor.group_name,"artifact_id":id,"links":links,"next_cursor":cursor}),
        )
    }
    pub async fn artifact_links_for_record(
        &self,
        actor: &Mailbox,
        id: &str,
        revision: i64,
    ) -> Result<Vec<Artifact>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let present:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM record_read_revisions WHERE group_name=? AND record_id=? AND revision=?)").bind(&actor.group_name).bind(id).bind(revision).fetch_one(&mut *tx).await?;
        ensure!(present, "record revision not found in group");
        let target = serde_json::to_string(&ArtifactLink::RecordRevision {
            id: id.into(),
            version: revision,
        })?;
        let rows:Vec<String>=sqlx::query_scalar("SELECT a.snapshot FROM artifacts a JOIN artifact_links l ON l.group_name=a.group_name AND l.artifact_id=a.id WHERE l.group_name=? AND l.target=? ORDER BY a.id LIMIT 16").bind(&actor.group_name).bind(target).fetch_all(&mut *tx).await?;
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
    pub async fn artifact_links_for_message(
        &self,
        actor: &Mailbox,
        id: i64,
    ) -> Result<Vec<Artifact>> {
        let mut tx = self.pool().begin().await?;
        Self::lock_actor(&mut tx, actor).await?;
        let global:Option<String>=sqlx::query_scalar("SELECT m.global_id FROM messages m JOIN mailboxes a ON a.id=m.sender WHERE a.group_name=? AND m.id=? AND (m.sender=? OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message=m.id AND d.recipient=?))").bind(&actor.group_name).bind(id).bind(actor.id).bind(actor.id).fetch_optional(&mut *tx).await?;
        let global = global.context("message unavailable to this mailbox")?;
        let local_target = serde_json::to_string(&ArtifactLink::Message { id })?;
        let global_target = serde_json::to_string(&WireArtifactLink::MessageGlobal {
            id: uuid::Uuid::parse_str(&global)?,
        })?;
        let rows:Vec<String>=sqlx::query("SELECT DISTINCT a.id,a.snapshot FROM artifacts a JOIN artifact_links l ON l.group_name=a.group_name AND l.artifact_id=a.id WHERE l.group_name=? AND (l.target=? OR l.target=?) ORDER BY a.id LIMIT 16").bind(&actor.group_name).bind(local_target).bind(global_target).fetch_all(&mut *tx).await?.into_iter().map(|row|row.get("snapshot")).collect();
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
}
async fn portable_target(
    tx: &mut Transaction<'_, Sqlite>,
    group: &str,
    encoded: &str,
) -> Result<WireArtifactLink> {
    if let Ok(portable) = serde_json::from_str::<WireArtifactLink>(encoded) {
        return Ok(portable);
    }
    match serde_json::from_str::<ArtifactLink>(encoded)? {
        ArtifactLink::Task { id } => Ok(WireArtifactLink::Task { id }),
        ArtifactLink::RecordRevision { id, version } => {
            Ok(WireArtifactLink::RecordRevision { id, version })
        }
        ArtifactLink::Message { id } => {
            let global:String=sqlx::query_scalar("SELECT m.global_id FROM messages m JOIN mailboxes a ON a.id=m.sender WHERE a.group_name=? AND m.id=?").bind(group).bind(id).fetch_one(&mut **tx).await?;
            Ok(WireArtifactLink::MessageGlobal {
                id: uuid::Uuid::parse_str(&global)?,
            })
        }
    }
}

#[cfg(not(test))]
fn failure_boundary(_: &str) {}
#[cfg(test)]
fn failure_boundary(name: &str) {
    if std::env::var("AGENT_MAIL_TEST_ARTIFACT_BOUNDARY")
        .ok()
        .as_deref()
        == Some(name)
    {
        std::process::exit(86);
    }
}
#[derive(Default)]
struct OrphanCandidates {
    objects: Vec<(String, u64)>,
    temporary: Vec<(String, u64)>,
}
impl OrphanCandidates {
    fn bytes(&self) -> u64 {
        self.objects
            .iter()
            .chain(&self.temporary)
            .fold(0u64, |total, (_, size)| total.saturating_add(*size))
    }
}
fn file_bytes(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(metadata.is_file(), "artifact object is not a regular file");
            Ok(metadata.len())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}
#[cfg(test)]
mod crash_tests {
    use super::*;
    fn draft() -> ArtifactDraft {
        ArtifactDraft {
            id: "crash-witness".into(),
            location: ResourceLocation::Managed,
            digest: None,
            media_type: None,
            size: None,
            provenance: None,
        }
    }
    #[tokio::test]
    async fn boundary_child() -> Result<()> {
        let Ok(root) = std::env::var("AGENT_MAIL_TEST_ARTIFACT_ROOT") else {
            return Ok(());
        };
        let store = Store::open(Path::new(&root), false).await?;
        let actor = store.mailbox("g", "a").await?;
        let boundary = std::env::var("AGENT_MAIL_TEST_ARTIFACT_BOUNDARY")?;
        if boundary.starts_with("link-") {
            store
                .artifact_link(
                    &actor,
                    "crash-witness",
                    ArtifactLink::Task {
                        id: "crash-task".into(),
                    },
                    crate::now()?,
                )
                .await?;
        } else if boundary.starts_with("prune-") {
            store
                .artifact_prune(&actor, 0, false, crate::now()?)
                .await?;
        } else {
            store
                .artifact_ingest(
                    &actor,
                    draft(),
                    b"immutable crash witness".as_slice(),
                    &ArtifactLimits::default(),
                    crate::now()?,
                )
                .await?;
        }
        panic!("boundary was not reached")
    }
    #[tokio::test]
    async fn process_termination_cannot_publish_partial_or_collect_live_references() -> Result<()> {
        for boundary in [
            "published",
            "blob-metadata",
            "reference-before-commit",
            "link-before-commit",
            "link-after-commit",
            "prune-after-metadata",
            "prune-after-delete",
        ] {
            let temp = tempfile::tempdir()?;
            let store = Store::open(temp.path(), true).await?;
            store.enroll("g", None).await?;
            store.register("g", "a", false).await?;
            let actor = store.mailbox("g", "a").await?;
            if boundary.starts_with("link-") || boundary.starts_with("prune-") {
                store
                    .artifact_ingest(
                        &actor,
                        draft(),
                        b"immutable crash witness".as_slice(),
                        &ArtifactLimits::default(),
                        crate::now()?,
                    )
                    .await?;
                store
                    .work_create(
                        &actor,
                        crate::work::WorkDraft {
                            id: "crash-task".into(),
                            scope: "Retain evidence".into(),
                            owner: "a".into(),
                            state: crate::states::TaskState::Open,
                            next_action: "Inspect".into(),
                            deadline: None,
                            evidence: vec![],
                        },
                        crate::now()?,
                    )
                    .await?;
            }
            drop(store);
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "artifacts::crash_tests::boundary_child",
                    "--nocapture",
                ])
                .env("AGENT_MAIL_TEST_ARTIFACT_ROOT", temp.path())
                .env("AGENT_MAIL_TEST_ARTIFACT_BOUNDARY", boundary)
                .output()?;
            ensure!(
                output.status.code() == Some(86),
                "boundary {boundary} did not terminate as expected: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let store = Store::open(temp.path(), false).await?;
            let actor = store.mailbox("g", "a").await?;
            if boundary.starts_with("link-") {
                store
                    .artifact_fetch(
                        &actor,
                        "crash-witness",
                        std::io::sink(),
                        &ArtifactLimits::default(),
                    )
                    .await?;
                let links = store.artifact_links_for_task(&actor, "crash-task").await?;
                assert_eq!(links.len(), usize::from(boundary == "link-after-commit"));
            } else if boundary.starts_with("prune-") {
                assert!(matches!(
                    store
                        .artifact_check(&actor, "crash-witness", &ArtifactLimits::default())
                        .await?,
                    ArtifactAccess::Unavailable { .. }
                ));
            } else {
                assert!(store.artifact_show(&actor, "crash-witness").await.is_err());
            }
            store
                .artifact_prune(&actor, 0, false, crate::now()? + 1)
                .await?;
            if boundary == "link-after-commit" {
                store
                    .artifact_fetch(
                        &actor,
                        "crash-witness",
                        std::io::sink(),
                        &ArtifactLimits::default(),
                    )
                    .await?;
            } else {
                assert_eq!(store.artifact_stats(&actor).await?.physical_stored_bytes, 0);
            }
        }
        Ok(())
    }
}
struct LimitedWriter<'a> {
    file: &'a mut File,
    remaining: u64,
    exceeded: bool,
}
impl Write for LimitedWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() as u64 > self.remaining {
            self.exceeded = true;
            return Err(std::io::Error::other("artifact compression spool limit"));
        }
        let written = self.file.write(buffer)?;
        self.remaining -= written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
async fn ensure_required_blobs(pool: &sqlx::SqlitePool) -> Result<()> {
    let missing:Option<(String,String)>=sqlx::query_as("SELECT a.group_name,a.id FROM artifacts a WHERE json_extract(a.snapshot,'$.resource.location.kind')='managed' AND (a.pinned=1 OR EXISTS(SELECT 1 FROM artifact_links l WHERE l.group_name=a.group_name AND l.artifact_id=a.id)) AND NOT EXISTS(SELECT 1 FROM artifact_blobs b WHERE b.group_name=a.group_name AND b.digest=a.digest) LIMIT 1").fetch_optional(pool).await?;
    if let Some((group, id)) = missing {
        bail!(
            "incomplete artifact backup: required managed artifact {group}/{id} unavailable on this machine"
        );
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactCursor {
    format: u8,
    group: String,
    artifact: String,
    actor: i64,
    generation: i64,
    target: String,
}
