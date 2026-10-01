//! Immutable artifact descriptions and complete physical-effect reconciliation.
//!
//! These values describe bytes and historical effects. They confer no task or
//! publication authority. Selection must compose the actual model and scheduler
//! guards in the caller's writer transaction before inserting an effect receipt.

use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};

use crate::managed_runtime::RuntimeDirectory;
use anyhow::{Context, Result, ensure};
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, fsync, linkat, openat, unlinkat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Maximum encoded manifest size before storage or transaction work.
pub const MANIFEST_LIMIT: usize = 64 * 1024;
/// Maximum number of regular files in one artifact.
pub const FILE_LIMIT: usize = 256;
/// Maximum bytes in one staged file in the initial bounded profile.
pub const FILE_BYTES_LIMIT: u64 = 64 * 1024 * 1024;
/// Maximum aggregate bytes in an artifact, independent of compressed size.
pub const ARTIFACT_BYTES_LIMIT: u64 = 256 * 1024 * 1024;

/// Maximum complete native structured answer, independently of the native frame limit.
pub const TEXT_RESULT_LIMIT: usize = 24 * 1024;
/// Maximum text files emitted by the first managed producer.
pub const TEXT_FILE_LIMIT: usize = 16;
/// Maximum UTF-8 content bytes in one text file.
pub const TEXT_FILE_BYTES: usize = 4 * 1024;
/// Maximum aggregate UTF-8 content bytes in one text artifact.
pub const TEXT_ARTIFACT_BYTES: usize = 12 * 1024;
/// Maximum canonical manifest bytes in the first text profile.
pub const TEXT_MANIFEST_BYTES: usize = 8 * 1024;
/// Largest supported relative review request; existing authority may impose a smaller bound.
pub const MAX_REVIEW_AFTER_SECONDS: u32 = 3600;

/// One untrusted text file; it has no host path or executable metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTextFile {
    /// Canonical path inside the admitted virtual artifact tree.
    pub path: String,
    /// Exact UTF-8 contents whose digest is computed by the runtime.
    pub text: String,
}

/// Strict native final-answer grammar. Parsing grants no publication or execution authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedTextResult {
    /// Candidate artifact data, still requiring publication and an authorized business decision.
    Artifact {
        /// Supported wire format, currently one.
        schema_version: u32,
        /// Original admitted artifact binding; it is not a bearer credential.
        binding: String,
        /// Bounded untrusted description.
        summary: String,
        /// Complete text file set.
        files: Vec<ManagedTextFile>,
    },
    /// A completed native turn with unfinished business work and a relative review request.
    Yield {
        /// Supported wire format, currently one.
        schema_version: u32,
        /// Original admitted artifact binding.
        binding: String,
        /// Bounded untrusted description.
        summary: String,
        /// Concrete next step, never an authority extension.
        next_step: String,
        /// Requested interval in the supported range; trusted deadlines still constrain it.
        review_after_seconds: u32,
        /// Complete partial artifact file set.
        files: Vec<ManagedTextFile>,
    },
}

/// Bounded exact text bytes supplied to the protected publisher.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedArtifactContents {
    /// One object per distinct manifest digest; missing, duplicate and extra objects are rejected.
    pub objects: Vec<ManagedTextObject>,
}

/// A content-addressed untrusted text object, verified from its actual bytes before use.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTextObject {
    /// Caller-declared digest, never trusted without hashing the text.
    pub digest: ContentDigest,
    /// Exact bounded UTF-8 data.
    pub text: String,
}

impl ManagedTextResult {
    /// Parse a complete bounded result without truncation or tolerant fallback.
    ///
    /// # Errors
    /// Rejects unsupported fields/versions, duplicate keys, aliases, and any profile limit violation.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= TEXT_RESULT_LIMIT,
            "managed_text_result_too_large"
        );
        // Validate duplicate keys before tagged-enum buffering can collapse an object.
        reject_duplicate_json_keys(bytes)?;
        let value: Self = serde_json::from_slice(bytes)?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        let (version, binding, summary, files) = match self {
            Self::Artifact {
                schema_version,
                binding,
                summary,
                files,
            }
            | Self::Yield {
                schema_version,
                binding,
                summary,
                files,
                ..
            } => (*schema_version, binding, summary, files),
        };
        ensure!(version == 1, "managed_text_version_unsupported");
        crate::bounded(binding, 128, "artifact binding")?;
        ensure!(
            !binding.is_empty() && !binding.chars().any(char::is_control),
            "invalid_artifact_binding"
        );
        crate::bounded(summary, 1024, "artifact summary")?;
        ensure!(!summary.trim().is_empty(), "artifact_summary_required");
        if let Self::Yield {
            next_step,
            review_after_seconds,
            ..
        } = self
        {
            crate::bounded(next_step, 512, "yield next step")?;
            ensure!(!next_step.trim().is_empty(), "yield_next_step_required");
            ensure!(
                (1..=MAX_REVIEW_AFTER_SECONDS).contains(review_after_seconds),
                "yield_review_interval_out_of_range"
            );
        }
        validate_text_files(files)?;
        Ok(())
    }

    /// Return the exact untrusted binding selector for comparison with admitted protected data.
    pub fn binding(&self) -> &str {
        match self {
            Self::Artifact { binding, .. } | Self::Yield { binding, .. } => binding,
        }
    }

    /// Return the bounded untrusted summary.
    pub fn summary(&self) -> &str {
        match self {
            Self::Artifact { summary, .. } | Self::Yield { summary, .. } => summary,
        }
    }

    /// Compute the manifest and distinct text objects from actual bounded contents.
    ///
    /// # Errors
    /// Rejects malformed programmatically constructed values as well as size/path violations.
    pub fn artifact(&self) -> Result<(ArtifactManifest, ManagedArtifactContents)> {
        self.validate()?;
        let files = match self {
            Self::Artifact { files, .. } | Self::Yield { files, .. } => files,
        };
        let mut manifest = ArtifactManifest {
            version: 1,
            files: Vec::with_capacity(files.len()),
        };
        let mut objects = std::collections::BTreeMap::new();
        for file in files {
            let digest = ContentDigest::of_bytes(file.text.as_bytes());
            manifest.files.push(ArtifactFile {
                path: file.path.clone(),
                digest: digest.clone(),
                bytes: file.text.len() as u64,
            });
            if let Some(old) = objects.insert(digest, file.text.clone()) {
                ensure!(old == file.text, "artifact_digest_content_conflict");
            }
        }
        manifest.normalize()?;
        ensure!(
            manifest.canonical_bytes()?.len() <= TEXT_MANIFEST_BYTES,
            "text_manifest_too_large"
        );
        let contents = ManagedArtifactContents {
            objects: objects
                .into_iter()
                .map(|(digest, text)| ManagedTextObject { digest, text })
                .collect(),
        };
        contents.validate(&manifest)?;
        Ok((manifest, contents))
    }
}

fn validate_text_files(files: &[ManagedTextFile]) -> Result<()> {
    ensure!(
        !files.is_empty() && files.len() <= TEXT_FILE_LIMIT,
        "text_file_count_invalid"
    );
    let mut total = 0usize;
    let mut manifest = ArtifactManifest {
        version: 1,
        files: Vec::with_capacity(files.len()),
    };
    for file in files {
        ensure!(file.path.len() <= 256, "text_path_too_long");
        validate_artifact_path(&file.path)?;
        ensure!(file.text.len() <= TEXT_FILE_BYTES, "text_file_too_large");
        total = total
            .checked_add(file.text.len())
            .context("text_size_overflow")?;
        ensure!(total <= TEXT_ARTIFACT_BYTES, "text_artifact_too_large");
        manifest.files.push(ArtifactFile {
            path: file.path.clone(),
            digest: ContentDigest::of_bytes(file.text.as_bytes()),
            bytes: file.text.len() as u64,
        });
    }
    manifest.normalize()?;
    ensure!(
        manifest.canonical_bytes()?.len() <= TEXT_MANIFEST_BYTES,
        "text_manifest_too_large"
    );
    Ok(())
}

impl ManagedArtifactContents {
    /// Verify every object and the complete manifest/content relation without filesystem I/O.
    ///
    /// # Errors
    /// Rejects oversized, missing, duplicate, extra or digest/length-conflicting data.
    pub fn validate(&self, manifest: &ArtifactManifest) -> Result<()> {
        let mut normalized = manifest.clone();
        normalized.normalize()?;
        ensure!(
            !normalized.files.is_empty()
                && normalized.files.len() <= TEXT_FILE_LIMIT
                && normalized.canonical_bytes()?.len() <= TEXT_MANIFEST_BYTES,
            "text_manifest_limits"
        );
        let mut expected = std::collections::BTreeMap::new();
        let mut total = 0u64;
        for file in &normalized.files {
            ensure!(
                file.path.len() <= 256 && file.bytes <= TEXT_FILE_BYTES as u64,
                "text_file_limits"
            );
            total = total
                .checked_add(file.bytes)
                .context("text_size_overflow")?;
            ensure!(
                total <= TEXT_ARTIFACT_BYTES as u64,
                "text_artifact_too_large"
            );
            if let Some(old) = expected.insert(file.digest.clone(), file.bytes) {
                ensure!(old == file.bytes, "artifact_digest_length_conflict");
            }
        }
        ensure!(
            self.objects.len() == expected.len(),
            "artifact_object_set_conflict"
        );
        let mut seen = BTreeSet::new();
        for object in &self.objects {
            ensure!(seen.insert(&object.digest), "duplicate_artifact_object");
            ensure!(
                object.text.len() <= TEXT_FILE_BYTES
                    && expected.get(&object.digest) == Some(&(object.text.len() as u64))
                    && ContentDigest::of_bytes(object.text.as_bytes()) == object.digest,
                "artifact_object_digest_or_length_conflict"
            );
        }
        Ok(())
    }
}

// serde_json::Value and internally tagged enums can discard duplicate object keys while
// buffering. This bounded prepass rejects them recursively before typed deserialization.
struct UniqueJson;

pub(crate) fn reject_duplicate_json_keys(bytes: &[u8]) -> Result<()> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let _ = UniqueJson::deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(())
}

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                out.write_str("JSON without duplicate object keys")
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                _: bool,
            ) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<UniqueJson, A::Error> {
                while seq.next_element::<UniqueJson>()?.is_some() {}
                Ok(UniqueJson)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<UniqueJson, A::Error> {
                let mut keys = BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    let _ = map.next_value::<UniqueJson>()?;
                }
                Ok(UniqueJson)
            }
        }
        decoder.deserialize_any(Visitor)
    }
}

/// A canonical lowercase SHA256 digest. Parsing never verifies the referenced bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ContentDigest(String);

impl ContentDigest {
    /// Validate the digest representation without granting trust to its producer.
    pub fn parse(value: String) -> Result<Self> {
        ensure!(
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "content digest must be 64 lowercase hexadecimal characters"
        );
        Ok(Self(value))
    }

    /// Canonical content address suitable for an opaque storage key.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Hash the exact bytes, independently of any caller-supplied address.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl<'de> Deserialize<'de> for ContentDigest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// One regular file in a virtual artifact tree. Symlinks and device nodes have no representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactFile {
    /// Canonical relative path inside the artifact, never a host destination path.
    pub path: String,
    /// Intended bytes, verified by the protected storage implementation before sealing.
    pub digest: ContentDigest,
    /// Exact uncompressed length.
    pub bytes: u64,
}

/// A bounded immutable artifact description; file order is canonicalized before identity assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    /// Fixed wire format version for the first managed profile.
    pub version: u32,
    /// Regular files only; contents live in the supervisor-owned immutable store.
    pub files: Vec<ArtifactFile>,
}

impl ArtifactManifest {
    /// Reject malformed/unbounded input before producing deterministic manifest bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MANIFEST_LIMIT,
            "artifact manifest exceeds limit"
        );
        let mut manifest: Self = serde_json::from_slice(bytes)?;
        manifest.normalize()?;
        Ok(manifest)
    }

    /// Validate bounds and sort paths. Duplicate paths and file/parent aliases are errors.
    pub fn normalize(&mut self) -> Result<()> {
        ensure!(self.version == 1, "unsupported artifact manifest version");
        ensure!(self.files.len() <= FILE_LIMIT, "too many artifact files");
        self.files.sort_by(|left, right| left.path.cmp(&right.path));
        let mut paths = BTreeSet::new();
        let mut total = 0_u64;
        for file in &self.files {
            validate_artifact_path(&file.path)?;
            ensure!(paths.insert(file.path.as_str()), "duplicate artifact path");
            ensure!(
                file.bytes <= FILE_BYTES_LIMIT,
                "artifact file exceeds limit"
            );
            total = total
                .checked_add(file.bytes)
                .ok_or_else(|| anyhow::anyhow!("artifact size overflow"))?;
            ensure!(
                total <= ARTIFACT_BYTES_LIMIT,
                "artifact exceeds aggregate byte limit"
            );
        }
        for path in &paths {
            for (position, byte) in path.bytes().enumerate() {
                if byte == b'/' {
                    ensure!(
                        !paths.contains(&path[..position]),
                        "artifact file also used as directory"
                    );
                }
            }
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= MANIFEST_LIMIT,
            "artifact manifest exceeds limit"
        );
        Ok(())
    }

    /// Deterministic bytes for the validated manifest. Callers hash and seal these exact bytes.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let mut manifest = self.clone();
        manifest.normalize()?;
        Ok(serde_json::to_vec(&manifest)?)
    }
}

fn validate_artifact_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty() && path.len() <= 512,
        "artifact path length is invalid"
    );
    ensure!(
        !path
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'\\'),
        "artifact path contains unsupported characters"
    );
    ensure!(
        path.split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".."),
        "artifact path must be canonical and relative"
    );
    // Reject platform aliases rather than interpreting Windows paths on a Unix host.
    ensure!(
        !path.contains(':'),
        "artifact path cannot contain a drive or alternate stream"
    );
    Ok(())
}

/// One already established physical-effect disposition, referenced by closure reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectDisposition {
    /// Historical committed selection; later revocation cannot erase this effect.
    Published {
        /// Immutable publication receipt. This reference itself is not authenticated proof.
        receipt: String,
        /// Exact selected destination generation.
        generation: i64,
        /// Committed immutable manifest.
        manifest: ContentDigest,
    },
    /// A sealed intent was proven never selected and permanently abandoned.
    Abandoned {
        /// Authenticated reconciliation record, validated by the owner before closure.
        receipt: String,
    },
    /// A physical effect cannot currently be established; it prevents automatic closure.
    Unknown {
        /// Bounded causal diagnostic retained for the responsible decision.
        reason: String,
    },
}

/// A member of the complete sealed effect set for an admitted attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciledEffect {
    /// Stable logical effect identity, never repurposed for changed bytes.
    pub effect: String,
    /// Established or unresolved physical disposition.
    pub disposition: EffectDisposition,
}

/// Check set completeness without treating supplied receipts as authentic closure authority.
/// The scheduler/runtime transaction still validates every referenced receipt and lifecycle fact.
pub(crate) fn validate_effect_set(
    sealed_effect_ids: &[String],
    reconciled: &[ReconciledEffect],
) -> Result<()> {
    ensure!(
        sealed_effect_ids.len() <= FILE_LIMIT && reconciled.len() <= FILE_LIMIT,
        "effect set exceeds limit"
    );
    let expected: BTreeSet<_> = sealed_effect_ids.iter().collect();
    ensure!(
        expected.len() == sealed_effect_ids.len(),
        "duplicate sealed effect identity"
    );
    let actual: BTreeSet<_> = reconciled.iter().map(|effect| &effect.effect).collect();
    ensure!(
        actual.len() == reconciled.len() && actual == expected,
        "effect reconciliation is incomplete or duplicated"
    );
    for effect in reconciled {
        ensure!(
            !effect.effect.is_empty() && effect.effect.len() <= 128,
            "invalid effect identity"
        );
        match &effect.disposition {
            EffectDisposition::Published {
                receipt,
                generation,
                ..
            } => {
                ensure!(*generation > 0, "invalid publication generation");
                validate_receipt_ref(receipt)?;
            }
            EffectDisposition::Abandoned { receipt } => validate_receipt_ref(receipt)?,
            EffectDisposition::Unknown { reason } => {
                ensure!(
                    !reason.trim().is_empty() && reason.len() <= 1024,
                    "invalid uncertainty reason"
                );
                anyhow::bail!("effect state is unknown: {}", effect.effect);
            }
        }
    }
    Ok(())
}

fn validate_receipt_ref(receipt: &str) -> Result<()> {
    ensure!(
        !receipt.is_empty() && receipt.len() <= 256 && !receipt.chars().any(char::is_control),
        "invalid receipt reference"
    );
    Ok(())
}

/// Protected immutable content storage. It exposes no arbitrary destination writes or deletion.
/// Runtime must persist the intent/retention pin before calling `seal`; this type grants no
/// SQLite selection or effect authority, and all filesystem operations stay outside transactions.
pub(crate) struct ArtifactStore {
    directory: RuntimeDirectory,
}

/// Physical content evidence returned only after bounded digest verification and durable storage.
/// A seal alone is not a publication receipt; the current transaction must validate its retained pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SealedBlob {
    digest: ContentDigest,
    bytes: u64,
    device: u64,
    inode: u64,
}

/// Created by the protected store after checking every referenced immutable object.
/// Only this non-deserializable value may establish a new durable manifest seal.
#[derive(Debug, Serialize)]
pub(crate) struct SealedManifest {
    manifest: SealedBlob,
    files: Vec<SealedBlob>,
}

impl ArtifactStore {
    /// Open an existing operator-approved private directory without following any symlink component.
    /// The protected runtime account owns it; native clients must not see this directory or its FD.
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let directory = RuntimeDirectory::open(root)?;
        Ok(Self { directory })
    }

    fn read_bounded(&self, digest: &ContentDigest, limit: usize) -> Result<Vec<u8>> {
        let fd = openat(
            &self.directory.fd,
            digest.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let stat = fstat(&fd)?;
        ensure!(
            FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
                && stat.st_uid == rustix::process::geteuid().as_raw()
                && stat.st_mode & 0o222 == 0
                && usize::try_from(stat.st_size)? <= limit,
            "invalid_immutable_text_object"
        );
        let mut bytes = Vec::new();
        File::from(fd)
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= limit
                && bytes.len() == usize::try_from(stat.st_size)?
                && ContentDigest::of_bytes(&bytes) == *digest,
            "immutable_text_object_corrupt"
        );
        Ok(bytes)
    }

    /// Read and hash the original selected text bytes for a successor prompt.
    pub(crate) fn read_text_artifact(
        &self,
        digest: &ContentDigest,
    ) -> Result<Vec<ManagedTextFile>> {
        let bytes = self.read_bounded(digest, TEXT_MANIFEST_BYTES)?;
        let manifest = ArtifactManifest::decode(&bytes)?;
        ensure!(
            !manifest.files.is_empty() && manifest.files.len() <= TEXT_FILE_LIMIT,
            "text_file_count_invalid"
        );
        ensure!(
            manifest.canonical_bytes()? == bytes,
            "stored_text_manifest_not_canonical"
        );
        let mut files = Vec::with_capacity(manifest.files.len());
        let mut total = 0u64;
        for file in manifest.files {
            total = total
                .checked_add(file.bytes)
                .context("text_size_overflow")?;
            ensure!(
                file.bytes <= TEXT_FILE_BYTES as u64 && total <= TEXT_ARTIFACT_BYTES as u64,
                "stored_text_artifact_exceeds_limits"
            );
            let bytes = self.read_bounded(&file.digest, TEXT_FILE_BYTES)?;
            ensure!(
                bytes.len() as u64 == file.bytes,
                "stored_text_length_conflict"
            );
            files.push(ManagedTextFile {
                path: file.path,
                text: String::from_utf8(bytes)?,
            });
        }
        validate_text_files(&files)?;
        Ok(files)
    }

    /// Requires a previously persisted retention pin for this manifest and every file.
    /// This performs I/O and must finish before opening the publication transaction.
    pub(crate) fn seal_manifest(&self, manifest: &ArtifactManifest) -> Result<SealedManifest> {
        let bytes = manifest.canonical_bytes()?;
        let mut files = Vec::with_capacity(manifest.files.len());
        for file in &manifest.files {
            files.push(self.verify(&file.digest, file.bytes)?);
        }
        files.sort_by(|left, right| left.digest.cmp(&right.digest));
        let digest = ContentDigest::of_bytes(&bytes);
        let manifest = self.seal(&digest, bytes.len() as u64, bytes.as_slice())?;
        Ok(SealedManifest { manifest, files })
    }

    /// Copy a bounded stream into a new immutable object with atomic absent-or-identical semantics.
    /// The digest and length are verified from bytes, not trusted from the native report.
    pub(crate) fn seal<R: Read>(
        &self,
        expected: &ContentDigest,
        expected_bytes: u64,
        mut input: R,
    ) -> Result<SealedBlob> {
        ensure!(
            expected_bytes <= FILE_BYTES_LIMIT,
            "artifact file exceeds limit"
        );
        let temporary = format!(".staging-{}", uuid::Uuid::new_v4());
        let fd = openat(
            &self.directory.fd,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?;
        let mut file = File::from(fd);
        let result = (|| -> Result<()> {
            let mut hasher = Sha256::new();
            let mut bytes = 0_u64;
            let mut buffer = [0_u8; 8192];
            loop {
                // Read at most one excess byte, so an unexpected large producer stays bounded.
                let remaining = expected_bytes
                    .checked_sub(bytes)
                    .context("artifact length exceeded")?;
                let limit = usize::try_from(remaining.min(buffer.len() as u64 - 1) + 1)?;
                let count = input.read(&mut buffer[..limit])?;
                if count == 0 {
                    break;
                }
                bytes = bytes
                    .checked_add(count as u64)
                    .context("artifact length overflow")?;
                ensure!(
                    bytes <= expected_bytes,
                    "artifact length differs from intent"
                );
                hasher.update(&buffer[..count]);
                file.write_all(&buffer[..count])?;
            }
            ensure!(
                bytes == expected_bytes && format!("{:x}", hasher.finalize()) == expected.as_str(),
                "artifact bytes differ from intent"
            );
            file.set_permissions(std::fs::Permissions::from_mode(0o400))?;
            file.sync_all()?;
            match linkat(
                &self.directory.fd,
                temporary.as_str(),
                &self.directory.fd,
                expected.as_str(),
                AtFlags::empty(),
            ) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => {
                    self.verify(expected, expected_bytes)?;
                }
                Err(error) => return Err(error.into()),
            }
            Ok(())
        })();
        drop(file);
        // Remove only our private random temporary name. Never overwrite/delete a content key.
        let cleanup = unlinkat(&self.directory.fd, temporary.as_str(), AtFlags::empty());
        fsync(&self.directory.fd)?;
        result?;
        cleanup?;
        self.verify(expected, expected_bytes)
    }

    /// Reconcile existing immutable bytes after a lost response or restart. Corruption stays an error.
    pub(crate) fn verify(
        &self,
        expected: &ContentDigest,
        expected_bytes: u64,
    ) -> Result<SealedBlob> {
        ensure!(
            expected_bytes <= FILE_BYTES_LIMIT,
            "artifact file exceeds limit"
        );
        let fd = openat(
            &self.directory.fd,
            expected.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let stat = fstat(&fd)?;
        ensure!(
            FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile,
            "artifact object is not a regular file"
        );
        ensure!(
            stat.st_uid == rustix::process::geteuid().as_raw() && stat.st_mode & 0o222 == 0,
            "artifact object is not immutable runtime-owned storage"
        );
        ensure!(
            u64::try_from(stat.st_size)? == expected_bytes,
            "artifact length changed"
        );
        let mut file = File::from(fd);
        let mut hasher = Sha256::new();
        let mut bytes = 0_u64;
        let mut buffer = [0_u8; 8192];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            bytes = bytes
                .checked_add(count as u64)
                .context("artifact length overflow")?;
            ensure!(
                bytes <= expected_bytes,
                "artifact object grew during verification"
            );
            hasher.update(&buffer[..count]);
        }
        ensure!(
            bytes == expected_bytes && format!("{:x}", hasher.finalize()) == expected.as_str(),
            "artifact integrity mismatch"
        );
        let metadata = file.metadata()?;
        Ok(SealedBlob {
            digest: expected.clone(),
            bytes,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::symlink};

    fn storage() -> (tempfile::TempDir, ArtifactStore) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let store = ArtifactStore::open(&root).unwrap();
        (temporary, store)
    }

    #[test]
    fn immutable_seal_is_idempotent_and_never_overwrites_conflicting_content() {
        let (temporary, store) = storage();
        let digest = ContentDigest::of_bytes(b"artifact");
        let first = store.seal(&digest, 8, b"artifact".as_slice()).unwrap();
        assert_eq!(
            store.seal(&digest, 8, b"artifact".as_slice()).unwrap(),
            first
        );
        assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 1);
        let path = temporary.path().join(digest.as_str());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, b"conflict").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(store.verify(&digest, 8).is_err());
        assert!(store.seal(&digest, 8, b"artifact".as_slice()).is_err());
        assert_eq!(fs::read(path).unwrap(), b"conflict");
        assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_seal_removes_only_its_private_temporary_object() {
        let (temporary, store) = storage();
        let digest = ContentDigest::of_bytes(b"artifact");
        for (length, content) in [
            (8, b"short".as_slice()),
            (7, b"artifact".as_slice()),
            (8, b"changed!".as_slice()),
        ] {
            assert!(store.seal(&digest, length, content).is_err());
            assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 0);
        }
        symlink("missing", temporary.path().join(digest.as_str())).unwrap();
        assert!(store.seal(&digest, 8, b"artifact".as_slice()).is_err());
        assert!(
            fs::symlink_metadata(temporary.path().join(digest.as_str()))
                .unwrap()
                .is_symlink()
        );
        assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 1);
    }

    #[test]
    fn closure_reconciliation_requires_every_sealed_effect_and_known_disposition() {
        let sealed = vec!["a".to_owned(), "b".to_owned()];
        let a = ReconciledEffect {
            effect: "a".into(),
            disposition: EffectDisposition::Abandoned {
                receipt: "receipt-a".into(),
            },
        };
        let b = ReconciledEffect {
            effect: "b".into(),
            disposition: EffectDisposition::Published {
                receipt: "receipt-b".into(),
                generation: 1,
                manifest: ContentDigest::of_bytes(b"manifest"),
            },
        };
        assert!(validate_effect_set(&sealed, &[a.clone(), b.clone()]).is_ok());
        assert!(validate_effect_set(&sealed, std::slice::from_ref(&a)).is_err());
        assert!(validate_effect_set(&sealed, &[a.clone(), a.clone()]).is_err());
        let unknown = ReconciledEffect {
            effect: "b".into(),
            disposition: EffectDisposition::Unknown {
                reason: "lost seal".into(),
            },
        };
        assert!(validate_effect_set(&sealed, &[a, unknown]).is_err());
        assert!(validate_effect_set(&[], &[]).is_ok());
        assert!(validate_effect_set(&[], &[b]).is_err());
    }
}

/// Immutable logical publication identity. It grants no runtime or task authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationRequest {
    /// Original admitted scheduler correlation, never a replacement attempt.
    pub correlation: crate::execution::Correlation,
    /// Stable effect retry identity, scoped to the original attempt.
    pub effect: String,
    /// Registered controlled-artifact destination.
    pub destination: String,
    /// Exact manifest whose referenced bytes were retained before publication.
    pub manifest: ArtifactManifest,
    /// Literal allowed and approved scope unit validated by the model owner.
    pub scope_unit: String,
    /// Exact current destination generation; absence is not a wildcard.
    pub expected_generation: i64,
    /// Exact previous selection, including the original empty selection.
    pub expected_manifest: Option<ContentDigest>,
    /// Optional additional task CAS, independent of snapshot freshness.
    pub expected_task_version: Option<i64>,
}

impl PublicationRequest {
    fn canonical(&self) -> Result<String> {
        ensure!(
            !self.effect.is_empty()
                && self.effect.len() <= 128
                && !self.effect.chars().any(char::is_control),
            "invalid effect identity"
        );
        ensure!(
            !self.destination.is_empty()
                && self.destination.len() <= 128
                && !self.destination.chars().any(char::is_control),
            "invalid destination identity"
        );
        ensure!(
            !self.scope_unit.trim().is_empty()
                && self.scope_unit.len() <= 1024
                && !self.scope_unit.chars().any(char::is_control),
            "invalid publication scope"
        );
        ensure!(
            self.expected_generation > 0
                && self.expected_task_version.is_none_or(|version| version > 0),
            "invalid publication CAS"
        );
        let mut canonical = self.clone();
        canonical.manifest.normalize()?;
        let bytes = serde_json::to_string(&canonical)?;
        ensure!(
            bytes.len() <= MANIFEST_LIMIT + 8192,
            "publication request exceeds limit"
        );
        Ok(bytes)
    }
}

/// Historical committed receipt. Reading it grants no new selection or task acceptance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationReceipt {
    /// Immutable receipt ID.
    pub id: String,
    /// Exact original request, retained for authenticated retry comparison.
    pub request: PublicationRequest,
    /// Newly selected destination generation.
    pub generation: i64,
    /// Exact canonical manifest content address.
    pub manifest: ContentDigest,
    /// Current version observed in the successful writer transaction.
    pub task_version: i64,
    /// Original semantic inputs revalidated in that transaction.
    pub input_epoch: i64,
    /// Commit intent timestamp; SQLite commit is the selection point.
    pub created: i64,
}

fn decode_publication_receipt(
    encoded: &str,
    id: &str,
    canonical: &str,
    created: i64,
) -> Result<PublicationReceipt> {
    crate::bounded(
        encoded,
        MANIFEST_LIMIT + 16384,
        "stored publication receipt",
    )?;
    let receipt: PublicationReceipt = serde_json::from_str(encoded)?;
    validate_receipt_ref(id)?;
    ensure!(
        receipt.id == id
            && receipt.request.canonical()? == canonical
            && receipt.generation
                == receipt
                    .request
                    .expected_generation
                    .checked_add(1)
                    .context("publication_generation_overflow")?
            && receipt.manifest
                == ContentDigest::of_bytes(&receipt.request.manifest.canonical_bytes()?)
            && receipt.created == created,
        "protected_publication_receipt_conflict"
    );
    Ok(receipt)
}

async fn validate_admitted_publication_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &PublicationRequest,
    inputs: &crate::task_graph::InputSnapshot,
    now: i64,
) -> Result<()> {
    let admitted = crate::runtime_lifecycle::admitted_artifact_tx(tx, &request.correlation).await?;
    ensure!(
        request.destination == admitted.destination
            && request.expected_generation == admitted.destination_generation
            && request.expected_manifest == admitted.destination_manifest
            && request.scope_unit == admitted.scope_unit
            && request
                .manifest
                .files
                .iter()
                .all(|file| admitted.allowed_paths.contains(&file.path)),
        "publication_differs_from_original_artifact_binding"
    );
    crate::task_graph::validate_artifact_binding_current_tx(
        tx,
        &admitted.authority,
        inputs,
        &request.scope_unit,
        now,
    )
    .await?;
    Ok(())
}

impl crate::store::Store {
    /// Publish bounded text bytes through the original admitted binding and current guards.
    /// Pins and conservative storage reservations commit before filesystem writes; selection
    /// and its immutable receipt commit together after fresh authority revalidation.
    ///
    /// # Errors
    /// Rejects forged/stale producers, changed retries, altered bindings, quota exhaustion,
    /// corrupt bytes and failed storage. Such failures do not discard retained effects.
    pub async fn publish_managed_artifact(
        &self,
        actor: &crate::store::Mailbox,
        request: &PublicationRequest,
        contents: &ManagedArtifactContents,
        now: i64,
    ) -> Result<crate::execution::Checked<PublicationReceipt>> {
        use crate::{execution::Checked, runtime_adapter::ManagedRuntimeGate, runtime_lifecycle};
        ensure!(now >= 0, "invalid_publication_time");
        contents.validate(&request.manifest)?;
        request.canonical()?;
        let mut tx = self.pool().begin().await?;
        runtime_lifecycle::authenticate_producer_tx(&mut tx, actor, &request.correlation).await?;
        let prior: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM runtime_receipts WHERE attempt=? AND effect=?)",
        )
        .bind(&request.correlation.attempt)
        .bind(&request.effect)
        .fetch_one(&mut *tx)
        .await?;
        if prior {
            let receipt = publish_artifact_tx(&mut tx, &ManagedRuntimeGate, request, now).await?;
            tx.commit().await?;
            return Ok(receipt);
        }
        tx.commit().await?;
        let io_custody =
            crate::runtime_capture::effect_lock(self, &request.correlation, false).await?;
        let mut tx = self.pool().begin().await?;
        runtime_lifecycle::authenticate_producer_tx(&mut tx, actor, &request.correlation).await?;
        let prior: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM runtime_receipts WHERE attempt=? AND effect=?)",
        )
        .bind(&request.correlation.attempt)
        .bind(&request.effect)
        .fetch_one(&mut *tx)
        .await?;
        if prior {
            let receipt = publish_artifact_tx(&mut tx, &ManagedRuntimeGate, request, now).await?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if let Checked::Held(holds) =
            pin_publication_tx(&mut tx, &ManagedRuntimeGate, request, now).await?
        {
            tx.commit().await?;
            return Ok(Checked::Held(holds));
        }
        let admitted =
            runtime_lifecycle::admitted_artifact_tx(&mut tx, &request.correlation).await?;
        admitted.specification.validate()?;
        let root = admitted
            .specification
            .artifact_root
            .context("artifact_storage_root_missing")?;
        // Every I/O invocation reserves again, including exact retries. A crashed staging
        // file consumes its original reservation permanently and cannot amplify free space.
        let logical_bytes = request
            .manifest
            .files
            .iter()
            .try_fold(0u64, |total, file| {
                total
                    .checked_add(file.bytes)
                    .context("artifact_reservation_overflow")
            })?;
        let reserved = logical_bytes
            .checked_add(request.manifest.canonical_bytes()?.len() as u64)
            .and_then(|bytes| bytes.checked_add(((request.manifest.files.len() + 1) * 4096) as u64))
            .and_then(|bytes| bytes.checked_mul(2))
            .context("artifact_reservation_overflow")?;
        sqlx::query("INSERT INTO runtime_artifact_reservations(id,attempt,effect,bytes,created) VALUES(?,?,?,?,?)")
            .bind(uuid::Uuid::new_v4().to_string()).bind(&request.correlation.attempt).bind(&request.effect)
            .bind(i64::try_from(reserved)?).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        let owned_contents = contents.clone();
        let manifest = request.manifest.clone();
        let (seal, _io_custody) = tokio::task::spawn_blocking(
            move || -> Result<(SealedManifest, Option<crate::runtime_capture::CustodyLock>)> {
                let storage = ArtifactStore::open(&root)?;
                for object in &owned_contents.objects {
                    storage.seal(
                        &object.digest,
                        object.text.len() as u64,
                        object.text.as_bytes(),
                    )?;
                }
                let seal = storage.seal_manifest(&manifest)?;
                Ok((seal, io_custody))
            },
        )
        .await
        .context("artifact storage worker failed")??;
        // Physical retention is historical and survives a later producer revocation.
        let mut tx = self.pool().begin().await?;
        let home = sqlx::query(
            "UPDATE groups SET paused=paused WHERE name=? AND home_machine=(SELECT id FROM node)",
        )
        .bind(&request.correlation.group)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(home == 1, "artifact retention requires original home group");
        record_manifest_seal_tx(&mut tx, request, &seal).await?;
        tx.commit().await?;
        let mut tx = self.pool().begin().await?;
        runtime_lifecycle::authenticate_producer_tx(&mut tx, actor, &request.correlation).await?;
        // Reuse the exact canonical request; no refreshed task/destination/binding CAS.
        let result =
            publish_artifact_tx(&mut tx, &ManagedRuntimeGate, request, crate::now()?).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Read an original producer's committed publication without granting a new selection.
    ///
    /// # Errors
    /// Rejects unauthorized original producers, invalid selectors and corrupt protected receipts.
    pub async fn managed_publication(
        &self,
        actor: &crate::store::Mailbox,
        correlation: &crate::execution::Correlation,
        effect: &str,
        now: i64,
    ) -> Result<Option<PublicationReceipt>> {
        ensure!(now >= 0, "invalid_publication_observation_time");
        crate::bounded(effect, 128, "publication effect")?;
        ensure!(!effect.is_empty(), "publication_effect_required");
        let mut tx = self.pool().begin().await?;
        crate::runtime_lifecycle::authenticate_producer_tx(&mut tx, actor, correlation).await?;
        let row: Option<(String, String, String, i64)> = sqlx::query_as("SELECT id,canonical_request,observation,created FROM runtime_receipts WHERE attempt=? AND effect=? AND disposition='published'")
            .bind(&correlation.attempt).bind(effect).fetch_optional(&mut *tx).await?;
        let result = if let Some((id, canonical, encoded, created)) = row {
            let receipt = decode_publication_receipt(&encoded, &id, &canonical, created)?;
            ensure!(
                receipt.request.canonical()? == canonical
                    && receipt.request.correlation == *correlation
                    && receipt.request.effect == effect,
                "protected_publication_receipt_conflict"
            );
            Some(receipt)
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }
}

/// Reserve the immutable intent and permanent retention pin before storage I/O.
/// The runtime caller authenticates its producer before entering this crate-private seam.
/// Held results must be committed by the caller to retain scheduler cause/clock facts.
pub(crate) async fn pin_publication_tx<R: crate::execution::RuntimeGate>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    runtime: &R,
    request: &PublicationRequest,
    now: i64,
) -> Result<crate::execution::Checked<()>> {
    use crate::execution::{self, Checked, CurrentUse};
    let bytes = request.canonical()?;
    let c = &request.correlation;
    let original =
        match execution::validate_current_attempt_tx(tx, runtime, c, CurrentUse::Report, now)
            .await?
        {
            Checked::Ready(original) => original,
            Checked::Held(holds) => return Ok(Checked::Held(holds)),
        };
    let observation =
        crate::task_graph::validate_publication_inputs_tx(tx, &original, &request.scope_unit)
            .await?;
    validate_admitted_publication_tx(tx, request, &original, now).await?;
    ensure!(
        request
            .expected_task_version
            .is_none_or(|version| version == observation.task_version),
        "publication_task_version_conflict"
    );
    let old: Option<String> = sqlx::query_scalar(
        "SELECT canonical_request FROM runtime_effects WHERE attempt=? AND effect=?",
    )
    .bind(&c.attempt)
    .bind(&request.effect)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(old) = old {
        ensure!(old == bytes, "publication_retry_conflict");
        return Ok(Checked::Ready(()));
    }
    let manifest = request.manifest.canonical_bytes()?;
    let digest = ContentDigest::of_bytes(&manifest);
    let available: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_segments s JOIN runtime_destinations d ON d.target=s.target AND d.target_generation=s.target_generation AND d.group_name=s.group_name WHERE s.attempt=? AND s.group_name=? AND s.task=? AND s.fence=? AND s.dispatch_key=? AND s.tombstoned=0 AND s.state='running' AND d.id=? AND d.generation=? AND d.manifest IS ?)")
        .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
        .bind(&request.destination).bind(request.expected_generation)
        .bind(request.expected_manifest.as_ref().map(ContentDigest::as_str))
        .fetch_one(&mut **tx).await?;
    ensure!(available, "publication_destination_or_segment_conflict");
    sqlx::query("INSERT INTO runtime_effects(attempt,effect,group_name,task,destination,canonical_request,manifest,manifest_bytes,scope_unit,expected_generation,expected_manifest,expected_task_version,state,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'pinned',?)")
        .bind(&c.attempt).bind(&request.effect).bind(&c.group).bind(&c.task).bind(&request.destination)
        .bind(bytes).bind(digest.as_str()).bind(i64::try_from(manifest.len())?).bind(&request.scope_unit)
        .bind(request.expected_generation).bind(request.expected_manifest.as_ref().map(ContentDigest::as_str))
        .bind(request.expected_task_version).bind(now).execute(&mut **tx).await?;
    Ok(Checked::Ready(()))
}

/// Record an actual protected-store seal. This is physical evidence, not selection authority.
/// Can reconcile a historical pin after revocation without publishing anything.
pub(crate) async fn record_manifest_seal_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &PublicationRequest,
    seal: &SealedManifest,
) -> Result<()> {
    let bytes = request.canonical()?;
    let canonical_manifest = request.manifest.canonical_bytes()?;
    ensure!(
        seal.manifest.digest == ContentDigest::of_bytes(&canonical_manifest)
            && seal.manifest.bytes == canonical_manifest.len() as u64,
        "publication_seal_does_not_match_intent"
    );
    let seal = serde_json::to_string(seal)?;
    let changed = sqlx::query("UPDATE runtime_effects SET state='sealed',seal=? WHERE attempt=? AND effect=? AND canonical_request=? AND retention_pin=1 AND state='pinned' AND seal IS NULL")
        .bind(&seal).bind(&request.correlation.attempt).bind(&request.effect).bind(&bytes)
        .execute(&mut **tx).await?.rows_affected();
    if changed == 0 {
        let prior: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT canonical_request,seal FROM runtime_effects WHERE attempt=? AND effect=?",
        )
        .bind(&request.correlation.attempt)
        .bind(&request.effect)
        .fetch_optional(&mut **tx)
        .await?;
        ensure!(
            prior == Some((bytes, Some(seal))),
            "publication_seal_conflict_or_missing_pin"
        );
    }
    Ok(())
}

/// Select immutable bytes and insert their receipt in one writer transaction.
/// No filesystem/runtime I/O, commit, slot release, admission or business acceptance occurs here.
/// Only the trusted runtime entrypoint may call this after authenticating the original producer.
pub(crate) async fn publish_artifact_tx<R: crate::execution::RuntimeGate>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    runtime: &R,
    request: &PublicationRequest,
    now: i64,
) -> Result<crate::execution::Checked<PublicationReceipt>> {
    use crate::execution::{self, Checked, CurrentUse};
    let bytes = request.canonical()?;
    let c = &request.correlation;
    // Historical exact replay comes before current positive guards. It selects nothing.
    let prior: Option<(String,String,String,String,i64)> = sqlx::query_as("SELECT id,canonical_request,disposition,observation,created FROM runtime_receipts WHERE attempt=? AND effect=?")
        .bind(&c.attempt).bind(&request.effect).fetch_optional(&mut **tx).await?;
    if let Some((id, old, disposition, encoded, created)) = prior {
        ensure!(
            old == bytes && disposition == "published",
            "publication_retry_conflict"
        );
        return Ok(Checked::Ready(decode_publication_receipt(
            &encoded, &id, &old, created,
        )?));
    }
    let original =
        match execution::validate_current_attempt_tx(tx, runtime, c, CurrentUse::Publish, now)
            .await?
        {
            Checked::Ready(original) => original,
            Checked::Held(holds) => return Ok(Checked::Held(holds)),
        };
    let model =
        crate::task_graph::validate_publication_inputs_tx(tx, &original, &request.scope_unit)
            .await?;
    validate_admitted_publication_tx(tx, request, &original, now).await?;
    ensure!(
        request
            .expected_task_version
            .is_none_or(|version| version == model.task_version),
        "publication_task_version_conflict"
    );
    let pinned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_effects e JOIN runtime_segments s ON s.attempt=e.attempt WHERE e.attempt=? AND e.effect=? AND e.canonical_request=? AND e.state='sealed' AND e.seal IS NOT NULL AND e.retention_pin=1 AND s.tombstoned=0 AND s.state='running')")
        .bind(&c.attempt).bind(&request.effect).bind(&bytes).fetch_one(&mut **tx).await?;
    ensure!(pinned, "publication_requires_current_retained_seal");
    let digest = ContentDigest::of_bytes(&request.manifest.canonical_bytes()?);
    let generation = request
        .expected_generation
        .checked_add(1)
        .context("destination_generation_overflow")?;
    let changed = sqlx::query("UPDATE runtime_destinations SET generation=?,manifest=? WHERE id=? AND group_name=? AND generation=? AND manifest IS ? AND EXISTS(SELECT 1 FROM runtime_segments s WHERE s.attempt=? AND s.target=runtime_destinations.target AND s.target_generation=runtime_destinations.target_generation)")
        .bind(generation).bind(digest.as_str()).bind(&request.destination).bind(&c.group)
        .bind(request.expected_generation).bind(request.expected_manifest.as_ref().map(ContentDigest::as_str))
        .bind(&c.attempt).execute(&mut **tx).await?.rows_affected();
    ensure!(changed == 1, "publication_destination_conflict");
    let receipt = PublicationReceipt {
        id: uuid::Uuid::new_v4().to_string(),
        request: request.clone(),
        generation,
        manifest: digest,
        task_version: model.task_version,
        input_epoch: model.input_epoch,
        created: now,
    };
    sqlx::query("INSERT INTO runtime_receipts(id,attempt,effect,canonical_request,disposition,observation,created) VALUES(?,?,?,?,'published',?,?)")
        .bind(&receipt.id).bind(&c.attempt).bind(&request.effect).bind(bytes)
        .bind(serde_json::to_string(&receipt)?).bind(now).execute(&mut **tx).await?;
    let changed = sqlx::query("UPDATE runtime_effects SET state='published' WHERE attempt=? AND effect=? AND state='sealed'")
        .bind(&c.attempt).bind(&request.effect).execute(&mut **tx).await?.rows_affected();
    ensure!(changed == 1, "publication_effect_state_conflict");
    Ok(Checked::Ready(receipt))
}

/// Freeze the complete effect identity set only after launch is permanently closed
/// and actual containment quiescence is recorded. Empty is a real sealed set.
pub(crate) async fn seal_effect_set_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    correlation: &crate::execution::Correlation,
    now: i64,
) -> Result<String> {
    let eligible: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_segments WHERE attempt=? AND group_name=? AND task=? AND fence=? AND dispatch_key=? AND tombstoned=1 AND state='quiescent')")
        .bind(&correlation.attempt).bind(&correlation.group).bind(&correlation.task)
        .bind(correlation.fence).bind(&correlation.dispatch_key).fetch_one(&mut **tx).await?;
    ensure!(
        eligible,
        "effect_set_requires_original_quiescence_and_tombstone"
    );
    let effects: Vec<String> =
        sqlx::query_scalar("SELECT effect FROM runtime_effects WHERE attempt=? ORDER BY effect")
            .bind(&correlation.attempt)
            .fetch_all(&mut **tx)
            .await?;
    ensure!(effects.len() <= FILE_LIMIT, "effect set exceeds limit");
    let effects = serde_json::to_string(&effects)?;
    let prior: Option<(String, String)> =
        sqlx::query_as("SELECT id,effects FROM runtime_effect_sets WHERE attempt=?")
            .bind(&correlation.attempt)
            .fetch_optional(&mut **tx)
            .await?;
    if let Some((id, original)) = prior {
        ensure!(original == effects, "sealed_effect_set_changed");
        return Ok(id);
    }
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO runtime_effect_sets(attempt,id,effects,created) VALUES(?,?,?,?)")
        .bind(&correlation.attempt)
        .bind(&id)
        .bind(effects)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(id)
}

/// Permanently abandon one never-selected effect after original launch quiescence.
/// Lost authority does not prevent this historical cleanup. It selects no object.
pub(crate) async fn abandon_effect_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &PublicationRequest,
    now: i64,
) -> Result<String> {
    let canonical = request.canonical()?;
    let c = &request.correlation;
    let prior: Option<(String,String,String)> = sqlx::query_as("SELECT id,canonical_request,disposition FROM runtime_receipts WHERE attempt=? AND effect=?")
        .bind(&c.attempt).bind(&request.effect).fetch_optional(&mut **tx).await?;
    if let Some((id, original, disposition)) = prior {
        ensure!(
            original == canonical && disposition == "abandoned",
            "effect_disposition_conflict"
        );
        return Ok(id);
    }
    let eligible: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_effects e JOIN runtime_segments s ON s.attempt=e.attempt JOIN runtime_effect_sets f ON f.attempt=s.attempt WHERE s.attempt=? AND s.group_name=? AND s.task=? AND s.fence=? AND s.dispatch_key=? AND s.tombstoned=1 AND s.state='quiescent' AND e.effect=? AND e.canonical_request=? AND e.state IN ('pinned','sealed'))")
        .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
        .bind(&request.effect).bind(&canonical).fetch_one(&mut **tx).await?;
    ensure!(
        eligible,
        "abandon_requires_original_quiescence_and_unselected_intent"
    );
    let id = uuid::Uuid::new_v4().to_string();
    // Publication always inserts the unique receipt atomically with selection.
    // No existing receipt plus an unselected intent therefore proves this effect never selected.
    sqlx::query("INSERT INTO runtime_receipts(id,attempt,effect,canonical_request,disposition,observation,created) VALUES(?,?,?,?,'abandoned',?,?)")
        .bind(&id).bind(&c.attempt).bind(&request.effect).bind(canonical)
        .bind(serde_json::to_string(&serde_json::json!({"receipt":id,"correlation":c,"effect":request.effect}))?)
        .bind(now).execute(&mut **tx).await?;
    let changed = sqlx::query("UPDATE runtime_effects SET state='abandoned' WHERE attempt=? AND effect=? AND state IN ('pinned','sealed')")
        .bind(&c.attempt).bind(&request.effect).execute(&mut **tx).await?.rows_affected();
    ensure!(changed == 1, "effect_disposition_conflict");
    Ok(id)
}

/// Authenticate the complete sealed set against actual immutable runtime receipts.
/// Every pinned effect must have one matching terminal disposition; partial sets remain held.
pub(crate) async fn effect_set_reconciled_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt: &str,
    effect_set: &str,
) -> Result<bool> {
    let encoded: Option<String> =
        sqlx::query_scalar("SELECT effects FROM runtime_effect_sets WHERE id=? AND attempt=?")
            .bind(effect_set)
            .bind(attempt)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(encoded) = encoded else {
        return Ok(false);
    };
    ensure!(
        encoded.len() <= MANIFEST_LIMIT,
        "sealed effect set exceeds limit"
    );
    let expected: Vec<String> = serde_json::from_str(&encoded)?;
    let actual: Vec<EffectReceiptRow> = sqlx::query_as("SELECT e.effect,e.state,e.canonical_request,r.id AS receipt,r.disposition,r.canonical_request AS receipt_request,r.observation,r.created AS receipt_created FROM runtime_effects e LEFT JOIN runtime_receipts r ON r.attempt=e.attempt AND r.effect=e.effect WHERE e.attempt=? ORDER BY e.effect")
        .bind(attempt).fetch_all(&mut **tx).await?;
    ensure!(actual.len() <= FILE_LIMIT, "effect set exceeds limit");
    let mut reconciled = Vec::with_capacity(actual.len());
    for row in actual {
        let Some(effect) = authenticated_effect_receipt(attempt, row)? else {
            return Ok(false);
        };
        reconciled.push(effect);
    }
    // The complete-set validator operates on protected, authenticated receipt bodies.
    // Missing/extra entries remain held; receipt corruption above is an error.
    Ok(validate_effect_set(&expected, &reconciled).is_ok())
}

#[derive(sqlx::FromRow)]
struct EffectReceiptRow {
    effect: String,
    state: String,
    canonical_request: String,
    receipt: Option<String>,
    disposition: Option<String>,
    receipt_request: Option<String>,
    observation: Option<String>,
    receipt_created: Option<i64>,
}

fn authenticated_effect_receipt(
    attempt: &str,
    row: EffectReceiptRow,
) -> Result<Option<ReconciledEffect>> {
    if !matches!(row.state.as_str(), "published" | "abandoned") || row.receipt.is_none() {
        return Ok(None);
    }
    ensure!(
        row.disposition.as_deref() == Some(row.state.as_str())
            && row.receipt_request.as_deref() == Some(row.canonical_request.as_str()),
        "protected_effect_receipt_conflict"
    );
    crate::bounded(
        &row.canonical_request,
        MANIFEST_LIMIT + 8192,
        "stored publication request",
    )?;
    let request: PublicationRequest = serde_json::from_str(&row.canonical_request)?;
    ensure!(
        request.correlation.attempt == attempt
            && request.effect == row.effect
            && request.canonical()? == row.canonical_request,
        "protected_effect_request_conflict"
    );
    let id = row.receipt.context("protected_effect_receipt_missing")?;
    validate_receipt_ref(&id)?;
    let observation = row
        .observation
        .context("protected_effect_receipt_body_missing")?;
    crate::bounded(
        &observation,
        MANIFEST_LIMIT + 16384,
        "stored effect receipt",
    )?;
    let disposition = if row.state == "published" {
        let receipt = decode_publication_receipt(
            &observation,
            &id,
            &row.canonical_request,
            row.receipt_created
                .context("protected_receipt_time_missing")?,
        )?;
        // Later selections do not invalidate this immutable historical disposition.
        EffectDisposition::Published {
            receipt: id,
            generation: receipt.generation,
            manifest: receipt.manifest,
        }
    } else {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AbandonedReceipt {
            receipt: String,
            correlation: crate::execution::Correlation,
            effect: String,
        }
        let receipt: AbandonedReceipt = serde_json::from_str(&observation)?;
        ensure!(
            receipt.receipt == id
                && receipt.correlation == request.correlation
                && receipt.effect == request.effect,
            "protected_abandonment_receipt_conflict"
        );
        EffectDisposition::Abandoned { receipt: id }
    };
    Ok(Some(ReconciledEffect {
        effect: row.effect,
        disposition,
    }))
}

#[cfg(test)]
mod receipt_body_tests {
    use super::*;

    #[test]
    fn closure_rejects_a_terminal_row_with_a_conflicting_retained_receipt_body() {
        let request = PublicationRequest {
            correlation: crate::execution::Correlation {
                group: "g".into(),
                task: "t".into(),
                attempt: "attempt".into(),
                fence: 1,
                dispatch_key: "dispatch".into(),
            },
            effect: "effect".into(),
            destination: "destination".into(),
            manifest: ArtifactManifest {
                version: 1,
                files: vec![ArtifactFile {
                    path: "report.txt".into(),
                    digest: ContentDigest::of_bytes(b"text"),
                    bytes: 4,
                }],
            },
            scope_unit: "artifact".into(),
            expected_generation: 1,
            expected_manifest: None,
            expected_task_version: None,
        };
        let receipt = PublicationReceipt {
            id: "receipt".into(),
            request: request.clone(),
            generation: 2,
            manifest: ContentDigest::of_bytes(&request.manifest.canonical_bytes().unwrap()),
            task_version: 1,
            input_epoch: 1,
            created: 100,
        };
        let row = |receipt: &PublicationReceipt| EffectReceiptRow {
            effect: request.effect.clone(),
            state: "published".into(),
            canonical_request: request.canonical().unwrap(),
            receipt: Some("receipt".into()),
            disposition: Some("published".into()),
            receipt_request: Some(request.canonical().unwrap()),
            observation: Some(serde_json::to_string(receipt).unwrap()),
            receipt_created: Some(100),
        };
        assert!(
            authenticated_effect_receipt("attempt", row(&receipt))
                .unwrap()
                .is_some()
        );
        let mut corrupt = receipt.clone();
        corrupt.generation = 3;
        assert!(authenticated_effect_receipt("attempt", row(&corrupt)).is_err());
        corrupt = receipt.clone();
        corrupt.request.correlation.dispatch_key = "replacement".into();
        assert!(authenticated_effect_receipt("attempt", row(&corrupt)).is_err());
        corrupt = receipt.clone();
        corrupt.manifest = ContentDigest::of_bytes(b"another manifest");
        assert!(authenticated_effect_receipt("attempt", row(&corrupt)).is_err());
        let mut missing = row(&receipt);
        missing.receipt = None;
        assert!(
            authenticated_effect_receipt("attempt", missing)
                .unwrap()
                .is_none()
        );
    }
}
