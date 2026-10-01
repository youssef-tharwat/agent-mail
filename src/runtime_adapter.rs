//! Managed target descriptors and truthful per-profile capability projections.
//!
//! Registration describes a target and starts nothing. Neither a configuration
//! value nor a capability projection authorizes an attempt. Actual admission and
//! publication compose the scheduler/model transaction helpers with runtime facts.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{bounded, managed_runtime::NativeClient, name, runtime_effects::ContentDigest};

/// Receipt of configuration registration, never evidence of execution readiness.
#[derive(Debug, Serialize)]
pub struct ManagedTargetRegistration {
    /// Stable identity retained across configuration generations.
    pub identity: String,
    /// Current immutable configuration generation.
    pub generation: i64,
    /// Existing enablement is preserved only for an exact registration replay.
    pub enabled: bool,
}

/// Operator-provisioned policy format. It is read from private installation storage;
/// callers may reference its name but cannot submit policy bytes or capability witnesses.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedPolicyDocument {
    pub(crate) format: u8,
    pub(crate) client: NativeClient,
    pub(crate) profile: RuntimeProfile,
    pub(crate) client_version: String,
    pub(crate) executable: PathBuf,
    pub(crate) launcher: PathBuf,
    pub(crate) delegated_cgroup: PathBuf,
    pub(crate) effective_configuration: serde_json::Value,
}

pub(crate) struct ResolvedManagedPolicy {
    pub(crate) document: ManagedPolicyDocument,
    pub(crate) policy_digest: ContentDigest,
    pub(crate) executable_digest: ContentDigest,
}

/// Read and hash actual files without starting the client or reading credential stores.
/// This establishes configuration identity only; qualification is a separate native witness.
pub(crate) fn resolve_managed_policy(
    installation: &Path,
    spec: &ManagedTargetSpec,
) -> Result<ResolvedManagedPolicy> {
    use rustix::fs::{FileType, Mode, OFlags, fstat, openat};
    use std::{fs::File, io::Read};
    spec.validate()?;
    name(&spec.configuration)?;
    let directory =
        crate::managed_runtime::RuntimeDirectory::open(&installation.join("managed-policies"))?;
    let fd = openat(
        &directory.fd,
        format!("{}.json", spec.configuration),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let stat = fstat(&fd)?;
    ensure!(
        FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
            && stat.st_uid == rustix::process::geteuid().as_raw()
            && stat.st_mode & 0o077 == 0
            && stat.st_nlink == 1
            && stat.st_size <= 65536,
        "managed policy must be a private regular file"
    );
    let mut bytes = Vec::new();
    File::from(fd).take(65537).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 65536, "managed policy exceeds limit");
    let document: ManagedPolicyDocument = serde_json::from_slice(&bytes)?;
    ensure!(
        document.format == 1 && document.client == spec.client && document.profile == spec.profile,
        "managed policy format or client/profile conflict"
    );
    bounded(&document.client_version, 128, "native client version")?;
    ensure!(
        !document.client_version.is_empty() && document.effective_configuration.is_object(),
        "managed policy requires an exact client version and resolved configuration"
    );
    validate_absolute_path(&document.delegated_cgroup)?;
    let executable_digest = digest_executable(&document.executable)?;
    let launcher_digest = digest_executable(&document.launcher)?;
    // Pin actual launcher bytes along with exact policy bytes. A changed launcher invalidates
    // the old witness even when its configured pathname and version label are unchanged.
    let policy_digest = ContentDigest::of_bytes(&serde_json::to_vec(&(bytes, launcher_digest))?);
    Ok(ResolvedManagedPolicy {
        document,
        policy_digest,
        executable_digest,
    })
}

fn digest_executable(path: &Path) -> Result<ContentDigest> {
    use rustix::fs::{FileType, Mode, OFlags, fstat, open, openat};
    use sha2::{Digest, Sha256};
    use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Component};
    validate_absolute_path(path)?;
    let mut directory = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let parts: Vec<_> = path
        .components()
        .filter_map(|part| match part {
            Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect();
    let (filename, parents) = parts
        .split_last()
        .context("native artifact path is empty")?;
    for parent in parents {
        directory = openat(
            &directory,
            *parent,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
    }
    let fd = openat(
        &directory,
        *filename,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let before = fstat(&fd)?;
    ensure!(
        FileType::from_raw_mode(before.st_mode) == FileType::RegularFile
            && (before.st_uid == 0 || before.st_uid == rustix::process::geteuid().as_raw())
            && before.st_mode & 0o022 == 0
            && before.st_mode & 0o111 != 0
            && before.st_size > 0
            && before.st_size <= 512 * 1024 * 1024,
        "native artifact must be an owned, non-shared-writable regular executable"
    );
    let mut file = File::from(fd);
    let metadata_before = file.metadata()?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        ensure!(
            total <= 512 * 1024 * 1024,
            "native artifact changed or exceeds limit"
        );
        hash.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    ensure!(
        metadata_before.len() == after.len()
            && metadata_before.mtime() == after.mtime()
            && metadata_before.mtime_nsec() == after.mtime_nsec()
            && metadata_before.ctime() == after.ctime()
            && metadata_before.ctime_nsec() == after.ctime_nsec()
            && total == u64::try_from(before.st_size)?,
        "native artifact changed while hashing"
    );
    // Strict ContentDigest parsing validates the lowercase SHA256 representation.
    serde_json::from_value(serde_json::Value::String(format!("{:x}", hash.finalize())))
        .context("invalid executable digest")
}

impl crate::store::Store {
    /// Register this authenticated home-local owner's actual managed policy identity.
    /// New or changed generations are disabled and have no inherited capability witness.
    pub async fn register_managed_target(
        &self,
        actor: &crate::store::Mailbox,
        spec: &ManagedTargetSpec,
        expected_generation: Option<i64>,
        now: i64,
    ) -> Result<ManagedTargetRegistration> {
        spec.validate()?;
        ensure!(
            spec.owner == actor.name
                && !matches!(actor.binding, crate::identity::Binding::Remote { .. }),
            "managed registration requires the target owner on its home machine"
        );
        let policy = resolve_managed_policy(self.root(), spec)?;
        let mut tx = self.pool().begin().await?;
        let registration =
            register_managed_target_tx(&mut tx, actor, spec, expected_generation, &policy, now)
                .await?;
        crate::runtime_lifecycle::record_legacy_registration_tx(
            &mut tx,
            actor,
            spec,
            expected_generation,
            &policy,
            &registration,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(registration)
    }

    /// Append evidence for the original authenticated attempt. Yield is a report category;
    /// this operation does not close an attempt, release a slot, or schedule a successor.
    pub async fn record_managed_report(
        &self,
        actor: &crate::store::Mailbox,
        report: &crate::execution::ExecutionReport,
        now: i64,
    ) -> Result<crate::execution::Checked<i64>> {
        let mut tx = self.pool().begin().await?;
        // The scheduler authenticates historical exact replay before fresh positive guards.
        // Do not add an outer preflight that would prevent that historical read.
        let result =
            crate::execution::record_report_tx(&mut tx, &ManagedRuntimeGate, actor, report, now)
                .await?;
        tx.commit().await?; // Held carries durable scheduler cause and clock observations.
        Ok(result)
    }
}

/// Shared registration core. The caller owns the writer transaction and audit commit.
pub(crate) async fn register_managed_target_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    actor: &crate::store::Mailbox,
    spec: &ManagedTargetSpec,
    expected_generation: Option<i64>,
    policy: &ResolvedManagedPolicy,
    now: i64,
) -> Result<ManagedTargetRegistration> {
    spec.validate()?;
    ensure!(
        spec.owner == actor.name,
        "managed registration requires target owner"
    );
    let canonical = serde_json::to_string(spec)?;
    crate::runtime_lifecycle::authenticate_owner_tx(tx, actor).await?;
    let home: bool =
        sqlx::query_scalar("SELECT home_machine=(SELECT id FROM node) FROM groups WHERE name=?")
            .bind(&actor.group_name)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(home, "managed registration requires a home group");
    #[derive(sqlx::FromRow)]
    struct Existing {
        id: String,
        generation: i64,
        enabled: bool,
        owner: String,
        owner_binding: i64,
        specification: String,
        policy_digest: String,
        executable_digest: String,
    }
    let existing:Option<Existing>=sqlx::query_as("SELECT t.id,t.current_generation AS generation,t.enabled,v.owner,v.owner_binding,v.specification,v.policy_digest,v.executable_digest FROM runtime_targets t JOIN runtime_target_versions v ON v.target=t.id AND v.generation=t.current_generation WHERE t.group_name=? AND t.name=?")
            .bind(&actor.group_name).bind(&spec.target).fetch_optional(&mut **tx).await?;
    let (identity, generation) = if let Some(old) = existing {
        ensure!(
            old.owner == actor.name,
            "managed target belongs to another owner"
        );
        if old.owner_binding == actor.binding_version
            && old.specification == canonical
            && old.policy_digest == policy.policy_digest.as_str()
            && old.executable_digest == policy.executable_digest.as_str()
        {
            ensure!(
                expected_generation == Some(old.generation)
                    || expected_generation.and_then(|version| version.checked_add(1))
                        == Some(old.generation)
                    || (expected_generation.is_none() && old.generation == 1),
                "managed target generation conflict"
            );
            return Ok(ManagedTargetRegistration {
                identity: old.id,
                generation: old.generation,
                enabled: old.enabled,
            });
        }
        let retired: bool =
            sqlx::query_scalar("SELECT retired FROM runtime_target_lifecycle WHERE target=?")
                .bind(&old.id)
                .fetch_one(&mut **tx)
                .await?;
        ensure!(!retired, "managed_target_retired");
        // An admitted identity remains owned by its original cleanup path. A reservation
        // can exist before runtime_segments, so inspect the scheduler's original target.
        let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE group_name=? AND holds_slot=1 AND json_extract(runtime,'$.identity')=?)")
                .bind(&actor.group_name).bind(&old.id).fetch_one(&mut **tx).await?;
        ensure!(!held, "managed_target_has_held_attempts");
        ensure!(
            expected_generation == Some(old.generation),
            "changed target requires its current generation"
        );
        let next = old
            .generation
            .checked_add(1)
            .context("managed target generation overflow")?;
        sqlx::query("UPDATE runtime_targets SET current_generation=?,enabled=0 WHERE id=? AND current_generation=?")
                .bind(next).bind(&old.id).bind(old.generation).execute(&mut **tx).await?;
        (old.id, next)
    } else {
        ensure!(
            expected_generation.is_none(),
            "managed target does not exist"
        );
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO runtime_targets(id,group_name,name,current_generation,enabled) VALUES(?,?,?,1,0)")
                .bind(&id).bind(&actor.group_name).bind(&spec.target).execute(&mut **tx).await?;
        (id, 1)
    };
    let client = match spec.client {
        NativeClient::Codex => "codex",
        NativeClient::Claude => "claude",
    };
    let profile = match spec.profile {
        RuntimeProfile::ReadOnly => "read_only",
        RuntimeProfile::StagedFiles => "staged_files",
    };
    sqlx::query("INSERT INTO runtime_target_versions(target,generation,owner,owner_binding,client,profile,concurrency_key,specification,policy_digest,executable_digest,created) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&identity).bind(generation).bind(&actor.name).bind(actor.binding_version).bind(client).bind(profile)
            .bind(format!("managed:{identity}")).bind(canonical).bind(policy.policy_digest.as_str()).bind(policy.executable_digest.as_str()).bind(now)
            .execute(&mut **tx).await?;
    Ok(ManagedTargetRegistration {
        identity,
        generation,
        enabled: false,
    })
}

/// Initial bounded effect profile; arbitrary shell/network/export is a separate capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    /// Immutable inputs and structured output, with known private infrastructure writes.
    ReadOnly,
    /// Private staged files selected only through the controlled-artifact publisher.
    StagedFiles,
}

/// Explicit registration inputs. Credentials and caller-supplied safety evidence are excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTargetSpec {
    /// Stable target identifier, independent of Mail owner identity.
    pub target: String,
    /// Explicit existing home-local owner in this group.
    pub owner: String,
    /// Native client family.
    pub client: NativeClient,
    /// Approved absolute input/staging root. Registration does not create or copy it.
    pub cwd: PathBuf,
    /// Requested effect restriction.
    pub profile: RuntimeProfile,
    /// Protected artifact storage for staged files; not an arbitrary mutable export root.
    pub artifact_root: Option<PathBuf>,
    /// Opaque reference to operator-approved effective configuration; never inline secrets.
    pub configuration: String,
}

impl ManagedTargetSpec {
    /// Validate request shape only. Existence, ownership and effective policy are runtime checks.
    pub fn validate(&self) -> Result<()> {
        name(&self.target)?;
        name(&self.owner)?;
        validate_absolute_path(&self.cwd)?;
        bounded(&self.configuration, 256, "runtime configuration reference")?;
        ensure!(
            !self.configuration.trim().is_empty()
                && !self.configuration.chars().any(char::is_control),
            "runtime configuration reference is required"
        );
        match (self.profile, &self.artifact_root) {
            (RuntimeProfile::ReadOnly, None) => {}
            (RuntimeProfile::StagedFiles, Some(root)) => {
                validate_absolute_path(root)?;
                ensure!(
                    !root.starts_with(&self.cwd) && !self.cwd.starts_with(root),
                    "artifact storage and child workspace must be disjoint"
                );
            }
            _ => anyhow::bail!(
                "staged files require an artifact root; read-only does not accept one"
            ),
        }
        Ok(())
    }
}

fn validate_absolute_path(path: &Path) -> Result<()> {
    let text = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("runtime paths must be UTF-8"))?;
    ensure!(
        path.is_absolute() && text.len() <= 4096 && !text.chars().any(char::is_control),
        "invalid absolute runtime path"
    );
    ensure!(
        !path.components().any(|part| matches!(
            part,
            std::path::Component::ParentDir | std::path::Component::CurDir
        )),
        "runtime paths must not contain traversal components"
    );
    Ok(())
}

/// Capabilities are independent; delivery readiness never implies execution safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCapability {
    /// Stable attempt admission accepted only through current scheduler guards.
    AdmissionFence,
    /// Identical dispatch retries cannot create a second physical execution.
    DurableDedupe,
    /// Native process and all descendants can be proven quiescent.
    Quiescence,
    /// A durable tombstone prevents future launch of a closed dispatch key.
    LateStartRejection,
    /// The complete profile effect set can be reconciled after failure.
    EffectReconciliation,
    /// Tools cannot escape the approved filesystem/input/staging roots.
    FilesystemContainment,
    /// Allowed inference remains usable while prohibited model-tool network effects fail.
    ToolNetworkRestriction,
    /// Actual resolved client telemetry and network behavior are witnessed.
    TelemetryDisabled,
    /// A successor can use durable input/report context after predecessor closure.
    ContinuationReplay,
    /// Optional genuine same-session recovery, independent of semantic continuation.
    NativeSessionResume,
    /// Actual cost accounting in an explicit unit.
    CostMetering,
    /// A required hard cap is enforced before unreserved external spending.
    CostCap,
}

/// Explicit knowledge state for one capability, without an optimistic default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CapabilityState {
    /// Exact current-generation witness exists; this is still not attempt authority.
    Verified {
        /// Immutable authenticated evidence record resolved by the runtime owner.
        evidence: String,
    },
    /// No actual evidence establishes the capability for this configuration.
    Unverified {
        /// Required concrete prerequisite or witness.
        reason: String,
    },
    /// This runtime/platform/profile cannot provide the capability.
    Unsupported {
        /// Concrete unsupported boundary.
        reason: String,
    },
    /// Evidence belongs to a superseded target/configuration generation.
    Stale {
        /// Retained historical evidence, never silently erased or reused as current.
        evidence: String,
    },
}

/// An individual read-only capability projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityObservation {
    /// Dimension observed.
    pub capability: RuntimeCapability,
    /// Honest evidence disposition.
    pub state: CapabilityState,
}

/// Protected configuration identity observed by a read-only inspection.
#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ManagedConfigurationState {
    /// Current policy, launcher and executable bytes match the registered generation.
    Current,
    /// Current bytes differ from the immutable registered identity.
    Changed,
    /// Current identity could not be established without launching or repairing anything.
    Unavailable {
        /// Bounded diagnostic that contains no policy contents or credentials.
        reason: String,
    },
}

/// One capability with its actual witness interval, when evidence exists.
#[derive(Debug, Serialize)]
pub struct ManagedCapabilityView {
    /// Independent capability dimension.
    pub capability: RuntimeCapability,
    /// Current, stale, absent, or unsupported evidence state.
    pub state: CapabilityState,
    /// Original witness generation, never rewritten to the inspected generation.
    pub witness_generation: Option<i64>,
    /// Actual witness time; inspection does not create one.
    pub witnessed_at: Option<i64>,
    /// Actual finite evidence expiry; absence is not an invented interval.
    pub valid_until: Option<i64>,
}

/// Read-only runtime facts. Even a qualified profile is not task admission authority.
#[derive(Debug, Serialize)]
pub struct ManagedTargetView {
    /// Projection format version.
    pub schema_version: u32,
    /// Stable target ID returned by registration and accepted by inspection.
    pub identity: String,
    /// Group-scoped display name, separate from the owner identity.
    pub name: String,
    /// Current immutable configuration generation.
    pub generation: i64,
    /// Current lifecycle projection revision.
    pub revision: i64,
    /// Permanent archive state; historical inspection remains available.
    pub retired: bool,
    /// Original configured Mail owner.
    pub owner: String,
    /// Owner binding captured by this generation.
    pub owner_binding: i64,
    /// Registered native client family.
    pub client: NativeClient,
    /// Registered effect profile.
    pub profile: RuntimeProfile,
    /// Nonsecret named configuration reference; policy contents are not returned.
    pub configuration: String,
    /// Registered policy and launcher digest.
    pub policy_digest: ContentDigest,
    /// Registered native executable digest.
    pub executable_digest: ContentDigest,
    /// Inspection time, distinct from each witness time.
    pub observed_at: i64,
    /// Result of checking current nonsecret policy and artifact identity.
    pub configuration_state: ManagedConfigurationState,
    /// Stored admission switch; this alone is not qualification.
    pub enabled: bool,
    /// Actual target gate and configuration matched at inspection, not task permission.
    pub profile_qualified: bool,
    /// Explicit runtime prerequisites that remain unsatisfied.
    pub holds: Vec<String>,
    /// All twelve independent capability dimensions in stable order.
    pub capabilities: Vec<ManagedCapabilityView>,
}

const CAPABILITY_KEYS: &[(RuntimeCapability, &str)] = &[
    (RuntimeCapability::AdmissionFence, "admission_fence"),
    (RuntimeCapability::DurableDedupe, "durable_dedupe"),
    (RuntimeCapability::Quiescence, "quiescence"),
    (
        RuntimeCapability::LateStartRejection,
        "late_start_rejection",
    ),
    (
        RuntimeCapability::EffectReconciliation,
        "effect_reconciliation",
    ),
    (
        RuntimeCapability::FilesystemContainment,
        "filesystem_containment",
    ),
    (
        RuntimeCapability::ToolNetworkRestriction,
        "tool_network_restriction",
    ),
    (RuntimeCapability::TelemetryDisabled, "telemetry_disabled"),
    (RuntimeCapability::ContinuationReplay, "continuation_replay"),
    (
        RuntimeCapability::NativeSessionResume,
        "native_session_resume",
    ),
    (RuntimeCapability::CostMetering, "cost_metering"),
    (RuntimeCapability::CostCap, "cost_cap"),
];

#[derive(sqlx::FromRow)]
struct CapabilityWitness {
    id: String,
    generation: i64,
    observed: i64,
    valid_until: i64,
    evidence: String,
}

impl crate::store::Store {
    /// Inspect one exact registration ID in this authenticated home group. Missing targets
    /// return None; corrupt protected facts remain errors. No probe, write, or repair occurs.
    pub async fn managed_target_capabilities(
        &self,
        actor: &crate::store::Mailbox,
        target: &str,
        now: i64,
    ) -> Result<Option<ManagedTargetView>> {
        use crate::execution::RuntimeGate;
        ensure!(now >= 0, "invalid runtime inspection time");
        bounded(target, 128, "managed target identity")?;
        ensure!(
            !target.is_empty() && !target.chars().any(char::is_control),
            "invalid managed target identity"
        );
        let mut tx = self.pool().begin().await?;
        Self::check_actor(&mut tx, actor).await?;
        let home_actor: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mailboxes m JOIN groups g ON g.name=m.group_name WHERE m.id=? AND m.name=? AND m.group_name=? AND g.home_machine=(SELECT id FROM node))")
            .bind(actor.id).bind(&actor.name).bind(&actor.group_name).fetch_one(&mut *tx).await?;
        ensure!(
            home_actor,
            "managed inspection requires an authenticated home group"
        );
        let header: Option<(String, i64, bool, i64, bool)> = sqlx::query_as("SELECT t.name,t.current_generation,t.enabled,l.revision,l.retired FROM runtime_targets t JOIN runtime_target_lifecycle l ON l.target=t.id WHERE t.id=? AND t.group_name=?")
            .bind(target).bind(&actor.group_name).fetch_optional(&mut *tx).await?;
        let Some((target_name, generation, enabled, revision, retired)) = header else {
            tx.commit().await?;
            return Ok(None);
        };
        #[derive(sqlx::FromRow)]
        struct Version {
            owner: String,
            owner_binding: i64,
            client: String,
            profile: String,
            specification: String,
            policy_digest: String,
            executable_digest: String,
        }
        // A target without its referenced version is corruption, not an absent target.
        let version: Version = sqlx::query_as("SELECT owner,owner_binding,client,profile,specification,policy_digest,executable_digest FROM runtime_target_versions WHERE target=? AND generation=?")
            .bind(target).bind(generation).fetch_one(&mut *tx).await?;
        bounded(
            &version.specification,
            16384,
            "stored managed specification",
        )?;
        let spec: ManagedTargetSpec = serde_json::from_str(&version.specification)?;
        spec.validate()?;
        ensure!(
            generation > 0
                && version.owner_binding > 0
                && spec.target == target_name
                && spec.owner == version.owner
                && serde_json::to_value(spec.client)?.as_str() == Some(version.client.as_str())
                && serde_json::to_value(spec.profile)?.as_str() == Some(version.profile.as_str()),
            "inconsistent protected managed target identity"
        );
        let policy_digest = ContentDigest::parse(version.policy_digest)?;
        let executable_digest = ContentDigest::parse(version.executable_digest)?;
        let configuration_state = match resolve_managed_policy(self.root(), &spec) {
            Ok(policy)
                if policy.policy_digest == policy_digest
                    && policy.executable_digest == executable_digest =>
            {
                ManagedConfigurationState::Current
            }
            Ok(_) => ManagedConfigurationState::Changed,
            Err(_) => ManagedConfigurationState::Unavailable {
                reason: "managed policy or executable identity could not be verified".into(),
            },
        };
        let binding_current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mailboxes WHERE group_name=? AND name=? AND binding_version=? AND agent_state='registered' AND remote_machine IS NULL)")
            .bind(&actor.group_name).bind(&version.owner).bind(version.owner_binding).fetch_one(&mut *tx).await?;
        let configuration_current =
            matches!(configuration_state, ManagedConfigurationState::Current);
        let supported_host = cfg!(target_os = "linux");
        let supported_profile = spec.profile == RuntimeProfile::ReadOnly;
        let mut holds = Vec::new();
        if retired {
            holds.push("managed_target_retired".into());
        }
        if !enabled {
            holds.push("managed_target_disabled".into());
        }
        if !binding_current {
            holds.push("managed_owner_binding_changed".into());
        }
        if !supported_host {
            holds.push("managed_runtime_unsupported_host".into());
        }
        if !supported_profile {
            holds.push("managed_native_qualification_unavailable".into());
        }
        match &configuration_state {
            ManagedConfigurationState::Current => {}
            ManagedConfigurationState::Changed => {
                holds.push("managed_configuration_changed".into())
            }
            ManagedConfigurationState::Unavailable { .. } => {
                holds.push("managed_configuration_unavailable".into())
            }
        }
        let mut capabilities = Vec::with_capacity(CAPABILITY_KEYS.len());
        for &(capability, key) in CAPABILITY_KEYS {
            let fresh: Option<CapabilityWitness> = sqlx::query_as("SELECT id,generation,observed,valid_until,evidence FROM runtime_capability_witnesses WHERE target=? AND generation=? AND capability=? AND observed<=? AND valid_until>? ORDER BY observed DESC,id DESC LIMIT 1")
                .bind(target).bind(generation).bind(key).bind(now).bind(now).fetch_optional(&mut *tx).await?;
            let witness = match fresh {
                Some(witness) => Some(witness),
                None => sqlx::query_as("SELECT id,generation,observed,valid_until,evidence FROM runtime_capability_witnesses WHERE target=? AND generation<=? AND capability=? AND observed<=? ORDER BY generation DESC,observed DESC,id DESC LIMIT 1")
                    .bind(target).bind(generation).bind(key).bind(now).fetch_optional(&mut *tx).await?,
            };
            let (state, witness_generation, witnessed_at, valid_until) = if let Some(witness) =
                witness
            {
                bounded(&witness.id, 256, "runtime witness reference")?;
                bounded(&witness.evidence, 65536, "runtime witness evidence")?;
                ensure!(
                    !witness.id.is_empty()
                        && !witness.id.chars().any(char::is_control)
                        && witness.generation > 0
                        && witness.observed >= 0
                        && witness.valid_until > witness.observed,
                    "invalid protected runtime witness"
                );
                let _: serde_json::Value = serde_json::from_str(&witness.evidence)?;
                let current = witness.generation == generation
                    && witness.valid_until > now
                    && configuration_current
                    && binding_current
                    && supported_host
                    && supported_profile
                    && !matches!(
                        capability,
                        RuntimeCapability::NativeSessionResume
                            | RuntimeCapability::CostMetering
                            | RuntimeCapability::CostCap
                    );
                let state = if current {
                    CapabilityState::Verified {
                        evidence: witness.id,
                    }
                } else {
                    CapabilityState::Stale {
                        evidence: witness.id,
                    }
                };
                (
                    state,
                    Some(witness.generation),
                    Some(witness.observed),
                    Some(witness.valid_until),
                )
            } else {
                let state = match capability {
                    RuntimeCapability::NativeSessionResume => CapabilityState::Unsupported { reason: "native same-session recovery is not implemented by this managed adapter".into() },
                    RuntimeCapability::CostMetering | RuntimeCapability::CostCap => CapabilityState::Unsupported { reason: "native cost metering and enforced external spending caps are not implemented".into() },
                    _ if !supported_host => CapabilityState::Unsupported { reason: "managed execution requires a qualified Linux host".into() },
                    _ if !supported_profile => CapabilityState::Unverified { reason: "the staged native qualification collector is unavailable".into() },
                    _ => CapabilityState::Unverified { reason: "no usable runtime-owned witness for this capability".into() },
                };
                (state, None, None, None)
            };
            capabilities.push(ManagedCapabilityView {
                capability,
                state,
                witness_generation,
                witnessed_at,
                valid_until,
            });
        }
        let resolved = ManagedRuntimeGate
            .target(
                &mut tx,
                &actor.group_name,
                "",
                &version.owner,
                version.owner_binding,
                now,
            )
            .await?;
        let profile_qualified = !retired
            && supported_host
            && supported_profile
            && configuration_current
            && binding_current
            && resolved.as_ref().is_some_and(|resolved| {
                resolved.identity == target && resolved.generation == generation
            });
        if !profile_qualified {
            holds.push("managed_profile_unqualified".into());
        }
        tx.commit().await?;
        Ok(Some(ManagedTargetView {
            schema_version: 2,
            identity: target.into(),
            name: target_name,
            generation,
            revision,
            retired,
            owner: version.owner,
            owner_binding: version.owner_binding,
            client: spec.client,
            profile: spec.profile,
            configuration: spec.configuration,
            policy_digest,
            executable_digest,
            observed_at: now,
            configuration_state,
            enabled,
            profile_qualified,
            holds,
            capabilities,
        }))
    }
}

/// Runtime-owned projection of the exact configuration under observation.
/// This deserializable DTO must never be accepted as a positive admission guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCapabilities {
    /// Current target version, read from authoritative runtime rows.
    pub target_version: i64,
    /// Approved effective policy identity.
    pub policy_digest: ContentDigest,
    /// Exact observed native executable identity, not only a mutable version label.
    pub executable_digest: ContentDigest,
    /// Observation timestamp; a future timestamp is not fresh evidence.
    pub observed_at: i64,
    /// Finite validity bound. Configuration replacement invalidates evidence immediately.
    pub valid_until: i64,
    /// One observation per capability; missing dimensions remain unverified.
    pub observations: Vec<CapabilityObservation>,
}

impl RuntimeCapabilities {
    /// Validate presentation data. The trusted owner must still authenticate evidence references.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.target_version > 0 && self.observed_at >= 0 && self.valid_until > self.observed_at,
            "invalid capability observation identity or interval"
        );
        ensure!(
            self.observations.len() <= 32,
            "too many capability observations"
        );
        let mut seen = BTreeSet::new();
        for observation in &self.observations {
            ensure!(
                seen.insert(observation.capability),
                "duplicate capability observation"
            );
            let (value, limit) = match &observation.state {
                CapabilityState::Verified { evidence } | CapabilityState::Stale { evidence } => {
                    (evidence, 256)
                }
                CapabilityState::Unverified { reason }
                | CapabilityState::Unsupported { reason } => (reason, 1024),
            };
            ensure!(
                !value.trim().is_empty() && !value.chars().any(char::is_control),
                "capability evidence or reason is required"
            );
            bounded(value, limit, "capability observation")?;
        }
        Ok(())
    }

    /// Whether this projection's time and configuration identity still match.
    /// A true result conveys freshness only, never lifecycle or business permission.
    pub fn matches_current(
        &self,
        target_version: i64,
        policy: &ContentDigest,
        executable: &ContentDigest,
        now: i64,
    ) -> bool {
        self.validate().is_ok()
            && self.target_version == target_version
            && &self.policy_digest == policy
            && &self.executable_digest == executable
            && self.observed_at <= now
            && now < self.valid_until
    }
}

/// Reads authenticated physical facts from runtime-owned rows in the caller's transaction.
/// This private implementation cannot be supplied over a CLI or deserialized as permission.
pub(crate) struct ManagedRuntimeGate;

const REQUIRED_CAPABILITIES: &[&str] = &[
    "admission_fence",
    "durable_dedupe",
    "quiescence",
    "late_start_rejection",
    "effect_reconciliation",
    "filesystem_containment",
    "tool_network_restriction",
    "telemetry_disabled",
    "continuation_replay",
];

#[derive(sqlx::FromRow)]
struct TargetRow {
    id: String,
    generation: i64,
    concurrency_key: String,
    profile: String,
}

impl crate::execution::RuntimeGate for ManagedRuntimeGate {
    async fn target(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        group: &str,
        _task: &str,
        owner: &str,
        binding: i64,
        now: i64,
    ) -> Result<Option<crate::execution::RuntimeTarget>> {
        let targets: Vec<TargetRow> = sqlx::query_as("SELECT t.id,v.generation,v.concurrency_key,v.profile FROM runtime_targets t JOIN runtime_target_versions v ON v.target=t.id AND v.generation=t.current_generation JOIN mailboxes m ON m.group_name=t.group_name AND m.name=v.owner AND m.binding_version=v.owner_binding WHERE t.group_name=? AND t.enabled=1 AND v.owner=? AND v.owner_binding=? ORDER BY t.id LIMIT 2")
            .bind(group).bind(owner).bind(binding).fetch_all(&mut **tx).await?;
        // Ambiguous owner configuration requires an explicit operator correction.
        if targets.len() != 1 {
            return Ok(None);
        }
        let row = &targets[0];
        // Schema27 has no protected native qualification collector. Legacy witness
        // rows cannot qualify the new staged profile.
        if row.profile == "staged_files" {
            return Ok(None);
        }
        let available: Vec<String> = sqlx::query_scalar("SELECT DISTINCT capability FROM runtime_capability_witnesses WHERE target=? AND generation=? AND observed<=? AND valid_until>?")
            .bind(&row.id).bind(row.generation).bind(now).bind(now).fetch_all(&mut **tx).await?;
        if !REQUIRED_CAPABILITIES
            .iter()
            .all(|required| available.iter().any(|item| item == required))
        {
            return Ok(None);
        }
        // Native API spending caps remain unsupported until an actual enforced-unit witness exists.
        // Empty caps let scheduler admit only contracts without a required external cost ceiling.
        Ok(Some(crate::execution::RuntimeTarget {
            identity: row.id.clone(),
            concurrency_key: row.concurrency_key.clone(),
            generation: row.generation,
            profile: row.profile.clone(),
            durable_dedupe: true,
            cost_caps: Default::default(),
        }))
    }

    async fn current(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        c: &crate::execution::Correlation,
        target: &crate::execution::RuntimeTarget,
        purpose: crate::execution::CurrentUse,
        now: i64,
    ) -> Result<bool> {
        use crate::execution::CurrentUse;
        let identity: Option<(String,i64,String)> = sqlx::query_as("SELECT owner,owner_binding,runtime FROM execution_attempts WHERE id=? AND group_name=? AND task=? AND fence=? AND dispatch_key=?")
            .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
            .fetch_optional(&mut **tx).await?;
        let Some((owner, binding, original_target)) = identity else {
            return Ok(false);
        };
        let original: crate::execution::RuntimeTarget = serde_json::from_str(&original_target)?;
        if &original != target
            || self
                .target(tx, &c.group, &c.task, &owner, binding, now)
                .await?
                .as_ref()
                != Some(target)
        {
            return Ok(false);
        }
        let segment: Option<(String,i64,bool,bool,String,bool)> = sqlx::query_as("SELECT target,target_generation,tombstoned,launch_committed,state,containment IS NOT NULL FROM runtime_segments WHERE attempt=? AND group_name=? AND task=? AND fence=? AND dispatch_key=?")
            .bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
            .fetch_optional(&mut **tx).await?;
        let Some((identity, generation, tombstoned, launched, state, contained)) = segment else {
            return Ok(matches!(purpose, CurrentUse::Dispatch));
        };
        if identity != target.identity || generation != target.generation || tombstoned {
            return Ok(false);
        }
        Ok(match purpose {
            CurrentUse::Dispatch => !launched && matches!(state.as_str(), "prepared" | "starting"),
            CurrentUse::Admit => !launched && contained && state == "starting",
            CurrentUse::Report | CurrentUse::Publish => launched && contained && state == "running",
        })
    }

    async fn closed(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        c: &crate::execution::Correlation,
        target: &crate::execution::RuntimeTarget,
        receipt: &str,
    ) -> Result<Option<crate::execution::ClosedRuntime>> {
        let closed: Option<(String,String,bool,String)> = sqlx::query_as("SELECT r.id,r.effect_set,a.admitted,r.costs FROM runtime_closures r JOIN runtime_segments s ON s.attempt=r.attempt JOIN execution_attempts a ON a.id=s.attempt WHERE r.id=? AND s.attempt=? AND s.group_name=? AND s.task=? AND s.fence=? AND s.dispatch_key=? AND s.target=? AND s.target_generation=? AND s.tombstoned=1 AND s.state='quiescent'")
            .bind(receipt).bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
            .bind(&target.identity).bind(target.generation).fetch_optional(&mut **tx).await?;
        let Some((receipt, effect_set, admitted, costs)) = closed else {
            return Ok(None);
        };
        if !crate::runtime_effects::effect_set_reconciled_tx(tx, &c.attempt, &effect_set).await? {
            return Ok(None);
        }
        crate::runtime_capture::validate_closure_tx(tx, c, &receipt).await?;
        let costs: std::collections::BTreeMap<String, i64> = serde_json::from_str(&costs)?;
        ensure!(
            costs.len() <= 32
                && costs
                    .iter()
                    .all(|(unit, value)| !unit.is_empty() && unit.len() <= 64 && *value >= 0),
            "invalid persisted closure cost evidence"
        );
        Ok(Some(crate::execution::ClosedRuntime {
            receipt,
            effect_set,
            admitted,
            costs,
        }))
    }

    async fn observation(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        c: &crate::execution::Correlation,
        target: &crate::execution::RuntimeTarget,
        receipt: &str,
    ) -> Result<Option<crate::execution::RuntimeObservation>> {
        let observation: Option<(String,i64,i64,String)> = sqlx::query_as("SELECT o.id,o.sequence,o.observed,o.status FROM runtime_observations o JOIN runtime_segments s ON s.attempt=o.attempt WHERE o.id=? AND s.attempt=? AND s.group_name=? AND s.task=? AND s.fence=? AND s.dispatch_key=? AND s.target=? AND s.target_generation=?")
            .bind(receipt).bind(&c.attempt).bind(&c.group).bind(&c.task).bind(c.fence).bind(&c.dispatch_key)
            .bind(&target.identity).bind(target.generation).fetch_optional(&mut **tx).await?;
        observation
            .map(|(receipt, sequence, observed_at, status)| {
                let status = match status.as_str() {
                    "active" => crate::execution::ObservationStatus::Active,
                    "unknown" => crate::execution::ObservationStatus::Unknown,
                    "exit_observed" => crate::execution::ObservationStatus::ExitObserved,
                    _ => anyhow::bail!("invalid persisted runtime observation"),
                };
                Ok(crate::execution::RuntimeObservation {
                    receipt,
                    sequence,
                    observed_at,
                    status,
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    fn fixture() -> (tempfile::TempDir, ManagedTargetSpec) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let policies = root.join("managed-policies");
        fs::create_dir(&policies).unwrap();
        fs::set_permissions(&policies, fs::Permissions::from_mode(0o700)).unwrap();
        // Executable-shaped files are hashed only; no subprocess/native witness is run.
        for name in ["native", "launcher"] {
            let path = root.join(name);
            fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let policy = serde_json::json!({"format":1,"client":"codex","profile":"read_only",
            "client_version":"fixture-only","executable":root.join("native"),
            "launcher":root.join("launcher"),"delegated_cgroup":root.join("cgroup"),
            "effective_configuration":{"fixture":true}});
        let policy_path = policies.join("fixture.json");
        fs::write(&policy_path, serde_json::to_vec(&policy).unwrap()).unwrap();
        fs::set_permissions(policy_path, fs::Permissions::from_mode(0o600)).unwrap();
        let spec = ManagedTargetSpec {
            target: "fixture".into(),
            owner: "worker".into(),
            client: NativeClient::Codex,
            cwd: root.join("input"),
            profile: RuntimeProfile::ReadOnly,
            artifact_root: None,
            configuration: "fixture".into(),
        };
        (directory, spec)
    }

    #[test]
    fn changed_client_or_launcher_bytes_invalidate_registered_identity() {
        let (directory, spec) = fixture();
        let root = directory.path().canonicalize().unwrap();
        let first = resolve_managed_policy(&root, &spec).unwrap();
        fs::write(root.join("native"), b"#!/bin/sh\nexit 1\n").unwrap();
        let second = resolve_managed_policy(&root, &spec).unwrap();
        assert_ne!(first.executable_digest, second.executable_digest);
        assert_eq!(first.policy_digest, second.policy_digest);
        fs::write(root.join("launcher"), b"#!/bin/sh\nexit 2\n").unwrap();
        let third = resolve_managed_policy(&root, &spec).unwrap();
        assert_ne!(second.policy_digest, third.policy_digest);
    }

    #[test]
    fn registration_rejects_shared_policy_and_symlinked_artifacts() {
        let (directory, spec) = fixture();
        let root = directory.path().canonicalize().unwrap();
        let policy = root.join("managed-policies/fixture.json");
        fs::set_permissions(&policy, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(resolve_managed_policy(&root, &spec).is_err());
        fs::set_permissions(policy, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(root.join("native"), root.join("actual-native")).unwrap();
        std::os::unix::fs::symlink(root.join("actual-native"), root.join("native")).unwrap();
        assert!(resolve_managed_policy(&root, &spec).is_err());
    }

    async fn registered_fixture() -> Result<(
        tempfile::TempDir,
        crate::store::Store,
        crate::store::Mailbox,
        ManagedTargetSpec,
        ManagedTargetRegistration,
    )> {
        let (directory, spec) = fixture();
        let store = crate::store::Store::open(&directory.path().canonicalize()?, true).await?;
        store.enroll("g", None).await?;
        let credential = store.register("g", "worker", false).await?;
        let actor = store.authenticate("g", Some(&credential)).await?;
        let registration = store
            .register_managed_target(&actor, &spec, None, 100)
            .await?;
        Ok((directory, store, actor, spec, registration))
    }

    #[tokio::test]
    async fn keyed_lifecycle_replay_preserves_original_receipt_and_current_archive() -> Result<()> {
        use crate::{
            execution::Checked,
            runtime_lifecycle::{
                ManagedTargetAction, ManagedTargetChange, ManagedTargetConfiguration,
            },
        };
        let (directory, store, actor, spec, registration) = registered_fixture().await?;
        let configure = ManagedTargetConfiguration {
            key: "configured".into(),
            reason: "Retain exact configuration".into(),
            spec: spec.clone(),
            expected_generation: Some(1),
            expected_revision: Some(1),
        };
        let first = store
            .configure_managed_target(&actor, &configure, 101)
            .await?;
        assert_eq!(first.revision, 1);
        let retire = ManagedTargetChange {
            key: "retire".into(),
            reason: "Archive unused target".into(),
            target: registration.identity.clone(),
            expected_generation: 1,
            expected_revision: 1,
            action: ManagedTargetAction::Retire,
        };
        let Checked::Ready(retired) = store.change_managed_target(&actor, &retire, 102).await?
        else {
            anyhow::bail!("unused retirement held");
        };
        assert!(retired.retired && !retired.enabled);
        assert_eq!(retired.revision, 2);
        let legacy = store
            .register_managed_target(&actor, &spec, None, 103)
            .await?;
        assert!(!legacy.enabled && legacy.generation == 1);
        fs::remove_file(directory.path().join("native"))?;
        let replay = store
            .configure_managed_target(&actor, &configure, 104)
            .await?;
        assert_eq!(
            serde_json::to_value(&first)?,
            serde_json::to_value(&replay)?
        );
        let Checked::Ready(replayed_retire) =
            store.change_managed_target(&actor, &retire, 105).await?
        else {
            anyhow::bail!("historical retirement held");
        };
        assert_eq!(
            serde_json::to_value(&retired)?,
            serde_json::to_value(&replayed_retire)?
        );
        let view = store
            .managed_target_capabilities(&actor, &registration.identity, 106)
            .await?
            .unwrap();
        assert_eq!(view.schema_version, 2);
        assert!(view.retired && !view.enabled && !view.profile_qualified);
        assert_eq!(view.revision, 2);
        let mut conflict = retire.clone();
        conflict.reason = "Changed request".into();
        assert!(
            store
                .change_managed_target(&actor, &conflict, 107)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn binding_uses_real_writer_and_retains_revision_and_destination_ownership() -> Result<()>
    {
        use crate::{
            runtime_lifecycle::ManagedArtifactBindingRequest,
            task_graph::{
                AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract,
                Criterion, TaskCreate, TaskDraft,
            },
        };
        let (directory, store, owner, mut spec, _registration) = registered_fixture().await?;
        let path = directory.path().join("managed-policies/fixture.json");
        let mut policy: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        policy["profile"] = "staged_files".into();
        fs::write(path, serde_json::to_vec(&policy)?)?;
        spec.profile = RuntimeProfile::StagedFiles;
        spec.artifact_root = Some(directory.path().canonicalize()?.join("artifacts"));
        let target = store
            .register_managed_target(&owner, &spec, Some(1), 101)
            .await?;
        let credential = store.register("g", "writer", false).await?;
        let writer = store.authenticate("g", Some(&credential)).await?;
        for id in ["artifact-one", "artifact-two"] {
            store
                .task_create(
                    &writer,
                    TaskCreate {
                        key: format!("create-{id}"),
                        reason: "Real binding authority control".into(),
                        expected_parent_versions: Default::default(),
                        draft: TaskDraft {
                            work: crate::work::WorkDraft {
                                id: id.into(),
                                scope: "artifact".into(),
                                owner: owner.name.clone(),
                                state: crate::states::TaskState::Ready,
                                next_action: "Produce bounded text".into(),
                                deadline: None,
                                evidence: vec![],
                            },
                            contract: Contract {
                                deliverable: "Text artifact".into(),
                                criteria: vec![Criterion {
                                    id: "text".into(),
                                    description: "Reviewed text".into(),
                                }],
                                allowed_scope: vec!["artifact".into()],
                                completion: Completion::WriterAcceptance,
                                allow_delegation: false,
                                allow_input_invalidation: true,
                                budget: Budget {
                                    max_attempts: 2,
                                    max_elapsed_seconds: 600,
                                    max_cost: None,
                                },
                            },
                            authorization: Authorization {
                                state: AuthorityState::Authorized,
                                source: AuthoritySource::Direct {
                                    authority_ref: "binding-test-only".into(),
                                },
                                approved_scope: vec!["artifact".into()],
                                reason: "Binding test only".into(),
                            },
                            requirements: vec![],
                            parent: None,
                        },
                    },
                    102,
                )
                .await?;
        }
        let request = ManagedArtifactBindingRequest {
            id: "text-tree".into(),
            key: "bind-one".into(),
            reason: "Bind original task".into(),
            task: "artifact-one".into(),
            expected_task_version: 1,
            target: target.identity,
            target_generation: 2,
            destination: "text-selection".into(),
            scope_unit: "artifact".into(),
            allowed_paths: vec!["report.txt".into()],
            expected_binding_revision: None,
        };
        assert!(
            store
                .bind_managed_artifact(&owner, &request, 103)
                .await
                .is_err()
        );
        let first = store.bind_managed_artifact(&writer, &request, 103).await?;
        assert_eq!(first.revision, 1);
        assert_eq!(first.destination_generation, 1);
        let mut correction = request.clone();
        correction.key = "bind-correction".into();
        correction.expected_binding_revision = Some(1);
        correction.allowed_paths.push("notes.txt".into());
        let second = store
            .bind_managed_artifact(&writer, &correction, 104)
            .await?;
        assert_eq!(second.revision, 2);
        let replay = store.bind_managed_artifact(&writer, &request, 105).await?;
        assert_eq!(
            serde_json::to_value(&replay)?,
            serde_json::to_value(&first)?
        );
        let current = store
            .managed_artifact_binding(&owner, "text-tree", 105)
            .await?
            .unwrap();
        assert_eq!(current.revision, 2);
        let mut cross_task = request.clone();
        cross_task.id = "another-tree".into();
        cross_task.key = "another-task".into();
        cross_task.task = "artifact-two".into();
        assert!(
            store
                .bind_managed_artifact(&writer, &cross_task, 106)
                .await
                .is_err()
        );
        let mut conflicting = request.clone();
        conflicting.reason = "Changed same retry key".into();
        assert!(
            store
                .bind_managed_artifact(&writer, &conflicting, 106)
                .await
                .is_err()
        );
        let mut malformed = correction.clone();
        malformed.key = "alias".into();
        malformed.expected_binding_revision = Some(2);
        malformed.allowed_paths = vec!["a".into(), "a/child".into()];
        assert!(
            store
                .bind_managed_artifact(&writer, &malformed, 106)
                .await
                .is_err()
        );
        let versions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime_artifact_binding_versions")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(versions, 2);
        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_noop_is_audited_and_cannot_enable_from_a_claimed_witness() -> Result<()> {
        use crate::{
            execution::Checked,
            runtime_lifecycle::{ManagedTargetAction, ManagedTargetChange},
        };
        let (_directory, store, actor, _spec, registration) = registered_fixture().await?;
        let request = ManagedTargetChange {
            key: "disable-noop".into(),
            reason: "Keep disabled".into(),
            target: registration.identity.clone(),
            expected_generation: 1,
            expected_revision: 1,
            action: ManagedTargetAction::Disable,
        };
        let Checked::Ready(receipt) = store.change_managed_target(&actor, &request, 101).await?
        else {
            anyhow::bail!("disable held");
        };
        assert_eq!(receipt.revision, 1);
        let mut enable = request.clone();
        enable.key = "unqualified-enable".into();
        enable.action = ManagedTargetAction::Enable {
            qualification: "caller-claim".into(),
        };
        assert!(matches!(
            store.change_managed_target(&actor, &enable, 102).await?,
            Checked::Held(_)
        ));
        let enabled: bool = sqlx::query_scalar("SELECT enabled FROM runtime_targets WHERE id=?")
            .bind(&registration.identity)
            .fetch_one(store.pool())
            .await?;
        assert!(!enabled);
        let mut stale = request.clone();
        stale.key = "stale-disable".into();
        stale.expected_revision = 2;
        assert!(
            store
                .change_managed_target(&actor, &stale, 103)
                .await
                .is_err()
        );
        let mut forged = actor.clone();
        forged.name = "another-owner".into();
        assert!(
            store
                .change_managed_target(&forged, &request, 103)
                .await
                .is_err()
        );
        let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_lifecycle_receipts")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(receipts, 2); // Original legacy registration and accepted no-op only.
        Ok(())
    }

    #[tokio::test]
    async fn capability_inspection_is_read_only_and_does_not_invent_witnesses() -> Result<()> {
        let (_directory, store, actor, _spec, registration) = registered_fixture().await?;
        // Fail on any attempted application write, including the tempting actor-lock UPDATE.
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_all(store.pool())
        .await?;
        for (index, table) in tables.iter().enumerate() {
            let table = table.replace('"', "\"\"");
            for verb in ["INSERT", "UPDATE", "DELETE"] {
                sqlx::query(&format!("CREATE TRIGGER inspection_{index}_{verb} BEFORE {verb} ON \"{table}\" BEGIN SELECT RAISE(ABORT,'inspection wrote application state'); END"))
                    .execute(store.pool()).await?;
            }
        }
        let view = store
            .managed_target_capabilities(&actor, &registration.identity, 101)
            .await?
            .unwrap();
        assert_eq!(view.identity, registration.identity);
        assert_eq!(view.capabilities.len(), 12);
        assert!(!view.enabled && !view.profile_qualified);
        assert!(matches!(
            view.configuration_state,
            ManagedConfigurationState::Current
        ));
        assert!(
            view.capabilities
                .iter()
                .all(|item| item.witnessed_at.is_none()
                    && item.valid_until.is_none()
                    && item.witness_generation.is_none()
                    && !matches!(item.state, CapabilityState::Verified { .. }))
        );
        assert!(
            view.holds
                .iter()
                .any(|hold| hold == "managed_target_disabled")
        );
        assert!(
            store
                .managed_target_capabilities(&actor, "missing-target", 101)
                .await?
                .is_none()
        );
        // A display name or owner name is not an implicit selector for a target ID.
        assert!(
            store
                .managed_target_capabilities(&actor, "fixture", 101)
                .await?
                .is_none()
        );
        let mut forged = actor.clone();
        forged.name = "different-owner".into();
        assert!(
            store
                .managed_target_capabilities(&forged, &registration.identity, 101)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn capability_evidence_preserves_expiry_generation_and_configuration_identity()
    -> Result<()> {
        let (directory, store, actor, mut spec, registration) = registered_fixture().await?;
        // Protected-row fixtures exercise projection only; they are not native qualification.
        for (id, capability, observed, until) in [
            ("expired", "admission_fence", 99, 101),
            ("current", "durable_dedupe", 100, 200),
            ("future", "quiescence", 150, 200),
        ] {
            sqlx::query("INSERT INTO runtime_capability_witnesses(id,target,generation,capability,observed,valid_until,evidence) VALUES(?,?,1,?,?,?,?)")
                .bind(id).bind(&registration.identity).bind(capability).bind(observed).bind(until)
                .bind("{\"fixture_only\":true}").execute(store.pool()).await?;
        }
        let view = store
            .managed_target_capabilities(&actor, &registration.identity, 102)
            .await?
            .unwrap();
        assert!(
            matches!(&view.capabilities[0].state, CapabilityState::Stale { evidence } if evidence == "expired")
        );
        assert_eq!(view.capabilities[0].witnessed_at, Some(99));
        assert_eq!(view.capabilities[0].valid_until, Some(101));
        assert_eq!(view.capabilities[1].witness_generation, Some(1));
        if cfg!(target_os = "linux") {
            assert!(
                matches!(&view.capabilities[1].state, CapabilityState::Verified { evidence } if evidence == "current")
            );
        } else {
            assert!(!view.profile_qualified);
            assert!(!matches!(
                view.capabilities[1].state,
                CapabilityState::Verified { .. }
            ));
        }
        assert!(view.capabilities[2].witnessed_at.is_none());
        assert!(!matches!(
            view.capabilities[2].state,
            CapabilityState::Verified { .. }
        ));
        fs::write(directory.path().join("native"), b"#!/bin/sh\nexit 2\n")?;
        let changed = store
            .managed_target_capabilities(&actor, &registration.identity, 103)
            .await?
            .unwrap();
        assert!(matches!(
            changed.configuration_state,
            ManagedConfigurationState::Changed
        ));
        assert!(
            matches!(&changed.capabilities[1].state, CapabilityState::Stale { evidence } if evidence == "current")
        );
        spec.cwd = directory.path().canonicalize()?.join("replacement-input");
        let next = store
            .register_managed_target(&actor, &spec, Some(1), 104)
            .await?;
        assert_eq!(next.generation, 2);
        let old = store
            .managed_target_capabilities(&actor, &registration.identity, 105)
            .await?
            .unwrap();
        assert_eq!(old.capabilities[1].witness_generation, Some(1));
        assert!(matches!(
            old.capabilities[1].state,
            CapabilityState::Stale { .. }
        ));
        assert!(matches!(
            old.configuration_state,
            ManagedConfigurationState::Current
        ));
        fs::remove_file(directory.path().join("native"))?;
        let unavailable = store
            .managed_target_capabilities(&actor, &registration.identity, 106)
            .await?
            .unwrap();
        assert!(matches!(
            unavailable.configuration_state,
            ManagedConfigurationState::Unavailable { .. }
        ));
        assert!(!unavailable.profile_qualified);
        Ok(())
    }

    #[tokio::test]
    async fn capability_inspection_rejects_stale_actor_and_never_hides_corrupt_target() -> Result<()>
    {
        let (_directory, store, actor, _spec, registration) = registered_fixture().await?;
        sqlx::query("UPDATE mailboxes SET binding_version=binding_version+1 WHERE id=?")
            .bind(actor.id)
            .execute(store.pool())
            .await?;
        assert!(
            store
                .managed_target_capabilities(&actor, &registration.identity, 101)
                .await
                .is_err()
        );
        let current = store.mailbox("g", "worker").await?;
        let view = store
            .managed_target_capabilities(&current, &registration.identity, 101)
            .await?
            .unwrap();
        assert!(
            view.holds
                .iter()
                .any(|reason| reason == "managed_owner_binding_changed")
        );
        // Model actual malformed stored facts without relaxing production constraints.
        // The schema allows a JSON specification that is not a managed descriptor.
        let mut tx = store.pool().begin().await?;
        sqlx::query("UPDATE runtime_targets SET current_generation=2 WHERE id=?")
            .bind(&registration.identity)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO runtime_target_versions SELECT target,2,owner,owner_binding,client,profile,concurrency_key,'{}',policy_digest,executable_digest,created FROM runtime_target_versions WHERE target=? AND generation=1")
            .bind(&registration.identity).execute(&mut *tx).await?;
        tx.commit().await?;
        assert!(
            store
                .managed_target_capabilities(&current, &registration.identity, 102)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn capability_profile_qualification_requires_one_current_target() -> Result<()> {
        let (_directory, store, actor, mut spec, registration) = registered_fixture().await?;
        // This data fixture is confined to this isolated test Store; no real target is qualified.
        for &capability in REQUIRED_CAPABILITIES {
            sqlx::query("INSERT INTO runtime_capability_witnesses(id,target,generation,capability,observed,valid_until,evidence) VALUES(?,?,1,?,100,200,'{\"fixture_only\":true}')")
                .bind(capability).bind(&registration.identity).bind(capability).execute(store.pool()).await?;
        }
        sqlx::query("UPDATE runtime_targets SET enabled=1 WHERE id=?")
            .bind(&registration.identity)
            .execute(store.pool())
            .await?;
        let current = store
            .managed_target_capabilities(&actor, &registration.identity, 101)
            .await?
            .unwrap();
        assert!(current.profile_qualified);
        assert!(current.holds.is_empty());
        assert_eq!(
            current
                .capabilities
                .iter()
                .filter(|item| matches!(item.state, CapabilityState::Verified { .. }))
                .count(),
            9
        );
        spec.target = "second-target".into();
        let second = store
            .register_managed_target(&actor, &spec, None, 102)
            .await?;
        sqlx::query("UPDATE runtime_targets SET enabled=1 WHERE id=?")
            .bind(&second.identity)
            .execute(store.pool())
            .await?;
        let ambiguous = store
            .managed_target_capabilities(&actor, &registration.identity, 103)
            .await?
            .unwrap();
        assert!(!ambiguous.profile_qualified);
        assert!(
            ambiguous
                .holds
                .iter()
                .any(|reason| reason == "managed_profile_unqualified")
        );
        assert_eq!(
            ambiguous
                .capabilities
                .iter()
                .filter(|item| matches!(item.state, CapabilityState::Verified { .. }))
                .count(),
            9
        );
        Ok(())
    }

    #[tokio::test]
    async fn staged_profile_cannot_be_qualified_by_stored_witness_claims() -> Result<()> {
        let (directory, store, actor, mut spec, registration) = registered_fixture().await?;
        let path = directory.path().join("managed-policies/fixture.json");
        let mut policy: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        policy["profile"] = "staged_files".into();
        fs::write(path, serde_json::to_vec(&policy)?)?;
        spec.profile = RuntimeProfile::StagedFiles;
        spec.artifact_root = Some(directory.path().canonicalize()?.join("artifacts"));
        let staged = store
            .register_managed_target(&actor, &spec, Some(1), 101)
            .await?;
        // Guard-sensitivity fixture, not a way to write native qualification in production.
        for &(_, capability) in CAPABILITY_KEYS {
            sqlx::query("INSERT INTO runtime_capability_witnesses(id,target,generation,capability,observed,valid_until,evidence) VALUES(?,?,2,?,101,200,'{\"fixture_only\":true}')")
                .bind(capability).bind(&staged.identity).bind(capability).execute(store.pool()).await?;
        }
        sqlx::query("UPDATE runtime_targets SET enabled=1 WHERE id=?")
            .bind(&registration.identity)
            .execute(store.pool())
            .await?;
        let view = store
            .managed_target_capabilities(&actor, &staged.identity, 102)
            .await?
            .unwrap();
        assert!(view.enabled);
        assert!(!view.profile_qualified);
        assert!(
            view.holds
                .iter()
                .any(|hold| hold == "managed_native_qualification_unavailable")
        );
        assert!(
            view.capabilities
                .iter()
                .all(|capability| !matches!(capability.state, CapabilityState::Verified { .. }))
        );
        Ok(())
    }

    // Only scheduler lifecycle setup uses this fixture. It never creates a capability witness.
    struct RegistrationRuntime(crate::execution::RuntimeTarget);
    impl crate::execution::RuntimeGate for RegistrationRuntime {
        async fn target(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<Option<crate::execution::RuntimeTarget>> {
            Ok(Some(self.0.clone()))
        }
        async fn current(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            _: crate::execution::CurrentUse,
            _: i64,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn closed(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<crate::execution::ClosedRuntime>> {
            Ok(Some(crate::execution::ClosedRuntime {
                receipt: receipt.into(),
                effect_set: "fixture-only-empty-set".into(),
                admitted: true,
                costs: Default::default(),
            }))
        }
        async fn observation(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &crate::execution::Correlation,
            _: &crate::execution::RuntimeTarget,
            receipt: &str,
        ) -> Result<Option<crate::execution::RuntimeObservation>> {
            Ok(Some(crate::execution::RuntimeObservation {
                receipt: receipt.into(),
                sequence: 1,
                observed_at: 103,
                status: crate::execution::ObservationStatus::Unknown,
            }))
        }
    }

    #[tokio::test]
    async fn changed_registration_waits_for_original_held_attempt_even_before_segment() -> Result<()>
    {
        use crate::{
            execution::{self, Checked},
            task_graph::{
                AuthoritySource, AuthorityState, Authorization, Budget, Completion, Contract,
                Criterion, TaskCreate, TaskDraft,
            },
        };
        let (directory, store, actor, spec, registration) = registered_fixture().await?;
        store
            .task_create(
                &actor,
                TaskCreate {
                    key: "registration-control".into(),
                    reason: "configuration correction control".into(),
                    expected_parent_versions: Default::default(),
                    draft: TaskDraft {
                        work: crate::work::WorkDraft {
                            id: "job".into(),
                            scope: "artifact".into(),
                            owner: actor.name.clone(),
                            state: crate::states::TaskState::Ready,
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
                            allow_delegation: true,
                            allow_input_invalidation: true,
                            budget: Budget {
                                max_attempts: 4,
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
                            reason: "fixture-only authority".into(),
                        },
                        requirements: vec![],
                        parent: None,
                    },
                },
                100,
            )
            .await?;
        let runtime = RegistrationRuntime(execution::RuntimeTarget {
            identity: registration.identity.clone(),
            concurrency_key: format!("managed:{}", registration.identity),
            generation: 1,
            profile: "read_only".into(),
            durable_dedupe: true,
            cost_caps: Default::default(),
        });
        let mut tx = store.pool().begin().await?;
        execution::sync_model_tx(&mut tx, "g", &["job".into()], 100).await?;
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM execution_tasks WHERE group_name='g' AND task='job'",
        )
        .fetch_one(&mut *tx)
        .await?;
        let Checked::Ready(c) = execution::claim_attempt_tx(
            &mut tx,
            &runtime,
            &execution::ClaimRequest {
                group: "g".into(),
                task: "job".into(),
                revision,
                key: "claim".into(),
            },
            100,
        )
        .await?
        else {
            anyhow::bail!("registration fixture claim held");
        };
        tx.commit().await?;
        let segments: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_segments")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(segments, 0);
        async fn protected_attempt(
            store: &crate::store::Store,
            attempt: &str,
        ) -> Result<(String, String, String)> {
            let state = sqlx::query_scalar("SELECT json_object('state',state,'runtime',runtime,'slot',holds_slot,'admitted',admitted,'closure',closure,'closed',closed_at,'slot_rows',(SELECT count(*) FROM execution_slots WHERE attempt=execution_attempts.id)) FROM execution_attempts WHERE id=?")
                .bind(attempt).fetch_one(store.pool()).await?;
            let charges = sqlx::query_scalar("SELECT json_group_array(json_object('account',account,'cap',cost_cap,'unit',cost_unit,'settled',settled,'actual',actual_cost)) FROM execution_charges WHERE attempt=?")
                .bind(attempt).fetch_one(store.pool()).await?;
            let budget = sqlx::query_scalar("SELECT json_object('spent',attempts_spent,'reserved',attempts_reserved,'cost_spent',cost_spent,'cost_reserved',cost_reserved,'unknown',unknown_cost,'anchor',anchor,'deadline',deadline) FROM execution_budgets WHERE group_name='g' AND task='job'")
                .fetch_one(store.pool()).await?;
            Ok((state, charges, budget))
        }
        let before = protected_attempt(&store, &c.attempt).await?;
        let mut replacement = spec.clone();
        replacement.cwd = directory.path().canonicalize()?.join("changed-input");
        let error = store
            .register_managed_target(&actor, &replacement, Some(1), 101)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("managed_target_has_held_attempts")
        );
        assert_eq!(protected_attempt(&store, &c.attempt).await?, before);
        assert_eq!(
            store
                .register_managed_target(&actor, &spec, Some(1), 101)
                .await?
                .generation,
            1
        );
        // A held original target cannot freeze unrelated target/group configuration.
        let mut other = spec.clone();
        other.target = "other-target".into();
        let unrelated = store
            .register_managed_target(&actor, &other, None, 101)
            .await?;
        other.cwd = replacement.cwd.clone();
        assert_eq!(
            store
                .register_managed_target(&actor, &other, Some(1), 101)
                .await?
                .generation,
            2
        );
        store.enroll("other-group", None).await?;
        let credential = store.register("other-group", "worker", false).await?;
        let other_actor = store.authenticate("other-group", Some(&credential)).await?;
        let other_group = store
            .register_managed_target(&other_actor, &spec, None, 101)
            .await?;
        assert_ne!(other_group.identity, registration.identity);
        assert_ne!(unrelated.identity, registration.identity);
        assert_eq!(
            store
                .register_managed_target(&other_actor, &replacement, Some(1), 101)
                .await?
                .generation,
            2
        );
        assert_eq!(protected_attempt(&store, &c.attempt).await?, before);
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            execution::expose_dispatch_tx(&mut tx, &runtime, &c, "fixture", 1, 101).await?,
            Checked::Ready(_)
        ));
        assert!(matches!(
            execution::admit_execution_tx(&mut tx, &runtime, &c, 102).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let before = protected_attempt(&store, &c.attempt).await?;
        assert!(
            store
                .register_managed_target(&actor, &replacement, Some(1), 102)
                .await
                .unwrap_err()
                .to_string()
                .contains("managed_target_has_held_attempts")
        );
        assert_eq!(protected_attempt(&store, &c.attempt).await?, before);
        let mut tx = store.pool().begin().await?;
        let _ =
            execution::record_observation_tx(&mut tx, &runtime, &c, "fixture-unknown", 103).await?;
        tx.commit().await?;
        let before = protected_attempt(&store, &c.attempt).await?;
        assert!(
            store
                .register_managed_target(&actor, &replacement, Some(1), 104)
                .await
                .unwrap_err()
                .to_string()
                .contains("managed_target_has_held_attempts")
        );
        assert_eq!(protected_attempt(&store, &c.attempt).await?, before);
        let versions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime_target_versions WHERE target=?")
                .bind(&registration.identity)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(versions, 1);
        let mut tx = store.pool().begin().await?;
        assert!(matches!(
            execution::close_attempt_tx(&mut tx, &runtime, &c, "fixture-closure", 105).await?,
            Checked::Ready(_)
        ));
        tx.commit().await?;
        let next = store
            .register_managed_target(&actor, &replacement, Some(1), 106)
            .await?;
        assert_eq!(next.identity, registration.identity);
        assert_eq!(next.generation, 2);
        assert!(!next.enabled);
        let task: (String, i64) = sqlx::query_as(
            "SELECT state,version FROM work_items WHERE group_name='g' AND id='job'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(task, ("ready".into(), 1));
        Ok(())
    }
}
