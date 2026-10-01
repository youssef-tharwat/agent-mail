//! Execution and managed-runtime requests through their owners' public transactions.
use agent_mail::{
    execution as owner,
    managed_runtime::NativeClient,
    runtime_adapter::{ManagedTargetSpec, RuntimeProfile},
    runtime_effects::{self as effects, ContentDigest},
    runtime_lifecycle as lifecycle,
    states::NativeRuntime,
    store::{Mailbox, Store},
};
use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum ReportKind {
    Yield,
    Result,
    Failure,
}
impl From<ReportKind> for owner::ReportKind {
    fn from(value: ReportKind) -> Self {
        match value {
            ReportKind::Yield => Self::Yield,
            ReportKind::Result => Self::Result,
            ReportKind::Failure => Self::Failure,
        }
    }
}

#[derive(Args)]
pub(super) struct Guard {
    #[arg(long)]
    version: i64,
    #[arg(long)]
    execution_version: i64,
    #[arg(long)]
    key: String,
    #[arg(long)]
    reason: String,
}
#[derive(Subcommand)]
pub(super) enum Execution {
    /// Append evidence for an exact admitted attempt; yield does not close or restart it.
    Report {
        id: String,
        #[arg(long)]
        attempt: String,
        #[arg(long)]
        fence: i64,
        #[arg(long)]
        dispatch_key: String,
        #[arg(long)]
        key: String,
        #[arg(long, value_enum)]
        kind: ReportKind,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        evidence: Vec<String>,
    },
    /// Current attempt, independent budgets, held causes and cleanup state.
    Show { id: String },
    /// Correct a due time within existing authority and original hard limits.
    Schedule {
        id: String,
        #[command(flatten)]
        guard: Guard,
        #[arg(long,value_parser=super::parse_deadline)]
        check_at: i64,
    },
    /// Request stop for the exact attempt. This does not prove physical closure.
    Stop {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        attempt: String,
        #[arg(long)]
        fence: i64,
        #[arg(long)]
        dispatch_key: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
    },
    /// Acknowledge one observed clock discontinuity without changing deadlines.
    ResolveClock {
        id: String,
        #[command(flatten)]
        guard: Guard,
        #[arg(long)]
        generation: i64,
    },
}
impl Execution {
    pub(super) fn prepare(self, group: &str) -> Operation {
        match self {
            Self::Report {
                id,
                attempt,
                fence,
                dispatch_key,
                key,
                kind,
                summary,
                evidence,
            } => Operation::Report(owner::ExecutionReport {
                correlation: owner::Correlation {
                    group: group.into(),
                    task: id,
                    attempt,
                    fence,
                    dispatch_key,
                },
                key,
                kind: kind.into(),
                summary,
                evidence,
            }),
            Self::Show { id } => Operation::Show(id),
            Self::Schedule {
                id,
                guard: g,
                check_at,
            } => Operation::Schedule(owner::ScheduleRequest {
                task: id,
                task_version: g.version,
                execution_revision: g.execution_version,
                key: g.key,
                reason: g.reason,
                due_at: check_at,
            }),
            Self::Stop {
                id,
                version,
                attempt,
                fence,
                dispatch_key,
                key,
                reason,
            } => Operation::Stop(owner::StopRequest {
                correlation: owner::Correlation {
                    group: group.into(),
                    task: id,
                    attempt,
                    fence,
                    dispatch_key,
                },
                task_version: version,
                key,
                reason,
            }),
            Self::ResolveClock {
                id,
                guard: g,
                generation,
            } => Operation::Clock(owner::ClockResolution {
                task: id,
                task_version: g.version,
                execution_revision: g.execution_version,
                generation,
                key: g.key,
                reason: g.reason,
            }),
        }
    }
}
#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Profile {
    ReadOnly,
    StagedFiles,
}
impl From<Profile> for RuntimeProfile {
    fn from(value: Profile) -> Self {
        match value {
            Profile::ReadOnly => Self::ReadOnly,
            Profile::StagedFiles => Self::StagedFiles,
        }
    }
}

#[derive(Subcommand)]
pub(super) enum Target {
    /// Register a disabled target, or correct its exact generation and revision.
    Configure(ConfigureTarget),
    /// Inspect an exact registered target ID, including current lifecycle and holds.
    Show { target: String },
    /// Prevent new admission; existing attempts still require genuine closure.
    Disable(ChangeTarget),
    /// Retain history and retire only after the owner verifies all work is resolved.
    Retire(ChangeTarget),
    /// Request enablement; currently held because native qualification is unavailable.
    Enable {
        #[command(flatten)]
        change: ChangeTarget,
        /// Exact protected qualification ID; supplying it does not establish qualification.
        #[arg(long, required_unless_present = "file", conflicts_with = "file")]
        qualification: Option<String>,
    },
}
impl Target {
    pub(super) fn prepare(self) -> Result<Operation> {
        match self {
            Self::Configure(args) => args.prepare(),
            Self::Show { target } => Ok(Operation::Capabilities(target)),
            Self::Disable(args) => args.prepare(lifecycle::ManagedTargetAction::Disable),
            Self::Retire(args) => args.prepare(lifecycle::ManagedTargetAction::Retire),
            Self::Enable {
                change,
                qualification,
            } => {
                change.prepare(lifecycle::ManagedTargetAction::Enable {
                    // File input supplies the complete action; only its variant is compared.
                    qualification: qualification.unwrap_or_default(),
                })
            }
        }
    }
}

#[derive(Args)]
pub(super) struct ConfigureTarget {
    /// Group-scoped configuration name; subsequent commands use the returned target ID.
    target: String,
    /// Advanced ManagedTargetConfiguration JSON (or -); cannot mix with mutation flags.
    #[arg(long, conflicts_with = "target_configuration_flags")]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: TargetConfigurationFlags,
}
#[derive(Args)]
#[group(id = "target_configuration_flags", multiple = true)]
struct TargetConfigurationFlags {
    #[arg(long, required_unless_present = "file")]
    key: Option<String>,
    #[arg(long, required_unless_present = "file")]
    reason: Option<String>,
    /// Actual home-local target owner; must be the authenticated actor.
    #[arg(long, required_unless_present = "file")]
    owner: Option<String>,
    #[arg(long, value_enum, required_unless_present = "file")]
    client: Option<NativeRuntime>,
    /// Already provisioned absolute input root; this command does not create it.
    #[arg(long, required_unless_present = "file")]
    cwd: Option<PathBuf>,
    #[arg(long, value_enum, required_unless_present = "file")]
    profile: Option<Profile>,
    /// Opaque name of an operator-provisioned protected policy, never inline secrets.
    #[arg(long, required_unless_present = "file")]
    configuration: Option<String>,
    /// Protected storage for staged-files; disjoint from the native input root.
    #[arg(long)]
    artifact_root: Option<PathBuf>,
    /// Both versions are required for correction; omit both for initial configuration.
    #[arg(long, requires = "revision")]
    generation: Option<i64>,
    #[arg(long, requires = "generation")]
    revision: Option<i64>,
}
impl ConfigureTarget {
    fn prepare(self) -> Result<Operation> {
        let request: lifecycle::ManagedTargetConfiguration = if let Some(path) = self.file {
            read_runtime_json(&path)?
        } else {
            let f = self.flags;
            lifecycle::ManagedTargetConfiguration {
                key: f.key.context("supply --key")?,
                reason: f.reason.context("supply --reason")?,
                spec: ManagedTargetSpec {
                    target: self.target.clone(),
                    owner: f.owner.context("supply --owner")?,
                    client: match f.client.context("supply --client")? {
                        NativeRuntime::Codex => NativeClient::Codex,
                        NativeRuntime::Claude => NativeClient::Claude,
                    },
                    cwd: f.cwd.context("supply --cwd")?,
                    profile: f.profile.context("supply --profile")?.into(),
                    artifact_root: f.artifact_root,
                    configuration: f.configuration.context("supply --configuration")?,
                },
                expected_generation: f.generation,
                expected_revision: f.revision,
            }
        };
        ensure!(
            request.spec.target == self.target,
            "target_identity_conflict"
        );
        Ok(Operation::ConfigureTarget(Box::new(request)))
    }
}

#[derive(Args)]
pub(super) struct ChangeTarget {
    /// Exact registered target ID, not its owner or display name.
    target: String,
    /// Advanced ManagedTargetChange JSON (or -); action must match the command.
    #[arg(long, conflicts_with = "target_change_flags")]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: TargetChangeFlags,
}
#[derive(Args)]
#[group(id = "target_change_flags", multiple = true)]
struct TargetChangeFlags {
    #[arg(long, required_unless_present = "file")]
    key: Option<String>,
    #[arg(long, required_unless_present = "file")]
    reason: Option<String>,
    #[arg(long, required_unless_present = "file")]
    generation: Option<i64>,
    #[arg(long, required_unless_present = "file")]
    revision: Option<i64>,
}
impl ChangeTarget {
    fn prepare(self, action: lifecycle::ManagedTargetAction) -> Result<Operation> {
        let request: lifecycle::ManagedTargetChange = if let Some(path) = self.file {
            let request: lifecycle::ManagedTargetChange = read_runtime_json(&path)?;
            use lifecycle::ManagedTargetAction::{Disable, Enable, Retire};
            ensure!(
                matches!(
                    (&request.action, &action),
                    (Disable, Disable) | (Retire, Retire) | (Enable { .. }, Enable { .. })
                ),
                "target_action_conflict"
            );
            request
        } else {
            let f = self.flags;
            lifecycle::ManagedTargetChange {
                key: f.key.context("supply --key")?,
                reason: f.reason.context("supply --reason")?,
                target: self.target.clone(),
                expected_generation: f.generation.context("supply --generation")?,
                expected_revision: f.revision.context("supply --revision")?,
                action,
            }
        };
        ensure!(request.target == self.target, "target_identity_conflict");
        Ok(Operation::ChangeTarget(Box::new(request)))
    }
}

#[derive(Subcommand)]
pub(super) enum Artifact {
    /// Writer: bind a task to an exact target and controlled destination.
    Bind(BindArtifact),
    /// Inspect a group-scoped binding without changing its selection or revision.
    Show { binding: String },
    /// Original producer: publish bounded text under exact admitted authority and CAS.
    Publish(PublishArtifact),
    /// Original producer: inspect a historical receipt by exact correlation and effect.
    Receipt {
        task: String,
        #[arg(long)]
        attempt: String,
        #[arg(long)]
        fence: i64,
        #[arg(long)]
        dispatch_key: String,
        #[arg(long)]
        effect: String,
    },
}
impl Artifact {
    pub(super) fn prepare(self, group: &str) -> Result<Operation> {
        match self {
            Self::Bind(args) => args.prepare(),
            Self::Show { binding } => Ok(Operation::ArtifactBinding(binding)),
            Self::Publish(args) => args.prepare(group),
            Self::Receipt {
                task,
                attempt,
                fence,
                dispatch_key,
                effect,
            } => Ok(Operation::PublicationReceipt(
                owner::Correlation {
                    group: group.into(),
                    task,
                    attempt,
                    fence,
                    dispatch_key,
                },
                effect,
            )),
        }
    }
}

#[derive(Args)]
pub(super) struct BindArtifact {
    /// Stable group-scoped binding ID; a correction keeps this identity and task.
    binding: String,
    /// Advanced ManagedArtifactBindingRequest JSON (or -).
    #[arg(long, conflicts_with = "artifact_binding_flags")]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: ArtifactBindingFlags,
}
#[derive(Args)]
#[group(id = "artifact_binding_flags", multiple = true)]
struct ArtifactBindingFlags {
    #[arg(long, required_unless_present = "file")]
    key: Option<String>,
    #[arg(long, required_unless_present = "file")]
    reason: Option<String>,
    #[arg(long, required_unless_present = "file")]
    task: Option<String>,
    #[arg(long, required_unless_present = "file")]
    task_version: Option<i64>,
    #[arg(long, required_unless_present = "file")]
    target: Option<String>,
    #[arg(long, required_unless_present = "file")]
    target_generation: Option<i64>,
    #[arg(long, required_unless_present = "file")]
    destination: Option<String>,
    /// Literal scope already allowed by the task's current contract and authorization.
    #[arg(long, required_unless_present = "file")]
    scope: Option<String>,
    /// Complete allowed virtual path set (repeat); never a host output path.
    #[arg(long, required_unless_present = "file")]
    allowed_path: Vec<String>,
    /// Exact observed binding revision for correction; omitted only for creation.
    #[arg(long)]
    binding_revision: Option<i64>,
}
impl BindArtifact {
    fn prepare(self) -> Result<Operation> {
        let request: lifecycle::ManagedArtifactBindingRequest = if let Some(path) = self.file {
            read_runtime_json(&path)?
        } else {
            let f = self.flags;
            lifecycle::ManagedArtifactBindingRequest {
                id: self.binding.clone(),
                key: f.key.context("supply --key")?,
                reason: f.reason.context("supply --reason")?,
                task: f.task.context("supply --task")?,
                expected_task_version: f.task_version.context("supply --task-version")?,
                target: f.target.context("supply --target")?,
                target_generation: f.target_generation.context("supply --target-generation")?,
                destination: f.destination.context("supply --destination")?,
                scope_unit: f.scope.context("supply --scope")?,
                allowed_paths: f.allowed_path,
                expected_binding_revision: f.binding_revision,
            }
        };
        ensure!(
            request.id == self.binding,
            "artifact_binding_identity_conflict"
        );
        Ok(Operation::BindArtifact(Box::new(request)))
    }
}

#[derive(Args)]
pub(super) struct PublishArtifact {
    task: String,
    /// Advanced JSON with request: PublicationRequest and contents: ManagedArtifactContents.
    #[arg(long, conflicts_with = "artifact_publication_flags")]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: PublicationFlags,
}
#[derive(Args)]
#[group(id = "artifact_publication_flags", multiple = true)]
struct PublicationFlags {
    #[arg(long, required_unless_present = "file")]
    attempt: Option<String>,
    #[arg(long, required_unless_present = "file")]
    fence: Option<i64>,
    #[arg(long, required_unless_present = "file")]
    dispatch_key: Option<String>,
    /// Stable original effect retry identity; changed bytes conflict.
    #[arg(long, required_unless_present = "file")]
    effect: Option<String>,
    #[arg(long, required_unless_present = "file")]
    destination: Option<String>,
    #[arg(long, required_unless_present = "file")]
    scope: Option<String>,
    /// Original destination generation, not the target configuration generation.
    #[arg(long, required_unless_present = "file")]
    generation: Option<i64>,
    #[arg(long, required_unless_present = "file")]
    task_version: Option<i64>,
    /// Exact prior selected digest, including the original admission basis.
    #[arg(long, required_unless_present_any = ["empty_destination", "file"], conflicts_with = "empty_destination")]
    previous_manifest: Option<String>,
    /// Explicitly expect no prior selection; omission never implies this CAS.
    #[arg(long)]
    empty_destination: bool,
    /// Virtual artifact path=local UTF-8 input file (repeat); at most 4 KiB each, 12 KiB total.
    #[arg(long, required_unless_present = "file")]
    text_file: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationInput {
    request: effects::PublicationRequest,
    contents: effects::ManagedArtifactContents,
}
impl PublishArtifact {
    fn prepare(self, group: &str) -> Result<Operation> {
        let input: PublicationInput = if let Some(path) = self.file {
            let body = read_runtime_body(&path)?;
            // Deserialize the owner DTO first, preserving its unknown/duplicate-field checks.
            let input: PublicationInput =
                serde_json::from_str(&body).context("invalid runtime request JSON")?;
            let fields: Value = serde_json::from_str(&body)?;
            ensure!(
                fields["request"].get("expected_manifest").is_some(),
                "supply an explicit publication manifest CAS (null means empty)"
            );
            ensure!(
                input.request.expected_task_version.is_some(),
                "supply an explicit publication task CAS"
            );
            input.contents.validate(&input.request.manifest)?;
            input
        } else {
            let f = self.flags;
            let (manifest, contents) = read_text_files(&f.text_file)?;
            let expected_manifest = match f.previous_manifest {
                Some(value) => Some(ContentDigest::parse(value)?),
                None => {
                    ensure!(
                        f.empty_destination,
                        "supply --previous-manifest or --empty-destination"
                    );
                    None
                }
            };
            PublicationInput {
                request: effects::PublicationRequest {
                    correlation: owner::Correlation {
                        group: group.into(),
                        task: self.task.clone(),
                        attempt: f.attempt.context("supply --attempt")?,
                        fence: f.fence.context("supply --fence")?,
                        dispatch_key: f.dispatch_key.context("supply --dispatch-key")?,
                    },
                    effect: f.effect.context("supply --effect")?,
                    destination: f.destination.context("supply --destination")?,
                    manifest,
                    scope_unit: f.scope.context("supply --scope")?,
                    expected_generation: f.generation.context("supply --generation")?,
                    expected_manifest,
                    expected_task_version: Some(f.task_version.context("supply --task-version")?),
                },
                contents,
            }
        };
        ensure!(
            input.request.correlation.task == self.task && input.request.correlation.group == group,
            "publication_source_identity_conflict"
        );
        Ok(Operation::PublishArtifact(
            Box::new(input.request),
            input.contents,
        ))
    }
}

// Input-envelope limit only. The owner enforces the smaller per-field/content limits.
const RUNTIME_INPUT_LIMIT: usize = 128 * 1024;

fn read_regular_input(path: &Path, limit: usize) -> Result<String> {
    use rustix::fs::{Mode, OFlags, open};
    // Nonblocking open lets us refuse a FIFO/device without waiting for a writer.
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .with_context(|| format!("open {}", path.display()))?;
    let file = File::from(fd);
    ensure!(
        file.metadata()?.is_file(),
        "input must be a regular file: {}",
        path.display()
    );
    read_limited(file, limit).with_context(|| format!("read {}", path.display()))
}
fn read_limited(input: impl Read, limit: usize) -> Result<String> {
    let mut bytes = Vec::new();
    input.take((limit + 1) as u64).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "input exceeds {limit} UTF-8 bytes");
    String::from_utf8(bytes).context("input must be UTF-8")
}
fn read_runtime_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_str(&read_runtime_body(path)?).context("invalid runtime request JSON")
}
fn read_runtime_body(path: &Path) -> Result<String> {
    if path == Path::new("-") {
        read_limited(std::io::stdin(), RUNTIME_INPUT_LIMIT)
    } else {
        read_regular_input(path, RUNTIME_INPUT_LIMIT)
    }
}
fn read_text_files(
    files: &[String],
) -> Result<(effects::ArtifactManifest, effects::ManagedArtifactContents)> {
    ensure!(
        !files.is_empty() && files.len() <= effects::TEXT_FILE_LIMIT,
        "text_file_count_invalid"
    );
    let mut manifest = effects::ArtifactManifest {
        version: 1,
        files: Vec::new(),
    };
    let mut objects = BTreeMap::new();
    let mut total = 0;
    for file in files {
        let (path, source) = file
            .split_once('=')
            .context("use --text-file VIRTUAL_PATH=INPUT_FILE")?;
        ensure!(
            !path.is_empty() && !source.is_empty(),
            "virtual path and input file must be nonempty"
        );
        let text = read_regular_input(Path::new(source), effects::TEXT_FILE_BYTES)?;
        total += text.len();
        ensure!(
            total <= effects::TEXT_ARTIFACT_BYTES,
            "text_artifact_too_large"
        );
        let digest = ContentDigest::of_bytes(text.as_bytes());
        manifest.files.push(effects::ArtifactFile {
            path: path.into(),
            digest: digest.clone(),
            bytes: text.len() as u64,
        });
        if let Some(old) = objects.insert(digest, text.clone()) {
            ensure!(old == text, "artifact_digest_content_conflict");
        }
    }
    manifest.normalize()?;
    let contents = effects::ManagedArtifactContents {
        objects: objects
            .into_iter()
            .map(|(digest, text)| effects::ManagedTextObject { digest, text })
            .collect(),
    };
    contents.validate(&manifest)?;
    Ok((manifest, contents))
}

pub(crate) enum Operation {
    Capabilities(String),
    ConfigureTarget(Box<lifecycle::ManagedTargetConfiguration>),
    ChangeTarget(Box<lifecycle::ManagedTargetChange>),
    BindArtifact(Box<lifecycle::ManagedArtifactBindingRequest>),
    ArtifactBinding(String),
    PublishArtifact(
        Box<effects::PublicationRequest>,
        effects::ManagedArtifactContents,
    ),
    PublicationReceipt(owner::Correlation, String),
    Report(owner::ExecutionReport),
    Show(String),
    Schedule(owner::ScheduleRequest),
    Stop(owner::StopRequest),
    Clock(owner::ClockResolution),
}
impl Operation {
    pub(crate) async fn run(self, store: &Store, actor: &Mailbox, time: i64) -> Result<Value> {
        Ok(match self {
            Self::ConfigureTarget(request) => {
                json!({"schema_version":1,"receipt":store.configure_managed_target(actor,&request,time).await?})
            }
            Self::ChangeTarget(request) => {
                json!({"schema_version":1,"result":store.change_managed_target(actor,&request,time).await?})
            }
            Self::BindArtifact(request) => {
                json!({"schema_version":1,"binding":store.bind_managed_artifact(actor,&request,time).await?})
            }
            Self::ArtifactBinding(id) => {
                let binding = store.managed_artifact_binding(actor, &id, time).await?;
                json!({"schema_version":1,"observed_at":time,"binding_id":id,"present":binding.is_some(),"binding":binding})
            }
            Self::PublishArtifact(request, contents) => {
                json!({"schema_version":1,"result":store.publish_managed_artifact(actor,&request,&contents,time).await?})
            }
            Self::PublicationReceipt(correlation, effect) => {
                let receipt = store
                    .managed_publication(actor, &correlation, &effect, time)
                    .await?;
                json!({"schema_version":1,"observed_at":time,"correlation":correlation,"effect":effect,"present":receipt.is_some(),"receipt":receipt})
            }
            Self::Capabilities(target) => {
                let capability = store
                    .managed_target_capabilities(actor, &target, time)
                    .await?;
                json!({"schema_version":1,"observed_at":time,"target_identity":target,"present":capability.is_some(),"capabilities":capability})
            }
            Self::Report(request) => {
                json!({"schema_version":1,"result":store.record_managed_report(actor,&request,time).await?})
            }
            Self::Show(id) => {
                json!({"schema_version":1,"observed_at":time,"execution":store.execution_inspect(actor,&id).await?})
            }
            Self::Schedule(request) => {
                json!({"schema_version":1,"execution_revision":store.execution_schedule(actor,&request,time).await?})
            }
            Self::Stop(request) => {
                json!({"schema_version":1,"record":store.execution_stop(actor,&request,time).await?,"closure_confirmed":false})
            }
            Self::Clock(request) => {
                json!({"schema_version":1,"record":store.execution_resolve_clock(actor,&request,time).await?})
            }
        })
    }
}
