//! Shared records and artifact commands.
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum RecordCommand {
    /// Create a group-visible shared record from JSON.
    Create {
        #[arg(long)]
        file: PathBuf,
    },
    /// Supersede a revision using the observed version and reason in JSON.
    Update {
        id: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Fetch the current or exact immutable revision.
    Show {
        id: String,
        #[arg(long)]
        revision: Option<i64>,
    },
    /// Page record summaries.
    List {
        #[arg(long, default_value = "")]
        after: String,
    },
    /// Page immutable revision history.
    History {
        id: String,
        #[arg(long)]
        before: Option<i64>,
    },
    /// Attach an exact revision to a task or authorized message using JSON.
    Link {
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub(crate) enum ArtifactCommand {
    /// Register typed metadata for an external or repository resource.
    Register {
        #[arg(long)]
        file: PathBuf,
    },
    /// Ingest original bytes into managed storage with metadata from JSON.
    Ingest {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        input: PathBuf,
    },
    /// Inspect bounded resource metadata.
    Show { id: String },
    /// Page retained task, message and exact record revision links.
    Links {
        id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Page resources visible to the group.
    List {
        #[arg(long, default_value = "")]
        after: String,
    },
    /// Retrieve and verify original bytes into a new output file.
    Fetch {
        id: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Check access and integrity without accepting evidence.
    Check { id: String },
    /// Link an artifact to a task, message or exact record revision from JSON.
    Link {
        id: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Remove an explicitly owned resource link from JSON.
    Unlink {
        id: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Set an explicit retention pin.
    Pin {
        id: String,
        #[arg(long)]
        release: bool,
    },
    /// Report logical, unique, stored and reclaimable bytes.
    Stats,
    /// Inspect or prune unreferenced evidence; defaults to a dry run.
    Prune {
        #[arg(long, default_value_t = 86400)]
        grace: i64,
        #[arg(long)]
        apply: bool,
    },
    /// Export a consistent database and every required managed blob (operator).
    Backup { destination: PathBuf },
    /// Restore and verify a complete backup into a new state directory (operator).
    Restore {
        source: PathBuf,
        destination: PathBuf,
    },
}

pub(crate) fn read_json<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> anyhow::Result<T> {
    use std::io::Read;
    let input: Box<dyn Read> = if path == std::path::Path::new("-") {
        Box::new(std::io::stdin())
    } else {
        Box::new(std::fs::File::open(path)?)
    };
    let mut bytes = Vec::new();
    input.take(256 * 1024 + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= 256 * 1024,
        "coordination JSON exceeds 256 KiB"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordLink {
    target: agent_mail::records::RecordTarget,
    id: String,
    revision: i64,
}

pub(crate) async fn run_record(
    store: &agent_mail::store::Store,
    actor: &agent_mail::store::Mailbox,
    command: RecordCommand,
) -> anyhow::Result<serde_json::Value> {
    use serde_json::json;
    Ok(match command {
        RecordCommand::Create { file } => serde_json::to_value(
            store
                .record_create(actor, read_json(&file)?, agent_mail::now()?)
                .await?,
        )?,
        RecordCommand::Update { id, file } => serde_json::to_value(
            store
                .record_update(actor, &id, read_json(&file)?, agent_mail::now()?)
                .await?,
        )?,
        RecordCommand::Show { id, revision } => {
            let record = store.record_show(actor, &id, revision).await?;
            let artifacts = store
                .artifact_links_for_record(actor, &id, record.revision)
                .await?;
            let mut value = serde_json::to_value(record)?;
            value["artifacts"] = serde_json::to_value(artifacts)?;
            value
        }
        RecordCommand::List { after } => {
            let items = store.record_list(actor, &after).await?;
            let more = items.len() > 5;
            let items: Vec<_> = items.into_iter().take(5).collect();
            let next_after = items.last().map(|r| r.id.as_str()).unwrap_or(&after);
            json!({"items":items,"more":more,"next_after":next_after})
        }
        RecordCommand::History { id, before } => {
            let items = store.record_history(actor, &id, before).await?;
            let next_before = items.last().map(|r| r.revision);
            json!({"items":items,"next_before":next_before})
        }
        RecordCommand::Link { file } => {
            let link: RecordLink = read_json(&file)?;
            store
                .record_link(
                    actor,
                    &link.target,
                    &link.id,
                    link.revision,
                    agent_mail::now()?,
                )
                .await?;
            json!({"record":link.id,"revision":link.revision,"linked":true})
        }
    })
}

pub(crate) async fn run_artifact(
    store: &agent_mail::store::Store,
    actor: &agent_mail::store::Mailbox,
    command: ArtifactCommand,
) -> anyhow::Result<serde_json::Value> {
    use agent_mail::artifacts::ArtifactLimits;
    use serde_json::json;
    let limits = ArtifactLimits::default();
    Ok(match command {
        ArtifactCommand::Register { file } => serde_json::to_value(
            store
                .artifact_register(actor, read_json(&file)?, agent_mail::now()?)
                .await?,
        )?,
        ArtifactCommand::Ingest { file, input } => {
            let reader = std::fs::File::open(input)?;
            serde_json::to_value(
                store
                    .artifact_ingest(
                        actor,
                        read_json(&file)?,
                        reader,
                        &limits,
                        agent_mail::now()?,
                    )
                    .await?,
            )?
        }
        ArtifactCommand::Show { id } => {
            serde_json::to_value(store.artifact_show(actor, &id).await?)?
        }
        ArtifactCommand::Links { id, after, limit } => {
            store
                .artifact_targets(actor, &id, after.as_deref(), limit)
                .await?
        }
        ArtifactCommand::List { after } => {
            let items = store
                .artifact_list(
                    actor,
                    if after.is_empty() { None } else { Some(&after) },
                    21,
                )
                .await?;
            let more = items.len() > 20;
            let items: Vec<_> = items.into_iter().take(20).collect();
            let next_after = items
                .last()
                .map(|r| r.resource.id.as_str())
                .unwrap_or(&after);
            json!({"items":items,"more":more,"next_after":next_after})
        }
        ArtifactCommand::Fetch { id, output } => {
            // Verify to a temporary file and publish only after complete success.
            let parent = output
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new("."));
            let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
            store
                .artifact_fetch(actor, &id, temporary.as_file_mut(), &limits)
                .await?;
            temporary.as_file().sync_all()?;
            temporary.persist_noclobber(&output)?;
            json!({"artifact":id,"output":output,"verified":true})
        }
        ArtifactCommand::Check { id } => {
            serde_json::to_value(store.artifact_check(actor, &id, &limits).await?)?
        }
        ArtifactCommand::Link { id, file } => {
            store
                .artifact_link(actor, &id, read_json(&file)?, agent_mail::now()?)
                .await?;
            json!({"artifact":id,"linked":true})
        }
        ArtifactCommand::Unlink { id, file } => {
            store
                .artifact_unlink(actor, &id, read_json(&file)?, agent_mail::now()?)
                .await?;
            json!({"artifact":id,"unlinked":true})
        }
        ArtifactCommand::Pin { id, release } => {
            store
                .artifact_pin(actor, &id, !release, agent_mail::now()?)
                .await?;
            json!({"artifact":id,"pinned":!release})
        }
        ArtifactCommand::Stats => serde_json::to_value(store.artifact_stats(actor).await?)?,
        ArtifactCommand::Prune { grace, apply } => serde_json::to_value(
            store
                .artifact_prune(actor, grace, !apply, agent_mail::now()?)
                .await?,
        )?,
        ArtifactCommand::Restore { .. } => unreachable!("restore is handled before opening state"),
        ArtifactCommand::Backup { destination } => {
            store.artifact_backup(&destination, &limits).await?;
            json!({"backup":destination,"includes_managed_blobs":true})
        }
    })
}
