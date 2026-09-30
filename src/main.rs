//! Agent Mail command-line interface and supervised delivery worker.
use agent_mail::{
    BODY_LIMIT, herdr, now, relay, service,
    store::{Publish, Store},
    supervision,
    work::WorkDraft,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{io::Read, path::PathBuf};

// Use the application allocator consistently across the CLI and long-lived worker.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod cli;
mod launch;

struct RunArgs {
    state_dir: Option<PathBuf>,
    session: Option<uuid::Uuid>,
    command: Command,
}

enum Command {
    Checkpoint {
        group: String,
        source: agent_mail::followup::Source,
        key: String,
        report: agent_mail::followup::Checkpoint,
    },
    AttentionList {
        group: String,
        after: i64,
    },
    AttentionShow {
        group: String,
        id: i64,
    },
    AttentionHistory {
        group: String,
        task: Option<String>,
        mail: Option<i64>,
    },
    AttentionConfigure {
        group: String,
        policy: agent_mail::followup::Policy,
    },
    WatchChanges {
        group: String,
        after: Option<String>,
    },
    WaitMail {
        group: String,
        id: i64,
        timeout: Option<i64>,
    },
    AckDelivery {
        group: String,
        nonce: uuid::Uuid,
    },
    AgentShow {
        group: String,
        name: String,
    },
    AgentUpdate {
        group: String,
        name: String,
        version: i64,
        state: agent_mail::states::AgentState,
        reason: String,
    },
    AgentHistory {
        group: String,
        name: String,
    },
    /// Native Claude streaming session bridge; stdin/stdout remain the client protocol.
    ClaudeBridge {
        socket: PathBuf,
        args: Vec<String>,
    },
    /// Inspect setup without starting agents or changing configuration.
    Doctor {
        group: String,
        name: Option<String>,
    },
    /// Create the database and enroll a group. Does not bind or prompt agents.
    Setup {
        group: String,
        socket: Option<PathBuf>,
        /// Create a group without Herdr, ignoring an inherited socket environment.
        standalone: bool,
        install_service: bool,
    },
    /// Operator: associate an inbox with a verified native agent session.
    Bind {
        group: String,
        name: String,
        target: String,
        replace: bool,
    },
    /// Operator: register a standalone participant; --replace rotates its session.
    Register {
        show_session: bool,
        group: String,
        name: String,
        replace: bool,
    },
    /// Operator: attach automatic wake to an existing, persistent Codex thread.
    AttachCodex {
        group: String,
        name: String,
        socket: PathBuf,
        thread: uuid::Uuid,
    },
    /// Operator: attach a verified native Claude streaming session.
    AttachClaude {
        group: String,
        name: String,
        socket: PathBuf,
        session_id: uuid::Uuid,
    },
    /// Operator: disable native Claude delivery.
    DetachClaude {
        group: String,
        name: String,
    },
    /// Operator: disable the Codex wake endpoint for this participant.
    EnableRuntime {
        group: String,
        name: String,
    },
    /// List registered participants without exposing session credentials.
    Participants {
        group: String,
    },
    /// Show this installation's stable machine identity.
    MachineId,
    /// Set this group's authoritative home after local setup on a remote host.
    Join {
        group: String,
        home: String,
    },
    /// Configure an SSH alias for a peer installation.
    Peer {
        machine: String,
        ssh_target: String,
    },
    /// Operator: explicitly allow or stop periodic SSH sync for a configured peer.
    AutoSync {
        peer: String,
        enable: bool,
        disable: bool,
    },
    /// Register a named inbox on another machine.
    Route {
        group: String,
        name: String,
        machine: String,
    },
    /// Exchange durable events with configured peers over SSH.
    Sync {
        peer: Option<String>,
    },
    /// Internal SSH stdio protocol.
    Bridge(Bridge),
    /// Publish one durable message. Reuse its key when retrying.
    Send {
        group: String,
        recipients: Vec<String>,
        key: String,
        summary: String,
        body_file: Option<PathBuf>,
        due_after: Option<i64>,
        work_id: Option<String>,
    },
    /// List pending summaries, or fetch a single message body.
    Inbox {
        group: String,
        message: Option<i64>,
        after: i64,
    },
    /// Resolve your delivery, optionally publishing a reply in the same transaction.
    Resolve {
        group: String,
        message: i64,
        note: String,
        reply_key: Option<String>,
        reply_body: Option<String>,
        withdraw: bool,
    },
    /// Recover owned work and unresolved mail in one bounded response.
    Context {
        group: String,
        work_after: String,
        mail_after: i64,
    },
    /// Print lifecycle hook configuration; merge it into the client's existing hooks.
    /// Native Claude lifecycle adapter; reads runtime input and endpoint environment.
    ClaudeHook {
        group: String,
    },
    /// Runtime adapter: receive bounded JSON lifecycle input on stdin.
    Hook {
        group: String,
    },
    /// Read durable notifications. Reading does not acknowledge or resolve them.
    Events {
        group: String,
        after: i64,
    },
    /// Adapter: acknowledge one successfully delivered event for this binding.
    Ack {
        group: String,
        event: i64,
    },
    /// Maintain small versioned work records in the same store as mail.
    Work(WorkCommand),
    /// Stream committed events. Resume cursors must include their binding generation.
    Watch {
        group: String,
        after: i64,
        generation: Option<i64>,
    },
    /// Operator: show durable pending work and the latest service diagnostics.
    Status {
        group: Option<String>,
        json: bool,
    },
    /// Operator: stop automatic prompts for a group. Message operations still work.
    Pause {
        group: String,
    },
    /// Operator: resume a group; optionally reset a participant's reminder budget.
    Resume {
        group: String,
        rearm: Option<String>,
    },
    /// Opt in to unguarded agent prompts, or return to safe notification-only mode.
    PromptMode {
        group: String,
        enable_unguarded: bool,
        disable: bool,
    },
    Service(Service),
    /// Plugin startup hook: restore an already configured service.
    Restore,
    Retry {
        group: String,
        name: String,
    },
}

#[derive(clap::Subcommand)]
enum Service {
    /// Foreground worker, suitable for a process supervisor.
    Run {
        #[arg(long)]
        once: bool,
    },
    /// Install the macOS user launchd job. Keeps state outside plugin source.
    Install,
    /// Unload this plugin's macOS user job, preserving its database.
    Uninstall,
}

#[derive(clap::Subcommand)]
enum Bridge {
    Export,
    Exchange {
        #[arg(long)]
        source: String,
    },
}

enum WorkCommand {
    Checkpoint {
        group: String,
        id: String,
        version: i64,
        key: String,
        report: agent_mail::followup::Checkpoint,
    },
    /// Apply a JSON decision and optionally resolve a linked request atomically.
    Decide {
        group: String,
        id: String,
        update: agent_mail::work::WorkUpdate,
    },
    Create {
        group: String,
        id: String,
        scope: String,
        owner: String,
        state: agent_mail::states::TaskState,
        next_action: String,
        deadline: Option<i64>,
        evidence: Vec<String>,
    },
    Show {
        group: String,
        id: String,
    },
    List {
        group: String,
        after: String,
    },
    History {
        group: String,
        id: String,
    },
}

fn read_body(path: &std::path::Path) -> Result<String> {
    let mut bytes = Vec::new();
    let input: Box<dyn Read> = if path == std::path::Path::new("-") {
        Box::new(std::io::stdin())
    } else {
        Box::new(std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?)
    };
    input
        .take((BODY_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= BODY_LIMIT,
        "body exceeds {BODY_LIMIT} UTF-8 bytes; reference larger evidence by path"
    );
    String::from_utf8(bytes).context("body must be UTF-8")
}

#[tokio::main]
async fn main() {
    if let Err(error) = execute().await {
        eprintln!("agent-mail: {error:#}");
        std::process::exit(1);
    }
}

async fn execute() -> Result<()> {
    if let Some(args) = cli::Cli::parse_cli().prepare().await? {
        run(args).await?;
    }
    Ok(())
}

async fn run(cli: RunArgs) -> Result<()> {
    if let Command::ClaudeBridge { socket, args } = cli.command {
        let result = agent_mail::claude::run(&socket, args).await;
        if let Err(error) = &result {
            eprintln!("agent-mail: {error:#}");
        }
        // Tokio stdin uses a blocking reader; exit after the bridge has joined/aborted
        // its tasks and closed its child so a quiet stdin cannot stall shutdown.
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    let explicit_state = cli.state_dir.is_some();
    let root = supervision::state_root(cli.state_dir)?;
    if let Command::Doctor { group, name } = &cli.command {
        let report =
            agent_mail::doctor::inspect(&root, group, name.as_deref(), cli.session.as_ref()).await;
        println!("{}", serde_json::to_string_pretty(&report)?);
        ensure!(!report.failed(), "diagnostic checks failed");
        return Ok(());
    }
    if matches!(cli.command, Command::Restore) && !root.join("mail.db").exists() {
        println!("{}", json!({"configured":false}));
        return Ok(());
    }
    // Service lifecycle operations cannot hold a database lock while launchd starts a worker.
    match &cli.command {
        Command::Service(Service::Install) => {
            ensure!(
                root.join("mail.db").is_file(),
                "run agent-mail init GROUP first"
            );
            agent_mail::upgrade::open(&root, agent_mail::upgrade::OpenMode::Existing)
                .await?
                .close()
                .await;
            supervision::install(&root)?;
            println!("{}", json!({"installed":true}));
            return Ok(());
        }
        Command::Service(Service::Uninstall) => {
            supervision::uninstall(&root)?;
            println!("{}", json!({"uninstalled":true,"state_preserved":true}));
            return Ok(());
        }
        Command::Restore => {
            agent_mail::upgrade::open(&root, agent_mail::upgrade::OpenMode::Existing)
                .await?
                .close()
                .await;
            let restored = supervision::restore(&root)?;
            println!("{}", json!({"restored":restored}));
            return Ok(());
        }
        _ => {}
    }
    let mode = match &cli.command {
        Command::Setup { .. } => agent_mail::upgrade::OpenMode::Initialize,
        Command::Service(Service::Run { .. }) => agent_mail::upgrade::OpenMode::Worker,
        _ => agent_mail::upgrade::OpenMode::Existing,
    };
    let store = agent_mail::upgrade::open(&root, mode).await?;
    let output: Value = match cli.command {
        Command::Setup {
            group,
            socket,
            standalone,
            install_service,
        } => {
            let socket = if standalone {
                store
                    .groups()
                    .await?
                    .into_iter()
                    .find(|existing| existing.name == group)
                    .and_then(|existing| existing.socket)
            } else {
                socket
            };
            store.enroll(&group, socket.as_deref()).await?;
            if !explicit_state {
                supervision::save_locator(&root)?;
            }
            store.close().await;
            if install_service {
                supervision::install(&root)?;
            }
            println!(
                "{}",
                json!({"group":group,"state_dir":root.canonicalize()?,"service_installed":install_service})
            );
            return Ok(());
        }
        Command::Bind {
            group,
            name,
            target,
            replace,
        } => {
            let config = store.group(&group).await?;
            let socket = config
                .socket
                .as_deref()
                .context("configure a Herdr socket before binding")?;
            ensure!(
                herdr::plugin_enabled(socket).await?,
                "Mail plugin is disabled in this Herdr session; run herdr plugin enable youssef-tharwat.agent-mail, then bind again"
            );
            let agent = herdr::agent(std::path::Path::new(socket), &target).await?;
            store.bind(&group, &name, &agent, replace).await?;
            json!({"bound":name,"group":group,"pane":agent.pane_id,"delivery":store.recipient_delivery_outcome(&group,&name).await})
        }
        Command::Register {
            show_session,
            group,
            name,
            replace,
        } => {
            let session = store.register(&group, &name, replace).await?;
            let mut result = json!({"group":group,"name":name,"runtime":"standalone","delivery":store.recipient_delivery_outcome(&group,&name).await});
            if show_session {
                result["session"] = json!(session);
            }
            result
        }
        Command::AttachCodex {
            group,
            name,
            socket,
            thread,
        } => {
            let actor = store.mailbox(&group, &name).await?;
            store.attach_codex(&actor, &socket, thread).await?;
            json!({"attached":name,"thread":thread,"group":group})
        }
        Command::AttachClaude {
            group,
            name,
            socket,
            session_id,
        } => {
            let actor = store.mailbox(&group, &name).await?;
            store.attach_claude(&actor, &socket, session_id).await?;
            json!({"attached":name,"session_id":session_id,"group":group})
        }
        Command::EnableRuntime { group, name } => {
            let actor = store.mailbox(&group, &name).await?;
            store.set_runtime_enabled(&actor, true).await?;
            json!({"enabled":name,"group":group,"next_action":"resume the native session to register its endpoint"})
        }
        Command::Retry { group, name } => {
            let actor = store.mailbox(&group, &name).await?;
            ensure!(
                actor.state == agent_mail::states::AgentState::Registered,
                "agent is retired; restore it before retrying delivery"
            );
            supervision::ensure_running(&store, &actor, &std::env::current_exe()?).await?;
            store.rearm(&group, &name).await?;
            json!({"retry_started":name,"group":group,"delivery":store.recipient_delivery_outcome(&group,&name).await})
        }
        Command::DetachClaude { group, name } => {
            let actor = store.mailbox(&group, &name).await?;
            store.set_runtime_enabled(&actor, false).await?;
            json!({"detached":name,"group":group})
        }
        Command::AckDelivery { group, nonce } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store.acknowledge_delivery(&actor, nonce, now()?).await?;
            json!({"group":group,"agent":actor.name,"notification_acknowledged":true,"task_completed":false})
        }
        Command::AgentShow { group, name } => {
            let mut result = serde_json::to_value(store.agent_record(&group, &name).await?)?;
            result["delivery"] = store.recipient_delivery_outcome(&group, &name).await;
            result
        }
        Command::AgentUpdate {
            group,
            name,
            version,
            state,
            reason,
        } => serde_json::to_value(
            store
                .update_agent(&group, &name, version, state, &reason)
                .await?,
        )?,
        Command::AgentHistory { group, name } => {
            serde_json::to_value(store.agent_history(&group, &name).await?)?
        }
        Command::Participants { group } => serde_json::to_value(store.participants(&group).await?)?,
        Command::MachineId => json!({"machine_id":store.machine_id().await?}),
        Command::Join { group, home } => {
            store.set_home(&group, relay::machine(&home)?).await?;
            json!({"group":group,"home_machine":home})
        }
        Command::Peer {
            machine,
            ssh_target,
        } => {
            store
                .add_peer(relay::machine(&machine)?, &ssh_target)
                .await?;
            json!({"machine_id":machine,"ssh_target":ssh_target})
        }
        Command::AutoSync {
            peer,
            enable,
            disable,
        } => {
            ensure!(enable || disable, "choose --enable or --disable");
            store.set_auto_sync(relay::machine(&peer)?, enable).await?;
            json!({"machine_id":peer,"auto_sync":enable})
        }
        Command::Route {
            group,
            name,
            machine,
        } => {
            store
                .route(&group, &name, relay::machine(&machine)?, now()?)
                .await?;
            json!({"group":group,"name":name,"machine_id":machine})
        }
        Command::Sync { peer } => {
            let peers = store.peers_status().await?;
            ensure!(
                peer.as_ref()
                    .is_none_or(|id| peers.iter().any(|p| &p.machine_id == id)),
                "requested peer is not configured"
            );
            let mut synced = Vec::new();
            for configured in peers {
                if peer.as_ref().is_some_and(|id| id != &configured.machine_id) {
                    continue;
                }
                let machine = relay::machine(&configured.machine_id)?;
                match store.sync_peer(machine, now()?).await {
                    Ok(sent) => {
                        synced.push(json!({"machine_id":configured.machine_id,"sent":sent}))
                    }
                    Err(error) => synced.push(
                        json!({"machine_id":configured.machine_id,"error":format!("{error:#}")}),
                    ),
                }
            }
            json!({"peers":synced})
        }
        Command::Bridge(Bridge::Export) => serde_json::to_value(store.export().await?)?,
        Command::Bridge(Bridge::Exchange { source }) => {
            let mut bytes = Vec::new();
            std::io::stdin()
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            let exchange = relay::decode_exchange(&bytes)?;
            serde_json::to_value(
                store
                    .exchange(relay::machine(&source)?, exchange, now()?)
                    .await?,
            )?
        }
        Command::Send {
            group,
            recipients,
            key,
            summary,
            body_file,
            due_after,
            work_id,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            let body = body_file
                .as_deref()
                .map(read_body)
                .transpose()?
                .unwrap_or_default();
            let id = store
                .publish(
                    &actor,
                    Publish {
                        recipients,
                        key,
                        summary,
                        body,
                        due_after,
                        reply_to: None,
                        work_id,
                    },
                    now()?,
                )
                .await?;
            json!({"id":id,"persisted":true,"delivery":store.message_delivery_outcome(&group,id).await})
        }
        Command::Checkpoint {
            group,
            source,
            key,
            report,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store
                .checkpoint(&actor, source, &key, report, now()?)
                .await?
        }
        Command::AttentionConfigure { group, policy } => {
            store.configure_followups(&group, &policy, now()?).await?;
            json!({"group":group,"policy":policy})
        }
        Command::AttentionList { group, after } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store.attention_list(&actor, after).await?
        }
        Command::AttentionShow { group, id } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store.attention_show(&actor, id).await?
        }
        Command::AttentionHistory { group, task, mail } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            json!(
                store
                    .checkpoint_history(&actor, task.as_deref(), mail)
                    .await?
            )
        }
        Command::Inbox {
            group,
            message,
            after,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            if let Some(id) = message {
                let mut value = serde_json::to_value(store.message(&actor, id).await?)?;
                value["followup"] = store.source_followup(&actor, None, Some(id)).await?;
                value
            } else {
                let records = store.inbox(&actor, after).await?;
                let mut items = Vec::new();
                let available = records.len();
                let mut cursor = after;
                for record in records.into_iter().take(5) {
                    let id = record.id;
                    items.push(record);
                    let candidate = json!({"items":items,"more":true,"next_after":id});
                    if serde_json::to_vec(&candidate)?.len() > 2047 {
                        items.pop();
                        break;
                    }
                    cursor = id;
                }
                store
                    .retrieved(&actor, &items.iter().map(|m| m.id).collect::<Vec<_>>(), &[])
                    .await?;
                let more = available > items.len();
                json!({"items":items,"more":more,"next_after":cursor})
            }
        }
        Command::Resolve {
            group,
            message,
            note,
            reply_key,
            reply_body,
            withdraw,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            if withdraw {
                store.withdraw(&actor, message, now()?).await?;
                json!({"id":message,"withdrawn":true})
            } else {
                let reply = reply_key.zip(reply_body);
                let reply_id = store.resolve(&actor, message, &note, reply, now()?).await?;
                json!({"id":message,"resolved":true,"reply_id":reply_id,"delivery":match reply_id {Some(id)=>store.message_delivery_outcome(&group,id).await,None=>json!([])}})
            }
        }
        Command::Context {
            group,
            work_after,
            mail_after,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store.context_value(&actor, work_after, mail_after).await?
        }
        Command::ClaudeHook { group } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            let mut bytes = Vec::new();
            std::io::stdin().take(65537).read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 65536, "hook input exceeds 64 KiB");
            observe_hook(&store, &actor, &bytes).await?;
            let input: agent_mail::claude_inbox::Input = serde_json::from_slice(&bytes)?;
            let receipt = match (
                std::env::var("CLAUDE_CODE_MESSAGING_SOCKET").ok(),
                std::env::var("CLAUDE_CODE_MESSAGING_TOKEN").ok(),
            ) {
                (Some(socket), Some(token)) => {
                    store
                        .claude_inbox_hook(
                            &actor,
                            &input,
                            std::path::Path::new(socket.strip_prefix("uds:").unwrap_or(&socket)),
                            &token,
                        )
                        .await?
                }
                (None, None) => None, // Headless clients can still recover via hooks.
                _ => anyhow::bail!("incomplete Claude messaging environment"),
            };
            match receipt {
                Some(receipt_context) => {
                    store
                        .reserve_hook(&actor, &input.session_id.to_string(), false, false, now()?)
                        .await?;
                    receipt_context
                }
                None if matches!(
                    input.hook_event_name,
                    agent_mail::claude_inbox::Event::SessionEnd
                        | agent_mail::claude_inbox::Event::StopFailure
                ) =>
                {
                    json!({})
                }
                None => {
                    store
                        .hook(&actor, serde_json::from_slice(&bytes)?, now()?)
                        .await?
                }
            }
        }
        Command::Hook { group } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            let mut bytes = Vec::new();
            std::io::stdin().take(65537).read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 65536, "hook input exceeds 64 KiB");
            let current_launch = observe_hook(&store, &actor, &bytes).await?;
            let input: agent_mail::hooks::HookInput = serde_json::from_slice(&bytes)?;
            if let Some(socket) = std::env::var_os("AGENT_MAIL_CODEX_SOCKET") {
                if current_launch && store.runtime_enabled(&actor).await? {
                    // The client owns the thread; the hook provides its identity.
                    let thread = uuid::Uuid::parse_str(&input.session_id)?;
                    if let Err(error) = store
                        .attach_codex_from_hook(
                            &actor,
                            std::path::Path::new(&socket),
                            thread,
                            &std::env::var("AGENT_MAIL_LAUNCH")?,
                        )
                        .await
                    {
                        eprintln!("agent-mail: idle delivery attachment pending: {error:#}");
                    }
                }
            }
            store.hook(&actor, input, now()?).await?
        }
        Command::Events { group, after } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            let mut items = store.notifications(&actor, after).await?;
            let more = items.len() > 5;
            items.truncate(5);
            let next = items.last().map_or(after, |item| item.id);
            json!({"items":items,"more":more,"next_after":next,"binding_version":actor.binding_version})
        }
        Command::Ack { group, event } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            store.acknowledge(&actor, event).await?;
            json!({"event":event,"acknowledged":true,"resolved":false})
        }
        Command::Work(command) => match command {
            WorkCommand::Checkpoint {
                group,
                id,
                version,
                key,
                report,
            } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                store
                    .checkpoint(
                        &actor,
                        agent_mail::followup::Source::Task { id, version },
                        &key,
                        report,
                        now()?,
                    )
                    .await?
            }
            WorkCommand::Decide { group, id, update } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                let mut value =
                    serde_json::to_value(store.update_work(&actor, &id, update, now()?).await?)?;
                value["delivery"] = store
                    .task_delivery_outcome(&group, &id, value["version"].as_i64().unwrap_or(0))
                    .await;
                value
            }
            WorkCommand::Create {
                group,
                id,
                scope,
                owner,
                state,
                next_action,
                deadline,
                evidence,
            } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                let task_id = id.clone();
                let mut value = serde_json::to_value(
                    store
                        .work_create(
                            &actor,
                            WorkDraft {
                                id,
                                scope,
                                owner,
                                state,
                                next_action,
                                deadline,
                                evidence,
                            },
                            now()?,
                        )
                        .await?,
                )?;
                value["delivery"] = store
                    .task_delivery_outcome(&group, &task_id, value["version"].as_i64().unwrap_or(0))
                    .await;
                value
            }
            WorkCommand::Show { group, id } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                let mut value = serde_json::to_value(store.work_show(&actor, &id).await?)?;
                value["followup"] = store.source_followup(&actor, Some(&id), None).await?;
                value
            }
            WorkCommand::List { group, after } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                let items = store.work_list(&actor, &after).await?;
                let more = items.len() > 5;
                let items: Vec<_> = items.into_iter().take(5).collect();
                store
                    .retrieved(
                        &actor,
                        &[],
                        &items
                            .iter()
                            .map(|w| (w.id.clone(), w.version))
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                let next_after = items.last().map(|item| item.id.clone()).unwrap_or(after);
                json!({"items":items,"more":more,"next_after":next_after})
            }
            WorkCommand::History { group, id } => {
                let actor = store.authenticate(&group, cli.session.as_ref()).await?;
                serde_json::to_value(store.work_history(&actor, &id).await?)?
            }
        },
        Command::WatchChanges { group, after } => {
            use std::io::Write;
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            let mut watch = store.watch(&actor, after.as_deref()).await?;
            println!(
                "{}",
                json!({"type":"ready","group":group,"agent":actor.name,"cursor":watch.cursor()})
            );
            std::io::stdout().flush()?;
            loop {
                let batch = watch.next().await?;
                println!("{}", serde_json::to_string(&batch)?);
                std::io::stdout().flush()?;
            }
        }
        Command::WaitMail { group, id, timeout } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            serde_json::to_value(
                store
                    .wait_mail(
                        &actor,
                        id,
                        timeout.map(|seconds| std::time::Duration::from_secs(seconds as u64)),
                    )
                    .await?,
            )?
        }
        Command::Watch {
            group,
            after,
            generation,
        } => {
            let actor = store.authenticate(&group, cli.session.as_ref()).await?;
            ensure!(
                after == 0 || generation == Some(actor.binding_version),
                "resuming requires --generation matching the current binding; otherwise recover with --after 0"
            );
            let mut stream = agent_mail::stream::connect(&store, &actor, after).await?;
            loop {
                let frame = agent_mail::stream::next(&mut stream).await?;
                println!("{}", serde_json::to_string(&frame)?);
                ensure!(
                    !matches!(frame, agent_mail::stream::Frame::Error { .. }),
                    "stream ended; recover and reconnect"
                );
            }
        }
        Command::Status { group, json } => {
            if !json {
                println!(
                    "{}",
                    agent_mail::status::summary(&store, group.as_deref()).await?
                );
                return Ok(());
            }
            agent_mail::status::report(&store, group.as_deref()).await?
        }
        Command::Pause { group } => {
            store.pause(&group, true).await?;
            json!({"group":group,"paused":true})
        }
        Command::Resume { group, rearm } => {
            if let Some(name) = &rearm {
                store.rearm(&group, name).await?;
            }
            store.pause(&group, false).await?;
            json!({"group":group,"paused":false,"rearmed":rearm})
        }
        Command::PromptMode {
            group,
            enable_unguarded,
            disable,
        } => {
            ensure!(
                enable_unguarded || disable,
                "choose --enable-unguarded or --disable"
            );
            store.set_auto_prompt(&group, enable_unguarded).await?;
            json!({"group":group,"auto_prompt":enable_unguarded,"guarded":false})
        }
        Command::Service(Service::Run { once }) => {
            service::run(&store, once).await?;
            return Ok(());
        }
        Command::ClaudeBridge { .. }
        | Command::Doctor { .. }
        | Command::Service(_)
        | Command::Restore => {
            unreachable!("handled before opening database")
        }
    };
    println!("{}", serde_json::to_string(&output)?);
    store.close().await;
    Ok(())
}

async fn observe_hook(
    store: &Store,
    actor: &agent_mail::store::Mailbox,
    bytes: &[u8],
) -> Result<bool> {
    if let Ok(launch) = std::env::var("AGENT_MAIL_LAUNCH") {
        let input: serde_json::Value = serde_json::from_slice(bytes)?;
        return store
            .observe_hook(
                actor,
                &launch,
                input["session_id"]
                    .as_str()
                    .context("hook session missing")?,
                input["hook_event_name"]
                    .as_str()
                    .context("hook event missing")?
                    .parse()?,
                now()?,
            )
            .await;
    }
    Ok(false)
}
