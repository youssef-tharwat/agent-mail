use agent_mail::{
    BODY_LIMIT, herdr, now, relay, service,
    store::{Publish, Store},
    supervision,
    work::{WorkDraft, WorkPatch},
};
use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::{io::Read, path::PathBuf};

#[derive(Parser)]
#[command(version, about = "Durable, low-noise messaging for agents in Herdr")]
struct Cli {
    #[arg(long, global = true, env = "AGENT_MAIL_STATE_DIR")]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create the database and enroll a group. Does not bind or prompt agents.
    Setup {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, env = "HERDR_SOCKET_PATH")]
        socket: PathBuf,
        #[arg(long)]
        install_service: bool,
    },
    /// Operator: associate an inbox with a verified native agent session.
    Bind {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        replace: bool,
    },
    /// Show this installation's stable machine identity.
    MachineId,
    /// Set this group's authoritative home after local setup on a remote host.
    Join {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long)]
        home: String,
    },
    /// Configure an SSH alias for a peer installation.
    Peer {
        #[arg(long)]
        machine: String,
        #[arg(long)]
        ssh_target: String,
    },
    /// Operator: explicitly allow or stop periodic SSH sync for a configured peer.
    AutoSync {
        #[arg(long)]
        peer: String,
        #[arg(long, conflicts_with = "disable")]
        enable: bool,
        #[arg(long)]
        disable: bool,
    },
    /// Register a named inbox on another machine.
    Route {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        machine: String,
    },
    /// Exchange durable events with configured peers over SSH.
    Sync {
        #[arg(long)]
        peer: Option<String>,
    },
    /// Internal SSH stdio protocol.
    #[command(subcommand, hide = true)]
    Bridge(Bridge),
    /// Publish one durable message. Reuse its key when retrying.
    Send {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long = "to", required = true)]
        recipients: Vec<String>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        body_file: Option<PathBuf>,
        #[arg(long, default_value_t = 900)]
        due_after: i64,
        #[arg(long)]
        work_id: Option<String>,
    },
    /// List pending summaries, or fetch a single message body.
    Inbox {
        #[arg(long, default_value = "default")]
        group: String,
        message: Option<i64>,
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Resolve your delivery, optionally publishing a reply in the same transaction.
    Resolve {
        #[arg(long, default_value = "default")]
        group: String,
        message: i64,
        #[arg(long, default_value = "handled")]
        note: String,
        #[arg(long, requires = "reply_file", conflicts_with = "withdraw")]
        reply_key: Option<String>,
        #[arg(long, requires = "reply_key", conflicts_with = "withdraw")]
        reply_file: Option<PathBuf>,
        #[arg(long)]
        withdraw: bool,
    },
    /// Recover owned work and unresolved mail in one bounded response.
    Context {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, default_value = "")]
        work_after: String,
        #[arg(long, default_value_t = 0)]
        mail_after: i64,
    },
    /// Maintain small versioned work records in the same store as mail.
    #[command(subcommand)]
    Work(WorkCommand),
    /// Operator: show durable pending work and the latest service diagnostics.
    Status,
    /// Operator: stop automatic prompts for a group. Message operations still work.
    Pause {
        #[arg(long, default_value = "default")]
        group: String,
    },
    /// Operator: resume a group; optionally reset a participant's reminder budget.
    Resume {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long)]
        rearm: Option<String>,
    },
    /// Opt in to unguarded agent prompts, or return to safe notification-only mode.
    PromptMode {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, conflicts_with = "disable")]
        enable_unguarded: bool,
        #[arg(long)]
        disable: bool,
    },
    #[command(subcommand)]
    Service(Service),
    /// Plugin startup hook: restore an already configured service.
    Restore,
}

#[derive(Subcommand)]
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

#[derive(Subcommand)]
enum Bridge {
    Export,
    Exchange {
        #[arg(long)]
        source: String,
    },
}

#[derive(Subcommand)]
enum WorkCommand {
    Create {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        scope: String,
        #[arg(long)]
        owner: String,
        #[arg(long, default_value = "open")]
        state: String,
        #[arg(long)]
        next_action: String,
        #[arg(long)]
        deadline: Option<i64>,
        #[arg(long)]
        evidence: Vec<String>,
    },
    Show {
        #[arg(long, default_value = "default")]
        group: String,
        id: String,
    },
    List {
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, default_value = "")]
        after: String,
    },
    Update {
        #[arg(long, default_value = "default")]
        group: String,
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        next_action: Option<String>,
        #[arg(long, conflicts_with = "reopen")]
        close: bool,
        #[arg(long)]
        reopen: bool,
        #[arg(long, conflicts_with = "clear_deadline")]
        deadline: Option<i64>,
        #[arg(long)]
        clear_deadline: bool,
        #[arg(long, conflicts_with = "clear_accepted_revision")]
        accepted_revision: Option<String>,
        #[arg(long)]
        clear_accepted_revision: bool,
        #[arg(long)]
        evidence: Vec<String>,
        #[arg(long)]
        clear_evidence: bool,
    },
    History {
        #[arg(long, default_value = "default")]
        group: String,
        id: String,
    },
}

fn read_body(path: &std::path::Path) -> Result<String> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?
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
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("agent-mail: {error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let explicit_state = cli.state_dir.is_some();
    let root = supervision::state_root(cli.state_dir)?;
    if matches!(cli.command, Command::Restore) && !root.join("mail.db").exists() {
        println!("{}", json!({"configured":false}));
        return Ok(());
    }
    // Service lifecycle operations cannot hold a database lock while launchd starts a worker.
    match &cli.command {
        Command::Service(Service::Install) => {
            ensure!(root.join("mail.db").is_file(), "run setup first");
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
            let restored = supervision::restore(&root)?;
            println!("{}", json!({"restored":restored}));
            return Ok(());
        }
        _ => {}
    }
    let setup = matches!(cli.command, Command::Setup { .. });
    let (store, guard) = Store::open(&root, setup).await?;
    let output: Value = match cli.command {
        Command::Setup {
            group,
            socket,
            install_service,
        } => {
            store
                .enroll(&group, socket.to_str().context("socket path is not UTF-8")?)
                .await?;
            if !explicit_state {
                supervision::save_locator(&root)?;
            }
            store.pool.close().await;
            drop(guard);
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
            let agent = herdr::agent(std::path::Path::new(&config.socket), &target).await?;
            store.bind(&group, &name, &agent, replace).await?;
            json!({"bound":name,"group":group,"pane":agent.pane_id})
        }
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
                .route(&group, &name, relay::machine(&machine)?)
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
                match store.sync_peer(machine).await {
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
            let actor = store.authenticate(&group).await?;
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
            json!({"id":id,"persisted":true})
        }
        Command::Inbox {
            group,
            message,
            after,
        } => {
            let actor = store.authenticate(&group).await?;
            if let Some(id) = message {
                serde_json::to_value(store.message(&actor, id).await?)?
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
                let more = available > items.len();
                json!({"items":items,"more":more,"next_after":cursor})
            }
        }
        Command::Resolve {
            group,
            message,
            note,
            reply_key,
            reply_file,
            withdraw,
        } => {
            let actor = store.authenticate(&group).await?;
            if withdraw {
                store.withdraw(&actor, message).await?;
                json!({"id":message,"withdrawn":true})
            } else {
                let reply = reply_key
                    .zip(reply_file)
                    .map(|(key, path)| Ok::<_, anyhow::Error>((key, read_body(&path)?)))
                    .transpose()?;
                let reply_id = store.resolve(&actor, message, &note, reply, now()?).await?;
                json!({"id":message,"resolved":true,"reply_id":reply_id})
            }
        }
        Command::Context {
            group,
            work_after,
            mail_after,
        } => {
            let actor = store.authenticate(&group).await?;
            let work = store.work_list(&actor, &work_after).await?;
            let mail = store.inbox(&actor, mail_after).await?;
            let mut works = Vec::new();
            let mut mails = Vec::new();
            let mut next_work = work_after;
            let mut next_mail = mail_after;
            for item in work.iter().take(5) {
                works.push(item);
                let candidate = json!({"group":group,"work":works,"mail":mails,"work_more":true,"mail_more":true,"next_work_after":item.id,"next_mail_after":next_mail});
                if serde_json::to_vec(&candidate)?.len() > 3400 {
                    works.pop();
                    break;
                }
                next_work = item.id.clone();
            }
            for item in mail.iter().take(5) {
                mails.push(item);
                let candidate = json!({"group":group,"work":works,"mail":mails,"work_more":true,"mail_more":true,"next_work_after":next_work,"next_mail_after":item.id});
                if serde_json::to_vec(&candidate)?.len() > 3400 {
                    mails.pop();
                    break;
                }
                next_mail = item.id;
            }
            let peers = store.peers_status().await?;
            let stale_peers: Vec<_> = peers.iter().filter(|p| p.queued > 0 || p.last_error.is_some()).take(3).map(|p| json!({"machine_id":p.machine_id,"queued":p.queued,"last_sync":p.last_sync})).collect();
            let (outbox_pending, outbox_oldest) = store.outbox_status().await?;
            json!({"group":group,"work":works,"mail":mails,"work_more":work.len()>works.len(),"mail_more":mail.len()>mails.len(),"next_work_after":next_work,"next_mail_after":next_mail,"stale_peers":stale_peers,"outbox_pending":outbox_pending,"outbox_oldest":outbox_oldest})
        }
        Command::Work(command) => match command {
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
                let actor = store.authenticate(&group).await?;
                serde_json::to_value(
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
                )?
            }
            WorkCommand::Show { group, id } => {
                let actor = store.authenticate(&group).await?;
                serde_json::to_value(store.work_show(&actor, &id).await?)?
            }
            WorkCommand::List { group, after } => {
                let actor = store.authenticate(&group).await?;
                let items = store.work_list(&actor, &after).await?;
                let more = items.len() > 5;
                let items: Vec<_> = items.into_iter().take(5).collect();
                let next_after = items.last().map(|item| item.id.clone()).unwrap_or(after);
                json!({"items":items,"more":more,"next_after":next_after})
            }
            WorkCommand::Update {
                group,
                id,
                version,
                reason,
                owner,
                state,
                next_action,
                close,
                reopen,
                deadline,
                clear_deadline,
                accepted_revision,
                clear_accepted_revision,
                evidence,
                clear_evidence,
            } => {
                let actor = store.authenticate(&group).await?;
                let patch = WorkPatch {
                    owner,
                    state,
                    next_action,
                    open: if close {
                        Some(false)
                    } else if reopen {
                        Some(true)
                    } else {
                        None
                    },
                    deadline: if clear_deadline {
                        Some(None)
                    } else {
                        deadline.map(Some)
                    },
                    accepted_revision: if clear_accepted_revision {
                        Some(None)
                    } else {
                        accepted_revision.map(Some)
                    },
                    evidence: if clear_evidence {
                        Some(Vec::new())
                    } else if evidence.is_empty() {
                        None
                    } else {
                        Some(evidence)
                    },
                };
                serde_json::to_value(
                    store
                        .work_update(&actor, &id, version, patch, &reason, now()?)
                        .await?,
                )?
            }
            WorkCommand::History { group, id } => {
                let actor = store.authenticate(&group).await?;
                serde_json::to_value(store.work_history(&actor, &id).await?)?
            }
        },
        Command::Status => {
            let report = root.join("service-status.json");
            let diagnostics = if report.exists() {
                serde_json::from_slice::<Value>(&std::fs::read(report)?)?
            } else {
                Value::Null
            };
            let (outbox_pending, outbox_oldest) = store.outbox_status().await?;
            json!({"service_running":service::running(&root),"now":now()?,"groups":store.groups().await?,"inboxes":store.pending().await?,"peers":store.peers_status().await?,"outbox_pending":outbox_pending,"outbox_oldest":outbox_oldest,"last_scan":diagnostics})
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
        Command::Service(_) | Command::Restore => unreachable!("handled before opening database"),
    };
    println!("{}", serde_json::to_string(&output)?);
    store.pool.close().await;
    drop(guard);
    Ok(())
}
