//! Public workflow-oriented command grammar and validation before mutation.
use super::{Bridge, Command, RunArgs, Service, WorkCommand, read_body};
use agent_mail::states::{NativeRuntime, TaskState};
use agent_mail::{
    supervision,
    work::{WorkPatch, WorkUpdate},
};
use anyhow::{Context, Result, ensure};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use std::{io::Write, path::PathBuf};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    version,
    about = "Durable tasks and messages for coding agents",
    arg_required_else_help = true,
    after_help = "Start: agent-mail init project\n       agent-mail run worker -- claude\n\nMost commands return JSON. Status is a readable summary; use status --json for structured output.\nAdvanced integrations: agent-mail remote --help; agent-mail adapter --help."
)]
pub(super) struct Cli {
    /// Coordination group; otherwise inferred from identity or the sole group.
    #[arg(long, short = 'g', global = true, env = "AGENT_MAIL_GROUP")]
    group: Option<String>,
    /// Local state directory; normally discovered automatically.
    #[arg(long, global = true, env = "AGENT_MAIL_STATE_DIR")]
    state_dir: Option<PathBuf>,
    /// Agent credential; never falls back to another identity if invalid.
    #[arg(
        long,
        global = true,
        env = "AGENT_MAIL_SESSION",
        hide = true,
        hide_env_values = true
    )]
    session: Option<Uuid>,
    /// Print the bundled, version-matched agent skill.
    #[arg(long)]
    skill: bool,
    #[command(subcommand)]
    command: Option<Action>,
}
#[derive(Subcommand)]
enum Action {
    /// Create or verify a local coordination group. Never starts an agent.
    Init {
        /// Explicit group name to initialize.
        name: String,
    },
    /// Recover my assigned tasks and pending requests in a bounded response.
    Context {
        /// Continue task summaries after this task ID.
        #[arg(long, default_value = "")]
        task_after: String,
        /// Continue message summaries after this ID.
        #[arg(long, default_value_t = 0)]
        mail_after: i64,
    },
    /// Stream small change batches; resume with a cursor from an earlier batch.
    Watch {
        /// Exclusive resume cursor from watch output. Omit to start now.
        #[arg(long)]
        after: Option<String>,
    },
    /// Send, read and resolve durable requests.
    #[command(subcommand)]
    Mail(Mail),
    /// Create, inspect and update versioned assignments.
    #[command(subcommand)]
    Task(Task),
    /// Inspect scheduled attention and configure follow-through.
    #[command(subcommand)]
    Attention(Attention),
    /// Register, inspect and retry agents.
    #[command(subcommand)]
    Agent(Agent),
    /// Launch an agent; create its identity if missing and configure recovery.
    Run {
        name: String,
        /// Client or command and its arguments, after --.
        #[arg(required = true, last = true, num_args = 1..)]
        command: Vec<std::ffi::OsString>,
    },
    /// Configure recovery and automatic delivery (operator).
    #[command(subcommand)]
    Runtime(Runtime),
    /// Run or supervise the local delivery worker (operator).
    #[command(subcommand)]
    Service(Service),
    /// Read coordination health; --check also probes runtime setup.
    Status {
        /// Emit full structured status instead of the short summary.
        #[arg(long)]
        json: bool,
        /// Installation-wide operator view, including every group.
        #[arg(long, conflicts_with_all = ["check", "group"])]
        all_groups: bool,
        /// Probe setup, optionally for a named agent; never repairs or prompts.
        #[arg(long,num_args=0..=1,default_missing_value="")]
        check: Option<String>,
    },
    #[command(subcommand, hide = true)]
    Remote(Remote),
    #[command(subcommand, hide = true)]
    Adapter(Adapter),
}
#[derive(Subcommand)]
enum Mail {
    /// Record a next step or waiting condition without resolving work.
    Checkpoint {
        id: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        file: PathBuf,
    },

    /// Send a new request. Reuse the key only for an identical retry.
    Send {
        /// Primary recipient in this group.
        recipient: String,
        /// Short request summary.
        summary: String,
        /// Stable identity for this logical request.
        #[arg(long)]
        key: String,
        /// Additional recipients (repeat for more).
        #[arg(long)]
        to: Vec<String>,
        /// Related task ID; never inferred from recent activity.
        #[arg(long = "task")]
        work: Option<String>,
        /// Full body file, or - to read bounded stdin.
        #[arg(long)]
        body_file: Option<PathBuf>,
        /// Optional business deadline from now, e.g. 15m, 2h or 1d.
        #[arg(long,value_parser=parse_duration)]
        due_in: Option<i64>,
    },
    /// List a bounded page of pending requests.
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Wait for all recipients to settle a sent request, or its deadline.
    Wait {
        id: i64,
        /// Stop earlier than the request deadline, e.g. 30s or 5m.
        #[arg(long,value_parser=parse_duration)]
        timeout: Option<i64>,
    },
    /// Read one message addressed to this agent.
    Show { id: i64 },
    /// Send a final answer and resolve the request atomically; safe to retry.
    Reply {
        id: i64,
        /// Final answer, or use --body-file.
        #[arg(required_unless_present = "body_file", conflicts_with = "body_file")]
        text: Option<String>,
        /// Answer file, or - for bounded stdin.
        #[arg(long)]
        body_file: Option<PathBuf>,
    },
    /// Record an outcome without sending an answer.
    Resolve {
        id: i64,
        #[arg(long)]
        note: String,
    },
    /// Withdraw a request you sent. Does not accept or close its task.
    Withdraw { id: i64 },
}
#[derive(Subcommand)]
enum Task {
    /// Report progress for the observed task revision without changing task state.
    Checkpoint {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Assign a task. Identical creation retries return the original result.
    Create {
        id: String,
        /// Task scope; also the initial next action unless overridden.
        task: String,
        /// Agent responsible for the assignment.
        #[arg(long)]
        owner: String,
        /// Initial next action when it differs from the task scope.
        #[arg(long)]
        next_action: Option<String>,
        /// Initial task lifecycle status.
        #[arg(long, default_value = "open")]
        state: TaskState,
        /// UTC deadline, e.g. 2026-10-01T12:00:00Z.
        #[arg(long,value_parser=parse_deadline)]
        deadline: Option<i64>,
        /// Evidence references, repeated as needed.
        #[arg(long)]
        evidence: Vec<String>,
    },
    /// Read the current task, including version and linked messages.
    Show { id: String },
    /// List open tasks owned or maintained by this agent.
    List {
        #[arg(long, default_value = "")]
        after: String,
    },
    /// Apply one authorized change and optionally resolve a linked request.
    Update {
        id: String,
        /// JSON update file or - for stdin; cannot be mixed with change flags.
        #[arg(long, conflicts_with = "changes")]
        file: Option<PathBuf>,
        #[command(flatten)]
        changes: Changes,
    },
    /// Inspect recent versions and the reasons for each change.
    History { id: String },
}
#[derive(Subcommand)]
enum Attention {
    /// Record a next step for an addressed occurrence; authority may extend its review time.
    Checkpoint {
        id: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Read my current attention occurrences; fetching details records retrieval.
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Fetch an addressed occurrence and current source references.
    Show { id: i64 },
    /// Inspect checkpoint history for a task or inbox delivery.
    History {
        #[arg(long, conflicts_with = "mail", required_unless_present = "mail")]
        task: Option<String>,
        #[arg(long)]
        mail: Option<i64>,
    },
    /// Operator: configure observation/dispatch, intervals and optional notifier.
    Configure {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Args, Default)]
#[group(id = "changes", multiple = true)]
struct Changes {
    /// Version observed before deciding this change; never fetched implicitly.
    #[arg(long)]
    version: Option<i64>,
    /// Why this change is authorized.
    #[arg(long)]
    reason: Option<String>,
    /// New responsible agent.
    #[arg(long)]
    owner: Option<String>,
    /// Task status; done, accepted and cancelled close the assignment.
    #[arg(long)]
    state: Option<TaskState>,
    /// Next business action expected from the owner.
    #[arg(long)]
    next_action: Option<String>,
    /// UTC RFC3339 deadline; use JSON null to clear it.
    #[arg(long,value_parser=parse_deadline)]
    deadline: Option<i64>,
    /// Revision accepted by the writer after applying workflow rules.
    #[arg(long)]
    accepted_revision: Option<String>,
    /// Replace evidence references; repeat for multiple references.
    #[arg(long)]
    evidence: Vec<String>,
    /// Linked inbox message resolved in this same transaction.
    #[arg(long)]
    resolve: Option<i64>,
}
#[derive(Subcommand)]
enum Agent {
    /// Retry notifications and delivery verification after repairing the cause.
    Retry { name: String },
    /// Respond to the exact delivery challenge received by this agent.
    #[command(hide = true)]
    Ack { nonce: uuid::Uuid },
    /// Read durable state and version.
    Show { name: String },
    /// Explicitly retire or restore an agent using the observed version.
    Update {
        name: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        state: agent_mail::states::AgentState,
        #[arg(long)]
        reason: String,
    },
    /// Show the latest 20 registration changes.
    History { name: String },
    /// Issue a new standalone identity. Never replaces an existing credential.
    Add {
        name: String,
        /// Print the credential for a manual integration. Normally use run.
        #[arg(long)]
        show_session: bool,
    },
    /// Explicitly replace a standalone identity and invalidate its old credential.
    Replace {
        name: String,
        /// Print the new credential for a manual integration. Normally use run.
        #[arg(long)]
        show_session: bool,
    },
    /// List registrations without secrets; registration does not imply liveness.
    List,
    /// Bind an address to a verified Herdr pane.
    Bind {
        name: String,
        #[arg(long)]
        herdr_pane: String,
        /// Explicitly replace a previous runtime binding.
        #[arg(long)]
        replace: bool,
    },
}
#[derive(ValueEnum, Clone, Copy)]
enum HerdrPolicy {
    Notify,
    Unguarded,
}
#[derive(Subcommand)]
enum Runtime {
    /// Generate a separate hook settings file without changing global settings.
    Configure {
        #[arg(value_enum)]
        client: NativeRuntime,
        #[arg(long)]
        output: PathBuf,
    },
    /// Attach a verified endpoint to an existing agent.
    Attach {
        name: String,
        #[command(subcommand)]
        endpoint: Endpoint,
    },
    /// Disable delivery and automatic reattachment until explicitly enabled.
    Detach { name: String },
    /// Permit automatic attachment again; resume Claude to register its inbox.
    Enable { name: String },
    /// Pause this group's delivery without stopping the service or blocking mail.
    Pause,
    /// Resume this group's delivery without resetting retry budgets.
    Resume,
    /// Configure the group's Herdr socket explicitly.
    Herdr {
        #[arg(long, env = "HERDR_SOCKET_PATH")]
        socket: PathBuf,
    },
    /// Choose notification-only or explicitly unguarded Herdr prompts.
    HerdrPolicy {
        #[arg(value_enum)]
        policy: HerdrPolicy,
    },
}
#[derive(Subcommand)]
enum Endpoint {
    /// Connect to a persistent thread in an existing Codex app-server.
    Codex {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        thread: Uuid,
    },
    /// Advanced: connect to a client-owned Claude streaming bridge.
    ClaudeStream {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        session: Uuid,
    },
}
#[derive(Subcommand)]
enum Remote {
    /// Show this store's machine identity.
    Id,
    /// Set the selected group's authoritative home machine.
    Join { home: String },
    /// Configure an SSH peer.
    Peer { machine: String, ssh_target: String },
    /// Enable or disable periodic exchange for a configured peer.
    AutoSync {
        peer: String,
        #[arg(value_enum)]
        mode: Switch,
    },
    /// Route a agent address to another machine.
    Route { name: String, machine: String },
    /// Exchange durable events with peers over SSH.
    Sync { peer: Option<String> },
}
#[derive(ValueEnum, Clone, Copy)]
enum Switch {
    Enable,
    Disable,
}
#[derive(Subcommand)]
enum Adapter {
    /// Generic runtime lifecycle hook; JSON on stdin/stdout.
    Hook,
    /// Native Claude inbox lifecycle hook; JSON on stdin/stdout.
    ClaudeHook,
    /// Read a bounded event page for a programmatic consumer.
    Events {
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Confirm one event after actual transport receipt.
    Ack { event: i64 },
    /// Replay and stream events for a programmatic consumer.
    Watch {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        generation: Option<i64>,
    },
    /// Herdr startup callback for an already installed worker.
    Restore,
    /// SSH protocol transport.
    #[command(subcommand)]
    Bridge(Bridge),
    /// Native Claude streaming protocol; not an interactive terminal client.
    ClaudeBridge {
        #[arg(long)]
        socket: PathBuf,
        #[arg(last = true)]
        args: Vec<String>,
    },
}

fn parse_duration(value: &str) -> std::result::Result<i64, String> {
    let (digits, factor) = match value.as_bytes().last() {
        Some(b's') => (&value[..value.len() - 1], 1),
        Some(b'm') => (&value[..value.len() - 1], 60),
        Some(b'h') => (&value[..value.len() - 1], 3600),
        Some(b'd') => (&value[..value.len() - 1], 86400),
        _ => return Err("use a duration such as 30s, 15m, 2h or 1d".into()),
    };
    let seconds = digits
        .parse::<i64>()
        .ok()
        .and_then(|n| n.checked_mul(factor))
        .filter(|n| (1..=31_536_000).contains(n));
    seconds.ok_or_else(|| "duration must be positive and at most 365 days".into())
}
fn parse_deadline(value: &str) -> std::result::Result<i64, String> {
    // RFC3339 parsing delegates calendar and timezone validation to time.
    let date = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|e| e.to_string())?;
    if date.offset() != time::UtcOffset::UTC || date.unix_timestamp() <= 0 {
        return Err("deadline must be a positive UTC timestamp".into());
    }
    Ok(date.unix_timestamp())
}

fn grouped_help(command: clap::Command, categories: &[(&str, &[&str])]) -> clap::Command {
    let mut template = String::from("{about}\n\n{usage-heading} {usage}\n");
    for (heading, names) in categories {
        template.push_str(&format!("\n{heading}:\n"));
        for name in *names {
            let sub = command
                .find_subcommand(name)
                .expect("category command exists");
            template.push_str(&format!(
                "  {:<13} {}\n",
                sub.get_name(),
                sub.get_about().expect("command description")
            ));
        }
    }
    template.push_str("\nOptions:\n{options}\n{after-help}");
    command.help_template(template)
}

impl Cli {
    pub(super) fn parse_cli() -> Self {
        let command = Self::command()
            .mut_subcommand("agent", |c| {
                grouped_help(
                    c,
                    &[
                        ("Inspect", &["list", "show", "history"]),
                        ("Maintain", &["add", "update", "replace", "retry"]),
                        ("Herdr", &["bind"]),
                    ],
                )
            })
            .mut_subcommand("runtime", |c| {
                grouped_help(
                    c,
                    &[
                        (
                            "Native clients",
                            &["configure", "attach", "detach", "enable"],
                        ),
                        ("Group delivery", &["pause", "resume"]),
                        ("Herdr", &["herdr", "herdr-policy"]),
                    ],
                )
            });
        let matches = grouped_help(
            command,
            &[
                ("Start", &["init", "run"]),
                ("Coordinate", &["context", "task", "mail", "watch"]),
                ("Manage", &["status", "agent"]),
                ("Integrations", &["runtime", "service"]),
            ],
        )
        .get_matches();
        Self::from_arg_matches(&matches).unwrap_or_else(|e| e.exit())
    }

    pub(super) async fn prepare(self) -> Result<Option<RunArgs>> {
        if self.skill {
            ensure!(
                self.command.is_none(),
                "--skill cannot be combined with a command"
            );
            print!("{}", agent_mail::SKILL);
            return Ok(None);
        }
        let command = self.command.context("provide a command or --skill")?;
        ensure!(
            !matches!(
                &command,
                Action::Status {
                    all_groups: true,
                    ..
                }
            ) || self.group.is_none(),
            "--all-groups cannot be combined with --group or AGENT_MAIL_GROUP"
        );
        let root = supervision::state_root(self.state_dir.clone())?;
        if let Action::Runtime(Runtime::Configure { client, output }) = &command {
            configure(*client, output)?;
            return Ok(None);
        }
        let scoped = !matches!(
            &command,
            Action::Init { .. }
                | Action::Service(_)
                | Action::Status { check: Some(_), .. }
                | Action::Status {
                    all_groups: true,
                    ..
                }
                | Action::Remote(
                    Remote::Id
                        | Remote::Peer { .. }
                        | Remote::AutoSync { .. }
                        | Remote::Sync { .. }
                )
                | Action::Adapter(
                    Adapter::Restore | Adapter::Bridge(_) | Adapter::ClaudeBridge { .. }
                )
        );
        let group = if scoped {
            let store = agent_mail::upgrade::open(&root, agent_mail::upgrade::OpenMode::Existing)
                .await
                .with_context(|| format!("open Agent Mail store {}", root.display()))?;
            let agent = matches!(
                &command,
                Action::Context { .. }
                    | Action::Watch { .. }
                    | Action::Agent(Agent::Ack { .. })
                    | Action::Mail(_)
                    | Action::Task(_)
                    | Action::Attention(
                        Attention::List { .. }
                            | Attention::Show { .. }
                            | Attention::History { .. }
                            | Attention::Checkpoint { .. }
                    )
                    | Action::Adapter(_)
                    | Action::Status { .. }
            );
            let selected = store
                .select_group(self.group.as_deref(), self.session.as_ref(), agent)
                .await?;
            store.close().await;
            selected
        } else {
            self.group.clone().unwrap_or_default()
        };
        if let Action::Run { name, command } = &command {
            super::launch::launch(&root, &group, name, command).await?;
            return Ok(None);
        }
        let command = match command {
            Action::Run { .. } => unreachable!("handled before dispatch"),
            Action::Init { name } => {
                ensure!(
                    self.group.as_ref().is_none_or(|g| g == &name),
                    "init name conflicts with the selected group"
                );
                Command::Setup {
                    group: name,
                    socket: None,
                    standalone: true,
                    install_service: false,
                }
            }
            Action::Context {
                task_after,
                mail_after,
            } => Command::Context {
                group,
                work_after: task_after,
                mail_after,
            },
            Action::Status {
                check: Some(name), ..
            } => Command::Doctor {
                group,
                name: if name.is_empty() { None } else { Some(name) },
            },
            Action::Status {
                all_groups,
                check: None,
                json,
            } => Command::Status {
                group: if all_groups { None } else { Some(group) },
                json,
            },
            Action::Service(s) => Command::Service(s),
            Action::Attention(attention) => match attention {
                Attention::Checkpoint { id, key, file } => Command::Checkpoint {
                    group,
                    source: agent_mail::followup::Source::Attention { id },
                    key,
                    report: serde_json::from_str(&read_body(&file)?)?,
                },
                Attention::List { after } => Command::AttentionList { group, after },
                Attention::Show { id } => Command::AttentionShow { group, id },
                Attention::History { task, mail } => {
                    Command::AttentionHistory { group, task, mail }
                }
                Attention::Configure { file } => Command::AttentionConfigure {
                    group,
                    policy: serde_json::from_str(&read_body(&file)?)?,
                },
            },
            Action::Agent(p) => match p {
                Agent::Retry { name } => Command::Retry { group, name },
                Agent::Ack { nonce } => Command::AckDelivery { group, nonce },
                Agent::Add { name, show_session } => Command::Register {
                    show_session,
                    group,
                    name,
                    replace: false,
                },
                Agent::Replace { name, show_session } => Command::Register {
                    show_session,
                    group,
                    name,
                    replace: true,
                },
                Agent::Show { name } => Command::AgentShow { group, name },
                Agent::Update {
                    name,
                    version,
                    state,
                    reason,
                } => Command::AgentUpdate {
                    group,
                    name,
                    version,
                    state,
                    reason,
                },
                Agent::History { name } => Command::AgentHistory { group, name },
                Agent::List => Command::Participants { group },
                Agent::Bind {
                    name,
                    herdr_pane,
                    replace,
                } => Command::Bind {
                    group,
                    name,
                    target: herdr_pane,
                    replace,
                },
            },
            Action::Watch { after } => Command::WatchChanges { group, after },
            Action::Mail(mail) => match mail {
                Mail::Checkpoint { id, key, file } => Command::Checkpoint {
                    group,
                    source: agent_mail::followup::Source::Mail { id },
                    key,
                    report: serde_json::from_str(&read_body(&file)?)?,
                },
                Mail::Wait { id, timeout } => Command::WaitMail { group, id, timeout },
                Mail::Send {
                    recipient,
                    summary,
                    key,
                    mut to,
                    work,
                    body_file,
                    due_in,
                } => {
                    to.push(recipient);
                    Command::Send {
                        group,
                        recipients: to,
                        summary,
                        key,
                        work_id: work,
                        body_file,
                        due_after: due_in,
                    }
                }
                Mail::List { after } => Command::Inbox {
                    group,
                    message: None,
                    after,
                },
                Mail::Show { id } => Command::Inbox {
                    group,
                    message: Some(id),
                    after: 0,
                },
                Mail::Reply {
                    id,
                    text,
                    body_file,
                } => {
                    let body = match body_file {
                        Some(path) => read_body(&path)?,
                        None => text.context("reply text is required")?,
                    };
                    ensure!(!body.trim().is_empty(), "reply text is empty");
                    Command::Resolve {
                        group,
                        message: id,
                        note: "replied".into(),
                        reply_key: Some(format!("reply:{id}")),
                        reply_body: Some(body),
                        withdraw: false,
                    }
                }
                Mail::Resolve { id, note } => {
                    ensure!(!note.trim().is_empty(), "outcome is required");
                    Command::Resolve {
                        group,
                        message: id,
                        note,
                        reply_key: None,
                        reply_body: None,
                        withdraw: false,
                    }
                }
                Mail::Withdraw { id } => Command::Resolve {
                    group,
                    message: id,
                    note: String::new(),
                    reply_key: None,
                    reply_body: None,
                    withdraw: true,
                },
            },
            Action::Task(task) => {
                Command::Work(match task {
                    Task::Checkpoint {
                        id,
                        version,
                        key,
                        file,
                    } => WorkCommand::Checkpoint {
                        group,
                        id,
                        version,
                        key,
                        report: serde_json::from_str(&read_body(&file)?)?,
                    },
                    Task::Create {
                        id,
                        task,
                        owner,
                        next_action,
                        state,
                        deadline,
                        evidence,
                    } => WorkCommand::Create {
                        group,
                        id,
                        next_action: next_action.unwrap_or_else(|| task.clone()),
                        scope: task,
                        owner,
                        state,
                        deadline,
                        evidence,
                    },
                    Task::Show { id } => WorkCommand::Show { group, id },
                    Task::List { after } => WorkCommand::List { group, after },
                    Task::History { id } => WorkCommand::History { group, id },
                    Task::Update {
                        id,
                        file,
                        changes: c,
                    } => {
                        let update = if let Some(path) = file {
                            serde_json::from_str::<WorkUpdate>(&read_body(&path)?)?
                        } else {
                            WorkUpdate{version:c.version.context("supply --version from task show, or --file with a complete update")?,reason:c.reason.context("supply --reason for the change")?,resolve_message:c.resolve,
                        patch:WorkPatch{owner:c.owner,state:c.state,next_action:c.next_action,deadline:c.deadline.map(Some),accepted_revision:c.accepted_revision.map(Some),evidence:if c.evidence.is_empty(){None}else{Some(c.evidence)}}}
                        };
                        WorkCommand::Decide { group, id, update }
                    }
                })
            }
            Action::Runtime(runtime) => match runtime {
                Runtime::Attach { name, endpoint } => match endpoint {
                    Endpoint::Codex { socket, thread } => Command::AttachCodex {
                        group,
                        name,
                        socket,
                        thread,
                    },
                    Endpoint::ClaudeStream { socket, session } => Command::AttachClaude {
                        group,
                        name,
                        socket,
                        session_id: session,
                    },
                },
                Runtime::Detach { name } => Command::DetachClaude { group, name },
                Runtime::Enable { name } => Command::EnableRuntime { group, name },
                Runtime::Pause => Command::Pause { group },
                Runtime::Resume => Command::Resume { group, rearm: None },
                Runtime::Herdr { socket } => Command::Setup {
                    group,
                    socket: Some(socket),
                    standalone: false,
                    install_service: false,
                },
                Runtime::HerdrPolicy { policy } => Command::PromptMode {
                    group,
                    enable_unguarded: matches!(policy, HerdrPolicy::Unguarded),
                    disable: matches!(policy, HerdrPolicy::Notify),
                },
                Runtime::Configure { .. } => unreachable!("handled before opening state"),
            },
            Action::Remote(remote) => match remote {
                Remote::Id => Command::MachineId,
                Remote::Join { home } => Command::Join { group, home },
                Remote::Peer {
                    machine,
                    ssh_target,
                } => Command::Peer {
                    machine,
                    ssh_target,
                },
                Remote::AutoSync { peer, mode } => Command::AutoSync {
                    peer,
                    enable: matches!(mode, Switch::Enable),
                    disable: matches!(mode, Switch::Disable),
                },
                Remote::Route { name, machine } => Command::Route {
                    group,
                    name,
                    machine,
                },
                Remote::Sync { peer } => Command::Sync { peer },
            },
            Action::Adapter(adapter) => match adapter {
                Adapter::Hook => Command::Hook { group },
                Adapter::ClaudeHook => Command::ClaudeHook { group },
                Adapter::Events { after } => Command::Events { group, after },
                Adapter::Ack { event } => Command::Ack { group, event },
                Adapter::Watch { after, generation } => Command::Watch {
                    group,
                    after,
                    generation,
                },
                Adapter::Restore => Command::Restore,
                Adapter::Bridge(bridge) => Command::Bridge(bridge),
                Adapter::ClaudeBridge { socket, args } => Command::ClaudeBridge { socket, args },
            },
        };
        Ok(Some(RunArgs {
            state_dir: self.state_dir,
            session: self.session,
            command,
        }))
    }
}

fn configure(client: NativeRuntime, path: &std::path::Path) -> Result<()> {
    let claude = matches!(client, NativeRuntime::Claude);
    let content = serde_json::to_vec_pretty(&super::launch::hook_settings(claude, "agent-mail"))?;
    if path.exists() {
        ensure!(
            std::fs::read(path)? == content,
            "settings file already exists with different content; choose a separate output path"
        );
    } else {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(&content)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist_noclobber(path)
            .context("create settings file without overwriting existing settings")?;
    }
    println!(
        "{}",
        serde_json::json!({"configured":path,"next_action":if claude{"Launch Claude with --settings pointing to this file, in the agent identity environment"}else{"Use this file as the project's .codex/hooks.json, review and trust it via /hooks, then attach the app-server endpoint"}})
    );
    Ok(())
}
