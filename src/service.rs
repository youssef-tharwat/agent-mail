//! Bounded retry workers for Herdr, native clients, and remote relay peers.
//!
//! [`run`] owns an exclusive worker lock and writes a private status snapshot.
//! Continuous mode also owns an event server; shutdown drains clients before the
//! lock is released. [`tick`] accepts Unix seconds for deterministic retry decisions.
//! Wake attempts reserve their durable budget before external delivery.

use crate::states::DeliveryState;
use crate::{
    herdr,
    identity::Binding,
    now,
    store::{Group, Pending, Store},
};
use anyhow::{Context, Result};
use fs2::FileExt;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::task::{JoinError, JoinSet};

const RECOVERY_SCAN_INTERVAL: Duration = Duration::from_secs(5);

fn deadline_delay(deadline: Option<i64>, wall_time: Duration) -> Duration {
    deadline
        .and_then(|deadline| u64::try_from(deadline).ok())
        .map_or(RECOVERY_SCAN_INTERVAL, |deadline| {
            Duration::from_secs(deadline)
                .saturating_sub(wall_time)
                .min(RECOVERY_SCAN_INTERVAL)
        })
}

/// Exclusive ownership of one installation’s delivery worker.
#[derive(Debug)]
pub struct WorkerLock(File);

impl WorkerLock {
    /// Acquire an exclusive worker lock for this installation.
    ///
    /// # Errors
    /// Another worker holds the lock or filesystem access fails.
    pub fn acquire(root: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(root.join("worker.lock"))?;
        file.try_lock_exclusive()
            .context("another agent-mail service is already running")?;
        Ok(Self(file))
    }
}

impl Drop for WorkerLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

/// Serialize external wakes for one participant, including verification wakes.
/// The OS releases this lock on cancellation, timeout, or process exit.
pub(crate) fn wake_lock(root: &Path, recipient: i64) -> Result<Option<WorkerLock>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(root.join(format!("wake-{recipient}.lock")))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(WorkerLock(file))),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Check whether an existing worker lock is currently held.
pub fn running(root: &Path) -> bool {
    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("worker.lock"))
    else {
        return false;
    };
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = FileExt::unlock(&file);
            false
        }
        Err(e) => e.kind() == std::io::ErrorKind::WouldBlock,
    }
}

/// A delivery scan result for one group participant.
#[derive(Debug, Serialize)]
pub struct Observation {
    /// Enrolled group containing the referenced participant or record.
    pub group: String,
    /// Name of the participant addressed by this result.
    pub participant: String,
    /// Delivery scan outcome; it never resolves business obligations.
    pub state: DeliveryState,
    /// Diagnostic context for an unsuccessful attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl Observation {
    fn with_detail(mut self, detail: String) -> Self {
        self.detail = Some(detail);
        self
    }
    fn for_inbox(item: &Pending, state: DeliveryState) -> Self {
        Self {
            group: item.group_name.clone(),
            participant: item.name.clone(),
            state,
            detail: None,
        }
    }
}

async fn session_tick(
    store: Store,
    socket: PathBuf,
    groups: Vec<Group>,
    pending: Vec<Pending>,
    time: i64,
) -> Vec<Observation> {
    match session_tick_inner(&store, &socket, groups, &pending, time).await {
        Ok(states) => states,
        Err(error) => pending
            .iter()
            .map(|p| {
                Observation::for_inbox(p, DeliveryState::Held).with_detail(format!("{error:#}"))
            })
            .collect(),
    }
}

async fn session_tick_inner(
    store: &Store,
    socket: &Path,
    groups: Vec<Group>,
    pending: &[Pending],
    time: i64,
) -> Result<Vec<Observation>> {
    if !herdr::plugin_enabled(socket).await? {
        return Ok(pending
            .iter()
            .map(|p| Observation::for_inbox(p, DeliveryState::PluginDisabled))
            .collect());
    }
    let agents = herdr::agents(socket).await?;
    let mut observations = Vec::new();
    for group in groups {
        let mut alerts = 0;
        for item in pending.iter().filter(|p| p.group_name == group.name) {
            if group.paused != 0 {
                observations.push(Observation::for_inbox(item, DeliveryState::Paused));
                continue;
            }
            if group.auto_prompt == 0 {
                observations.push(Observation::for_inbox(item, DeliveryState::PromptDisabled));
                if store.reserve_alert(item.id, time).await? {
                    alerts += 1;
                }
                continue;
            }
            let binding = store.mailbox(&group.name, &item.name).await?;
            let (attention, fresh) = store.herdr_attention(&binding).await?;
            let mut detail = None;
            let state = if let Some(agent) = agents.iter().find(|a| {
                binding
                    .binding
                    .herdr()
                    .is_some_and(|bound| a.pane_id == bound.pane)
            }) {
                if !attention {
                    DeliveryState::Settled
                } else if !agent.matches(&binding) {
                    DeliveryState::BindingMismatch
                } else if let Some(reason) = agent.readiness_reason() {
                    detail = Some(reason.into());
                    DeliveryState::Busy
                } else if item.attempts >= 3 && !fresh {
                    DeliveryState::Exhausted
                } else if item.next_wake > time {
                    DeliveryState::Waiting
                } else {
                    wake(store, socket, &binding, time)
                        .await
                        .unwrap_or_else(|e| {
                            detail = Some(format!("{e:#}"));
                            DeliveryState::Uncertain
                        })
                }
            } else {
                DeliveryState::Unavailable
            };
            let mut observation = Observation::for_inbox(item, state);
            observation.detail = detail;
            observations.push(observation);
            if store.reserve_alert(item.id, time).await? {
                alerts += 1;
            }
        }
        if alerts > 0 {
            // Reservation is durable even if delivery is ambiguous. Status always retains the problem.
            if let Err(e) = herdr::notify(socket, &group.name, alerts).await {
                observations.push(Observation {
                    group: group.name,
                    participant: String::new(),
                    state: DeliveryState::NotificationFailed,
                    detail: Some(format!("{e:#}")),
                });
            }
        }
    }
    Ok(observations)
}

async fn wake(
    store: &Store,
    socket: &Path,
    binding: &crate::store::Mailbox,
    time: i64,
) -> Result<DeliveryState> {
    let Some(_wake_lock) = wake_lock(store.root(), binding.id)? else {
        return Ok(DeliveryState::Ineligible);
    };
    let Some(target) = binding.binding.herdr() else {
        return Ok(DeliveryState::Unavailable);
    };
    let live = herdr::agent(socket, &target.pane).await?;
    if !live.matches(binding) || !live.ready() {
        return Ok(DeliveryState::StateChanged);
    }
    if !herdr::plugin_enabled(socket).await? {
        return Ok(DeliveryState::PluginDisabled);
    }
    if !store.reserve(binding, time).await? {
        return Ok(DeliveryState::Ineligible);
    }
    let challenge = match store.reserve_delivery_challenge(binding, time).await {
        Ok(challenge) => challenge,
        Err(error) => {
            eprintln!(
                "agent-mail: could not attach delivery check to {}/{} notification: {error:#}",
                binding.group_name, binding.name
            );
            None
        }
    };
    let text = store
        .herdr_notification_text(binding, challenge.as_deref())
        .await?;
    herdr::call(
        socket,
        "agent.prompt",
        json!({"target": target.pane, "text": text}),
    )
    .await?;
    let recorded = if let Some(nonce) = challenge.as_deref() {
        store.record_delivery_challenge(binding, nonce, time).await
    } else {
        Ok(())
    };
    if let Err(error) = recorded {
        eprintln!(
            "agent-mail: could not record delivery check for {}/{}: {error:#}",
            binding.group_name, binding.name
        );
    }
    Ok(DeliveryState::Queued)
}

/// Check native and Herdr delivery eligibility at the supplied Unix timestamp.
///
/// # Errors
/// Database access or a spawned session task fails.
pub async fn tick(store: &Store, time: i64) -> Result<Vec<Observation>> {
    crate::followup::reconcile(store, time).await?;
    crate::followup::notify_operators(store, time).await?;
    let mut pending = Vec::new();
    let mut observations = crate::native::tick(store, time).await?;
    for item in store.pending().await? {
        let mailbox = store.mailbox(&item.group_name, &item.name).await?;
        if matches!(mailbox.binding, Binding::Standalone { .. }) {
            if store.has_native(&mailbox).await? {
                continue;
            }
            let state = if item.due.is_some_and(|due| due <= time) {
                DeliveryState::Overdue
            } else {
                DeliveryState::Unavailable
            };
            observations.push(Observation::for_inbox(&item, state));
        } else {
            pending.push(item);
        }
    }
    let mut sessions: BTreeMap<PathBuf, Vec<Group>> = BTreeMap::new();
    for group in store.groups().await? {
        if pending.iter().any(|p| p.group_name == group.name) {
            let socket = group
                .socket
                .clone()
                .context("Herdr binding has no group socket")?;
            sessions.entry(socket).or_default().push(group);
        }
    }
    let mut jobs = JoinSet::new();
    for (socket, groups) in sessions {
        let subset = pending
            .iter()
            .filter(|p| groups.iter().any(|g| g.name == p.group_name))
            .map(|p| Pending {
                id: p.id,
                group_name: p.group_name.clone(),
                name: p.name.clone(),
                pending: p.pending,
                oldest: p.oldest,
                due: p.due,
                attempts: p.attempts,
                next_wake: p.next_wake,
                alerted: p.alerted,
            })
            .collect();
        jobs.spawn(session_tick(store.clone(), socket, groups, subset, time));
        if jobs.len() >= 4 {
            observations.extend(jobs.join_next().await.context("missing service task")??);
        }
    }
    while let Some(result) = jobs.join_next().await {
        observations.extend(result?);
    }
    Ok(observations)
}

async fn relay_tick(store: &Store, time: i64) -> Result<Vec<Value>> {
    let mut jobs = JoinSet::new();
    let mut reports = Vec::new();
    for peer in store
        .peers_status()
        .await?
        .into_iter()
        .filter(|peer| peer.auto_sync)
    {
        let store = store.clone();
        jobs.spawn(async move {
            let result = match crate::relay::machine(&peer.machine_id) {
                Ok(machine) => store.sync_peer(machine, time).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(sent) => json!({"machine_id":peer.machine_id,"sent":sent}),
                Err(error) => json!({"machine_id":peer.machine_id,"error":format!("{error:#}")}),
            }
        });
        if jobs.len() >= 2 {
            reports.push(jobs.join_next().await.context("missing relay task")??);
        }
    }
    while let Some(result) = jobs.join_next().await {
        reports.push(result?);
    }
    Ok(reports)
}

fn relay_result(job: std::result::Result<Result<Vec<Value>>, JoinError>) -> Vec<Value> {
    match job {
        Ok(Ok(report)) => report,
        Ok(Err(error)) => vec![json!({"error":format!("{error:#}")})],
        Err(error) => vec![json!({"error":format!("relay task failed: {error}")})],
    }
}

/// Run one worker scan or serve continuously until interrupted.
///
/// # Errors
/// Worker locking, state I/O, clock reading, signal handling, or server shutdown fails.
pub async fn run(store: &Store, once: bool) -> Result<()> {
    let _once_lock = if once {
        Some(WorkerLock::acquire(store.root())?)
    } else {
        None
    };
    let stream = if once {
        None
    } else {
        Some(crate::stream::Server::start(store.clone())?)
    };
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut last_relay = None::<tokio::time::Instant>;
    let mut relay_jobs = JoinSet::<Result<Vec<Value>>>::new();
    let mut relay_report = Vec::new();
    let mut verification_jobs = JoinSet::new();
    loop {
        let time = now()?;
        while verification_jobs.try_join_next().is_some() {}
        if verification_jobs.is_empty() {
            let state = store.clone();
            verification_jobs
                .spawn(async move { crate::verification::reconcile(&state, time).await });
        }
        let result = tokio::select! {
            result=tick(store,time)=>result,
            ()=async {if let Some(server)=&stream {server.upgrade_requested().await} else {std::future::pending::<()>().await}}=>{break;},
            result=tokio::signal::ctrl_c()=>{result?;break;},
            _=terminate.recv()=>{break;},
        };
        if relay_jobs.is_empty()
            && last_relay.is_none_or(|last| last.elapsed() >= Duration::from_secs(30))
        {
            let store = store.clone();
            relay_jobs.spawn(async move { relay_tick(&store, time).await });
            last_relay = Some(tokio::time::Instant::now());
        }
        let finished = if once {
            relay_jobs.join_next().await
        } else {
            relay_jobs.try_join_next()
        };
        if let Some(job) = finished {
            relay_report = relay_result(job);
        }
        let report = match result {
            Ok(observations) => {
                json!({"checked_at": time, "observations": observations,"relay":relay_report,"relay_running":!relay_jobs.is_empty()})
            }
            Err(error) => {
                json!({"checked_at": time, "error": format!("{error:#}"),"relay":relay_report,"relay_running":!relay_jobs.is_empty()})
            }
        };
        let mut bytes = serde_json::to_vec(&report)?;
        // Runtime diagnostics are bounded and replace the previous snapshot.
        if bytes.len() > 128 * 1024 {
            bytes = serde_json::to_vec(
                &json!({"checked_at":time,"error":"status exceeded limit; inspect inboxes with status"}),
            )?;
        }
        let path = store.root().join("service-status.tmp");
        use std::io::Write;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&bytes)?;
        std::fs::rename(path, store.root().join("service-status.json"))?;
        if once {
            while let Some(job) = verification_jobs.join_next().await {
                job??;
            }
            println!("{}", String::from_utf8(bytes)?);
            break;
        }
        // Include deadlines that elapsed while this scan was running.
        let delay = match crate::followup::next_deadline(store, time).await {
            Ok(deadline) => deadline_delay(
                deadline,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .context("system clock is before the Unix epoch")?,
            ),
            Err(error) => {
                eprintln!("agent-mail: could not read attention schedule: {error:#}");
                RECOVERY_SCAN_INTERVAL
            }
        };
        tokio::select! {
            () = tokio::time::sleep(delay) => {},
            () = async { if let Some(server)=&stream { server.changed().await } else { std::future::pending::<()>().await } } => {},
            result = tokio::signal::ctrl_c() => { result?; break; }
            _ = terminate.recv() => { break; }
            () = async { if let Some(server)=&stream { server.upgrade_requested().await } else { std::future::pending::<()>().await } } => { break; }
        }
    }
    verification_jobs.abort_all();
    while verification_jobs.join_next().await.is_some() {}
    relay_jobs.abort_all();
    while relay_jobs.join_next().await.is_some() {}
    if let Some(server) = stream {
        server.shutdown().await?;
    }
    Ok(())
}
