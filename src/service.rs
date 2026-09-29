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
    path::Path,
    time::Duration,
};
use tokio::task::{JoinError, JoinSet};

pub struct WorkerLock(File);

impl WorkerLock {
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

#[derive(Debug, Serialize)]
pub struct Observation {
    pub group: String,
    pub participant: String,
    pub state: String,
}

impl Observation {
    fn for_inbox(item: &Pending, state: impl Into<String>) -> Self {
        Self {
            group: item.group_name.clone(),
            participant: item.name.clone(),
            state: state.into(),
        }
    }
}

async fn session_tick(
    store: Store,
    socket: String,
    groups: Vec<Group>,
    pending: Vec<Pending>,
    time: i64,
) -> Vec<Observation> {
    match session_tick_inner(&store, &socket, groups, &pending, time).await {
        Ok(states) => states,
        Err(error) => pending
            .iter()
            .map(|p| Observation::for_inbox(p, format!("held: {error:#}")))
            .collect(),
    }
}

async fn session_tick_inner(
    store: &Store,
    socket: &str,
    groups: Vec<Group>,
    pending: &[Pending],
    time: i64,
) -> Result<Vec<Observation>> {
    let socket = Path::new(socket);
    if !herdr::plugin_enabled(socket).await? {
        return Ok(pending
            .iter()
            .map(|p| Observation::for_inbox(p, "plugin disabled or unlinked"))
            .collect());
    }
    let agents = herdr::agents(socket).await?;
    let mut observations = Vec::new();
    for group in groups {
        let mut alerts = 0;
        for item in pending.iter().filter(|p| p.group_name == group.name) {
            if group.paused != 0 {
                observations.push(Observation::for_inbox(item, "paused"));
                continue;
            }
            if group.auto_prompt == 0 {
                observations.push(Observation::for_inbox(
                    item,
                    "held: automatic agent prompts disabled; use inbox or explicitly opt in",
                ));
                if store.reserve_alert(item.id, time).await? {
                    alerts += 1;
                }
                continue;
            }
            let binding = store.mailbox(&group.name, &item.name).await?;
            let state = if let Some(agent) = agents.iter().find(|a| {
                binding
                    .binding
                    .herdr()
                    .is_some_and(|bound| a.pane_id == bound.pane)
            }) {
                if !agent.matches(&binding) {
                    "binding mismatch; rebind explicitly".to_string()
                } else if !agent.ready() {
                    format!("queued: {}", agent.agent_status)
                } else if item.attempts >= 3 {
                    "reminders exhausted".to_string()
                } else if item.next_wake > time {
                    "waiting for reminder deadline".to_string()
                } else {
                    wake(store, socket, &binding, time)
                        .await
                        .unwrap_or_else(|e| format!("wake uncertain: {e:#}"))
                }
            } else {
                "participant absent; rebind explicitly".to_string()
            };
            observations.push(Observation::for_inbox(item, state));
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
                    state: format!("operator notification uncertain: {e:#}"),
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
) -> Result<String> {
    let Some(target) = binding.binding.herdr() else {
        return Ok("standalone participant; availability unknown; use context".into());
    };
    let live = herdr::agent(socket, &target.pane).await?;
    if !live.matches(binding) || !live.ready() {
        return Ok("state changed; queued".into());
    }
    if !herdr::plugin_enabled(socket).await? {
        return Ok("plugin disabled or unlinked".into());
    }
    if !store.reserve(binding, time).await? {
        return Ok("reservation no longer eligible".into());
    }
    // Group names are validated ASCII identifiers; no message content enters the prompt.
    let text = format!(
        "Agent Mail: work or mail changed. Run agent-mail context --group {}. Handle relevant obligations.",
        binding.group_name
    );
    anyhow::ensure!(text.len() <= 160, "wake-up exceeds size limit");
    herdr::call(
        socket,
        "agent.prompt",
        json!({"target": target.pane, "text": text}),
    )
    .await?;
    Ok("wake-up submitted; messages remain pending".into())
}

pub async fn tick(store: &Store, time: i64) -> Result<Vec<Observation>> {
    let mut pending = Vec::new();
    let mut observations = crate::codex::tick(store, time).await?;
    for item in store.pending().await? {
        let mailbox = store.mailbox(&item.group_name, &item.name).await?;
        if matches!(mailbox.binding, Binding::Standalone { .. }) {
            if store.has_codex(&mailbox).await? {
                continue;
            }
            let state = if item.due <= time {
                "overdue; standalone availability unknown; check context"
            } else {
                "standalone availability unknown; delivery requires client hooks or an explicit adapter"
            };
            observations.push(Observation::for_inbox(&item, state));
        } else {
            pending.push(item);
        }
    }
    let mut sessions: BTreeMap<String, Vec<Group>> = BTreeMap::new();
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

async fn relay_tick(store: &Store) -> Result<Vec<Value>> {
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
                Ok(machine) => store.sync_peer(machine).await,
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

pub async fn run(store: &Store, once: bool) -> Result<()> {
    let guard = WorkerLock::acquire(&store.root)?;
    let stream = if once {
        None
    } else {
        Some(crate::stream::Server::start(store.clone())?)
    };
    let mut last_relay = None::<tokio::time::Instant>;
    let mut relay_jobs = JoinSet::<Result<Vec<Value>>>::new();
    let mut relay_report = Vec::new();
    loop {
        let time = now()?;
        let result = tick(store, time).await;
        if relay_jobs.is_empty()
            && last_relay.is_none_or(|last| last.elapsed() >= Duration::from_secs(30))
        {
            let store = store.clone();
            relay_jobs.spawn(async move { relay_tick(&store).await });
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
        let path = store.root.join("service-status.tmp");
        use std::io::Write;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&bytes)?;
        std::fs::rename(path, store.root.join("service-status.json"))?;
        if once {
            println!("{}", String::from_utf8(bytes)?);
            break;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => {},
            () = async { if let Some(server)=&stream { server.changed.notified().await } else { std::future::pending::<()>().await } } => {},
            result = tokio::signal::ctrl_c() => { result?; break; }
        }
    }
    relay_jobs.abort_all();
    while relay_jobs.join_next().await.is_some() {}
    drop(guard);
    Ok(())
}
