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

#[cfg(test)]
mod wake_writer_tests {
    use super::*;
    use crate::{
        herdr::{Agent, AgentStatus, Session},
        states::SessionKind,
        store::{Mailbox, Publish},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
        sync::Notify,
    };

    #[derive(Clone, Copy)]
    enum Scenario {
        SlowChecks,
        AmbiguousPrompt,
        ChangedBeforePrompt,
        ChangedAfterPrompt,
        Healthy,
    }
    struct Fixture {
        _temp: tempfile::TempDir,
        store: Store,
        socket: PathBuf,
        target: Mailbox,
        entered: Arc<Notify>,
        prompts: Arc<AtomicUsize>,
        server: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }
    impl Fixture {
        async fn new(scenario: Scenario) -> Result<Self> {
            let temp = tempfile::Builder::new()
                .prefix("wake-writer-")
                .tempdir_in("/tmp")?;
            let socket = temp.path().join("herdr.sock");
            let listener = UnixListener::bind(&socket)?;
            let live = Agent {
                pane_id: "w:p".into(),
                terminal_id: "terminal".into(),
                agent: Some("codex".into()),
                agent_session: Some(Session {
                    agent: "codex".into(),
                    kind: SessionKind::Id,
                    value: "session".into(),
                }),
                agent_status: AgentStatus::Idle,
                interactive_ready: Some(true),
                launch_pending: false,
                cwd: None,
            };
            let entered = Arc::new(Notify::new());
            let prompts = Arc::new(AtomicUsize::new(0));
            let calls = Arc::new(AtomicUsize::new(0));
            let (signal, sent, agent) = (entered.clone(), prompts.clone(), live.clone());
            let server = tokio::spawn(async move {
                let mut clients = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted=listener.accept()=>{
                            let Ok((stream,_))=accepted else {break};
                            let (signal,sent,calls,mut agent)=(signal.clone(),sent.clone(),calls.clone(),agent.clone());
                            clients.spawn(async move {
                                let (read,mut write)=stream.into_split();
                                let mut line=String::new();
                                if BufReader::new(read).read_line(&mut line).await.is_err() {return;}
                                let Ok(request)=serde_json::from_str::<Value>(&line) else {return};
                                let index=calls.fetch_add(1,Ordering::SeqCst);
                                if index==2 {signal.notify_one();}
                                let method=request["method"].as_str().unwrap_or_default();
                                if matches!(scenario,Scenario::SlowChecks) && index>=2 {
                                    tokio::time::sleep(Duration::from_millis(1100)).await;
                                }
                                let result=match method {
                                    "agent.get"=>{
                                        if (matches!(scenario,Scenario::ChangedBeforePrompt) && index>=2)
                                            || (matches!(scenario,Scenario::ChangedAfterPrompt) && sent.load(Ordering::SeqCst)>0) {
                                            agent.agent_session.as_mut().expect("fixture session").value="replacement".into();
                                        }
                                        json!({"type":"agent_info","agent":agent})
                                    },
                                    "plugin.list"=>json!({"plugins":[{"plugin_id":crate::PLUGIN_ID,"enabled":true}]}),
                                    "agent.prompt"=>{
                                        sent.fetch_add(1,Ordering::SeqCst);
                                        if matches!(scenario,Scenario::AmbiguousPrompt) {
                                            tokio::time::sleep(Duration::from_secs(5)).await;
                                        }
                                        json!({"type":"ok"})
                                    },
                                    _=>json!({"type":"ok"}),
                                };
                                let response=format!("{}\n",json!({"id":request["id"],"result":result}));
                                let _=write.write_all(response.as_bytes()).await;
                            });
                        },
                        _=clients.join_next(),if !clients.is_empty()=>{},
                    }
                }
            });
            let store = Store::open(&temp.path().join("state"), true).await?;
            store.enroll("g", Some(&socket)).await?;
            store.set_auto_prompt("g", true).await?;
            store.bind("g", "target", &live, false).await?;
            let credential = store.register("g", "writer", false).await?;
            let writer = store.authenticate("g", Some(&credential)).await?;
            store
                .publish(
                    &writer,
                    Publish {
                        recipients: vec!["target".into()],
                        key: "wake".into(),
                        summary: "actual bounded wake".into(),
                        body: "writer control".into(),
                        due_after: Some(900),
                        reply_to: None,
                        work_id: None,
                    },
                    1000,
                )
                .await?;
            let target = store.mailbox("g", "target").await?;
            Ok(Self {
                _temp: temp,
                store,
                socket,
                target,
                entered,
                prompts,
                server,
            })
        }
        async fn reservation(&self) -> Result<String> {
            Ok(sqlx::query_scalar("SELECT json_object('wake_attempts',b.attempts,'next_wake',b.next_wake,'attempts',p.attempts,'deadline',p.deadline,'nonce',p.nonce,'accepted',p.transport_accepted_at,'ack',p.acknowledged_at) FROM mailboxes b JOIN delivery_probes p ON p.recipient=b.id WHERE b.id=?")
                .bind(self.target.id).fetch_one(self.store.pool()).await?)
        }
        async fn assert_uncertain(&self) -> Result<()> {
            let value: Value = serde_json::from_str(&self.reservation().await?)?;
            assert_eq!(value["wake_attempts"], 1);
            assert_eq!(value["next_wake"], 1300);
            assert_eq!(value["attempts"], 1);
            assert!(
                value["deadline"]
                    .as_i64()
                    .is_some_and(|deadline| deadline > 1000)
            );
            assert!(value["accepted"].is_null() && value["ack"].is_null());
            Ok(())
        }
    }

    #[tokio::test]
    async fn aggregate_writer_deadline_releases_for_unrelated_writer() -> Result<()> {
        let f = Fixture::new(Scenario::SlowChecks).await?;
        let writer = async {
            tokio::time::timeout(Duration::from_secs(5), f.entered.notified()).await?;
            let started = tokio::time::Instant::now();
            tokio::time::timeout(
                Duration::from_millis(2800),
                f.store.enroll("unrelated", None),
            )
            .await??;
            Ok::<_, anyhow::Error>(started.elapsed())
        };
        let (wake_result, writer_result) =
            tokio::join!(wake(&f.store, &f.socket, &f.target, 1000), writer);
        assert!(
            wake_result.is_err(),
            "aggregate budget must end individually bounded slow RPCs"
        );
        assert!(writer_result? < Duration::from_millis(2800));
        assert_eq!(f.prompts.load(Ordering::SeqCst), 0);
        f.assert_uncertain().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_wake_releases_writer_without_refunding_reservations() -> Result<()> {
        let f = Fixture::new(Scenario::SlowChecks).await?;
        tokio::time::timeout(Duration::from_secs(5),async {
            let request=wake(&f.store,&f.socket,&f.target,1000);
            tokio::pin!(request);
            tokio::select! {
                _=f.entered.notified()=>{},
                result=&mut request=>panic!("wake returned before held-writer cancellation: {result:?}"),
            }
            // Dropping the actual in-flight wake also drops its transaction.
        }).await?;
        tokio::time::timeout(
            Duration::from_millis(2800),
            f.store.enroll("after-cancellation", None),
        )
        .await??;
        f.assert_uncertain().await?;
        assert_eq!(f.prompts.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_send_timeout_keeps_both_committed_reservations() -> Result<()> {
        let f = Fixture::new(Scenario::AmbiguousPrompt).await?;
        let started = tokio::time::Instant::now();
        assert!(wake(&f.store, &f.socket, &f.target, 1000).await.is_err());
        assert!(started.elapsed() < Duration::from_millis(2800));
        assert_eq!(
            f.prompts.load(Ordering::SeqCst),
            1,
            "possible exposure is real"
        );
        f.assert_uncertain().await?;
        let retained = f.reservation().await?;
        assert!(matches!(
            wake(&f.store, &f.socket, &f.target, 1001).await?,
            DeliveryState::Ineligible
        ));
        assert_eq!(
            f.reservation().await?,
            retained,
            "timeout cannot refund or renew either reservation"
        );
        assert_eq!(f.prompts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn route_change_before_or_after_prompt_never_records_acceptance() -> Result<()> {
        for (scenario, prompts) in [
            (Scenario::ChangedBeforePrompt, 0),
            (Scenario::ChangedAfterPrompt, 1),
        ] {
            let f = Fixture::new(scenario).await?;
            assert!(matches!(
                wake(&f.store, &f.socket, &f.target, 1000).await?,
                DeliveryState::StateChanged
            ));
            assert_eq!(f.prompts.load(Ordering::SeqCst), prompts);
            f.assert_uncertain().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn healthy_bounded_wake_records_transport_without_agent_verification() -> Result<()> {
        let f = Fixture::new(Scenario::Healthy).await?;
        assert!(matches!(
            wake(&f.store, &f.socket, &f.target, 1000).await?,
            DeliveryState::Queued
        ));
        let value: Value = serde_json::from_str(&f.reservation().await?)?;
        assert_eq!(value["accepted"], 1000);
        assert!(value["ack"].is_null());
        assert_eq!(value["wake_attempts"], 1);
        assert_eq!(value["attempts"], 1);
        Ok(())
    }
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
    let mut tx = store.pool().begin().await?;
    Store::lock_actor(&mut tx, binding).await?;
    if !Store::reserve_tx(&mut tx, binding, time).await? {
        tx.commit().await?;
        return Ok(DeliveryState::Ineligible);
    }
    let route_key = Store::delivery_route_key_tx(&mut tx, binding)
        .await?
        .context("notification route unavailable")?;
    let challenge = Store::reserve_delivery_challenge_tx(&mut tx, binding, time).await?;
    let text = store
        .herdr_notification_text(binding, challenge.as_deref())
        .await?;
    // Both reservations commit before the potentially ambiguous transport call.
    tx.commit().await?;
    // All four RPCs share one writer budget, with 500ms reserved for
    // commit/rollback. Store's independent writers have a 3s busy timeout.
    let mut connection = store.pool().acquire().await?;
    // Cancellation must not return a possibly open transaction to the pool.
    connection.close_on_drop();
    let release_at = tokio::time::Instant::now() + Duration::from_secs(2);
    let transport_until = release_at - Duration::from_millis(500);
    let mut tx =
        tokio::time::timeout_at(transport_until, sqlx::Connection::begin(&mut *connection))
            .await
            .context("Herdr wake writer begin timed out")??;
    let outcome = tokio::time::timeout_at(transport_until, async {
        Store::lock_actor(&mut tx, binding).await?;
        if Store::delivery_route_key_tx(&mut tx, binding)
            .await?
            .as_deref()
            != Some(route_key.as_str())
        {
            return Ok(DeliveryState::StateChanged);
        }
        let live = herdr::agent(socket, &target.pane).await?;
        if !live.matches(binding) || !live.ready() {
            return Ok(DeliveryState::StateChanged);
        }
        if !herdr::plugin_enabled(socket).await? {
            return Ok(DeliveryState::PluginDisabled);
        }
        herdr::call(
            socket,
            "agent.prompt",
            json!({"target":target.pane,"text":text}),
        )
        .await?;
        // Herdr can replace a live session independently of the Mail binding writer.
        if !herdr::agent(socket, &target.pane).await?.matches(binding)
            || Store::delivery_route_key_tx(&mut tx, binding)
                .await?
                .as_deref()
                != Some(route_key.as_str())
        {
            return Ok(DeliveryState::StateChanged);
        }
        if let Some(nonce) = challenge.as_deref() {
            Store::record_delivery_challenge_tx(&mut tx, binding, nonce, time).await?;
        }
        Ok::<_, anyhow::Error>(DeliveryState::Queued)
    })
    .await;
    let commit = matches!(&outcome, Ok(Ok(DeliveryState::Queued)));
    let released = tokio::time::timeout_at(release_at, async {
        if commit {
            tx.commit().await
        } else {
            tx.rollback().await
        }
    })
    .await;
    if !matches!(&released, Ok(Ok(()))) {
        // Transaction cancellation queues rollback in SQLx's SQLite worker.
        // Discard the connection as well; it must never re-enter the pool.
        sqlx::Connection::close_hard(connection.detach()).await?;
        return match released {
            Err(error) => Err(anyhow::Error::new(error)
                .context("Herdr wake writer release timed out; delivery remains uncertain")),
            Ok(Err(error)) => Err(anyhow::Error::new(error)
                .context("Herdr wake writer release failed; delivery remains uncertain")),
            Ok(Ok(())) => unreachable!(),
        };
    }
    released.context("Herdr wake writer release timed out")??;
    outcome.context("Herdr wake writer transport timed out; delivery remains uncertain")?
}

/// Check native and Herdr delivery eligibility at the supplied Unix timestamp.
///
/// # Errors
/// Database access or a spawned session task fails.
pub async fn tick(store: &Store, time: i64) -> Result<Vec<Observation>> {
    // One notifier opportunity must remain reachable even when ordinary source
    // reconciliation is poisoned. Both actual errors remain visible.
    let notices = crate::followup::notify_operators(store, time).await;
    let reconciliation = crate::followup::reconcile(store, time).await;
    match (notices, reconciliation) {
        (Ok(()), Ok(())) => {}
        (Err(notice), Err(reconcile)) => {
            anyhow::bail!("operator notices: {notice:#}; followup reconciliation: {reconcile:#}")
        }
        (Err(error), Ok(())) => return Err(error.context("operator notices")),
        (Ok(()), Err(error)) => return Err(error.context("followup reconciliation")),
    }
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
    let mut lifecycle = crate::lifecycle::Monitors::default();
    let execution_controller = crate::execution_driver::Controller::acquire(store, now()?).await?;
    let mut execution_jobs: Vec<_> = crate::execution_driver::JobKind::ALL
        .into_iter()
        .map(|kind| (kind, JoinSet::<Result<()>>::new()))
        .collect();
    let (execution_status, execution_report) = tokio::sync::watch::channel(Value::Null);
    let service_result: Result<()> = async {
    loop {
        let time = now()?;
        for (kind,jobs) in &mut execution_jobs {
            while let Some(job) = jobs.try_join_next() {
                let error = match job {Ok(Ok(()))=>None,Ok(Err(error))=>Some(format!("{error:#}")),Err(error)=>Some(error.to_string())};
                if let Some(error)=error {
                    crate::execution_driver::publish_status(&execution_status,*kind,json!({"error":error.chars().take(2048).collect::<String>(),"external_closure":false}));
                }
            }
            if jobs.is_empty() {
                let state=store.clone();
                let controller=execution_controller.clone();
                let status=execution_status.clone();
                let kind=*kind;
                jobs.spawn(async move {
                    if once {
                        let report=controller.tick_job(&state,kind).await?;
                        crate::execution_driver::publish_status(&status,kind,serde_json::to_value(report)?);
                        Ok(())
                    } else { controller.run(&state,kind,status).await }
                });
            }
        }
        if !once {
            lifecycle.refresh(store).await?;
        }
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
        if once {
            for (kind,jobs) in &mut execution_jobs {
                if let Some(job)=jobs.join_next().await {
                    let error=match job {Ok(Ok(()))=>None,Ok(Err(error))=>Some(format!("{error:#}")),Err(error)=>Some(error.to_string())};
                    if let Some(error)=error {
                        crate::execution_driver::publish_status(&execution_status,*kind,json!({"error":error.chars().take(2048).collect::<String>(),"external_closure":false}));
                    }
                }
            }
        }
        let report = match result {
            Ok(observations) => {
                json!({"checked_at": time, "observations": observations,"relay":relay_report,"relay_running":!relay_jobs.is_empty(),"execution":execution_report.borrow().clone(),"execution_running":execution_jobs.iter().any(|(_,jobs)| !jobs.is_empty())})
            }
            Err(error) => {
                json!({"checked_at": time, "error": format!("{error:#}"),"relay":relay_report,"relay_running":!relay_jobs.is_empty(),"execution":execution_report.borrow().clone(),"execution_running":execution_jobs.iter().any(|(_,jobs)| !jobs.is_empty())})
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
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => {},
            () = async { if let Some(server)=&stream { server.changed().await } else { std::future::pending::<()>().await } } => {},
            result = tokio::signal::ctrl_c() => { result?; break; }
            _ = terminate.recv() => { break; }
            () = async { if let Some(server)=&stream { server.upgrade_requested().await } else { std::future::pending::<()>().await } } => { break; }
        }
    }
    Ok(())
    }.await;
    // Join cancelled controller jobs before releasing their lock or recording
    // controller shutdown. This says nothing about external worker containment.
    for (_, jobs) in &mut execution_jobs {
        jobs.abort_all();
    }
    for (_, jobs) in &mut execution_jobs {
        while jobs.join_next().await.is_some() {}
    }
    let execution_finished = execution_controller
        .finish(
            store,
            now()?,
            if service_result.is_ok() {
                "shutdown_joined"
            } else {
                "service_error_joined"
            },
        )
        .await;
    verification_jobs.abort_all();
    while verification_jobs.join_next().await.is_some() {}
    relay_jobs.abort_all();
    while relay_jobs.join_next().await.is_some() {}
    if let Some(server) = stream {
        server.shutdown().await?;
    }
    execution_finished?;
    service_result?;
    Ok(())
}
