//! Paired product regressions using an isolated database and an explicit Herdr fixture.
//! This is real Store/verification behavior, not a genuine native-runtime witness.
mod support;

use agent_mail::{
    PLUGIN_ID,
    followup::{Mode, PolicyPatch},
    herdr::{Agent, AgentStatus, Session},
    service::{self, WorkerLock},
    states::{DeliveryReadiness, SessionKind, TaskState},
    store::{Mailbox, Store},
    verification,
    work::WorkDraft,
};
use anyhow::Result;
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::Mutex,
    task::JoinHandle,
};

struct Host {
    agent: Agent,
    prompts: Vec<String>,
}

struct Fixture {
    _temp: TempDir,
    store: Store,
    _worker: WorkerLock,
    owner: Mailbox,
    writer: Mailbox,
    host: Arc<Mutex<Host>>,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix("runtime-verification-")
            .tempdir_in("/tmp")?;
        let socket = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket)?;
        let agent = Agent {
            pane_id: "w1:p1".into(),
            terminal_id: "fixture-terminal".into(),
            agent: Some("codex".into()),
            agent_session: Some(Session {
                agent: "codex".into(),
                kind: SessionKind::Id,
                value: "fixture-session".into(),
            }),
            agent_status: AgentStatus::Idle,
            interactive_ready: Some(true),
            launch_pending: false,
            cwd: None,
        };
        let host = Arc::new(Mutex::new(Host {
            agent: agent.clone(),
            prompts: Vec::new(),
        }));
        let shared = Arc::clone(&host);
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let shared = Arc::clone(&shared);
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut line = String::new();
                    if BufReader::new(read).read_line(&mut line).await.is_err() {
                        return;
                    }
                    let Ok(request) = serde_json::from_str::<Value>(&line) else {
                        return;
                    };
                    let mut host = shared.lock().await;
                    let result = match request["method"].as_str().unwrap_or_default() {
                        "agent.get" => json!({"type":"agent_info","agent":host.agent}),
                        "agent.list" => json!({"type":"agent_list","agents":[host.agent]}),
                        "plugin.list" => {
                            json!({"plugins":[{"plugin_id":PLUGIN_ID,"enabled":true}]})
                        }
                        "agent.prompt" => {
                            host.prompts.push(
                                request["params"]["text"]
                                    .as_str()
                                    .expect("prompt text")
                                    .to_owned(),
                            );
                            json!({"type":"ok"})
                        }
                        "notification.show" => json!({"type":"ok"}),
                        method => panic!("unexpected fixture RPC {method}"),
                    };
                    drop(host);
                    let response = format!("{}\n", json!({"id":request["id"],"result":result}));
                    let _ = write.write_all(response.as_bytes()).await;
                });
            }
        });
        let store = Store::open(&temp.path().join("state"), true).await?;
        store.enroll("g", Some(&socket)).await?;
        store.set_auto_prompt("g", true).await?;
        // Keep unrelated follow-up reminders from creating another pending event.
        store
            .patch_followups(
                "g",
                &PolicyPatch {
                    mode: Some(Mode::Observe),
                    ..Default::default()
                },
                1000,
            )
            .await?;
        store.bind("g", "worker", &agent, false).await?;
        store.register("g", "writer", false).await?;
        let owner = store.mailbox("g", "worker").await?;
        let writer = store.mailbox("g", "writer").await?;
        let worker = WorkerLock::acquire(store.root())?;
        Ok(Self {
            _temp: temp,
            store,
            _worker: worker,
            owner,
            writer,
            host,
            server,
        })
    }

    async fn assign(&self) -> Result<()> {
        self.store
            .work_create(
                &self.writer,
                WorkDraft {
                    id: "unfinished".into(),
                    scope: "Inspect one immutable report".into(),
                    owner: self.owner.name.clone(),
                    state: TaskState::Active,
                    next_action: "Inspect the report".into(),
                    deadline: None,
                    evidence: Vec::new(),
                },
                1000,
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn idle_probe_positive_control_preserves_explicit_ack() -> Result<()> {
    let f = Fixture::new().await?;
    verification::reconcile(&f.store, 1000).await?;
    let status = f.store.delivery_status(&f.owner, 1000).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    assert_eq!(status.attempts, 1);
    assert_eq!(status.transport_accepted_at, Some(1000));
    assert!(!status.agent_acknowledged);
    assert!(!status.ready);
    assert_eq!(status.deadline, Some(1180));
    // Never extract or manufacture an acknowledgment for this fixture.
    Ok(())
}

#[tokio::test]
async fn busy_before_first_dispatch_does_not_expire_challenge() -> Result<()> {
    let f = Fixture::new().await?;
    f.host.lock().await.agent.agent_status = AgentStatus::Working;
    verification::reconcile(&f.store, 1000).await?;
    verification::reconcile(&f.store, 1181).await?;
    let waiting = f.store.delivery_status(&f.owner, 1181).await?;
    assert_eq!(waiting.attempts, 0);
    assert!(f.host.lock().await.prompts.is_empty());
    eprintln!("busy-before-send: {}", serde_json::to_string(&waiting)?);
    f.host.lock().await.agent.agent_status = AgentStatus::Idle;
    verification::reconcile(&f.store, 1182).await?;
    let sent = f.store.delivery_status(&f.owner, 1182).await?;
    eprintln!("idle-after-wait: {}", serde_json::to_string(&sent)?);
    assert_eq!(
        f.host.lock().await.prompts.len(),
        1,
        "first eligible dispatch was suppressed by an unsent challenge deadline"
    );
    assert_eq!(sent.attempts, 1);
    assert_eq!(sent.deadline, Some(1362));
    assert!(!sent.agent_acknowledged);
    Ok(())
}

#[tokio::test]
async fn retrieved_unfinished_work_does_not_starve_idle_probe() -> Result<()> {
    let f = Fixture::new().await?;
    f.assign().await?;
    // Ordinary public retrieval receipts the notification, leaving the work active.
    let work = f.store.work_show(&f.owner, "unfinished").await?;
    assert_eq!(work.state, TaskState::Active);
    let pool = support::pool(&f.store).await?;
    let raw: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wake_events WHERE recipient=?")
        .bind(f.owner.id)
        .fetch_one(&pool)
        .await?;
    let undelivered: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM herdr_wake_events WHERE recipient=?")
            .bind(f.owner.id)
            .fetch_one(&pool)
            .await?;
    assert!(raw > 0, "control must retain actionable business work");
    assert_eq!(undelivered, 0, "control must receipt all delivery events");
    verification::reconcile(&f.store, 1001).await?;
    let status = f.store.delivery_status(&f.owner, 1001).await?;
    eprintln!("retrieved-active-work: {}", serde_json::to_string(&status)?);
    assert_eq!(
        f.host.lock().await.prompts.len(),
        1,
        "retrieved business obligation starved a separate idle verification dispatch"
    );
    assert_eq!(status.attempts, 1);
    assert!(!status.agent_acknowledged);
    assert_eq!(
        f.store.work_show(&f.owner, "unfinished").await?.state,
        TaskState::Active
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn normal_work_wake_positive_control_is_not_business_completion() -> Result<()> {
    let f = Fixture::new().await?;
    f.assign().await?;
    let observations = service::tick(&f.store, 1001).await?;
    eprintln!("normal-wake: {}", serde_json::to_string(&observations)?);
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    let status = f.store.delivery_status(&f.owner, 1001).await?;
    assert!(!status.agent_acknowledged);
    assert_ne!(status.state, DeliveryReadiness::Verified);
    assert_eq!(
        f.store.work_show(&f.owner, "unfinished").await?.state,
        TaskState::Active
    );
    Ok(())
}
