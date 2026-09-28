//! Protocol and process-boundary tests using disposable databases and a fake Herdr socket.
use agent_mail::work::{WorkDraft, WorkPatch};
use agent_mail::{
    PLUGIN_ID,
    herdr::{Agent, Session},
    service,
    store::{DatabaseGuard, Mailbox, Publish, Store},
};
use anyhow::Result;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::Mutex,
    task::JoinHandle,
};

#[derive(Default)]
struct Host {
    agents: Vec<Agent>,
    prompts: Vec<String>,
    notifications: usize,
    enabled: bool,
    drop_prompt_response: bool,
}

struct Fixture {
    _temp: TempDir,
    store: Store,
    _guard: DatabaseGuard,
    socket: PathBuf,
    host: Arc<Mutex<Host>>,
    server: JoinHandle<()>,
    a: Mailbox,
    b: Mailbox,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn agent(pane: &str) -> Agent {
    Agent {
        pane_id: pane.into(),
        terminal_id: format!("terminal-{pane}"),
        agent: Some("codex".into()),
        agent_session: Some(Session {
            agent: "codex".into(),
            kind: "id".into(),
            value: format!("session-{pane}"),
        }),
        agent_status: "idle".into(),
        interactive_ready: true,
        launch_pending: false,
        cwd: None,
    }
}

impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix("agent-mail-")
            .tempdir_in("/tmp")?;
        let socket = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket)?;
        let agents = vec![agent("w1:p1"), agent("w1:p2")];
        let host = Arc::new(Mutex::new(Host {
            agents: agents.clone(),
            enabled: true,
            ..Host::default()
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
                        "agent.get" => {
                            let target = request["params"]["target"].as_str().unwrap_or_default();
                            json!({"type":"agent_info","agent":host.agents.iter().find(|a| a.pane_id==target)})
                        }
                        "agent.list" => json!({"type":"agent_list","agents":host.agents}),
                        "plugin.list" => {
                            json!({"plugins":[{"plugin_id":PLUGIN_ID,"enabled":host.enabled}]})
                        }
                        "agent.prompt" => {
                            host.prompts.push(
                                request["params"]["text"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_string(),
                            );
                            if host.drop_prompt_response {
                                return;
                            }
                            json!({"type":"ok"})
                        }
                        "notification.show" => {
                            host.notifications += 1;
                            json!({"type":"ok"})
                        }
                        _ => json!({"type":"ok"}),
                    };
                    drop(host);
                    let response = format!("{}\n", json!({"id":request["id"],"result":result}));
                    let _ = write.write_all(response.as_bytes()).await;
                });
            }
        });
        let root = temp.path().join("state");
        let (initial, setup_guard) = Store::open(&root, true).await?;
        initial.enroll("g", socket.to_str().unwrap()).await?;
        initial.set_auto_prompt("g", true).await?;
        initial.bind("g", "a", &agents[0], false).await?;
        initial.bind("g", "b", &agents[1], false).await?;
        initial.pool.close().await;
        drop(setup_guard);
        let (store, guard) = Store::open(&root, false).await?;
        let a = store.mailbox("g", "a").await?;
        let b = store.mailbox("g", "b").await?;
        Ok(Self {
            _temp: temp,
            store,
            _guard: guard,
            socket,
            host,
            server,
            a,
            b,
        })
    }

    async fn send(&self, key: &str) -> Result<i64> {
        self.store.publish(&self.a, message(key), 1000).await
    }

    async fn cli(&self, pane: &str, args: &[&str]) -> Result<std::process::Output> {
        Ok(
            tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
                .args(["--state-dir", self.store.root.to_str().unwrap()])
                .args(args)
                .env("HERDR_ENV", "1")
                .env("HERDR_PANE_ID", pane)
                .env("HERDR_SOCKET_PATH", &self.socket)
                .env_remove("HERDR_PLUGIN_ID")
                .output()
                .await?,
        )
    }
}

fn message(key: &str) -> Publish {
    Publish {
        recipients: vec!["b".into()],
        key: key.into(),
        summary: "Inspect the contract".into(),
        body: "Durable body".into(),
        due_after: 900,
        reply_to: None,
        work_id: None,
    }
}

#[tokio::test]
async fn publish_is_durable_and_retry_safe() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("once").await?;
    let (reopened, second_guard) = Store::open(&f.store.root, false).await?;
    assert_eq!(reopened.message(&f.b, id).await?.body, "Durable body");
    assert_eq!(reopened.publish(&f.a, message("once"), 5000).await?, id);
    assert_eq!(reopened.inbox(&f.b, 0).await?.len(), 1);
    let mut changed = message("once");
    changed.body = "Different".into();
    assert!(reopened.publish(&f.a, changed, 5000).await.is_err());
    reopened.pool.close().await;
    drop(second_guard);
    Ok(())
}

#[tokio::test]
async fn fanout_is_all_or_nothing() -> Result<()> {
    let f = Fixture::new().await?;
    let mut p = message("fanout");
    p.recipients = vec!["b".into(), "missing".into()];
    assert!(f.store.publish(&f.a, p, 1000).await.is_err());
    assert!(f.store.inbox(&f.b, 0).await?.is_empty());
    assert_eq!(f.send("fanout").await?, 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_retries_publish_once() -> Result<()> {
    let f = Fixture::new().await?;
    let (a, b) = tokio::join!(f.send("same"), f.send("same"));
    assert_eq!(a?, b?);
    assert_eq!(f.store.inbox(&f.b, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn reply_and_resolution_are_atomic_and_idempotent() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("ask").await?;
    let reply = Some(("answer".to_string(), "Reviewed; see report.md".to_string()));
    let first = f
        .store
        .resolve(&f.b, id, "answered", reply.clone(), 1001)
        .await?;
    assert_eq!(
        first,
        f.store.resolve(&f.b, id, "answered", reply, 3000).await?
    );
    assert!(f.store.inbox(&f.b, 0).await?.is_empty());
    assert_eq!(f.store.inbox(&f.a, 0).await?.len(), 1);
    assert!(
        f.store
            .resolve(&f.b, id, "changed", None, 3000)
            .await
            .is_err()
    );
    let bad = f.send("bad").await?;
    assert!(
        f.store
            .resolve(
                &f.b,
                bad,
                "answer",
                Some(("oversized".into(), "x".repeat(8193))),
                1002
            )
            .await
            .is_err()
    );
    assert_eq!(f.store.message(&f.b, bad).await?.state, "pending");
    Ok(())
}

#[tokio::test]
async fn withdrawal_does_not_erase_history() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("withdraw").await?;
    assert!(f.store.withdraw(&f.b, id).await.is_err());
    f.store.withdraw(&f.a, id).await?;
    assert!(f.store.inbox(&f.b, 0).await?.is_empty());
    assert_eq!(f.store.message(&f.b, id).await?.state, "withdrawn");
    assert_eq!(f.send("withdraw").await?, id);
    assert!(
        f.store
            .resolve(&f.b, id, "too late", None, 1001)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn groups_and_rebound_identities_are_isolated() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("isolated").await?;
    f.store.enroll("other", f.socket.to_str().unwrap()).await?;
    f.store.bind("other", "b", &agent("w1:p2"), false).await?;
    let other = f.store.mailbox("other", "b").await?;
    assert!(f.store.message(&other, id).await.is_err());
    let mut replacement = agent("w1:p2");
    replacement.agent_session.as_mut().unwrap().value = "replacement".into();
    assert!(f.store.bind("g", "b", &replacement, false).await.is_err());
    f.store.bind("g", "b", &replacement, true).await?;
    assert!(
        f.store
            .resolve(&f.b, id, "old owner", None, 1001)
            .await
            .is_err()
    );
    assert!(
        f.store.message(&f.b, id).await.is_err(),
        "old binding must not read after rebind"
    );
    Ok(())
}

#[tokio::test]
async fn bursts_batch_and_retry_budget_survives_restarts() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..20 {
        f.send(&format!("burst-{i}")).await?;
    }
    service::tick(&f.store, 1000).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    service::tick(&f.store, 1001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    service::tick(&f.store, 1300).await?;
    service::tick(&f.store, 1600).await?;
    f.send("new-arrival").await?;
    let (reopened, second_guard) = Store::open(&f.store.root, false).await?;
    service::tick(&reopened, 5000).await?;
    let host = f.host.lock().await;
    assert_eq!(host.prompts.len(), 3);
    assert!(
        host.prompts
            .iter()
            .all(|p| p.len() <= 160 && !p.contains("Durable body"))
    );
    assert_eq!(host.notifications, 1);
    drop(host);
    reopened.pool.close().await;
    drop(second_guard);
    Ok(())
}

#[tokio::test]
async fn ambiguous_prompt_consumes_attempt_and_waits() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("ambiguous").await?;
    f.host.lock().await.drop_prompt_response = true;
    service::tick(&f.store, 1000).await?;
    service::tick(&f.store, 1001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 1);
    Ok(())
}

#[tokio::test]
async fn blocked_working_missing_and_disabled_agents_are_not_prompted() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("held").await?;
    for status in ["working", "blocked", "unknown"] {
        f.host.lock().await.agents[1].agent_status = status.into();
        service::tick(&f.store, 1000).await?;
    }
    f.host.lock().await.agents[1].agent_status = "idle".into();
    f.host.lock().await.agents[1].agent_session = None;
    service::tick(&f.store, 1000).await?;
    f.host.lock().await.agents[1] = agent("w1:p2");
    f.host.lock().await.enabled = false;
    service::tick(&f.store, 1000).await?;
    assert!(f.host.lock().await.prompts.is_empty());
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 0);
    Ok(())
}

#[tokio::test]
async fn backward_clock_and_sleep_do_not_burst() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("clock").await?;
    service::tick(&f.store, 1000).await?;
    service::tick(&f.store, 100).await?;
    service::tick(&f.store, 90000).await?;
    service::tick(&f.store, 90001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    Ok(())
}

#[tokio::test]
async fn pause_and_empty_inbox_reset_are_explicit() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("pause").await?;
    f.store.pause("g", true).await?;
    service::tick(&f.store, 1000).await?;
    assert!(f.host.lock().await.prompts.is_empty());
    f.store.pause("g", false).await?;
    service::tick(&f.store, 1000).await?;
    f.store.resolve(&f.b, id, "done", None, 1001).await?;
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 0);
    f.send("again").await?;
    service::tick(&f.store, 1002).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    Ok(())
}

#[tokio::test]
async fn singleton_and_schema_locks_are_released() -> Result<()> {
    let f = Fixture::new().await?;
    let lock = service::WorkerLock::acquire(&f.store.root)?;
    assert!(service::running(&f.store.root));
    assert!(service::WorkerLock::acquire(&f.store.root).is_err());
    assert!(Store::open(&f.store.root, true).await.is_err());
    drop(lock);
    assert!(!service::running(&f.store.root));
    Ok(())
}

#[tokio::test]
async fn cli_roundtrip_bounds_identity_and_restart() -> Result<()> {
    let f = Fixture::new().await?;
    let sent = f
        .cli(
            "w1:p1",
            &[
                "send",
                "--group",
                "g",
                "--to",
                "b",
                "--key",
                "cli",
                "--summary",
                "Check report",
            ],
        )
        .await?;
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    let id = serde_json::from_slice::<Value>(&sent.stdout)?["id"].to_string();
    let inbox = f.cli("w1:p2", &["inbox", "--group", "g"]).await?;
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    assert!(inbox.stdout.len() <= 2048);
    let full = f.cli("w1:p2", &["inbox", "--group", "g", &id]).await?;
    assert!(
        full.status.success(),
        "{}",
        String::from_utf8_lossy(&full.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&full.stdout)?["summary"],
        "Check report"
    );
    let done = f.cli("w1:p2", &["resolve", "--group", "g", &id]).await?;
    assert!(
        done.status.success(),
        "{}",
        String::from_utf8_lossy(&done.stderr)
    );
    let replay = f
        .cli(
            "w1:p1",
            &[
                "send",
                "--group",
                "g",
                "--to",
                "b",
                "--key",
                "cli",
                "--summary",
                "Check report",
            ],
        )
        .await?;
    assert_eq!(sent.stdout, replay.stdout);
    let empty = f.cli("w1:p2", &["inbox", "--group", "g"]).await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&empty.stdout)?["items"],
        json!([])
    );
    f.host.lock().await.agents[1]
        .agent_session
        .as_mut()
        .unwrap()
        .value = "different".into();
    assert!(
        !f.cli("w1:p2", &["inbox", "--group", "g"])
            .await?
            .status
            .success()
    );
    Ok(())
}

#[tokio::test]
async fn cli_worker_crash_keeps_messages_and_reservations() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("crash").await?;
    let mut worker = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .args([
            "--state-dir",
            f.store.root.to_str().unwrap(),
            "service",
            "run",
        ])
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while f.host.lock().await.prompts.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    worker.kill().await?;
    worker.wait().await?;
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 1);
    assert_eq!(f.store.inbox(&f.b, 0).await?.len(), 1);
    let again = f.cli("w1:p1", &["service", "run", "--once"]).await?;
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    Ok(())
}

#[tokio::test]
async fn missing_database_does_not_get_recreated() -> Result<()> {
    let temp = tempfile::tempdir()?;
    assert!(Store::open(temp.path(), false).await.is_err());
    assert!(!temp.path().join("mail.db").exists());
    Ok(())
}

#[tokio::test]
async fn work_writer_versions_and_mail_links_survive_restart() -> Result<()> {
    let f = Fixture::new().await?;
    let item = f
        .store
        .work_create(
            &f.a,
            WorkDraft {
                id: "lane-api".into(),
                scope: "Implement the API".into(),
                owner: "b".into(),
                state: "implementing".into(),
                next_action: "Produce evidence".into(),
                deadline: Some(2000),
                evidence: vec!["commit:abc".into()],
            },
            1000,
        )
        .await?;
    assert_eq!(item.version, 1);
    assert!(
        f.store
            .work_update(
                &f.b,
                "lane-api",
                1,
                WorkPatch::default(),
                "not writer",
                1001
            )
            .await
            .is_err()
    );
    let update = f
        .store
        .work_update(
            &f.a,
            "lane-api",
            1,
            WorkPatch {
                state: Some("review".into()),
                next_action: Some("Review commit abc".into()),
                ..WorkPatch::default()
            },
            "implementation submitted",
            1002,
        )
        .await?;
    assert_eq!(update.version, 2);
    assert!(
        f.store
            .work_update(&f.a, "lane-api", 1, WorkPatch::default(), "stale", 1003)
            .await
            .is_err()
    );
    let mut linked = message("linked");
    linked.work_id = Some("lane-api".into());
    let id = f.store.publish(&f.a, linked, 1003).await?;
    assert_eq!(
        f.store.message(&f.b, id).await?.work_id.as_deref(),
        Some("lane-api")
    );
    f.store.resolve(&f.b, id, "reviewed", None, 1004).await?;
    let (reopened, _guard) = Store::open(&f.store.root, false).await?;
    let current = reopened.work_show(&f.b, "lane-api").await?;
    assert_eq!(current.state, "review");
    assert!(current.open);
    assert_eq!(current.accepted_revision, None);
    assert_eq!(reopened.work_history(&f.a, "lane-api").await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn context_is_bounded_and_reveals_owned_work_and_mail() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..8 {
        f.store
            .work_create(
                &f.a,
                WorkDraft {
                    id: format!("lane-{i}"),
                    scope: "A bounded scope".into(),
                    owner: "b".into(),
                    state: "active".into(),
                    next_action: "Produce the next revision".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1000,
            )
            .await?;
        f.send(&format!("mail-{i}")).await?;
    }
    let output = f.cli("w1:p2", &["context", "--group", "g"]).await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() <= 4097);
    let view: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(view["group"], "g");
    assert!(
        view["work"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    assert!(
        view["mail"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    assert_eq!(view["work_more"], true);
    assert_eq!(view["mail_more"], true);
    Ok(())
}

#[tokio::test]
async fn safe_default_holds_unguarded_prompts_without_losing_mail() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.set_auto_prompt("g", false).await?;
    f.send("held-for-input-safety").await?;
    let observations = service::tick(&f.store, 1000).await?;
    assert!(f.host.lock().await.prompts.is_empty());
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 0);
    assert_eq!(f.store.inbox(&f.b, 0).await?.len(), 1);
    assert!(
        observations
            .iter()
            .any(|o| o.state.contains("automatic agent prompts disabled"))
    );
    Ok(())
}
