//! Protocol and process-boundary tests using disposable databases and a fake Herdr socket.
mod support;
use agent_mail::work::{WorkDraft, WorkPatch};
use agent_mail::{
    PLUGIN_ID,
    herdr::{Agent, Session},
    service,
    store::{Mailbox, Publish, Store},
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
            kind: agent_mail::states::SessionKind::Id,
            value: format!("session-{pane}"),
        }),
        agent_status: agent_mail::herdr::AgentStatus::Idle,
        interactive_ready: Some(true),
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
        let initial = Store::open(&root, true).await?;
        initial.enroll("g", Some(&socket)).await?;
        initial.set_auto_prompt("g", true).await?;
        initial.bind("g", "a", &agents[0], false).await?;
        initial.bind("g", "b", &agents[1], false).await?;
        initial.close().await;
        let store = Store::open(&root, false).await?;
        let a = store.mailbox("g", "a").await?;
        let b = store.mailbox("g", "b").await?;
        Ok(Self {
            _temp: temp,
            store,
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
        self.cli_session(pane, args, None).await
    }

    async fn cli_session(
        &self,
        pane: &str,
        args: &[&str],
        session: Option<&str>,
    ) -> Result<std::process::Output> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        command
            .args(["--state-dir", self.store.root().to_str().unwrap()])
            .args(args)
            .env("HERDR_ENV", "1")
            .env("HERDR_PANE_ID", pane)
            .env("HERDR_SOCKET_PATH", &self.socket)
            .env_remove("CODEX_SESSION_ID")
            .env_remove("HERDR_PLUGIN_ID")
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_SESSION");
        if let Some(session) = session {
            command.env("AGENT_MAIL_SESSION", session);
        }
        Ok(command.output().await?)
    }
}

fn message(key: &str) -> Publish {
    Publish {
        recipients: vec!["b".into()],
        key: key.into(),
        summary: "Inspect the contract".into(),
        body: "Durable body".into(),
        due_after: Some(900),
        reply_to: None,
        work_id: None,
    }
}

#[tokio::test]
async fn publish_is_durable_and_retry_safe() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("once").await?;
    let reopened = Store::open(f.store.root(), false).await?;
    assert_eq!(reopened.message(&f.b, id).await?.body, "Durable body");
    assert_eq!(reopened.publish(&f.a, message("once"), 5000).await?, id);
    assert_eq!(reopened.inbox(&f.b, 0).await?.len(), 1);
    let mut changed = message("once");
    changed.body = "Different".into();
    assert!(reopened.publish(&f.a, changed, 5000).await.is_err());
    reopened.close().await;
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
    assert_eq!(
        f.store.message(&f.b, bad).await?.state,
        agent_mail::states::MessageState::Pending
    );
    Ok(())
}

#[tokio::test]
async fn withdrawal_does_not_erase_history() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("withdraw").await?;
    assert!(f.store.withdraw(&f.b, id, 1_000).await.is_err());
    f.store.withdraw(&f.a, id, 1_000).await?;
    assert!(f.store.inbox(&f.b, 0).await?.is_empty());
    assert_eq!(
        f.store.message(&f.b, id).await?.state,
        agent_mail::states::MessageState::Withdrawn
    );
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
    f.store.enroll("other", Some(&f.socket)).await?;
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
    assert!(f.host.lock().await.prompts[0].contains("agent ack"));
    let prompt = f.host.lock().await.prompts[0].clone();
    assert!(prompt.contains("act or checkpoint"));
    if let Some(line) = prompt.lines().find(|line| line.starts_with('{')) {
        let notice: Value = serde_json::from_str(line)?;
        assert!(notice["new_mail"].as_array().unwrap().len() <= 5);
        assert_eq!(notice["more"], true);
    } else {
        assert!(prompt.contains("agent-mail context"));
    }
    agent_mail::verification::reconcile(&f.store, 1000).await?;
    // The other registered lane has no pending notification, so it receives its
    // one bounded standalone probe. The busy target is challenged in its wake.
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    service::tick(&f.store, 1001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    service::tick(&f.store, 1300).await?;
    service::tick(&f.store, 1600).await?;
    f.send("new-arrival").await?;
    let reopened = Store::open(f.store.root(), false).await?;
    service::tick(&reopened, 5000).await?;
    let host = f.host.lock().await;
    assert_eq!(host.prompts.len(), 5);
    assert!(
        host.prompts
            .iter()
            .all(|p| p.len() <= 480 && !p.contains("Durable body"))
    );
    assert_eq!(host.notifications, 2);
    drop(host);
    reopened.close().await;
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
        f.host.lock().await.agents[1].agent_status =
            serde_json::from_value(serde_json::json!(status))?;
        service::tick(&f.store, 1000).await?;
    }
    f.host.lock().await.agents[1].agent_status = agent_mail::herdr::AgentStatus::Idle;
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
    let lock = service::WorkerLock::acquire(f.store.root())?;
    assert!(service::running(f.store.root()));
    assert!(service::WorkerLock::acquire(f.store.root()).is_err());
    assert!(Store::open(f.store.root(), true).await.is_err());
    drop(lock);
    assert!(!service::running(f.store.root()));
    Ok(())
}

#[tokio::test]
async fn cli_roundtrip_bounds_identity_and_restart() -> Result<()> {
    let f = Fixture::new().await?;
    let sent = f
        .cli(
            "w1:p1",
            &[
                "mail",
                "send",
                "b",
                "Check report",
                "--group",
                "g",
                "--key",
                "cli",
            ],
        )
        .await?;
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    let id = serde_json::from_slice::<Value>(&sent.stdout)?["id"].to_string();
    let inbox = f.cli("w1:p2", &["mail", "list", "--group", "g"]).await?;
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    assert!(inbox.stdout.len() <= 2048);
    let full = f
        .cli("w1:p2", &["mail", "show", "--group", "g", &id])
        .await?;
    assert!(
        full.status.success(),
        "{}",
        String::from_utf8_lossy(&full.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&full.stdout)?["summary"],
        "Check report"
    );
    let done = f
        .cli(
            "w1:p2",
            &["mail", "resolve", "--group", "g", &id, "--note", "handled"],
        )
        .await?;
    assert!(
        done.status.success(),
        "{}",
        String::from_utf8_lossy(&done.stderr)
    );
    let replay = f
        .cli(
            "w1:p1",
            &[
                "mail",
                "send",
                "b",
                "Check report",
                "--group",
                "g",
                "--key",
                "cli",
            ],
        )
        .await?;
    assert_eq!(sent.stdout, replay.stdout);
    let empty = f.cli("w1:p2", &["mail", "list", "--group", "g"]).await?;
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
        !f.cli("w1:p2", &["mail", "list", "--group", "g"])
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
            f.store.root().to_str().unwrap(),
            "service",
            "run",
        ])
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !f
            .host
            .lock()
            .await
            .prompts
            .iter()
            .any(|p| p.starts_with("Agent Mail changes:"))
        {
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
    assert_eq!(
        f.host
            .lock()
            .await
            .prompts
            .iter()
            .filter(|p| p.starts_with("Agent Mail changes:"))
            .count(),
        1
    );
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
                state: agent_mail::states::TaskState::Active,
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
            .update_work(
                &f.b,
                "lane-api",
                agent_mail::work::WorkUpdate {
                    version: 1,
                    patch: WorkPatch::default(),
                    reason: ("not writer").to_owned(),
                    resolve_message: None
                },
                1001
            )
            .await
            .is_err()
    );
    let update = f
        .store
        .update_work(
            &f.a,
            "lane-api",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    state: Some(agent_mail::states::TaskState::Review),
                    next_action: Some("Review commit abc".into()),
                    ..WorkPatch::default()
                },
                reason: ("implementation submitted").to_owned(),
                resolve_message: None,
            },
            1002,
        )
        .await?;
    assert_eq!(update.version, 2);
    assert!(
        f.store
            .update_work(
                &f.a,
                "lane-api",
                agent_mail::work::WorkUpdate {
                    version: 1,
                    patch: WorkPatch::default(),
                    reason: ("stale").to_owned(),
                    resolve_message: None
                },
                1003
            )
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
    let reopened = Store::open(f.store.root(), false).await?;
    let current = reopened.work_show(&f.b, "lane-api").await?;
    assert_eq!(current.state, agent_mail::states::TaskState::Review);
    assert!(current.state.is_open());
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
                    state: agent_mail::states::TaskState::Active,
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
            .any(|o| o.state == agent_mail::states::DeliveryState::PromptDisabled)
    );
    Ok(())
}

#[tokio::test]
async fn standalone_and_herdr_share_mail_without_sharing_session_authority() -> Result<()> {
    let f = Fixture::new().await?;
    let token = f.store.register("g", "standalone", false).await?;
    let standalone = f.store.authenticate("g", Some(&token)).await?;
    let mut request = message("mixed");
    request.recipients = vec!["standalone".into()];
    let id = f.store.publish(&f.a, request, 1000).await?;
    // An explicit credential selects its own mailbox even in another Herdr pane.
    let cli = f
        .cli_session(
            "w1:p1",
            &["context", "--group", "g"],
            Some(&token.to_string()),
        )
        .await?;
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&cli.stdout)?["mail"][0]["id"],
        id
    );
    let invalid = uuid::Uuid::new_v4().to_string();
    assert!(
        !f.cli_session("w1:p1", &["context", "--group", "g"], Some(&invalid))
            .await?
            .status
            .success()
    );
    let observed = service::tick(&f.store, 1001).await?;
    assert!(observed.iter().any(|o| o.participant == "standalone"
        && o.state == agent_mail::states::DeliveryState::Unavailable));
    assert!(f.host.lock().await.prompts.is_empty());
    assert_eq!(f.store.inbox(&standalone, 0).await?.len(), 1);
    let rotated = f.store.register("g", "standalone", true).await?;
    assert!(f.store.authenticate("g", Some(&token)).await.is_err());
    assert!(f.store.inbox(&standalone, 0).await.is_err());
    assert!(
        f.store
            .resolve(&standalone, id, "stale", None, 1002)
            .await
            .is_err()
    );
    assert!(
        f.store
            .publish(&standalone, message("stale"), 1002)
            .await
            .is_err()
    );
    let current = f.store.authenticate("g", Some(&rotated)).await?;
    assert_eq!(current.id, standalone.id);
    f.store
        .resolve(
            &current,
            id,
            "handled",
            Some(("reply".into(), "Reviewed".into())),
            1003,
        )
        .await?;
    assert_eq!(f.store.inbox(&f.a, 0).await?.len(), 1);

    assert!(
        f.store
            .bind("g", "standalone", &agent("w1:p3"), false)
            .await
            .is_err()
    );
    f.store
        .bind("g", "standalone", &agent("w1:p3"), true)
        .await?;
    assert!(f.store.authenticate("g", Some(&rotated)).await.is_err());
    assert!(f.store.inbox(&current, 0).await.is_err());
    f.host.lock().await.agents.push(agent("w1:p3"));
    assert!(
        f.cli("w1:p3", &["context", "--group", "g"])
            .await?
            .status
            .success()
    );

    assert!(f.store.register("g", "b", false).await.is_err());
    let b_token = f.store.register("g", "b", true).await?;
    assert!(
        !f.cli("w1:p2", &["context", "--group", "g"])
            .await?
            .status
            .success()
    );
    let b = f.store.authenticate("g", Some(&b_token)).await?;
    assert_eq!(b.id, f.b.id);
    // Restoring the original Herdr identity must not revive an old actor snapshot.
    f.store.bind("g", "b", &agent("w1:p2"), true).await?;
    assert!(f.store.inbox(&f.b, 0).await.is_err());
    assert!(f.store.inbox(&b, 0).await.is_err());
    assert!(
        f.cli("w1:p2", &["context", "--group", "g"])
            .await?
            .status
            .success()
    );
    Ok(())
}

#[tokio::test]
async fn work_only_changes_wake_without_separate_mail_and_keep_retry_budget() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .work_create(
            &f.a,
            WorkDraft {
                id: "assignment".into(),
                scope: "Review changes".into(),
                owner: f.b.name.clone(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    assert!(f.store.inbox(&f.b, 0).await?.is_empty());
    service::tick(&f.store, 1000).await?;
    let initial = f.host.lock().await.prompts.len();
    assert_eq!(initial, 1); // Both subscribe, but only the owner has a new obligation.
    service::tick(&f.store, 1001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), initial);
    assert!(f.host.lock().await.prompts[0].contains("tasks"));
    assert!(!f.host.lock().await.prompts[0].contains("Inspect evidence"));
    Ok(())
}

#[tokio::test]
async fn doctor_checks_plugin_even_when_bound_pane_matches() -> Result<()> {
    let f = Fixture::new().await?;
    f.host.lock().await.enabled = false;
    let report = agent_mail::doctor::inspect(f.store.root(), "g", Some("a"), None).await;
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "herdr_plugin" && c.status == agent_mail::doctor::Level::Fail)
    );
    f.host.lock().await.enabled = true;
    let report = agent_mail::doctor::inspect(f.store.root(), "g", Some("a"), None).await;
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "herdr_plugin" && c.status == agent_mail::doctor::Level::Pass)
    );
    Ok(())
}

#[tokio::test]
async fn herdr_verification_obeys_policy_and_requires_exact_agent_response() -> Result<()> {
    use agent_mail::{states::DeliveryReadiness, verification};
    let f = Fixture::new().await?;
    let _lock = service::WorkerLock::acquire(f.store.root())?;
    f.store.set_auto_prompt("g", false).await?;
    verification::reconcile(&f.store, 1000).await?;
    assert!(f.host.lock().await.prompts.is_empty());
    assert_eq!(
        f.store.delivery_status(&f.b, 1000).await?.state,
        DeliveryReadiness::NotifyOnly
    );
    f.store.set_auto_prompt("g", true).await?;
    f.host.lock().await.enabled = false;
    verification::reconcile(&f.store, 1001).await?;
    assert!(f.host.lock().await.prompts.is_empty());
    f.host.lock().await.enabled = true;
    verification::reconcile(&f.store, 1002).await?;
    let prompts = f.host.lock().await.prompts.clone();
    assert_eq!(prompts.len(), 2);
    for prompt in prompts {
        let nonce = prompt
            .split("agent ack ")
            .nth(1)
            .unwrap()
            .split('`')
            .next()
            .unwrap()
            .parse()?;
        // Both challenges have the same group, but each belongs to only one actor.
        let a = f.store.acknowledge_delivery(&f.a, nonce, 1003).await;
        let b = f.store.acknowledge_delivery(&f.b, nonce, 1003).await;
        assert_ne!(a.is_ok(), b.is_ok());
    }
    assert!(f.store.delivery_status(&f.b, 1003).await?.ready);
    f.host.lock().await.enabled = false;
    verification::reconcile(&f.store, 1004).await?;
    assert!(!f.store.delivery_status(&f.b, 1004).await?.ready);
    Ok(())
}

#[tokio::test]
async fn retrieved_mail_stays_pending_without_repeated_wakes() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.send("read-but-undecided").await?;
    service::tick(&f.store, 1000).await?;
    f.store.message(&f.b, id).await?;
    service::tick(&f.store, 1300).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    assert_eq!(
        f.store.inbox(&f.b, 0).await?.len(),
        1,
        "retrieval never resolves a request"
    );
    f.send("fresh-decision").await?;
    service::tick(&f.store, 1301).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 1);
    Ok(())
}

#[tokio::test]
async fn new_events_keep_cooldown_and_context_receipts_cover_only_visible_records() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..8 {
        f.send(&format!("page-{i}")).await?;
    }
    service::tick(&f.store, 1000).await?;
    let context = f.store.context_value(&f.b, String::new(), 0).await?;
    let visible = context["mail"].as_array().unwrap();
    assert_eq!(visible.len(), 5);
    let notices = f.store.notifications(&f.b, 0).await?;
    assert_eq!(notices.len(), 3, "hidden page records remain unreceived");
    f.send("arrived-during-cooldown").await?;
    service::tick(&f.store, 1001).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    service::tick(&f.store, 1300).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 1);
    Ok(())
}

#[tokio::test]
async fn task_retrieval_never_receipts_a_later_revision() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .work_create(
            &f.a,
            WorkDraft {
                id: "revision".into(),
                scope: "Versioned assignment".into(),
                owner: "b".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "First action".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    service::tick(&f.store, 1000).await?;
    f.store.work_show(&f.b, "revision").await?;
    service::tick(&f.store, 1300).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    f.store
        .update_work(
            &f.a,
            "revision",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    next_action: Some("Second action".into()),
                    ..WorkPatch::default()
                },
                reason: "Next authorized step".into(),
                resolve_message: None,
            },
            1301,
        )
        .await?;
    // A delayed receipt for the old view must leave the new revision actionable.
    f.store
        .retrieved(&f.b, &[], &[("revision".into(), 1)])
        .await?;
    service::tick(&f.store, 1301).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 2);
    assert_eq!(f.store.work_show(&f.b, "revision").await?.version, 2);
    Ok(())
}

#[tokio::test]
async fn partial_retrieval_does_not_reset_the_same_generation_budget() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("older-unread").await?;
    let latest = f.send("newer-read").await?;
    service::tick(&f.store, 1000).await?;
    f.store.message(&f.b, latest).await?;
    service::tick(&f.store, 1300).await?;
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 2);
    service::tick(&f.store, 1600).await?;
    service::tick(&f.store, 1900).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 3);
    assert_eq!(f.store.inbox(&f.b, 0).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn herdr_exhaustion_diagnostics_are_event_and_group_scoped() -> Result<()> {
    use agent_mail::attention::AttentionKind;
    let f = Fixture::new().await?;
    let id = f.send("unreceived").await?;
    for now in [1000, 1300, 1600] {
        service::tick(&f.store, now).await?;
    }
    let is_exhausted = |items: &[agent_mail::attention::AttentionItem]| {
        items
            .iter()
            .any(|i| i.participant == "b" && matches!(i.kind, AttentionKind::DeliveryExhausted))
    };
    assert!(is_exhausted(
        &f.store.attention_for(Some("g"), 1600).await?.items
    ));
    assert!(!is_exhausted(
        &f.store.attention_for(Some("other"), 1600).await?.items
    ));
    let server = agent_mail::stream::Server::start(f.store.clone())?;
    let report = agent_mail::doctor::inspect(f.store.root(), "g", Some("b"), None).await;
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "herdr_wake" && c.status == agent_mail::doctor::Level::Fail)
    );
    f.send("fresh-generation").await?;
    let report = agent_mail::doctor::inspect(f.store.root(), "g", Some("b"), None).await;
    assert!(report.checks.iter().any(|c| c.check == "herdr_wake"
        && c.status == agent_mail::doctor::Level::Pass
        && c.detail["attempts"] == 0));
    server.shutdown().await?;
    assert!(
        !is_exhausted(&f.store.attention_for(Some("g"), 1601).await?.items),
        "old exhaustion does not describe new work"
    );
    service::tick(&f.store, 1900).await?;
    let pending = f.store.inbox(&f.b, 0).await?;
    for message in pending {
        f.store.message(&f.b, message.id).await?;
    }
    assert!(!is_exhausted(
        &f.store.attention_for(Some("g"), 1901).await?.items
    ));
    assert_eq!(
        f.store.message(&f.b, id).await?.state,
        agent_mail::states::MessageState::Pending
    );
    Ok(())
}

#[tokio::test]
async fn cli_recovery_retrieves_visible_mail_without_resolution_or_polling() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..8 {
        f.send(&format!("cli-recovery-{i}")).await?;
    }
    service::tick(&f.store, 1000).await?;
    let page = f.cli("w1:p2", &["mail", "list", "--group", "g"]).await?;
    assert!(
        page.status.success(),
        "{}",
        String::from_utf8_lossy(&page.stderr)
    );
    let data: Value = serde_json::from_slice(&page.stdout)?;
    assert_eq!(data["items"].as_array().unwrap().len(), 5);
    assert_eq!(data["more"], true);
    assert_eq!(f.store.notifications(&f.b, 0).await?.len(), 3);
    let after = data["next_after"].to_string();
    let next = f
        .cli(
            "w1:p2",
            &["mail", "list", "--group", "g", "--after", &after],
        )
        .await?;
    assert!(
        next.status.success(),
        "{}",
        String::from_utf8_lossy(&next.stderr)
    );
    let reopened = Store::open(f.store.root(), false).await?;
    service::tick(&reopened, 1300).await?;
    assert_eq!(
        f.host.lock().await.prompts.len(),
        1,
        "retrieved reports don't repeatedly wake the agent"
    );
    assert_eq!(
        reopened.inbox(&f.b, 0).await?.len(),
        6,
        "all eight unresolved requests remain, across bounded pages"
    );
    assert!(reopened.notifications(&f.b, 0).await?.is_empty());
    f.send("cli-new-work").await?;
    service::tick(&reopened, 1301).await?;
    assert_eq!(
        f.host.lock().await.prompts.len(),
        2,
        "new work wakes automatically after restart"
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn alerted_legacy_inbox_still_wakes_a_done_agent() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("legacy-unattempted").await?;
    let pool = support::pool(&f.store).await?;
    sqlx::query!(
        "UPDATE mailboxes SET alerted=1,attempts=0,next_wake=0 WHERE id=?",
        f.b.id
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    f.host.lock().await.agents[1].agent_status = agent_mail::herdr::AgentStatus::Done;
    service::tick(&f.store, 1000).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    assert_eq!(f.store.mailbox("g", "b").await?.attempts, 1);
    Ok(())
}

#[tokio::test]
async fn manually_started_done_client_wakes_without_launch_metadata() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("manual-session").await?;
    {
        let mut host = f.host.lock().await;
        host.agents[1].interactive_ready = None;
        host.agents[1].agent_status = agent_mail::herdr::AgentStatus::Done;
    }
    service::tick(&f.store, 1000).await?;
    assert_eq!(f.host.lock().await.prompts.len(), 1);
    assert!(f.host.lock().await.prompts[0].contains("act or checkpoint"));
    Ok(())
}

#[tokio::test]
async fn explicit_unready_and_blocked_clients_keep_precise_reasons() -> Result<()> {
    let f = Fixture::new().await?;
    f.send("not-ready").await?;
    f.host.lock().await.agents[1].interactive_ready = Some(false);
    let report = service::tick(&f.store, 1000).await?;
    assert_eq!(report[0].detail.as_deref(), Some("not_interactive"));
    {
        let mut host = f.host.lock().await;
        host.agents[1].interactive_ready = None;
        host.agents[1].agent_status = agent_mail::herdr::AgentStatus::Blocked;
    }
    let report = service::tick(&f.store, 1001).await?;
    assert_eq!(report[0].detail.as_deref(), Some("approval_or_question"));
    assert!(f.host.lock().await.prompts.is_empty());
    Ok(())
}
