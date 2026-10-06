//! Regression coverage for codex wake behavior.
use agent_mail::{service, store::Store, work::WorkDraft};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::net::UnixListener;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

struct Server {
    received: Arc<AtomicUsize>,
    loaded: Arc<std::sync::Mutex<Value>>,
    lose: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    unloaded: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn server(listener: UnixListener, thread: Uuid) -> Server {
    let loaded = Arc::new(std::sync::Mutex::new(
        json!({"data":[thread],"nextCursor":null}),
    ));
    let loaded_response = loaded.clone();
    let received = Arc::new(AtomicUsize::new(0));
    let lose = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicBool::new(false));
    let unloaded = Arc::new(AtomicBool::new(false));
    let absent = unloaded.clone();
    let queued_ids = Arc::new(std::sync::Mutex::new(
        std::collections::HashSet::<String>::new(),
    ));
    let (count, loss, busy) = (received.clone(), lose.clone(), active.clone());
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let absent = absent.clone();
            let loaded = loaded_response.clone();
            let queued_ids = queued_ids.clone();
            let (count, loss, busy) = (count.clone(), loss.clone(), busy.clone());
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let r: Value = serde_json::from_str(&text).unwrap();
                    let Some(id) = r.get("id") else {
                        continue;
                    };
                    let result = match r["method"].as_str().unwrap() {
                        "initialize" => json!({}),
                        "thread/loaded/list" => loaded.lock().unwrap().clone(),
                        "thread/read" => {
                            json!({"thread":{"id":thread,"ephemeral":false,"turns":[{"id":"turn-active","status":"inProgress"}],"status":{"type":if absent.load(Ordering::SeqCst){"notLoaded"}else if busy.load(Ordering::SeqCst){"active"}else{"idle"}}}})
                        }
                        "turn/steer" => {
                            assert_eq!(r["params"]["expectedTurnId"], "turn-active");
                            assert!(
                                r["params"]["input"][0]["text"]
                                    .as_str()
                                    .unwrap()
                                    .contains("Stop only assignments")
                            );
                            count.fetch_add(1, Ordering::SeqCst);
                            json!({"turnId":"turn-active"})
                        }
                        "thread/queue/list" => json!({"queuedSubmissions":[]}),
                        "thread/queue/add" => {
                            assert!(
                                r["params"]["clientUserMessageId"]
                                    .as_str()
                                    .is_some_and(|id| id.starts_with("agent-mail-"))
                            );
                            assert_eq!(r["params"]["threadId"], thread.to_string());
                            let message_id = r["params"]["clientUserMessageId"].as_str().unwrap();
                            if !queued_ids.lock().unwrap().insert(message_id.to_owned()) {
                                ws.send(Message::Text(
                                    json!({"id":id,"result":{"queuedSubmission":{"id":"receipt"}}})
                                        .to_string()
                                        .into(),
                                ))
                                .await
                                .unwrap();
                                continue;
                            }
                            let text = r["params"]["input"][0]["text"].as_str().unwrap();
                            assert!(
                                text.len() <= 6000
                                    && text.contains("tasks")
                                    && !text.contains("Inspect evidence")
                            );
                            let prior = count.fetch_add(1, Ordering::SeqCst);
                            if prior == 0 {
                                assert!(text.contains("agent ack"));
                            }
                            *loaded.lock().unwrap() = json!({
                                "data":[thread],
                                "nextCursor":null,
                                "challenge":text
                            });
                            if loss.load(Ordering::SeqCst) {
                                break;
                            }
                            json!({"queuedSubmission":{"id":"receipt"}})
                        }
                        other => panic!("unexpected method {other}"),
                    };
                    if ws
                        .send(Message::Text(
                            json!({"id":id,"result":result}).to_string().into(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
    });
    Server {
        loaded,
        received,
        lose,
        active,
        unloaded,
        task,
    }
}

#[tokio::test]
async fn distinct_attention_pages_use_distinct_runtime_queue_identities() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "worker", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let worker = store.mailbox("g", "worker").await?;
    store.attach_codex(&worker, &socket, thread).await?;
    for i in 0..8 {
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: format!("task-{i}"),
                    scope: "Review".into(),
                    owner: "worker".into(),
                    state: agent_mail::states::TaskState::Active,
                    next_action: "Inspect evidence".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1000,
            )
            .await?;
    }
    service::tick(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    let prompt = server.loaded.lock().unwrap()["challenge"]
        .as_str()
        .unwrap()
        .to_owned();
    let token = prompt
        .split("attention acknowledge ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap();
    store.acknowledge_attention(&worker, token).await?;
    assert_eq!(store.attention_snapshot(&worker).await?.items.len(), 3);
    // No new coordination event arrives between these pages. A runtime deduplicates
    // queue submissions by clientUserMessageId, so the reservation must identify it.
    service::tick(&store, 1001).await?;
    assert_eq!(
        server.received.load(Ordering::SeqCst),
        2,
        "the second page must reach a deduplicating runtime"
    );
    Ok(())
}

#[tokio::test]
async fn delivery_survives_restart_and_never_accepts_work_or_targets_a_replacement() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    store.pause("g", true).await?;
    service::tick(&store, 999).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    store.pause("g", false).await?;
    agent_mail::verification::reconcile(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    server.active.store(true, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    server.active.store(false, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    let prompt = server.loaded.lock().unwrap()["challenge"]
        .as_str()
        .unwrap()
        .to_owned();
    let nonce: Uuid = prompt
        .split("agent ack ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap()
        .parse()?;
    store.acknowledge_delivery(&actor, nonce, 1001).await?;
    agent_mail::verification::reconcile(&store, 1002).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    assert!(
        !store.notifications(&actor, 0).await?.is_empty(),
        "queue acceptance and a route challenge do not receipt a batch"
    );
    let token = prompt
        .split("attention acknowledge ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap();
    store.acknowledge_attention(&actor, token).await?;
    assert!(store.notifications(&actor, 0).await?.is_empty());
    assert!(store.work_show(&actor, "task").await?.state.is_open());
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    service::tick(&store, 1400).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    // Idempotent attach must not replay an already accepted notification.
    store.attach_codex(&actor, &socket, thread).await?;
    service::tick(&store, 1500).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    store.register("g", "worker", true).await?;
    service::tick(&store, 1600).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    let replacement = store.mailbox("g", "worker").await?;
    assert!(!store.notifications(&replacement, 0).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn lost_receipts_use_durable_bounded_retries_and_pause_is_respected() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    server.lose.store(true, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    for time in [1001, 1300, 1600, 1900] {
        service::tick(&store, time).await?;
    }
    assert_eq!(server.received.load(Ordering::SeqCst), 3);
    assert!(!store.notifications(&actor, 0).await?.is_empty());
    store.rearm("g", "worker").await?;
    server.lose.store(false, Ordering::SeqCst);
    service::tick(&store, 2000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 4);
    let prompt = server.loaded.lock().unwrap()["challenge"]
        .as_str()
        .unwrap()
        .to_owned();
    let token = prompt
        .split("attention acknowledge ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap();
    store.acknowledge_attention(&actor, token).await?;
    assert!(store.notifications(&actor, 0).await?.is_empty());
    // A new revision preserves an older uncertain attempt's cooldown.
    server.lose.store(true, Ordering::SeqCst);
    store
        .update_work(
            &writer,
            "task",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: agent_mail::work::WorkPatch {
                    state: Some(agent_mail::states::TaskState::Review),
                    ..Default::default()
                },
                reason: ("Review").to_owned(),
                resolve_message: None,
            },
            2001,
        )
        .await?;
    service::tick(&store, 2001).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 5);
    store
        .update_work(
            &writer,
            "task",
            agent_mail::work::WorkUpdate {
                version: 2,
                patch: agent_mail::work::WorkPatch {
                    state: Some(agent_mail::states::TaskState::Active),
                    ..Default::default()
                },
                reason: ("New evidence").to_owned(),
                resolve_message: None,
            },
            2002,
        )
        .await?;
    server.lose.store(false, Ordering::SeqCst);
    service::tick(&store, 2002).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 5);
    service::tick(&store, 2301).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 6);
    store.set_runtime_enabled(&actor, false).await?;
    service::tick(&store, 2400).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 6);
    Ok(())
}

#[tokio::test]
async fn cancellation_reaches_active_owner_but_idle_closure_does_not_wake() -> Result<()> {
    for (active, reassigned) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("codex.sock");
        let thread = Uuid::new_v4();
        let server = server(UnixListener::bind(&socket)?, thread);
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "new-owner", false).await?;
        store.register("g", "worker", false).await?;
        let actor = store.mailbox("g", "worker").await?;
        let writer = store.mailbox("g", "writer").await?;
        store.attach_codex(&actor, &socket, thread).await?;
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: "task".into(),
                    scope: "Review".into(),
                    owner: "worker".into(),
                    state: agent_mail::states::TaskState::Active,
                    next_action: "Inspect evidence".into(),
                    deadline: Some(1100),
                    evidence: vec![],
                },
                1000,
            )
            .await?;
        service::tick(&store, 1000).await?;
        assert_eq!(server.received.load(Ordering::SeqCst), 1);
        // Accepted runtime input without a decision remains open and overdue.
        assert_eq!(store.attention(1200).await?.work.len(), 1);
        service::tick(&store, 1200).await?;
        assert_eq!(server.received.load(Ordering::SeqCst), 1);
        server.active.store(active, Ordering::SeqCst);
        store
            .update_work(
                &writer,
                "task",
                agent_mail::work::WorkUpdate {
                    version: 1,
                    patch: agent_mail::work::WorkPatch {
                        state: (!reassigned).then_some(agent_mail::states::TaskState::Cancelled),
                        owner: reassigned.then(|| "new-owner".into()),
                        ..Default::default()
                    },
                    reason: ("Cancelled").to_owned(),
                    resolve_message: None,
                },
                1201,
            )
            .await?;
        service::tick(&store, 1201).await?;
        assert_eq!(
            server.received.load(Ordering::SeqCst),
            if active { 2 } else { 1 }
        );
        assert_eq!(
            store.work_show(&writer, "task").await?.state.is_open(),
            reassigned
        );
        // Passive records remain available after a reset even without a courtesy turn.
        assert!(
            store
                .latest_changes(&actor)
                .await?
                .iter()
                .any(|e| e.subject == "task" && e.version == 2)
        );
    }
    Ok(())
}

#[tokio::test]
async fn doctor_rejects_an_unloaded_thread_even_when_its_socket_responds() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    store.close().await;
    server.unloaded.store(true, Ordering::SeqCst);
    let report = agent_mail::doctor::inspect(dir.path(), "g", Some("worker"), None).await;
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "endpoint" && c.status == agent_mail::doctor::Level::Fail)
    );
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn automatic_attachment_requires_current_launch_and_preserves_pause() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let _server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store
        .begin_launch(&actor, "new", agent_mail::states::NativeRuntime::Codex)
        .await?;
    store
        .observe_hook(
            &actor,
            "new",
            &thread.to_string(),
            agent_mail::states::HookEvent::SessionStart,
            100,
        )
        .await?;
    store
        .attach_codex_from_hook(&actor, &socket, thread, "old")
        .await?;
    assert!(store.native_status().await?.as_array().unwrap().is_empty());
    store
        .attach_codex_from_hook(&actor, &socket, thread, "new")
        .await?;
    assert_eq!(store.native_status().await?.as_array().unwrap().len(), 1);
    store.set_runtime_enabled(&actor, false).await?;
    store
        .attach_codex_from_hook(&actor, &socket, thread, "new")
        .await?;
    assert!(!store.runtime_enabled(&actor).await?);
    assert!(store.native_status().await?.as_array().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn discovery_never_guesses_between_loaded_threads() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    assert_eq!(
        agent_mail::codex::sole_loaded_thread(&socket).await?,
        Some(thread)
    );
    for result in [
        json!({"data":[]}),
        json!({"data":[thread,Uuid::new_v4()]}),
        json!({"data":[thread],"nextCursor":"more"}),
    ] {
        *server.loaded.lock().unwrap() = result;
        assert_eq!(agent_mail::codex::sole_loaded_thread(&socket).await?, None);
    }
    Ok(())
}

#[tokio::test]
async fn partial_progress_resumes_after_turn_end_and_worker_restart_without_new_messages()
-> Result<()> {
    use agent_mail::{
        followup::{Checkpoint, Source},
        states::TaskState,
    };
    use std::{process::Stdio, time::Duration};
    let dir = tempfile::Builder::new()
        .prefix("am-continue-")
        .tempdir_in("/tmp")?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "coordinator", false).await?;
    store.register("g", "worker", false).await?;
    let writer = store.mailbox("g", "coordinator").await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    let time = agent_mail::now()?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "partial".into(),
                scope: "Finish remaining work".into(),
                owner: "worker".into(),
                state: TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            time,
        )
        .await?;
    service::tick(&store, time).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    server.active.store(true, Ordering::SeqCst);
    store
        .checkpoint(
            &actor,
            Source::Task {
                id: "partial".into(),
                version: 1,
            },
            "progress",
            Checkpoint {
                version: 0,
                next_step: "Finish remaining work".into(),
                next_check_at: time + 2,
                waiting: None,
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            time,
        )
        .await?;
    store
        .hook(
            &actor,
            serde_json::from_value(
                json!({"hook_event_name":"Stop","session_id":thread.to_string()}),
            )?,
            time,
        )
        .await?;
    server.active.store(false, Ordering::SeqCst);
    store.close().await;
    // A fresh worker process recovers the deadline; no incoming message or completion subscription.
    let mut worker = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .arg("--state-dir")
        .arg(dir.path())
        .args(["service", "run"])
        .env_remove("AGENT_MAIL_SESSION")
        .env_remove("AGENT_MAIL_GROUP")
        .env_remove("HERDR_ENV")
        .env_remove("HERDR_SOCKET_PATH")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let resumed = tokio::time::timeout(Duration::from_secs(7), async {
        while server.received.load(Ordering::SeqCst) < 2 {
            anyhow::ensure!(
                worker.try_wait()?.is_none(),
                "worker exited before continuation"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    worker.kill().await?;
    worker.wait().await?;
    resumed??;
    assert_eq!(server.received.load(Ordering::SeqCst), 2);
    let recovered = Store::open(dir.path(), false).await?;
    let task = recovered.work_show(&actor, "partial").await?;
    assert_eq!((task.state, task.version), (TaskState::Active, 1));
    assert!(
        server.loaded.lock().unwrap()["challenge"]
            .as_str()
            .unwrap()
            .contains("followups")
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_and_fresh_wakes_preserve_uncertainty_and_escalate_to_coordinator() -> Result<()>
{
    use agent_mail::states::TaskState;
    let dir = tempfile::Builder::new()
        .prefix("am-uncertain-")
        .tempdir_in("/tmp")?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "coordinator", false).await?;
    store.register("g", "worker", false).await?;
    let writer = store.mailbox("g", "coordinator").await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    for (id, time) in [("first", 1000), ("fresh", 1001)] {
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: id.into(),
                    scope: "Review".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Inspect evidence".into(),
                    deadline: None,
                    evidence: vec![],
                },
                time,
            )
            .await?;
        if id == "first" {
            server.lose.store(true, Ordering::SeqCst);
            let (one, two) = tokio::join!(service::tick(&store, time), service::tick(&store, time));
            let one = one?;
            let two = two?;
            assert!(
                one.iter()
                    .chain(&two)
                    .any(|o| o.state == agent_mail::states::DeliveryState::Uncertain)
            );
            assert_eq!(server.received.load(Ordering::SeqCst), 1);
        }
    }
    service::tick(&store, 1001).await?;
    assert_eq!(
        server.received.load(Ordering::SeqCst),
        1,
        "fresh work cannot bypass an ambiguous reservation"
    );
    assert_eq!(
        store.native_status().await?[0]["delivery_unconfirmed"],
        true
    );
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    assert_eq!(
        store.native_status().await?[0]["delivery_unconfirmed"],
        true
    );
    service::tick(&store, 1299).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    for time in [1300, 1600, 1900, 1901] {
        service::tick(&store, time).await?;
    }
    // The fresh event starts a new bounded budget after the original reservation expires.
    assert_eq!(server.received.load(Ordering::SeqCst), 4);
    assert_eq!(
        store.attention_list(&writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "the fresh batch still has its final receipt window"
    );
    service::tick(&store, 2200).await?;
    let escalations = store.attention_list(&writer, 0).await?;
    assert_eq!(escalations["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        store.work_show(&actor, "first").await?.state,
        TaskState::Active
    );
    assert_eq!(
        store.work_show(&actor, "fresh").await?.state,
        TaskState::Active
    );
    Ok(())
}
