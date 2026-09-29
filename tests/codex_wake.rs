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
    lose: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn server(listener: UnixListener, thread: Uuid) -> Server {
    let received = Arc::new(AtomicUsize::new(0));
    let lose = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicBool::new(false));
    let (count, loss, busy) = (received.clone(), lose.clone(), active.clone());
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
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
                        "thread/read" => {
                            json!({"thread":{"id":thread,"ephemeral":false,"status":{"type":if busy.load(Ordering::SeqCst){"active"}else{"idle"}}}})
                        }
                        "thread/queue/add" => {
                            assert_eq!(r["params"]["threadId"], thread.to_string());
                            let text = r["params"]["input"][0]["text"].as_str().unwrap();
                            assert!(text.len() <= 6000 && text.contains("Inspect evidence"));
                            count.fetch_add(1, Ordering::SeqCst);
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
        received,
        lose,
        active,
        task,
    }
}

#[tokio::test]
async fn delivery_survives_restart_and_never_accepts_work_or_targets_a_replacement() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let (store, guard) = Store::open(dir.path(), true).await?;
    store.enroll("g", "").await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: "active".into(),
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
    server.active.store(true, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    server.active.store(false, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    assert!(store.notifications(&actor, 0).await?.is_empty());
    assert!(store.work_show(&actor, "task").await?.open);
    store.pool.close().await;
    drop(guard);
    let (store, _guard) = Store::open(dir.path(), false).await?;
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
    let (store, guard) = Store::open(dir.path(), true).await?;
    store.enroll("g", "").await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: "active".into(),
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    server.lose.store(true, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    store.pool.close().await;
    drop(guard);
    let (store, _guard) = Store::open(dir.path(), false).await?;
    for time in [1001, 1300, 1600, 1900] {
        service::tick(&store, time).await?;
    }
    assert_eq!(server.received.load(Ordering::SeqCst), 3);
    assert!(!store.notifications(&actor, 0).await?.is_empty());
    assert!(store.work_show(&actor, "task").await?.open);
    store.rearm("g", "worker").await?;
    server.lose.store(false, Ordering::SeqCst);
    service::tick(&store, 2000).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 4);
    assert!(store.notifications(&actor, 0).await?.is_empty());
    // A new revision bypasses an older uncertain attempt's cooldown.
    server.lose.store(true, Ordering::SeqCst);
    store
        .work_update(
            &actor,
            "task",
            1,
            agent_mail::work::WorkPatch {
                state: Some("review".into()),
                ..Default::default()
            },
            "Review",
            2001,
        )
        .await?;
    service::tick(&store, 2001).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 5);
    store
        .work_update(
            &actor,
            "task",
            2,
            agent_mail::work::WorkPatch {
                state: Some("active".into()),
                ..Default::default()
            },
            "New evidence",
            2002,
        )
        .await?;
    server.lose.store(false, Ordering::SeqCst);
    service::tick(&store, 2002).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 6);
    store.detach_codex(&actor).await?;
    service::tick(&store, 2400).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 6);
    Ok(())
}
