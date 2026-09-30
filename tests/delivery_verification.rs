//! Regression coverage for codex wake behavior.
use agent_mail::{service, states::DeliveryReadiness, store::Store, verification};
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
    let (count, loss, busy) = (received.clone(), lose.clone(), active.clone());
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let absent = absent.clone();
            let loaded = loaded_response.clone();
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
                                    .is_some_and(|id| Uuid::parse_str(id).is_ok())
                            );
                            assert_eq!(r["params"]["threadId"], thread.to_string());
                            let text = r["params"]["input"][0]["text"].as_str().unwrap();
                            assert!(text.contains("agent ack"));
                            *loaded.lock().unwrap() =
                                json!({"data":[thread],"nextCursor":null,"challenge":text});
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
        loaded,
        received,
        lose,
        active,
        unloaded,
        task,
    }
}

fn nonce(server: &Server) -> Uuid {
    let v = server.loaded.lock().unwrap();
    let text = v["challenge"].as_str().unwrap();
    text.split("agent ack ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}
#[tokio::test]
async fn exact_ack_current_health_and_generation_are_required() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    let _lock = service::WorkerLock::acquire(dir.path())?;
    server.active.store(true, Ordering::SeqCst);
    verification::reconcile(&store, 999).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 0);
    server.active.store(false, Ordering::SeqCst);
    verification::reconcile(&store, 1000).await?;
    let pending = store.delivery_status(&actor, 1000).await?;
    assert_eq!(pending.state, DeliveryReadiness::Verifying);
    assert_eq!(pending.transport_accepted_at, Some(1000));
    assert!(!pending.ready);
    assert_eq!(pending.next_attempt_at, Some(1060));
    let challenge = nonce(&server);
    assert!(!serde_json::to_string(&pending)?.contains(&challenge.to_string()));
    store.enroll("other", None).await?;
    store.register("other", "worker", false).await?;
    let other = store.mailbox("other", "worker").await?;
    assert!(
        store
            .acknowledge_delivery(&other, challenge, 1001)
            .await
            .is_err()
    );
    assert!(
        store
            .acknowledge_delivery(&actor, Uuid::new_v4(), 1001)
            .await
            .is_err()
    );
    store.acknowledge_delivery(&actor, challenge, 1001).await?;
    store.acknowledge_delivery(&actor, challenge, 1002).await?;
    assert!(store.delivery_status(&actor, 1002).await?.ready);
    assert!(!store.delivery_status(&actor, 1031).await?.ready);
    server.unloaded.store(true, Ordering::SeqCst);
    verification::reconcile(&store, 1003).await?;
    assert!(!store.delivery_status(&actor, 1003).await?.ready);
    server.unloaded.store(false, Ordering::SeqCst);
    verification::reconcile(&store, 1004).await?;
    assert!(store.delivery_status(&actor, 1004).await?.ready);
    store.pause("g", true).await?;
    assert_eq!(
        store.delivery_status(&actor, 1004).await?.state,
        DeliveryReadiness::Paused
    );
    store.pause("g", false).await?;
    store.register("g", "worker", true).await?;
    let replacement = store.mailbox("g", "worker").await?;
    store.attach_codex(&replacement, &socket, thread).await?;
    assert!(
        store
            .acknowledge_delivery(&replacement, challenge, 1005)
            .await
            .is_err()
    );
    assert!(!store.delivery_status(&replacement, 1005).await?.ready);
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    store
        .begin_launch(
            &replacement,
            "new-launch",
            agent_mail::states::NativeRuntime::Codex,
        )
        .await?;
    assert_eq!(
        store.delivery_status(&replacement, 1006).await?.state,
        DeliveryReadiness::MissingEndpoint
    );
    store.clone().close().await;
    let failed = store.recipient_delivery_outcome("g", "worker").await;
    assert_eq!(failed["state"], "unknown");
    assert_eq!(failed["ready"], false);
    Ok(())
}
#[tokio::test]
async fn retry_budget_survives_reopen_and_requires_explicit_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("codex.sock");
    let thread = Uuid::new_v4();
    let server = server(UnixListener::bind(&socket)?, thread);
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    store.attach_codex(&actor, &socket, thread).await?;
    let lock = service::WorkerLock::acquire(dir.path())?;
    server.lose.store(true, Ordering::SeqCst);
    verification::reconcile(&store, 1000).await?;
    verification::reconcile(&store, 1059).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 1);
    let challenge = nonce(&server);
    store.close().await;
    drop(lock);
    let store = Store::open(dir.path(), false).await?;
    let _lock = service::WorkerLock::acquire(dir.path())?;
    for time in [1060, 1120, 1180, 1300] {
        verification::reconcile(&store, time).await?;
    }
    assert_eq!(server.received.load(Ordering::SeqCst), 3);
    assert_eq!(nonce(&server), challenge);
    assert_eq!(
        store.delivery_status(&actor, 1300).await?.state,
        DeliveryReadiness::Expired
    );
    assert!(
        store
            .acknowledge_delivery(&actor, challenge, 1300)
            .await
            .is_err()
    );
    store.rearm("g", "worker").await?;
    verification::reconcile(&store, 1301).await?;
    assert_eq!(server.received.load(Ordering::SeqCst), 4);
    assert_ne!(nonce(&server), challenge);
    Ok(())
}
