//! Regression coverage for claude wake behavior.
use agent_mail::{
    service,
    store::Store,
    work::{WorkDraft, WorkPatch},
};
use anyhow::Result;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};
use uuid::Uuid;

#[tokio::test]
async fn claude_uses_shared_delivery_recovery_cancellation_and_retry_rules() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("claude.sock");
    let session = Uuid::new_v4();
    let listener = UnixListener::bind(&socket)?;
    let delivered = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicBool::new(false));
    let lose = Arc::new(AtomicBool::new(false));
    let (count, busy, loss) = (delivered.clone(), active.clone(), lose.clone());
    let server = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let (count, busy, loss) = (count.clone(), busy.clone(), loss.clone());
            tokio::spawn(async move {
                let mut io = BufReader::new(io);
                let mut line = String::new();
                io.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                let response = if request["method"] == "status" {
                    json!({"type":"status","protocol":1,"session":session,"client":"2.1.284","ready":true,"active":busy.load(Ordering::SeqCst),"epoch":1})
                } else {
                    assert_eq!(request["session"], session.to_string());
                    assert_eq!(request["epoch"], 1);
                    assert_eq!(request["active"], busy.load(Ordering::SeqCst));
                    assert!(request["text"].as_str().unwrap().len() <= 6000);
                    count.fetch_add(1, Ordering::SeqCst);
                    if loss.load(Ordering::SeqCst) {
                        return;
                    }
                    json!({"type":"accepted","uuid":Uuid::new_v4()})
                };
                io.get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            });
        }
    });
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "owner", false).await?;
    store.register("g", "writer", false).await?;
    let owner = store.mailbox("g", "owner").await?;
    let writer = store.mailbox("g", "writer").await?;
    store.attach_claude(&owner, &socket, session).await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "owner".into(),
                state: "custom".into(),
                next_action: "Inspect".into(),
                deadline: Some(1001),
                evidence: vec![],
            },
            1000,
        )
        .await?;
    active.store(true, Ordering::SeqCst);
    service::tick(&store, 1000).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 0);
    active.store(false, Ordering::SeqCst);
    service::tick(&store, 1001).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 1);
    assert!(store.work_show(&owner, "task").await?.open);
    assert!(store.notifications(&owner, 0).await?.is_empty());
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    service::tick(&store, 1100).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 1);
    active.store(true, Ordering::SeqCst);
    store
        .update_work(
            &writer,
            "task",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    open: Some(false),
                    ..Default::default()
                },
                reason: ("Cancel").to_owned(),
                resolve_message: None,
            },
            1101,
        )
        .await?;
    service::tick(&store, 1101).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 2);
    active.store(false, Ordering::SeqCst);
    lose.store(true, Ordering::SeqCst);
    store
        .update_work(
            &writer,
            "task",
            agent_mail::work::WorkUpdate {
                version: 2,
                patch: WorkPatch {
                    open: Some(true),
                    ..Default::default()
                },
                reason: ("Reopen").to_owned(),
                resolve_message: None,
            },
            1200,
        )
        .await?;
    for time in [1200, 1201, 1500, 1800, 2100] {
        service::tick(&store, time).await?;
    }
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        5,
        "only three uncertain retries"
    );
    store.rearm("g", "owner").await?;
    lose.store(false, Ordering::SeqCst);
    service::tick(&store, 2200).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 6);
    store.register("g", "owner", true).await?;
    service::tick(&store, 2300).await?;
    assert_eq!(delivered.load(Ordering::SeqCst), 6);
    assert!(
        !store
            .notifications(&store.mailbox("g", "owner").await?, 0)
            .await?
            .is_empty()
    );
    server.abort();
    Ok(())
}
