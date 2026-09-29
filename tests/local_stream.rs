//! Regression coverage for local stream behavior.
mod support;
use agent_mail::{
    doctor::{self, Level},
    store::Store,
    stream::{self, Frame, Server},
    work::{WorkDraft, WorkPatch},
};
use anyhow::Result;
use std::time::Duration;

async fn setup() -> Result<(tempfile::TempDir, Store)> {
    let temp = tempfile::Builder::new()
        .prefix("am-stream-")
        .tempdir_in("/tmp")?;
    let store = Store::open(&temp.path().join("state"), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "owner", false).await?;
    store.close().await;
    let store = Store::open(&temp.path().join("state"), false).await?;
    Ok((temp, store))
}
fn draft(id: &str) -> WorkDraft {
    WorkDraft {
        id: id.into(),
        scope: "Review change".into(),
        owner: "owner".into(),
        state: "custom-state".into(),
        next_action: "Review evidence".into(),
        deadline: None,
        evidence: vec![],
    }
}
async fn next(reader: &mut tokio::io::BufReader<tokio::net::UnixStream>) -> Result<Frame> {
    tokio::time::timeout(Duration::from_secs(2), stream::next(reader)).await?
}

#[tokio::test]
async fn replay_live_restart_and_binding_rotation() -> Result<()> {
    let (_temp, store) = setup().await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    // Writes succeed while no worker or socket exists.
    store.work_create(&writer, draft("one"), 100).await?;
    let server = Server::start(store.clone())?;
    let mut client = stream::connect(&store, &owner, 0).await?;
    assert!(matches!(
        next(&mut client).await?,
        Frame::Ready { version: 1, .. }
    ));
    let cursor = match next(&mut client).await? {
        Frame::Event { id, subject, .. } => {
            assert_eq!(subject, "one");
            id
        }
        other => panic!("{other:?}"),
    };
    store.work_create(&writer, draft("two"), 101).await?;
    let second = match next(&mut client).await? {
        Frame::Event { id, subject, .. } => {
            assert_eq!(subject, "two");
            assert!(id > cursor);
            id
        }
        other => panic!("{other:?}"),
    };
    // Stream consumption never supplies a runtime receipt.
    let receipts = sqlx::query!("SELECT COUNT(*) AS 'count!:i64' FROM event_receipts")
        .fetch_one(&support::pool(&store).await?)
        .await?;
    assert_eq!(receipts.count, 0);
    drop(client);
    server.shutdown().await?;
    store.work_create(&writer, draft("three"), 102).await?;
    let _server = Server::start(store.clone())?;
    let mut client = stream::connect(&store, &owner, second).await?;
    assert!(matches!(next(&mut client).await?, Frame::Ready { .. }));
    assert!(matches!(next(&mut client).await?,Frame::Event{subject,..} if subject=="three"));
    store.register("g", "owner", true).await?;
    assert!(matches!(next(&mut client).await?, Frame::Error { .. }));
    let mut old = stream::connect(&store, &owner, 0).await?;
    assert!(matches!(next(&mut old).await?, Frame::Error { .. }));
    Ok(())
}

#[tokio::test]
async fn subscription_race_has_no_gap_and_is_recipient_scoped() -> Result<()> {
    let (_temp, store) = setup().await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    store.register("g", "unrelated", false).await?;
    let _server = Server::start(store.clone())?;
    let (client, result) = tokio::join!(
        stream::connect(&store, &owner, 0),
        store.work_create(&writer, draft("racing"), 100)
    );
    result?;
    let mut client = client?;
    assert!(matches!(next(&mut client).await?, Frame::Ready { .. }));
    assert!(matches!(next(&mut client).await?,Frame::Event{subject,..} if subject=="racing"));
    let unrelated = store.mailbox("g", "unrelated").await?;
    let mut other = stream::connect(&store, &unrelated, 0).await?;
    assert!(matches!(next(&mut other).await?, Frame::Ready { .. }));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream::next(&mut other))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn attention_does_not_infer_waiting_or_completion() -> Result<()> {
    let (_temp, store) = setup().await?;
    let writer = store.mailbox("g", "writer").await?;
    let mut work = draft("overdue");
    work.deadline = Some(200);
    store.work_create(&writer, work, 100).await?;
    store.work_create(&writer, draft("unbounded"), 100).await?;
    let report = serde_json::to_value(store.attention(10000).await?)?;
    assert_eq!(report["work"].as_array().unwrap().len(), 2);
    let overdue: Vec<_> = report["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] == "work_overdue")
        .collect();
    assert_eq!(overdue.len(), 1);
    assert_eq!(overdue[0]["subject"], "overdue");
    assert_eq!(store.work_show(&writer, "overdue").await?.version, 1);
    store
        .update_work(
            &writer,
            "overdue",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    open: Some(false),
                    ..Default::default()
                },
                reason: ("Accepted").to_owned(),
                resolve_message: None,
            },
            10001,
        )
        .await?;
    assert_eq!(store.attention(10002).await?.work.len(), 1);
    Ok(())
}

#[tokio::test]
async fn doctor_missing_state_is_read_only_and_reports_unknown_trust() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("absent");
    let report = doctor::inspect(&root, "g", Some("owner"), None).await;
    assert!(report.failed());
    assert!(!root.exists());
    let (_temp, store) = setup().await?;
    let report = doctor::inspect(store.root(), "g", Some("owner"), None).await;
    assert!(report.failed());
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "hook_trust" && c.status == Level::Unknown)
    );
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "endpoint" && c.status == Level::Warning)
    );
    let stale = uuid::Uuid::new_v4();
    let report = doctor::inspect(store.root(), "g", Some("owner"), Some(&stale)).await;
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.check == "identity" && c.status == Level::Fail)
    );
    Ok(())
}

#[tokio::test]
async fn missed_hint_reconciles_and_unread_subscriber_does_not_block_writes() -> Result<()> {
    let (_temp, store) = setup().await?;
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    let _server = Server::start(store.clone())?;
    let mut live = stream::connect(&store, &owner, 0).await?;
    assert!(matches!(next(&mut live).await?, Frame::Ready { .. }));
    // Remove only this disposable socket name: existing connection remains live,
    // while the post-commit hint cannot reach the listener.
    std::fs::remove_file(stream::socket(store.root()))?;
    store
        .work_create(&writer, draft("missed-hint"), 100)
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(7), stream::next(&mut live)).await??;
    assert!(matches!(frame,Frame::Event{subject,..} if subject=="missed-hint"));
    // Stop reading. Writes remain independent of subscriber progress.
    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..100 {
            store
                .work_create(&writer, draft(&format!("unread-{i}")), 101 + i)
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn slow_subscriber_is_disconnected_and_can_replay() -> Result<()> {
    let (_temp, store) = setup().await?;
    let owner = store.mailbox("g", "owner").await?;
    // Large durable backlog; the server still reads only 32 records per batch.
    sqlx::query!("WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<20000) INSERT INTO coordination_events(recipient,kind,subject,version,created) SELECT ?,'work_changed','backpressure',n,n FROM seq",owner.id).execute(&support::pool(&store).await?).await?;
    let _server = Server::start(store.clone())?;
    let mut slow = stream::connect(&store, &owner, 0).await?;
    assert!(matches!(next(&mut slow).await?, Frame::Ready { .. }));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let last = tokio::time::timeout(Duration::from_secs(6), async {
        let mut last = 0;
        while let Ok(Frame::Event { id, .. }) = stream::next(&mut slow).await {
            last = id;
        }
        last
    })
    .await?;
    assert!(
        last > 0 && last < 20000,
        "slow consumer should disconnect before the full backlog"
    );
    let mut resumed = stream::connect(&store, &owner, last).await?;
    assert!(matches!(next(&mut resumed).await?, Frame::Ready { .. }));
    assert!(matches!(next(&mut resumed).await?,Frame::Event{id,..} if id==last+1));
    Ok(())
}
