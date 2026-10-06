//! End-to-end coverage for resumable subscriptions and event-driven mail waits.
use agent_mail::{
    states::{MessageState, TaskState},
    store::{Publish, Store},
    stream::Server,
    watch::{WaitOutcome, WaitResult},
    work::WorkDraft,
};
use anyhow::Result;
use std::time::Duration;

async fn setup() -> Result<(tempfile::TempDir, Store)> {
    let temp = tempfile::Builder::new()
        .prefix("am-async-")
        .tempdir_in("/tmp")?;
    let initial = Store::open(&temp.path().join("state"), true).await?;
    initial.enroll("one", None).await?;
    initial.register("one", "writer", false).await?;
    initial.register("one", "worker", false).await?;
    initial.register("one", "worker2", false).await?;
    initial.enroll("two", None).await?;
    initial.register("two", "writer", false).await?;
    initial.close().await;
    let store = Store::open(&temp.path().join("state"), false).await?;
    Ok((temp, store))
}
fn task(id: &str) -> WorkDraft {
    WorkDraft {
        id: id.into(),
        scope: "review patch".into(),
        owner: "worker".into(),
        state: TaskState::Open,
        next_action: "inspect change".into(),
        deadline: None,
        evidence: vec![],
    }
}
fn mail(recipients: &[&str], key: &str, due_after: Option<i64>) -> Publish {
    Publish {
        intent: agent_mail::states::MessageIntent::Request,
        recipients: recipients.iter().map(|value| (*value).into()).collect(),
        key: key.into(),
        summary: "Please review".into(),
        body: "Review the patch".into(),
        due_after,
        reply_to: None,
        work_id: None,
    }
}

#[tokio::test]
async fn watch_groups_changes_and_resumes_only_for_its_identity() -> Result<()> {
    let (_temp, store) = setup().await?;
    let _server = Server::start(store.clone())?;
    let writer = store.mailbox("one", "writer").await?;
    let worker = store.mailbox("one", "worker").await?;
    let other = store.mailbox("two", "writer").await?;
    let mut watch = store.watch(&worker, None).await?;
    let initial = watch.cursor();
    store.work_create(&writer, task("review"), 100).await?;
    let message = store
        .publish(&writer, mail(&["worker"], "watch-new-mail", None), 100)
        .await?;
    let batch = tokio::time::timeout(Duration::from_secs(2), watch.next()).await??;
    assert_eq!(batch.changes.tasks.len(), 1);
    assert_eq!(batch.changes.tasks[0].id, "review");
    assert_eq!(batch.changes.new_mail[0].id, message.to_string());
    assert!(batch.changes.mail_updates.is_empty());
    let cursor = batch.cursor;
    drop(watch);
    assert!(store.watch(&other, Some(&cursor)).await.is_err());
    let mut resumed = store.watch(&worker, Some(&cursor)).await?;
    store.work_create(&writer, task("follow-up"), 101).await?;
    let batch = tokio::time::timeout(Duration::from_secs(2), resumed.next()).await??;
    assert_eq!(batch.changes.tasks.len(), 1);
    assert_eq!(batch.changes.tasks[0].id, "follow-up");
    assert!(
        store
            .watch(&worker, Some(&initial))
            .await?
            .next()
            .await?
            .changes
            .tasks
            .iter()
            .any(|item| item.id == "review")
    );
    Ok(())
}

#[tokio::test]
async fn watch_reconnects_after_worker_restart_and_replays_downtime() -> Result<()> {
    let (_temp, store) = setup().await?;
    let server = Server::start(store.clone())?;
    let writer = store.mailbox("one", "writer").await?;
    let worker = store.mailbox("one", "worker").await?;
    let mut watch = store.watch(&worker, None).await?;
    server.shutdown().await?;
    store
        .work_create(&writer, task("during-restart"), 100)
        .await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let _replacement = Server::start(store.clone())?;
    let batch = tokio::time::timeout(Duration::from_secs(3), watch.next()).await??;
    assert_eq!(batch.changes.tasks[0].id, "during-restart");
    Ok(())
}

#[tokio::test]
async fn wait_observes_reply_and_timeout_without_resolving_request() -> Result<()> {
    let (_temp, store) = setup().await?;
    let _server = Server::start(store.clone())?;
    let writer = store.mailbox("one", "writer").await?;
    let worker = store.mailbox("one", "worker").await?;
    let worker2 = store.mailbox("one", "worker2").await?;
    let id = store
        .publish(&writer, mail(&["worker", "worker2"], "fanout", None), 100)
        .await?;
    let mut notices = store.watch(&writer, None).await?;
    let pending: WaitResult = store
        .wait_mail(&writer, id, Some(Duration::from_millis(40)))
        .await?;
    assert_eq!(pending.outcome, WaitOutcome::Deadline);
    assert_eq!(pending.pending, 2);
    assert_eq!(
        store.message(&worker, id).await?.state,
        MessageState::Pending
    );

    let waiting = {
        let store = store.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            store
                .wait_mail(&writer, id, Some(Duration::from_secs(3)))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reply = store
        .resolve(
            &worker,
            id,
            "answered",
            Some(("worker-reply".into(), "reply:fanout:w".into())),
            101,
        )
        .await?;
    assert!(reply.is_some());
    let notice = tokio::time::timeout(Duration::from_secs(2), notices.next()).await??;
    assert!(
        notice
            .changes
            .mail_updates
            .iter()
            .any(|item| item.id == id.to_string())
    );
    assert!(
        notice
            .changes
            .new_mail
            .iter()
            .any(|item| Some(item.id.parse::<i64>().unwrap()) == reply)
    );
    let result = waiting.await??;
    assert_eq!(result.outcome, WaitOutcome::Reply);
    assert_eq!(result.pending, 1);
    assert_eq!(result.replies, 1);
    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().any(|item| item.reply_id.is_some()));
    store
        .resolve(&worker2, id, "no answer needed", None, 102)
        .await?;
    Ok(())
}

#[tokio::test]
async fn wait_returns_at_request_deadline_without_resolving() -> Result<()> {
    let (_temp, store) = setup().await?;
    let _server = Server::start(store.clone())?;
    let writer = store.mailbox("one", "writer").await?;
    let worker = store.mailbox("one", "worker").await?;
    let id = store
        .publish(
            &writer,
            mail(&["worker"], "deadline", Some(1)),
            agent_mail::now()?,
        )
        .await?;
    let result = store.wait_mail(&writer, id, None).await?;
    assert_eq!(result.outcome, WaitOutcome::Deadline);
    assert_eq!(result.pending, 1);
    assert_eq!(
        store.message(&worker, id).await?.state,
        MessageState::Pending
    );
    Ok(())
}

#[tokio::test]
async fn no_deadline_wait_can_finish_by_reply() -> Result<()> {
    let (_temp, store) = setup().await?;
    let _server = Server::start(store.clone())?;
    let writer = store.mailbox("one", "writer").await?;
    let worker = store.mailbox("one", "worker").await?;
    let id = store
        .publish(&writer, mail(&["worker"], "no-deadline", None), 100)
        .await?;
    let waiting = {
        let store = store.clone();
        let writer = writer.clone();
        tokio::spawn(async move { store.wait_mail(&writer, id, None).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    store.resolve(&worker, id, "done", None, 101).await?;
    assert_eq!(waiting.await??.outcome, WaitOutcome::Settled);
    Ok(())
}
