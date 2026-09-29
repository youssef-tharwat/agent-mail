//! Domain guarantees behind the simplified command interface.
use agent_mail::{
    store::{Publish, Store},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;

#[tokio::test]
async fn concurrent_creation_and_update_retries_commit_once() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let actor = store.mailbox("g", "writer").await?;
    let draft = WorkDraft {
        id: "task".into(),
        scope: "Review".into(),
        owner: "writer".into(),
        state: "open".into(),
        next_action: "Review".into(),
        deadline: None,
        evidence: vec![],
    };
    let (a, b) = tokio::join!(
        store.work_create(&actor, draft.clone(), 1000),
        store.work_create(&actor, draft, 1001)
    );
    assert_eq!(a?.updated, b?.updated);
    let update = WorkUpdate {
        version: 1,
        reason: "Clarify".into(),
        patch: WorkPatch {
            next_action: Some("Review retries".into()),
            ..Default::default()
        },
        resolve_message: None,
    };
    let (a, b) = tokio::join!(
        store.update_work(&actor, "task", update.clone(), 1002),
        store.update_work(&actor, "task", update, 1003)
    );
    assert_eq!(a?.updated, b?.updated);
    assert_eq!(store.work_history(&actor, "task").await?.len(), 2);
    assert_eq!(store.work_show(&actor, "task").await?.version, 2);
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn no_deadline_still_publishes_delivery_events_without_false_overdue() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "sender", false).await?;
    store.register("g", "worker", false).await?;
    let sender = store.mailbox("g", "sender").await?;
    let worker = store.mailbox("g", "worker").await?;
    let id = store
        .publish(
            &sender,
            Publish {
                recipients: vec!["worker".into()],
                key: "request".into(),
                summary: "Review".into(),
                body: String::new(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            1000,
        )
        .await?;
    assert_eq!(store.message(&worker, id).await?.due, None);
    assert_eq!(store.notifications(&worker, 0).await?.len(), 1);
    let attention = serde_json::to_value(store.attention(10_000_000).await?)?;
    assert!(
        !attention["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["kind"] == "mail_overdue")
    );
    store.resolve(&worker, id, "handled", None, 1001).await?;
    assert!(
        store
            .resolve(
                &worker,
                id,
                "replied",
                Some((format!("reply:{id}"), "Answer".into())),
                1002
            )
            .await
            .is_err()
    );
    assert!(store.inbox(&sender, 0).await?.is_empty());
    store.close().await;
    Ok(())
}
