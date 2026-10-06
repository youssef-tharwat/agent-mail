//! Mandatory contextual mail, inherited task observations, and private threads.
mod support;
use agent_mail::{
    mail_context::{ContextSource, ConversationId, MessageContext, MessageId, TaskVersion},
    states::{MessageIntent, MessageState, TaskState},
    store::{Mailbox, Publish, Store},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;
use serde_json::json;

async fn setup() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox, Mailbox)> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    for name in ["writer", "owner", "other"] {
        store.register("g", name, false).await?;
    }
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    let other = store.mailbox("g", "other").await?;
    Ok((temp, store, writer, owner, other))
}

fn publish(to: &str, key: &str, context: ContextSource) -> Publish {
    Publish {
        intent: MessageIntent::Request,
        recipients: vec![to.into()],
        key: key.into(),
        summary: format!("Request {key}"),
        body: format!("Private {key}"),
        due_after: None,
        context,
    }
}

#[test]
fn absent_ambiguous_and_invalid_contexts_are_rejected_at_the_boundary() {
    let bare = json!({"recipients":["owner"],"key":"k","summary":"s","body":"","due_after":null});
    assert!(serde_json::from_value::<Publish>(bare.clone()).is_err());
    for context in [
        json!({"kind":"task","id":"t"}),
        json!({"kind":"task","id":"t","version":0}),
        json!({"kind":"task","id":"t","version":1,"conversation":"extra"}),
        json!({"kind":"reply","message":0}),
        json!({"kind":"conversation","id":"00000000-0000-0000-0000-000000000000"}),
    ] {
        let mut value = bare.clone();
        value["context"] = context;
        assert!(serde_json::from_value::<Publish>(value).is_err());
    }
    assert!(TaskVersion::new(-1).is_err());
    assert!(MessageId::new(0).is_err());
    assert!(ConversationId::new(uuid::Uuid::nil()).is_err());
    let mut valid = bare;
    valid["context"] = json!({"kind":"new_conversation"});
    valid["work_id"] = json!("untyped-old-association");
    assert!(serde_json::from_value::<Publish>(valid).is_err());
}

#[tokio::test]
async fn task_observations_survive_updates_and_replies_without_accepting_work() -> Result<()> {
    let (_temp, store, writer, owner, _) = setup().await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "t".into(),
                owner: "owner".into(),
                scope: "Review".into(),
                state: TaskState::Open,
                next_action: "Review".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    let observed = ContextSource::Task {
        id: "t".parse()?,
        version: 1.try_into()?,
    };
    let request = store
        .publish(&writer, publish("owner", "review", observed.clone()), 101)
        .await?;
    store
        .update_work(
            &writer,
            "t",
            WorkUpdate {
                version: 1,
                reason: "Wait".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Blocked),
                    ..Default::default()
                },
                resolve_message: None,
            },
            102,
        )
        .await?;
    let interim = store
        .publish(
            &owner,
            publish(
                "writer",
                "report",
                ContextSource::Reply {
                    message: request.try_into()?,
                },
            ),
            103,
        )
        .await?;
    let message = store.message(&writer, interim).await?;
    assert_eq!(
        message.context,
        MessageContext::Task {
            id: "t".parse()?,
            version: 1.try_into()?
        }
    );
    assert_eq!(message.reply_to, Some(request));
    assert_eq!(store.work_show(&writer, "t").await?.version, 2);
    assert_eq!(
        store.work_show(&writer, "t").await?.state,
        TaskState::Blocked
    );
    assert_eq!(
        store.message(&owner, request).await?.state,
        MessageState::Pending
    );
    let response = store
        .resolve(
            &owner,
            request,
            "Answered",
            Some(("answer".into(), "Review complete".into())),
            104,
        )
        .await?
        .unwrap();
    assert_eq!(
        store.message(&writer, response).await?.context,
        message.context
    );
    assert_eq!(
        store.work_show(&writer, "t").await?.state,
        TaskState::Blocked
    );
    // A report may retain a genuinely older observation; a future version cannot be asserted.
    store
        .publish(&writer, publish("owner", "older", observed), 105)
        .await?;
    for (id, version) in [("t", 3), ("missing", 1)] {
        assert!(
            store
                .publish(
                    &writer,
                    publish(
                        "owner",
                        "invalid",
                        ContextSource::Task {
                            id: id.parse()?,
                            version: version.try_into()?
                        }
                    ),
                    106
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn threads_preserve_private_recipients_and_do_not_receipt_hidden_messages() -> Result<()> {
    let (_temp, store, writer, owner, other) = setup().await?;
    let first = store
        .publish(
            &writer,
            publish("owner", "first", ContextSource::NewConversation),
            100,
        )
        .await?;
    let MessageContext::Conversation { id } = store.message_context(&writer, first).await? else {
        panic!("conversation")
    };
    assert!(store.conversation(&other, id, 0).await.is_err());
    assert!(
        store
            .publish(
                &other,
                publish("writer", "hijack", ContextSource::Conversation { id }),
                101
            )
            .await
            .is_err()
    );
    assert!(
        store
            .publish(
                &other,
                publish(
                    "writer",
                    "private-parent",
                    ContextSource::Reply {
                        message: first.try_into()?
                    }
                ),
                101
            )
            .await
            .is_err()
    );
    let second = store
        .publish(
            &writer,
            publish("other", "second", ContextSource::Conversation { id }),
            102,
        )
        .await?;
    let page = store.conversation(&owner, id, 0).await?;
    assert_eq!(
        page.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
        [first]
    );
    let page = store.conversation(&other, id, 0).await?;
    assert_eq!(
        page.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
        [second]
    );
    assert_eq!(store.conversation(&writer, id, 0).await?.messages.len(), 2);
    assert_eq!(
        store.inbox(&owner, 0).await?.len(),
        1,
        "thread metadata does not consume a request"
    );
    assert!(!store.attention_snapshot(&owner).await?.items.is_empty());
    // Group membership never lends access to a thread with the same UUID.
    store.enroll("other-group", None).await?;
    store.register("other-group", "writer", false).await?;
    let outsider = store.mailbox("other-group", "writer").await?;
    assert!(store.conversation(&outsider, id, 0).await.is_err());
    assert!(
        store
            .publish(
                &outsider,
                publish(
                    "writer",
                    "other-group",
                    ContextSource::Reply {
                        message: first.try_into()?
                    }
                ),
                103
            )
            .await
            .is_err()
    );
    assert!(store.conversation(&writer, id, -1).await.is_err());
    Ok(())
}

#[tokio::test]
async fn conversation_creation_retries_restart_and_paging_preserve_context() -> Result<()> {
    let (_temp, store, writer, owner, _) = setup().await?;
    let request = publish("owner", "first", ContextSource::NewConversation);
    let (a, b) = tokio::join!(
        store.publish(&writer, request.clone(), 100),
        store.publish(&writer, request.clone(), 101)
    );
    let first = a?;
    assert_eq!(first, b?);
    let MessageContext::Conversation { id } = store.message_context(&writer, first).await? else {
        panic!("conversation")
    };
    for index in 0..8 {
        store
            .publish(
                &writer,
                publish(
                    "owner",
                    &format!("next-{index}"),
                    ContextSource::Conversation { id },
                ),
                102 + index,
            )
            .await?;
    }
    let page = store.conversation(&owner, id, 0).await?;
    assert_eq!(page.messages.len(), 6);
    assert!(page.more);
    let rest = store.conversation(&owner, id, page.next_after).await?;
    assert_eq!(rest.messages.len(), 3);
    assert!(!rest.more);
    let root = store.root().to_path_buf();
    store.close().await;
    let reopened = Store::open(&root, false).await?;
    assert_eq!(
        reopened.message_context(&owner, first).await?,
        MessageContext::Conversation { id }
    );
    assert_eq!(reopened.publish(&writer, request, 200).await?, first);
    let mut notice = publish("owner", "quiet", ContextSource::Conversation { id });
    notice.intent = MessageIntent::Notice;
    let before = reopened.attention_snapshot(&owner).await?.items;
    reopened.publish(&writer, notice, 201).await?;
    assert_eq!(
        reopened.attention_snapshot(&owner).await?.items.len(),
        before.len()
    );
    Ok(())
}

#[tokio::test]
async fn persisted_context_is_required_and_cannot_be_rewritten() -> Result<()> {
    let (_temp, store, writer, _, _) = setup().await?;
    let id = store
        .publish(
            &writer,
            publish("owner", "first", ContextSource::NewConversation),
            100,
        )
        .await?;
    let pool = support::pool(&store).await?;
    assert!(
        sqlx::query("UPDATE messages SET context=NULL WHERE id=?")
            .bind(id)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due) VALUES (?,'bare','{}','bare','',101,101)").bind(writer.id).execute(&pool).await.is_err());
    for context in [
        r#"{"kind":"task","id":"missing"}"#,
        r#"{"kind":"conversation"}"#,
        "not-json",
    ] {
        assert!(sqlx::query("INSERT INTO messages(sender,dedup_key,canonical,summary,body,created,due,context) VALUES (?,'bad','{}','bad','',101,101,?)").bind(writer.id).bind(context).execute(&pool).await.is_err());
    }
    Ok(())
}
