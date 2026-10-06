//! Audited authority, relationship facts, and complete bounded history contracts.
use agent_mail::{
    relationships::{RelationKind, RelationQuery, RelationUpdate},
    states::TaskState,
    store::Store,
    work::{WorkDraft, WorkListQuery, WorkPatch, WorkUpdate, WriterTransfer},
};
use anyhow::Result;

#[tokio::test]
async fn transfer_revokes_old_authority_and_preserves_obligations() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    for n in ["writer", "owner", "next"] {
        store.register("g", n, false).await?;
    }
    let writer = store.mailbox("g", "writer").await?;
    let owner = store.mailbox("g", "owner").await?;
    let next = store.mailbox("g", "next").await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "t".into(),
                scope: "review".into(),
                owner: "owner".into(),
                state: TaskState::Blocked,
                next_action: "wait approval".into(),
                deadline: Some(1000),
                evidence: vec!["revision:a".into()],
            },
            1,
        )
        .await?;
    store
        .checkpoint(
            &owner,
            agent_mail::followup::Source::Task {
                id: "t".into(),
                version: 1,
            },
            "hold",
            agent_mail::followup::Checkpoint {
                version: 0,
                next_step: "await approval".into(),
                next_check_at: 61,
                waiting: Some(agent_mail::followup::WaitFor::External {
                    responsible: "reviewer".into(),
                    reason: "approval pending".into(),
                }),
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            1,
        )
        .await?;
    let transfer = WriterTransfer {
        version: 1,
        old_writer: "writer".into(),
        new_writer: "next".into(),
        new_binding_version: next.binding_version,
        reason: "handoff".into(),
    };
    assert!(
        store
            .work_transfer(&owner, "t", transfer.clone(), 2)
            .await
            .is_err()
    );
    let result = store
        .work_transfer(&writer, "t", transfer.clone(), 2)
        .await?;
    assert_eq!(result.writer, "next");
    assert_eq!(result.owner, "owner");
    assert_eq!(result.state, TaskState::Blocked);
    assert_eq!(result.deadline, Some(1000));
    let followup = store.source_followup(&next, Some("t"), None).await?;
    assert_eq!(followup["checkpoint"]["next_step"], "await approval");
    assert_eq!(
        store
            .work_transfer(&writer, "t", transfer.clone(), 3)
            .await?
            .version,
        2
    );
    let mut changed = transfer;
    changed.reason = "changed retry".into();
    assert!(store.work_transfer(&writer, "t", changed, 3).await.is_err());
    let update = WorkUpdate {
        version: 2,
        reason: "decision".into(),
        patch: WorkPatch {
            state: Some(TaskState::Review),
            ..Default::default()
        },
        resolve_message: None,
    };
    assert!(
        store
            .update_work(&writer, "t", update.clone(), 4)
            .await
            .is_err()
    );
    assert_eq!(store.update_work(&next, "t", update, 4).await?.version, 3);
    let history = store.work_history_page(&owner, "t", None, None).await?;
    assert!(
        history["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["reason"].as_str().unwrap().contains("writer transfer"))
    );
    Ok(())
}

#[tokio::test]
async fn relationships_and_paging_keep_terminal_revision_facts() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "reader", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let reader = store.mailbox("g", "reader").await?;
    for i in 0..25 {
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: format!("t{i:02}"),
                    scope: "review".into(),
                    owner: "writer".into(),
                    state: if i % 2 == 0 {
                        TaskState::Accepted
                    } else {
                        TaskState::Cancelled
                    },
                    next_action: "retained".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1,
            )
            .await?;
    }
    let mut count = 0;
    let mut cursor = None;
    loop {
        let page = store
            .work_list_page(
                &reader,
                WorkListQuery {
                    cursor: cursor.clone(),
                    limit: Some(7),
                    ..Default::default()
                },
            )
            .await?;
        count += page["items"].as_array().unwrap().len();
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 25);
    let relation = RelationUpdate {
        version: 1,
        target: "t01".into(),
        target_version: 1,
        kind: RelationKind::Parent,
        review_round: "round1".into(),
        source_revision: "a".into(),
        active: true,
        reason: "review belongs to lane".into(),
    };
    store
        .work_relation(&writer, "t00", relation.clone(), 2)
        .await?;
    assert_eq!(
        store
            .work_relation(&writer, "t00", relation.clone(), 3)
            .await?["version"],
        2
    );
    let mut changed_relation = relation;
    changed_relation.reason = "different retry".into();
    assert!(
        store
            .work_relation(&writer, "t00", changed_relation, 3)
            .await
            .is_err()
    );
    let query = RelationQuery {
        source_revision: Some("a".into()),
        ..Default::default()
    };
    assert_eq!(
        store.work_relations(&reader, "t01", query).await?["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let query = RelationQuery {
        source_revision: Some("b".into()),
        ..Default::default()
    };
    assert_eq!(
        store.work_relations(&reader, "t01", query).await?["items"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let cycle = RelationUpdate {
        version: 1,
        target: "t00".into(),
        target_version: 2,
        kind: RelationKind::Parent,
        review_round: "".into(),
        source_revision: "".into(),
        active: true,
        reason: "bad cycle".into(),
    };
    assert!(store.work_relation(&writer, "t01", cycle, 3).await.is_err());
    for version in 2..27 {
        store
            .update_work(
                &writer,
                "t00",
                WorkUpdate {
                    version,
                    reason: format!("revision {version}"),
                    patch: WorkPatch {
                        next_action: Some(format!("action {version}")),
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                version,
            )
            .await?;
    }
    let first = store
        .work_history_page(&reader, "t00", None, Some(7))
        .await?;
    let cursor = first["next_cursor"].as_str().unwrap();
    assert!(
        store
            .work_history_page(&reader, "t01", Some(cursor), Some(7))
            .await
            .is_err()
    );
    store
        .update_work(
            &writer,
            "t00",
            WorkUpdate {
                version: 27,
                reason: "concurrent".into(),
                patch: WorkPatch::default(),
                resolve_message: None,
            },
            28,
        )
        .await?;
    let mut seen = first["items"].as_array().unwrap().len();
    let mut cursor = Some(cursor.to_owned());
    while let Some(c) = cursor {
        let page = store
            .work_history_page(&reader, "t00", Some(&c), Some(7))
            .await?;
        seen += page["items"].as_array().unwrap().len();
        cursor = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(seen, 27);
    Ok(())
}

#[tokio::test]
async fn multiple_prerequisites_survive_restart_and_do_not_accept_holds() -> Result<()> {
    use agent_mail::followup::{
        self, Checkpoint, PrerequisiteMode, Source, TaskPrerequisite, WaitFor,
    };
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let now = agent_mail::now()?;
    for id in ["held", "a", "b"] {
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: id.into(),
                    scope: "review".into(),
                    owner: "writer".into(),
                    state: TaskState::Blocked,
                    next_action: "wait".into(),
                    deadline: None,
                    evidence: vec![],
                },
                now,
            )
            .await?;
    }
    store
        .checkpoint(
            &writer,
            Source::Task {
                id: "held".into(),
                version: 1,
            },
            "all",
            Checkpoint {
                version: 0,
                next_step: "reassess after both".into(),
                next_check_at: now + 60,
                waiting: Some(WaitFor::Tasks {
                    mode: PrerequisiteMode::All,
                    tasks: vec![
                        TaskPrerequisite {
                            id: "a".into(),
                            states: vec![TaskState::Accepted],
                            accepted_revision: Some("rev-a".into()),
                        },
                        TaskPrerequisite {
                            id: "b".into(),
                            states: vec![TaskState::Done],
                            accepted_revision: None,
                        },
                    ],
                }),
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            now,
        )
        .await?;
    let cycle = Checkpoint {
        version: 0,
        next_step: "bad cycle".into(),
        next_check_at: now + 60,
        waiting: Some(WaitFor::Task {
            id: "held".into(),
            states: vec![TaskState::Accepted],
        }),
        evidence: vec![],
        extend_until: None,
        reason: None,
    };
    assert!(
        store
            .checkpoint(
                &writer,
                Source::Task {
                    id: "a".into(),
                    version: 1
                },
                "cycle",
                cycle,
                now
            )
            .await
            .is_err()
    );
    store.close().await;
    let store = Store::open(temp.path(), false).await?;
    for (id, state, revision) in [
        ("a", TaskState::Accepted, Some(Some("rev-a".into()))),
        ("b", TaskState::Done, None),
    ] {
        store
            .update_work(
                &writer,
                id,
                WorkUpdate {
                    version: 1,
                    reason: "finished".into(),
                    patch: WorkPatch {
                        state: Some(state),
                        accepted_revision: revision,
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                now + 1,
            )
            .await?;
    }
    followup::reconcile(&store, now + 2).await?;
    assert_eq!(
        store.work_show(&writer, "held").await?.state,
        TaskState::Blocked
    );
    let plan = store.source_followup(&writer, Some("held"), None).await?;
    assert_eq!(plan["checkpoint"]["waiting"]["mode"], "all");
    Ok(())
}

#[tokio::test]
async fn linked_message_paging_never_exposes_private_payloads() -> Result<()> {
    use agent_mail::store::Publish;
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    for id in ["writer", "recipient", "reader"] {
        store.register("g", id, false).await?;
    }
    let writer = store.mailbox("g", "writer").await?;
    let reader = store.mailbox("g", "reader").await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "t".into(),
                scope: "public facts".into(),
                owner: "writer".into(),
                state: TaskState::Done,
                next_action: "audit".into(),
                deadline: None,
                evidence: vec![],
            },
            1,
        )
        .await?;
    for i in 0..25 {
        store
            .publish(
                &writer,
                Publish {
                    intent: agent_mail::states::MessageIntent::Request,
                    recipients: vec!["recipient".into()],
                    key: format!("msg{i}"),
                    summary: "private summary".into(),
                    body: "private body".into(),
                    due_after: None,
                    context: agent_mail::mail_context::ContextSource::Task {
                        id: "t".parse().unwrap(),
                        version: 1.try_into().unwrap(),
                    },
                },
                2,
            )
            .await?;
    }
    let mut count = 0;
    let mut cursor = None;
    loop {
        let page = store
            .work_messages_page(&reader, "t", cursor.as_deref(), Some(6))
            .await?;
        for item in page["items"].as_array().unwrap() {
            assert!(item.get("body").is_none());
            assert!(item.get("summary").is_none());
            assert_eq!(item["body_visible"], false);
            count += 1;
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 25);
    Ok(())
}

#[tokio::test]
async fn transfer_to_remote_writer_is_rejected_without_task_changes() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
    let before = store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "review".into(),
                owner: "writer".into(),
                state: TaskState::Blocked,
                next_action: "wait approval".into(),
                deadline: Some(1000),
                evidence: vec!["revision:a".into()],
            },
            1,
        )
        .await?;
    store
        .route("g", "remote-writer", uuid::Uuid::new_v4(), 2)
        .await?;
    let remote = store.mailbox("g", "remote-writer").await?;
    let transfer = WriterTransfer {
        version: before.version,
        old_writer: before.writer.clone(),
        new_writer: remote.name,
        new_binding_version: remote.binding_version,
        reason: "handoff".into(),
    };
    let error = store
        .work_transfer(&writer, "task", transfer.clone(), 3)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("local mailbox on the home machine")
    );
    assert!(
        store
            .operator_work_transfer("g", "task", transfer, 3)
            .await
            .is_err()
    );
    let after = store.work_show(&writer, "task").await?;
    assert_eq!(after.writer, before.writer);
    assert_eq!(after.version, before.version);
    assert_eq!(after.owner, before.owner);
    assert_eq!(after.state, before.state);
    assert_eq!(after.evidence, before.evidence);
    assert_eq!(after.deadline, before.deadline);
    assert_eq!(store.work_history(&writer, "task").await?.len(), 1);
    Ok(())
}
