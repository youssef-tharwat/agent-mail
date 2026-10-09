//! Typed results preserve writer obligations through retries, decisions and handoffs.
mod support;
use agent_mail::{
    states::{MessageIntent, MessageState, TaskState},
    store::{Mailbox, Store},
    task_reports::TaskReport,
    work::{WorkDraft, WorkPatch, WorkUpdate, WriterTransfer},
};
use anyhow::Result;

async fn setup() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox, Mailbox)> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("project", None).await?;
    for name in ["writer", "owner", "successor"] {
        store.register("project", name, false).await?;
    }
    let writer = store.mailbox("project", "writer").await?;
    let owner = store.mailbox("project", "owner").await?;
    let successor = store.mailbox("project", "successor").await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Implement the change".into(),
                owner: "owner".into(),
                state: TaskState::Active,
                next_action: "Implement".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    Ok((dir, store, writer, owner, successor))
}

fn report() -> TaskReport {
    TaskReport {
        version: 1.try_into().unwrap(),
        key: "result-1".into(),
        summary: "Revision ready; review required".into(),
        revision: "abc123".into(),
        evidence: vec!["ci/run/42".into()],
        body: "Tests passed. Please review and accept.".into(),
    }
}

#[tokio::test]
async fn result_retrieval_waits_for_an_unrelated_writer_before_recording_receipts() -> Result<()> {
    let (_dir, store, writer, owner, _) = setup().await?;
    let result = store.report_task(&owner, "task", report(), 101).await?;
    let id = result["id"].as_i64().unwrap();
    let pool = support::pool(&store).await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("UPDATE mailboxes SET attempts=attempts WHERE id=?")
        .bind(owner.id)
        .execute(&mut *transaction)
        .await?;
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        transaction.commit().await
    });
    let read = store.task_report_value(&writer, id).await;
    release.await??;
    assert_eq!(read?["disposition"], "pending");
    assert!(store.attention_snapshot(&writer).await?.items.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_writer_transfer_preserves_an_existing_escalation_after_result_retrieval() -> Result<()> {
    use agent_mail::states::AttentionReason;
    let (_dir, store, writer, owner, successor) = setup().await?;
    let submitted = store.report_task(&owner, "task", report(), 101).await?;
    let id = submitted["id"].as_i64().unwrap();
    let initial = store.source_followup(&owner, Some("task"), None).await?;
    let boundary = initial["escalate_at"].as_i64().unwrap();
    agent_mail::followup::reconcile(&store, boundary).await?;
    let prior = store
        .attention_snapshot(&writer)
        .await?
        .items
        .into_iter()
        .find(|item| item.reason == AttentionReason::ReviewDue)
        .expect("the original writer must receive the task escalation");
    store
        .attention_show(&writer, prior.subject.parse()?)
        .await?;
    let pool = support::pool(&store).await?;
    sqlx::query("UPDATE attention_occurrences SET operator_attempts=2,operator_next=?,operator_detail='Earlier notification attempt' WHERE id=?")
        .bind(boundary + 300).bind(prior.subject.parse::<i64>()?).execute(&pool).await?;
    let budget: (Option<i64>, i64, i64, String, Option<String>) = sqlx::query_as("SELECT operator_after,operator_attempts,operator_next,operator_state,operator_detail FROM attention_occurrences WHERE id=?")
        .bind(prior.subject.parse::<i64>()?).fetch_one(&pool).await?;
    let created: i64 = sqlx::query_scalar("SELECT created FROM attention_occurrences WHERE id=?")
        .bind(prior.subject.parse::<i64>()?)
        .fetch_one(&pool)
        .await?;
    let mut last_event = prior.event;
    for (from, to, version) in [(&writer, &successor, 1), (&successor, &writer, 2)] {
        store
            .work_transfer(
                from,
                "task",
                WriterTransfer {
                    version,
                    old_writer: from.name.clone(),
                    new_writer: to.name.clone(),
                    new_binding_version: to.binding_version,
                    reason: "Transfer an overdue result decision".into(),
                },
                boundary + version,
            )
            .await?;
        assert_eq!(
            store.task_report_value(to, id).await?["disposition"],
            "pending"
        );
        store.work_show(to, "task").await?;
        agent_mail::followup::reconcile(&store, boundary + version).await?;
        let due = store
            .attention_snapshot(to)
            .await?
            .items
            .into_iter()
            .find(|item| item.reason == AttentionReason::ReviewDue)
            .expect("reading a result must not erase its transferred task escalation");
        assert_ne!(
            due.event, last_event,
            "a returning writer needs a fresh event"
        );
        last_event = due.event;
        let plan = store.source_followup(to, Some("task"), None).await?;
        assert_eq!(plan["opened"], initial["opened"]);
        assert_eq!(plan["escalate_at"], boundary);
        assert_eq!(plan["stage"], 3);
        assert_eq!(
            sqlx::query_as::<_, (Option<i64>, i64, i64, String, Option<String>)>("SELECT operator_after,operator_attempts,operator_next,operator_state,operator_detail FROM attention_occurrences WHERE id=?")
                .bind(due.subject.parse::<i64>()?).fetch_one(&pool).await?,
            budget
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT created FROM attention_occurrences WHERE id=?")
                .bind(due.subject.parse::<i64>()?)
                .fetch_one(&pool)
                .await?,
            created
        );
        store.attention_show(to, due.subject.parse()?).await?;
    }
    store
        .work_transfer(
            &writer,
            "task",
            WriterTransfer {
                version: 3,
                old_writer: writer.name.clone(),
                new_writer: owner.name.clone(),
                new_binding_version: owner.binding_version,
                reason: "The owner also becomes the decision writer".into(),
            },
            boundary + 3,
        )
        .await?;
    store.task_report_value(&owner, id).await?;
    store.work_show(&owner, "task").await?;
    assert!(
        store.attention_snapshot(&owner).await?.items.is_empty(),
        "self-supervision must retain the operator route without a self-reminder"
    );
    assert!(
        store.attention_list(&owner, 0).await?["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["task"] == "task" && item["stage"] == 3)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT recipient FROM attention_occurrences WHERE id=?")
            .bind(prior.subject.parse::<i64>()?)
            .fetch_one(&pool)
            .await?,
        writer.id,
        "historical attention must retain its original recipient"
    );
    Ok(())
}

#[tokio::test]
async fn reported_revisions_match_the_acceptance_bound_and_metadata_cursors_are_validated()
-> Result<()> {
    let (_dir, store, writer, owner, _) = setup().await?;
    let mut oversized = report();
    oversized.revision = "r".repeat(129);
    assert!(
        store
            .report_task(&owner, "task", oversized, 101)
            .await
            .is_err()
    );
    assert!(store.task_reports(&writer, "task", -1).await.is_err());
    assert!(
        store
            .task_reports(&writer, "invalid task", 0)
            .await
            .is_err()
    );
    let mut valid = report();
    valid.revision = "r".repeat(128);
    let revision = valid.revision.clone();
    let submitted = store.report_task(&owner, "task", valid, 102).await?;
    let accepted = store
        .update_work(
            &writer,
            "task",
            WorkUpdate {
                version: 1,
                reason: "Accept the exact reported revision".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    accepted_revision: Some(Some(revision.clone())),
                    ..Default::default()
                },
                resolve_message: submitted["message"].as_i64(),
            },
            103,
        )
        .await?;
    assert_eq!(
        accepted.accepted_revision.as_deref(),
        Some(revision.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_mail_cannot_preoccupy_a_transferred_result_obligation() -> Result<()> {
    use agent_mail::{mail_context::ContextSource, store::Publish};
    for state in ["withdrawn", "resolved", "conflicting"] {
        let (_dir, store, writer, owner, successor) = setup().await?;
        let submitted = store.report_task(&owner, "task", report(), 101).await?;
        let id = submitted["id"].as_i64().unwrap();
        let original = submitted["message"].as_i64().unwrap();
        let result = store.message(&writer, original).await?;
        let collision = store
            .publish(
                &owner,
                Publish {
                    intent: MessageIntent::Request,
                    recipients: vec!["successor".into()],
                    key: format!("report-transfer:{id}:{original}"),
                    summary: if state == "conflicting" {
                        "Unrelated ordinary request".into()
                    } else {
                        result.summary
                    },
                    body: result.body,
                    due_after: None,
                    context: ContextSource::Task {
                        id: "task".parse()?,
                        version: 1.try_into()?,
                    },
                },
                102,
            )
            .await?;
        match state {
            "withdrawn" => store.withdraw(&owner, collision, 103).await?,
            "resolved" => {
                store
                    .resolve(&successor, collision, "Ordinary request settled", None, 103)
                    .await?;
            }
            _ => {}
        }
        let transfer = WriterTransfer {
            version: 1,
            old_writer: "writer".into(),
            new_writer: "successor".into(),
            new_binding_version: successor.binding_version,
            reason: "Successor reviews the result".into(),
        };
        store
            .work_transfer(&writer, "task", transfer.clone(), 104)
            .await?;
        let forwarded = store.task_report_value(&owner, id).await?;
        assert_eq!(forwarded["disposition"], "pending", "{state}");
        let message = forwarded["message"].as_i64().unwrap();
        assert_ne!(message, collision, "{state}");
        assert_eq!(
            store.message(&writer, original).await?.state,
            MessageState::Withdrawn
        );
        assert!(store.withdraw(&owner, message, 105).await.is_err());
        let pool = support::pool(&store).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM active_followups WHERE message=?")
                .bind(message)
                .fetch_one(&pool)
                .await?,
            1
        );
        store.work_transfer(&writer, "task", transfer, 106).await?;
        assert_eq!(
            store.task_report_value(&owner, id).await?["message"],
            message
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_binding_replacement_cannot_mask_a_committed_report_response() -> Result<()> {
    for _ in 0..8 {
        let (_dir, store, _writer, owner, _) = setup().await?;
        let (submitted, replaced) = tokio::join!(
            store.report_task(&owner, "task", report(), 101),
            store.register("project", "owner", true)
        );
        replaced?;
        let pool = support::pool(&store).await?;
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_reports")
            .fetch_one(&pool)
            .await?;
        match submitted {
            Ok(result) => {
                assert_eq!(count, 1);
                assert_eq!(result["persisted"], true);
            }
            Err(_) => assert_eq!(
                count, 0,
                "an identity error must not hide a committed report"
            ),
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_writer_transfers_do_not_reacquire_the_pool_inside_a_writer_transaction()
-> Result<()> {
    let (_dir, store, writer, _owner, successor) = setup().await?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "other-task".into(),
                scope: "Another independent task".into(),
                owner: "owner".into(),
                state: TaskState::Active,
                next_action: "Implement".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    let transfer = WriterTransfer {
        version: 1,
        old_writer: "writer".into(),
        new_writer: "successor".into(),
        new_binding_version: successor.binding_version,
        reason: "Transfer independent tasks".into(),
    };
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(
            store.work_transfer(&writer, "task", transfer.clone(), 101),
            store.work_transfer(&writer, "other-task", transfer, 101)
        )
    })
    .await?;
    assert_eq!(a?.writer, "successor");
    assert_eq!(b?.writer, "successor");
    Ok(())
}

#[tokio::test]
async fn transferred_results_preserve_the_reporting_owner_for_replies_and_waits() -> Result<()> {
    use agent_mail::watch::WaitOutcome;
    let (_dir, store, writer, owner, successor) = setup().await?;
    let submitted = store.report_task(&owner, "task", report(), 101).await?;
    let id = submitted["id"].as_i64().unwrap();
    store
        .work_transfer(
            &writer,
            "task",
            WriterTransfer {
                version: 1,
                old_writer: "writer".into(),
                new_writer: "successor".into(),
                new_binding_version: successor.binding_version,
                reason: "Successor reviews the result".into(),
            },
            102,
        )
        .await?;
    let message = store.task_report_value(&owner, id).await?["message"]
        .as_i64()
        .unwrap();
    assert_eq!(
        store
            .wait_mail(&owner, message, Some(std::time::Duration::ZERO))
            .await?
            .outcome,
        WaitOutcome::Deadline
    );
    let response = store
        .resolve(
            &successor,
            message,
            "Changes required",
            Some((
                "review-response".into(),
                "Please correct the edge case".into(),
            )),
            103,
        )
        .await?
        .unwrap();
    assert_eq!(
        store.message(&owner, response).await?.body,
        "Please correct the edge case"
    );
    assert!(store.message(&writer, response).await.is_err());
    assert_eq!(
        store
            .wait_mail(&owner, message, Some(std::time::Duration::ZERO))
            .await?
            .outcome,
        WaitOutcome::Reply
    );
    assert_eq!(
        store.work_show(&successor, "task").await?.state,
        TaskState::Active
    );
    Ok(())
}

#[tokio::test]
async fn a_result_invalidates_an_already_published_owner_reminder_without_erasing_escalation()
-> Result<()> {
    let (_dir, store, writer, owner, _) = setup().await?;
    store.work_show(&owner, "task").await?;
    let plan = store.source_followup(&owner, Some("task"), None).await?;
    let boundary = plan["escalate_at"].as_i64().unwrap();
    let checkpoint = serde_json::from_value(
        serde_json::json!({"version":plan["version"],"next_step":"Implementing","next_check_at":200,"waiting":null,"evidence":[]}),
    )?;
    store
        .checkpoint(
            &owner,
            agent_mail::followup::Source::Task {
                id: "task".into(),
                version: 1,
            },
            "owner-progress",
            checkpoint,
            101,
        )
        .await?;
    agent_mail::followup::reconcile(&store, 200).await?;
    assert!(
        store
            .attention_snapshot(&owner)
            .await?
            .items
            .iter()
            .any(|item| item.kind == agent_mail::states::EventKind::AttentionDue)
    );
    store.report_task(&owner, "task", report(), 201).await?;
    assert!(
        store.attention_snapshot(&owner).await?.items.is_empty(),
        "a published reminder must stop asking an owner to redo submitted work"
    );
    agent_mail::followup::reconcile(&store, boundary).await?;
    let pool = support::pool(&store).await?;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE f.task='task' AND o.stage=3 AND o.recipient=?").bind(writer.id).fetch_one(&pool).await?,1);
    assert_eq!(
        store.source_followup(&owner, Some("task"), None).await?["escalate_at"],
        boundary
    );
    Ok(())
}

#[tokio::test]
async fn submission_and_cancellation_serialize_without_reviving_cancelled_work() -> Result<()> {
    let (_dir, store, writer, owner, _) = setup().await?;
    let cancel = WorkUpdate {
        version: 1,
        reason: "The assignment is obsolete".into(),
        patch: WorkPatch {
            state: Some(TaskState::Cancelled),
            ..Default::default()
        },
        resolve_message: None,
    };
    let (result, cancelled) = tokio::join!(
        store.report_task(&owner, "task", report(), 101),
        store.update_work(&writer, "task", cancel, 102)
    );
    cancelled?;
    assert_eq!(
        store.work_show(&writer, "task").await?.state,
        TaskState::Cancelled
    );
    let pool = support::pool(&store).await?;
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_reports")
        .fetch_one(&pool)
        .await?;
    match result {
        Ok(result) => {
            assert_eq!(count, 1);
            assert_eq!(
                store
                    .task_report_value(&owner, result["id"].as_i64().unwrap())
                    .await?["disposition"],
                "pending"
            );
        }
        Err(_) => assert_eq!(count, 0),
    }
    Ok(())
}

#[tokio::test]
async fn writer_handoff_does_not_make_superseded_evidence_hold_a_new_assignment() -> Result<()> {
    let (_dir, store, writer, owner, successor) = setup().await?;
    let old = store.report_task(&owner, "task", report(), 101).await?;
    store
        .update_work(
            &writer,
            "task",
            WorkUpdate {
                version: 1,
                reason: "New requirements need a fresh revision".into(),
                patch: WorkPatch {
                    next_action: Some("Implement updated requirements".into()),
                    ..Default::default()
                },
                resolve_message: None,
            },
            102,
        )
        .await?;
    let mut current = report();
    current.version = 2.try_into()?;
    current.key = "result-2".into();
    current.revision = "def456".into();
    let current = store.report_task(&owner, "task", current, 103).await?;
    store
        .work_transfer(
            &writer,
            "task",
            WriterTransfer {
                version: 2,
                old_writer: "writer".into(),
                new_writer: "successor".into(),
                new_binding_version: successor.binding_version,
                reason: "Review the current and superseded evidence".into(),
            },
            104,
        )
        .await?;
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT authority_version FROM task_reports WHERE message=?")
            .bind(old["id"].as_i64().unwrap())
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT authority_version FROM task_reports WHERE message=?")
            .bind(current["id"].as_i64().unwrap())
            .fetch_one(&pool)
            .await?,
        3
    );
    assert_eq!(
        store
            .task_report_value(&successor, old["id"].as_i64().unwrap())
            .await?["disposition"],
        "pending"
    );
    Ok(())
}

#[tokio::test]
async fn report_is_atomic_retry_safe_and_requires_a_writer_decision_after_restart() -> Result<()> {
    let (dir, store, writer, owner, _) = setup().await?;
    let (first, retry) = tokio::join!(
        store.report_task(&owner, "task", report(), 101),
        store.report_task(&owner, "task", report(), 102)
    );
    let first = first?;
    assert_eq!(first, retry?);
    let id = first["id"].as_i64().unwrap();
    assert!(
        store.withdraw(&owner, id, 103).await.is_err(),
        "an owner cannot erase the writer's result obligation"
    );
    assert_eq!(
        store.work_show(&owner, "task").await?.state,
        TaskState::Active
    );
    assert_eq!(store.inbox(&writer, 0).await?.len(), 1);
    assert_eq!(store.attention_snapshot(&writer).await?.items.len(), 1);
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_reports")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM followups WHERE message=?")
            .bind(id)
            .fetch_one(&pool)
            .await?,
        1
    );
    pool.close().await;
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    assert_eq!(
        store.report_task(&owner, "task", report(), 999).await?["id"],
        id
    );
    assert_eq!(
        store.message(&writer, id).await?.intent,
        MessageIntent::Request
    );
    assert_eq!(
        store.message(&writer, id).await?.state,
        MessageState::Pending
    );
    assert!(
        store.attention_snapshot(&writer).await?.items.is_empty(),
        "retrieval suppresses hints, while the decision obligation remains"
    );
    assert_eq!(
        store.task_report_value(&owner, id).await?["disposition"],
        "pending"
    );
    store
        .update_work(
            &writer,
            "task",
            WorkUpdate {
                version: 1,
                reason: "Reviewed evidence".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    accepted_revision: Some(Some("abc123".into())),
                    ..Default::default()
                },
                resolve_message: Some(id),
            },
            1000,
        )
        .await?;
    assert_eq!(
        store.report_task(&owner, "task", report(), 1001).await?["disposition"],
        "resolved"
    );
    assert_eq!(
        store
            .work_show(&writer, "task")
            .await?
            .accepted_revision
            .as_deref(),
        Some("abc123")
    );
    let mut changed = report();
    changed.revision = "different".into();
    assert!(
        store
            .report_task(&owner, "task", changed, 1002)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn stale_unauthorized_and_quiet_results_cannot_create_partial_obligations() -> Result<()> {
    let (_dir, store, writer, owner, other) = setup().await?;
    assert!(
        store
            .report_task(&other, "task", report(), 101)
            .await
            .is_err()
    );
    let mut bad = report();
    bad.version = 2.try_into()?;
    assert!(store.report_task(&owner, "task", bad, 101).await.is_err());
    let mut bad = report();
    bad.body = "x".repeat(agent_mail::BODY_LIMIT);
    assert!(store.report_task(&owner, "task", bad, 101).await.is_err());
    let mut input = serde_json::to_value(report())?;
    input["intent"] = "notice".into();
    assert!(serde_json::from_value::<TaskReport>(input).is_err());
    assert!(store.inbox(&writer, 0).await?.is_empty());
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_reports")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
            .fetch_one(&pool)
            .await?,
        0
    );
    store
        .update_work(
            &writer,
            "task",
            WorkUpdate {
                version: 1,
                reason: "Cancel obsolete work".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Cancelled),
                    ..Default::default()
                },
                resolve_message: None,
            },
            102,
        )
        .await?;
    assert!(
        store
            .report_task(&owner, "task", report(), 103)
            .await
            .is_err()
    );
    let mut current = report();
    current.version = 2.try_into()?;
    assert!(
        store
            .report_task(&owner, "task", current, 103)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn result_body_is_private_and_writer_transfer_preserves_the_obligation_and_boundary()
-> Result<()> {
    let (_dir, store, writer, owner, successor) = setup().await?;
    let first = store.report_task(&owner, "task", report(), 101).await?;
    let id = first["id"].as_i64().unwrap();
    assert!(store.task_report_value(&successor, id).await.is_err());
    assert_eq!(
        store.task_reports(&successor, "task", 0).await?["items"][0]["revision"],
        "abc123"
    );
    let pool = support::pool(&store).await?;
    let original: i64 = sqlx::query_scalar("SELECT escalate_at FROM followups WHERE message=?")
        .bind(id)
        .fetch_one(&pool)
        .await?;
    let transfer = WriterTransfer {
        version: 1,
        old_writer: "writer".into(),
        new_writer: "successor".into(),
        new_binding_version: successor.binding_version,
        reason: "Successor reviews this task and its submitted results".into(),
    };
    store
        .work_transfer(&writer, "task", transfer.clone(), 200)
        .await?;
    store.work_transfer(&writer, "task", transfer, 201).await?;
    let moved = store.task_report_value(&owner, id).await?;
    let next = moved["message"].as_i64().unwrap();
    assert_ne!(next, id);
    assert_eq!(
        moved["report"], first["report"],
        "the reported evidence and observed version stay immutable"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT escalate_at FROM followups WHERE message=?")
            .bind(next)
            .fetch_one(&pool)
            .await?,
        original
    );
    assert_eq!(
        store.message(&writer, id).await?.state,
        MessageState::Withdrawn
    );
    assert_eq!(
        store.message(&successor, next).await?.state,
        MessageState::Pending
    );
    assert!(store.task_report_value(&writer, id).await.is_err());
    assert_eq!(
        store.task_report_value(&successor, id).await?["disposition"],
        "pending"
    );
    assert_eq!(
        store.report_task(&owner, "task", report(), 202).await?["id"],
        id
    );
    assert!(
        store
            .update_work(
                &writer,
                "task",
                WorkUpdate {
                    version: 2,
                    reason: "Old writer cannot accept".into(),
                    patch: WorkPatch {
                        state: Some(TaskState::Accepted),
                        ..Default::default()
                    },
                    resolve_message: Some(next)
                },
                203
            )
            .await
            .is_err()
    );
    store
        .update_work(
            &successor,
            "task",
            WorkUpdate {
                version: 2,
                reason: "Accept reviewed evidence".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    ..Default::default()
                },
                resolve_message: Some(next),
            },
            203,
        )
        .await?;
    assert_eq!(
        store.task_report_value(&owner, id).await?["disposition"],
        "resolved"
    );
    Ok(())
}

#[tokio::test]
async fn a_pending_result_holds_owner_reminders_and_escalates_to_its_writer() -> Result<()> {
    let (_dir, store, writer, owner, _) = setup().await?;
    store.report_task(&owner, "task", report(), 101).await?;
    store.work_show(&owner, "task").await?;
    let plan = store.source_followup(&owner, Some("task"), None).await?;
    let time = plan["escalate_at"].as_i64().unwrap();
    agent_mail::followup::reconcile(&store, time).await?;
    let pool = support::pool(&store).await?;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE f.task='task' AND o.recipient=? AND o.stage=3").bind(writer.id).fetch_one(&pool).await?,1);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM attention_occurrences o JOIN followups f ON f.id=o.followup WHERE f.task='task' AND o.recipient=? AND o.stage<3").bind(owner.id).fetch_one(&pool).await?,0);
    assert_eq!(
        store.work_show(&writer, "task").await?.state,
        TaskState::Active
    );
    Ok(())
}
