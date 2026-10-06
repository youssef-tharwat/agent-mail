//! Communication meaning, quiet observation, and shared delivery ownership.
mod support;
use agent_mail::{
    followup::MailPredicate,
    names::DeliveryConsumer,
    states::{AttentionReason, MessageIntent, MessageState},
    store::{Mailbox, Publish, Store},
    watch::WaitOutcome,
};
use anyhow::Result;

async fn setup() -> Result<(tempfile::TempDir, Store, Mailbox, Mailbox, Mailbox)> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(&temp.path().join("state"), true).await?;
    store.enroll("g", None).await?;
    for name in ["writer", "one", "two"] {
        store.register("g", name, false).await?;
    }
    let a = store.mailbox("g", "writer").await?;
    let b = store.mailbox("g", "one").await?;
    let c = store.mailbox("g", "two").await?;
    Ok((temp, store, a, b, c))
}
fn message(to: &str, key: &str, intent: MessageIntent) -> Publish {
    Publish {
        intent,
        recipients: vec![to.into()],
        key: key.into(),
        summary: "Communication".into(),
        body: "Details".into(),
        due_after: None,
        context: agent_mail::mail_context::ContextSource::NewConversation,
    }
}

#[tokio::test]
async fn notices_and_final_answers_do_not_create_reciprocal_obligations() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    let notice = store
        .publish(&a, message("one", "notice", MessageIntent::Notice), 100)
        .await?;
    assert!(store.attention_snapshot(&b).await?.items.is_empty());
    assert!(store.pending().await?.is_empty());
    assert_eq!(store.inbox(&b, 0).await?.len(), 1);
    assert!(store.resolve(&b, notice, "ack", None, 101).await.is_err());
    assert_eq!(
        store.message(&b, notice).await?.intent,
        MessageIntent::Notice
    );
    assert!(store.inbox(&b, 0).await?.is_empty());
    let request = store
        .publish(&a, message("one", "request", MessageIntent::Request), 102)
        .await?;
    let response = store
        .resolve(
            &b,
            request,
            "answered",
            Some(("answer".into(), "Final answer".into())),
            103,
        )
        .await?
        .unwrap();
    assert_eq!(
        store.attention_snapshot(&a).await?.items[0].reason,
        AttentionReason::ResponseAvailable
    );
    assert!(
        store
            .resolve(&a, response, "received", None, 104)
            .await
            .is_err()
    );
    let pool = support::pool(&store).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM followups WHERE message IN (?,?)")
            .bind(notice)
            .bind(response)
            .fetch_one(&pool)
            .await?,
        0
    );
    let batch = store
        .claim_attention(&a, "watch:coordinator".parse()?, 104)
        .await?
        .unwrap();
    store.acknowledge_attention(&a, &batch.token).await?;
    store.acknowledge_attention(&a, &batch.token).await?;
    assert!(store.attention_snapshot(&a).await?.items.is_empty());
    assert_eq!(
        store.inbox(&a, 0).await?.len(),
        1,
        "ingestion of a hint is not retrieval of its answer"
    );
    assert_eq!(store.message(&a, response).await?.body, "Final answer");
    assert!(store.inbox(&a, 0).await?.is_empty());
    assert_eq!(
        store.message(&b, request).await?.state,
        MessageState::Resolved
    );
    Ok(())
}

#[tokio::test]
async fn response_observation_and_request_disposition_remain_distinct() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    let request = store
        .publish(&a, message("one", "request", MessageIntent::Request), 100)
        .await?;
    let mut answer = message("writer", "answer", MessageIntent::Response);
    answer.context = agent_mail::mail_context::ContextSource::Reply {
        message: request.try_into()?,
    };
    let response = store.publish(&b, answer, 101).await?;
    let first_reply = store
        .wait_mail_for(&a, request, None, MailPredicate::FirstReply)
        .await?;
    assert_eq!(first_reply.outcome, WaitOutcome::Reply);
    assert_eq!(first_reply.replies, 1);
    assert_eq!(first_reply.pending, 1);
    assert_eq!(
        store
            .wait_mail_for(
                &a,
                request,
                Some(std::time::Duration::ZERO),
                MailPredicate::AllSettled,
            )
            .await?
            .outcome,
        WaitOutcome::Deadline
    );
    assert_eq!(
        store.resolve(&b, request, "complete", None, 102).await?,
        Some(response)
    );
    assert_eq!(
        store.resolve(&b, request, "complete", None, 103).await?,
        Some(response),
        "retries preserve the authenticated response link"
    );
    let settled = store
        .wait_mail_for(&a, request, None, MailPredicate::AllSettled)
        .await?;
    assert_eq!(settled.outcome, WaitOutcome::Reply);
    assert_eq!(settled.pending, 0);
    assert_eq!(store.inbox(&a, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn competing_delivery_paths_share_a_batch_and_preserve_truncated_records() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    for i in 0..8 {
        store
            .publish(
                &a,
                message("one", &format!("request-{i}"), MessageIntent::Request),
                100,
            )
            .await?;
    }
    let (native, watch) = tokio::join!(
        store.claim_attention(&b, DeliveryConsumer::Native, 100),
        store.claim_attention(&b, "watch:coordinator".parse()?, 100)
    );
    let native = native?;
    let watch = watch?;
    assert_ne!(native.is_some(), watch.is_some());
    let batch = native.or(watch).unwrap();
    assert_eq!(batch.attention.items.len(), 5);
    assert!(batch.attention.more);
    assert_eq!(
        store.attention_snapshot(&b).await?.items.len(),
        5,
        "printing or reserving records creates no receipt"
    );
    store.acknowledge_attention(&b, &batch.token).await?;
    let next = store.attention_snapshot(&b).await?;
    assert_eq!(next.items.len(), 3);
    assert!(!next.more);
    assert!(
        next.items
            .iter()
            .all(|i| !batch.attention.items.contains(i))
    );
    assert_eq!(
        store.inbox(&b, 0).await?.len(),
        6,
        "request dispositions survive transport receipts"
    );
    Ok(())
}

#[tokio::test]
async fn abandoned_delivery_is_bounded_and_old_receipts_cannot_claim_a_replacement() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    store
        .publish(&a, message("one", "request", MessageIntent::Request), 100)
        .await?;
    let first = store
        .claim_attention(&b, "watch:coordinator".parse()?, 100)
        .await?
        .unwrap();
    assert!(
        store
            .claim_attention(&b, DeliveryConsumer::Native, 101)
            .await?
            .is_none()
    );
    let second = store
        .claim_attention(&b, DeliveryConsumer::Native, 400)
        .await?
        .unwrap();
    assert!(store.acknowledge_attention(&b, &first.token).await.is_err());
    let third = store
        .claim_attention(&b, DeliveryConsumer::Native, 700)
        .await?
        .unwrap();
    assert_ne!(second.token, third.token);
    assert!(
        store
            .claim_attention(&b, DeliveryConsumer::Native, 1000)
            .await?
            .is_none()
    );
    assert!(!store.attention_snapshot(&b).await?.items.is_empty());
    Ok(())
}

#[test]
fn required_business_outcomes_are_distinct_from_receipts_and_partial_answers() {
    assert!(!MailPredicate::AllSettled.satisfied(8, 7, 1));
    assert!(MailPredicate::FirstReply.satisfied(8, 7, 1));
    assert!(MailPredicate::AnySettled.satisfied(8, 7, 0));
    assert!(!MailPredicate::FirstReply.satisfied(8, 0, 0));
    assert!(MailPredicate::AllSettled.satisfied(8, 0, 0));
}

#[tokio::test]
async fn any_settled_wait_reports_remaining_obligations_explicitly() -> Result<()> {
    use agent_mail::watch::WaitOutcome;
    let (_temp, store, a, b, c) = setup().await?;
    let mut request = message("one", "fanout", MessageIntent::Request);
    request.recipients.push("two".into());
    let id = store.publish(&a, request, 100).await?;
    store.resolve(&b, id, "done", None, 101).await?;
    let partial = store
        .wait_mail_for(&a, id, None, MailPredicate::AnySettled)
        .await?;
    assert_eq!(partial.outcome, WaitOutcome::PartiallySettled);
    assert_eq!(partial.pending, 1);
    store.resolve(&c, id, "done", None, 102).await?;
    let complete = store
        .wait_mail_for(&a, id, None, MailPredicate::AllSettled)
        .await?;
    assert_eq!(complete.outcome, WaitOutcome::Settled);
    assert_eq!(complete.pending, 0);
    let unanswered = store
        .wait_mail_for(&a, id, None, MailPredicate::FirstReply)
        .await?;
    assert_eq!(unanswered.outcome, WaitOutcome::Unsatisfied);
    Ok(())
}

#[tokio::test]
async fn stored_request_retries_and_intent_conflicts_keep_their_identity() -> Result<()> {
    let (_temp, store, a, _, _) = setup().await?;
    let request = message("one", "key", MessageIntent::Request);
    let json = serde_json::to_value(&request)?;
    assert!(json.get("intent").is_none());
    let old: Publish = serde_json::from_value(json)?;
    let id = store.publish(&a, old, 100).await?;
    assert_eq!(store.publish(&a, request, 200).await?, id);
    assert!(
        store
            .publish(&a, message("one", "key", MessageIntent::Notice), 201)
            .await
            .is_err()
    );
    let mut invalid = message("one", "deadline", MessageIntent::Notice);
    invalid.due_after = Some(60);
    assert!(store.publish(&a, invalid, 201).await.is_err());
    Ok(())
}

#[test]
fn attention_identities_validate_during_construction_and_deserialization() -> Result<()> {
    use agent_mail::names::{GroupName, ParticipantName, SessionId};
    assert_eq!(GroupName::new("awp")?.as_str(), "awp");
    assert!(GroupName::new("bad group").is_err());
    assert!(serde_json::from_str::<GroupName>("\"bad/group\"").is_err());
    assert!(ParticipantName::new("").is_err());
    assert!(SessionId::new("line\nbreak").is_err());
    assert!("arbitrary-consumer".parse::<DeliveryConsumer>().is_err());
    assert!("watch:".parse::<DeliveryConsumer>().is_err());
    let watch: DeliveryConsumer = serde_json::from_str("\"watch:coordinator\"")?;
    assert_eq!(serde_json::to_string(&watch)?, "\"watch:coordinator\"");
    Ok(())
}

#[tokio::test]
async fn fresh_reasons_do_not_restart_exhausted_budgets_or_starve_behind_them() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    let old = store
        .publish(&a, message("one", "old", MessageIntent::Request), 100)
        .await?;
    for time in [100, 400, 700] {
        store
            .claim_attention(&b, DeliveryConsumer::Native, time)
            .await?
            .unwrap();
    }
    let new = store
        .publish(&a, message("one", "fresh", MessageIntent::Request), 999)
        .await?;
    let batch = store
        .claim_attention(&b, DeliveryConsumer::Native, 1000)
        .await?
        .unwrap();
    assert_eq!(batch.attention.items.len(), 1);
    assert_eq!(batch.attention.items[0].subject, new.to_string());
    store.acknowledge_attention(&b, &batch.token).await?;
    assert!(
        store
            .claim_attention(&b, DeliveryConsumer::Native, 1300)
            .await?
            .is_none()
    );
    assert_eq!(
        store.attention_snapshot(&b).await?.items[0].subject,
        old.to_string()
    );
    assert_eq!(store.message(&b, old).await?.state, MessageState::Pending);
    Ok(())
}

#[tokio::test]
async fn a_replacement_binding_cannot_accept_its_predecessors_batch() -> Result<()> {
    let (_temp, store, a, b, _) = setup().await?;
    store
        .publish(&a, message("one", "request", MessageIntent::Request), 100)
        .await?;
    let first = store
        .claim_attention(&b, DeliveryConsumer::Native, 100)
        .await?
        .unwrap();
    store.register("g", "one", true).await?;
    let replacement = store.mailbox("g", "one").await?;
    assert!(store.acknowledge_attention(&b, &first.token).await.is_err());
    assert!(
        store
            .acknowledge_attention(&replacement, &first.token)
            .await
            .is_err()
    );
    let next = store
        .claim_attention(&replacement, DeliveryConsumer::Native, 101)
        .await?
        .unwrap();
    assert_ne!(
        first.attention.binding_version,
        next.attention.binding_version
    );
    assert_eq!(first.attention.items, next.attention.items);
    store
        .acknowledge_attention(&replacement, &next.token)
        .await?;
    assert!(
        store
            .attention_snapshot(&replacement)
            .await?
            .items
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn current_cancellation_preempts_a_normal_lease_and_invalidates_its_receipt() -> Result<()> {
    use agent_mail::{
        states::TaskState,
        work::{WorkDraft, WorkPatch, WorkUpdate},
    };
    let (_temp, store, a, b, _) = setup().await?;
    store
        .publish(&a, message("one", "request", MessageIntent::Request), 100)
        .await?;
    store
        .work_create(
            &a,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "one".into(),
                state: TaskState::Active,
                next_action: "Inspect".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    assert!(
        store.attention_snapshot(&a).await?.items.is_empty(),
        "a confirmed own command is already observed"
    );
    let first = store
        .claim_attention(&b, DeliveryConsumer::Native, 100)
        .await?
        .unwrap();
    store
        .update_work(
            &a,
            "task",
            WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    state: Some(TaskState::Cancelled),
                    ..Default::default()
                },
                reason: "Stop".into(),
                resolve_message: None,
            },
            101,
        )
        .await?;
    let stop = store
        .claim_attention(&b, "watch:coordinator".parse()?, 101)
        .await?
        .unwrap();
    assert_eq!(stop.attention.items[0].reason, AttentionReason::StopWork);
    assert!(store.acknowledge_attention(&b, &first.token).await.is_err());
    store.acknowledge_attention(&b, &stop.token).await?;
    assert_eq!(
        store.attention_snapshot(&b).await?.items.len(),
        1,
        "an omitted cooling-down request is not receipted"
    );
    Ok(())
}

#[tokio::test]
async fn eight_business_dispositions_qualify_one_persisted_all_settled_condition() -> Result<()> {
    use agent_mail::{
        followup::{self, Checkpoint, Source, WaitFor},
        states::{EventKind, TaskState},
        work::WorkDraft,
    };
    let (_temp, store, a, _, _) = setup().await?;
    let mut recipients = Vec::new();
    for i in 0..8 {
        let name = format!("lane-{i}");
        store.register("g", &name, false).await?;
        recipients.push(name);
    }
    let mut request = message("lane-0", "broadcast", MessageIntent::Request);
    request.recipients = recipients.clone();
    let id = store.publish(&a, request, 100).await?;
    store
        .work_create(
            &a,
            WorkDraft {
                id: "train".into(),
                scope: "Continue after acknowledgement".into(),
                owner: "writer".into(),
                state: TaskState::Active,
                next_action: "Wait for every lane".into(),
                deadline: None,
                evidence: vec![],
            },
            100,
        )
        .await?;
    store
        .checkpoint(
            &a,
            Source::Task {
                id: "train".into(),
                version: 1,
            },
            "wait-all",
            Checkpoint {
                version: 0,
                next_step: "Continue the train".into(),
                next_check_at: 200,
                waiting: Some(WaitFor::Mail {
                    id,
                    predicate: MailPredicate::AllSettled,
                }),
                evidence: vec![],
                extend_until: None,
                reason: None,
            },
            100,
        )
        .await?;
    for (index, name) in recipients.iter().enumerate() {
        let lane = store.mailbox("g", name).await?;
        let batch = store
            .claim_attention(&lane, DeliveryConsumer::Native, 101)
            .await?
            .unwrap();
        store.acknowledge_attention(&lane, &batch.token).await?;
        followup::reconcile(&store, 101).await?;
        assert!(
            store.attention_snapshot(&a).await?.items.is_empty(),
            "transport receipts cannot qualify all-settled"
        );
        store
            .resolve(&lane, id, &format!("settled-{index}"), None, 102)
            .await?;
        followup::reconcile(&store, 102).await?;
        let snapshot = store.attention_snapshot(&a).await?;
        if index < 7 {
            assert!(snapshot.items.is_empty(), "partial settlements stay quiet");
        } else {
            assert_eq!(snapshot.items.len(), 1);
            assert_eq!(snapshot.items[0].kind, EventKind::AttentionDue);
            assert_eq!(snapshot.items[0].reason, AttentionReason::DependencyReady);
        }
    }
    let batch = store
        .claim_attention(&a, "watch:coordinator".parse()?, 103)
        .await?
        .unwrap();
    store.acknowledge_attention(&a, &batch.token).await?;
    for time in [104, 105] {
        followup::reconcile(&store, time).await?;
        assert!(store.attention_snapshot(&a).await?.items.is_empty());
    }
    assert!(store.work_show(&a, "train").await?.state.is_open());
    Ok(())
}
