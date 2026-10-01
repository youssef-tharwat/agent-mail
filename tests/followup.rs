//! Follow-through contracts at the durable store, event, and CLI boundaries.
mod support;
use agent_mail::{
    followup::{self, Checkpoint, Mode, Policy, Source, WaitFor},
    states::TaskState,
    store::{Mailbox, Publish, Store},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;
use serde_json::{Value, json};

struct Fixture {
    temp: tempfile::TempDir,
    store: Store,
    writer: Mailbox,
    owner: Mailbox,
    time: i64,
}
impl Fixture {
    async fn hook(&self, event: &str, time: i64) -> Result<Value> {
        self.store
            .hook(
                &self.owner,
                serde_json::from_value(json!({
                    "hook_event_name":event,"session_id":"followup-test-session"
                }))?,
                time,
            )
            .await
    }
    async fn new() -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix("am-followup-")
            .tempdir_in("/tmp")?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "owner", false).await?;
        let writer = store.mailbox("g", "writer").await?;
        let owner = store.mailbox("g", "owner").await?;
        let time = agent_mail::now()?;
        store
            .configure_followups(
                "g",
                &Policy {
                    mode: Mode::Enabled,
                    interval_seconds: 60,
                    max_seconds: 240,
                    notifier: None,
                },
                time,
            )
            .await?;
        store.close().await;
        let store = Store::open(temp.path(), false).await?;
        Ok(Self {
            temp,
            store,
            writer,
            owner,
            time,
        })
    }
    async fn task(&self, id: &str) -> Result<()> {
        self.store
            .work_create(
                &self.writer,
                WorkDraft {
                    id: id.into(),
                    scope: "Review changes".into(),
                    owner: "owner".into(),
                    state: TaskState::Active,
                    next_action: "Review changes".into(),
                    deadline: None,
                    evidence: vec![],
                },
                self.time,
            )
            .await?;
        Ok(())
    }
    async fn mail(&self, key: &str) -> Result<i64> {
        self.store
            .publish(
                &self.writer,
                Publish {
                    recipients: vec!["owner".into()],
                    key: key.into(),
                    summary: "Decision needed".into(),
                    body: "Evidence".into(),
                    due_after: None,
                    reply_to: None,
                    work_id: None,
                },
                self.time,
            )
            .await
    }
    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            version: 0,
            next_step: "Inspect the remaining evidence".into(),
            next_check_at: self.time + 90,
            waiting: None,
            evidence: vec![],
            extend_until: None,
            reason: None,
        }
    }
    async fn events(&self, actor: &Mailbox) -> Result<Vec<Value>> {
        Ok(self
            .store
            .notifications(actor, 0)
            .await?
            .iter()
            .filter(|e| e.kind == agent_mail::states::EventKind::AttentionDue)
            .map(|e| json!({"id":e.subject,"version":e.version}))
            .collect())
    }
}
#[tokio::test]
async fn scheduling_respects_checkpoints_pause_and_operator_escalation() -> Result<()> {
    let f = Fixture::new().await?;
    assert_eq!(followup::next_deadline(&f.store, f.time).await?, None);
    f.task("scheduled").await?;
    assert_eq!(
        followup::next_deadline(&f.store, f.time).await?,
        Some(f.time + 240)
    );
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "scheduled".into(),
                version: 1,
            },
            "planned",
            f.checkpoint(),
            f.time + 1,
        )
        .await?;
    assert_eq!(
        followup::next_deadline(&f.store, f.time + 1).await?,
        Some(f.time + 90)
    );
    // An elapsed checkpoint does not cause repeated immediate scans.
    assert_eq!(
        followup::next_deadline(&f.store, f.time + 90).await?,
        Some(f.time + 240)
    );
    let pool = support::pool(&f.store).await?;
    sqlx::query("UPDATE groups SET paused=1 WHERE name='g'")
        .execute(&pool)
        .await?;
    assert_eq!(followup::next_deadline(&f.store, f.time + 1).await?, None);
    sqlx::query("UPDATE groups SET paused=0 WHERE name='g'")
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE followup_policy SET mode='observe' WHERE group_name='g'")
        .execute(&pool)
        .await?;
    assert_eq!(followup::next_deadline(&f.store, f.time + 1).await?, None);
    sqlx::query("UPDATE followup_policy SET mode='enabled' WHERE group_name='g'")
        .execute(&pool)
        .await?;
    followup::reconcile(&f.store, f.time + 240).await?;
    assert_eq!(
        followup::next_deadline(&f.store, f.time + 240).await?,
        Some(f.time + 540)
    );
    sqlx::query("UPDATE attention_occurrences SET operator_state='accepted' WHERE stage=3")
        .execute(&pool)
        .await?;
    assert_eq!(followup::next_deadline(&f.store, f.time + 240).await?, None);
    Ok(())
}

#[tokio::test]
async fn reconciled_attention_notifies_watchers_without_waiting_for_recovery() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("scheduled").await?;
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "scheduled".into(),
                version: 1,
            },
            "planned",
            f.checkpoint(),
            f.time + 1,
        )
        .await?;
    let server = agent_mail::stream::Server::start(f.store.clone())?;
    let mut watch = f.store.watch(&f.owner, None).await?;
    followup::reconcile(&f.store, f.time + 90).await?;
    let batch = tokio::time::timeout(std::time::Duration::from_secs(2), watch.next()).await??;
    assert_eq!(batch.changes.followups.len(), 1);
    assert_eq!(
        f.store.work_show(&f.owner, "scheduled").await?.state,
        TaskState::Active
    );
    drop(watch);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn running_worker_wakes_for_a_new_checkpoint_before_its_recovery_scan() -> Result<()> {
    use std::{process::Stdio, time::Duration};
    let f = Fixture::new().await?;
    f.task("scheduled").await?;
    let mut worker = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .args(["--state-dir"])
        .arg(f.temp.path())
        .args(["service", "run"])
        .env_remove("AGENT_MAIL_SESSION")
        .env_remove("AGENT_MAIL_GROUP")
        .env_remove("HERDR_ENV")
        .env_remove("HERDR_SOCKET_PATH")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f.temp.path().join("service-status.json").exists() {
            anyhow::ensure!(
                worker.try_wait()?.is_none(),
                "worker exited before its first scan"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let mut watch = f.store.watch(&f.owner, None).await?;
    let time = agent_mail::now()?;
    let mut checkpoint = f.checkpoint();
    checkpoint.next_check_at = time + 3;
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "scheduled".into(),
                version: 1,
            },
            "planned",
            checkpoint,
            time,
        )
        .await?;
    let batch = tokio::time::timeout(Duration::from_secs(4), watch.next()).await??;
    assert_eq!(batch.changes.followups.len(), 1);
    assert_eq!(
        f.store.work_show(&f.owner, "scheduled").await?.state,
        TaskState::Active
    );
    worker.kill().await?;
    worker.wait().await?;
    Ok(())
}

#[tokio::test]
async fn completed_turns_cover_only_offered_versions_and_escalate_once() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("offered").await?;
    f.hook("SessionStart", f.time).await?;
    f.task("later").await?;
    f.hook("Stop", f.time + 1).await?;
    let pool = support::pool(&f.store).await?;
    let stages: Vec<(String, i64)> =
        sqlx::query_as("SELECT task,stage FROM followups ORDER BY task")
            .fetch_all(&pool)
            .await?;
    assert_eq!(stages, vec![("later".into(), 0), ("offered".into(), 1)]);
    f.hook("Stop", f.time + 2).await?;
    assert_eq!(
        f.events(&f.owner).await?.len(),
        1,
        "duplicate Stop is idempotent"
    );
    // The corrective attention is actually supplied to a subsequent turn.
    f.hook("UserPromptSubmit", f.time + 3).await?;
    f.hook("Stop", f.time + 4).await?;
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    let stage: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='offered'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(stage, 3);
    assert!(f.store.work_show(&f.owner, "offered").await?.state != TaskState::Done);
    // History survives reopening; replaying Stop cannot create more attention.
    pool.close().await;
    f.store.close().await;
    let store = Store::open(f.temp.path(), false).await?;
    store
        .hook(
            &f.owner,
            serde_json::from_value(
                json!({"hook_event_name":"Stop","session_id":"followup-test-session"}),
            )?,
            f.time + 5,
        )
        .await?;
    assert_eq!(
        store.attention_list(&f.writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn completed_turns_respect_checkpoints_holds_and_changed_versions() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("checkpoint").await?;
    f.task("held").await?;
    f.task("revised").await?;
    f.hook("SessionStart", f.time).await?;
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "checkpoint".into(),
                version: 1,
            },
            "planned",
            f.checkpoint(),
            f.time + 1,
        )
        .await?;
    for (id, state) in [("held", Some(TaskState::Blocked)), ("revised", None)] {
        f.store
            .update_work(
                &f.writer,
                id,
                WorkUpdate {
                    version: 1,
                    reason: "Record current decision".into(),
                    patch: WorkPatch {
                        state,
                        next_action: Some("Wait for the recorded decision".into()),
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                f.time + 1,
            )
            .await?;
    }
    f.hook("Stop", f.time + 2).await?;
    assert!(f.events(&f.owner).await?.is_empty());
    // Offering the current held task still does not authorize implementation.
    f.hook("SessionStart", f.time + 3).await?;
    f.hook("Stop", f.time + 4).await?;
    let pool = support::pool(&f.store).await?;
    let held: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='held'")
        .fetch_one(&pool)
        .await?;
    let planned: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='checkpoint'")
        .fetch_one(&pool)
        .await?;
    assert_eq!((held, planned), (0, 0));
    Ok(())
}

#[tokio::test]
async fn failed_turn_and_wrong_session_do_not_invent_completion() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("work").await?;
    f.hook("SessionStart", f.time).await?;
    f.store
        .hook(
            &f.owner,
            serde_json::from_value(json!({"hook_event_name":"Stop","session_id":"other"}))?,
            f.time + 1,
        )
        .await?;
    assert!(f.events(&f.owner).await?.is_empty());
    f.hook("StopFailure", f.time + 2).await?;
    f.hook("Stop", f.time + 3).await?;
    assert!(f.events(&f.owner).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_new_revision_offered_during_the_same_turn_is_not_hidden_by_the_old_one() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    f.hook("SessionStart", f.time).await?;
    f.store
        .update_work(
            &f.writer,
            "review",
            WorkUpdate {
                version: 1,
                reason: "New review scope".into(),
                patch: WorkPatch {
                    next_action: Some("Check the new revision".into()),
                    ..Default::default()
                },
                resolve_message: None,
            },
            f.time + 1,
        )
        .await?;
    f.hook("PostToolUse", f.time + 2).await?;
    f.hook("Stop", f.time + 3).await?;
    let pool = support::pool(&f.store).await?;
    let versions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM turn_offer_items")
        .fetch_one(&pool)
        .await?;
    assert_eq!(versions, 2);
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn policy_defaults_enabled_and_partial_changes_preserve_routes() -> Result<()> {
    use agent_mail::followup::PolicyPatch;
    let f = Fixture::new().await?;
    f.store.enroll("new", None).await?;
    assert_eq!(f.store.followup_policy("new").await?.mode, Mode::Enabled);
    let route = vec!["/usr/bin/true".into(), "fleet".into()];
    f.store
        .patch_followups(
            "new",
            &PolicyPatch {
                notifier: Some(Some(route.clone())),
                ..Default::default()
            },
            f.time,
        )
        .await?;
    f.store
        .patch_followups(
            "new",
            &PolicyPatch {
                mode: Some(Mode::Observe),
                ..Default::default()
            },
            f.time + 1,
        )
        .await?;
    f.store.enroll("new", None).await?;
    let policy = f.store.followup_policy("new").await?;
    assert_eq!(policy.mode, Mode::Observe);
    assert_eq!(policy.notifier, Some(route));
    assert!(
        f.store
            .patch_followups(
                "new",
                &PolicyPatch {
                    interval_seconds: Some(1000),
                    ..Default::default()
                },
                f.time + 2
            )
            .await
            .is_err()
    );
    assert_eq!(
        f.store.followup_policy("new").await?.interval_seconds,
        900,
        "invalid update rolls back"
    );
    Ok(())
}
#[tokio::test]
async fn retrieved_work_is_reminded_then_escalated_without_business_mutation() -> Result<()> {
    let f = Fixture::new().await?;
    let id = f.mail("result").await?;
    f.store.message(&f.owner, id).await?;
    followup::reconcile(&f.store, f.time + 61).await?;
    let events = f.events(&f.owner).await?;
    assert_eq!(events.len(), 1);
    let occurrence = events[0]["id"].as_str().unwrap().parse()?;
    f.store.attention_show(&f.owner, occurrence).await?;
    assert!(
        f.events(&f.owner).await?.is_empty(),
        "attention retrieval stops its transport retry"
    );
    followup::reconcile(&f.store, f.time + 122).await?;
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    followup::reconcile(&f.store, f.time + 183).await?;
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    let status = f.store.followup_status(Some("g"), f.time + 183).await?;
    assert_eq!(status["items"][0]["escalated"], true);
    assert_eq!(f.store.inbox(&f.owner, 0).await?.len(), 1);
    f.store
        .resolve(&f.owner, id, "Reviewed", None, f.time + 184)
        .await?;
    assert!(
        f.store.attention_list(&f.writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}
#[tokio::test]
async fn escalated_mail_is_visible_to_sender_without_receipting_or_resolving_for_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    let message = f.mail("unread-result").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    let id = f.store.attention_list(&f.writer, 0).await?["items"][0]["id"]
        .as_i64()
        .unwrap();
    assert!(f.store.message(&f.writer, message).await.is_err());
    let detail = f.store.attention_show(&f.writer, id).await?;
    assert_eq!(detail["current"], true);
    assert_eq!(detail["mail"]["id"], message);
    assert_eq!(detail["mail"]["sender"], "writer");
    assert_eq!(detail["mail"]["summary"], "Decision needed");
    assert_eq!(detail["mail"]["body"], "Evidence");
    assert_eq!(detail["mail"]["state"], "pending");
    assert!(
        f.store.followup_status(Some("g"), f.time + 242).await?["items"][0]["retrieved_at"]
            .is_null()
    );
    assert!(
        f.store
            .resolve(
                &f.writer,
                message,
                "cannot act as owner",
                None,
                f.time + 242
            )
            .await
            .is_err()
    );
    assert!(f.store.attention_show(&f.owner, id).await.is_err());
    f.store.register("g", "outsider", false).await?;
    let outsider = f.store.mailbox("g", "outsider").await?;
    assert!(f.store.attention_show(&outsider, id).await.is_err());
    assert_eq!(f.store.inbox(&f.owner, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn self_escalation_receipts_only_the_mail_actually_returned() -> Result<()> {
    let f = Fixture::new().await?;
    let message = f
        .store
        .publish(
            &f.owner,
            Publish {
                recipients: vec!["owner".into()],
                key: "self".into(),
                summary: "Own decision".into(),
                body: "Read this source".into(),
                due_after: None,
                reply_to: None,
                work_id: None,
            },
            f.time,
        )
        .await?;
    f.mail("still-unread").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    let id = f.store.attention_list(&f.owner, 0).await?["items"][0]["id"]
        .as_i64()
        .unwrap();
    let detail = f.store.attention_show(&f.owner, id).await?;
    assert_eq!(detail["mail"]["id"], message);
    assert_eq!(detail["mail"]["body"], "Read this source");
    let status = f.store.followup_status(Some("g"), f.time + 242).await?;
    for plan in status["items"].as_array().unwrap() {
        assert_eq!(plan["retrieved_at"].is_null(), plan["message"] != message);
    }
    assert_eq!(f.store.inbox(&f.owner, 0).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn checkpoint_is_versioned_retry_safe_and_never_grants_writer_authority() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    let source = Source::Task {
        id: "review".into(),
        version: 1,
    };
    let report = f.checkpoint();
    let (a, b) = tokio::join!(
        f.store
            .checkpoint(&f.owner, source.clone(), "cp", report.clone(), f.time),
        f.store
            .checkpoint(&f.owner, source.clone(), "cp", report.clone(), f.time)
    );
    assert_eq!(a?, b?);
    assert_eq!(f.store.work_show(&f.owner, "review").await?.version, 1);
    let mut changed = report.clone();
    changed.next_step = "Different".into();
    assert!(
        f.store
            .checkpoint(&f.owner, source.clone(), "cp", changed, f.time)
            .await
            .is_err()
    );
    assert!(
        f.store
            .checkpoint(&f.owner, source.clone(), "new", report.clone(), f.time)
            .await
            .is_err()
    );
    let mut extend = report;
    extend.version = 1;
    extend.extend_until = Some(f.time + 1000);
    extend.reason = Some("Need more time".into());
    assert!(
        f.store
            .checkpoint(&f.owner, source.clone(), "extend", extend.clone(), f.time)
            .await
            .is_err()
    );
    assert!(
        f.store
            .checkpoint(&f.writer, source, "extend", extend, f.time)
            .await
            .is_ok()
    );
    assert_eq!(
        f.store
            .checkpoint_history(&f.owner, Some("review"), None)
            .await?
            .len(),
        2
    );
    Ok(())
}
#[tokio::test]
async fn dependency_wakes_reassessment_and_cycles_are_rejected() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    f.task("dependency").await?;
    let mut cp = f.checkpoint();
    cp.waiting = Some(WaitFor::Task {
        id: "dependency".into(),
        states: vec![TaskState::Accepted],
    });
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "review".into(),
                version: 1,
            },
            "wait",
            cp.clone(),
            f.time,
        )
        .await?;
    cp.waiting = Some(WaitFor::Task {
        id: "review".into(),
        states: vec![TaskState::Accepted],
    });
    assert!(
        f.store
            .checkpoint(
                &f.owner,
                Source::Task {
                    id: "dependency".into(),
                    version: 1
                },
                "cycle",
                cp,
                f.time
            )
            .await
            .is_err()
    );
    followup::reconcile(&f.store, f.time + 1).await?;
    assert!(f.events(&f.owner).await?.is_empty());
    f.store
        .update_work(
            &f.writer,
            "dependency",
            WorkUpdate {
                version: 1,
                reason: "Verified".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    ..Default::default()
                },
                resolve_message: None,
            },
            f.time + 2,
        )
        .await?;
    followup::reconcile(&f.store, f.time + 3).await?;
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    assert_eq!(
        f.store.work_show(&f.owner, "review").await?.state,
        TaskState::Active
    );
    followup::reconcile(&f.store, f.time + 4).await?;
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    Ok(())
}
#[tokio::test]
async fn holds_escalate_for_review_without_prompting_worker_to_resume() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    let mut cp = f.checkpoint();
    cp.waiting = Some(WaitFor::External {
        responsible: "operator".into(),
        reason: "Deployment approval".into(),
    });
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "review".into(),
                version: 1,
            },
            "hold",
            cp,
            f.time,
        )
        .await?;
    followup::reconcile(&f.store, f.time + 91).await?;
    assert!(f.events(&f.owner).await?.is_empty());
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    assert_eq!(f.store.work_show(&f.owner, "review").await?.version, 1);
    Ok(())
}
#[tokio::test]
async fn restart_preserves_attempts_and_task_revisions_invalidate_old_plans() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    f.store.work_show(&f.owner, "review").await?;
    followup::reconcile(&f.store, f.time + 61).await?;
    let event = f.events(&f.owner).await?[0]["id"]
        .as_str()
        .unwrap()
        .parse::<i64>()?;
    let reopened = Store::open(f.temp.path(), false).await?;
    followup::reconcile(&reopened, f.time + 61).await?;
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    f.store
        .update_work(
            &f.writer,
            "review",
            WorkUpdate {
                version: 1,
                reason: "New assignment".into(),
                patch: WorkPatch {
                    owner: Some("writer".into()),
                    ..Default::default()
                },
                resolve_message: None,
            },
            f.time + 62,
        )
        .await?;
    assert_eq!(
        f.store.attention_show(&f.owner, event).await?["current"],
        false
    );
    assert!(
        f.store
            .checkpoint(
                &f.owner,
                Source::Task {
                    id: "review".into(),
                    version: 1
                },
                "stale",
                f.checkpoint(),
                f.time + 63
            )
            .await
            .is_err()
    );
    assert!(
        f.store.attention_list(&f.owner, 0).await?["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}
#[tokio::test]
async fn unseen_pages_and_operator_reads_never_claim_retrieval() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..8 {
        f.mail(&format!("request-{i}")).await?;
    }
    f.store.followup_status(Some("g"), f.time).await?;
    let before = f.store.followup_status(Some("g"), f.time).await?;
    assert!(
        before["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["retrieved_at"].is_null())
    );
    let context = f.store.context_value(&f.owner, String::new(), 0).await?;
    let visible = context["mail"].as_array().unwrap().len();
    let after = f.store.followup_status(Some("g"), f.time).await?;
    assert_eq!(
        after["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v["retrieved_at"].is_null())
            .count(),
        visible
    );
    assert!(visible < 8);
    Ok(())
}
#[tokio::test]
async fn exhausted_unretrieved_source_escalates_without_new_owner_budget() -> Result<()> {
    let f = Fixture::new().await?;
    f.mail("ignored").await?;
    let pool = support::pool(&f.store).await?;
    sqlx::query("UPDATE mailboxes SET attempts=3,wake_attempted=(SELECT MAX(id) FROM coordination_events WHERE recipient=?) WHERE id=?").bind(f.owner.id).bind(f.owner.id).execute(&pool).await?;
    followup::reconcile(&f.store, f.time + 1).await?;
    assert!(f.events(&f.owner).await?.is_empty());
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    followup::reconcile(&f.store, f.time + 2).await?;
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    assert_eq!(f.store.mailbox("g", "owner").await?.attempts, 3);
    Ok(())
}
#[tokio::test]
async fn self_escalation_is_visible_and_never_creates_recursive_mail() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .work_create(
            &f.writer,
            WorkDraft {
                id: "decision".into(),
                scope: "Decide".into(),
                owner: "writer".into(),
                state: TaskState::Active,
                next_action: "Decide".into(),
                deadline: None,
                evidence: vec![],
            },
            f.time,
        )
        .await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    assert!(f.events(&f.writer).await?.is_empty());
    followup::notify_operators(&f.store, f.time + 241).await?;
    let status = f.store.followup_status(Some("g"), f.time + 241).await?;
    assert_eq!(status["operator_notifications"][0]["state"], "unconfigured");
    assert!(f.store.inbox(&f.writer, 0).await?.is_empty());
    Ok(())
}
#[tokio::test]
async fn observation_mode_and_pause_keep_due_work_visible_without_dispatch() -> Result<()> {
    let f = Fixture::new().await?;
    f.mail("pending").await?;
    f.store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Observe,
                ..Policy::default()
            },
            f.time,
        )
        .await?;
    followup::reconcile(&f.store, f.time + 10000).await?;
    assert!(f.events(&f.writer).await?.is_empty());
    assert_eq!(
        f.store.followup_status(Some("g"), f.time + 10000).await?["items"][0]["due"],
        true
    );
    f.store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Enabled,
                ..Policy::default()
            },
            f.time,
        )
        .await?;
    let pool = support::pool(&f.store).await?;
    sqlx::query("UPDATE groups SET paused=1 WHERE name='g'")
        .execute(&pool)
        .await?;
    followup::reconcile(&f.store, f.time + 10000).await?;
    assert!(f.events(&f.writer).await?.is_empty());
    sqlx::query("UPDATE groups SET paused=0 WHERE name='g'")
        .execute(&pool)
        .await?;
    followup::reconcile(&f.store, f.time + 10000).await?;
    assert_eq!(f.events(&f.writer).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn late_dependency_wakes_owner_once_without_erasing_escalation() -> Result<()> {
    for previously_escalated in [false, true] {
        let f = Fixture::new().await?;
        f.task("review").await?;
        f.task("dependency").await?;
        let mut report = f.checkpoint();
        report.waiting = Some(WaitFor::Task {
            id: "dependency".into(),
            states: vec![TaskState::Accepted],
        });
        f.store
            .checkpoint(
                &f.owner,
                Source::Task {
                    id: "review".into(),
                    version: 1,
                },
                "wait",
                report,
                f.time,
            )
            .await?;
        if previously_escalated {
            followup::reconcile(&f.store, f.time + 91).await?;
        }
        f.store
            .update_work(
                &f.writer,
                "dependency",
                WorkUpdate {
                    version: 1,
                    reason: "Ready".into(),
                    patch: WorkPatch {
                        state: Some(TaskState::Accepted),
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                f.time + 242,
            )
            .await?;
        followup::reconcile(&f.store, f.time + 243).await?;
        let owner = f.store.attention_list(&f.owner, 0).await?;
        assert_eq!(owner["items"].as_array().unwrap().len(), 1);
        assert_eq!(owner["items"][0]["reason"], "dependency_ready");
        assert_eq!(
            f.store.attention_list(&f.writer, 0).await?["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        followup::reconcile(&f.store, f.time + 244).await?;
        assert_eq!(f.store.attention_list(&f.owner, 0).await?, owner);
        let status = f.store.followup_status(Some("g"), f.time + 244).await?;
        assert_eq!(status["totals"]["escalated"], 1);
        assert_eq!(status["items"][0]["escalate_at"], f.time + 240);
    }
    Ok(())
}

#[tokio::test]
async fn operator_retry_budget_survives_restart_and_route_repair() -> Result<()> {
    let f = Fixture::new().await?;
    let policy = Policy {
        mode: Mode::Enabled,
        interval_seconds: 60,
        max_seconds: 240,
        notifier: Some(vec!["/usr/bin/false".into()]),
    };
    f.store.configure_followups("g", &policy, f.time).await?;
    f.mail("unhandled").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    let store = Store::open(f.temp.path(), false).await?;
    for attempt in 0..3 {
        let time = f.time + 542 + attempt * 300;
        followup::notify_operators(&store, time).await?;
        followup::notify_operators(&store, time).await?;
        let status = store.followup_status(Some("g"), time).await?;
        assert_eq!(status["operator_notifications"][0]["attempts"], attempt + 1);
        assert_eq!(status["operator_notifications"][0]["state"], "failed");
    }
    followup::notify_operators(&store, f.time + 2000).await?;
    assert_eq!(
        store.followup_status(Some("g"), f.time + 2000).await?["operator_notifications"][0]["attempts"],
        3
    );
    store
        .configure_followups(
            "g",
            &Policy {
                notifier: Some(vec!["/usr/bin/tee".into()]),
                ..policy
            },
            f.time + 2001,
        )
        .await?;
    followup::notify_operators(&store, f.time + 2001).await?;
    let status = store.followup_status(Some("g"), f.time + 2001).await?;
    assert_eq!(status["operator_notifications"][0]["attempts"], 1);
    assert_eq!(status["operator_notifications"][0]["state"], "accepted");
    assert_eq!(status["totals"]["escalated"], 1);
    assert_eq!(store.inbox(&f.owner, 0).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn interrupted_final_operator_attempt_is_uncertain_and_stays_bounded() -> Result<()> {
    let f = Fixture::new().await?;
    f.mail("unhandled").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    let pool = support::pool(&f.store).await?;
    sqlx::query("UPDATE attention_occurrences SET operator_attempts=3,operator_next=?,operator_state='attempting' WHERE stage=3").bind(f.time + 542).execute(&pool).await?;
    let store = Store::open(f.temp.path(), false).await?;
    followup::notify_operators(&store, f.time + 542).await?;
    let status = store.followup_status(Some("g"), f.time + 542).await?;
    assert_eq!(status["operator_notifications"][0]["attempts"], 3);
    assert_eq!(status["operator_notifications"][0]["state"], "uncertain");
    Ok(())
}

#[tokio::test]
async fn scanner_is_fair_and_status_counts_records_beyond_its_page() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..120 {
        f.mail(&format!("pending-{i}")).await?;
    }
    followup::reconcile(&f.store, f.time + 241).await?;
    let status = f.store.followup_status(Some("g"), f.time + 241).await?;
    assert_eq!(status["totals"]["pending"], 120);
    assert_eq!(status["totals"]["escalated"], 100);
    assert_eq!(status["items"].as_array().unwrap().len(), 100);
    assert_eq!(status["more"], true);
    followup::reconcile(&f.store, f.time + 242).await?;
    assert_eq!(
        f.store.followup_status(Some("g"), f.time + 242).await?["totals"]["escalated"],
        120
    );
    Ok(())
}

#[tokio::test]
async fn identical_checkpoints_and_task_revisions_preserve_the_hard_boundary() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    let source = Source::Task {
        id: "review".into(),
        version: 1,
    };
    let mut report = f.checkpoint();
    f.store
        .checkpoint(&f.owner, source.clone(), "progress", report.clone(), f.time)
        .await?;
    report.version = 1;
    let saved = f
        .store
        .checkpoint(&f.owner, source, "same-progress", report, f.time + 1)
        .await?;
    assert_eq!(saved["version"], 1);
    assert_eq!(saved["escalate_at"], f.time + 240);
    f.store
        .update_work(
            &f.writer,
            "review",
            WorkUpdate {
                version: 1,
                reason: "Clarify next step".into(),
                patch: WorkPatch {
                    next_action: Some("Inspect evidence again".into()),
                    ..Default::default()
                },
                resolve_message: None,
            },
            f.time + 100,
        )
        .await?;
    let saved = f
        .store
        .source_followup(&f.owner, Some("review"), None)
        .await?;
    assert_eq!(saved["next_check"], f.time + 160);
    assert_eq!(saved["escalate_at"], f.time + 240);
    assert!(saved["checkpoint"].is_null());
    Ok(())
}

#[tokio::test]
async fn recovery_accounts_for_attention_metadata_before_receipting_sources() -> Result<()> {
    let f = Fixture::new().await?;
    for i in 0..8 {
        f.task(&format!("review-{i}-{}", "x".repeat(35))).await?;
        f.mail(&format!("pending-{i}")).await?;
    }
    followup::reconcile(&f.store, f.time + 241).await?;
    let view = f.store.context_value(&f.writer, String::new(), 0).await?;
    assert!(serde_json::to_vec(&view)?.len() <= 4096);
    assert!(!view["followups"]["items"].as_array().unwrap().is_empty());
    assert_eq!(view["followups"]["more"], true);
    let view = f.store.context_value(&f.owner, String::new(), 0).await?;
    assert!(serde_json::to_vec(&view)?.len() <= 4096);
    let count = view["work"].as_array().unwrap().len() + view["mail"].as_array().unwrap().len();
    let status = f.store.followup_status(Some("g"), f.time + 242).await?;
    assert_eq!(
        status["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v["retrieved_at"].is_null())
            .count(),
        count
    );
    Ok(())
}
#[tokio::test]
async fn old_event_stream_client_receives_an_explicit_upgrade_error() -> Result<()> {
    use tokio::{io::AsyncWriteExt, net::UnixStream};
    let f = Fixture::new().await?;
    let server = agent_mail::stream::Server::start(f.store.clone())?;
    let mut socket = UnixStream::connect(agent_mail::stream::socket(f.store.root())).await?;
    let mut request = serde_json::to_vec(
        &json!({"method":"subscribe","version":1,"group":"g","participant":"owner","binding":f.owner.binding,"binding_version":f.owner.binding_version,"after":0}),
    )?;
    request.push(b'\n');
    socket.write_all(&request).await?;
    let reply = agent_mail::stream::next(&mut tokio::io::BufReader::new(socket)).await?;
    assert!(
        matches!(reply,agent_mail::stream::Frame::Error{message} if message.contains("upgrade client"))
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn sender_can_record_an_audited_extension_when_a_request_escalates() -> Result<()> {
    let f = Fixture::new().await?;
    let mail = f.mail("await-decision").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    let event = f.events(&f.writer).await?[0]["id"]
        .as_str()
        .unwrap()
        .parse()?;
    let mut report = f.checkpoint();
    report.next_check_at = f.time + 600;
    report.extend_until = Some(f.time + 900);
    report.reason = Some("Operator agreed to review the dependency tomorrow".into());
    let saved = f
        .store
        .checkpoint(
            &f.writer,
            Source::Attention { id: event },
            "extend-request",
            report,
            f.time + 242,
        )
        .await?;
    assert_eq!(saved["escalate_at"], f.time + 900);
    assert_eq!(
        f.store.message(&f.owner, mail).await?.state,
        agent_mail::states::MessageState::Pending
    );
    assert!(
        f.store.attention_list(&f.writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn reply_before_checkpoint_is_not_lost_and_does_not_accept_work() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    let request = f
        .store
        .publish(
            &f.owner,
            Publish {
                recipients: vec!["writer".into()],
                key: "result".into(),
                summary: "Review result".into(),
                body: String::new(),
                due_after: None,
                reply_to: None,
                work_id: Some("review".into()),
            },
            f.time,
        )
        .await?;
    f.store
        .resolve(
            &f.writer,
            request,
            "replied",
            Some(("decision".into(), "Please wait for approval".into())),
            f.time + 1,
        )
        .await?;
    let mut report = f.checkpoint();
    report.waiting = Some(WaitFor::Mail { id: request });
    f.store
        .checkpoint(
            &f.owner,
            Source::Task {
                id: "review".into(),
                version: 1,
            },
            "wait-reply",
            report,
            f.time + 2,
        )
        .await?;
    followup::reconcile(&f.store, f.time + 2).await?;
    assert_eq!(f.events(&f.owner).await?.len(), 1);
    assert_eq!(
        f.store.work_show(&f.owner, "review").await?.state,
        TaskState::Active
    );
    Ok(())
}

#[tokio::test]
async fn cli_records_checkpoint_and_exposes_it_on_current_records() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("review").await?;
    let path = f.temp.path().join("checkpoint.json");
    std::fs::write(&path, serde_json::to_vec(&f.checkpoint())?)?;
    let agent_mail::identity::Binding::Standalone { session } = f.owner.binding else {
        unreachable!()
    };
    let run = |args: Vec<String>| {
        let mut c = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        c.args([
            "--state-dir",
            f.temp.path().to_str().unwrap(),
            "--group",
            "g",
        ])
        .args(args)
        .env("AGENT_MAIL_SESSION", session.to_string())
        .env_remove("AGENT_MAIL_GROUP")
        .env_remove("HERDR_ENV")
        .env_remove("HERDR_PANE_ID");
        c
    };
    let out = run(vec![
        "task".into(),
        "checkpoint".into(),
        "review".into(),
        "--version".into(),
        "1".into(),
        "--key".into(),
        "cli-checkpoint".into(),
        "--file".into(),
        path.to_str().unwrap().into(),
    ])
    .output()
    .await?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(vec!["task".into(), "show".into(), "review".into()])
        .output()
        .await?;
    assert!(out.status.success());
    let value: Value = serde_json::from_slice(&out.stdout)?;
    assert_eq!(value["version"], 1);
    assert_eq!(value["followup"]["version"], 1);
    assert_eq!(
        value["followup"]["checkpoint"]["next_step"],
        "Inspect the remaining evidence"
    );
    Ok(())
}
