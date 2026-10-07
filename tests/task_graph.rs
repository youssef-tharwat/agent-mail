//! Task structure, persistent dependency contracts and attention invalidation.
mod support;
use agent_mail::{
    followup::{self, Checkpoint, Mode, Policy, PrerequisiteMode, Source, WaitFor},
    mail_context::TaskVersion,
    names::{DeliveryConsumer, TaskId},
    states::{AttentionReason, TaskState},
    store::{Mailbox, Store},
    task_graph::{DependencyUpdate, ObservedRequirement, TaskReference, TaskRequirement},
    work::{WorkDraft, WorkPatch, WorkUpdate},
};
use anyhow::Result;

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
    writer: Mailbox,
    owner: Mailbox,
    time: i64,
}

#[tokio::test]
async fn readiness_queries_use_the_source_key_without_materializing_every_plan() -> Result<()> {
    use sqlx::Row;
    let f = Fixture::new().await?;
    f.task("a").await?;
    f.task("b").await?;
    let update = f
        .update(
            "a",
            PrerequisiteMode::All,
            vec![("b", vec![TaskState::Accepted], None)],
        )
        .await?;
    f.store
        .task_dependencies_set(&f.writer, "a", update, f.time)
        .await?;
    let pool = support::pool(&f.store).await?;
    let rows = sqlx::query("EXPLAIN QUERY PLAN SELECT mode,total,satisfied,ready FROM task_readiness WHERE group_name=? AND task=?").bind("g").bind("a").fetch_all(&pool).await?;
    let details: Vec<String> = rows.iter().map(|r| r.get("detail")).collect();
    assert!(
        !details
            .iter()
            .any(|d| d.contains("MATERIALIZE task_dependency_evaluations")),
        "{details:?}"
    );
    assert!(
        details
            .iter()
            .any(|d| d.contains("SEARCH p") && d.contains("group_name=? AND source=?")),
        "{details:?}"
    );
    Ok(())
}

#[tokio::test]
async fn invalid_contracts_fail_before_any_task_or_plan_changes() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("a").await?;
    f.task("b").await?;
    let valid = f
        .update(
            "a",
            PrerequisiteMode::All,
            vec![("b", vec![TaskState::Accepted], None)],
        )
        .await?;
    let mut duplicate = valid.clone();
    duplicate
        .requirements
        .push(duplicate.requirements[0].clone());
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "a", duplicate, f.time)
            .await
            .is_err()
    );
    let mut stale = valid.clone();
    stale.requirements[0].version = TaskVersion::new(2)?;
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "a", stale, f.time)
            .await
            .is_err()
    );
    let mut missing = valid.clone();
    missing.requirements[0].condition.task = TaskId::new("missing")?;
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "a", missing, f.time)
            .await
            .is_err()
    );
    let mut oversized = valid.clone();
    oversized.requirements = vec![valid.requirements[0].clone(); 33];
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "a", oversized, f.time)
            .await
            .unwrap_err()
            .to_string()
            .contains("at most 32")
    );
    let mut empty_states = valid.clone();
    empty_states.requirements[0].condition.states.clear();
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "a", empty_states, f.time)
            .await
            .is_err()
    );
    assert_eq!(f.store.work_show(&f.writer, "a").await?.version, 1);
    assert_eq!(
        f.store.task_dependencies(&f.owner, "a").await?["readiness"]["total"],
        0
    );
    let mut untyped = serde_json::to_value(valid)?;
    untyped["version"] = serde_json::json!(0);
    assert!(serde_json::from_value::<DependencyUpdate>(untyped).is_err());
    Ok(())
}
impl Fixture {
    async fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "owner", false).await?;
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
        let writer = store.mailbox("g", "writer").await?;
        let owner = store.mailbox("g", "owner").await?;
        Ok(Self {
            dir,
            store,
            writer,
            owner,
            time,
        })
    }
    fn draft(id: &str) -> WorkDraft {
        WorkDraft {
            id: id.into(),
            scope: "implement".into(),
            owner: "owner".into(),
            state: TaskState::Open,
            next_action: "implement".into(),
            deadline: None,
            evidence: vec![],
        }
    }
    async fn task(&self, id: &str) -> Result<()> {
        self.store
            .work_create(&self.writer, Self::draft(id), self.time)
            .await?;
        self.store.work_show(&self.owner, id).await?;
        Ok(())
    }
    async fn state(
        &self,
        id: &str,
        state: TaskState,
        revision: Option<&str>,
        offset: i64,
    ) -> Result<()> {
        let item = self.store.work_show(&self.writer, id).await?;
        self.store
            .update_work(
                &self.writer,
                id,
                WorkUpdate {
                    version: item.version,
                    reason: "test outcome".into(),
                    patch: WorkPatch {
                        state: Some(state),
                        accepted_revision: Some(revision.map(str::to_owned)),
                        ..Default::default()
                    },
                    resolve_message: None,
                },
                self.time + offset,
            )
            .await?;
        Ok(())
    }
    async fn update(
        &self,
        task: &str,
        mode: PrerequisiteMode,
        conditions: Vec<(&str, Vec<TaskState>, Option<&str>)>,
    ) -> Result<DependencyUpdate> {
        let item = self.store.work_show(&self.writer, task).await?;
        let mut requirements = Vec::new();
        for (id, states, revision) in conditions {
            let target = self.store.work_show(&self.writer, id).await?;
            requirements.push(ObservedRequirement {
                condition: TaskRequirement {
                    task: TaskId::new(id)?,
                    states,
                    accepted_revision: revision.map(str::to_owned),
                },
                version: TaskVersion::new(target.version)?,
            });
        }
        Ok(DependencyUpdate {
            version: TaskVersion::new(item.version)?,
            mode,
            requirements,
            reason: "set prerequisite contract".into(),
        })
    }
}

#[tokio::test]
async fn plan_outcomes_authority_retries_and_restart() -> Result<()> {
    let f = Fixture::new().await?;
    for id in ["dependent", "a", "b"] {
        f.task(id).await?;
    }
    let update = f
        .update(
            "dependent",
            PrerequisiteMode::All,
            vec![
                ("a", vec![TaskState::Accepted], Some("rev-a")),
                ("b", vec![TaskState::Done], None),
            ],
        )
        .await?;
    assert!(
        f.store
            .task_dependencies_set(&f.owner, "dependent", update.clone(), f.time)
            .await
            .is_err()
    );
    let result = f
        .store
        .task_dependencies_set(&f.writer, "dependent", update.clone(), f.time)
        .await?;
    assert_eq!(result["version"], 2);
    f.state("a", TaskState::Accepted, Some("wrong-revision"), 1)
        .await?;
    f.state("b", TaskState::Cancelled, None, 2).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        false
    );
    // Retry does not revalidate endpoint observations after their progress.
    assert_eq!(
        f.store
            .task_dependencies_set(&f.writer, "dependent", update.clone(), f.time + 3)
            .await?,
        result
    );
    f.state("dependent", TaskState::Active, None, 3).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["total"],
        2
    );
    let mut changed = update;
    changed.reason = "different retry".into();
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "dependent", changed, f.time + 3)
            .await
            .is_err()
    );
    f.state("a", TaskState::Accepted, Some("rev-a"), 4).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        false
    );
    f.state("b", TaskState::Done, None, 5).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        true
    );
    assert_eq!(
        f.store.work_show(&f.owner, "dependent").await?.state,
        TaskState::Active
    );
    let any = f
        .update(
            "dependent",
            PrerequisiteMode::Any,
            vec![
                ("a", vec![TaskState::Cancelled], None),
                ("b", vec![TaskState::Done], None),
            ],
        )
        .await?;
    f.store
        .task_dependencies_set(&f.writer, "dependent", any, f.time + 6)
        .await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        true
    );
    let Fixture {
        dir,
        store,
        writer,
        owner,
        time,
    } = f;
    store.close().await;
    let f = Fixture {
        store: Store::open(dir.path(), false).await?,
        dir,
        writer,
        owner,
        time,
    };
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["plan"]["mode"],
        "any"
    );
    f.state("b", TaskState::Open, None, 7).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        false
    );
    f.state("a", TaskState::Cancelled, None, 8).await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["ready"],
        true
    );
    let clear = f.update("dependent", PrerequisiteMode::All, vec![]).await?;
    f.store
        .task_dependencies_set(&f.writer, "dependent", clear, f.time + 9)
        .await?;
    assert_eq!(
        f.store.task_dependencies(&f.owner, "dependent").await?["readiness"]["total"],
        0
    );
    Ok(())
}

#[tokio::test]
async fn readiness_invalidates_claims_and_rearms_without_extending_deadline() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("dependent").await?;
    f.task("prerequisite").await?;
    let update = f
        .update(
            "dependent",
            PrerequisiteMode::All,
            vec![("prerequisite", vec![TaskState::Done], None)],
        )
        .await?;
    f.store
        .task_dependencies_set(&f.writer, "dependent", update, f.time)
        .await?;
    f.store.work_show(&f.owner, "dependent").await?;
    let boundary = f
        .store
        .source_followup(&f.owner, Some("dependent"), None)
        .await?["escalate_at"]
        .clone();
    followup::reconcile(&f.store, f.time + 61).await?;
    assert!(
        !f.store.attention_list(&f.owner, 0).await?["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["task"] == "dependent")
    );
    f.state("prerequisite", TaskState::Done, None, 62).await?;
    followup::reconcile(&f.store, f.time + 63).await?;
    let batch = f
        .store
        .claim_attention(&f.owner, DeliveryConsumer::Native, f.time + 63)
        .await?
        .unwrap();
    assert!(
        batch
            .attention
            .items
            .iter()
            .any(|i| i.reason == AttentionReason::DependencyReady)
    );
    f.state("prerequisite", TaskState::Open, None, 64).await?;
    assert!(
        f.store
            .acknowledge_attention(&f.owner, &batch.token)
            .await
            .is_err()
    );
    assert!(
        !f.store
            .attention_snapshot(&f.owner)
            .await?
            .items
            .iter()
            .any(|i| i.reason == AttentionReason::DependencyReady)
    );
    f.state("prerequisite", TaskState::Done, None, 65).await?;
    followup::reconcile(&f.store, f.time + 66).await?;
    let ready = f.store.attention_list(&f.owner, 0).await?;
    assert_eq!(ready["items"][0]["reason"], "dependency_ready");
    assert_eq!(
        f.store
            .source_followup(&f.owner, Some("dependent"), None)
            .await?["escalate_at"],
        boundary
    );
    assert_eq!(
        f.store.work_show(&f.owner, "dependent").await?.state,
        TaskState::Open
    );
    Ok(())
}

#[tokio::test]
async fn unmet_plans_and_held_tasks_escalate_to_writer_and_union_cycles_fail() -> Result<()> {
    let f = Fixture::new().await?;
    for id in ["a", "b", "held"] {
        f.task(id).await?;
    }
    let update = f
        .update(
            "a",
            PrerequisiteMode::All,
            vec![("b", vec![TaskState::Accepted], None)],
        )
        .await?;
    f.store
        .task_dependencies_set(&f.writer, "a", update, f.time)
        .await?;
    let cycle = f
        .update(
            "b",
            PrerequisiteMode::All,
            vec![("a", vec![TaskState::Done], None)],
        )
        .await?;
    assert!(
        f.store
            .task_dependencies_set(&f.writer, "b", cycle, f.time)
            .await
            .is_err()
    );
    assert!(
        f.store
            .checkpoint(
                &f.owner,
                Source::Task {
                    id: "b".into(),
                    version: 1
                },
                "cycle",
                Checkpoint {
                    version: 0,
                    next_step: "wait".into(),
                    next_check_at: f.time + 60,
                    waiting: Some(WaitFor::Task {
                        id: "a".into(),
                        states: vec![TaskState::Done]
                    }),
                    evidence: vec![],
                    extend_until: None,
                    reason: None
                },
                f.time
            )
            .await
            .is_err()
    );
    f.store.work_show(&f.owner, "a").await?;
    followup::reconcile(&f.store, f.time + 241).await?;
    assert!(
        f.store.attention_list(&f.writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["task"] == "a" && v["stage"] == 3)
    );
    f.state("b", TaskState::Accepted, Some("rev-b"), 242)
        .await?;
    followup::reconcile(&f.store, f.time + 243).await?;
    assert!(
        f.store.attention_list(&f.owner, 0).await?["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["task"] == "a" && v["reason"] == "dependency_ready")
    );
    f.state("b", TaskState::Open, None, 244).await?;
    followup::reconcile(&f.store, f.time + 245).await?;
    assert!(
        !f.store
            .attention_snapshot(&f.owner)
            .await?
            .items
            .iter()
            .any(|i| i.reason == AttentionReason::DependencyReady)
    );
    assert!(
        f.store.attention_list(&f.writer, 0).await?["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["task"] == "a" && v["stage"] == 3)
    );
    f.state("held", TaskState::Review, None, 246).await?;
    let held = f
        .update(
            "held",
            PrerequisiteMode::All,
            vec![("b", vec![TaskState::Done], None)],
        )
        .await?;
    f.store
        .task_dependencies_set(&f.writer, "held", held, f.time + 247)
        .await?;
    f.store.work_show(&f.owner, "held").await?;
    f.state("b", TaskState::Done, None, 248).await?;
    followup::reconcile(&f.store, f.time + 249).await?;
    assert_eq!(
        f.store.work_show(&f.owner, "held").await?.state,
        TaskState::Review
    );
    let pool = support::pool(&f.store).await?;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM active_attention o JOIN followups f ON f.id=o.followup WHERE f.task='held' AND o.reason='dependency_ready'").fetch_one(&pool).await?, 0);
    Ok(())
}

#[tokio::test]
async fn subtask_creation_is_atomic_and_child_progress_never_accepts_parent() -> Result<()> {
    let f = Fixture::new().await?;
    f.task("parent").await?;
    let stale = TaskReference {
        task: TaskId::new("parent")?,
        version: TaskVersion::new(2)?,
    };
    assert!(
        f.store
            .create_subtask(&f.writer, stale, Fixture::draft("child"), f.time)
            .await
            .is_err()
    );
    assert!(f.store.work_show(&f.writer, "child").await.is_err());
    // Another writer can own the child lifecycle without inheriting parent authority.
    let parent = TaskReference {
        task: TaskId::new("parent")?,
        version: TaskVersion::new(1)?,
    };
    f.store
        .create_subtask(&f.owner, parent.clone(), Fixture::draft("child"), f.time)
        .await?;
    assert_eq!(f.store.work_show(&f.writer, "child").await?.writer, "owner");
    let tree = f.store.task_tree(&f.writer, "parent", 100).await?;
    assert_eq!(tree["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(tree["parents"][0]["child"], "child");
    assert_eq!(
        f.store.task_tree(&f.writer, "parent", 1).await?["more"],
        true
    );
    f.state("parent", TaskState::Active, None, 1).await?;
    assert_eq!(
        f.store
            .create_subtask(&f.owner, parent, Fixture::draft("child"), f.time + 2)
            .await?
            .version,
        1
    );
    f.store
        .update_work(
            &f.owner,
            "child",
            WorkUpdate {
                version: 1,
                reason: "child accepted".into(),
                patch: WorkPatch {
                    state: Some(TaskState::Accepted),
                    ..Default::default()
                },
                resolve_message: None,
            },
            f.time + 3,
        )
        .await?;
    assert!(
        f.store
            .attention_snapshot(&f.writer)
            .await?
            .items
            .iter()
            .any(|i| i.reason == AttentionReason::SubtaskChanged && i.subject == "child")
    );
    let parent = f.store.work_show(&f.writer, "parent").await?;
    assert_eq!(parent.state, TaskState::Active);
    assert_eq!(parent.version, 2);
    assert!(parent.accepted_revision.is_none());
    Ok(())
}
