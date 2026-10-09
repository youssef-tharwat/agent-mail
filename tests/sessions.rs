//! Shared session delivery preserves project isolation, opt-outs and generation checks.
mod support;
use agent_mail::{
    herdr::{Agent, AgentStatus, Session},
    states::{SessionKind, TaskState},
    store::Store,
    task_reports::TaskReport,
    work::WorkDraft,
};
use anyhow::Result;
use std::path::Path;

fn agent(pane: &str, session: &str) -> Agent {
    Agent {
        pane_id: pane.into(),
        terminal_id: format!("term-{pane}"),
        agent: Some("claude".into()),
        agent_session: Some(Session {
            agent: "claude".into(),
            kind: SessionKind::Id,
            value: session.into(),
        }),
        agent_status: AgentStatus::Idle,
        interactive_ready: Some(true),
        launch_pending: false,
        cwd: None,
    }
}

#[tokio::test]
async fn session_attention_pages_within_its_budget_and_rejects_unbounded_cursors() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    let shared = agent("w1:p1", "paged-session");
    let mut expected = Vec::new();
    for i in 0..12 {
        let group = format!("project-{i:02}");
        expected.push(group.clone());
        store
            .enroll(&group, Some(Path::new("/tmp/paged-session.sock")))
            .await?;
        store.bind(&group, "coordinator", &shared, false).await?;
        store.register(&group, "worker", false).await?;
        let worker = store.mailbox(&group, "worker").await?;
        for j in 0..5 {
            store
                .work_create(
                    &worker,
                    WorkDraft {
                        id: format!("task-{j}-{}", "x".repeat(41)),
                        scope: "Assignment requiring attention".into(),
                        owner: "coordinator".into(),
                        state: TaskState::Active,
                        next_action: "Implement".into(),
                        deadline: None,
                        evidence: vec![],
                    },
                    100,
                )
                .await?;
        }
    }
    let actor = store.mailbox(&expected[0], "coordinator").await?;
    let pool = support::pool(&store).await?;
    let receipts_before = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_receipts")
        .fetch_one(&pool)
        .await?;
    let mut after = String::new();
    let mut observed = Vec::new();
    loop {
        let page = store.session_attention(&actor, &after).await?;
        assert!(serde_json::to_vec(&page)?.len() <= 4096);
        assert_eq!(page["retrieved"], false);
        for group in page["groups"].as_array().unwrap() {
            observed.push(group["group"].as_str().unwrap().to_owned());
        }
        if page["more"] == false {
            break;
        }
        let next = page["next_after_group"].as_str().unwrap();
        assert!(next > after.as_str(), "a truncated page must progress");
        after = next.into();
    }
    assert_eq!(observed, expected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_receipts")
            .fetch_one(&pool)
            .await?,
        receipts_before
    );
    assert!(
        store
            .session_attention(&actor, &"x".repeat(5000))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn four_groups_share_exact_session_consent_and_attention_without_sharing_receipts()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    let shared = agent("w1:p1", "shared-session");
    let socket = Path::new("/tmp/session-test-herdr.sock");
    let mut coordinators = Vec::new();
    let mut results = Vec::new();
    for group in ["neola-main", "neola-intake", "neola-gap", "neola-auskunft"] {
        store.enroll(group, Some(socket)).await?;
        store.bind(group, "coordinator", &shared, false).await?;
        store.register(group, "worker", false).await?;
        let coordinator = store.mailbox(group, "coordinator").await?;
        let worker = store.mailbox(group, "worker").await?;
        store
            .work_create(
                &coordinator,
                WorkDraft {
                    id: "task".into(),
                    scope: "Deliver this project's change".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Implement".into(),
                    deadline: None,
                    evidence: vec![],
                },
                100,
            )
            .await?;
        let result = store
            .report_task(
                &worker,
                "task",
                TaskReport {
                    version: 1.try_into()?,
                    key: "same-key".into(),
                    summary: "Review required".into(),
                    revision: group.into(),
                    evidence: vec![],
                    body: format!("Private result for {group}"),
                },
                101,
            )
            .await?;
        results.push(result["id"].as_i64().unwrap());
        coordinators.push(coordinator);
    }
    store
        .set_herdr_prompt_policy(&coordinators[0], true)
        .await?;
    for actor in &coordinators {
        assert!(store.herdr_prompt_enabled(actor).await?);
    }
    let all = store.session_attention(&coordinators[0], "").await?;
    assert_eq!(all["total_bindings"], 4);
    assert_eq!(all["groups"].as_array().unwrap().len(), 4);
    assert_eq!(all["retrieved"], false);
    for (actor, id) in coordinators.iter().zip(&results) {
        assert_eq!(store.attention_snapshot(actor).await?.items.len(), 1);
        let inbox = store.inbox(actor, 0).await?;
        assert_eq!(inbox[0].id, *id);
        assert_eq!(inbox[0].context.task_id().unwrap().as_str(), "task");
    }
    assert!(
        store.message(&coordinators[0], results[1]).await.is_err(),
        "overlapping names never authorize another group's mailbox"
    );
    store.pause(&coordinators[1].group_name, true).await?;
    assert!(
        store
            .claim_attention(
                &coordinators[1],
                agent_mail::names::DeliveryConsumer::Herdr,
                102
            )
            .await?
            .is_none()
    );
    store.set_runtime_enabled(&coordinators[2], false).await?;
    assert!(
        store
            .claim_attention(
                &coordinators[2],
                agent_mail::names::DeliveryConsumer::Herdr,
                102
            )
            .await?
            .is_none()
    );
    assert!(
        store
            .claim_attention(
                &coordinators[3],
                agent_mail::names::DeliveryConsumer::Herdr,
                102
            )
            .await?
            .is_some()
    );
    assert!(store.group(&coordinators[1].group_name).await?.paused != 0);
    assert!(!store.runtime_enabled(&coordinators[2]).await?);
    Ok(())
}

#[tokio::test]
async fn another_pane_socket_session_or_credential_cannot_join_the_session_view() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    let shared = agent("w1:p1", "original");
    for group in ["a", "b", "other-pane", "other-socket", "other-session"] {
        store
            .enroll(
                group,
                Some(if group == "other-socket" {
                    Path::new("/tmp/other-session.sock")
                } else {
                    Path::new("/tmp/first-session.sock")
                }),
            )
            .await?;
    }
    store.bind("a", "coordinator", &shared, false).await?;
    store.bind("b", "coordinator", &shared, false).await?;
    store
        .bind(
            "other-pane",
            "coordinator",
            &agent("w1:p2", "original"),
            false,
        )
        .await?;
    store
        .bind("other-socket", "coordinator", &shared, false)
        .await?;
    store
        .bind(
            "other-session",
            "coordinator",
            &agent("w1:p1", "different"),
            false,
        )
        .await?;
    let a = store.mailbox("a", "coordinator").await?;
    store.set_herdr_prompt_policy(&a, true).await?;
    assert_eq!(store.session_attention(&a, "").await?["total_bindings"], 2);
    assert_eq!(
        store.session_attention(&a, "a").await?["groups"][0]["group"],
        "b"
    );
    for group in ["other-pane", "other-socket", "other-session"] {
        assert!(
            !store
                .herdr_prompt_enabled(&store.mailbox(group, "coordinator").await?)
                .await?
        );
    }
    store
        .bind("a", "coordinator", &agent("w1:p1", "replacement"), true)
        .await?;
    assert!(store.session_attention(&a, "").await.is_err());
    let replacement = store.mailbox("a", "coordinator").await?;
    assert!(!store.herdr_prompt_enabled(&replacement).await?);
    assert_eq!(
        store.session_attention(&replacement, "").await?["total_bindings"],
        1
    );
    store.register("a", "native", false).await?;
    let native = store.mailbox("a", "native").await?;
    assert_eq!(
        store.session_attention(&native, "").await?["total_bindings"],
        1
    );
    assert!(store.set_herdr_prompt_policy(&native, true).await.is_err());
    Ok(())
}

#[tokio::test]
async fn legacy_mixed_group_consent_migrates_without_silently_expanding_permission() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    let shared = agent("w1:p1", "shared");
    for group in ["enabled", "disabled"] {
        store
            .enroll(group, Some(Path::new("/tmp/migration-herdr.sock")))
            .await?;
        store.bind(group, "coordinator", &shared, false).await?;
    }
    let pool = support::pool(&store).await?;
    let original = store.mailbox("enabled", "coordinator").await?;
    store
        .hook(
            &original,
            agent_mail::hooks::HookInput {
                hook_event_name: agent_mail::states::HookEvent::SessionStart,
                session_id: "existing-client".into(),
                stop_hook_active: false,
            },
            100,
        )
        .await?;
    assert!(
        !store
            .hook_needs_instructions(&original, "existing-client")
            .await?
    );
    store.close().await;
    sqlx::raw_sql("DROP TABLE task_reports; DROP TABLE herdr_session_policy; DELETE FROM _sqlx_migrations WHERE version=28; UPDATE groups SET auto_prompt=(name='enabled'); PRAGMA user_version=27;").execute(&pool).await?;
    pool.close().await;
    let store = Store::open(dir.path(), true).await?;
    let a = store.mailbox("enabled", "coordinator").await?;
    let b = store.mailbox("disabled", "coordinator").await?;
    assert!(
        store.hook_needs_instructions(&a, "existing-client").await?,
        "the new operating guide must be offered to existing launches once"
    );
    assert!(!store.herdr_prompt_enabled(&a).await?);
    assert!(!store.herdr_prompt_enabled(&b).await?);
    store.set_herdr_prompt_policy(&a, true).await?;
    assert!(store.herdr_prompt_enabled(&a).await?);
    assert!(store.herdr_prompt_enabled(&b).await?);
    Ok(())
}
