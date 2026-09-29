//! Native inbox receipts must come from the runtime hook, never socket writes.
mod support;
use agent_mail::{
    claude_inbox::Input,
    service,
    store::Store,
    work::{WorkDraft, WorkPatch},
};
use anyhow::Result;
use serde_json::Value;
use std::{os::unix::fs::PermissionsExt, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixListener,
    sync::Mutex,
};
use uuid::Uuid;

fn input(session_id: Uuid, event: agent_mail::states::HookEvent, prompt: String) -> Input {
    Input {
        session_id,
        hook_event_name: event,
        prompt,
    }
}

#[tokio::test]
async fn socket_write_is_unconfirmed_until_matching_hook_and_cancellation_uses_priority()
-> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let frames = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = frames.clone();
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let mut lines = BufReader::new(stream).lines();
            assert_eq!(
                serde_json::from_str::<Value>(&lines.next_line().await.unwrap().unwrap()).unwrap()
                    ["token"],
                "fixture-token"
            );
            captured
                .lock()
                .await
                .push(serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap());
        }
    });
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let session = Uuid::new_v4();
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    service::tick(&store, 1000).await?;
    let pool = support::pool(&store).await?;
    let status = sqlx::query!("SELECT delivered,attempts FROM runtime_wakes")
        .fetch_one(&pool)
        .await?;
    assert_eq!((status.delivered, status.attempts), (0, 1));
    assert!(!store.notifications(&actor, 0).await?.is_empty());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while frames.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let prompt = frames.lock().await[0]["message"]["content"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    Uuid::new_v4(),
                    agent_mail::states::HookEvent::UserPromptSubmit,
                    prompt.clone()
                ),
                &socket,
                "fixture-token"
            )
            .await
            .is_err()
    );
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    session,
                    agent_mail::states::HookEvent::UserPromptSubmit,
                    "unrelated".into()
                ),
                &socket,
                "fixture-token"
            )
            .await?
            .is_none()
    );
    assert!(!store.notifications(&actor, 0).await?.is_empty());
    let context = store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::UserPromptSubmit,
                prompt.clone(),
            ),
            &socket,
            "fixture-token",
        )
        .await?
        .unwrap();
    assert!(context.to_string().contains("Inspect evidence"));
    assert!(store.notifications(&actor, 0).await?.is_empty());
    assert!(store.work_show(&actor, "task").await?.state.is_open());
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    session,
                    agent_mail::states::HookEvent::UserPromptSubmit,
                    prompt
                ),
                &socket,
                "fixture-token"
            )
            .await?
            .is_none()
    );
    store
        .update_work(
            &actor,
            "task",
            agent_mail::work::WorkUpdate {
                version: 1,
                patch: WorkPatch {
                    state: Some(agent_mail::states::TaskState::Cancelled),
                    ..Default::default()
                },
                reason: ("Cancel").to_owned(),
                resolve_message: None,
            },
            1001,
        )
        .await?;
    service::tick(&store, 1001).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while frames.lock().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(frames.lock().await[1]["priority"], "now");
    server.abort();
    pool.close().await;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn lost_hooks_keep_bounded_attempts_across_resume_and_identity_rotation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let mut lines = BufReader::new(stream).lines();
            while lines.next_line().await.unwrap().is_some() {}
        }
    });
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let session = Uuid::new_v4();
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    store
        .work_create(
            &actor,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect".into(),
                deadline: None,
                evidence: vec![],
            },
            1000,
        )
        .await?;
    for time in [1000, 1001, 1300, 1600, 1900] {
        service::tick(&store, time).await?;
    }
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    service::tick(&store, 2000).await?;
    let pool = support::pool(&store).await?;
    let status = sqlx::query!("SELECT delivered,attempts FROM runtime_wakes")
        .fetch_one(&pool)
        .await?;
    assert_eq!((status.delivered, status.attempts), (0, 3));
    assert!(
        !store
            .native_status()
            .await?
            .to_string()
            .contains("fixture-token")
    );
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    Uuid::new_v4(),
                    agent_mail::states::HookEvent::SessionStart,
                    String::new()
                ),
                &socket,
                "fixture-token"
            )
            .await
            .is_err()
    );
    store.register("g", "worker", true).await?;
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    session,
                    agent_mail::states::HookEvent::SessionStart,
                    String::new()
                ),
                &socket,
                "fixture-token"
            )
            .await
            .is_err()
    );
    server.abort();
    pool.close().await;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn replaced_socket_is_rejected_and_exit_authenticates_after_unlink() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let session = Uuid::new_v4();
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    // Keep the old inode alive while replacing its pathname.
    std::fs::remove_file(&socket)?;
    let replacement = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(session, agent_mail::states::HookEvent::Stop, String::new()),
                &socket,
                "fixture-token"
            )
            .await
            .is_err()
    );
    drop(replacement);
    std::fs::remove_file(&socket)?;
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    Uuid::new_v4(),
                    agent_mail::states::HookEvent::SessionEnd,
                    String::new()
                ),
                &socket,
                "fixture-token"
            )
            .await
            .is_err()
    );
    assert!(
        store
            .claude_inbox_hook(
                &actor,
                &input(
                    session,
                    agent_mail::states::HookEvent::SessionEnd,
                    String::new()
                ),
                &socket,
                "wrong-token"
            )
            .await
            .is_err()
    );
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionEnd,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    let pool = support::pool(&store).await?;
    let state = sqlx::query!("SELECT activity FROM claude_inboxes")
        .fetch_one(&pool)
        .await?;
    assert_eq!(state.activity, "ended");
    drop(listener);
    pool.close().await;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn detach_survives_startup_hooks_until_explicit_enable() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let session = Uuid::new_v4();
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    store.set_runtime_enabled(&actor, false).await?;
    store.close().await;
    let store = Store::open(temp.path(), false).await?;
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    assert!(store.native_status().await?.as_array().unwrap().is_empty());
    store.set_runtime_enabled(&actor, true).await?;
    store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::SessionStart,
                String::new(),
            ),
            &socket,
            "fixture-token",
        )
        .await?;
    assert_eq!(store.native_status().await?.as_array().unwrap().len(), 1);
    drop(listener);
    store.close().await;
    Ok(())
}
