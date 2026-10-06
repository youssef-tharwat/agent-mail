//! Native inbox receipts must come from the runtime hook, never socket writes.
mod support;
use agent_mail::{
    claude_inbox::Input,
    service,
    store::Store,
    work::{WorkDraft, WorkPatch},
};
use anyhow::{Context, Result};
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
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
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
    let now = agent_mail::now()?;
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "task".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: agent_mail::states::TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            now,
        )
        .await?;
    service::tick(&store, now).await?;
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
    assert!(!context.to_string().contains("Inspect evidence"));
    assert!(context.to_string().contains("changes"));
    let context_text = context["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .context("Claude notification did not return its delivery context")?;
    assert!(context_text.contains("agent ack"));
    let nonce: Uuid = context_text
        .split("agent ack ")
        .nth(1)
        .context("Claude notification omitted its challenge")?
        .split('`')
        .next()
        .context("Claude challenge nonce missing")?
        .parse()?;
    store
        .acknowledge_delivery(&actor, nonce, agent_mail::now()?)
        .await?;
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
            &writer,
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
            now + 1,
        )
        .await?;
    service::tick(&store, now + 1).await?;
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
    store.register("g", "writer", false).await?;
    let writer = store.mailbox("g", "writer").await?;
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
            &writer,
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

#[tokio::test]
async fn delivery_probe_hook_is_receipt_not_agent_acknowledgment() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let frames = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = frames.clone();
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let mut lines = BufReader::new(stream).lines();
            let Some(auth) = lines.next_line().await.unwrap() else {
                continue;
            };
            assert_eq!(
                serde_json::from_str::<Value>(&auth).unwrap()["token"],
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

    let _lock = service::WorkerLock::acquire(temp.path())?;
    let now = agent_mail::now()?;
    agent_mail::verification::reconcile(&store, now).await?;
    for _ in 0..100 {
        if !frames.lock().await.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let frame = frames.lock().await[0].clone();
    let prompt = frame["message"]["content"].as_str().unwrap().to_owned();
    let before = store.delivery_status(&actor, now).await?;
    assert!(!before.ready);
    assert_eq!(before.runtime_received_at, None);
    let context = store
        .claude_inbox_hook(
            &actor,
            &input(
                session,
                agent_mail::states::HookEvent::UserPromptSubmit,
                prompt,
            ),
            &socket,
            "fixture-token",
        )
        .await?
        .unwrap();
    let after = store.delivery_status(&actor, now).await?;
    assert!(!after.ready);
    assert!(after.runtime_received_at.is_some());
    let text = context["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    let nonce: Uuid = text
        .split("agent ack ")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap()
        .parse()?;
    store.acknowledge_delivery(&actor, nonce, now).await?;
    assert!(store.delivery_status(&actor, now).await?.ready);
    assert!(store.notifications(&actor, 0).await?.is_empty());
    server.abort();
    let _ = server.await;
    agent_mail::verification::reconcile(&store, now + 1).await?;
    assert!(!store.delivery_status(&actor, now + 1).await?.ready);
    Ok(())
}
