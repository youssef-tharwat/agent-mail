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

#[tokio::test]
async fn queued_probe_cannot_complete_prior_hook_offer() -> Result<()> {
    use agent_mail::{
        followup::{Mode, Policy},
        identity::Binding,
        states::{HookEvent, NativeRuntime, TaskState},
    };
    use serde_json::json;
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    let temp = tempfile::Builder::new()
        .prefix("am-probe-r3-")
        .tempdir_in("/tmp")?;
    let socket = temp.path().join("claude.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let (sent, mut frames) = tokio::sync::mpsc::unbounded_channel();
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
            sent.send(
                serde_json::from_str::<Value>(&lines.next_line().await.unwrap().unwrap()).unwrap(),
            )
            .unwrap();
        }
    });
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "writer", false).await?;
    store.register("g", "worker", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let writer = store.mailbox("g", "writer").await?;
    let now = agent_mail::now()?;
    store
        .configure_followups(
            "g",
            &Policy {
                mode: Mode::Enabled,
                interval_seconds: 60,
                max_seconds: 240,
                notifier: None,
            },
            now,
        )
        .await?;
    let session = Uuid::new_v4();
    store
        .begin_launch(&actor, "A", NativeRuntime::Claude)
        .await?;
    assert!(
        store
            .bind_launch_session(&actor, "A", &session.to_string())
            .await?
    );
    store
        .claude_inbox_hook(
            &actor,
            &input(session, HookEvent::SessionStart, String::new()),
            &socket,
            "fixture-token",
        )
        .await?;
    let worker = service::WorkerLock::acquire(temp.path())?;
    agent_mail::verification::reconcile(&store, now).await?;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), frames.recv())
        .await?
        .context("queued probe")?;
    let prompt = frame["message"]["content"]
        .as_str()
        .context("probe prompt")?
        .to_owned();
    store
        .work_create(
            &writer,
            WorkDraft {
                id: "probe-work".into(),
                scope: "Review".into(),
                owner: "worker".into(),
                state: TaskState::Active,
                next_action: "Inspect evidence".into(),
                deadline: None,
                evidence: vec![],
            },
            now,
        )
        .await?;
    drop(worker);
    store.close().await;
    let store = Store::open(temp.path(), false).await?;
    let Binding::Standalone {
        session: credential,
    } = &actor.binding
    else {
        anyhow::bail!("fixture identity")
    };
    let hook = |event: &'static str, prompt: String| {
        let root = temp.path().to_owned();
        let socket = socket.clone();
        let credential = credential.to_string();
        async move {
            let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("AGENT_MAIL_SESSION", credential)
                .env("AGENT_MAIL_LAUNCH", "A")
                .env("CLAUDE_CODE_MESSAGING_SOCKET", socket)
                .env("CLAUDE_CODE_MESSAGING_TOKEN", "fixture-token")
                .args([
                    "--state-dir",
                    root.to_str().unwrap(),
                    "--group",
                    "g",
                    "adapter",
                    "claude-hook",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;
            let mut stdin = child.stdin.take().context("stdin")?;
            stdin
                .write_all(&serde_json::to_vec(
                    &json!({"hook_event_name":event,"session_id":session,"prompt":prompt}),
                )?)
                .await?;
            stdin.shutdown().await?;
            drop(stdin);
            let out =
                tokio::time::timeout(std::time::Duration::from_secs(10), child.wait_with_output())
                    .await??;
            anyhow::ensure!(
                out.status.success(),
                "CLI hook: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            Ok::<Value, anyhow::Error>(serde_json::from_slice(&out.stdout)?)
        }
    };
    let startup = hook("SessionStart", String::new()).await?;
    assert!(startup.to_string().contains("probe-work"));
    let pool = support::pool(&store).await?;
    let old: String = sqlx::query_scalar("SELECT id FROM turn_offers WHERE state='offered'")
        .fetch_one(&pool)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM turn_offer_items WHERE offer=?")
        .bind(&old)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    let response = hook("UserPromptSubmit", prompt).await?;
    let text = response["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .context("probe response")?;
    assert!(!text.contains("probe-work"));
    let nonce: Uuid = text
        .split("agent ack ")
        .nth(1)
        .context("nonce")?
        .split('`')
        .next()
        .unwrap()
        .parse()?;
    let worker = service::WorkerLock::acquire(temp.path())?;
    let received = store.delivery_status(&actor, now).await?;
    assert!(received.runtime_received_at.is_some());
    assert!(!received.ready);
    store.acknowledge_delivery(&actor, nonce, now).await?;
    assert!(store.delivery_status(&actor, now).await?.ready);
    drop(worker);
    hook("Stop", String::new()).await?;
    let state: String = sqlx::query_scalar("SELECT state FROM turn_offers WHERE id=?")
        .bind(&old)
        .fetch_one(&pool)
        .await?;
    let stage: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='probe-work'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        (state.as_str(), stage),
        ("abandoned", 0),
        "FND-6: probe Stop must not complete the prior offered work"
    );
    let new_items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM turn_offer_items WHERE offer<>?")
        .bind(&old)
        .fetch_one(&pool)
        .await?;
    assert_eq!(new_items, 0);
    agent_mail::followup::reconcile(&store, now + 61).await?;
    let recovered: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='probe-work'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        recovered, 1,
        "timer independently recovers the outstanding work"
    );
    assert_eq!(
        store.work_show(&actor, "probe-work").await?.state,
        TaskState::Active
    );
    server.abort();
    let _ = server.await;
    Ok(())
}
