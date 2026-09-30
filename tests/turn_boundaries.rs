//! Turn attribution must survive input, launch and transport boundaries.
mod support;

use agent_mail::{
    identity::Binding,
    states::{NativeRuntime, TaskState},
    store::{Mailbox, Store},
    work::WorkDraft,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

struct Fixture {
    _temp: tempfile::TempDir,
    store: Store,
    actor: Mailbox,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix("am-boundary-")
            .tempdir_in("/tmp")?;
        let store = Store::open(temp.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "worker", false).await?;
        let actor = store.mailbox("g", "worker").await?;
        let writer = store.mailbox("g", "writer").await?;
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: "work".into(),
                    scope: "Inspect evidence".into(),
                    owner: "worker".into(),
                    state: TaskState::Active,
                    next_action: "Inspect evidence".into(),
                    deadline: None,
                    evidence: vec![],
                },
                agent_mail::now()?,
            )
            .await?;
        store.close().await;
        let store = Store::open(temp.path(), false).await?;
        Ok(Self {
            _temp: temp,
            store,
            actor,
        })
    }

    async fn hook(
        &self,
        adapter: &str,
        launch: Option<&str>,
        session: &str,
        event: &str,
    ) -> Result<Value> {
        let Binding::Standalone {
            session: credential,
        } = &self.actor.binding
        else {
            anyhow::bail!("fixture requires its own standalone identity");
        };
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("AGENT_MAIL_SESSION", credential.to_string())
            .args([
                "--state-dir",
                self.store.root().to_str().unwrap(),
                "--group",
                "g",
                "adapter",
                adapter,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(launch) = launch {
            command.env("AGENT_MAIL_LAUNCH", launch);
        }
        let mut child = command.spawn()?;
        let mut input = child.stdin.take().context("hook stdin")?;
        input
            .write_all(&serde_json::to_vec(&json!({
                "hook_event_name":event, "session_id":session,
            }))?)
            .await?;
        input.shutdown().await?;
        drop(input);
        let output = child.wait_with_output().await?;
        ensure!(
            output.status.success(),
            "hook failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    async fn offers(&self) -> Result<Vec<(String, String, Option<String>, i64)>> {
        let pool = support::pool(&self.store).await?;
        Ok(sqlx::query_as("SELECT id,state,turn,(SELECT COUNT(*) FROM turn_offer_items i WHERE i.offer=o.id) FROM turn_offers o ORDER BY id")
            .fetch_all(&pool).await?)
    }

    async fn no_attention(&self) -> Result<()> {
        let pool = support::pool(&self.store).await?;
        let stage: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='work'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            stage, 0,
            "FND-3: stale managed hooks cannot advance attention"
        );
        assert_eq!(
            self.store.work_show(&self.actor, "work").await?.state,
            TaskState::Active
        );
        Ok(())
    }
}

#[tokio::test]
async fn stale_managed_hooks_cannot_mutate_the_current_launch_offer() -> Result<()> {
    for adapter in ["hook", "claude-hook"] {
        for event in ["SessionStart", "PostToolUse", "Stop"] {
            let f = Fixture::new().await?;
            let runtime = if adapter == "hook" {
                NativeRuntime::Codex
            } else {
                NativeRuntime::Claude
            };
            let first = uuid::Uuid::new_v4().to_string();
            let second = uuid::Uuid::new_v4().to_string();
            f.store.begin_launch(&f.actor, "A", runtime).await?;
            f.hook(adapter, Some("A"), &first, "SessionStart").await?;
            f.store.begin_launch(&f.actor, "B", runtime).await?;
            f.hook(adapter, Some("B"), &second, "SessionStart").await?;
            let before = f.offers().await?;
            let ready_before = serde_json::to_value(f.store.launch_readiness(&f.actor).await?)?;
            f.hook(adapter, Some("A"), &first, event).await?;
            assert_eq!(
                f.offers().await?,
                before,
                "FND-3: stale {adapter} {event} must not mutate B's offers"
            );
            assert_eq!(
                serde_json::to_value(f.store.launch_readiness(&f.actor).await?)?,
                ready_before
            );
            f.no_attention().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn stale_stop_before_replacement_startup_cannot_complete_old_work() -> Result<()> {
    for adapter in ["hook", "claude-hook"] {
        let f = Fixture::new().await?;
        let runtime = if adapter == "hook" {
            NativeRuntime::Codex
        } else {
            NativeRuntime::Claude
        };
        let session = uuid::Uuid::new_v4().to_string();
        f.store.begin_launch(&f.actor, "A", runtime).await?;
        f.hook(adapter, Some("A"), &session, "SessionStart").await?;
        f.store.begin_launch(&f.actor, "B", runtime).await?;
        let before = f.offers().await?;
        f.hook(adapter, Some("A"), &session, "Stop").await?;
        f.no_attention().await?;
        assert_eq!(f.offers().await?, before);
    }
    Ok(())
}

#[tokio::test]
async fn wrong_pinned_session_cannot_reset_current_managed_offers() -> Result<()> {
    let f = Fixture::new().await?;
    let session = uuid::Uuid::new_v4().to_string();
    f.store
        .begin_launch(&f.actor, "A", NativeRuntime::Codex)
        .await?;
    f.store.bind_launch_session(&f.actor, "A", &session).await?;
    f.hook("hook", Some("A"), &session, "SessionStart").await?;
    let before = f.offers().await?;
    f.hook(
        "hook",
        Some("A"),
        &uuid::Uuid::new_v4().to_string(),
        "SessionStart",
    )
    .await?;
    assert_eq!(
        f.offers().await?,
        before,
        "FND-3: pinned session fences offer resets"
    );
    f.no_attention().await?;
    Ok(())
}

#[tokio::test]
async fn authenticated_unmanaged_hooks_still_complete_their_own_offer() -> Result<()> {
    for adapter in ["hook", "claude-hook"] {
        let f = Fixture::new().await?;
        let session = uuid::Uuid::new_v4().to_string();
        assert_ne!(
            f.hook(adapter, None, &session, "SessionStart").await?,
            json!({})
        );
        f.hook(adapter, None, &session, "Stop").await?;
        let pool = support::pool(&f.store).await?;
        let stage: i64 = sqlx::query_scalar("SELECT stage FROM followups WHERE task='work'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(stage, 1);
        assert_eq!(
            f.store.work_show(&f.actor, "work").await?.state,
            TaskState::Active
        );
    }
    Ok(())
}
