//! A prior session or binding cannot establish readiness for a new launch.
use agent_mail::store::Store;
use anyhow::Result;

#[tokio::test]
async fn observations_are_scoped_to_launch_and_binding() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    for args in [vec!["init", "demo"], vec!["agent", "add", "worker"]] {
        assert!(
            std::process::Command::new(binary)
                .args(["--state-dir", temp.path().to_str().unwrap()])
                .args(args)
                .env_remove("AGENT_MAIL_SESSION")
                .env_remove("AGENT_MAIL_GROUP")
                .status()?
                .success()
        );
    }
    let store = Store::open(temp.path(), false).await?;
    let actor = store.mailbox("demo", "worker").await?;
    assert_eq!(
        store.launch_readiness(&actor).await?.state,
        agent_mail::states::RecoveryState::NotObserved
    );
    store
        .begin_launch(&actor, "first", agent_mail::states::NativeRuntime::Codex)
        .await?;
    assert_eq!(
        store.launch_readiness(&actor).await?.state,
        agent_mail::states::RecoveryState::AwaitingHook
    );
    store
        .observe_hook(
            &actor,
            "first",
            "session-1",
            agent_mail::states::HookEvent::SessionStart,
            10,
        )
        .await?;
    assert_eq!(
        store.launch_readiness(&actor).await?.state,
        agent_mail::states::RecoveryState::HookObserved
    );
    store
        .begin_launch(&actor, "second", agent_mail::states::NativeRuntime::Codex)
        .await?;
    store
        .observe_hook(
            &actor,
            "first",
            "session-1",
            agent_mail::states::HookEvent::Stop,
            20,
        )
        .await?;
    assert_eq!(
        store.launch_readiness(&actor).await?.state,
        agent_mail::states::RecoveryState::AwaitingHook
    );
    assert!(
        store
            .bind_launch_session(&actor, "second", "session-2")
            .await?
    );
    assert_eq!(
        store.launch_readiness(&actor).await?.state,
        agent_mail::states::RecoveryState::AwaitingHook
    );
    assert!(
        !store
            .observe_hook(
                &actor,
                "second",
                "different-thread",
                agent_mail::states::HookEvent::SessionStart,
                25
            )
            .await?
    );
    store
        .observe_hook(
            &actor,
            "second",
            "session-2",
            agent_mail::states::HookEvent::SessionStart,
            30,
        )
        .await?;
    let observed = store.launch_readiness(&actor).await?;
    assert_eq!(observed.session.as_deref(), Some("session-2"));
    assert!(!observed.model_consumption_confirmed);
    assert!(
        std::process::Command::new(binary)
            .args([
                "--state-dir",
                temp.path().to_str().unwrap(),
                "agent",
                "replace",
                "worker"
            ])
            .env_remove("AGENT_MAIL_SESSION")
            .env_remove("AGENT_MAIL_GROUP")
            .status()?
            .success()
    );
    let replacement = store.mailbox("demo", "worker").await?;
    assert_eq!(
        store.launch_readiness(&replacement).await?.state,
        agent_mail::states::RecoveryState::NotObserved
    );
    assert!(
        store
            .observe_hook(
                &actor,
                "second",
                "session-2",
                agent_mail::states::HookEvent::Stop,
                40
            )
            .await
            .is_err()
    );
    Ok(())
}
