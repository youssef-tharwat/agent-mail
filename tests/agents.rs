//! Registration lifecycle and its boundary with mail, tasks and runtime identity.
use agent_mail::{
    identity::Binding,
    states::{
        AgentState::{Registered, Retired},
        TaskState,
    },
    store::{Publish, Store},
    work::WorkDraft,
};
use anyhow::Result;
fn request(to: &str, key: &str) -> Publish {
    Publish {
        intent: agent_mail::states::MessageIntent::Request,
        recipients: vec![to.into()],
        key: key.into(),
        summary: "Review".into(),
        body: "Evidence".into(),
        due_after: None,
        context: agent_mail::mail_context::ContextSource::NewConversation,
    }
}
fn task(id: &str, owner: &str) -> WorkDraft {
    WorkDraft {
        id: id.into(),
        owner: owner.into(),
        scope: "Review".into(),
        state: TaskState::Open,
        next_action: "Inspect".into(),
        deadline: None,
        evidence: vec![],
    }
}
#[tokio::test]
async fn lifecycle_is_versioned_retry_safe_and_invalidates_old_sessions() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let token = store.register("g", "a", false).await?;
    let actor = store.authenticate("g", Some(&token)).await?;
    store.set_runtime_enabled(&actor, false).await?;
    assert_eq!(store.agent_record("g", "a").await?.state, Registered);
    assert!(store.update_agent("g", "a", 1, Retired, "").await.is_err());
    let retired = store.update_agent("g", "a", 1, Retired, "Finished").await?;
    assert_eq!(retired.version, 2);
    assert_eq!(
        store
            .update_agent("g", "a", 1, Retired, "Finished")
            .await?
            .version,
        2
    );
    assert!(
        store
            .update_agent("g", "a", 1, Retired, "Different decision")
            .await
            .is_err()
    );
    assert!(store.authenticate("g", Some(&token)).await.is_err());
    assert!(store.register("g", "a", true).await.is_err());
    assert!(
        store
            .publish(&actor, request("a", "stale"), 10)
            .await
            .is_err()
    );
    assert!(store.runtime_enabled(&actor).await.is_err());
    let restored = store
        .update_agent("g", "a", 2, Registered, "New assignment")
        .await?;
    assert_eq!(restored.version, 3);
    assert!(store.authenticate("g", Some(&token)).await.is_err());
    let fresh = store.mailbox("g", "a").await?;
    let Binding::Standalone { session } = fresh.binding else {
        panic!("expected standalone")
    };
    let fresh = store.authenticate("g", Some(&session)).await?;
    assert!(!store.runtime_enabled(&fresh).await?);
    assert!(
        store
            .publish(&actor, request("a", "stale2"), 10)
            .await
            .is_err()
    );
    assert_eq!(store.agent_history("g", "a").await?.len(), 3);
    // Retrying an old decision does not retire a restored registration again.
    assert_eq!(
        store
            .update_agent("g", "a", 1, Retired, "Finished")
            .await?
            .state,
        Retired
    );
    assert_eq!(store.agent_record("g", "a").await?.state, Registered);
    let encoded = serde_json::to_string(&store.agent_records("g").await?)?;
    assert!(!encoded.contains(&session.to_string()));
    store.close().await;
    let reopened = Store::open(temp.path(), false).await?;
    assert_eq!(reopened.agent_record("g", "a").await?.version, 3);
    Ok(())
}
#[tokio::test]
async fn retirement_checks_incoming_outgoing_mail_and_both_task_roles() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let ta = store.register("g", "a", false).await?;
    let tb = store.register("g", "b", false).await?;
    let a = store.authenticate("g", Some(&ta)).await?;
    let b = store.authenticate("g", Some(&tb)).await?;
    let message = store.publish(&a, request("b", "one"), 10).await?;
    for name in ["a", "b"] {
        assert!(
            store
                .update_agent("g", name, 1, Retired, "Finished")
                .await
                .unwrap_err()
                .to_string()
                .contains("pending mail")
        );
    }
    store.resolve(&b, message, "Reviewed", None, 11).await?;
    store.work_create(&a, task("review", "b"), 12).await?;
    for name in ["a", "b"] {
        assert!(
            store
                .update_agent("g", name, 1, Retired, "Finished")
                .await
                .unwrap_err()
                .to_string()
                .contains("open tasks")
        );
    }
    Ok(())
}
#[tokio::test]
async fn retirement_and_new_obligations_are_serialized() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("g", None).await?;
    let ta = store.register("g", "a", false).await?;
    store.register("g", "b", false).await?;
    store.register("g", "c", false).await?;
    let a = store.authenticate("g", Some(&ta)).await?;
    let (retire, send) = tokio::join!(
        store.update_agent("g", "b", 1, Retired, "Finished"),
        store.publish(&a, request("b", "race"), 10)
    );
    assert_ne!(retire.is_ok(), send.is_ok());
    let (retire, assign) = tokio::join!(
        store.update_agent("g", "c", 1, Retired, "Finished"),
        store.work_create(&a, task("race", "c"), 10)
    );
    assert_ne!(retire.is_ok(), assign.is_ok());
    Ok(())
}

#[tokio::test]
async fn current_schema_allows_another_group_while_service_holds_store() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), true).await?;
    store.enroll("awp", None).await?;
    let token = store.register("awp", "coord", false).await?;
    store.close().await;
    let store = Store::open(temp.path(), false).await?;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .args([
            "--state-dir",
            temp.path().to_str().unwrap(),
            "init",
            "recall",
        ])
        .env_remove("AGENT_MAIL_SESSION")
        .env_remove("AGENT_MAIL_GROUP")
        .env_remove("HERDR_SOCKET_PATH")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(store.groups().await?.len(), 2);
    assert_eq!(store.authenticate("awp", Some(&token)).await?.name, "coord");
    assert!(store.agent_records("recall").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn version_fourteen_imports_registration_without_replacing_identity() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("state");
    let migrations = temp.path().join("migrations");
    std::fs::create_dir_all(&root)?;
    std::fs::create_dir_all(&migrations)?;
    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().as_ref() < "0015" {
            std::fs::copy(entry.path(), migrations.join(entry.file_name()))?;
        }
    }
    let mut connection: sqlx::SqliteConnection = sqlx::Connection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(root.join("mail.db"))
            .create_if_missing(true),
    )
    .await?;
    sqlx::migrate::Migrator::new(migrations.as_path())
        .await?
        .run(&mut connection)
        .await?;
    sqlx::raw_sql("INSERT INTO node VALUES ('00000000-0000-4000-8000-000000000001'); INSERT INTO groups(name,socket,home_machine) VALUES('g','','00000000-0000-4000-8000-000000000001'); INSERT INTO mailboxes(group_name,name,binding) VALUES('g','a','{\"runtime\":\"standalone\",\"session\":\"00000000-0000-4000-8000-000000000002\"}');").execute(&mut connection).await?;
    sqlx::Connection::close(connection).await?;
    let store = Store::open(&root, true).await?;
    let token = "00000000-0000-4000-8000-000000000002".parse()?;
    assert_eq!(store.authenticate("g", Some(&token)).await?.name, "a");
    assert_eq!(store.agent_record("g", "a").await?.version, 1);
    assert_eq!(
        store.agent_history("g", "a").await?[0].reason,
        "Imported existing registration"
    );
    Ok(())
}
