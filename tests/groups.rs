//! Multi-fleet isolation across tasks, mail, bounded diagnostics and CLI selection.
use agent_mail::{
    states::TaskState,
    store::{Publish, Store},
    work::WorkDraft,
};
use anyhow::Result;
use serde_json::{Value, json};
fn cli(root: &std::path::Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .args(["--state-dir", root.to_str().unwrap()])
        .args(args)
        .env_remove("AGENT_MAIL_GROUP")
        .env_remove("AGENT_MAIL_SESSION")
        .env_remove("HERDR_ENV")
        .output()
        .unwrap()
}
#[tokio::test]
async fn overlapping_names_and_large_fleets_do_not_mix_context_or_status() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let store = Store::open(tmp.path(), true).await?;
    for group in ["awp", "recall"] {
        store.enroll(group, None).await?;
        store.register(group, "coord", false).await?;
        store.register(group, "worker", false).await?;
    }
    let a = store.mailbox("awp", "coord").await?;
    let b = store.mailbox("recall", "coord").await?;
    for n in 0..105 {
        store
            .work_create(
                &a,
                WorkDraft {
                    id: format!("t{n}"),
                    owner: "worker".into(),
                    scope: "private-awp".into(),
                    state: TaskState::Open,
                    next_action: "Inspect".into(),
                    deadline: None,
                    evidence: vec![],
                },
                1,
            )
            .await?;
    }
    store
        .work_create(
            &b,
            WorkDraft {
                id: "t0".into(),
                owner: "worker".into(),
                scope: "private-recall".into(),
                state: TaskState::Open,
                next_action: "Inspect".into(),
                deadline: None,
                evidence: vec![],
            },
            1,
        )
        .await?;
    let id = store
        .publish(
            &a,
            Publish {
                intent: agent_mail::states::MessageIntent::Request,
                recipients: vec!["worker".into()],
                key: "one".into(),
                summary: "private-awp".into(),
                body: "private-awp".into(),
                due_after: None,
                context: agent_mail::mail_context::ContextSource::NewConversation,
            },
            1,
        )
        .await?;
    let aw = store.mailbox("awp", "worker").await?;
    let bw = store.mailbox("recall", "worker").await?;
    assert_eq!(store.inbox(&aw, 0).await?.len(), 1);
    assert!(store.inbox(&bw, 0).await?.is_empty());
    assert!(store.message(&bw, id).await.is_err());
    assert!(
        store
            .resolve(&bw, id, "wrong group", None, 2)
            .await
            .is_err()
    );
    assert_eq!(store.work_show(&bw, "t0").await?.scope, "private-recall");
    std::fs::write(
        tmp.path().join("service-status.json"),
        serde_json::to_vec(
            &json!({"checked_at":1,"observations":[{"group":"awp","detail":"private-awp"},{"group":"recall","state":"waiting"}],"error":"private-awp","relay":[{"error":"private-awp"}],"verification":{"checked":1,"failed":1,"errors":["private-awp"]},"delivery_timings":[{"operation":"herdr_delivery","phase":"writer_hold","max_us":100}]}),
        )?,
    )?;
    let report = agent_mail::status::report(&store, Some("recall")).await?;
    assert_eq!(report["attention"]["work"].as_array().unwrap().len(), 1);
    assert_eq!(report["attention"]["more"], false);
    assert!(!report.to_string().contains("awp"));
    assert_eq!(report["last_scan"]["verification_error"], true);
    assert!(report["last_scan"].get("delivery_timings").is_none());
    assert_eq!(report["groups"].as_array().unwrap().len(), 1);
    assert!(
        agent_mail::status::report(&store, None).await?["attention"]["more"]
            .as_bool()
            .unwrap()
    );
    store.close().await;
    let ambiguous = cli(tmp.path(), &["status"]);
    assert!(!ambiguous.status.success());
    let scoped = cli(tmp.path(), &["--group", "recall", "status"]);
    assert!(
        scoped.status.success(),
        "{}",
        String::from_utf8_lossy(&scoped.stderr)
    );
    assert!(!String::from_utf8_lossy(&scoped.stdout).contains("awp"));
    let all = cli(tmp.path(), &["status", "--all-groups", "--json"]);
    assert!(all.status.success());
    let all: Value = serde_json::from_slice(&all.stdout)?;
    assert_eq!(all["groups"].as_array().unwrap().len(), 2);
    assert!(
        !cli(tmp.path(), &["--group", "missing", "status"])
            .status
            .success()
    );
    assert!(
        !cli(tmp.path(), &["--group", "recall", "status", "--all-groups"])
            .status
            .success()
    );
    Ok(())
}
