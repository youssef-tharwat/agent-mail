//! Fresh processes exercise durable subscriptions, atomic decisions, and hook contracts.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Stdio},
};

struct Demo {
    temp: tempfile::TempDir,
    writer: String,
    worker: String,
}
impl Demo {
    fn new() -> Result<Self> {
        let mut d = Self {
            temp: tempfile::tempdir()?,
            writer: String::new(),
            worker: String::new(),
        };
        d.call(None, &["init", "default"], None, false)?;
        d.writer = d.call(
            None,
            &["agent", "add", "writer", "--show-session"],
            None,
            false,
        )?["session"]
            .as_str()
            .unwrap()
            .into();
        d.worker = d.call(
            None,
            &["agent", "add", "worker", "--show-session"],
            None,
            false,
        )?["session"]
            .as_str()
            .unwrap()
            .into();
        Ok(d)
    }
    fn call(
        &self,
        session: Option<&str>,
        args: &[&str],
        input: Option<Value>,
        fail: bool,
    ) -> Result<Value> {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        cmd.args(["--state-dir", self.temp.path().to_str().unwrap()])
            .args(args)
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_SESSION")
            .env_remove("HERDR_ENV")
            .env_remove("HERDR_SOCKET_PATH")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(session) = session {
            cmd.env("AGENT_MAIL_SESSION", session);
        }
        let mut child = cmd.spawn()?;
        if let Some(input) = input {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(serde_json::to_string(&input)?.as_bytes())?;
        }
        drop(child.stdin.take());
        let out = child.wait_with_output()?;
        if fail {
            ensure!(!out.status.success(), "unexpected success");
            return Ok(Value::Null);
        }
        ensure!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(serde_json::from_slice(&out.stdout)?)
    }
    fn create(&self) -> Result<Value> {
        self.call(
            Some(&self.writer),
            &[
                "task",
                "create",
                "task",
                "Review",
                "--owner",
                "worker",
                "--next-action",
                "Inspect revision",
            ],
            None,
            false,
        )
    }
    fn events(&self, session: &str) -> Result<Value> {
        self.call(Some(session), &["adapter", "events"], None, false)
    }
    fn hook(&self, event: &str, active: bool) -> Result<Value> {
        self.call(Some(&self.worker), &["adapter", "hook"],Some(json!({"hook_event_name":event,"session_id":"test-client","stop_hook_active":active})),false)
    }
    fn consume_hook(&self, session: &str, output: &Value) -> Result<()> {
        if let Some(text) = output["hookSpecificOutput"]["additionalContext"].as_str() {
            let state: Value = serde_json::from_str(text.lines().nth(1).unwrap())?;
            if let Some(token) = state["receipt"]["token"].as_str() {
                self.call(
                    Some(session),
                    &["attention", "acknowledge", token],
                    None,
                    false,
                )?;
            }
        }
        Ok(())
    }
    fn decide(&self, value: Value, fail: bool) -> Result<Value> {
        let path = self.temp.path().join("decision.json");
        std::fs::write(&path, serde_json::to_vec(&value)?)?;
        self.call(
            Some(&self.writer),
            &["task", "update", "task", "--file", path.to_str().unwrap()],
            None,
            fail,
        )
    }
}

#[test]
fn changes_publish_without_a_separate_send_and_receipts_do_not_resolve() -> Result<()> {
    let d = Demo::new()?;
    d.create()?;
    let events = d.events(&d.worker)?;
    assert_eq!(events["items"][0]["kind"], "work_changed");
    let id = events["items"][0]["id"].to_string();
    d.call(Some(&d.worker), &["adapter", "ack", &id], None, false)?;
    assert_eq!(d.events(&d.worker)?["items"], json!([]));
    assert_eq!(
        d.call(Some(&d.worker), &["context"], None, false)?["work"][0]["id"],
        "task"
    );
    // Another participant cannot acknowledge this event.
    d.call(Some(&d.writer), &["adapter", "ack", &id], None, true)?;
    let rotated = d.call(
        None,
        &["agent", "replace", "worker", "--show-session"],
        None,
        false,
    )?;
    let new = rotated["session"].as_str().unwrap();
    d.call(Some(&d.worker), &["adapter", "ack", &id], None, true)?;
    assert_eq!(d.events(new)?["items"][0]["id"], events["items"][0]["id"]);
    d.decide(
        json!({"version":1,"reason":"Reassign","patch":{"owner":"writer"}}),
        false,
    )?;
    let former = d.events(new)?;
    assert!(
        former["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["version"] == 2)
    );
    assert_eq!(
        d.events(&d.writer)?["items"],
        json!([]),
        "the writer already consumed its own decision result"
    );
    Ok(())
}

#[test]
fn decisions_resolve_and_publish_once_or_roll_back_everything() -> Result<()> {
    let d = Demo::new()?;
    d.create()?;
    let sent = d.call(
        Some(&d.worker),
        &[
            "mail",
            "send",
            "writer",
            "Ready for review",
            "--key",
            "result",
            "--task",
            "task",
        ],
        None,
        false,
    )?;
    let before = d.events(&d.worker)?;
    let invalid =
        json!({"version":1,"reason":"Accept","patch":{"state":"accepted"},"resolve_message":9999});
    d.decide(invalid, true)?;
    assert_eq!(d.events(&d.worker)?, before);
    assert_eq!(
        d.call(Some(&d.writer), &["task", "show", "task"], None, false)?["version"],
        1
    );
    let decision = json!({"version":1,"reason":"Evidence verified","patch":{"state":"accepted","accepted_revision":"abc123"},"resolve_message":sent["id"]});
    let first = d.decide(decision.clone(), false)?;
    let events = d.events(&d.worker)?;
    assert_eq!(d.decide(decision.clone(), false)?, first);
    assert_eq!(d.events(&d.worker)?, events);
    assert_eq!(
        d.call(Some(&d.writer), &["mail", "list"], None, false)?["items"],
        json!([])
    );
    let reopened=d.decide(json!({"version":2,"reason":"New evidence","patch":{"state":"active","accepted_revision":null}}),false)?;
    assert_eq!(reopened["accepted_revision"], Value::Null);
    assert_eq!(reopened["version"], 3);
    let mut changed = decision;
    changed["reason"] = json!("Different decision");
    d.decide(changed, true)?;
    Ok(())
}

#[test]
fn hooks_restore_after_reset_suppress_repeats_and_leave_turns_to_deadlines() -> Result<()> {
    let d = Demo::new()?;
    // Observation mode retains recovery and keeps reassessment with persisted policy.
    d.call(None, &["attention", "configure", "--observe"], None, false)?;
    d.create()?;
    let start = d.hook("SessionStart", false)?;
    assert!(
        start["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap()
            .contains("Inspect revision")
    );
    // Recovery state has its own 6 KiB budget; startup adds the fixed bundled guide once.
    let startup = start["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    let state = startup
        .strip_suffix(agent_mail::SKILL)
        .expect("startup includes the complete bundled skill");
    assert!(state.trim_end().len() <= 6000);
    assert_eq!(startup.matches(agent_mail::SKILL).count(), 1);
    assert_eq!(d.hook("PostToolUse", false)?, json!({}));
    // New state is injected automatically at the next tool boundary.
    d.decide(
        json!({"version":1,"reason":"Clarify","patch":{"next_action":"Check changed contract"}}),
        false,
    )?;
    let update = d.hook("PostToolUse", false)?;
    assert!(update.to_string().contains("task"));
    assert!(!update.to_string().contains("Check changed contract"));
    let routine = update["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    assert!(
        !routine.contains(agent_mail::SKILL),
        "routine updates must not repeat the guide"
    );
    assert!(routine.len() <= 6000);
    d.consume_hook(&d.worker, &update)?;
    assert_eq!(d.hook("Stop", false)?, json!({}));
    d.decide(
        json!({"version":2,"reason":"Clarify again","patch":{"next_action":"Check tests"}}),
        false,
    )?;
    assert_eq!(d.hook("Stop", false)?, json!({}));
    assert_eq!(d.hook("Stop", true)?, json!({}));
    d.decide(
        json!({"version":3,"reason":"More work","patch":{"next_action":"Check diff"}}),
        false,
    )?;
    assert_eq!(d.hook("Stop", false)?, json!({}));
    // Older clients invalidate at PostCompact, then restore at the next boundary.
    assert_eq!(d.hook("PostCompact", false)?, json!({}));
    let recovered = d.hook("PreToolUse", false)?;
    assert!(
        recovered["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap()
            .ends_with(agent_mail::SKILL)
    );
    assert!(recovered.to_string().contains("Check diff"));
    // Compaction/restart restores durable records and receipts only those actually returned.
    assert!(
        d.hook("SessionStart", false)?
            .to_string()
            .contains("Check diff")
    );
    assert!(d.events(&d.worker)?["items"].as_array().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn lost_hook_output_retries_with_a_persisted_budget_and_reset_restores() -> Result<()> {
    use agent_mail::{
        hooks::{HookEvent, HookInput},
        store::Store,
        work::WorkDraft,
    };
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path(), true).await?;
    store.enroll("g", None).await?;
    store.register("g", "worker", false).await?;
    store.register("g", "writer", false).await?;
    let actor = store.mailbox("g", "worker").await?;
    let writer = store.mailbox("g", "writer").await?;
    store
        .hook(
            &actor,
            HookInput {
                hook_event_name: HookEvent::SessionStart,
                session_id: "client".into(),
                stop_hook_active: false,
            },
            999,
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
    // Emit a claimed batch, then lose its output. No ingestion receipt is manufactured.
    assert_ne!(
        store
            .hook(
                &actor,
                HookInput {
                    hook_event_name: HookEvent::PostToolUse,
                    session_id: "client".into(),
                    stop_hook_active: false
                },
                1000
            )
            .await?,
        json!({})
    );
    store.close().await;
    let store = Store::open(dir.path(), false).await?;
    let input = || HookInput {
        hook_event_name: HookEvent::PostToolUse,
        session_id: "client".into(),
        stop_hook_active: false,
    };
    assert_eq!(store.hook(&actor, input(), 1001).await?, json!({}));
    assert_ne!(store.hook(&actor, input(), 1300).await?, json!({}));
    assert_ne!(store.hook(&actor, input(), 1600).await?, json!({}));
    assert_eq!(store.hook(&actor, input(), 1900).await?, json!({}));
    assert!(!store.notifications(&actor, 0).await?.is_empty());
    let reset = HookInput {
        hook_event_name: HookEvent::SessionStart,
        session_id: "client".into(),
        stop_hook_active: false,
    };
    assert_ne!(store.hook(&actor, reset, 2000).await?, json!({}));
    Ok(())
}

#[test]
fn assignment_review_correction_and_acceptance_surface_without_manual_context() -> Result<()> {
    let d = Demo::new()?;
    d.create()?;
    assert!(
        d.hook("SessionStart", false)?
            .to_string()
            .contains("Inspect revision")
    );
    let submit = |key: &str| {
        d.call(
            Some(&d.worker),
            &[
                "mail",
                "send",
                "writer",
                "Submitted evidence",
                "--key",
                key,
                "--task",
                "task",
            ],
            None,
            false,
        )
    };
    let first = submit("submission1")?;
    let writer_hook = |event: &str| {
        d.call(
            Some(&d.writer),
            &["adapter", "hook"],
            Some(json!({"hook_event_name":event,"session_id":"writer-client"})),
            false,
        )
    };
    assert!(
        writer_hook("SessionStart")?
            .to_string()
            .contains("Submitted evidence")
    );
    d.decide(json!({"version":1,"reason":"Missing regression case","patch":{"state":"active","next_action":"Add regression case"},"resolve_message":first["id"]}),false)?;
    assert!(d.hook("PostToolUse", false)?.to_string().contains("task"));
    let second = submit("submission2")?;
    assert!(
        writer_hook("PostToolUse")?
            .to_string()
            .contains(&second["id"].to_string())
    );
    d.decide(json!({"version":2,"reason":"Evidence passed","patch":{"state":"accepted","accepted_revision":"abc123"},"resolve_message":second["id"]}),false)?;
    let final_hook = d.hook("PostToolUse", false)?;
    assert!(final_hook.to_string().contains("task"));
    let text = final_hook["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .unwrap();
    let state: Value = serde_json::from_str(text.split_once('\n').unwrap().1)?;
    assert!(state["context"].is_null());
    assert!(!state["changes"]["tasks"].as_array().unwrap().is_empty());
    Ok(())
}
