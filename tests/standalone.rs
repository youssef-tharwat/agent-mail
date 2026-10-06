//! Exercise the public CLI without a Herdr server or environment.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::process::{Command, Output};

struct Demo(tempfile::TempDir);
impl Demo {
    fn new() -> Result<Self> {
        Ok(Self(tempfile::tempdir()?))
    }
    fn call(&self, session: Option<&str>, args: &[&str]) -> Result<Output> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        command
            .args(["--state-dir", self.0.path().to_str().unwrap()])
            .args(args)
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_SESSION")
            .env_remove("HERDR_ENV")
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_PLUGIN_ID");
        if let Some(session) = session {
            command.env("AGENT_MAIL_SESSION", session);
        }
        Ok(command.output()?)
    }
    fn ok(&self, session: Option<&str>, args: &[&str]) -> Result<Value> {
        let output = self.call(session, args)?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    fn register(&self, name: &str) -> Result<String> {
        self.ok(None, &["agent", "add", name, "--show-session"])?["session"]
            .as_str()
            .map(str::to_owned)
            .context("registration did not return a session")
    }
}

#[test]
fn send_requires_context_and_conversation_replies_inherit_it() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["init", "g"])?;
    let writer = d.register("writer")?;
    let owner = d.register("owner")?;
    for extra in [
        vec![],
        vec!["--task", "t"],
        vec!["--task", "t", "--version", "0"],
        vec![
            "--new-conversation",
            "--conversation",
            "00000000-0000-4000-8000-000000000001",
        ],
    ] {
        let mut args = vec!["mail", "send", "owner", "Discussion", "--key", "first"];
        args.extend(extra);
        assert!(!d.call(Some(&writer), &args)?.status.success());
    }
    assert!(
        d.ok(Some(&owner), &["mail", "list"])?["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let sent = d.ok(
        Some(&writer),
        &[
            "mail",
            "send",
            "owner",
            "Discussion",
            "--key",
            "first",
            "--new-conversation",
        ],
    )?;
    let id = sent["id"].to_string();
    assert_eq!(sent["context"]["kind"], "conversation");
    let interim = d.ok(
        Some(&owner),
        &[
            "mail",
            "send",
            "writer",
            "Progress",
            "--key",
            "interim",
            "--reply-to",
            &id,
        ],
    )?;
    assert_eq!(interim["context"], sent["context"]);
    assert_eq!(
        d.ok(Some(&owner), &["mail", "show", &id])?["state"],
        "pending"
    );
    let thread = sent["context"]["id"].as_str().unwrap();
    let page = d.ok(Some(&writer), &["mail", "conversation", thread])?;
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    let reply = d.ok(Some(&owner), &["mail", "reply", &id, "Complete"])?;
    let response = d.ok(
        Some(&writer),
        &["mail", "show", &reply["reply_id"].to_string()],
    )?;
    assert_eq!(response["context"], sent["context"]);
    Ok(())
}

#[test]
fn followthrough_defaults_and_direct_policy_flags_need_no_file() -> Result<()> {
    let d = Demo::new()?;
    assert_eq!(
        d.ok(None, &["init", "fleet"])?["follow_through"]["mode"],
        "enabled"
    );
    let policy = d.ok(
        None,
        &[
            "attention",
            "configure",
            "--observe",
            "--interval",
            "10m",
            "--max",
            "1h",
            "--notifier",
            "/usr/bin/true",
            "--notifier-arg=--quiet",
        ],
    )?;
    assert_eq!(policy["policy"]["interval_seconds"], 600);
    assert_eq!(
        policy["policy"]["notifier"],
        json!(["/usr/bin/true", "--quiet"])
    );
    assert_eq!(
        d.ok(None, &["init", "fleet"])?["follow_through"]["mode"],
        "observe"
    );
    let policy = d.ok(None, &["attention", "configure", "--enable"])?;
    assert_eq!(policy["policy"]["interval_seconds"], 600);
    assert_eq!(
        policy["policy"]["notifier"],
        json!(["/usr/bin/true", "--quiet"])
    );
    assert!(
        !d.call(None, &["attention", "configure", "--enable", "--observe"])?
            .status
            .success()
    );
    assert!(
        !d.call(None, &["attention", "configure", "--interval", "1h"])?
            .status
            .success()
    );
    assert!(
        !d.call(
            None,
            &[
                "attention",
                "configure",
                "--file",
                "missing.json",
                "--enable"
            ]
        )?
        .status
        .success()
    );
    assert_eq!(
        d.ok(None, &["attention", "configure", "--clear-notifier"])?["policy"]["notifier"],
        Value::Null
    );
    assert_eq!(
        d.ok(None, &["attention", "configure"])?["policy"]["mode"],
        "enabled"
    );
    assert_eq!(
        d.ok(None, &["init", "manual", "--no-follow-through"])?["follow_through"]["mode"],
        "observe"
    );
    assert_eq!(
        d.ok(None, &["init", "manual", "--follow-through"])?["follow_through"]["mode"],
        "enabled"
    );
    Ok(())
}

#[test]
fn standalone_mail_work_and_session_replacement() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["init", "default"])?;
    let coordinator = d.register("coordinator")?;
    let worker = d.register("worker")?;
    d.ok(
        Some(&coordinator),
        &[
            "task",
            "create",
            "api",
            "Review API",
            "--owner",
            "worker",
            "--next-action",
            "Review abc123",
        ],
    )?;
    let sent = d.ok(
        Some(&coordinator),
        &[
            "mail",
            "send",
            "worker",
            "Review abc123",
            "--key",
            "request",
            "--task",
            "api",
            "--version",
            "1",
        ],
    )?;
    let id = sent["id"].to_string();
    let before = d.ok(Some(&worker), &["context"])?;
    assert_eq!(before["work"][0]["id"], "api");
    assert_eq!(before["mail"][0]["id"], sent["id"]);
    assert!(!d.call(None, &["context"])?.status.success());
    assert!(
        !d.call(
            Some(&worker),
            &[
                "task",
                "update",
                "api",
                "--version",
                "1",
                "--reason",
                "not the writer",
                "--state",
                "accepted"
            ]
        )?
        .status
        .success()
    );
    assert!(!d.call(None, &["agent", "add", "worker"])?.status.success());
    let replacement = d.ok(None, &["agent", "replace", "worker", "--show-session"])?;
    let next = replacement["session"]
        .as_str()
        .context("missing replacement")?;
    assert_ne!(next, worker);
    assert!(!d.call(Some(&worker), &["context"])?.status.success());
    assert!(
        !d.call(
            Some(&worker),
            &["mail", "resolve", &id, "--note", "handled"]
        )?
        .status
        .success()
    );
    let resumed = d.ok(Some(next), &["context"])?;
    assert_eq!(resumed["work"], before["work"]);
    assert_eq!(resumed["mail"], before["mail"]);
    let reply = d.0.path().join("reply.txt");
    std::fs::write(&reply, "Reviewed abc123")?;
    d.ok(
        Some(next),
        &["mail", "reply", &id, "--body-file", reply.to_str().unwrap()],
    )?;
    assert_eq!(d.ok(Some(next), &["context"])?["mail"], json!([]));
    assert_eq!(
        d.ok(Some(&coordinator), &["context"])?["mail"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    d.ok(
        Some(&coordinator),
        &[
            "task",
            "update",
            "api",
            "--version",
            "1",
            "--reason",
            "Review received",
            "--state",
            "review",
        ],
    )?;
    assert_eq!(d.ok(Some(next), &["context"])?["work"][0]["version"], 2);
    Ok(())
}

#[test]
fn standalone_identity_is_scoped_and_never_infers_liveness() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["init", "default"])?;
    let a = d.register("a")?;
    let b = d.register("b")?;
    d.ok(None, &["init", "other"])?;
    assert!(
        !d.call(Some(&a), &["context", "--group", "other"])?
            .status
            .success()
    );
    assert!(
        !d.call(Some("not-a-session"), &["context"])?
            .status
            .success()
    );
    d.ok(
        Some(&a),
        &[
            "mail",
            "send",
            "b",
            "Check mail",
            "--key",
            "one",
            "--new-conversation",
        ],
    )?;
    let status = d.ok(None, &["service", "run", "--once"])?;
    assert_eq!(status["observations"][0]["state"], "unavailable");
    let list = d.ok(None, &["agent", "list", "--group", "default"])?;
    assert!(!list.to_string().contains(&a));
    assert!(!list.to_string().contains(&b));
    assert_eq!(list[0]["runtime"], "standalone");
    assert!(
        !d.call(None, &["runtime", "herdr-policy", "unguarded"])?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn natural_retries_and_short_replies_preserve_one_logical_change() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["init", "project"])?;
    let writer = d.register("writer")?;
    let worker = d.register("worker")?;
    let create = ["task", "create", "api", "Review API", "--owner", "worker"];
    let first = d.ok(Some(&writer), &create)?;
    assert_eq!(first["next_action"], "Review API");
    assert_eq!(first, d.ok(Some(&writer), &create)?);
    let update = [
        "task",
        "update",
        "api",
        "--version",
        "1",
        "--reason",
        "Clarify",
        "--next-action",
        "Review retries",
    ];
    let changed = d.ok(Some(&writer), &update)?;
    assert_eq!(changed, d.ok(Some(&writer), &update)?);
    assert_eq!(first, d.ok(Some(&writer), &create)?);
    assert_eq!(d.ok(Some(&writer), &["task", "show", "api"])?["version"], 2);
    assert_eq!(
        d.ok(Some(&writer), &["task", "history", "api"])?["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        !d.call(
            Some(&writer),
            &[
                "task",
                "update",
                "api",
                "--version",
                "1",
                "--reason",
                "Different",
                "--state",
                "accepted"
            ]
        )?
        .status
        .success()
    );
    let sent = d.ok(
        Some(&writer),
        &[
            "mail",
            "send",
            "worker",
            "Question",
            "--key",
            "question",
            "--new-conversation",
        ],
    )?;
    let id = sent["id"].to_string();
    assert!(d.ok(Some(&worker), &["mail", "show", &id])?["due"].is_null());
    let reply = d.ok(Some(&worker), &["mail", "reply", &id, "Answered"])?;
    assert_eq!(
        reply,
        d.ok(Some(&worker), &["mail", "reply", &id, "Answered"])?
    );
    assert!(
        !d.call(Some(&worker), &["mail", "reply", &id, "Different"])?
            .status
            .success()
    );
    assert_eq!(
        d.ok(Some(&writer), &["mail", "list"])?["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    Ok(())
}

#[test]
fn selection_is_unambiguous_and_configuration_never_overwrites() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["init", "one"])?;
    let worker = d.register("worker")?;
    d.ok(None, &["init", "two"])?;
    assert!(!d.call(None, &["agent", "list"])?.status.success());
    d.ok(Some(&worker), &["context"])?;
    assert!(
        !d.call(Some(&worker), &["context", "--group", "two"])?
            .status
            .success()
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
    command
        .args(["--state-dir", d.0.path().to_str().unwrap(), "context"])
        .env("AGENT_MAIL_SESSION", &worker)
        .env("AGENT_MAIL_GROUP", "one");
    assert!(command.output()?.status.success());
    command.env("AGENT_MAIL_GROUP", "two");
    assert!(!command.output()?.status.success());
    command.args(["--group", "one"]);
    assert!(command.output()?.status.success());
    let path = d.0.path().join("settings/hooks.json");
    let config = [
        "runtime",
        "configure",
        "claude",
        "--output",
        path.to_str().unwrap(),
    ];
    d.ok(None, &config)?;
    d.ok(None, &config)?;
    let content = std::fs::read_to_string(&path)?;
    assert!(content.contains("agent-mail adapter claude-hook"));
    std::fs::write(&path, "do not overwrite")?;
    assert!(!d.call(None, &config)?.status.success());
    assert_eq!(std::fs::read_to_string(&path)?, "do not overwrite");
    Ok(())
}
