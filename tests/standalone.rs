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
        self.ok(None, &["register", "--name", name])?["session"]
            .as_str()
            .map(str::to_owned)
            .context("registration did not return a session")
    }
}

#[test]
fn standalone_mail_work_and_session_replacement() -> Result<()> {
    let d = Demo::new()?;
    d.ok(None, &["setup", "--standalone"])?;
    let coordinator = d.register("coordinator")?;
    let worker = d.register("worker")?;
    d.ok(
        Some(&coordinator),
        &[
            "work",
            "create",
            "--id",
            "api",
            "--scope",
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
            "send",
            "--to",
            "worker",
            "--key",
            "request",
            "--summary",
            "Review abc123",
            "--work-id",
            "api",
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
                "work",
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
    assert!(
        !d.call(None, &["register", "--name", "worker"])?
            .status
            .success()
    );
    let replacement = d.ok(None, &["register", "--name", "worker", "--replace"])?;
    let next = replacement["session"]
        .as_str()
        .context("missing replacement")?;
    assert_ne!(next, worker);
    assert!(!d.call(Some(&worker), &["context"])?.status.success());
    assert!(!d.call(Some(&worker), &["resolve", &id])?.status.success());
    let resumed = d.ok(Some(next), &["context"])?;
    assert_eq!(resumed["work"], before["work"]);
    assert_eq!(resumed["mail"], before["mail"]);
    let reply = d.0.path().join("reply.txt");
    std::fs::write(&reply, "Reviewed abc123")?;
    d.ok(
        Some(next),
        &[
            "resolve",
            &id,
            "--reply-key",
            "reviewed",
            "--reply-file",
            reply.to_str().unwrap(),
        ],
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
            "work",
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
    d.ok(None, &["setup"])?;
    let a = d.register("a")?;
    let b = d.register("b")?;
    d.ok(None, &["setup", "--standalone", "--group", "other"])?;
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
            "send",
            "--to",
            "b",
            "--key",
            "one",
            "--summary",
            "Check mail",
        ],
    )?;
    let status = d.ok(None, &["service", "run", "--once"])?;
    assert!(
        status["observations"][0]["state"]
            .as_str()
            .unwrap()
            .contains("unknown")
    );
    let list = d.ok(None, &["participants"])?;
    assert!(!list.to_string().contains(&a));
    assert!(!list.to_string().contains(&b));
    assert_eq!(list[0]["runtime"], "standalone");
    assert!(
        !d.call(None, &["prompt-mode", "--enable-unguarded"])?
            .status
            .success()
    );
    Ok(())
}
