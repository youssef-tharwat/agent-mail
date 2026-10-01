//! Exercise process-scoped identity and native-client launch arguments.
use anyhow::{Result, ensure};
use serde_json::Value;
use std::{
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::{Command, Output},
};

struct Demo(tempfile::TempDir);
impl Demo {
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        command
            .current_dir(self.0.path())
            .args(["--state-dir", self.0.path().to_str().unwrap()])
            .args(args);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("AGENT_MAIL_")
                || key.to_string_lossy().starts_with("HERDR_")
            {
                command.env_remove(key);
            }
        }
        command
    }
    fn call(&self, args: &[&str]) -> Result<Value> {
        decode(self.command(args).output()?)
    }
    fn new() -> Result<Self> {
        let d = Self(tempfile::tempdir()?);
        d.call(&["init", "demo"])?;
        for name in ["coordinator", "alice", "bob"] {
            let result = d.call(&["agent", "add", name])?;
            ensure!(
                result.get("session").is_none(),
                "normal registration exposed a credential"
            );
        }
        for owner in ["alice", "bob"] {
            d.call(&[
                "run",
                "coordinator",
                "--",
                "agent-mail",
                "task",
                "create",
                owner,
                "Review API",
                "--owner",
                owner,
                "--untracked",
            ])?;
        }
        Ok(d)
    }
    fn client(&self, name: &str) -> Result<std::path::PathBuf> {
        let path = self.0.path().join(name);
        std::fs::write(
            &path,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >&2\ntest -z \"${HERDR_ENV-}\" || exit 20\nexec agent-mail context\n",
        )?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
        Ok(path)
    }
}
fn decode(output: Output) -> Result<Value> {
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn concurrent_agents_keep_identity_and_rotation_preserves_assignments() -> Result<()> {
    let d = Demo::new()?;
    let a = d
        .command(&["run", "alice", "--", "agent-mail", "context"])
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let b = d
        .command(&["run", "bob", "--", "agent-mail", "context"])
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    assert_eq!(decode(a.wait_with_output()?)?["work"][0]["id"], "alice");
    assert_eq!(decode(b.wait_with_output()?)?["work"][0]["id"], "bob");
    d.call(&["agent", "replace", "alice"])?;
    assert_eq!(
        d.call(&["run", "alice", "--", "agent-mail", "context"])?["work"][0]["id"],
        "alice"
    );
    assert!(!d.command(&["context"]).output()?.status.success());
    assert!(
        !d.command(&["participant", "list"])
            .output()?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn launch_preserves_exit_status_signals_and_creates_missing_identity() -> Result<()> {
    let d = Demo::new()?;
    assert_eq!(
        d.command(&["run", "alice", "--", "/bin/sh", "-c", "exit 37"])
            .output()?
            .status
            .code(),
        Some(37)
    );
    assert_eq!(
        d.command(&["run", "alice", "--", "/bin/sh", "-c", "kill -TERM $$"])
            .output()?
            .status
            .signal(),
        Some(15)
    );
    let marker = d.0.path().join("must-not-launch");
    let output = d
        .command(&[
            "run",
            "unknown",
            "--",
            "/usr/bin/touch",
            marker.to_str().unwrap(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(marker.exists());
    let registration = d.call(&["agent", "show", "unknown"])?;
    assert_eq!(registration["state"], "registered");
    let missing = d
        .command(&["run", "alice", "--", "/nonexistent/mail-client"])
        .output()?;
    assert!(!missing.status.success());
    Ok(())
}

#[test]
fn native_client_hooks_are_scoped_and_arguments_remain_intact() -> Result<()> {
    let d = Demo::new()?;
    for name in ["claude", "codex"] {
        let client = d.client(name)?;
        let output = d
            .command(&[
                "run",
                "alice",
                "--",
                client.to_str().unwrap(),
                "--resume",
                "session with spaces",
            ])
            .env("HERDR_ENV", "1")
            .output()?;
        let args = String::from_utf8(output.stderr.clone())?;
        assert!(args.contains("--resume\nsession with spaces\n"));
        let config = if name == "claude" {
            assert!(args.contains("--plugin-dir\n"));
            std::fs::read_to_string(
                d.0.path()
                    .join("runtime-hooks/claude/.claude-plugin/plugin.json"),
            )?
        } else {
            assert!(args.contains("--no-daemon"));
            args.clone()
        };
        assert!(config.contains("SessionStart"));
        assert!(config.contains("PostCompact"));
        assert!(config.contains(if name == "claude" {
            "adapter claude-hook"
        } else {
            "adapter hook"
        }));
        assert_eq!(decode(output)?["work"][0]["id"], "alice");
    }
    assert!(!d.0.path().join(".codex/hooks.json").exists());
    Ok(())
}

#[test]
fn claude_preserves_explicit_settings_and_custom_hooks() -> Result<()> {
    let d = Demo::new()?;
    let client = d.client("claude")?;
    let settings = d.0.path().join("custom settings.json");
    let content = r#"{"model":"haiku","permissions":{"defaultMode":"plan"},"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"echo custom"}]}]}}"#;
    std::fs::write(&settings, content)?;
    let output = d
        .command(&[
            "run",
            "alice",
            "--",
            client.to_str().unwrap(),
            "--settings",
            settings.to_str().unwrap(),
        ])
        .output()?;
    let args = String::from_utf8(output.stderr.clone())?;
    assert!(args.contains(&format!("--settings\n{}\n", settings.display())));
    assert!(
        !args.contains("defaultMode"),
        "settings contents leaked into arguments"
    );
    assert_eq!(std::fs::read_to_string(settings)?, content);
    decode(output)?;
    Ok(())
}

#[test]
fn bundled_skill_needs_no_state_and_rejects_combined_mutation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_agent-mail"))
        .current_dir(temp.path())
        .arg("--skill")
        .env("AGENT_MAIL_STATE_DIR", temp.path().join("absent"))
        .output()?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout)?, agent_mail::SKILL);
    assert!(!temp.path().join("absent").exists());
    assert!(
        !Command::new(env!("CARGO_BIN_EXE_agent-mail"))
            .args(["--skill", "init", "oops"])
            .output()?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn simultaneous_native_launches_establish_one_worker_without_manual_setup() -> Result<()> {
    let d = Demo::new()?;
    let client = d.client("claude")?;
    let a = d
        .command(&["run", "alice", "--", client.to_str().unwrap()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let b = d
        .command(&["run", "bob", "--", client.to_str().unwrap()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    assert_eq!(decode(a.wait_with_output()?)?["work"][0]["id"], "alice");
    assert_eq!(decode(b.wait_with_output()?)?["work"][0]["id"], "bob");
    assert!(agent_mail::service::running(d.0.path()));
    assert!(agent_mail::service::WorkerLock::acquire(d.0.path()).is_err());
    let status = d.call(&["status", "--json"])?;
    assert!(
        status["delivery"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["ready"] == false)
    );
    Ok(())
}

#[test]
fn committed_mutations_report_unavailable_recipients_without_duplicate_writes() -> Result<()> {
    let d = Demo::new()?;
    let args = [
        "run",
        "coordinator",
        "--",
        "agent-mail",
        "mail",
        "send",
        "alice",
        "Review needed",
        "--key",
        "delivery-result",
    ];
    let sent = d.call(&args)?;
    assert_eq!(sent["persisted"], true);
    assert_eq!(sent["delivery"][0]["ready"], false);
    assert_eq!(sent["delivery"][0]["state"], "missing_endpoint");
    assert_eq!(d.call(&args)?["id"], sent["id"]);
    let id = sent["id"].as_i64().unwrap().to_string();
    let reply = d.call(&[
        "run",
        "alice",
        "--",
        "agent-mail",
        "mail",
        "reply",
        &id,
        "Reviewed",
    ])?;
    assert_eq!(reply["resolved"], true);
    assert_eq!(reply["delivery"][0]["agent"], "coordinator");
    assert_eq!(reply["delivery"][0]["ready"], false);
    let task = d.call(&[
        "run",
        "coordinator",
        "--",
        "agent-mail",
        "task",
        "create",
        "new",
        "Review",
        "--owner",
        "alice",
        "--untracked",
    ])?;
    assert!(
        task["delivery"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["agent"] == "alice" && r["ready"] == false)
    );
    let update = d.call(&[
        "run",
        "coordinator",
        "--",
        "agent-mail",
        "task",
        "update",
        "new",
        "--version",
        "1",
        "--owner",
        "bob",
        "--reason",
        "Reassign",
    ])?;
    let recipients = update["delivery"].as_array().unwrap();
    for name in ["alice", "bob"] {
        assert!(
            recipients
                .iter()
                .any(|r| r["agent"] == name && r["ready"] == false)
        );
    }
    Ok(())
}

#[test]
fn simultaneous_first_runs_keep_one_identity_and_retired_agents_stay_retired() -> Result<()> {
    let d = Demo::new()?;
    let mut children = Vec::new();
    for _ in 0..3 {
        children.push(
            d.command(&["run", "new-worker", "--", "agent-mail", "context"])
                .stdout(std::process::Stdio::piped())
                .spawn()?,
        );
    }
    for child in children {
        decode(child.wait_with_output()?)?;
    }
    let before = d.call(&["agent", "show", "new-worker"])?;
    d.call(&["run", "new-worker", "--", "agent-mail", "context"])?;
    assert_eq!(
        d.call(&["agent", "show", "new-worker"])?["version"],
        before["version"]
    );
    d.call(&[
        "agent",
        "update",
        "new-worker",
        "--version",
        "1",
        "--state",
        "retired",
        "--reason",
        "Finished",
    ])?;
    assert!(
        !d.command(&["run", "new-worker", "--", "/usr/bin/true"])
            .output()?
            .status
            .success()
    );
    assert_eq!(
        d.call(&["agent", "show", "new-worker"])?["state"],
        "retired"
    );
    Ok(())
}

#[test]
fn help_is_categorized_and_status_has_explicit_structured_output() -> Result<()> {
    let d = Demo::new()?;
    let help = d.command(&["--help"]).output()?;
    assert!(help.status.success());
    let text = String::from_utf8(help.stdout)?;
    for category in ["Start:", "Coordinate:", "Manage:", "Integrations:"] {
        assert!(text.contains(category));
    }
    let help = d.command(&["help"]).output()?;
    assert!(String::from_utf8(help.stdout)?.contains("Coordinate:"));
    let status = d.command(&["status"]).output()?;
    assert!(status.status.success());
    let text = String::from_utf8(status.stdout)?;
    assert!(text.contains("Group: demo"));
    assert!(text.contains("Unavailable · no runtime attached"));
    assert!(!text.contains("{\""));
    assert!(d.call(&["status", "--json"])?["delivery"].is_array());
    assert!(
        !d.command(&["agent", "verify", "alice"])
            .output()?
            .status
            .success()
    );
    assert!(
        !d.command(&["runtime", "retry", "alice"])
            .output()?
            .status
            .success()
    );
    Ok(())
}
