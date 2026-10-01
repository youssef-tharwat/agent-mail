//! Public process boundary: parse errors, contract creation and report versions.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Fixture(tempfile::TempDir);
impl Fixture {
    fn command(&self, session: Option<&str>, args: &[&str]) -> Result<Command> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        command
            .args(["--state-dir", self.0.path().to_str().context("temp path")?])
            .args(args)
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_SESSION")
            .env_remove("HERDR_ENV")
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_PLUGIN_ID");
        if let Some(session) = session {
            command.env("AGENT_MAIL_SESSION", session);
        }
        Ok(command)
    }
    fn call(&self, session: Option<&str>, args: &[&str]) -> Result<Output> {
        Ok(self.command(session, args)?.output()?)
    }
    async fn worker_input(&self, args: &[&str], input: &[u8], close_input: bool) -> Result<Output> {
        use tokio::io::AsyncWriteExt;
        let mut child = tokio::process::Command::from(self.command(None, args)?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut writer = Some(child.stdin.take().context("worker stdin")?);
        // Bound both writing and waiting, including a regression that never reads stdin.
        let finished = tokio::time::timeout(Duration::from_secs(8), async {
            writer
                .as_mut()
                .context("worker stdin")?
                .write_all(input)
                .await?;
            if close_input {
                drop(writer.take());
            }
            // Otherwise retain the write end: the child's own EOF timeout must exit.
            child.wait().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(&finished, Ok(Ok(()))) {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        drop(writer);
        finished.context("hidden worker failed to exit within eight seconds")??;
        Ok(child.wait_with_output().await?)
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
    fn refused(&self, session: Option<&str>, args: &[&str], code: i32, cause: &str) -> Result<()> {
        let output = self.call(session, args)?;
        assert_eq!(
            output.status.code(),
            Some(code),
            "{args:?}: {:?}",
            output.stderr
        );
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(cause),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}
#[test]
fn contracted_create_is_atomic_and_checkpoint_preserves_business_version() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    f.ok(None, &["init", "product"])?;
    let writer = f.ok(None, &["agent", "add", "writer", "--show-session"])?["session"]
        .as_str()
        .context("session")?
        .to_owned();
    f.ok(None, &["agent", "add", "worker"])?;
    let missing = f.call(
        Some(&writer),
        &[
            "task",
            "create",
            "review",
            "Review artifact",
            "--owner",
            "worker",
            "--key",
            "create-1",
            "--reason",
            "Approved assignment",
        ],
    )?;
    assert_eq!(missing.status.code(), Some(1));
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("incomplete_contract"));
    assert!(
        !f.call(Some(&writer), &["task", "show", "review"])?
            .status
            .success()
    );
    let mut create = vec![
        "task",
        "create",
        "review",
        "Review artifact",
        "--owner",
        "worker",
        "--key",
        "create-1",
        "--reason",
        "Approved assignment",
        "--criterion",
        "readable=Artifact is readable",
        "--allow",
        "read artifact",
        "--authorize",
        "approval/test",
        "--max-attempts",
        "3",
        "--max-elapsed",
        "15m",
    ];
    // Preserve the original R18 request: complete bounds are not consent.
    let no_consent = f.call(Some(&writer), &create)?;
    assert_eq!(no_consent.status.code(), Some(1));
    assert!(no_consent.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&no_consent.stderr).contains("input_invalidation_consent_required")
    );
    assert!(
        !f.call(Some(&writer), &["task", "show", "review"])?
            .status
            .success()
    );
    create.push("--allow-input-invalidation");
    let first = f.ok(Some(&writer), &create)?;
    let replay = f.ok(Some(&writer), &create)?;
    assert_eq!(first, replay);
    assert_eq!(first["work"]["version"], 1);
    assert!(first["model"].is_object());
    let inspect = f.ok(Some(&writer), &["task", "inspect", "review"])?;
    assert!(inspect["execution"]["revision"].is_number());
    let followup = f.ok(Some(&writer), &["task", "show", "review"])?;
    let plan_version = followup["followup"]["version"]
        .as_i64()
        .context("plan version")?
        .to_string();
    let now = time::OffsetDateTime::now_utc() + time::Duration::minutes(1);
    let check_at = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    f.ok(
        Some(&writer),
        &[
            "task",
            "checkpoint",
            "review",
            "--version",
            "1",
            "--plan-version",
            &plan_version,
            "--key",
            "checkpoint-1",
            "--next-step",
            "Inspect evidence",
            "--check-at",
            &check_at,
        ],
    )?;
    assert_eq!(
        f.ok(Some(&writer), &["task", "show", "review"])?["version"],
        1
    );
    let mixed = f.call(
        Some(&writer),
        &[
            "task",
            "create",
            "legacy",
            "Legacy record",
            "--owner",
            "worker",
            "--untracked",
            "--max-attempts",
            "3",
        ],
    )?;
    assert_eq!(mixed.status.code(), Some(2));
    assert!(
        !f.call(Some(&writer), &["task", "show", "legacy"])?
            .status
            .success()
    );
    let legacy = f.ok(
        Some(&writer),
        &[
            "task",
            "create",
            "legacy",
            "Legacy record",
            "--owner",
            "worker",
            "--untracked",
        ],
    )?;
    assert_eq!(legacy["version"], 1);
    Ok(())
}

#[test]
fn help_is_state_free_and_hidden_worker_is_not_advertised() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    for args in [
        vec!["--help"],
        vec!["task", "create", "--help"],
        vec!["task", "adopt", "--help"],
        vec!["task", "decide", "--help"],
        vec!["task", "candidate", "--help"],
        vec!["task", "inputs", "--help"],
        vec!["decision", "policy", "--help"],
        vec!["decision", "correct", "--help"],
        vec!["decision", "writer-fallback", "--help"],
        vec!["decision", "continue-strategy", "--help"],
        vec!["runtime", "capabilities", "--help"],
        vec!["runtime", "target", "configure", "--help"],
        vec!["runtime", "target", "show", "--help"],
        vec!["runtime", "target", "disable", "--help"],
        vec!["runtime", "target", "retire", "--help"],
        vec!["runtime", "target", "enable", "--help"],
        vec!["runtime", "artifact", "bind", "--help"],
        vec!["runtime", "artifact", "show", "--help"],
        vec!["runtime", "artifact", "publish", "--help"],
        vec!["runtime", "artifact", "receipt", "--help"],
        vec!["task", "execution", "report", "--help"],
        vec!["status", "--help"],
        vec!["task", "progress", "policy", "--help"],
        vec!["task", "progress", "judge", "--help"],
        vec!["task", "progress", "grant", "--help"],
        vec!["task", "followup", "correct", "--help"],
        vec!["mail", "followup", "correct", "--help"],
        vec!["task", "execution", "stop", "--help"],
        vec!["task", "execution", "schedule", "--help"],
        vec!["task", "execution", "resolve-clock", "--help"],
        vec!["task", "checkpoint", "--help"],
        vec!["mail", "checkpoint", "--help"],
        vec!["attention", "checkpoint", "--help"],
    ] {
        let output = f.call(None, &args)?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("__managed-worker-v1"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("__managed-contained-v1"));
    }
    assert!(!f.0.path().join("mail.db").exists());
    Ok(())
}

#[test]
fn tracking_pages_keep_terminal_legacy_work_and_runtime_absence_honest() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    f.ok(None, &["init", "product"])?;
    let writer = f.ok(None, &["agent", "add", "writer", "--show-session"])?["session"]
        .as_str()
        .context("writer session")?
        .to_owned();
    let other = f.ok(None, &["agent", "add", "other", "--show-session"])?["session"]
        .as_str()
        .context("other session")?
        .to_owned();
    for id in ["a", "b", "c"] {
        f.ok(
            Some(&writer),
            &[
                "task",
                "create",
                id,
                "Legacy review",
                "--owner",
                "writer",
                "--untracked",
            ],
        )?;
    }
    f.ok(
        Some(&other),
        &[
            "task",
            "create",
            "private",
            "Other assignment",
            "--owner",
            "other",
            "--untracked",
        ],
    )?;
    f.ok(
        Some(&writer),
        &[
            "task",
            "update",
            "a",
            "--version",
            "1",
            "--reason",
            "Legacy completion",
            "--state",
            "done",
        ],
    )?;
    let before = f.ok(
        Some(&writer),
        &["task", "followup", "show", "b", "--version", "1"],
    )?;
    let page = f.ok(
        Some(&writer),
        &["task", "list", "--details", "--limit", "2"],
    )?;
    assert_eq!(page["atomic_snapshot"], false);
    assert_eq!(page["has_more"], true);
    assert_eq!(page["next_cursor"], "b");
    assert_eq!(page["items"][0]["task"]["work"]["id"], "a");
    assert_eq!(page["items"][0]["task"]["work"]["state"], "done");
    assert_eq!(
        page["items"][0]["task"]["execution_hold"],
        "legacy_untracked"
    );
    let tail = f.ok(
        Some(&writer),
        &["task", "list", "--details", "--after", "b", "--limit", "2"],
    )?;
    assert_eq!(tail["has_more"], false);
    assert_eq!(tail["items"].as_array().context("items")?.len(), 1);
    assert_eq!(tail["items"][0]["task"]["work"]["id"], "c");
    let status = f.ok(
        Some(&writer),
        &["status", "--json", "--tasks", "--limit", "2"],
    )?;
    assert_eq!(status["task_tracking"]["next_cursor"], "b");
    assert_eq!(
        before,
        f.ok(
            Some(&writer),
            &["task", "followup", "show", "b", "--version", "1"]
        )?
    );
    let absent = f.ok(
        Some(&writer),
        &["runtime", "capabilities", "missing-target"],
    )?;
    assert_eq!(absent["present"], false);
    assert!(absent["capabilities"].is_null());
    let report = f.call(
        Some(&writer),
        &[
            "task",
            "execution",
            "report",
            "b",
            "--attempt",
            "missing-attempt",
            "--fence",
            "1",
            "--dispatch-key",
            "missing-dispatch",
            "--key",
            "report-1",
            "--kind",
            "yield",
            "--summary",
            "No admitted attempt",
        ],
    )?;
    assert!(!report.status.success());
    assert!(report.stdout.is_empty());
    let continuation = f.call(
        Some(&writer),
        &[
            "decision",
            "continue-strategy",
            "b",
            "--version",
            "1",
            "--case-version",
            "1",
            "--policy-version",
            "1",
            "--execution-version",
            "1",
            "--additional-segments",
            "1",
            "--expires-at",
            "2099-01-01T00:00:00Z",
            "--candidate",
            "absent-candidate",
            "--key",
            "outer-1",
            "--decision-key",
            "outcome-1",
            "--continuation-key",
            "strategy-1",
            "--reason",
            "No actual materialized decision",
        ],
    )?;
    assert!(!continuation.status.success());
    assert!(continuation.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&continuation.stderr)
            .contains("actual_materialized_decision_required")
    );
    assert_eq!(
        before,
        f.ok(
            Some(&writer),
            &["task", "followup", "show", "b", "--version", "1"]
        )?
    );
    Ok(())
}

#[test]
fn runtime_flags_require_explicit_guards_without_opening_state() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    for args in [
        vec![
            "runtime",
            "target",
            "disable",
            "target",
            "--key",
            "k",
            "--reason",
            "r",
            "--generation",
            "1",
        ],
        vec![
            "runtime",
            "target",
            "enable",
            "target",
            "--key",
            "k",
            "--reason",
            "r",
            "--generation",
            "1",
            "--revision",
            "1",
        ],
        vec![
            "runtime",
            "target",
            "configure",
            "name",
            "--key",
            "k",
            "--reason",
            "r",
            "--owner",
            "writer",
            "--client",
            "codex",
            "--cwd",
            "/tmp",
            "--profile",
            "read-only",
            "--configuration",
            "policy",
            "--generation",
            "1",
        ],
        vec![
            "runtime",
            "artifact",
            "bind",
            "binding",
            "--key",
            "k",
            "--reason",
            "r",
            "--task",
            "work",
            "--target",
            "target",
            "--target-generation",
            "1",
            "--destination",
            "review",
            "--scope",
            "write review",
            "--allowed-path",
            "review.txt",
        ],
        vec![
            "runtime",
            "artifact",
            "publish",
            "work",
            "--attempt",
            "attempt",
            "--fence",
            "1",
            "--dispatch-key",
            "dispatch",
            "--effect",
            "effect",
            "--destination",
            "review",
            "--scope",
            "write review",
            "--generation",
            "1",
            "--task-version",
            "1",
            "--text-file",
            "review.txt=absent",
        ],
        vec![
            "runtime",
            "artifact",
            "receipt",
            "work",
            "--attempt",
            "attempt",
            "--fence",
            "1",
            "--effect",
            "effect",
        ],
    ] {
        f.refused(None, &args, 2, "required arguments")?;
    }
    for args in [
        vec![
            "runtime",
            "target",
            "configure",
            "name",
            "--file",
            "absent",
            "--key",
            "k",
        ],
        vec![
            "runtime",
            "target",
            "disable",
            "target",
            "--file",
            "absent",
            "--revision",
            "1",
        ],
        vec![
            "runtime", "artifact", "bind", "binding", "--file", "absent", "--task", "work",
        ],
        vec![
            "runtime",
            "artifact",
            "publish",
            "work",
            "--file",
            "absent",
            "--empty-destination",
        ],
    ] {
        f.refused(None, &args, 2, "cannot be used with")?;
    }
    assert!(!f.0.path().join("mail.db").exists());
    Ok(())
}

#[test]
fn runtime_inputs_are_bounded_and_never_substitute_producer_authority() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    f.ok(None, &["init", "product"])?;
    let writer = f.ok(None, &["agent", "add", "writer", "--show-session"])?["session"]
        .as_str()
        .context("writer session")?
        .to_owned();
    f.ok(
        Some(&writer),
        &[
            "task",
            "create",
            "work",
            "Review",
            "--owner",
            "writer",
            "--untracked",
        ],
    )?;
    let before = f.ok(
        Some(&writer),
        &["task", "followup", "show", "work", "--version", "1"],
    )?;
    let absent = f.ok(
        Some(&writer),
        &["runtime", "target", "show", "missing-target"],
    )?;
    assert_eq!(absent["present"], false);
    assert!(absent["capabilities"].is_null());
    let absent = f.ok(
        Some(&writer),
        &["runtime", "artifact", "show", "missing-binding"],
    )?;
    assert_eq!(absent["present"], false);
    assert!(absent["binding"].is_null());
    for action in ["disable", "retire", "enable"] {
        let mut args = vec![
            "runtime",
            "target",
            action,
            "missing-target",
            "--key",
            action,
            "--reason",
            "Missing target",
            "--generation",
            "1",
            "--revision",
            "1",
        ];
        if action == "enable" {
            args.extend(["--qualification", "unproven-qualification"]);
        }
        f.refused(Some(&writer), &args, 1, "managed_target_missing")?;
    }
    let text_path = f.0.path().join("input.txt");
    let file_arg = format!("review.txt={}", text_path.display());
    let publish = vec![
        "runtime",
        "artifact",
        "publish",
        "work",
        "--attempt",
        "absent-attempt",
        "--fence",
        "1",
        "--dispatch-key",
        "absent-dispatch",
        "--effect",
        "review-1",
        "--destination",
        "review",
        "--scope",
        "write review",
        "--generation",
        "1",
        "--task-version",
        "1",
        "--empty-destination",
        "--text-file",
        &file_arg,
    ];
    std::fs::write(&text_path, b"review")?;
    f.refused(
        Some(&writer),
        &publish,
        1,
        "managed_original_producer_conflict",
    )?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "artifact",
            "receipt",
            "work",
            "--attempt",
            "absent-attempt",
            "--fence",
            "1",
            "--dispatch-key",
            "absent-dispatch",
            "--effect",
            "review-1",
        ],
        1,
        "managed_original_producer_conflict",
    )?;
    std::fs::write(&text_path, [0xff])?;
    f.refused(Some(&writer), &publish, 1, "input must be UTF-8")?;
    std::fs::write(&text_path, vec![b'x'; 4097])?;
    f.refused(Some(&writer), &publish, 1, "input exceeds 4096 UTF-8 bytes")?;
    std::fs::write(&text_path, vec![b'x'; 4096])?;
    let second = format!("second.txt={}", text_path.display());
    let third = format!("third.txt={}", text_path.display());
    let fourth = format!("fourth.txt={}", text_path.display());
    let mut too_large = publish.clone();
    too_large.extend([
        "--text-file",
        &second,
        "--text-file",
        &third,
        "--text-file",
        &fourth,
    ]);
    f.refused(Some(&writer), &too_large, 1, "text_artifact_too_large")?;
    std::fs::write(&text_path, b"review")?;
    let mut duplicate = publish.clone();
    duplicate.extend(["--text-file", &file_arg]);
    f.refused(Some(&writer), &duplicate, 1, "duplicate artifact path")?;
    let traversal = format!("../review.txt={}", text_path.display());
    let mut invalid_path = publish.clone();
    *invalid_path.last_mut().context("text file argument")? = &traversal;
    f.refused(
        Some(&writer),
        &invalid_path,
        1,
        "artifact path must be canonical and relative",
    )?;
    std::fs::remove_file(&text_path)?;
    std::fs::create_dir(&text_path)?;
    f.refused(Some(&writer), &publish, 1, "input must be a regular file")?;

    let request_path = f.0.path().join("request.json");
    let request_file = request_path.to_str().context("request path")?;
    let publication = serde_json::json!({
        "request": {
            "correlation": {"group":"product", "task":"work", "attempt":"absent-attempt", "fence":1, "dispatch_key":"absent-dispatch"},
            "effect":"review-1", "destination":"review", "scope_unit":"write review",
            "manifest":{"version":1,"files":[{"path":"review.txt", "bytes":6,
                "digest":agent_mail::runtime_effects::ContentDigest::of_bytes(b"review")}]},
            "expected_generation":1, "expected_manifest":null, "expected_task_version":1
        },
        "contents":{"objects":[{"digest":agent_mail::runtime_effects::ContentDigest::of_bytes(b"review"), "text":"review"}]}
    });
    let file_publish = [
        "runtime",
        "artifact",
        "publish",
        "work",
        "--file",
        request_file,
    ];
    std::fs::write(&request_path, serde_json::to_vec(&publication)?)?;
    f.refused(
        Some(&writer),
        &file_publish,
        1,
        "managed_original_producer_conflict",
    )?;
    let mut missing_cas = publication.clone();
    missing_cas["request"]
        .as_object_mut()
        .context("request")?
        .remove("expected_manifest");
    std::fs::write(&request_path, serde_json::to_vec(&missing_cas)?)?;
    f.refused(
        Some(&writer),
        &file_publish,
        1,
        "explicit publication manifest CAS",
    )?;
    let mut missing_cas = publication.clone();
    missing_cas["request"]["expected_task_version"] = Value::Null;
    std::fs::write(&request_path, serde_json::to_vec(&missing_cas)?)?;
    f.refused(
        Some(&writer),
        &file_publish,
        1,
        "explicit publication task CAS",
    )?;
    let mut wrong_group = publication;
    wrong_group["request"]["correlation"]["group"] = "other".into();
    std::fs::write(&request_path, serde_json::to_vec(&wrong_group)?)?;
    f.refused(
        Some(&writer),
        &file_publish,
        1,
        "publication_source_identity_conflict",
    )?;
    let configure = serde_json::json!({
        "key":"configure", "reason":"Identity guard", "spec":{
            "target":"other-name", "owner":"writer", "client":"codex", "cwd":"/tmp",
            "profile":"read_only", "artifact_root":null, "configuration":"unprovisioned-policy"
        }, "expected_generation":null, "expected_revision":null
    });
    std::fs::write(&request_path, serde_json::to_vec(&configure)?)?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "target",
            "configure",
            "name",
            "--file",
            request_file,
        ],
        1,
        "target_identity_conflict",
    )?;
    let binding = serde_json::json!({
        "id":"other-binding", "key":"binding", "reason":"Identity guard", "task":"work",
        "expected_task_version":1, "target":"missing-target", "target_generation":1,
        "destination":"review", "scope_unit":"write review", "allowed_paths":["review.txt"],
        "expected_binding_revision":null
    });
    std::fs::write(&request_path, serde_json::to_vec(&binding)?)?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "artifact",
            "bind",
            "binding",
            "--file",
            request_file,
        ],
        1,
        "artifact_binding_identity_conflict",
    )?;
    let mut change = serde_json::json!({"key":"disable", "reason":"Keep disabled", "target":"missing-target", "expected_generation":1, "expected_revision":1, "action":{"kind":"disable"}});
    std::fs::write(&request_path, serde_json::to_vec(&change)?)?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "target",
            "disable",
            "missing-target",
            "--file",
            request_file,
        ],
        1,
        "managed_target_missing",
    )?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "target",
            "retire",
            "missing-target",
            "--file",
            request_file,
        ],
        1,
        "target_action_conflict",
    )?;
    change["target"] = "another-target".into();
    std::fs::write(&request_path, serde_json::to_vec(&change)?)?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "target",
            "disable",
            "missing-target",
            "--file",
            request_file,
        ],
        1,
        "target_identity_conflict",
    )?;
    std::fs::write(&request_path, vec![b' '; 128 * 1024 + 1])?;
    f.refused(
        Some(&writer),
        &[
            "runtime",
            "target",
            "disable",
            "missing-target",
            "--file",
            request_file,
        ],
        1,
        "input exceeds 131072 UTF-8 bytes",
    )?;
    assert_eq!(
        before,
        f.ok(
            Some(&writer),
            &["task", "followup", "show", "work", "--version", "1"]
        )?
    );
    assert_eq!(
        f.ok(Some(&writer), &["task", "show", "work"])?["version"],
        1
    );
    Ok(())
}

#[test]
fn hidden_worker_roles_require_explicit_unambiguous_state() -> Result<()> {
    let f = Fixture(tempfile::tempdir()?);
    let root = f.0.path().to_str().context("worker root")?;
    let different = f.0.path().join("other");
    let different = different.to_str().context("different root")?;
    for role in ["__managed-worker-v1", "__managed-contained-v1"] {
        f.refused(None, &[role], 2, "--root")?;
        f.refused(
            None,
            &[role, "--root", "relative"],
            1,
            "requires an absolute --root",
        )?;
        f.refused(
            None,
            &[role, "--root", different],
            1,
            "state binding conflicts with --root",
        )?;
        f.refused(
            None,
            &["--group", "product", role, "--root", root],
            1,
            "does not accept Mail group/session credentials",
        )?;
        f.refused(
            Some("00000000-0000-0000-0000-000000000001"),
            &[role, "--root", root],
            1,
            "does not accept Mail group/session credentials",
        )?;
    }
    assert!(!f.0.path().join("mail.db").exists());
    assert!(!f.0.path().join("schema.lock").exists());
    Ok(())
}

#[tokio::test]
async fn hidden_worker_input_is_bounded_and_cannot_enroll_or_claim_authority() -> Result<()> {
    const FAILURE: &[u8] =
        b"agent-mail: managed worker failed; inspect its protected runtime records\n";
    let f = Fixture(tempfile::tempdir()?);
    let root = f.0.path().to_str().context("worker root")?;
    let oversized = vec![b' '; 8193];
    for role in ["__managed-worker-v1", "__managed-contained-v1"] {
        let args = [role, "--root", root];
        // Even a JSON object cannot initialize missing protected state.
        for input in [
            b"".as_slice(),
            b"[",
            b"[]",
            b"{} trailing",
            b"{}",
            &oversized,
        ] {
            let output = f.worker_input(&args, input, true).await?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert_eq!(output.stderr, FAILURE);
        }
        let started = Instant::now();
        let output = f.worker_input(&args, b"{}", false).await?;
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "did not exercise the EOF wait"
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, FAILURE);
    }
    assert!(!f.0.path().join("mail.db").exists());
    assert!(!f.0.path().join("schema.lock").exists());

    // Genuine ordinary isolated setup, with no protected process/admission fixtures.
    f.ok(None, &["init", "product"])?;
    // Establish that later refusals are not just invalid fixture storage.
    agent_mail::managed_runtime::open_worker_store(f.0.path())
        .await?
        .close()
        .await;
    let before = f.ok(None, &["agent", "list"])?;
    let request = br#"{"correlation":{"group":"product","task":"absent","attempt":"absent","fence":1,"dispatch_key":"absent"},"controller_receipt":"absent"}"#;
    for role in ["__managed-worker-v1", "__managed-contained-v1"] {
        for input in [request.as_slice(), br#"{"unknown":"not a worker request"}"#] {
            // Contained runtime must reject this ordinary pipe as a control socket;
            // custodian runtime must refuse absent original controller/process authority.
            let output = f.worker_input(&[role, "--root", root], input, true).await?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert_eq!(output.stderr, FAILURE);
        }
    }
    assert_eq!(before, f.ok(None, &["agent", "list"])?);
    assert!(!f.0.path().join("managed-runtime").exists());
    Ok(())
}
