//! New coordination capabilities exercised through fresh CLI processes.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Stdio},
};
struct Fixture {
    dir: tempfile::TempDir,
    writer: String,
    owner: String,
}

#[test]
fn subtasks_and_dependency_plans_use_the_public_cli() -> Result<()> {
    let f = Fixture::new()?;
    f.task("parent")?;
    f.call(
        Some(&f.writer),
        &[
            "task",
            "create",
            "child",
            "Implement part",
            "--owner",
            "owner",
            "--parent",
            "parent",
        ],
        None,
        true,
    )?;
    f.call(
        Some(&f.writer),
        &[
            "task",
            "create",
            "child",
            "Implement part",
            "--owner",
            "owner",
            "--parent",
            "parent",
            "--parent-version",
            "1",
        ],
        None,
        false,
    )?;
    let tree = f.call(Some(&f.owner), &["task", "tree", "parent"], None, false)?;
    assert_eq!(tree["tasks"].as_array().unwrap().len(), 2);
    let update = json!({"version":1,"mode":"all","requirements":[{"condition":{"task":"child","states":["accepted"],"accepted_revision":"rev-child"},"version":1}],"reason":"accept child before parent review"});
    f.call(
        Some(&f.owner),
        &["task", "dependencies", "parent", "--file", "-"],
        Some(update.clone()),
        true,
    )?;
    let result = f.call(
        Some(&f.writer),
        &["task", "dependencies", "parent", "--file", "-"],
        Some(update),
        false,
    )?;
    assert_eq!(result["version"], 2);
    let plan = f.call(
        Some(&f.owner),
        &["task", "dependencies", "parent"],
        None,
        false,
    )?;
    assert_eq!(plan["readiness"]["ready"], false);
    f.call(
        Some(&f.writer),
        &[
            "task",
            "update",
            "child",
            "--version",
            "1",
            "--state",
            "accepted",
            "--reason",
            "reviewed",
            "--accepted-revision",
            "rev-child",
        ],
        None,
        false,
    )?;
    assert_eq!(
        f.call(
            Some(&f.owner),
            &["task", "dependencies", "parent"],
            None,
            false
        )?["readiness"]["ready"],
        true
    );
    assert_eq!(
        f.call(Some(&f.owner), &["task", "show", "parent"], None, false)?["state"],
        "open"
    );
    Ok(())
}
impl Fixture {
    fn new() -> Result<Self> {
        let mut f = Self {
            dir: tempfile::tempdir()?,
            writer: String::new(),
            owner: String::new(),
        };
        f.call(None, &["init", "default"], None, false)?;
        f.writer = f.call(
            None,
            &["agent", "add", "writer", "--show-session"],
            None,
            false,
        )?["session"]
            .as_str()
            .unwrap()
            .into();
        f.owner = f.call(
            None,
            &["agent", "add", "owner", "--show-session"],
            None,
            false,
        )?["session"]
            .as_str()
            .unwrap()
            .into();
        Ok(f)
    }
    fn call(
        &self,
        session: Option<&str>,
        args: &[&str],
        input: Option<Value>,
        fail: bool,
    ) -> Result<Value> {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_agent-mail"));
        cmd.args(["--state-dir", self.dir.path().to_str().unwrap()])
            .args(args)
            .env_remove("AGENT_MAIL_GROUP")
            .env_remove("AGENT_MAIL_SESSION")
            .env_remove("HERDR_ENV")
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("AGENT_MAIL_LAUNCH")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(s) = session {
            cmd.env("AGENT_MAIL_SESSION", s);
        }
        let mut child = cmd.spawn()?;
        if let Some(input) = input {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(&serde_json::to_vec(&input)?)?;
        }
        drop(child.stdin.take());
        let out = child.wait_with_output()?;
        if fail {
            ensure!(
                !out.status.success(),
                "unexpected success: {}",
                String::from_utf8_lossy(&out.stdout)
            );
            return Ok(Value::Null);
        }
        ensure!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(serde_json::from_slice(&out.stdout)?)
    }
    fn task(&self, id: &str) -> Result<Value> {
        self.call(
            Some(&self.writer),
            &[
                "task",
                "create",
                id,
                "Implement contract",
                "--owner",
                "owner",
            ],
            None,
            false,
        )
    }
}
#[test]
fn cli_preserves_pinned_record_and_fetches_verified_artifact() -> Result<()> {
    let f = Fixture::new()?;
    f.task("task")?;
    let first=f.call(Some(&f.writer),&["record","create","--file","-"],Some(json!({"id":"contract","title":"Contract","body":"Original text","summary":"Shared interface"})),false)?;
    assert_eq!(first["revision"], 1);
    f.call(
        Some(&f.writer),
        &["record", "link", "--file", "-"],
        Some(json!({"target":{"task":"task"},"id":"contract","revision":1})),
        false,
    )?;
    let update = json!({"revision":1,"title":"Contract","body":"Corrected text","summary":"Corrected interface","reason":"Fix constraint"});
    let changed = f.call(
        Some(&f.writer),
        &["record", "update", "contract", "--file", "-"],
        Some(update.clone()),
        false,
    )?;
    assert_eq!(
        changed,
        f.call(
            Some(&f.writer),
            &["record", "update", "contract", "--file", "-"],
            Some(update),
            false
        )?
    );
    f.call(Some(&f.owner),&["record","update","contract","--file","-"],Some(json!({"revision":2,"title":"Contract","body":"Unauthorized","summary":"No","reason":"No"})),true)?;
    let task = f.call(Some(&f.owner), &["task", "show", "task"], None, false)?;
    assert_eq!(task["records"][0]["revision"], 1);
    assert_eq!(task["records"][0]["current_revision"], 2);
    assert_eq!(
        f.call(
            Some(&f.owner),
            &["record", "show", "contract", "--revision", "1"],
            None,
            false
        )?["body"],
        "Original text"
    );
    let history = f.call(
        Some(&f.owner),
        &["record", "history", "contract", "--before", "2"],
        None,
        false,
    )?;
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    let input = f.dir.path().join("evidence.txt");
    let output = f.dir.path().join("restored.txt");
    std::fs::write(&input, b"verified evidence\n")?;
    let artifact=f.call(Some(&f.writer),&["artifact","ingest","--file","-","--input",input.to_str().unwrap()],Some(json!({"id":"evidence","location":{"kind":"managed"},"digest":null,"media_type":"text/plain","size":null,"provenance":"Review output"})),false)?;
    assert!(
        artifact["resource"]["digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    f.call(
        Some(&f.writer),
        &["artifact", "link", "evidence", "--file", "-"],
        Some(json!({"kind":"task","id":"task"})),
        false,
    )?;
    f.call(
        Some(&f.writer),
        &["artifact", "link", "evidence", "--file", "-"],
        Some(json!({"kind":"record_revision","id":"contract","version":1})),
        false,
    )?;
    let task = f.call(Some(&f.owner), &["task", "show", "task"], None, false)?;
    assert_eq!(task["artifacts"][0]["resource"]["id"], "evidence");
    f.call(
        Some(&f.owner),
        &[
            "artifact",
            "fetch",
            "evidence",
            "--output",
            output.to_str().unwrap(),
        ],
        None,
        false,
    )?;
    assert_eq!(std::fs::read(&output)?, std::fs::read(&input)?);
    let context = f.call(Some(&f.owner), &["context"], None, false)?;
    assert!(serde_json::to_vec(&context)?.len() <= 4096);
    assert_eq!(context["records"][0]["revision"], 1);
    assert_eq!(context["artifacts"][0]["artifact"], "evidence");
    let backup = f.dir.path().join("backup");
    f.call(
        Some(&f.owner),
        &["artifact", "backup", backup.to_str().unwrap()],
        None,
        true,
    )?;
    assert!(!backup.exists());
    f.call(
        None,
        &["artifact", "backup", backup.to_str().unwrap()],
        None,
        false,
    )?;
    assert!(backup.exists());
    Ok(())
}
#[test]
fn cli_pages_terminal_tasks_and_rejects_changed_history_cursor() -> Result<()> {
    let f = Fixture::new()?;
    for id in ["a", "b", "c"] {
        f.task(id)?;
        f.call(Some(&f.writer),&["task","update",id,"--file","-"],Some(json!({"version":1,"reason":"Stop work","patch":{"state":"cancelled"},"resolve_message":null})),false)?;
    }
    let normal = f.call(Some(&f.owner), &["task", "list"], None, false)?;
    assert!(normal["items"].as_array().unwrap().is_empty());
    let page = f.call(
        Some(&f.owner),
        &["task", "list", "--all-states", "--limit", "2"],
        None,
        false,
    )?;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    let cursor = page["next_cursor"].as_str().unwrap();
    let rest = f.call(
        Some(&f.owner),
        &[
            "task",
            "list",
            "--all-states",
            "--limit",
            "2",
            "--cursor",
            cursor,
        ],
        None,
        false,
    )?;
    assert_eq!(rest["items"].as_array().unwrap().len(), 1);
    assert_eq!(rest["items"][0]["id"], "c");
    f.call(
        Some(&f.owner),
        &[
            "task", "list", "--state", "active", "--limit", "2", "--cursor", cursor,
        ],
        None,
        true,
    )?;
    let history = f.call(
        Some(&f.owner),
        &["task", "history", "a", "--limit", "1"],
        None,
        false,
    )?;
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    let cursor = history["next_cursor"].as_str().unwrap();
    let rest = f.call(
        Some(&f.owner),
        &["task", "history", "a", "--limit", "1", "--cursor", cursor],
        None,
        false,
    )?;
    assert_eq!(rest["items"].as_array().unwrap().len(), 1);
    f.call(
        Some(&f.owner),
        &["task", "history", "b", "--limit", "1", "--cursor", cursor],
        None,
        true,
    )?;
    Ok(())
}
