//! Operator-owned launch context. No global current-agent state or credential files.
use agent_mail::{identity::Binding, store::Store};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{ffi::OsString, io::Write, os::unix::process::CommandExt, path::Path, process::Command};

/// Shared hook settings for both explicit configuration and managed launch.
pub(super) fn hook_settings(claude: bool, executable: &str) -> Value {
    let adapter = if claude { "claude-hook" } else { "hook" };
    let command = format!("{executable} adapter {adapter}");
    let mut hooks = serde_json::Map::new();
    for event in [
        "SessionStart",
        "PostCompact",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "Stop",
    ]
    .into_iter()
    .chain(claude.then_some("SessionEnd"))
    .chain(claude.then_some("StopFailure"))
    {
        hooks.insert(
            event.into(),
            json!([{"hooks":[{"type":"command","command":command,"timeout":10}]}]),
        );
    }
    json!({"hooks":hooks})
}

pub(super) async fn launch(root: &Path, group: &str, name: &str, args: &[OsString]) -> Result<()> {
    let (program, forwarded) = args.split_first().context("provide a command after --")?;
    let store = Store::open(root, false).await?;
    let actor = store
        .mailbox(group, name)
        .await
        .context("agent is not registered; run agent-mail agent add NAME")?;
    let Binding::Standalone { session } = actor.binding else {
        bail!("run requires a standalone agent; Herdr owns launches for pane-bound agents")
    };
    store.close().await;
    let root = root.canonicalize()?;
    let executable = std::env::current_exe()?.canonicalize()?;
    let quoted = format!(
        "'{}'",
        executable
            .to_str()
            .context("executable path must be UTF-8")?
            .replace('\'', "'\\''")
    );
    let mut child = Command::new(program);
    match Path::new(program).file_name().and_then(|s| s.to_str()) {
        Some("claude") => {
            let plugin = root.join("runtime-hooks/claude");
            let manifest = plugin.join(".claude-plugin/plugin.json");
            write_json(
                &manifest,
                &json!({
                    "name":"agent-mail-runtime",
                    "version":env!("CARGO_PKG_VERSION"),
                    "description":"Agent Mail lifecycle recovery and delivery receipts",
                    "author":{"name":"Youssef Tharwat"},
                    "hooks":hook_settings(true, &quoted)["hooks"]
                }),
            )?;
            child.arg("--plugin-dir").arg(plugin);
        }
        Some("codex") => {
            // Each event is an additive CLI configuration layer; existing hook sources remain.
            child.args(["--no-daemon", "-c", "features.hooks=true"]);
            for (event, groups) in hook_settings(false, &quoted)["hooks"]
                .as_object()
                .context("hook map")?
            {
                let command = groups[0]["hooks"][0]["command"]
                    .as_str()
                    .context("hook command")?;
                let command = serde_json::to_string(command)?;
                child.arg("-c").arg(format!(
                    "hooks.{event}=[{{hooks=[{{type=\"command\",command={command},timeout=10}}]}}]"
                ));
            }
        }
        _ => {}
    }
    child
        .args(forwarded)
        .env("AGENT_MAIL_SESSION", session.to_string())
        .env("AGENT_MAIL_GROUP", group)
        .env("AGENT_MAIL_STATE_DIR", root);
    for key in [
        "HERDR_ENV",
        "HERDR_PANE_ID",
        "HERDR_SOCKET_PATH",
        "HERDR_PLUGIN_ID",
    ] {
        child.env_remove(key);
    }
    // Make child tools and hooks use the same Mail binary as the launcher.
    let mut paths = vec![
        executable
            .parent()
            .context("executable directory")?
            .to_path_buf(),
    ];
    if let Some(path) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&path));
    }
    child.env("PATH", std::env::join_paths(paths)?);
    // Replace the launcher: native TTY, signals and exit status belong to the client.
    let error = child.exec();
    Err(error).context("launch command")
}

/// Atomically refresh generated, credential-free configuration inside private Mail state.
fn write_json(path: &Path, value: &Value) -> Result<()> {
    let content = serde_json::to_vec_pretty(value)?;
    if std::fs::read(path).is_ok_and(|existing| existing == content) {
        return Ok(());
    }
    let parent = path.parent().context("configuration directory")?;
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&content)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .context("write runtime hook configuration")?;
    Ok(())
}
