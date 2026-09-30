//! Operator-owned launch context. No global current-agent state or credential files.
use agent_mail::{identity::Binding, store::Store};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    io::{IsTerminal, Write},
    os::unix::process::CommandExt,
    path::Path,
    process::Command,
};

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
    let actor = store.launch_identity(group, name).await?;
    let Binding::Standalone { session } = &actor.binding else {
        bail!("run requires a standalone agent; Herdr owns launches for pane-bound agents")
    };
    let runtime = Path::new(program)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("custom");
    let launch_id = uuid::Uuid::new_v4().to_string();
    if matches!(runtime, "codex" | "claude") {
        agent_mail::supervision::ensure_running(&store, &actor, &std::env::current_exe()?).await?;
        eprintln!(
            "Agent Mail: worker connected; native delivery verification will run when this client is idle. Registration alone does not prove delivery."
        );
    }
    if matches!(runtime, "codex" | "claude") {
        store
            .begin_launch(&actor, &launch_id, runtime.parse()?)
            .await?;
    }
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
            if !(std::io::stdin().is_terminal() && interactive_codex(forwarded)) {
                child.arg("--no-daemon");
            }
            child.args(["-c", "features.hooks=true"]);
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
        .env("AGENT_MAIL_LAUNCH", &launch_id)
        .env("AGENT_MAIL_SESSION", session.to_string())
        .env("AGENT_MAIL_GROUP", group)
        .env("AGENT_MAIL_STATE_DIR", &root);
    for key in [
        "AGENT_MAIL_CODEX_SOCKET",
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
    if runtime == "codex" && std::io::stdin().is_terminal() && interactive_codex(forwarded) {
        return interactive(child, &root, &actor, &launch_id).await;
    }
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

// Only native interactive sessions need a persistent queue. Utility/exec commands
// retain their exact interface and process replacement semantics.
fn interactive_codex(args: &[OsString]) -> bool {
    !args.iter().any(|arg| {
        matches!(
            arg.to_str(),
            Some(
                "exec"
                    | "e"
                    | "app-server"
                    | "login"
                    | "logout"
                    | "mcp"
                    | "mcp-server"
                    | "completion"
                    | "debug"
                    | "features"
                    | "sandbox"
                    | "apply"
                    | "cloud"
                    | "--help"
                    | "-h"
                    | "--version"
                    | "-V"
                    | "--remote"
            )
        ) || arg.to_str().is_some_and(|s| s.starts_with("--remote="))
    })
}

/// Keep a private backend alive for the native TUI, then reap both children.
async fn interactive(
    mut ui: Command,
    root: &Path,
    actor: &agent_mail::store::Mailbox,
    launch: &str,
) -> Result<()> {
    use std::{process::Stdio, time::Duration};
    use tokio::{
        process::Command as AsyncCommand,
        signal::unix::{SignalKind, signal},
    };
    // A short private directory also stays below macOS's Unix socket path limit.
    let directory = tempfile::Builder::new()
        .prefix("agent-mail-")
        .tempdir_in("/tmp")?;
    let socket = directory.path().join("codex.sock");
    let endpoint = format!("unix://{}", socket.display());
    let log = std::fs::File::create(directory.path().join("server.log"))?;
    let mut backend = AsyncCommand::new(ui.get_program());
    // Pass only the managed hook configuration to app-server. Session/model and
    // permission arguments remain owned by the native UI's session request.
    backend.args(["app-server", "--listen", &endpoint]);
    let args: Vec<_> = ui.get_args().collect();
    let mut i = 0;
    while i + 1 < args.len() {
        if args[i] == "-c" && args[i + 1].to_string_lossy().starts_with("hooks.") {
            backend.arg(args[i]).arg(args[i + 1]);
        }
        i += 1;
    }
    backend.args(["-c", "features.hooks=true"]);
    for (key, value) in ui.get_envs() {
        if let Some(value) = value {
            backend.env(key, value);
        } else {
            backend.env_remove(key);
        }
    }
    backend.env("AGENT_MAIL_CODEX_SOCKET", &socket);
    // Backend cannot consume terminal input or compete with the UI's signal handling.
    backend
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .process_group(0);
    let mut term = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    // Ctrl-C belongs to the foreground native UI. Prevent it from killing this supervisor.
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut server = backend.spawn().context("start private Codex app-server")?;
    let startup = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = server.try_wait()? {
                bail!(
                    "Codex app-server exited: {status}; verify codex app-server --help supports --listen unix://PATH"
                );
            }
            if tokio::net::UnixStream::connect(&socket).await.is_ok() {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let interrupted = tokio::select! {
        ready = startup => { ready.context("Codex app-server startup timed out")??; None }
        _ = term.recv() => Some(143),
        _ = hangup.recv() => Some(129),
        _ = interrupt.recv() => Some(130),
    };
    if let Some(code) = interrupted {
        let _ = server.kill().await;
        drop(directory);
        std::process::exit(code);
    }
    ui.arg("--remote")
        .arg(endpoint)
        .env("AGENT_MAIL_CODEX_SOCKET", &socket);
    // Delivery uses the installation's existing worker. Do not silently install a service.
    if !agent_mail::service::running(root) {
        eprintln!("agent-mail: idle delivery needs `agent-mail service run` in another terminal");
    }
    let mut client = AsyncCommand::from(ui).kill_on_drop(true).spawn()?;
    let store = Store::open(root, false).await?;
    let mut discovery = Box::pin(async {
        loop {
            if store.runtime_enabled(actor).await? {
                if let Ok(Some(thread)) = agent_mail::codex::sole_loaded_thread(&socket).await {
                    if store
                        .bind_launch_session(actor, launch, &thread.to_string())
                        .await?
                        && store
                            .attach_codex_from_hook(actor, &socket, thread, launch)
                            .await
                            .is_ok()
                    {
                        break Ok::<_, anyhow::Error>(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let mut attached = false;
    let code = loop {
        tokio::select! {
            status = client.wait() => { break status?.code().unwrap_or(1); }
            status = server.wait() => {
                eprintln!("agent-mail: Codex backend stopped: {}", status?);
                let _ = client.kill().await;
                break 1;
            }
            _ = term.recv() => { let _ = client.kill().await; break 143; }
            _ = hangup.recv() => { let _ = client.kill().await; break 129; }
            _ = interrupt.recv() => {}
            result = &mut discovery, if !attached => { result?; attached = true; }
        }
    };
    let _ = server.kill().await;
    drop(discovery);
    store.close().await;
    drop(directory);
    eprintln!("agent-mail: local Codex backend stopped; tasks and mail are preserved");
    std::process::exit(code);
}
