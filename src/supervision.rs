//! Explicit macOS service installation and state-directory discovery.
//!
//! Installation copies the executable and writes a launchd plist; restoration and
//! uninstallation verify that the plist belongs to this state directory. Lifecycle
//! operations invoke launchctl and must run without holding the database schema lock.
//! Other Unix systems can run the worker through their own supervisor.

use anyhow::{Context, Result, ensure};
use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};

const LABEL: &str = "io.github.youssef-tharwat.agent-mail";

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Render a launchd property list for an executable and state directory.
///
/// # Errors
/// Either path cannot be represented as UTF-8.
pub fn plist(binary: &Path, state: &Path) -> Result<String> {
    let binary = xml(binary.to_str().context("binary path is not UTF-8")?);
    let state = xml(state.to_str().context("state path is not UTF-8")?);
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{LABEL}</string>
<key>ProgramArguments</key><array><string>{binary}</string><string>--state-dir</string><string>{state}</string><string>service</string><string>run</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>ThrottleInterval</key><integer>10</integer>
<key>ProcessType</key><string>Background</string>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>/dev/null</string>
</dict></plist>
"#
    ))
}

fn target() -> Result<(String, PathBuf)> {
    ensure!(
        cfg!(target_os = "macos"),
        "automatic service installation is macOS-only; use service run under your supervisor on other Unix systems"
    );
    let output = Command::new("id").arg("-u").output()?;
    ensure!(output.status.success(), "cannot determine user ID");
    let uid = String::from_utf8(output.stdout)?.trim().to_owned();
    ensure!(
        uid.bytes().all(|c| c.is_ascii_digit()) && !uid.is_empty(),
        "invalid user ID"
    );
    let path = dirs::home_dir()
        .context("home directory unavailable")?
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"));
    Ok((format!("gui/{uid}"), path))
}

fn launch(args: &[&str]) -> Result<()> {
    let output = Command::new("launchctl").args(args).output()?;
    ensure!(
        output.status.success(),
        "launchctl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Install the current executable and start its macOS launchd service.
///
/// # Errors
/// The platform is unsupported, configuration conflicts, a worker is running, or filesystem or launchctl operations fail.
pub fn install(root: &Path) -> Result<()> {
    let (domain, path) = target()?;
    let root = root.canonicalize()?;
    let bin = root.join("bin/agent-mail");
    let body = plist(&bin, &root)?;
    if path.exists() {
        ensure!(
            std::fs::read_to_string(&path)? == body,
            "existing service uses another state directory; uninstall it explicitly first"
        );
    }
    ensure!(
        !crate::service::running(&root),
        "stop the service before installing/updating its executable"
    );
    std::fs::create_dir_all(bin.parent().context("missing binary parent")?)?;
    let temp = bin.with_extension("new");
    std::fs::copy(std::env::current_exe()?, &temp)?;
    std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o700))?;
    std::fs::rename(temp, &bin)?;
    std::fs::create_dir_all(path.parent().context("missing LaunchAgents directory")?)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(body.as_bytes())?;
    file.sync_all()?;
    let service_target = format!("{domain}/{LABEL}");
    let loaded = Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success();
    if !loaded {
        launch(&[
            "bootstrap",
            &domain,
            path.to_str().context("invalid plist path")?,
        ])?;
    }
    launch(&["kickstart", &service_target])
}

/// Restart an existing service after verifying its state-directory ownership.
///
/// # Errors
/// The platform is unsupported, ownership conflicts, or filesystem or launchctl operations fail.
pub fn restore(root: &Path) -> Result<bool> {
    let (domain, path) = target()?;
    if !path.exists() {
        return Ok(false);
    }
    let root = root.canonicalize()?;
    ensure!(
        std::fs::read_to_string(&path)? == plist(&root.join("bin/agent-mail"), &root)?,
        "service belongs to another state directory"
    );
    let service_target = format!("{domain}/{LABEL}");
    if !Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success()
    {
        launch(&[
            "bootstrap",
            &domain,
            path.to_str().context("invalid plist path")?,
        ])?;
    }
    launch(&["kickstart", &service_target])?;
    Ok(true)
}

/// Unload the owned service and remove its plist while preserving state.
///
/// # Errors
/// The platform is unsupported, ownership conflicts, or filesystem or launchctl operations fail.
pub fn uninstall(root: &Path) -> Result<()> {
    let (domain, path) = target()?;
    if !path.exists() {
        return Ok(());
    }
    let root = root.canonicalize()?;
    ensure!(
        std::fs::read_to_string(&path)? == plist(&root.join("bin/agent-mail"), &root)?,
        "service belongs to another state directory"
    );
    let service_target = format!("{domain}/{LABEL}");
    if Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success()
    {
        launch(&["bootout", &service_target])?;
    }
    std::fs::remove_file(path)?;
    Ok(())
}

/// Return the per-user configuration path for state-directory discovery.
///
/// # Errors
/// The platform cannot provide a configuration directory.
pub fn locator() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("config directory unavailable")?
        .join("agent-mail/state-path"))
}

/// Resolve explicit, plugin, saved, or default state-directory configuration.
///
/// # Errors
/// Required environment or platform directories are absent, or the locator cannot be read.
pub fn state_root(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(root) = explicit {
        return Ok(root);
    }
    if std::env::var("HERDR_PLUGIN_ID").as_deref() == Ok(crate::PLUGIN_ID) {
        return std::env::var_os("HERDR_PLUGIN_STATE_DIR")
            .map(PathBuf::from)
            .context("missing plugin state directory");
    }
    let locator = locator()?;
    if locator.exists() {
        return Ok(PathBuf::from(std::fs::read_to_string(locator)?));
    }
    Ok(dirs::data_local_dir()
        .context("data directory unavailable")?
        .join("agent-mail"))
}

/// Persist a canonical state-directory locator with private permissions.
///
/// # Errors
/// The path cannot be canonicalized or encoded as UTF-8, or filesystem operations fail.
pub fn save_locator(root: &Path) -> Result<()> {
    let path = locator()?;
    std::fs::create_dir_all(path.parent().context("config parent missing")?)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(
        root.canonicalize()?
            .to_str()
            .context("state path is not UTF-8")?
            .as_bytes(),
    )?;
    file.sync_all()?;
    Ok(())
}

/// Establish one local worker and verify its authenticated event stream.
/// Managed launches fail before starting the client when this cannot be established.
/// # Errors
/// Process startup, current-binary mismatch, stream authentication or timeout fails.
pub async fn ensure_running(
    store: &crate::store::Store,
    actor: &crate::store::Mailbox,
    executable: &Path,
) -> Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    let mut child = None;
    if !crate::service::running(store.root()) {
        use std::os::unix::fs::OpenOptionsExt;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(store.root().join("service.log"))?;
        let mut command = Command::new(executable);
        command
            .args(["--state-dir"])
            .arg(store.root())
            .args(["service", "run"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .process_group(0);
        for key in [
            "AGENT_MAIL_SESSION",
            "AGENT_MAIL_GROUP",
            "AGENT_MAIL_LAUNCH",
            "HERDR_ENV",
            "HERDR_PLUGIN_ID",
            "HERDR_SOCKET_PATH",
        ] {
            command.env_remove(key);
        }
        child = Some(command.spawn().context("start local delivery worker")?);
    }
    for _ in 0..80 {
        if crate::service::running(store.root()) {
            let ready = tokio::time::timeout(std::time::Duration::from_millis(100), async {
                let mut stream = crate::stream::connect(store, actor, 0).await?;
                anyhow::ensure!(
                    matches!(
                        crate::stream::next(&mut stream).await?,
                        crate::stream::Frame::Ready { .. }
                    ),
                    "stream rejected identity"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await;
            if matches!(ready, Ok(Ok(()))) {
                return Ok(());
            }
        }
        if let Some(child) = child.as_mut() {
            let _ = child.try_wait()?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "delivery worker did not become ready; inspect {}/service.log or run agent-mail service run",
        store.root().display()
    )
}
